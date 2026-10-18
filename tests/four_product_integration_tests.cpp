#include "four_product_test_support.hpp"

#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <iostream>
#include <set>
#include <thread>

namespace {
namespace common = binance_market_data::common::v1;
namespace wire = binance_market_data::gateway::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace g11 = binance_market_data::gateway::g11;
namespace production = binance_market_data::gateway::production;
namespace fixture = production::four_product_test;
namespace support = production::test_support;

#define REQUIRE(condition)                                                     \
  do {                                                                         \
    if (!(condition)) {                                                        \
      throw std::runtime_error{#condition};                                    \
    }                                                                          \
  } while (false)

template <typename Predicate> void eventually(Predicate predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  while (!predicate()) {
    REQUIRE(std::chrono::steady_clock::now() < deadline);
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
  }
}

production::GatewayOptions options() {
  production::GatewayOptions result;
  result.allow_ephemeral_listen_for_testing = true;
  result.initial_startup_timeout = std::chrono::seconds{3};
  return result;
}

fixture::Configuration configuration() {
  return fixture::configure_file(BMD_GATEWAY_FOUR_PRODUCT_CONFIG);
}

void deadline(grpc::ClientContext &context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds{20});
}

wire::OrderBookSubscriptionRequest book_request(const g11::MarketKey &key) {
  wire::OrderBookSubscriptionRequest result;
  result.set_request_id("four-book");
  result.set_schema_version(g7::kOrderBookRequestSchema);
  result.set_venue(key.venue);
  result.set_market(key.market);
  result.set_symbol(key.symbol);
  result.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  result.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  result.add_supported_update_schema_versions(g7::kUpdateSchema);
  return result;
}

wire::EventSubscriptionRequest event_request(const g11::MarketKey &key) {
  wire::EventSubscriptionRequest result;
  result.set_request_id("four-event");
  result.set_schema_version(g9::kEventRequestSchema);
  auto *selector = result.add_selectors();
  selector->set_venue(key.venue);
  selector->set_market(key.market);
  selector->set_symbol(key.symbol);
  selector->set_stream(common::STREAM_DIFF_DEPTH);
  result.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  result.add_supported_payload_schema_versions(g9::kDepthEventSchema);
  return result;
}

using Stub = wire::BinanceMarketDataGatewayService::Stub;
std::unique_ptr<Stub> stub_for(production::ProductionGateway &gateway) {
  return wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(gateway.observe().selected_port),
      grpc::InsecureChannelCredentials()));
}

wire::GatewayStatusSnapshot status(Stub &stub) {
  grpc::ClientContext context;
  deadline(context);
  wire::GatewayStatusRequest request;
  request.set_request_id("four-status");
  request.set_schema_version(g10::kStatusRequestSchema);
  wire::GatewayStatusSnapshot result;
  REQUIRE(stub.GetGatewayStatus(&context, request, &result).ok());
  REQUIRE(result.markets_size() == 4);
  return result;
}

// Contexts outlive their readers and are cancelled on exceptional test exits.
struct Streams final {
  grpc::ClientContext book_context;
  grpc::ClientContext event_context;
  std::unique_ptr<grpc::ClientReader<wire::OrderBookStreamItem>> book;
  std::unique_ptr<grpc::ClientReader<wire::GatewayEventEnvelope>> events;
  Streams(Stub &stub, const g11::MarketKey &key) {
    deadline(book_context);
    deadline(event_context);
    book = stub.SubscribeOrderBook(&book_context, book_request(key));
    events = stub.SubscribeEvents(&event_context, event_request(key));
    wire::OrderBookStreamItem item;
    wire::GatewayEventEnvelope event;
    REQUIRE(book->Read(&item) && item.has_subscription_accepted());
    REQUIRE(book->Read(&item) && item.has_snapshot());
    REQUIRE(item.snapshot().venue() == key.venue);
    REQUIRE(item.snapshot().market() == key.market);
    REQUIRE(item.snapshot().symbol() == key.symbol);
    REQUIRE(item.snapshot().bids_size() == 1);
    REQUIRE(item.snapshot().bids(0).price() == fixture::sample(key).bid);
    REQUIRE(item.snapshot().bids(0).quantity() ==
            fixture::sample(key).quantity);
    REQUIRE(events->Read(&event) && event.has_subscription_accepted());
  }
  ~Streams() {
    book_context.TryCancel();
    event_context.TryCancel();
  }
  void read(const support::market::DepthUpdate &expected,
            std::uint64_t generation) {
    wire::OrderBookStreamItem item;
    wire::GatewayEventEnvelope event;
    REQUIRE(book->Read(&item) && item.has_depth_update());
    REQUIRE(events->Read(&event) && event.has_depth_update());
    REQUIRE(item.depth_update().SerializeAsString() ==
            expected.SerializeAsString());
    REQUIRE(event.depth_update().SerializeAsString() ==
            expected.SerializeAsString());
    REQUIRE(item.delivery_metadata().connection_generation() == generation);
    REQUIRE(event.delivery_metadata().connection_generation() == generation);
  }
  void drain() {
    wire::OrderBookStreamItem item;
    wire::GatewayEventEnvelope event;
    while (book->Read(&item)) {
    }
    while (events->Read(&event)) {
    }
    static_cast<void>(book->Finish());
    static_cast<void>(events->Finish());
  }
};

void stopped(production::ProductionGateway &gateway,
             const fixture::Configuration &config) {
  const auto observed = gateway.observe();
  REQUIRE(observed.state == production::GatewayState::Stopped);
  REQUIRE(observed.tracked_contexts == 0U);
  REQUIRE(observed.products.size() == 4U);
  for (std::size_t i = 0; i < 4U; ++i) {
    const auto &product = observed.products[i];
    REQUIRE(product.runtime.owner_joined);
    REQUIRE(product.runtime.resident_subscription_count == 0U);
    REQUIRE(product.runtime.pending_admission_count == 0U);
    REQUIRE(product.events.active_subscriptions == 0U);
    REQUIRE(product.recovery.active_transport_count == 0U);
    REQUIRE(config.attempts[i]->active.load() == 0U);
    REQUIRE(config.attempts[i]->maximum_active.load() <= 1U);
  }
}

support::market::DepthUpdate publish(production::ProductionGateway &gateway,
                                     const g11::MarketKey &key,
                                     std::uint64_t id,
                                     std::uint64_t generation = 1U,
                                     std::optional<std::uint64_t> pu = {}) {
  auto *product = gateway.products_for_testing().find(key);
  REQUIRE(product != nullptr);
  auto update = fixture::depth(key, generation, id, pu);
  REQUIRE(
      product->event_publication().publish(
          std::make_shared<
              const binance_market_data::gateway::g4::NormalizedMarketEvent>(
              update),
          generation) == g9::EventPublishResult::Published);
  REQUIRE(product->runtime().submit_depth_update(update, {generation}) ==
          g3::AdmissionResult::Accepted);
  return update;
}

void pipeline_recovery_slow_consumers_and_shutdown() {
  auto config = configuration();
  const auto keys = config.config.market_keys;
  const std::array<unsigned, 4> price_scales{2U, 1U, 1U, 2U};
  const std::array<unsigned, 4> quantity_scales{3U, 2U, 3U, 2U};
  for (std::size_t i = 0; i < 4U; ++i) {
    REQUIRE(config.specs[i].key == keys[i]);
    REQUIRE(config.specs[i].numeric_spec.price_scale.value() ==
            price_scales[i]);
    REQUIRE(config.specs[i].numeric_spec.quantity_scale.value() ==
            quantity_scales[i]);
  }
  production::ProductionGateway gateway{std::move(config.specs),
                                        support::fixed_clock(), "four-pipeline",
                                        "127.0.0.1:0", options()};
  REQUIRE(gateway.start().code == production::StartCode::Serving);
  auto stub = stub_for(gateway);
  std::vector<std::unique_ptr<Streams>> streams;
  std::set<std::thread::id> owners;
  for (const auto &key : keys) {
    streams.push_back(std::make_unique<Streams>(*stub, key));
    auto *product = gateway.products_for_testing().find(key);
    const auto observed = product->runtime().observe();
    REQUIRE(observed.state == g3::RuntimeState::Live);
    owners.insert(observed.owner_thread_id);
  }
  REQUIRE(owners.size() == 4U);
  REQUIRE(gateway.observe().tracked_contexts == 8U);
  // Unconsumed production-size queues: one slow Book and Event session on ETH.
  auto *slow_product = gateway.products_for_testing().find(keys[1]);
  auto slow_book_result = slow_product->runtime().admit_order_book_subscription(
      {"slow-book", "four-pipeline", {}});
  REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(slow_book_result));
  auto slow_book = std::get<g3::AcceptedSubscription>(slow_book_result).channel;
  auto slow_event_result = slow_product->event_publication().admit(
      {"slow-event", common::STREAM_DIFF_DEPTH, g9::kDepthEventSchema});
  REQUIRE(
      std::holds_alternative<g9::AcceptedEventSubscription>(slow_event_result));
  auto slow_event =
      std::get<g9::AcceptedEventSubscription>(slow_event_result).channel;
  for (std::uint64_t id = 102U; id <= 166U; ++id) {
    for (std::size_t i = 0; i < keys.size(); ++i) {
      streams[i]->read(publish(gateway, keys[i], id), 1U);
    }
  }
  REQUIRE(slow_book->terminal_reserved());
  REQUIRE(slow_event->state() == g9::EventChannelState::TerminalGap);
  REQUIRE(slow_book->ordinary_size() <= 64U);
  REQUIRE(slow_event->ordinary_size() <= 64U);
  slow_book->close_from_writer();
  slow_product->runtime().notify_subscriber_closed();
  slow_product->event_publication().remove(slow_event);
  auto before = status(*stub);
  for (int i = 0; i < 4; ++i) {
    REQUIRE(before.markets(i).symbol() ==
            keys[static_cast<std::size_t>(i)].symbol);
    REQUIRE(before.markets(i).market() ==
            keys[static_cast<std::size_t>(i)].market);
    REQUIRE(before.markets(i).state() == common::STREAM_LIFECYCLE_STATE_LIVE);
    REQUIRE(before.markets(i).last_event_utc_ns() == 1700000000000000166ULL);
  }
  // Invalid USD-M pu is visible in the pre-Projection Event but never Applied.
  auto *failed = gateway.products_for_testing().find(keys[3]);
  const auto bad = publish(gateway, keys[3], 167U, 1U, 999U);
  wire::GatewayEventEnvelope event;
  REQUIRE(streams[3]->events->Read(&event) && event.has_depth_update());
  REQUIRE(event.depth_update().SerializeAsString() == bad.SerializeAsString());
  streams[3]->drain();
  eventually([&] {
    const auto r = failed->recovery().observe();
    return r.connection_generation == 2U && r.state == g5::RecoveryState::Live;
  });
  REQUIRE(config.attempts[3]->starts.load() == 2U);
  auto after = status(*stub);
  REQUIRE(after.markets(3).last_event_utc_ns() == 1700000000000000101ULL);
  for (std::size_t i = 0; i < 3U; ++i) {
    REQUIRE(config.attempts[i]->starts.load() == 1U);
    REQUIRE(after.markets(static_cast<int>(i)).last_event_utc_ns() ==
            before.markets(static_cast<int>(i)).last_event_utc_ns());
    streams[i]->read(publish(gateway, keys[i], 167U), 1U);
  }
  streams[3] = std::make_unique<Streams>(*stub, keys[3]);
  streams[3]->read(publish(gateway, keys[3], 102U, 2U), 2U);
  gateway.stop();
  for (auto &stream : streams) {
    stream->drain();
  }
  stopped(gateway, config);
}

void startup_rollback() {
  for (std::size_t failed = 0; failed < 4U; ++failed) {
    auto config = configuration();
    config = fixture::configure(config.config, failed);
    const auto key = config.specs[failed].key;
    production::ProductionGateway gateway{
        std::move(config.specs), support::fixed_clock(), "four-failure",
        "127.0.0.1:0", options()};
    const auto result = gateway.start();
    REQUIRE(result.code == production::StartCode::ProductInitialFailure);
    REQUIRE(result.product == key);
    REQUIRE(gateway.observe().selected_port == 0);
    stopped(gateway, config);
  }
  auto config = configuration();
  config.specs.back().options.runtime_test.before_owner_thread_creation = [] {
    throw std::runtime_error{"owner creation failure"};
  };
  production::ProductionGateway gateway{
      std::move(config.specs), support::fixed_clock(), "four-exception",
      "127.0.0.1:0", options()};
  bool caught = false;
  try {
    static_cast<void>(gateway.start());
  } catch (const std::runtime_error &) {
    caught = true;
  }
  REQUIRE(caught);
  stopped(gateway, config);
}

void all_four_must_be_live() {
  auto config = configuration();
  config =
      fixture::configure(config.config, 3U, support::AttemptMode::NeverLive);
  auto configured_options = options();
  configured_options.initial_startup_timeout = std::chrono::milliseconds{100};
  production::ProductionGateway gateway{std::move(config.specs),
                                        support::fixed_clock(), "four-timeout",
                                        "127.0.0.1:0", configured_options};
  bool observed_three_live = false;
  const auto result = gateway.start([&] {
    const auto observed = gateway.observe();
    if (observed.products[0].runtime.state == g3::RuntimeState::Live &&
        observed.products[1].runtime.state == g3::RuntimeState::Live &&
        observed.products[2].runtime.state == g3::RuntimeState::Live) {
      observed_three_live = true;
      REQUIRE(observed.state == production::GatewayState::Starting);
      REQUIRE(observed.selected_port == 0);
    }
    return false;
  });
  REQUIRE(observed_three_live);
  REQUIRE(result.code == production::StartCode::InitialStartupTimeout);
  stopped(gateway, config);
}

void global_context_limit() {
  auto config = configuration();
  const auto keys = config.config.market_keys;
  production::ProductionGateway gateway{std::move(config.specs),
                                        support::fixed_clock(), "four-cap",
                                        "127.0.0.1:0", options()};
  REQUIRE(gateway.start().code == production::StartCode::Serving);
  auto stub = stub_for(gateway);
  std::vector<std::unique_ptr<Streams>> streams;
  for (const auto &key : keys) {
    for (unsigned i = 0; i < 6U; ++i) {
      streams.push_back(std::make_unique<Streams>(*stub, key));
    }
  }
  REQUIRE(gateway.observe().tracked_contexts == 48U);
  REQUIRE(status(*stub).markets_size() == 4);
  grpc::ClientContext context;
  deadline(context);
  auto rejected = stub->SubscribeEvents(&context, event_request(keys[3]));
  wire::GatewayEventEnvelope event;
  REQUIRE(!rejected->Read(&event));
  REQUIRE(rejected->Finish().error_code() ==
          grpc::StatusCode::RESOURCE_EXHAUSTED);
  REQUIRE(gateway.observe().tracked_contexts == 48U);
  streams.back()->book_context.TryCancel();
  streams.back()->event_context.TryCancel();
  streams.back()->drain();
  streams.pop_back();
  eventually([&] { return gateway.observe().tracked_contexts == 46U; });
  streams.push_back(std::make_unique<Streams>(*stub, keys[0]));
  REQUIRE(gateway.observe().tracked_contexts == 48U);
  gateway.stop();
  for (auto &stream : streams) {
    stream->drain();
  }
  stopped(gateway, config);
}
} // namespace

int main() {
  try {
    pipeline_recovery_slow_consumers_and_shutdown();
    std::cout
        << "FOUR_PRODUCT_PIPELINE_RECOVERY_SLOW_CONSUMERS_SHUTDOWN=PASS\n";
    startup_rollback();
    std::cout << "FOUR_PRODUCT_START_FAILURE_AND_EXCEPTION_ROLLBACK=PASS\n";
    all_four_must_be_live();
    std::cout << "FOUR_PRODUCT_ALL_INITIAL_LIVE_REQUIRED=PASS\n";
    global_context_limit();
    std::cout << "FOUR_PRODUCT_GLOBAL_48_CONTEXTS=PASS\n";
  } catch (const std::exception &error) {
    std::cerr << "FOUR_PRODUCT_INTEGRATION=FAIL " << error.what() << '\n';
    return 1;
  }
}
