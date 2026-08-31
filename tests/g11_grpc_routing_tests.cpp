#include "grpc_service.hpp"
#include "multi_market_runtime.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/market/v1/market_events.pb.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace g11 = binance_market_data::gateway::g11;
namespace market = binance_market_data::market::v1;
namespace wire = binance_market_data::gateway::v1;

class TestFailure final : public std::exception {
public:
  explicit TestFailure(std::string message) : message_{std::move(message)} {}

  [[nodiscard]] const char *what() const noexcept override {
    return message_.c_str();
  }

private:
  std::string message_;
};

void require(bool condition, std::string_view expression) {
  if (!condition) {
    throw TestFailure{std::string{expression}};
  }
}

template <typename Actual, typename Expected>
void require_equal(const Actual &actual, const Expected &expected,
                   std::string_view expression) {
  if (!(actual == expected)) {
    throw TestFailure{std::string{expression}};
  }
}

#define REQUIRE(condition) require((condition), #condition)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid NumericSpec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock test_clock() {
  auto next = std::make_shared<std::atomic<std::uint64_t>>(9'000'000'000'000U);
  return [next] {
    const auto monotonic = next->fetch_add(1U);
    return g3::ClockSample{1'700'000'000'000'000'000U + monotonic, monotonic};
  };
}

[[nodiscard]] market::DepthUpdate make_update(common::Market product,
                                              std::uint64_t generation,
                                              std::uint64_t final_id) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(product);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g11-routing-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(
      (product == common::MARKET_SPOT ? "spot-test-g" : "usdm-test-g") +
      std::to_string(generation));
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(g9::kDepthEventSchema);
  metadata->set_exchange_event_time_ms(1'700'000'000'002U + final_id);
  metadata->set_receive_time_utc_ns(1'700'000'000'002'000'000U + final_id);
  metadata->set_receive_monotonic_ns(9'000'000'000'002U + final_id);
  const auto bootstrap = final_id == 101U;
  update.set_first_update_id(
      product == common::MARKET_USD_M_PERPETUAL && bootstrap ? 100U : final_id);
  update.set_final_update_id(final_id);
  if (product == common::MARKET_USD_M_PERPETUAL) {
    update.set_previous_final_update_id(final_id - 1U);
  }
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("4.000");
  return update;
}

[[nodiscard]] market::ExchangeDepthSnapshot
make_snapshot(common::Market product, std::uint64_t generation) {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(product);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g11-routing-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id(
      (product == common::MARKET_SPOT ? "spot-snapshot-g" : "usdm-snapshot-g") +
      std::to_string(generation));
  snapshot.set_last_update_id(100U);
  snapshot.set_exchange_transaction_time_ms(1'700'000'000'001U);
  snapshot.set_receive_time_utc_ns(1'700'000'000'001'000'000U);
  snapshot.set_receive_monotonic_ns(9'000'000'000'001U);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("2.500");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("3.000");
  return snapshot;
}

class SyntheticLiveAttempt final : public g5::detail::RecoveryAttempt {
public:
  SyntheticLiveAttempt(g3::MarketRuntime &runtime, common::Market product,
                       std::uint64_t generation)
      : runtime_{runtime}, product_{product}, generation_{generation} {
    observation_.connection_generation = generation_;
    observation_.connection_id =
        (product_ == common::MARKET_SPOT ? "spot-attempt-g"
                                         : "usdm-attempt-g") +
        std::to_string(generation_);
  }

  [[nodiscard]] g4::TransportStartResult start() override {
    if (runtime_.submit_depth_update(make_update(product_, generation_, 101U),
                                     g3::SourceProvenance{generation_}) !=
            g3::AdmissionResult::Accepted ||
        runtime_.submit_snapshot(make_snapshot(product_, generation_),
                                 g3::SourceProvenance{generation_}) !=
            g3::AdmissionResult::Accepted) {
      return g4::TransportStartResult::Failed;
    }
    std::lock_guard lock{mutex_};
    observation_.started = true;
    observation_.running = true;
    observation_.tls_verified = true;
    observation_.websocket_handshake = true;
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
    observation_.last_event_utc_ns = 1'700'000'000'002'000'000U + generation_;
    return g4::TransportStartResult::Started;
  }

  void stop() noexcept override {
    std::lock_guard lock{mutex_};
    observation_.running = false;
    observation_.stopped = true;
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  g3::MarketRuntime &runtime_;
  const common::Market product_;
  const std::uint64_t generation_;
  mutable std::mutex mutex_;
  g4::TransportObservation observation_;
};

class AdmissionGate final {
public:
  void block() noexcept {
    std::lock_guard lock{mutex_};
    blocked_ = true;
  }

  void release() noexcept {
    {
      std::lock_guard lock{mutex_};
      blocked_ = false;
    }
    condition_.notify_all();
  }

  void wait() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return !blocked_; });
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool blocked_{false};
};

class ClockGate final {
public:
  [[nodiscard]] g3::ClockSample sample() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return !blocked_; });
    const auto monotonic = next_++;
    return {1'700'000'000'000'000'000U + monotonic, monotonic};
  }

  void block() noexcept {
    std::lock_guard lock{mutex_};
    blocked_ = true;
  }

  void release() noexcept {
    {
      std::lock_guard lock{mutex_};
      blocked_ = false;
    }
    condition_.notify_all();
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::uint64_t next_{9'000'000'000'000U};
  bool blocked_{false};
};

class BackoffGate final {
public:
  [[nodiscard]] bool wait(std::stop_token stop_token) {
    std::unique_lock lock{mutex_};
    const auto released =
        condition_.wait(lock, stop_token, [this] { return released_; });
    return released && !stop_token.stop_requested();
  }

private:
  std::mutex mutex_;
  std::condition_variable_any condition_;
  bool released_{false};
};

[[nodiscard]] g5::detail::RecoveryTestOptions
recovery_options(common::Market product) {
  g5::detail::RecoveryTestOptions options;
  options.attempt_factory = [product](g3::MarketRuntime &runtime,
                                      const g3::RuntimeClock &,
                                      std::uint64_t generation) {
    return std::make_unique<SyntheticLiveAttempt>(runtime, product, generation);
  };
  options.backoff_waiter = [](std::chrono::seconds,
                              std::stop_token stop_token) {
    return !stop_token.stop_requested();
  };
  return options;
}

[[nodiscard]] g11::TwoProductRuntimeOptions live_options() {
  g11::TwoProductRuntimeOptions options;
  options.spot.recovery_test = recovery_options(common::MARKET_SPOT);
  options.usdm.recovery_test = recovery_options(common::MARKET_USD_M_PERPETUAL);
  return options;
}

[[nodiscard]] g11::TwoProductRuntimeOptions capacity_options(
    const std::shared_ptr<AdmissionGate> &spot_gate,
    const std::shared_ptr<AdmissionGate> &usdm_gate,
    const std::shared_ptr<std::atomic<std::size_t>> &spot_enqueued,
    const std::shared_ptr<std::atomic<std::size_t>> &usdm_enqueued) {
  auto options = live_options();
  options.spot.runtime_test =
      g3::RuntimeTestOptions{false, [spot_enqueued] { ++*spot_enqueued; },
                             [spot_gate] { spot_gate->wait(); }};
  options.usdm.runtime_test =
      g3::RuntimeTestOptions{false, [usdm_enqueued] { ++*usdm_enqueued; },
                             [usdm_gate] { usdm_gate->wait(); }};
  return options;
}

void start_live(g11::TwoProductRuntime &products) {
  const auto started = products.start();
  REQUIRE_EQ(started.spot, g5::RecoveryStartResult::Started);
  REQUIRE_EQ(started.usdm, g5::RecoveryStartResult::Started);
  REQUIRE_EQ(products.spot().recovery().wait_for_generation_live(1U).state,
             g5::RecoveryState::Live);
  REQUIRE_EQ(products.usdm().recovery().wait_for_generation_live(1U).state,
             g5::RecoveryState::Live);
}

[[nodiscard]] wire::OrderBookSubscriptionRequest
order_book_request(common::Market product, std::string request_id) {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(product);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] const char *schema_for(common::Stream stream) {
  switch (stream) {
  case common::STREAM_DIFF_DEPTH:
    return g9::kDepthEventSchema;
  case common::STREAM_AGG_TRADE:
    return g9::kAggTradeEventSchema;
  case common::STREAM_BOOK_TICKER:
    return g9::kBookTickerEventSchema;
  case common::STREAM_DEPTH_SNAPSHOT:
  case common::STREAM_UNSPECIFIED:
    break;
  default:
    break;
  }
  throw TestFailure{"unsupported test event stream"};
}

[[nodiscard]] wire::EventSubscriptionRequest
event_request(common::Market product, common::Stream stream,
              std::string request_id) {
  wire::EventSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g9::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(product);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(stream);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(schema_for(stream));
  return request;
}

[[nodiscard]] std::shared_ptr<const g4::NormalizedMarketEvent>
normalized_event(common::Market product, common::Stream stream,
                 std::uint64_t id) {
  if (stream == common::STREAM_DIFF_DEPTH) {
    return std::make_shared<const g4::NormalizedMarketEvent>(
        make_update(product, 1U, id));
  }
  market::AggTrade trade;
  auto *metadata = trade.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(product);
  metadata->set_symbol("BTCUSDT");
  metadata->set_connection_id("spot-event-source");
  metadata->set_stream(stream);
  metadata->set_schema_version(g9::kAggTradeEventSchema);
  metadata->set_receive_time_utc_ns(100U + id);
  metadata->set_receive_monotonic_ns(200U + id);
  trade.set_aggregate_trade_id(id);
  trade.set_price("100.00");
  trade.set_quantity("1.000");
  trade.set_first_trade_id(id);
  trade.set_last_trade_id(id);
  trade.set_trade_time_ms(id);
  return std::make_shared<const g4::NormalizedMarketEvent>(std::move(trade));
}

[[nodiscard]] std::unique_ptr<wire::BinanceMarketDataGatewayService::Stub>
make_stub(int port) {
  return wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
}

void set_deadline(grpc::ClientContext &context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds{10});
}

template <typename Predicate>
void wait_until(Predicate predicate, std::string_view expression) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    if (predicate()) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  throw TestFailure{std::string{expression}};
}

[[nodiscard]] g7::ValidatedOrderBookSubscription
direct_order_book_request(std::string request_id) {
  return {std::move(request_id), "gw-g11-capacity", std::nullopt};
}

[[nodiscard]] g9::ValidatedEventSubscription
direct_event_request(std::string request_id) {
  return {std::move(request_id), common::STREAM_DIFF_DEPTH,
          g9::kDepthEventSchema};
}

void require_order_book_head(
    grpc::ClientReader<wire::OrderBookStreamItem> &reader,
    common::Market product, std::string_view expected_id) {
  wire::OrderBookStreamItem item;
  REQUIRE(reader.Read(&item));
  REQUIRE(item.has_subscription_accepted());
  REQUIRE_EQ(item.delivery_metadata().subscription_id(), expected_id);
  REQUIRE(reader.Read(&item));
  REQUIRE(item.has_snapshot());
  REQUIRE_EQ(item.snapshot().market(), product);
  REQUIRE_EQ(item.snapshot().symbol(), "BTCUSDT");
  REQUIRE_EQ(item.delivery_metadata().subscription_id(), expected_id);
}

void require_event_head(grpc::ClientReader<wire::GatewayEventEnvelope> &reader,
                        std::string_view expected_id) {
  wire::GatewayEventEnvelope item;
  REQUIRE(reader.Read(&item));
  REQUIRE(item.has_subscription_accepted());
  REQUIRE_EQ(item.delivery_metadata().subscription_id(), expected_id);
}

void selector_matrix_and_registry() {
  const auto clock = test_clock();
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), clock,
                                  "gw-g11-routing", live_options()};
  const auto &entries = products.registry().entries();
  REQUIRE_EQ(entries.size(), 2U);
  REQUIRE_EQ(entries[0].key, g11::spot_btcusdt_key());
  REQUIRE_EQ(entries[1].key, g11::usdm_btcusdt_key());
  REQUIRE(products.registry().find(g11::spot_btcusdt_key()) == &entries[0]);
  REQUIRE(products.registry().find(g11::usdm_btcusdt_key()) == &entries[1]);
  REQUIRE(products.registry().find({common::VENUE_BINANCE, common::MARKET_SPOT,
                                    "btcusdt"}) == nullptr);

  for (const auto product :
       {common::MARKET_SPOT, common::MARKET_USD_M_PERPETUAL}) {
    REQUIRE(std::holds_alternative<g7::ValidatedOrderBookSubscription>(
        g7::validate_g11_order_book_request(
            order_book_request(product, "book-validation"), "gw-g11-routing")));
  }
  for (const auto stream : {common::STREAM_DIFF_DEPTH, common::STREAM_AGG_TRADE,
                            common::STREAM_BOOK_TICKER}) {
    REQUIRE(std::holds_alternative<g9::ValidatedEventSubscription>(
        g7::validate_g11_event_request(
            event_request(common::MARKET_SPOT, stream, "event-validation"),
            "gw-g11-routing")));
  }
  REQUIRE(std::holds_alternative<g9::ValidatedEventSubscription>(
      g7::validate_g11_event_request(
          event_request(common::MARKET_USD_M_PERPETUAL,
                        common::STREAM_DIFF_DEPTH, "usdm-depth-validation"),
          "gw-g11-routing")));
  for (const auto stream :
       {common::STREAM_AGG_TRADE, common::STREAM_BOOK_TICKER}) {
    REQUIRE_EQ(
        std::get<g7::RequestValidationError>(g7::validate_g11_event_request(
            event_request(common::MARKET_USD_M_PERPETUAL, stream,
                          "usdm-reject-validation"),
            "gw-g11-routing")),
        g7::RequestValidationError::InvalidArgument);
  }
}

void loopback_routing_ids_status_and_generation_cuts() {
  const auto clock = test_clock();
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), clock,
                                  "gw-g11-routing", live_options()};
  start_live(products);
  g7::OrderBookGrpcServer server{products.registry(), clock, "gw-g11-routing"};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  const auto reject_usdm_event = [&](common::Stream stream) {
    grpc::ClientContext context;
    set_deadline(context);
    auto reader = stub->SubscribeEvents(
        &context,
        event_request(common::MARKET_USD_M_PERPETUAL, stream, "reject-usdm"));
    wire::GatewayEventEnvelope item;
    REQUIRE(!reader->Read(&item));
    REQUIRE_EQ(reader->Finish().error_code(),
               grpc::StatusCode::INVALID_ARGUMENT);
  };
  reject_usdm_event(common::STREAM_AGG_TRADE);
  reject_usdm_event(common::STREAM_BOOK_TICKER);

  grpc::ClientContext spot_book_context;
  grpc::ClientContext usdm_book_context;
  grpc::ClientContext spot_event_context;
  grpc::ClientContext usdm_event_context;
  set_deadline(spot_book_context);
  set_deadline(usdm_book_context);
  set_deadline(spot_event_context);
  set_deadline(usdm_event_context);
  auto spot_book = stub->SubscribeOrderBook(
      &spot_book_context,
      order_book_request(common::MARKET_SPOT, "spot-book-1"));
  auto usdm_book = stub->SubscribeOrderBook(
      &usdm_book_context,
      order_book_request(common::MARKET_USD_M_PERPETUAL, "usdm-book-1"));
  auto spot_event = stub->SubscribeEvents(
      &spot_event_context,
      event_request(common::MARKET_SPOT, common::STREAM_AGG_TRADE,
                    "spot-event-1"));
  auto usdm_event = stub->SubscribeEvents(
      &usdm_event_context,
      event_request(common::MARKET_USD_M_PERPETUAL, common::STREAM_DIFF_DEPTH,
                    "usdm-event-1"));

  require_order_book_head(*spot_book, common::MARKET_SPOT, "ob-1");
  require_order_book_head(*usdm_book, common::MARKET_USD_M_PERPETUAL, "ob-1");
  require_event_head(*spot_event, "ev-1");
  require_event_head(*usdm_event, "ev-1");

  wire::GatewayStatusRequest status_request;
  status_request.set_request_id("status-routing");
  status_request.set_schema_version(g10::kStatusRequestSchema);
  wire::GatewayStatusSnapshot status_snapshot;
  grpc::ClientContext status_context;
  const auto status =
      stub->GetGatewayStatus(&status_context, status_request, &status_snapshot);
  REQUIRE(status.ok());
  REQUIRE_EQ(status_snapshot.markets_size(), 2);
  REQUIRE_EQ(status_snapshot.markets(0).market(), common::MARKET_SPOT);
  REQUIRE_EQ(status_snapshot.markets(1).market(),
             common::MARKET_USD_M_PERPETUAL);
  REQUIRE_EQ(status_snapshot.markets(0).active_subscription_count(), 2U);
  REQUIRE_EQ(status_snapshot.markets(1).active_subscription_count(), 2U);
  REQUIRE_EQ(status_snapshot.total_active_subscriptions(), 4U);
  REQUIRE_EQ(status_snapshot.markets(0).connection_generation(), 1U);
  REQUIRE_EQ(status_snapshot.markets(1).connection_generation(), 1U);

  REQUIRE_EQ(
      products.spot().event_publication().publish(
          normalized_event(common::MARKET_SPOT, common::STREAM_AGG_TRADE, 10U),
          1U),
      g9::EventPublishResult::Published);
  REQUIRE_EQ(products.usdm().event_publication().publish(
                 normalized_event(common::MARKET_USD_M_PERPETUAL,
                                  common::STREAM_DIFF_DEPTH, 102U),
                 1U),
             g9::EventPublishResult::Published);
  REQUIRE_EQ(
      products.spot().runtime().submit_depth_update(
          make_update(common::MARKET_SPOT, 1U, 102U), g3::SourceProvenance{1U}),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(products.usdm().runtime().submit_depth_update(
                 make_update(common::MARKET_USD_M_PERPETUAL, 1U, 102U),
                 g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(products.spot().runtime().observe());
  static_cast<void>(products.usdm().runtime().observe());

  wire::OrderBookStreamItem book_item;
  REQUIRE(spot_book->Read(&book_item));
  REQUIRE(book_item.has_depth_update());
  REQUIRE_EQ(book_item.depth_update().metadata().market(), common::MARKET_SPOT);
  REQUIRE(usdm_book->Read(&book_item));
  REQUIRE(book_item.has_depth_update());
  REQUIRE_EQ(book_item.depth_update().metadata().market(),
             common::MARKET_USD_M_PERPETUAL);
  wire::GatewayEventEnvelope event_item;
  REQUIRE(spot_event->Read(&event_item));
  REQUIRE(event_item.has_agg_trade());
  REQUIRE_EQ(event_item.agg_trade().metadata().market(), common::MARKET_SPOT);
  REQUIRE(usdm_event->Read(&event_item));
  REQUIRE(event_item.has_depth_update());
  REQUIRE_EQ(event_item.depth_update().metadata().market(),
             common::MARKET_USD_M_PERPETUAL);

  REQUIRE(
      products.spot().recovery().request_controlled_recovery_for_acceptance());
  REQUIRE_EQ(products.spot()
                 .recovery()
                 .wait_for_generation_live(2U)
                 .connection_generation,
             2U);
  REQUIRE(spot_book->Read(&book_item));
  REQUIRE(book_item.has_consumer_gap());
  REQUIRE_EQ(book_item.consumer_gap().market(), common::MARKET_SPOT);
  REQUIRE(!spot_book->Read(&book_item));
  REQUIRE(spot_book->Finish().ok());
  REQUIRE(spot_event->Read(&event_item));
  REQUIRE(event_item.has_consumer_gap());
  REQUIRE_EQ(event_item.consumer_gap().market(), common::MARKET_SPOT);
  REQUIRE(!spot_event->Read(&event_item));
  REQUIRE(spot_event->Finish().ok());
  REQUIRE_EQ(products.usdm().recovery().observe().connection_generation, 1U);
  REQUIRE_EQ(products.usdm().runtime().observe().resident_subscription_count,
             1U);
  REQUIRE_EQ(products.usdm().event_publication().observe().active_subscriptions,
             1U);

  grpc::ClientContext fresh_spot_book_context;
  grpc::ClientContext fresh_spot_event_context;
  set_deadline(fresh_spot_book_context);
  set_deadline(fresh_spot_event_context);
  auto fresh_spot_book = stub->SubscribeOrderBook(
      &fresh_spot_book_context,
      order_book_request(common::MARKET_SPOT, "spot-book-2"));
  auto fresh_spot_event = stub->SubscribeEvents(
      &fresh_spot_event_context,
      event_request(common::MARKET_SPOT, common::STREAM_AGG_TRADE,
                    "spot-event-2"));
  require_order_book_head(*fresh_spot_book, common::MARKET_SPOT, "ob-2");
  require_event_head(*fresh_spot_event, "ev-2");

  REQUIRE_EQ(products.usdm().event_publication().publish(
                 normalized_event(common::MARKET_USD_M_PERPETUAL,
                                  common::STREAM_DIFF_DEPTH, 103U),
                 1U),
             g9::EventPublishResult::Published);
  REQUIRE_EQ(products.usdm().runtime().submit_depth_update(
                 make_update(common::MARKET_USD_M_PERPETUAL, 1U, 103U),
                 g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(products.usdm().runtime().observe());
  REQUIRE(usdm_book->Read(&book_item));
  REQUIRE(book_item.has_depth_update());
  REQUIRE_EQ(book_item.depth_update().final_update_id(), 103U);
  REQUIRE(usdm_event->Read(&event_item));
  REQUIRE(event_item.has_depth_update());
  REQUIRE_EQ(event_item.depth_update().final_update_id(), 103U);

  REQUIRE(
      products.usdm().recovery().request_controlled_recovery_for_acceptance());
  REQUIRE_EQ(products.usdm()
                 .recovery()
                 .wait_for_generation_live(2U)
                 .connection_generation,
             2U);
  REQUIRE(usdm_book->Read(&book_item));
  REQUIRE(book_item.has_consumer_gap());
  REQUIRE_EQ(book_item.consumer_gap().market(), common::MARKET_USD_M_PERPETUAL);
  REQUIRE(!usdm_book->Read(&book_item));
  REQUIRE(usdm_book->Finish().ok());
  REQUIRE(usdm_event->Read(&event_item));
  REQUIRE(event_item.has_consumer_gap());
  REQUIRE_EQ(event_item.consumer_gap().market(),
             common::MARKET_USD_M_PERPETUAL);
  REQUIRE(!usdm_event->Read(&event_item));
  REQUIRE(usdm_event->Finish().ok());
  REQUIRE_EQ(products.spot().recovery().observe().connection_generation, 2U);

  REQUIRE_EQ(
      products.spot().event_publication().publish(
          normalized_event(common::MARKET_SPOT, common::STREAM_AGG_TRADE, 20U),
          2U),
      g9::EventPublishResult::Published);
  REQUIRE_EQ(
      products.spot().runtime().submit_depth_update(
          make_update(common::MARKET_SPOT, 2U, 102U), g3::SourceProvenance{2U}),
      g3::AdmissionResult::Accepted);
  static_cast<void>(products.spot().runtime().observe());
  REQUIRE(fresh_spot_book->Read(&book_item));
  REQUIRE(book_item.has_depth_update());
  REQUIRE_EQ(book_item.depth_update().final_update_id(), 102U);
  REQUIRE(fresh_spot_event->Read(&event_item));
  REQUIRE(event_item.has_agg_trade());

  fresh_spot_book_context.TryCancel();
  while (fresh_spot_book->Read(&book_item)) {
  }
  const auto fresh_book_status = fresh_spot_book->Finish();
  REQUIRE(fresh_book_status.ok() ||
          fresh_book_status.error_code() == grpc::StatusCode::CANCELLED);
  fresh_spot_event_context.TryCancel();
  while (fresh_spot_event->Read(&event_item)) {
  }
  const auto fresh_event_status = fresh_spot_event->Finish();
  REQUIRE(fresh_event_status.ok() ||
          fresh_event_status.error_code() == grpc::StatusCode::CANCELLED);

  server.shutdown();
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  products.stop();
}

void per_market_capacities_and_pending_are_independent() {
  const auto clock = test_clock();
  auto spot_gate = std::make_shared<AdmissionGate>();
  auto usdm_gate = std::make_shared<AdmissionGate>();
  auto spot_enqueued = std::make_shared<std::atomic<std::size_t>>(0U);
  auto usdm_enqueued = std::make_shared<std::atomic<std::size_t>>(0U);
  g11::TwoProductRuntime products{
      numeric_spec(), numeric_spec(), clock, "gw-g11-capacity",
      capacity_options(spot_gate, usdm_gate, spot_enqueued, usdm_enqueued)};
  start_live(products);

  std::vector<std::shared_ptr<g7::SubscriberChannel>> order_book_channels;
  std::vector<std::shared_ptr<g9::EventSubscriberChannel>> event_channels;
  order_book_channels.reserve(16U);
  event_channels.reserve(16U);
  const auto fill_order_books = [&](g11::ProductRuntime &product,
                                    std::string_view prefix) {
    for (std::size_t index = 0U; index < 8U; ++index) {
      auto admission = product.runtime().admit_order_book_subscription(
          direct_order_book_request(std::string{prefix} +
                                    std::to_string(index)));
      REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(admission));
      auto channel =
          std::get<g3::AcceptedSubscription>(std::move(admission)).channel;
      REQUIRE(channel != nullptr);
      REQUIRE_EQ(channel->subscription_id(),
                 "ob-" + std::to_string(index + 1U));
      order_book_channels.push_back(std::move(channel));
    }
    const auto ninth = product.runtime().admit_order_book_subscription(
        direct_order_book_request(std::string{prefix} + "ninth"));
    REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(ninth),
               g3::SubscriptionAdmissionError::ActiveLimit);
  };
  fill_order_books(products.spot(), "spot-book-");
  fill_order_books(products.usdm(), "usdm-book-");

  const auto fill_events = [&](g11::ProductRuntime &product,
                               std::string_view prefix) {
    for (std::size_t index = 0U; index < 8U; ++index) {
      auto admission = product.event_publication().admit(
          direct_event_request(std::string{prefix} + std::to_string(index)));
      REQUIRE(std::holds_alternative<g9::AcceptedEventSubscription>(admission));
      auto channel =
          std::get<g9::AcceptedEventSubscription>(std::move(admission)).channel;
      REQUIRE(channel != nullptr);
      REQUIRE_EQ(channel->subscription_id(),
                 "ev-" + std::to_string(index + 1U));
      event_channels.push_back(std::move(channel));
    }
    const auto ninth = product.event_publication().admit(
        direct_event_request(std::string{prefix} + "ninth"));
    REQUIRE_EQ(std::get<g9::EventSubscriptionAdmissionError>(ninth),
               g9::EventSubscriptionAdmissionError::ActiveLimit);
  };
  fill_events(products.spot(), "spot-event-");
  fill_events(products.usdm(), "usdm-event-");

  spot_gate->block();
  usdm_gate->block();
  std::vector<std::optional<g3::SubscriptionAdmissionResult>> pending_results(
      16U);
  std::vector<std::thread> pending_threads;
  pending_threads.reserve(16U);
  for (std::size_t index = 0U; index < 8U; ++index) {
    pending_threads.emplace_back([&, index] {
      pending_results[index] =
          products.spot().runtime().admit_order_book_subscription(
              direct_order_book_request("spot-pending-" +
                                        std::to_string(index)));
    });
    pending_threads.emplace_back([&, index] {
      pending_results[8U + index] =
          products.usdm().runtime().admit_order_book_subscription(
              direct_order_book_request("usdm-pending-" +
                                        std::to_string(index)));
    });
  }
  wait_until(
      [&] {
        return spot_enqueued->load() == 17U && usdm_enqueued->load() == 17U;
      },
      "both runtimes reached eight pending admissions");

  const auto spot_ninth_pending =
      products.spot().runtime().admit_order_book_subscription(
          direct_order_book_request("spot-pending-ninth"));
  const auto usdm_ninth_pending =
      products.usdm().runtime().admit_order_book_subscription(
          direct_order_book_request("usdm-pending-ninth"));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(spot_ninth_pending),
             g3::SubscriptionAdmissionError::PendingLimit);
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(usdm_ninth_pending),
             g3::SubscriptionAdmissionError::PendingLimit);

  spot_gate->release();
  usdm_gate->release();
  for (auto &thread : pending_threads) {
    thread.join();
  }
  for (const auto &result : pending_results) {
    REQUIRE(result.has_value());
    REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(*result),
               g3::SubscriptionAdmissionError::ActiveLimit);
  }
  REQUIRE_EQ(products.spot().runtime().observe().resident_subscription_count,
             8U);
  REQUIRE_EQ(products.usdm().runtime().observe().resident_subscription_count,
             8U);
  REQUIRE_EQ(products.spot().event_publication().observe().active_subscriptions,
             8U);
  REQUIRE_EQ(products.usdm().event_publication().observe().active_subscriptions,
             8U);
  products.stop();
}

void context_tracker_reaches_and_never_exceeds_48() {
  static_assert(g7::kMaximumGrpcTrackedContexts == 48U);
  const auto clock = test_clock();
  auto spot_gate = std::make_shared<AdmissionGate>();
  auto usdm_gate = std::make_shared<AdmissionGate>();
  auto spot_enqueued = std::make_shared<std::atomic<std::size_t>>(0U);
  auto usdm_enqueued = std::make_shared<std::atomic<std::size_t>>(0U);
  g11::TwoProductRuntime products{
      numeric_spec(), numeric_spec(), clock, "gw-g11-contexts",
      capacity_options(spot_gate, usdm_gate, spot_enqueued, usdm_enqueued)};
  start_live(products);

  std::atomic<std::size_t> shutdown_snapshot_count{0U};
  std::atomic<bool> shutdown_snapshot_seen{false};
  g7::OrderBookGrpcServer *server_view = nullptr;
  g7::GrpcServiceOptions service_options;
  service_options.idle_cancellation_check_interval =
      std::chrono::milliseconds{1};
  service_options.cancellation_snapshot_ready = [&] {
    shutdown_snapshot_count = server_view->service().tracked_context_count();
    shutdown_snapshot_seen = true;
  };
  g7::OrderBookGrpcServer server{products.registry(), clock, "gw-g11-contexts",
                                 std::move(service_options)};
  server_view = &server;
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  std::vector<std::unique_ptr<grpc::ClientContext>> book_contexts;
  std::vector<std::unique_ptr<grpc::ClientReader<wire::OrderBookStreamItem>>>
      book_readers;
  std::vector<std::unique_ptr<grpc::ClientContext>> event_contexts;
  std::vector<std::unique_ptr<grpc::ClientReader<wire::GatewayEventEnvelope>>>
      event_readers;
  book_contexts.reserve(32U);
  book_readers.reserve(32U);
  event_contexts.reserve(16U);
  event_readers.reserve(16U);

  for (const auto product :
       {common::MARKET_SPOT, common::MARKET_USD_M_PERPETUAL}) {
    for (std::size_t index = 0U; index < 8U; ++index) {
      book_contexts.push_back(std::make_unique<grpc::ClientContext>());
      set_deadline(*book_contexts.back());
      book_readers.push_back(stub->SubscribeOrderBook(
          book_contexts.back().get(),
          order_book_request(product, "active-book-" + std::to_string(product) +
                                          "-" + std::to_string(index))));
      require_order_book_head(*book_readers.back(), product,
                              "ob-" + std::to_string(index + 1U));

      event_contexts.push_back(std::make_unique<grpc::ClientContext>());
      set_deadline(*event_contexts.back());
      event_readers.push_back(stub->SubscribeEvents(
          event_contexts.back().get(),
          event_request(product, common::STREAM_DIFF_DEPTH,
                        "active-event-" + std::to_string(product) + "-" +
                            std::to_string(index))));
      require_event_head(*event_readers.back(),
                         "ev-" + std::to_string(index + 1U));
    }
  }
  REQUIRE_EQ(server.service().tracked_context_count(), 32U);

  spot_gate->block();
  usdm_gate->block();
  for (const auto product :
       {common::MARKET_SPOT, common::MARKET_USD_M_PERPETUAL}) {
    for (std::size_t index = 0U; index < 8U; ++index) {
      book_contexts.push_back(std::make_unique<grpc::ClientContext>());
      set_deadline(*book_contexts.back());
      book_readers.push_back(stub->SubscribeOrderBook(
          book_contexts.back().get(),
          order_book_request(product, "pending-book-" +
                                          std::to_string(product) + "-" +
                                          std::to_string(index))));
    }
  }
  wait_until(
      [&] {
        return spot_enqueued->load() == 16U && usdm_enqueued->load() == 16U;
      },
      "both products reached their pending admission capacity");
  wait_until([&] { return server.service().tracked_context_count() == 48U; },
             "gRPC tracker reached its frozen G11 bound");

  grpc::ClientContext overflow_context;
  set_deadline(overflow_context);
  auto overflow_reader = stub->SubscribeEvents(
      &overflow_context,
      event_request(common::MARKET_SPOT, common::STREAM_DIFF_DEPTH,
                    "context-overflow"));
  wire::GatewayEventEnvelope overflow_item;
  REQUIRE(!overflow_reader->Read(&overflow_item));
  REQUIRE_EQ(overflow_reader->Finish().error_code(),
             grpc::StatusCode::RESOURCE_EXHAUSTED);
  REQUIRE_EQ(server.service().tracked_context_count(), 48U);

  std::thread shutdown{[&] { server.shutdown(); }};
  wait_until([&] { return !server.service().admission_open(); },
             "global service admission closed");
  spot_gate->release();
  usdm_gate->release();
  shutdown.join();
  REQUIRE(shutdown_snapshot_seen.load());
  REQUIRE(shutdown_snapshot_count.load() <= 48U);
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);

  wire::OrderBookStreamItem book_item;
  for (auto &reader : book_readers) {
    while (reader->Read(&book_item)) {
    }
    static_cast<void>(reader->Finish());
  }
  wire::GatewayEventEnvelope event_item;
  for (auto &reader : event_readers) {
    while (reader->Read(&event_item)) {
    }
    static_cast<void>(reader->Finish());
  }
  products.stop();
}

void adverse_global_shutdown_is_bounded_and_joined() {
  auto clock_gate = std::make_shared<ClockGate>();
  const g3::RuntimeClock clock = [clock_gate] { return clock_gate->sample(); };
  auto backoff_gate = std::make_shared<BackoffGate>();
  auto options = live_options();
  options.spot.recovery_test.backoff_waiter =
      [backoff_gate](std::chrono::seconds, std::stop_token stop_token) {
        return backoff_gate->wait(stop_token);
      };
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), clock,
                                  "gw-g11-adverse", std::move(options)};
  start_live(products);

  std::atomic<std::size_t> shutdown_snapshot_count{0U};
  std::atomic<bool> shutdown_snapshot_seen{false};
  g7::OrderBookGrpcServer *server_view = nullptr;
  g7::GrpcServiceOptions service_options;
  service_options.idle_cancellation_check_interval =
      std::chrono::milliseconds{1};
  service_options.cancellation_snapshot_ready = [&] {
    shutdown_snapshot_count = server_view->service().tracked_context_count();
    shutdown_snapshot_seen = true;
  };
  g7::OrderBookGrpcServer server{products.registry(), clock, "gw-g11-adverse",
                                 std::move(service_options)};
  server_view = &server;
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  grpc::ClientContext spot_book_context;
  grpc::ClientContext usdm_book_context;
  grpc::ClientContext spot_event_context;
  grpc::ClientContext usdm_event_context;
  set_deadline(spot_book_context);
  set_deadline(usdm_book_context);
  set_deadline(spot_event_context);
  set_deadline(usdm_event_context);
  auto spot_book = stub->SubscribeOrderBook(
      &spot_book_context,
      order_book_request(common::MARKET_SPOT, "adverse-spot-book"));
  auto usdm_book = stub->SubscribeOrderBook(
      &usdm_book_context,
      order_book_request(common::MARKET_USD_M_PERPETUAL, "adverse-usdm-book"));
  auto spot_event = stub->SubscribeEvents(
      &spot_event_context,
      event_request(common::MARKET_SPOT, common::STREAM_AGG_TRADE,
                    "adverse-spot-event"));
  auto usdm_event = stub->SubscribeEvents(
      &usdm_event_context,
      event_request(common::MARKET_USD_M_PERPETUAL, common::STREAM_DIFF_DEPTH,
                    "adverse-usdm-event"));
  require_order_book_head(*spot_book, common::MARKET_SPOT, "ob-1");
  require_order_book_head(*usdm_book, common::MARKET_USD_M_PERPETUAL, "ob-1");
  require_event_head(*spot_event, "ev-1");
  require_event_head(*usdm_event, "ev-1");
  REQUIRE_EQ(server.service().tracked_context_count(), 4U);

  const auto usdm_before = products.usdm().runtime().observe();
  REQUIRE(
      products.spot().recovery().request_controlled_recovery_for_acceptance());
  wait_until(
      [&] {
        return products.spot().recovery().observe().state ==
               g5::RecoveryState::Backoff;
      },
      "Spot entered deterministic recovery backoff");
  REQUIRE_EQ(products.spot().recovery().observe().active_transport_count, 0U);
  REQUIRE_EQ(products.usdm().recovery().observe().state,
             g5::RecoveryState::Live);
  REQUIRE_EQ(products.usdm().recovery().observe().connection_generation, 1U);

  clock_gate->block();
  wire::GatewayStatusRequest status_request;
  status_request.set_request_id("adverse-status-1");
  status_request.set_schema_version(g10::kStatusRequestSchema);
  wire::GatewayStatusSnapshot first_response;
  grpc::ClientContext first_status_context;
  grpc::Status first_status;
  std::thread first_status_thread{[&] {
    first_status = stub->GetGatewayStatus(&first_status_context, status_request,
                                          &first_response);
  }};
  wait_until([&] { return server.service().status_inflight(); },
             "first G11 status RPC acquired the global slot");

  wire::GatewayStatusRequest second_request = status_request;
  second_request.set_request_id("adverse-status-2");
  wire::GatewayStatusSnapshot second_response;
  grpc::ClientContext second_status_context;
  const auto second_status = stub->GetGatewayStatus(
      &second_status_context, second_request, &second_response);
  REQUIRE_EQ(second_status.error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);

  std::thread shutdown{[&] { server.shutdown(); }};
  wait_until([&] { return !server.service().admission_open(); },
             "adverse global shutdown closed service admission");
  clock_gate->release();
  first_status_thread.join();
  shutdown.join();
  REQUIRE(first_status.ok() ||
          first_status.error_code() == grpc::StatusCode::CANCELLED ||
          first_status.error_code() == grpc::StatusCode::UNAVAILABLE);
  REQUIRE(!server.service().status_inflight());
  REQUIRE(shutdown_snapshot_seen.load());
  REQUIRE(shutdown_snapshot_count.load() <= 48U);
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  REQUIRE_EQ(products.usdm().runtime().observe().reset_count,
             usdm_before.reset_count);
  REQUIRE_EQ(products.usdm().recovery().observe().connection_generation, 1U);

  wire::OrderBookStreamItem book_item;
  for (auto *reader : {spot_book.get(), usdm_book.get()}) {
    while (reader->Read(&book_item)) {
    }
    static_cast<void>(reader->Finish());
  }
  wire::GatewayEventEnvelope event_item;
  for (auto *reader : {spot_event.get(), usdm_event.get()}) {
    while (reader->Read(&event_item)) {
    }
    static_cast<void>(reader->Finish());
  }

  products.stop();
  const auto spot_runtime = products.spot().runtime().observe();
  const auto usdm_runtime = products.usdm().runtime().observe();
  REQUIRE(spot_runtime.owner_joined);
  REQUIRE(usdm_runtime.owner_joined);
  REQUIRE_EQ(products.spot().recovery().observe().state,
             g5::RecoveryState::Stopped);
  REQUIRE_EQ(products.usdm().recovery().observe().state,
             g5::RecoveryState::Stopped);
  REQUIRE_EQ(products.spot().recovery().observe().active_transport_count, 0U);
  REQUIRE_EQ(products.usdm().recovery().observe().active_transport_count, 0U);
}

void one_faulted_market_still_returns_two_status_rows() {
  const auto clock = test_clock();
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), clock,
                                  "gw-g11-fault-status", live_options()};
  start_live(products);
  REQUIRE(products.usdm().recovery().quiesce_for_acceptance().has_value());
  REQUIRE_EQ(products.usdm().runtime().submit_transport_failure(),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(products.usdm().runtime().observe().state,
             g3::RuntimeState::Faulted);

  g10::GatewayStatusAssembler assembler{products.registry(), clock,
                                        "gw-g11-fault-status"};
  REQUIRE(assembler.prepare_start_baseline());
  const auto result = assembler.collect();
  const auto *snapshot = std::get_if<wire::GatewayStatusSnapshot>(&result);
  REQUIRE(snapshot != nullptr);
  REQUIRE_EQ(snapshot->markets_size(), 2);
  REQUIRE_EQ(snapshot->markets(0).state(), common::STREAM_LIFECYCLE_STATE_LIVE);
  REQUIRE_EQ(snapshot->markets(1).state(),
             common::STREAM_LIFECYCLE_STATE_DEGRADED);
  products.stop();
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"SELECTOR_MATRIX_AND_REGISTRY", selector_matrix_and_registry},
      {"LOOPBACK_ROUTING_IDS_STATUS_AND_GENERATION_CUTS",
       loopback_routing_ids_status_and_generation_cuts},
      {"PER_MARKET_CAPACITIES_AND_PENDING_ARE_INDEPENDENT",
       per_market_capacities_and_pending_are_independent},
      {"CONTEXT_TRACKER_REACHES_AND_NEVER_EXCEEDS_48",
       context_tracker_reaches_and_never_exceeds_48},
      {"ADVERSE_GLOBAL_SHUTDOWN_IS_BOUNDED_AND_JOINED",
       adverse_global_shutdown_is_bounded_and_joined},
      {"ONE_FAULTED_MARKET_STILL_RETURNS_TWO_STATUS_ROWS",
       one_faulted_market_still_returns_two_status_rows},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &failure) {
      std::cerr << name << "=FAIL " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
