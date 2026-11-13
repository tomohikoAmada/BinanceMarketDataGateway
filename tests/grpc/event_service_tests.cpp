#include "grpc/grpc_service.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
namespace protocol = binance_market_data::gateway::protocol;
namespace publication = binance_market_data::gateway::publication;
namespace rpc = binance_market_data::gateway::rpc;
namespace rt = binance_market_data::gateway::runtime;

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
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

#define REQUIRE(value) require((value), #value)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] rt::RuntimeClock fixed_clock() {
  return
      [] { return rt::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
}

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid NumericSpec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] market::ExchangeDepthSnapshot make_snapshot() {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("g9-grpc-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("snapshot-1");
  snapshot.set_last_update_id(100U);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("1.000");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("2.000");
  return snapshot;
}

[[nodiscard]] market::DepthUpdate make_depth(std::uint64_t id) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("g9-grpc-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g9-source");
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(publication::kDepthEventSchema);
  metadata->set_receive_time_utc_ns(10U + id);
  metadata->set_receive_monotonic_ns(20U + id);
  update.set_first_update_id(id);
  update.set_final_update_id(id);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("3.000");
  return update;
}

[[nodiscard]] std::shared_ptr<const protocol::NormalizedSpotEvent>
make_event(common::Stream stream, std::uint64_t id) {
  if (stream == common::STREAM_DIFF_DEPTH) {
    return std::make_shared<const protocol::NormalizedSpotEvent>(
        make_depth(id));
  }
  if (stream == common::STREAM_AGG_TRADE) {
    market::AggTrade trade;
    auto *metadata = trade.mutable_metadata();
    metadata->set_venue(common::VENUE_BINANCE);
    metadata->set_market(common::MARKET_SPOT);
    metadata->set_symbol("BTCUSDT");
    metadata->set_connection_id("g9-source");
    metadata->set_stream(stream);
    metadata->set_schema_version(publication::kAggTradeEventSchema);
    metadata->set_receive_time_utc_ns(10U + id);
    metadata->set_receive_monotonic_ns(20U + id);
    trade.set_aggregate_trade_id(id);
    trade.set_price("100.00");
    trade.set_quantity("1.000");
    trade.set_first_trade_id(id);
    trade.set_last_trade_id(id);
    trade.set_trade_time_ms(id);
    return std::make_shared<const protocol::NormalizedSpotEvent>(
        std::move(trade));
  }
  market::BookTicker ticker;
  auto *metadata = ticker.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_connection_id("g9-source");
  metadata->set_stream(stream);
  metadata->set_schema_version(publication::kBookTickerEventSchema);
  metadata->set_receive_time_utc_ns(10U + id);
  metadata->set_receive_monotonic_ns(20U + id);
  ticker.set_update_id(id);
  ticker.set_best_bid_price("100.00");
  ticker.set_best_bid_quantity("1.000");
  ticker.set_best_ask_price("101.00");
  ticker.set_best_ask_quantity("2.000");
  return std::make_shared<const protocol::NormalizedSpotEvent>(
      std::move(ticker));
}

void bootstrap(rt::MarketRuntime &runtime) {
  REQUIRE_EQ(runtime.start(), rt::StartResult::Started);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot(), rt::SourceProvenance{1U}),
             rt::AdmissionResult::Accepted);
  REQUIRE_EQ(
      runtime.submit_depth_update(make_depth(101U), rt::SourceProvenance{1U}),
      rt::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().state, rt::RuntimeState::Live);
}

[[nodiscard]] const char *schema_for(common::Stream stream) {
  switch (stream) {
  case common::STREAM_DIFF_DEPTH:
    return publication::kDepthEventSchema;
  case common::STREAM_AGG_TRADE:
    return publication::kAggTradeEventSchema;
  case common::STREAM_BOOK_TICKER:
    return publication::kBookTickerEventSchema;
  default:
    throw TestFailure{"unsupported test stream"};
  }
}

[[nodiscard]] wire::EventSubscriptionRequest
valid_event_request(common::Stream stream) {
  wire::EventSubscriptionRequest request;
  request.set_request_id("event-request-1");
  request.set_schema_version(publication::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(common::MARKET_SPOT);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(stream);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(schema_for(stream));
  request.add_supported_payload_schema_versions("future-unknown.v1");
  return request;
}

[[nodiscard]] wire::OrderBookSubscriptionRequest order_book_request() {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id("book-request-1");
  request.set_schema_version(publication::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(common::MARKET_SPOT);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(publication::kSnapshotSchema);
  request.add_supported_update_schema_versions(publication::kUpdateSchema);
  return request;
}

[[nodiscard]] std::unique_ptr<wire::BinanceMarketDataGatewayService::Stub>
make_stub(int port) {
  return wire::BinanceMarketDataGatewayService::NewStub(
      ::grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                            ::grpc::InsecureChannelCredentials()));
}

void require_event_envelope(const wire::GatewayEventEnvelope &item,
                            std::uint64_t sequence) {
  REQUIRE(!item.has_envelope_metadata());
  REQUIRE(item.has_delivery_metadata());
  REQUIRE_EQ(item.delivery_metadata().protocol_version(),
             std::string{publication::kProtocolVersion});
  REQUIRE_EQ(item.delivery_metadata().gateway_instance_id(), "gw-g9");
  REQUIRE_EQ(item.delivery_metadata().session_sequence(), sequence);
}

void request_validation_policy() {
  auto request = valid_event_request(common::STREAM_DIFF_DEPTH);
  REQUIRE(std::holds_alternative<publication::ValidatedEventSubscription>(
      rpc::validate_event_request(request, "gw-g9")));

  const auto invalid = [](wire::EventSubscriptionRequest candidate) {
    REQUIRE_EQ(std::get<rpc::RequestValidationError>(
                   rpc::validate_event_request(candidate, "gw-g9")),
               rpc::RequestValidationError::InvalidArgument);
  };
  request.set_schema_version("wrong.v1");
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.set_delivery_mode(common::DELIVERY_MODE_LATEST_STATE);
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.clear_selectors();
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  *request.add_selectors() = request.selectors(0);
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.mutable_selectors(0)->set_venue(common::VENUE_UNSPECIFIED);
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.mutable_selectors(0)->set_market(common::MARKET_USD_M_PERPETUAL);
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.mutable_selectors(0)->set_symbol("ETHUSDT");
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.mutable_selectors(0)->set_stream(common::STREAM_DEPTH_SNAPSHOT);
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.mutable_selectors(0)->set_stream(
      static_cast<common::Stream>(std::numeric_limits<int>::max()));
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.clear_supported_payload_schema_versions();
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.add_supported_payload_schema_versions(publication::kDepthEventSchema);
  invalid(std::move(request));
  request = valid_event_request(common::STREAM_DIFF_DEPTH);
  request.clear_supported_payload_schema_versions();
  request.add_supported_payload_schema_versions("unknown.v1");
  REQUIRE_EQ(std::get<rpc::RequestValidationError>(
                 rpc::validate_event_request(request, "gw-g9")),
             rpc::RequestValidationError::FailedPrecondition);
}

void normal_loopback_each_stream() {
  rt::MarketRuntime runtime{{16U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  publication::EventPublication publication{"gw-g9", fixed_clock()};
  REQUIRE(publication.open_generation(1U));
  rpc::OrderBookGrpcServer server{
      runtime, publication, "gw-g9", {std::chrono::milliseconds{1}, 24U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  for (const auto stream : {common::STREAM_DIFF_DEPTH, common::STREAM_AGG_TRADE,
                            common::STREAM_BOOK_TICKER}) {
    ::grpc::ClientContext context;
    auto reader = stub->SubscribeEvents(&context, valid_event_request(stream));
    wire::GatewayEventEnvelope item;
    REQUIRE(reader->Read(&item));
    require_event_envelope(item, 1U);
    REQUIRE(item.has_subscription_accepted());
    REQUIRE(!item.delivery_metadata().has_connection_generation());
    REQUIRE_EQ(
        item.subscription_accepted().negotiated_payload_schema_versions_size(),
        1);
    REQUIRE_EQ(
        item.subscription_accepted().negotiated_payload_schema_versions(0),
        schema_for(stream));
    const auto subscription_id = item.delivery_metadata().subscription_id();

    REQUIRE_EQ(publication.publish(make_event(stream, 1U), 1U),
               publication::EventPublishResult::Published);
    REQUIRE_EQ(publication.publish(make_event(stream, 2U), 1U),
               publication::EventPublishResult::Published);
    for (std::uint64_t sequence = 2U; sequence <= 3U; ++sequence) {
      REQUIRE(reader->Read(&item));
      require_event_envelope(item, sequence);
      REQUIRE_EQ(item.delivery_metadata().subscription_id(), subscription_id);
      REQUIRE_EQ(item.delivery_metadata().connection_generation(), 1U);
      REQUIRE(
          (stream == common::STREAM_DIFF_DEPTH && item.has_depth_update()) ||
          (stream == common::STREAM_AGG_TRADE && item.has_agg_trade()) ||
          (stream == common::STREAM_BOOK_TICKER && item.has_book_ticker()));
    }
    context.TryCancel();
    while (reader->Read(&item)) {
    }
    const auto status = reader->Finish();
    REQUIRE(status.ok() ||
            status.error_code() == ::grpc::StatusCode::CANCELLED);
  }
  server.shutdown();
  runtime.stop();
  REQUIRE_EQ(publication.observe().active_subscriptions, 0U);
}

void replacement_and_permanent_lifecycle_status() {
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), rt::StartResult::Started);
  publication::EventPublication publication{"gw-g9", fixed_clock()};
  REQUIRE(publication.open_generation(1U));
  rpc::OrderBookGrpcServer server{
      runtime, publication, "gw-g9", {std::chrono::milliseconds{1}, 24U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  ::grpc::ClientContext old_context;
  auto old_reader = stub->SubscribeEvents(
      &old_context, valid_event_request(common::STREAM_AGG_TRADE));
  wire::GatewayEventEnvelope item;
  REQUIRE(old_reader->Read(&item));
  REQUIRE_EQ(publication.publish(make_event(common::STREAM_AGG_TRADE, 1U), 1U),
             publication::EventPublishResult::Published);
  REQUIRE(old_reader->Read(&item));
  REQUIRE(publication.quiesce_generation(1U));
  REQUIRE(publication.close_generation_replaced(1U));
  REQUIRE(old_reader->Read(&item));
  require_event_envelope(item, 3U);
  REQUIRE(item.has_consumer_gap());
  REQUIRE_EQ(item.consumer_gap().reason(),
             common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED);
  REQUIRE_EQ(item.consumer_gap().recovery_action(),
             common::RECOVERY_ACTION_RESUBSCRIBE);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 1U);
  REQUIRE(!old_reader->Read(&item));
  REQUIRE(old_reader->Finish().ok());

  REQUIRE(publication.open_generation(2U));
  ::grpc::ClientContext fresh_context;
  auto fresh_reader = stub->SubscribeEvents(
      &fresh_context, valid_event_request(common::STREAM_BOOK_TICKER));
  REQUIRE(fresh_reader->Read(&item));
  require_event_envelope(item, 1U);
  REQUIRE_EQ(
      publication.publish(make_event(common::STREAM_BOOK_TICKER, 2U), 2U),
      publication::EventPublishResult::Published);
  REQUIRE(fresh_reader->Read(&item));
  require_event_envelope(item, 2U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 2U);
  REQUIRE(publication.quiesce_generation(2U));
  REQUIRE(publication.close_generation_permanently(2U));
  REQUIRE(!fresh_reader->Read(&item));
  REQUIRE_EQ(fresh_reader->Finish().error_code(),
             ::grpc::StatusCode::UNAVAILABLE);

  server.shutdown();
  runtime.stop();
  REQUIRE_EQ(publication.observe().active_subscriptions, 0U);
}

void pre_projection_depth_and_g7_isolation() {
  rt::MarketRuntime runtime{{16U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  publication::EventPublication publication{"gw-g9", fixed_clock()};
  REQUIRE(publication.open_generation(1U));

  std::mutex mutex;
  std::condition_variable condition;
  std::vector<wire::OrderBookStreamItem> book_items;
  std::vector<wire::GatewayEventEnvelope> event_items;
  rpc::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.maximum_tracked_contexts = 24U;
  options.write_override =
      [&](::grpc::ServerWriter<wire::OrderBookStreamItem> &,
          const wire::OrderBookStreamItem &item) {
        {
          std::lock_guard lock{mutex};
          book_items.push_back(item);
        }
        condition.notify_all();
        return true;
      };
  options.event_write_override =
      [&](::grpc::ServerWriter<wire::GatewayEventEnvelope> &,
          const wire::GatewayEventEnvelope &item) {
        {
          std::lock_guard lock{mutex};
          event_items.push_back(item);
        }
        condition.notify_all();
        return true;
      };
  rpc::OrderBookGrpcServer server{runtime, publication, "gw-g9",
                                  std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  ::grpc::ClientContext book_context;
  ::grpc::ClientContext event_context;
  auto book_reader =
      stub->SubscribeOrderBook(&book_context, order_book_request());
  auto event_reader = stub->SubscribeEvents(
      &event_context, valid_event_request(common::STREAM_DIFF_DEPTH));
  {
    std::unique_lock lock{mutex};
    condition.wait(
        lock, [&] { return book_items.size() >= 2U && !event_items.empty(); });
  }

  const auto stale = make_event(common::STREAM_DIFF_DEPTH, 100U);
  REQUIRE_EQ(publication.publish(stale, 1U),
             publication::EventPublishResult::Published);
  REQUIRE_EQ(runtime.submit_depth_update(std::get<market::DepthUpdate>(*stale),
                                         rt::SourceProvenance{1U}),
             rt::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return event_items.size() >= 2U; });
    REQUIRE_EQ(book_items.size(), 2U);
  }

  const auto gap = make_event(common::STREAM_DIFF_DEPTH, 103U);
  REQUIRE_EQ(publication.publish(gap, 1U),
             publication::EventPublishResult::Published);
  REQUIRE_EQ(runtime.submit_depth_update(std::get<market::DepthUpdate>(*gap),
                                         rt::SourceProvenance{1U}),
             rt::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().state, rt::RuntimeState::NeedsResync);
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] {
      return event_items.size() >= 3U && book_items.size() >= 3U;
    });
    REQUIRE(book_items.back().has_consumer_gap());
    REQUIRE_EQ(book_items.back().consumer_gap().reason(),
               common::CONSUMER_GAP_REASON_UPSTREAM_SEQUENCE_GAP);
    REQUIRE(event_items.back().has_depth_update());
  }
  REQUIRE_EQ(publication.observe().source_state,
             publication::EventSourceState::Open);
  REQUIRE(publication.quiesce_generation(1U));
  REQUIRE(publication.close_generation_replaced(1U));
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return event_items.size() >= 4U; });
    REQUIRE(event_items.back().has_consumer_gap());
    REQUIRE_EQ(event_items.back().consumer_gap().reason(),
               common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED);
  }

  book_context.TryCancel();
  event_context.TryCancel();
  wire::OrderBookStreamItem book_item;
  wire::GatewayEventEnvelope event_item;
  while (book_reader->Read(&book_item)) {
  }
  while (event_reader->Read(&event_item)) {
  }
  static_cast<void>(book_reader->Finish());
  static_cast<void>(event_reader->Finish());
  server.shutdown();
  runtime.stop();
}

void slow_event_consumer_does_not_stall_g7() {
  rt::MarketRuntime runtime{{16U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  publication::EventPublication publication{
      "gw-g9", fixed_clock(), publication::EventPublicationLimits{8U, 2U}};
  REQUIRE(publication.open_generation(1U));
  auto admission = publication.admit(publication::ValidatedEventSubscription{
      "slow-event", common::STREAM_DIFF_DEPTH, publication::kDepthEventSchema});
  REQUIRE(std::holds_alternative<publication::AcceptedEventSubscription>(
      admission));
  const auto slow =
      std::get<publication::AcceptedEventSubscription>(std::move(admission))
          .channel;

  rpc::OrderBookGrpcServer server{runtime, publication, "gw-g9"};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  ::grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, order_book_request());
  wire::OrderBookStreamItem item;
  REQUIRE(reader->Read(&item));
  REQUIRE(item.has_subscription_accepted());
  REQUIRE(reader->Read(&item));
  REQUIRE(item.has_snapshot());

  for (const auto id : {102U, 103U}) {
    const auto event = make_event(common::STREAM_DIFF_DEPTH, id);
    REQUIRE_EQ(publication.publish(event, 1U),
               publication::EventPublishResult::Published);
    REQUIRE_EQ(
        runtime.submit_depth_update(std::get<market::DepthUpdate>(*event),
                                    rt::SourceProvenance{1U}),
        rt::AdmissionResult::Accepted);
    REQUIRE(reader->Read(&item));
    REQUIRE(item.has_depth_update());
  }
  REQUIRE_EQ(slow->state(), publication::EventChannelState::TerminalGap);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{103U});

  context.TryCancel();
  while (reader->Read(&item)) {
  }
  static_cast<void>(reader->Finish());
  publication.remove(slow);
  server.shutdown();
  runtime.stop();
}

void event_context_shutdown_race() {
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), rt::StartResult::Started);
  publication::EventPublication publication{"gw-g9", fixed_clock()};
  REQUIRE(publication.open_generation(1U));

  std::promise<void> finalization_reached;
  auto finalization_reached_future = finalization_reached.get_future();
  std::promise<void> release_finalization;
  auto release_finalization_future = release_finalization.get_future().share();
  std::promise<void> snapshot_ready;
  auto snapshot_ready_future = snapshot_ready.get_future();
  std::promise<void> release_snapshot;
  auto release_snapshot_future = release_snapshot.get_future().share();
  std::promise<::grpc::StatusCode> final_status;
  auto final_status_future = final_status.get_future();
  rpc::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.maximum_tracked_contexts = 24U;
  options.before_context_finalization = [&](::grpc::StatusCode status) {
    if (status == ::grpc::StatusCode::OK) {
      finalization_reached.set_value();
      release_finalization_future.wait();
    }
  };
  options.after_context_finalization = [&](::grpc::StatusCode status) {
    final_status.set_value(status);
  };
  options.cancellation_snapshot_ready = [&] {
    snapshot_ready.set_value();
    release_snapshot_future.wait();
  };
  rpc::OrderBookGrpcServer server{runtime, publication, "gw-g9",
                                  std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  ::grpc::ClientContext context;
  auto reader = stub->SubscribeEvents(
      &context, valid_event_request(common::STREAM_AGG_TRADE));
  wire::GatewayEventEnvelope item;
  REQUIRE(reader->Read(&item));
  publication.shutdown();
  finalization_reached_future.wait();
  auto shutdown = std::async(std::launch::async, [&] { server.shutdown(); });
  snapshot_ready_future.wait();
  release_finalization.set_value();
  release_snapshot.set_value();
  REQUIRE_EQ(final_status_future.get(), ::grpc::StatusCode::CANCELLED);
  REQUIRE(!reader->Read(&item));
  REQUIRE_EQ(reader->Finish().error_code(), ::grpc::StatusCode::CANCELLED);
  shutdown.get();
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  REQUIRE_EQ(publication.observe().active_subscriptions, 0U);
  runtime.stop();
}

void usdm_side_and_reference_envelopes() {
  publication::EventPublication publication{"gw-g5", fixed_clock()};
  REQUIRE(publication.open_generation(1U));
  const rt::MarketKey key{common::VENUE_BINANCE, common::MARKET_USD_M_PERPETUAL,
                          "BTCUSDT"};

  market::MarkPrice mark;
  mark.mutable_metadata()->set_venue(common::VENUE_BINANCE);
  mark.mutable_metadata()->set_market(common::MARKET_USD_M_PERPETUAL);
  mark.mutable_metadata()->set_symbol("BTCUSDT");
  mark.mutable_metadata()->set_stream(common::STREAM_MARK_PRICE);
  mark.mutable_metadata()->set_schema_version(
      publication::kMarkPriceEventSchema);
  mark.mutable_metadata()->set_receive_time_utc_ns(10U);
  mark.set_mark_price("11794.15");
  mark.set_index_price("11784.62");
  mark.set_estimated_settle_price("11784.25");
  mark.set_funding_rate("-0.00038167");
  mark.set_next_funding_time_ms(1562306400000U);

  market::Liquidation liquidation;
  liquidation.mutable_metadata()->set_venue(common::VENUE_BINANCE);
  liquidation.mutable_metadata()->set_market(common::MARKET_USD_M_PERPETUAL);
  liquidation.mutable_metadata()->set_symbol("BTCUSDT");
  liquidation.mutable_metadata()->set_stream(common::STREAM_LIQUIDATION);
  liquidation.mutable_metadata()->set_schema_version(
      publication::kLiquidationEventSchema);
  liquidation.set_side("SELL");
  liquidation.set_order_type("LIMIT");
  liquidation.set_time_in_force("IOC");
  liquidation.set_original_quantity("0.014");
  liquidation.set_price("9910");
  liquidation.set_average_price("9910");
  liquidation.set_order_status("FILLED");
  liquidation.set_last_filled_quantity("0.014");
  liquidation.set_accumulated_filled_quantity("0.014");
  liquidation.set_order_trade_time_ms(1568014460893U);

  market::ReferenceObservation reference;
  reference.set_schema_version("reference-observation.v1");
  reference.set_venue(common::VENUE_BINANCE);
  reference.set_market(common::MARKET_USD_M_PERPETUAL);
  reference.set_symbol("BTCUSDT");
  reference.set_request_id("reference-1");
  reference.set_source_kind(common::REFERENCE_SOURCE_KIND_OPEN_INTEREST);
  reference.set_receive_time_utc_ns(30U);
  reference.set_status(common::REFERENCE_OBSERVATION_STATUS_VALID);
  reference.mutable_open_interest()->set_open_interest("12345.678");

  const auto admit = [&publication](common::Stream stream, const char *schema,
                                    const char *request_id) {
    auto result = publication.admit({request_id, stream, schema});
    REQUIRE(
        std::holds_alternative<publication::AcceptedEventSubscription>(result));
    return std::get<publication::AcceptedEventSubscription>(std::move(result))
        .channel;
  };
  const auto mark_channel = admit(common::STREAM_MARK_PRICE,
                                  publication::kMarkPriceEventSchema, "mark");
  auto accepted = mark_channel->peek();
  REQUIRE(accepted.has_value());
  REQUIRE_EQ(mark_channel->acknowledge(accepted),
             publication::EventAcknowledgeResult::Acknowledged);
  REQUIRE_EQ(
      publication.publish(
          std::make_shared<const protocol::NormalizedMarketEvent>(mark), 1U),
      publication::EventPublishResult::Published);
  auto record = mark_channel->peek();
  auto envelope = rpc::materialize_event_envelope(*mark_channel, record, key);
  REQUIRE(envelope.has_mark_price());
  REQUIRE_EQ(envelope.mark_price().funding_rate(), "-0.00038167");
  REQUIRE_EQ(mark_channel->acknowledge(record),
             publication::EventAcknowledgeResult::Acknowledged);

  const auto liquidation_channel =
      admit(common::STREAM_LIQUIDATION, publication::kLiquidationEventSchema,
            "liquidation");
  accepted = liquidation_channel->peek();
  REQUIRE(accepted.has_value());
  REQUIRE_EQ(liquidation_channel->acknowledge(accepted),
             publication::EventAcknowledgeResult::Acknowledged);
  REQUIRE_EQ(
      publication.publish(
          std::make_shared<const protocol::NormalizedMarketEvent>(liquidation),
          1U),
      publication::EventPublishResult::Published);
  record = liquidation_channel->peek();
  envelope = rpc::materialize_event_envelope(*liquidation_channel, record, key);
  REQUIRE(envelope.has_liquidation());
  REQUIRE_EQ(envelope.liquidation().order_status(), "FILLED");
  REQUIRE_EQ(liquidation_channel->acknowledge(record),
             publication::EventAcknowledgeResult::Acknowledged);

  const auto reference_channel =
      admit(common::STREAM_REFERENCE_DATA,
            publication::kReferenceObservationSchema, "reference");
  accepted = reference_channel->peek();
  REQUIRE(accepted.has_value());
  REQUIRE_EQ(reference_channel->acknowledge(accepted),
             publication::EventAcknowledgeResult::Acknowledged);
  REQUIRE_EQ(
      publication.publish(
          std::make_shared<const protocol::NormalizedMarketEvent>(reference),
          1U),
      publication::EventPublishResult::Published);
  record = reference_channel->peek();
  envelope = rpc::materialize_event_envelope(*reference_channel, record, key);
  REQUIRE(envelope.has_reference_observation());
  REQUIRE_EQ(envelope.reference_observation().source_kind(),
             common::REFERENCE_SOURCE_KIND_OPEN_INTEREST);
  REQUIRE_EQ(envelope.reference_observation().open_interest().open_interest(),
             "12345.678");
  REQUIRE_EQ(reference_channel->acknowledge(record),
             publication::EventAcknowledgeResult::Acknowledged);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, void (*)()>> tests{
      {"REQUEST_VALIDATION_POLICY", request_validation_policy},
      {"NORMAL_LOOPBACK_EACH_STREAM", normal_loopback_each_stream},
      {"REPLACEMENT_AND_PERMANENT_LIFECYCLE_STATUS",
       replacement_and_permanent_lifecycle_status},
      {"PRE_PROJECTION_DEPTH_AND_G7_ISOLATION",
       pre_projection_depth_and_g7_isolation},
      {"SLOW_EVENT_CONSUMER_DOES_NOT_STALL_G7",
       slow_event_consumer_does_not_stall_g7},
      {"EVENT_CONTEXT_SHUTDOWN_RACE", event_context_shutdown_race},
      {"USDM_SIDE_AND_REFERENCE_ENVELOPES", usdm_side_and_reference_envelopes},
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
