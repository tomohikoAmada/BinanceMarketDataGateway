#include "grpc_service.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
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
namespace g7 = binance_market_data::gateway::g7;
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

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid numeric spec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock fixed_clock() {
  return
      [] { return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
}

[[nodiscard]] market::ExchangeDepthSnapshot make_snapshot() {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("g7-grpc-test");
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

[[nodiscard]] market::DepthUpdate make_update(std::uint64_t update_id) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("g7-grpc-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g7-grpc-source");
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(g7::kUpdateSchema);
  update.set_first_update_id(update_id);
  update.set_final_update_id(update_id);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("3.000");
  return update;
}

void bootstrap(g3::MarketRuntime &runtime) {
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(
      runtime.submit_snapshot(make_snapshot(), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(101U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Live);
}

[[nodiscard]] wire::OrderBookSubscriptionRequest valid_request() {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id("grpc-request-1");
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(common::MARKET_SPOT);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] std::unique_ptr<wire::BinanceMarketDataGatewayService::Stub>
make_stub(int port) {
  return wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
}

void require_canonical(const wire::OrderBookStreamItem &item,
                       std::uint64_t sequence) {
  REQUIRE(!item.has_envelope_metadata());
  REQUIRE(item.has_delivery_metadata());
  REQUIRE_EQ(item.delivery_metadata().protocol_version(),
             std::string{g7::kProtocolVersion});
  REQUIRE_EQ(item.delivery_metadata().gateway_instance_id(), "gw-loopback");
  REQUIRE_EQ(item.delivery_metadata().session_sequence(), sequence);
  REQUIRE(item.delivery_metadata().publish_time_utc_ns() != 0U);
}

void valid_loopback_stream_and_cancel_cleanup() {
  g3::MarketRuntime runtime{{16U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());

  wire::OrderBookStreamItem item;
  REQUIRE(reader->Read(&item));
  require_canonical(item, 1U);
  REQUIRE(item.has_subscription_accepted());
  REQUIRE(!item.delivery_metadata().has_connection_generation());
  REQUIRE_EQ(
      item.subscription_accepted().negotiated_payload_schema_versions_size(),
      2);
  const auto subscription_id = item.delivery_metadata().subscription_id();

  REQUIRE(reader->Read(&item));
  require_canonical(item, 2U);
  REQUIRE(item.has_snapshot());
  REQUIRE_EQ(item.snapshot().last_update_id(), 101U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 11U);
  REQUIRE_EQ(item.delivery_metadata().subscription_id(), subscription_id);

  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(102U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE(reader->Read(&item));
  require_canonical(item, 3U);
  REQUIRE(item.has_depth_update());
  REQUIRE_EQ(item.depth_update().final_update_id(), 102U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 11U);

  context.TryCancel();
  while (reader->Read(&item)) {
  }
  const auto status = reader->Finish();
  REQUIRE(status.error_code() == grpc::StatusCode::CANCELLED || status.ok());
  for (int attempt = 0; attempt < 1000; ++attempt) {
    if (runtime.observe().resident_subscription_count == 0U) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  REQUIRE_EQ(runtime.observe().resident_subscription_count, 0U);
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  server.shutdown();
  runtime.stop();
}

void preaccept_rejection() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  auto request = valid_request();
  request.set_symbol("ETHUSDT");
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, request);
  wire::OrderBookStreamItem item;
  REQUIRE(!reader->Read(&item));
  REQUIRE_EQ(reader->Finish().error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  REQUIRE_EQ(runtime.observe().resident_subscription_count, 0U);
  server.shutdown();
}

void loopback_request_status_policy() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  const auto reject = [&](wire::OrderBookSubscriptionRequest request,
                          grpc::StatusCode expected) {
    grpc::ClientContext context;
    auto reader = stub->SubscribeOrderBook(&context, request);
    wire::OrderBookStreamItem item;
    REQUIRE(!reader->Read(&item));
    REQUIRE_EQ(reader->Finish().error_code(), expected);
  };

  auto request = valid_request();
  request.set_schema_version("unknown.v1");
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  request = valid_request();
  request.set_venue(common::VENUE_UNSPECIFIED);
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  request = valid_request();
  request.set_market(common::MARKET_UNSPECIFIED);
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  request = valid_request();
  request.set_symbol("ETHUSDT");
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  request = valid_request();
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_NONE);
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  request = valid_request();
  request.clear_supported_snapshot_schema_versions();
  reject(std::move(request), grpc::StatusCode::FAILED_PRECONDITION);
  request = valid_request();
  request.clear_supported_update_schema_versions();
  reject(std::move(request), grpc::StatusCode::FAILED_PRECONDITION);
  request = valid_request();
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  request = valid_request();
  request.add_supported_snapshot_schema_versions(" ");
  reject(std::move(request), grpc::StatusCode::INVALID_ARGUMENT);
  REQUIRE_EQ(runtime.observe().resident_subscription_count, 0U);
  server.shutdown();
}

void request_validation_policy() {
  auto request = valid_request();
  REQUIRE(std::holds_alternative<g7::ValidatedOrderBookSubscription>(
      g7::validate_order_book_request(request, "gw-loopback")));

  request.set_venue(common::VENUE_UNSPECIFIED);
  REQUIRE_EQ(std::get<g7::RequestValidationError>(
                 g7::validate_order_book_request(request, "gw-loopback")),
             g7::RequestValidationError::InvalidArgument);
  request = valid_request();
  request.clear_supported_snapshot_schema_versions();
  request.add_supported_snapshot_schema_versions("unknown.v1");
  REQUIRE_EQ(std::get<g7::RequestValidationError>(
                 g7::validate_order_book_request(request, "gw-loopback")),
             g7::RequestValidationError::FailedPrecondition);
  request = valid_request();
  request.set_depth_limit(0);
  REQUIRE_EQ(std::get<g7::RequestValidationError>(
                 g7::validate_order_book_request(request, "gw-loopback")),
             g7::RequestValidationError::InvalidArgument);
}

void server_cancelled_ok_handler_returns_cancelled() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);

  std::promise<grpc::StatusCode> proposed_status;
  auto proposed_status_future = proposed_status.get_future();
  std::promise<void> release_finalization;
  auto release_finalization_future = release_finalization.get_future().share();
  std::promise<void> snapshot_ready;
  auto snapshot_ready_future = snapshot_ready.get_future();
  std::promise<void> release_snapshot;
  auto release_snapshot_future = release_snapshot.get_future().share();
  std::promise<void> finalization_waiting;
  auto finalization_waiting_future = finalization_waiting.get_future();
  std::promise<grpc::StatusCode> final_status;
  auto final_status_future = final_status.get_future();

  g7::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.before_context_finalization = [&](grpc::StatusCode status) {
    proposed_status.set_value(status);
    release_finalization_future.wait();
  };
  options.cancellation_snapshot_ready = [&] {
    snapshot_ready.set_value();
    release_snapshot_future.wait();
  };
  options.context_finalization_waiting = [&] {
    finalization_waiting.set_value();
  };
  options.after_context_finalization = [&](grpc::StatusCode status) {
    final_status.set_value(status);
  };

  g7::OrderBookGrpcServer server{runtime, "gw-loopback", std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  wire::OrderBookStreamItem item;
  REQUIRE(reader->Read(&item));
  REQUIRE(reader->Read(&item));
  REQUIRE_EQ(server.service().tracked_context_count(), 1U);

  runtime.close_publication_admission();
  REQUIRE_EQ(runtime.shutdown_publication(),
             g3::PublicationShutdownResult::ShutDown);
  const auto proposed = proposed_status_future.get();

  auto shutdown = std::async(std::launch::async, [&] { server.shutdown(); });
  snapshot_ready_future.wait();
  const auto tracked_during_snapshot = server.service().tracked_context_count();
  release_finalization.set_value();
  finalization_waiting_future.wait();
  const auto tracked_while_finalizer_waited =
      server.service().tracked_context_count();
  release_snapshot.set_value();

  const auto finalized = final_status_future.get();
  REQUIRE(!reader->Read(&item));
  const auto client_status = reader->Finish();
  shutdown.get();
  const auto tracked_after_shutdown = server.service().tracked_context_count();
  runtime.stop();

  REQUIRE_EQ(proposed, grpc::StatusCode::OK);
  REQUIRE_EQ(finalized, grpc::StatusCode::CANCELLED);
  REQUIRE_EQ(client_status.error_code(), grpc::StatusCode::CANCELLED);
  REQUIRE_EQ(tracked_during_snapshot, 1U);
  REQUIRE_EQ(tracked_while_finalizer_waited, 1U);
  REQUIRE_EQ(tracked_after_shutdown, 0U);
}

void server_context_lifetime_snapshot_blocks_finalization() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);

  std::promise<void> finalization_reached;
  auto finalization_reached_future = finalization_reached.get_future();
  std::promise<void> release_finalization;
  auto release_finalization_future = release_finalization.get_future().share();
  std::promise<void> snapshot_ready;
  auto snapshot_ready_future = snapshot_ready.get_future();
  std::promise<void> release_snapshot;
  auto release_snapshot_future = release_snapshot.get_future().share();
  std::promise<void> finalization_waiting;
  auto finalization_waiting_future = finalization_waiting.get_future();
  std::promise<void> finalization_complete;
  auto finalization_complete_future = finalization_complete.get_future();

  g7::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.before_context_finalization = [&](grpc::StatusCode status) {
    if (status == grpc::StatusCode::OK) {
      finalization_reached.set_value();
    }
    release_finalization_future.wait();
  };
  options.cancellation_snapshot_ready = [&] {
    snapshot_ready.set_value();
    release_snapshot_future.wait();
  };
  options.context_finalization_waiting = [&] {
    finalization_waiting.set_value();
  };
  options.after_context_finalization = [&](grpc::StatusCode) {
    finalization_complete.set_value();
  };

  g7::OrderBookGrpcServer server{runtime, "gw-loopback", std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  wire::OrderBookStreamItem item;
  REQUIRE(reader->Read(&item));
  REQUIRE(reader->Read(&item));

  runtime.close_publication_admission();
  REQUIRE_EQ(runtime.shutdown_publication(),
             g3::PublicationShutdownResult::ShutDown);
  finalization_reached_future.wait();

  auto shutdown = std::async(std::launch::async, [&] { server.shutdown(); });
  snapshot_ready_future.wait();
  release_finalization.set_value();
  finalization_waiting_future.wait();
  const auto completion_while_snapshot_active =
      finalization_complete_future.wait_for(std::chrono::seconds::zero());
  const auto tracked_while_snapshot_active =
      server.service().tracked_context_count();
  release_snapshot.set_value();

  finalization_complete_future.wait();
  REQUIRE(!reader->Read(&item));
  static_cast<void>(reader->Finish());
  shutdown.get();
  const auto tracked_after_shutdown = server.service().tracked_context_count();
  runtime.stop();

  REQUIRE(completion_while_snapshot_active == std::future_status::timeout);
  REQUIRE_EQ(tracked_while_snapshot_active, 1U);
  REQUIRE_EQ(tracked_after_shutdown, 0U);
}

void server_cancel_preserves_preexisting_non_ok_status() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);

  std::promise<grpc::StatusCode> proposed_status;
  auto proposed_status_future = proposed_status.get_future();
  std::promise<void> release_finalization;
  auto release_finalization_future = release_finalization.get_future().share();
  std::promise<void> snapshot_ready;
  auto snapshot_ready_future = snapshot_ready.get_future();
  std::promise<void> release_snapshot;
  auto release_snapshot_future = release_snapshot.get_future().share();
  std::promise<grpc::StatusCode> final_status;
  auto final_status_future = final_status.get_future();

  g7::GrpcServiceOptions options;
  options.before_context_finalization = [&](grpc::StatusCode status) {
    proposed_status.set_value(status);
    release_finalization_future.wait();
  };
  options.cancellation_snapshot_ready = [&] {
    snapshot_ready.set_value();
    release_snapshot_future.wait();
  };
  options.after_context_finalization = [&](grpc::StatusCode status) {
    final_status.set_value(status);
  };

  g7::OrderBookGrpcServer server{runtime, "gw-loopback", std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  auto invalid_request = valid_request();
  invalid_request.set_symbol("ETHUSDT");
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, invalid_request);
  const auto proposed = proposed_status_future.get();

  auto shutdown = std::async(std::launch::async, [&] { server.shutdown(); });
  snapshot_ready_future.wait();
  release_finalization.set_value();
  release_snapshot.set_value();

  const auto finalized = final_status_future.get();
  wire::OrderBookStreamItem item;
  REQUIRE(!reader->Read(&item));
  static_cast<void>(reader->Finish());
  shutdown.get();
  const auto tracked_after_shutdown = server.service().tracked_context_count();
  runtime.stop();

  REQUIRE_EQ(proposed, grpc::StatusCode::INVALID_ARGUMENT);
  REQUIRE_EQ(finalized, grpc::StatusCode::INVALID_ARGUMENT);
  REQUIRE_EQ(tracked_after_shutdown, 0U);
}

void idle_cancel_and_shutdown_join() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  wire::OrderBookStreamItem item;
  REQUIRE(reader->Read(&item));
  REQUIRE(reader->Read(&item));

  auto shutdown = std::async(std::launch::async, [&] { server.shutdown(); });
  REQUIRE(!reader->Read(&item));
  static_cast<void>(reader->Finish());
  REQUIRE(shutdown.wait_for(std::chrono::seconds{5}) ==
          std::future_status::ready);
  shutdown.get();
  REQUIRE(!server.service().admission_open());
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  REQUIRE(runtime.observe().publication_shutdown);
  runtime.stop();
}

void idle_client_cancel_cleanup() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  wire::OrderBookStreamItem item;
  REQUIRE(reader->Read(&item));
  REQUIRE(reader->Read(&item));
  context.TryCancel();
  REQUIRE(!reader->Read(&item));
  REQUIRE_EQ(reader->Finish().error_code(), grpc::StatusCode::CANCELLED);
  for (int attempt = 0; attempt < 1000; ++attempt) {
    if (runtime.observe().resident_subscription_count == 0U &&
        server.service().tracked_context_count() == 0U) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  REQUIRE_EQ(runtime.observe().resident_subscription_count, 0U);
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  server.shutdown();
}

void terminal_write_failure_is_not_retried() {
  g3::MarketRuntime runtime{{16U, 8U, g7::PublicationLimits{8U, 2U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime);

  std::mutex mutex;
  std::condition_variable condition;
  bool third_write_blocked = false;
  bool release_third_write = false;
  std::size_t total_write_attempts = 0U;
  std::size_t terminal_write_attempts = 0U;
  bool terminal_semantics = false;
  g7::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.write_override = [&](grpc::ServerWriter<wire::OrderBookStreamItem> &,
                               const wire::OrderBookStreamItem &item) {
    std::unique_lock lock{mutex};
    ++total_write_attempts;
    condition.notify_all();
    if (item.delivery_metadata().session_sequence() == 3U) {
      third_write_blocked = true;
      condition.notify_all();
      condition.wait(lock, [&] { return release_third_write; });
    }
    if (item.has_consumer_gap()) {
      ++terminal_write_attempts;
      terminal_semantics =
          !item.has_envelope_metadata() &&
          !item.delivery_metadata().has_connection_generation() &&
          !item.consumer_gap().has_last_delivered_session_sequence() &&
          !item.consumer_gap().has_next_available_session_sequence() &&
          item.consumer_gap().reason() ==
              common::CONSUMER_GAP_REASON_SLOW_CONSUMER &&
          item.consumer_gap().recovery_action() ==
              common::RECOVERY_ACTION_RESUBSCRIBE;
      return false;
    }
    return true;
  };
  g7::OrderBookGrpcServer server{runtime, "gw-loopback", std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  auto client_read = std::async(std::launch::async, [&] {
    wire::OrderBookStreamItem item;
    return reader->Read(&item);
  });

  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return total_write_attempts >= 2U; });
  }
  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(102U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return third_write_blocked; });
  }
  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(103U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(104U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{104U});
  {
    std::lock_guard lock{mutex};
    release_third_write = true;
  }
  condition.notify_all();

  REQUIRE(!client_read.get());
  REQUIRE(reader->Finish().ok());
  {
    std::lock_guard lock{mutex};
    REQUIRE_EQ(total_write_attempts, 5U);
    REQUIRE_EQ(terminal_write_attempts, 1U);
    REQUIRE(terminal_semantics);
  }
  server.shutdown();
}

void blocked_writer_does_not_stall_owner_or_shutdown() {
  g3::MarketRuntime runtime{{16U, 8U, g7::PublicationLimits{8U, 4U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime);
  std::mutex mutex;
  std::condition_variable condition;
  bool blocked = false;
  bool released = false;
  std::size_t initial_write_attempts = 0U;
  g7::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.write_override = [&](grpc::ServerWriter<wire::OrderBookStreamItem> &,
                               const wire::OrderBookStreamItem &item) {
    if (item.delivery_metadata().session_sequence() != 3U) {
      {
        std::lock_guard lock{mutex};
        ++initial_write_attempts;
      }
      condition.notify_all();
      return true;
    }
    std::unique_lock lock{mutex};
    blocked = true;
    condition.notify_all();
    condition.wait(lock, [&] { return released; });
    return true;
  };
  g7::OrderBookGrpcServer server{runtime, "gw-loopback", std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  auto client_read = std::async(std::launch::async, [&] {
    wire::OrderBookStreamItem item;
    return reader->Read(&item);
  });
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return initial_write_attempts >= 2U; });
  }
  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(102U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return blocked; });
  }
  REQUIRE_EQ(
      runtime.submit_depth_update(make_update(103U), g3::SourceProvenance{11U}),
      g3::AdmissionResult::Accepted);
  auto owner_work =
      std::async(std::launch::async, [&] { return runtime.observe(); });
  REQUIRE(owner_work.wait_for(std::chrono::seconds{1}) ==
          std::future_status::ready);
  REQUIRE_EQ(owner_work.get().last_update_id,
             std::optional<std::uint64_t>{103U});

  auto shutdown = std::async(std::launch::async, [&] { server.shutdown(); });
  {
    std::lock_guard lock{mutex};
    released = true;
  }
  condition.notify_all();
  REQUIRE(shutdown.wait_for(std::chrono::seconds{5}) ==
          std::future_status::ready);
  shutdown.get();
  REQUIRE(!client_read.get());
  static_cast<void>(reader->Finish());
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
}

void closed_gate_rejects_new_rpc_before_server_shutdown() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  server.service().begin_shutdown();

  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  wire::OrderBookStreamItem item;
  REQUIRE(!reader->Read(&item));
  REQUIRE_EQ(reader->Finish().error_code(), grpc::StatusCode::UNAVAILABLE);
  REQUIRE_EQ(runtime.observe().resident_subscription_count, 0U);
  server.shutdown();
}

void unavailable_when_not_live_and_service_closed() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  g7::OrderBookGrpcServer server{
      runtime, "gw-loopback", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, valid_request());
  wire::OrderBookStreamItem item;
  REQUIRE(!reader->Read(&item));
  REQUIRE_EQ(reader->Finish().error_code(), grpc::StatusCode::UNAVAILABLE);
  server.shutdown();
  REQUIRE(!server.service().admission_open());
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, void (*)()>> tests{
      {"valid_loopback_stream_and_cancel_cleanup",
       valid_loopback_stream_and_cancel_cleanup},
      {"preaccept_rejection", preaccept_rejection},
      {"loopback_request_status_policy", loopback_request_status_policy},
      {"request_validation_policy", request_validation_policy},
      {"server_cancelled_ok_handler_returns_cancelled",
       server_cancelled_ok_handler_returns_cancelled},
      {"server_context_lifetime_snapshot_blocks_finalization",
       server_context_lifetime_snapshot_blocks_finalization},
      {"server_cancel_preserves_preexisting_non_ok_status",
       server_cancel_preserves_preexisting_non_ok_status},
      {"idle_cancel_and_shutdown_join", idle_cancel_and_shutdown_join},
      {"idle_client_cancel_cleanup", idle_client_cancel_cleanup},
      {"terminal_write_failure_is_not_retried",
       terminal_write_failure_is_not_retried},
      {"blocked_writer_does_not_stall_owner_or_shutdown",
       blocked_writer_does_not_stall_owner_or_shutdown},
      {"closed_gate_rejects_new_rpc_before_server_shutdown",
       closed_gate_rejects_new_rpc_before_server_shutdown},
      {"unavailable_when_not_live_and_service_closed",
       unavailable_when_not_live_and_service_closed},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << "PASS " << name << '\n';
    } catch (const std::exception &failure) {
      std::cerr << "FAIL " << name << ": " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
