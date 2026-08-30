#include "grpc_service.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

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
    throw TestFailure{"invalid NumericSpec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock fixed_clock() {
  return
      [] { return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
}

[[nodiscard]] market::ExchangeDepthSnapshot
make_snapshot(std::uint64_t last_update_id, std::string request_id) {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("g8-integration-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id(std::move(request_id));
  snapshot.set_last_update_id(last_update_id);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("1.000");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("2.000");
  return snapshot;
}

[[nodiscard]] market::DepthUpdate make_update(std::uint64_t update_id,
                                              std::string connection_id) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("g8-integration-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::move(connection_id));
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(g7::kUpdateSchema);
  update.set_first_update_id(update_id);
  update.set_final_update_id(update_id);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("3.000");
  return update;
}

void bootstrap(g3::MarketRuntime &runtime, std::uint64_t baseline,
               std::uint64_t generation, std::string connection_id) {
  REQUIRE_EQ(
      runtime.submit_snapshot(
          make_snapshot(baseline, "snapshot-" + std::to_string(generation)),
          g3::SourceProvenance{generation}),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(
                 make_update(baseline + 1U, std::move(connection_id)),
                 g3::SourceProvenance{generation}),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::Synchronized);
  REQUIRE_EQ(observation.current_projection_generation,
             std::optional<std::uint64_t>{generation});
}

[[nodiscard]] wire::OrderBookSubscriptionRequest
request(std::string request_id) {
  wire::OrderBookSubscriptionRequest value;
  value.set_request_id(std::move(request_id));
  value.set_schema_version(g7::kOrderBookRequestSchema);
  value.set_venue(common::VENUE_BINANCE);
  value.set_market(common::MARKET_SPOT);
  value.set_symbol("BTCUSDT");
  value.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  value.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  value.add_supported_update_schema_versions(g7::kUpdateSchema);
  return value;
}

[[nodiscard]] std::unique_ptr<wire::BinanceMarketDataGatewayService::Stub>
make_stub(int port) {
  return wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
}

void require_item(const wire::OrderBookStreamItem &item,
                  const std::string &subscription_id, std::uint64_t sequence) {
  REQUIRE(!item.has_envelope_metadata());
  REQUIRE(item.has_delivery_metadata());
  REQUIRE_EQ(item.delivery_metadata().protocol_version(),
             std::string{g7::kProtocolVersion});
  REQUIRE_EQ(item.delivery_metadata().gateway_instance_id(), "gw-g8-test");
  REQUIRE_EQ(item.delivery_metadata().subscription_id(), subscription_id);
  REQUIRE_EQ(item.delivery_metadata().session_sequence(), sequence);
  REQUIRE(item.delivery_metadata().publish_time_utc_ns() != 0U);
}

[[nodiscard]] bool wait_for_subscriber_count(g3::MarketRuntime &runtime,
                                             std::size_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  do {
    if (runtime.observe().resident_subscription_count == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  } while (std::chrono::steady_clock::now() < deadline);
  return runtime.observe().resident_subscription_count == expected;
}

void projection_needs_resync_across_real_grpc() {
  g3::MarketRuntime runtime{{16U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  bootstrap(runtime, 100U, 41U, "g8-generation-41");

  g7::OrderBookGrpcServer server{
      runtime, "gw-g8-test", {std::chrono::milliseconds{1}, 16U}};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());

  grpc::ClientContext old_context;
  old_context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds{10});
  auto old_reader =
      stub->SubscribeOrderBook(&old_context, request("g8-generation-41"));
  wire::OrderBookStreamItem item;
  REQUIRE(old_reader->Read(&item));
  REQUIRE(item.has_subscription_accepted());
  const auto old_subscription_id = item.delivery_metadata().subscription_id();
  require_item(item, old_subscription_id, 1U);
  REQUIRE(!item.delivery_metadata().has_connection_generation());

  REQUIRE(old_reader->Read(&item));
  require_item(item, old_subscription_id, 2U);
  REQUIRE(item.has_snapshot());
  REQUIRE(item.snapshot().synchronized());
  REQUIRE_EQ(item.snapshot().last_update_id(), 101U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 41U);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "g8-generation-41"),
                                         g3::SourceProvenance{41U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{102U});
  REQUIRE(old_reader->Read(&item));
  require_item(item, old_subscription_id, 3U);
  REQUIRE(item.has_depth_update());
  REQUIRE_EQ(item.depth_update().final_update_id(), 102U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 41U);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(104U, "g8-generation-41"),
                                         g3::SourceProvenance{41U}),
             g3::AdmissionResult::Accepted);
  const auto needs_resync = runtime.observe();
  REQUIRE_EQ(needs_resync.state, g3::RuntimeState::NeedsResync);
  REQUIRE_EQ(needs_resync.projection_status,
             core::ProjectionStatus::NeedsResync);
  REQUIRE(needs_resync.last_gap.has_value());
  REQUIRE(needs_resync.last_apply.has_value());
  REQUIRE_EQ(needs_resync.last_apply->disposition,
             core::ApplyDisposition::GapDetected);

  REQUIRE(old_reader->Read(&item));
  require_item(item, old_subscription_id, 4U);
  REQUIRE(item.has_consumer_gap());
  REQUIRE_EQ(item.consumer_gap().reason(),
             common::CONSUMER_GAP_REASON_UPSTREAM_SEQUENCE_GAP);
  REQUIRE_EQ(item.consumer_gap().recovery_action(),
             common::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 41U);
  REQUIRE(!old_reader->Read(&item));
  REQUIRE(old_reader->Finish().ok());
  REQUIRE(wait_for_subscriber_count(runtime, 0U));

  REQUIRE_EQ(runtime.reset_for_rebootstrap(),
             g3::RebootstrapResetResult::Reset);
  bootstrap(runtime, 200U, 42U, "g8-generation-42");

  grpc::ClientContext new_context;
  new_context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds{10});
  auto new_reader =
      stub->SubscribeOrderBook(&new_context, request("g8-generation-42"));
  REQUIRE(new_reader->Read(&item));
  REQUIRE(item.has_subscription_accepted());
  const auto new_subscription_id = item.delivery_metadata().subscription_id();
  REQUIRE(new_subscription_id != old_subscription_id);
  require_item(item, new_subscription_id, 1U);
  REQUIRE(!item.delivery_metadata().has_connection_generation());

  REQUIRE(new_reader->Read(&item));
  require_item(item, new_subscription_id, 2U);
  REQUIRE(item.has_snapshot());
  REQUIRE(item.snapshot().synchronized());
  REQUIRE_EQ(item.snapshot().last_update_id(), 201U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 42U);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(202U, "g8-generation-42"),
                                         g3::SourceProvenance{42U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{202U});
  REQUIRE(new_reader->Read(&item));
  require_item(item, new_subscription_id, 3U);
  REQUIRE(item.has_depth_update());
  REQUIRE_EQ(item.depth_update().final_update_id(), 202U);
  REQUIRE_EQ(item.delivery_metadata().connection_generation(), 42U);

  new_context.TryCancel();
  while (new_reader->Read(&item)) {
  }
  const auto new_status = new_reader->Finish();
  REQUIRE(new_status.ok() ||
          new_status.error_code() == grpc::StatusCode::CANCELLED);
  REQUIRE(wait_for_subscriber_count(runtime, 0U));
  server.shutdown();
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);
  runtime.stop();
  REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Stopped);

  std::cout << "PROJECTION_NEEDS_RESYNC_CONSUMER_PATH=PASS\n"
            << "NEEDS_RESYNC_TERMINAL_REASON=UPSTREAM_SEQUENCE_GAP\n"
            << "NEEDS_RESYNC_RESUBSCRIBE_SNAPSHOT=PASS\n"
            << "OLD_SESSION_CROSSED_REBOOTSTRAP=NO\n"
            << "NEW_SESSION_SEQUENCE_RESTARTED=YES\n";
}

} // namespace

int main() {
  try {
    projection_needs_resync_across_real_grpc();
    std::cout << "PASS projection_needs_resync_across_real_grpc\n";
    return 0;
  } catch (const std::exception &failure) {
    std::cerr << "FAIL projection_needs_resync_across_real_grpc: "
              << failure.what() << '\n';
    return 1;
  }
}
