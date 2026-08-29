#include "market_runtime.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace market_wire = binance_market_data::market::v1;

constexpr std::uint64_t kSnapshotUtcNs = 1700000000123456000ULL;
constexpr std::uint64_t kSnapshotMonotonicNs = 9000000000999ULL;

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

template <typename Runtime>
concept ExposesProjection =
    requires(Runtime &runtime) { runtime.projection(); };

static_assert(!ExposesProjection<g3::MarketRuntime>);

[[nodiscard]] g3::RuntimeClock fixed_clock() {
  return [] { return g3::ClockSample{kSnapshotUtcNs, kSnapshotMonotonicNs}; };
}

[[nodiscard]] core::NumericSpec
numeric_spec(std::uint32_t price_scale = 2U,
             std::uint32_t quantity_scale = 3U) {
  const auto price = core::DecimalScale::create(price_scale);
  const auto quantity = core::DecimalScale::create(quantity_scale);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid test NumericSpec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] market_wire::ExchangeDepthSnapshot
make_snapshot(std::uint64_t last_update_id = 100U) {
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g3-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g3-snapshot-request-1");
  snapshot.set_last_update_id(last_update_id);
  snapshot.set_exchange_transaction_time_ms(1700000000001ULL);
  snapshot.set_receive_time_utc_ns(1700000000001000000ULL);
  snapshot.set_receive_monotonic_ns(9000000000001ULL);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("2.500");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("3.000");
  return snapshot;
}

[[nodiscard]] market_wire::DepthUpdate
make_update(std::uint64_t first_update_id, std::uint64_t final_update_id,
            const std::string &bid_quantity) {
  market_wire::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g3-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g3-synthetic-connection-1");
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(1700000000002ULL);
  metadata->set_receive_time_utc_ns(1700000000002000000ULL);
  metadata->set_receive_monotonic_ns(9000000000002ULL);
  update.set_first_update_id(first_update_id);
  update.set_final_update_id(final_update_id);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity(bid_quantity);
  return update;
}

[[nodiscard]] market_wire::DepthUpdate make_post_live_update() {
  auto update = make_update(102U, 103U, "5.000");
  auto *removed_ask = update.add_asks();
  removed_ask->set_price("102.00");
  removed_ask->set_quantity("0.000");
  auto *new_ask = update.add_asks();
  new_ask->set_price("103.00");
  new_ask->set_quantity("2.000");
  return update;
}

void require_live_at(g3::MarketRuntime &runtime,
                     std::uint64_t expected_update_id) {
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::Synchronized);
  REQUIRE(observation.last_update_id.has_value());
  REQUIRE_EQ(*observation.last_update_id, expected_update_id);
  REQUIRE(!observation.fault_reason.has_value());
}

void bootstrap_live(g3::MarketRuntime &runtime) {
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.500")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
             g3::AdmissionResult::Accepted);
  require_live_at(runtime, 101U);
}

[[nodiscard]] const g3::CapturedSnapshot &
require_captured(const g3::SnapshotResult &result) {
  REQUIRE(std::holds_alternative<g3::CapturedSnapshot>(result));
  return std::get<g3::CapturedSnapshot>(result);
}

void normal_serialized_bootstrap() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  REQUIRE_EQ(runtime.submit_depth_update(make_post_live_update()),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE(observation.last_install.has_value());
  REQUIRE_EQ(observation.last_install->disposition,
             core::InstallDisposition::Installed);
  REQUIRE(observation.last_apply.has_value());
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(observation.last_apply->last_update_id_after,
             core::UpdateId{103U});
  REQUIRE_EQ(observation.bootstrap_occupancy, 0U);

  const auto captured = runtime.capture_snapshot();
  const auto &snapshot = require_captured(captured).snapshot;
  REQUIRE_EQ(snapshot.last_update_id(), 103U);
  REQUIRE(snapshot.synchronized());
  REQUIRE_EQ(snapshot.bids_size(), 1);
  REQUIRE_EQ(snapshot.bids(0).quantity(), "5.000");
  REQUIRE_EQ(snapshot.asks_size(), 2);
  REQUIRE_EQ(snapshot.asks(0).price(), "101.00");
  REQUIRE_EQ(snapshot.asks(1).price(), "103.00");
}

void owner_only_projection_access() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  const auto caller_thread = std::this_thread::get_id();
  const auto observation = runtime.observe();
  const auto captured = runtime.capture_snapshot();
  const auto &capture = require_captured(captured);
  REQUIRE(observation.owner_thread_id != caller_thread);
  REQUIRE_EQ(capture.captured_on_thread, observation.owner_thread_id);
  REQUIRE(capture.captured_on_thread != caller_thread);
}

void source_receive_order() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, 102U, "5.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
             g3::AdmissionResult::Accepted);
  require_live_at(runtime, 102U);
  const auto capture = runtime.capture_snapshot();
  REQUIRE_EQ(require_captured(capture).snapshot.bids(0).quantity(), "5.000");
}

void ingress_bounded() {
  g3::MarketRuntime runtime{{2U, 8U}, fixed_clock(), numeric_spec(), {true}};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, 102U, "5.000")),
             g3::AdmissionResult::Accepted);
  const auto full = runtime.ingress_observation();
  REQUIRE_EQ(full.occupancy, 2U);
  REQUIRE_EQ(full.capacity, 2U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U, "6.000")),
             g3::AdmissionResult::Full);
  REQUIRE_EQ(runtime.ingress_observation().occupancy, 2U);
  runtime.release_owner_for_testing();
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason, g3::FaultReason::IngressOverflow);
  REQUIRE_EQ(observation.ingress_occupancy, 0U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(104U, 104U, "7.000")),
             g3::AdmissionResult::Faulted);
}

void bootstrap_buffer_bounded() {
  g3::MarketRuntime runtime{{8U, 1U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, 102U, "5.000")),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason,
             g3::FaultReason::BootstrapBufferOverflow);
  REQUIRE_EQ(observation.bootstrap_occupancy, 0U);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::AwaitingBaseline);
}

void no_bridge_then_later_bridge() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(90U, 99U, "9.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(90U, 100U, "8.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
             g3::AdmissionResult::Accepted);
  auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::AwaitingBridge);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::AwaitingBridge);
  REQUIRE(!observation.fault_reason.has_value());
  REQUIRE(observation.last_apply.has_value());
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::IgnoredDuplicate);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.500")),
             g3::AdmissionResult::Accepted);
  observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::Applied);
}

void gap_to_needs_resync() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U, "6.000")),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::NeedsResync);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::NeedsResync);
  REQUIRE(observation.last_apply.has_value());
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::GapDetected);
  REQUIRE(observation.last_gap.has_value());
  REQUIRE_EQ(observation.last_gap->reason, core::GapReason::SpotLiveForwardGap);
  REQUIRE_EQ(observation.last_gap->last_accepted_final, core::UpdateId{101U});
  REQUIRE_EQ(observation.last_gap->incoming_range.first(),
             core::UpdateId{103U});
  REQUIRE_EQ(runtime.submit_depth_update(make_update(104U, 104U, "7.000")),
             g3::AdmissionResult::Faulted);
}

void adapter_failure() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  auto invalid = make_update(102U, 102U, "5.000");
  invalid.mutable_metadata()->set_symbol("ETHUSDT");
  REQUIRE_EQ(runtime.submit_depth_update(std::move(invalid)),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason, g3::FaultReason::AdapterError);
  REQUIRE(observation.adapter_error.has_value());
  REQUIRE_EQ(observation.adapter_error->code,
             g3::adapter::AdapterErrorCode::IdentityMismatch);
}

void transport_failure_injection() {
  g3::MarketRuntime runtime{{4U, 4U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_transport_failure(), g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason, g3::FaultReason::TransportFailure);
  REQUIRE(observation.state != g3::RuntimeState::Live);
}

void snapshot_failure_injection() {
  g3::MarketRuntime runtime{{4U, 4U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_snapshot_failure(), g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason, g3::FaultReason::SnapshotFailure);
  REQUIRE(observation.state != g3::RuntimeState::Live);
}

void owner_snapshot_capture() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  REQUIRE_EQ(runtime.submit_depth_update(make_post_live_update()),
             g3::AdmissionResult::Accepted);
  const auto result = runtime.capture_snapshot();
  const auto &capture = require_captured(result);
  const auto &snapshot = capture.snapshot;
  REQUIRE_EQ(snapshot.venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(snapshot.market(), common_wire::MARKET_SPOT);
  REQUIRE_EQ(snapshot.symbol(), "BTCUSDT");
  REQUIRE_EQ(snapshot.source(), common_wire::SNAPSHOT_SOURCE_GATEWAY_LIVE);
  REQUIRE_EQ(snapshot.last_update_id(), 103U);
  REQUIRE_EQ(snapshot.generated_time_utc_ns(), kSnapshotUtcNs);
  REQUIRE_EQ(snapshot.generated_monotonic_ns(), kSnapshotMonotonicNs);
  REQUIRE_EQ(snapshot.producer(), "gateway-g3-runtime");
  REQUIRE_EQ(snapshot.producer_version(), "1.0.0");
  REQUIRE_EQ(capture.generated_at,
             (g3::ClockSample{kSnapshotUtcNs, kSnapshotMonotonicNs}));
  REQUIRE(capture.captured_on_thread != std::this_thread::get_id());
}

[[nodiscard]] std::string run_clock_scenario() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  const auto result = runtime.capture_snapshot();
  return require_captured(result).snapshot.SerializeAsString();
}

void deterministic_clock() {
  const auto first = run_clock_scenario();
  const auto second = run_clock_scenario();
  REQUIRE(!first.empty());
  REQUIRE_EQ(first, second);
}

void graceful_shutdown() {
  g3::MarketRuntime runtime{{4U, 4U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  runtime.stop();
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Stopped);
  REQUIRE(observation.owner_joined);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.000")),
             g3::AdmissionResult::Stopped);
  REQUIRE_EQ(runtime.start(), g3::StartResult::Stopped);
}

void shutdown_with_pending_work() {
  g3::MarketRuntime runtime{{4U, 4U}, fixed_clock(), numeric_spec(), {true}};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.500")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.ingress_observation().occupancy, 2U);
  runtime.stop();
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Stopped);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::Synchronized);
  REQUIRE_EQ(observation.last_update_id, 101U);
  REQUIRE(observation.last_apply.has_value());
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(observation.ingress_occupancy, 0U);
}

void shutdown_when_ingress_full() {
  g3::MarketRuntime runtime{{2U, 4U}, fixed_clock(), numeric_spec(), {true}};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.500")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.ingress_observation().occupancy, 2U);
  runtime.stop();
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Stopped);
  REQUIRE(observation.owner_joined);
  REQUIRE_EQ(observation.projection_status,
             core::ProjectionStatus::Synchronized);
  REQUIRE_EQ(observation.last_update_id, 101U);
}

void destructor_safety() {
  {
    g3::MarketRuntime runtime{{2U, 2U}, fixed_clock(), numeric_spec(), {true}};
    REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
    REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U, "4.500")),
               g3::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
               g3::AdmissionResult::Accepted);
  }
  REQUIRE(true);
}

void injected_real_numeric_spec() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec(2U, 4U)};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  auto update = make_update(101U, 101U, "4.5001");
  auto snapshot = make_snapshot();
  snapshot.mutable_bids(0)->set_quantity("2.5001");
  snapshot.mutable_asks(0)->set_quantity("3.0001");
  REQUIRE_EQ(runtime.submit_depth_update(std::move(update)),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(std::move(snapshot)),
             g3::AdmissionResult::Accepted);
  require_live_at(runtime, 101U);
  const auto captured = runtime.capture_snapshot();
  REQUIRE_EQ(require_captured(captured).snapshot.bids(0).quantity(), "4.5001");
}

void snapshot_exception_classification() {
  g3::RuntimeClock throwing_clock = []() -> g3::ClockSample {
    throw std::runtime_error{"clock failed"};
  };
  g3::MarketRuntime runtime{
      {8U, 8U}, std::move(throwing_clock), numeric_spec()};
  bootstrap_live(runtime);
  const auto result = runtime.capture_snapshot();
  REQUIRE(std::holds_alternative<g3::SnapshotRequestError>(result));
  REQUIRE_EQ(std::get<g3::SnapshotRequestError>(result),
             g3::SnapshotRequestError::ClockError);
  REQUIRE(g3::SnapshotRequestError::InternalError !=
          g3::SnapshotRequestError::ClockError);
}

void owner_domain_rebootstrap_reset() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U, "6.000")),
             g3::AdmissionResult::Accepted);
  const auto faulted = runtime.observe();
  REQUIRE_EQ(faulted.state, g3::RuntimeState::NeedsResync);
  REQUIRE(faulted.last_gap.has_value());

  REQUIRE_EQ(runtime.reset_for_rebootstrap(),
             g3::RebootstrapResetResult::Reset);
  const auto reset = runtime.observe();
  REQUIRE_EQ(reset.state, g3::RuntimeState::Buffering);
  REQUIRE_EQ(reset.projection_status, core::ProjectionStatus::AwaitingBaseline);
  REQUIRE_EQ(reset.owner_thread_id, faulted.owner_thread_id);
  REQUIRE_EQ(reset.last_reset_thread_id, reset.owner_thread_id);
  REQUIRE(reset.last_reset_thread_id != std::this_thread::get_id());
  REQUIRE_EQ(reset.reset_count, 1U);
  REQUIRE(!reset.last_gap.has_value());
  REQUIRE(!reset.last_install.has_value());
  REQUIRE(!reset.last_apply.has_value());
  REQUIRE(!reset.adapter_error.has_value());
  REQUIRE(!reset.fault_reason.has_value());
  REQUIRE(!reset.last_update_id.has_value());
  REQUIRE(reset.last_admitted_ticket >= faulted.last_admitted_ticket);
  REQUIRE(reset.processed_ticket >= faulted.processed_ticket);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(201U, 201U, "7.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot(200U)),
             g3::AdmissionResult::Accepted);
  require_live_at(runtime, 201U);
  const auto live_again = runtime.observe();
  REQUIRE(live_again.last_admitted_ticket > faulted.last_admitted_ticket);
  REQUIRE(live_again.processed_ticket > faulted.processed_ticket);
  REQUIRE_EQ(runtime.reset_for_rebootstrap(),
             g3::RebootstrapResetResult::InvalidState);
}

void direct_needs_resync_wait() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  std::stop_source stop_source;
  std::optional<g3::RuntimeObservation> waited;
  std::thread waiter{[&] {
    waited = runtime.wait_until_recovery_required(stop_source.get_token());
  }};
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U, "6.000")),
             g3::AdmissionResult::Accepted);
  waiter.join();
  REQUIRE(waited.has_value());
  REQUIRE_EQ(waited->state, g3::RuntimeState::NeedsResync);
  REQUIRE_EQ(waited->projection_status, core::ProjectionStatus::NeedsResync);
}

void owner_domain_planned_rebootstrap_reset() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap_live(runtime);
  const auto before = runtime.observe();

  REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
             g3::PlannedRebootstrapResetResult::Reset);
  const auto reset = runtime.observe();
  REQUIRE_EQ(reset.state, g3::RuntimeState::Buffering);
  REQUIRE_EQ(reset.projection_status, core::ProjectionStatus::AwaitingBaseline);
  REQUIRE_EQ(reset.last_reset_thread_id, reset.owner_thread_id);
  REQUIRE(reset.last_reset_thread_id != std::this_thread::get_id());
  REQUIRE_EQ(reset.reset_count, before.reset_count + 1U);
  REQUIRE(reset.last_admitted_ticket >= before.last_admitted_ticket);
  REQUIRE(reset.processed_ticket >= before.processed_ticket);
  REQUIRE(!reset.last_gap.has_value());
  REQUIRE(!reset.last_install.has_value());
  REQUIRE(!reset.last_apply.has_value());
  REQUIRE(!reset.fault_reason.has_value());
  REQUIRE(!reset.last_update_id.has_value());
}

void planned_reset_rejects_wrong_state() {
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
               g3::PlannedRebootstrapResetResult::NotStarted);
    REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
    REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
               g3::PlannedRebootstrapResetResult::InvalidState);
    runtime.stop();
    REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
               g3::PlannedRebootstrapResetResult::Stopped);
  }
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
    REQUIRE_EQ(runtime.submit_transport_failure(),
               g3::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Faulted);
    REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
               g3::PlannedRebootstrapResetResult::InvalidState);
    REQUIRE_EQ(runtime.reset_for_rebootstrap(),
               g3::RebootstrapResetResult::Reset);
  }
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap_live(runtime);
    REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U, "6.000")),
               g3::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::NeedsResync);
    REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
               g3::PlannedRebootstrapResetResult::InvalidState);
    REQUIRE_EQ(runtime.reset_for_rebootstrap(),
               g3::RebootstrapResetResult::Reset);
  }
}

void timed_recovery_or_deadline_wait() {
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap_live(runtime);
    REQUIRE_EQ(runtime.wait_until_recovery_required_for(
                   std::stop_token{}, std::chrono::nanoseconds::zero()),
               g3::TimedRecoveryWaitResult::DeadlineReached);
  }
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap_live(runtime);
    REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U, "6.000")),
               g3::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::NeedsResync);
    REQUIRE_EQ(runtime.wait_until_recovery_required_for(std::stop_token{},
                                                        std::chrono::hours{1}),
               g3::TimedRecoveryWaitResult::RecoveryRequired);
  }
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap_live(runtime);
    std::stop_source stop_source;
    REQUIRE(stop_source.request_stop());
    REQUIRE_EQ(runtime.wait_until_recovery_required_for(stop_source.get_token(),
                                                        std::chrono::hours{1}),
               g3::TimedRecoveryWaitResult::Stopped);
  }
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"NORMAL_SERIALIZED_BOOTSTRAP", normal_serialized_bootstrap},
      {"OWNER_ONLY_PROJECTION_ACCESS", owner_only_projection_access},
      {"SOURCE_RECEIVE_ORDER", source_receive_order},
      {"INGRESS_BOUNDED", ingress_bounded},
      {"BOOTSTRAP_BUFFER_BOUNDED", bootstrap_buffer_bounded},
      {"NO_BRIDGE_THEN_LATER_BRIDGE", no_bridge_then_later_bridge},
      {"GAP_TO_NEEDS_RESYNC", gap_to_needs_resync},
      {"ADAPTER_FAILURE", adapter_failure},
      {"TRANSPORT_FAILURE_INJECTION", transport_failure_injection},
      {"SNAPSHOT_FAILURE_INJECTION", snapshot_failure_injection},
      {"OWNER_SNAPSHOT_CAPTURE", owner_snapshot_capture},
      {"DETERMINISTIC_CLOCK", deterministic_clock},
      {"GRACEFUL_SHUTDOWN", graceful_shutdown},
      {"SHUTDOWN_WITH_PENDING_WORK", shutdown_with_pending_work},
      {"SHUTDOWN_WHEN_INGRESS_FULL", shutdown_when_ingress_full},
      {"DESTRUCTOR_SAFETY", destructor_safety},
      {"INJECTED_REAL_NUMERIC_SPEC", injected_real_numeric_spec},
      {"SNAPSHOT_EXCEPTION_CLASSIFICATION", snapshot_exception_classification},
      {"OWNER_DOMAIN_REBOOTSTRAP_RESET", owner_domain_rebootstrap_reset},
      {"DIRECT_NEEDS_RESYNC_WAIT", direct_needs_resync_wait},
      {"OWNER_DOMAIN_PLANNED_REBOOTSTRAP_RESET",
       owner_domain_planned_rebootstrap_reset},
      {"PLANNED_RESET_REJECTS_WRONG_STATE", planned_reset_rejects_wrong_state},
      {"TIMED_RECOVERY_OR_DEADLINE_WAIT", timed_recovery_or_deadline_wait},
  };

  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &error) {
      std::cerr << name << "=FAIL " << error.what() << '\n';
      return 1;
    }
  }
  return 0;
}
