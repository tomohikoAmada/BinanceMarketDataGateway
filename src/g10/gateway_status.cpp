#include "gateway_status.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace binance_market_data::gateway::g10 {

namespace {

[[nodiscard]] constexpr bool valid_recovery_observation(
    const g5::RecoveryObservation &observation) noexcept {
  return observation.active_transport_count <= 1U &&
         (observation.active_transport_count == 0U ||
          observation.connection_generation != 0U);
}

} // namespace

std::optional<common_wire::StreamLifecycleState>
map_runtime_state(g3::RuntimeState state) noexcept {
  switch (state) {
  case g3::RuntimeState::Constructed:
    return common_wire::STREAM_LIFECYCLE_STATE_ACCEPTED;
  case g3::RuntimeState::Buffering:
  case g3::RuntimeState::AwaitingBridge:
    return common_wire::STREAM_LIFECYCLE_STATE_SNAPSHOT_PENDING;
  case g3::RuntimeState::Live:
    return common_wire::STREAM_LIFECYCLE_STATE_LIVE;
  case g3::RuntimeState::NeedsResync:
    return common_wire::STREAM_LIFECYCLE_STATE_RESYNC_IN_PROGRESS;
  case g3::RuntimeState::Faulted:
    return common_wire::STREAM_LIFECYCLE_STATE_DEGRADED;
  case g3::RuntimeState::Stopping:
    return common_wire::STREAM_LIFECYCLE_STATE_CLOSING;
  case g3::RuntimeState::Stopped:
    return common_wire::STREAM_LIFECYCLE_STATE_CLOSED;
  }
  return std::nullopt;
}

GatewayStatusAssembler::GatewayStatusAssembler(
    g3::MarketRuntime &runtime, g5::SpotRecovery &recovery,
    g9::EventPublication &event_publication, g3::RuntimeClock clock,
    const std::string &gateway_instance_id)
    : runtime_{runtime}, recovery_{recovery},
      event_publication_{event_publication}, clock_{std::move(clock)},
      gateway_instance_id_{gateway_instance_id} {
  if (!clock_ ||
      event_publication_.gateway_instance_id() != gateway_instance_id_) {
    throw std::invalid_argument{"invalid G10 status assembler configuration"};
  }
}

bool GatewayStatusAssembler::prepare_start_baseline() noexcept {
  try {
    const auto sample = clock_();
    std::lock_guard lock{baseline_mutex_};
    start_baseline_monotonic_ns_ = sample.monotonic_ns;
    return true;
  } catch (...) {
    clear_start_baseline();
    return false;
  }
}

void GatewayStatusAssembler::clear_start_baseline() noexcept {
  std::lock_guard lock{baseline_mutex_};
  start_baseline_monotonic_ns_.reset();
}

StatusSnapshotResult GatewayStatusAssembler::collect() const {
  const auto runtime = runtime_.observe();
  const auto recovery = recovery_.observe();
  const auto event_publication = event_publication_.observe();

  const auto mapped_state = map_runtime_state(runtime.state);
  if (!mapped_state.has_value() || !valid_recovery_observation(recovery) ||
      runtime.resident_subscription_count > g7::kMaximumActiveSubscriptions ||
      event_publication.active_subscriptions >
          g9::kMaximumActiveEventSubscriptions) {
    return StatusSnapshotError::InvalidObservation;
  }

  const auto active_subscriptions = runtime.resident_subscription_count +
                                    event_publication.active_subscriptions;
  if (active_subscriptions >
      static_cast<std::size_t>(std::numeric_limits<std::uint64_t>::max())) {
    return StatusSnapshotError::InvalidObservation;
  }

  std::uint64_t baseline_monotonic_ns = 0U;
  {
    std::lock_guard lock{baseline_mutex_};
    if (!start_baseline_monotonic_ns_.has_value()) {
      return StatusSnapshotError::StartBaselineUnavailable;
    }
    baseline_monotonic_ns = *start_baseline_monotonic_ns_;
  }

  g3::ClockSample observed_at{};
  try {
    observed_at = clock_();
  } catch (...) {
    return StatusSnapshotError::ClockError;
  }
  if (observed_at.monotonic_ns < baseline_monotonic_ns) {
    return StatusSnapshotError::MonotonicClockRegression;
  }

  gateway_wire::GatewayStatusSnapshot snapshot;
  snapshot.set_schema_version(kStatusSnapshotSchema);
  snapshot.set_gateway_instance_id(gateway_instance_id_);
  snapshot.set_observed_time_utc_ns(observed_at.utc_ns);
  snapshot.set_uptime_seconds(
      (observed_at.monotonic_ns - baseline_monotonic_ns) / 1'000'000'000U);
  auto *market = snapshot.add_markets();
  market->set_venue(common_wire::VENUE_BINANCE);
  market->set_market(common_wire::MARKET_SPOT);
  market->set_symbol("BTCUSDT");
  market->set_state(*mapped_state);
  if (recovery.last_event_utc_ns.has_value()) {
    market->set_last_event_utc_ns(*recovery.last_event_utc_ns);
  }
  if (recovery.active_transport_count == 1U) {
    market->set_connection_generation(recovery.connection_generation);
  }
  market->set_active_subscription_count(
      static_cast<std::uint64_t>(active_subscriptions));
  snapshot.set_total_active_subscriptions(
      static_cast<std::uint64_t>(active_subscriptions));
  return snapshot;
}

} // namespace binance_market_data::gateway::g10
