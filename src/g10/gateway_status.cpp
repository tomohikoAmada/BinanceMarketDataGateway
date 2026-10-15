#include "gateway_status.hpp"

#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace binance_market_data::gateway::g10 {

namespace {

[[nodiscard]] constexpr bool valid_recovery_observation(
    const g5::RecoveryObservation &observation) noexcept {
  return observation.active_transport_count <= 1U &&
         (observation.active_transport_count == 0U ||
          observation.connection_generation != 0U);
}

struct CollectedMarket final {
  common_wire::Venue venue{common_wire::VENUE_UNSPECIFIED};
  common_wire::Market market{common_wire::MARKET_UNSPECIFIED};
  std::string symbol;
  g3::RuntimeObservation runtime;
  g5::RecoveryObservation recovery;
  g9::EventPublicationObservation event_publication;
};

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
    : runtime_{&runtime}, recovery_{&recovery},
      event_publication_{&event_publication}, clock_{std::move(clock)},
      gateway_instance_id_{gateway_instance_id} {
  if (!clock_ ||
      event_publication_->gateway_instance_id() != gateway_instance_id_) {
    throw std::invalid_argument{"invalid G10 status assembler configuration"};
  }
}

#if defined(BMD_GATEWAY_G11_ENABLED)
GatewayStatusAssembler::GatewayStatusAssembler(
    const g11::MarketRuntimeRegistry &registry, g3::RuntimeClock clock,
    const std::string &gateway_instance_id)
    : registry_{&registry}, clock_{std::move(clock)},
      gateway_instance_id_{gateway_instance_id} {
  if (!clock_) {
    throw std::invalid_argument{"invalid G11 status assembler clock"};
  }
  for (const auto &entry : registry_->entries()) {
    if (entry.event_publication->gateway_instance_id() !=
        gateway_instance_id_) {
      throw std::invalid_argument{
          "G11 status services must share the gateway_instance_id"};
    }
  }
}
#endif

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

StatusSnapshotResult
GatewayStatusAssembler::collect(std::chrono::steady_clock::time_point deadline,
                                std::stop_token stop_token) const {
  std::vector<CollectedMarket> markets;
#if defined(BMD_GATEWAY_G11_ENABLED)
  if (registry_ != nullptr) {
    markets.reserve(registry_->entries().size());
    for (const auto &entry : registry_->entries()) {
      const auto runtime = entry.runtime->observe_until(deadline, stop_token);
      if (!runtime.has_value()) {
        return StatusSnapshotError::ObservationTimeout;
      }
      const auto recovery = entry.recovery->observe_until(deadline, stop_token);
      if (!recovery.has_value()) {
        return StatusSnapshotError::ObservationTimeout;
      }
      markets.push_back(CollectedMarket{entry.key.venue, entry.key.market,
                                        entry.key.symbol, *runtime, *recovery,
                                        entry.event_publication->observe()});
    }
  } else
#endif
  {
    if (runtime_ == nullptr || recovery_ == nullptr ||
        event_publication_ == nullptr) {
      return StatusSnapshotError::InvalidObservation;
    }
    const auto runtime = runtime_->observe_until(deadline, stop_token);
    if (!runtime.has_value()) {
      return StatusSnapshotError::ObservationTimeout;
    }
    const auto recovery = recovery_->observe_until(deadline, stop_token);
    if (!recovery.has_value()) {
      return StatusSnapshotError::ObservationTimeout;
    }
    markets.push_back(CollectedMarket{
        common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, "BTCUSDT",
        *runtime, *recovery, event_publication_->observe()});
  }

  for (const auto &market : markets) {
    if (!map_runtime_state(market.runtime.state).has_value() ||
        !valid_recovery_observation(market.recovery) ||
        market.runtime.resident_subscription_count >
            g7::kMaximumActiveSubscriptions ||
        market.event_publication.active_subscriptions >
            g9::kMaximumActiveEventSubscriptions) {
      return StatusSnapshotError::InvalidObservation;
    }
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
  std::uint64_t total_active_subscriptions = 0U;
  for (const auto &observation : markets) {
    const auto mapped_state = map_runtime_state(observation.runtime.state);
    const auto active_subscriptions =
        observation.runtime.resident_subscription_count +
        observation.event_publication.active_subscriptions;
    auto *market = snapshot.add_markets();
    market->set_venue(observation.venue);
    market->set_market(observation.market);
    market->set_symbol(observation.symbol);
    market->set_state(*mapped_state);
    // This market status describes the applied order book. Trade/ticker
    // activity must never make a stalled book appear fresh. Missing source
    // provenance or an invalidated book leaves the existing optional time
    // unset.
    const auto &depth = observation.runtime.last_applied_depth;
    if (depth.has_value() &&
        observation.runtime.state == g3::RuntimeState::Live &&
        observation.runtime.projection_status ==
            g3::core::ProjectionStatus::Synchronized &&
        observation.recovery.state == g5::RecoveryState::Live &&
        observation.recovery.active_transport_count == 1U &&
        depth->connection_generation ==
            std::optional<std::uint64_t>{
                observation.recovery.connection_generation} &&
        depth->receive_time_utc_ns.has_value()) {
      market->set_last_event_utc_ns(*depth->receive_time_utc_ns);
    }
    if (observation.recovery.active_transport_count == 1U) {
      market->set_connection_generation(
          observation.recovery.connection_generation);
    }
    market->set_active_subscription_count(
        static_cast<std::uint64_t>(active_subscriptions));
    total_active_subscriptions +=
        static_cast<std::uint64_t>(active_subscriptions);
  }
  snapshot.set_total_active_subscriptions(total_active_subscriptions);
  return snapshot;
}

} // namespace binance_market_data::gateway::g10
