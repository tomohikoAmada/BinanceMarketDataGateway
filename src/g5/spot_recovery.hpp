#pragma once

#include "recovery_coordinator.hpp"

#include <memory>

namespace binance_market_data::gateway::g5 {

struct RecoveryOptions final {
  g4::SpotTransportOptions transport;
  SourceGenerationLifecycle source_generation_lifecycle;
};

// Historical Spot facade over the shared product-independent lifecycle core.
class SpotRecovery final {
public:
  SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
               detail::RecoveryTestOptions test_options = {});
  SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
               RecoveryOptions options,
               detail::RecoveryTestOptions test_options = {});
  SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
               PlannedRotationPolicy planned_rotation,
               detail::RecoveryTestOptions test_options = {});
  SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
               PlannedRotationPolicy planned_rotation, RecoveryOptions options,
               detail::RecoveryTestOptions test_options = {});
  ~SpotRecovery();

  SpotRecovery(const SpotRecovery &) = delete;
  SpotRecovery &operator=(const SpotRecovery &) = delete;
  SpotRecovery(SpotRecovery &&) = delete;
  SpotRecovery &operator=(SpotRecovery &&) = delete;

  [[nodiscard]] RecoveryStartResult start();
  void stop() noexcept;
  [[nodiscard]] RecoveryObservation observe() const;
  [[nodiscard]] RecoveryObservation
  wait_for_generation_live(std::uint64_t generation);
  [[nodiscard]] RecoveryObservation wait_until_terminal();
  [[nodiscard]] bool request_controlled_recovery_for_acceptance();
  [[nodiscard]] std::optional<QuiescentAcceptanceCut> quiesce_for_acceptance();

private:
  std::unique_ptr<RecoveryCoordinator> coordinator_;
};

} // namespace binance_market_data::gateway::g5
