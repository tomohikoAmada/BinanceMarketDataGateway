#pragma once

#include "recovery/spot_recovery.hpp"

#include <chrono>

namespace binance_market_data::gateway::recovery {

// Project policy: replace a healthy Spot connection ten minutes before
// Binance's documented 24-hour connection lifetime. This is not an
// exchange-prescribed value and intentionally has no jitter for the single
// BTCUSDT runtime.
inline constexpr auto kPlannedRotationAge =
    std::chrono::hours{23} + std::chrono::minutes{50};

[[nodiscard]] constexpr recovery::PlannedRotationPolicy production_policy() {
  return {std::chrono::duration_cast<std::chrono::nanoseconds>(
      kPlannedRotationAge)};
}

} // namespace binance_market_data::gateway::recovery
