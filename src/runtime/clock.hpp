#pragma once

#include <cstdint>
#include <functional>

namespace binance_market_data::gateway::runtime {

struct ClockSample final {
  std::uint64_t utc_ns;
  std::uint64_t monotonic_ns;

  friend constexpr bool operator==(ClockSample, ClockSample) noexcept = default;
};

using RuntimeClock = std::function<ClockSample()>;

} // namespace binance_market_data::gateway::runtime
