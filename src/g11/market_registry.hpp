#pragma once

#include "event_publication.hpp"
#include "market_key.hpp"
#include "market_runtime.hpp"
#include "recovery_coordinator.hpp"

#include <array>
#include <cstddef>

namespace binance_market_data::gateway::g11 {

inline constexpr std::size_t kFixedMarketCount = 2U;

struct MarketServices final {
  MarketKey key;
  g3::MarketRuntime *runtime{nullptr};
  g5::RecoveryCoordinator *recovery{nullptr};
  g9::EventPublication *event_publication{nullptr};
};

// A fixed non-owning view. ProductRuntime owners must outlive this registry.
class MarketRuntimeRegistry final {
public:
  MarketRuntimeRegistry(MarketServices spot, MarketServices usdm);

  MarketRuntimeRegistry(const MarketRuntimeRegistry &) = delete;
  MarketRuntimeRegistry &operator=(const MarketRuntimeRegistry &) = delete;

  [[nodiscard]] const MarketServices *find(const MarketKey &key) const noexcept;
  [[nodiscard]] const std::array<MarketServices, kFixedMarketCount> &
  entries() const noexcept;

private:
  std::array<MarketServices, kFixedMarketCount> entries_;
};

} // namespace binance_market_data::gateway::g11
