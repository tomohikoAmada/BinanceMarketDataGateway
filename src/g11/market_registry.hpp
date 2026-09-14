#pragma once

#include "event_publication.hpp"
#include "market_key.hpp"
#include "market_runtime.hpp"
#include "recovery_coordinator.hpp"

#include <cstddef>
#include <vector>

namespace binance_market_data::gateway::g11 {

inline constexpr std::size_t kMaximumConfiguredProducts = 8U;

struct MarketServices final {
  MarketKey key;
  g3::MarketRuntime *runtime{nullptr};
  g5::RecoveryCoordinator *recovery{nullptr};
  g9::EventPublication *event_publication{nullptr};
};

// A finite immutable non-owning view. ProductRuntime owners must outlive it.
class MarketRuntimeRegistry final {
public:
  explicit MarketRuntimeRegistry(std::vector<MarketServices> entries);

  MarketRuntimeRegistry(const MarketRuntimeRegistry &) = delete;
  MarketRuntimeRegistry &operator=(const MarketRuntimeRegistry &) = delete;

  [[nodiscard]] const MarketServices *find(const MarketKey &key) const noexcept;
  [[nodiscard]] const std::vector<MarketServices> &entries() const noexcept;

private:
  const std::vector<MarketServices> entries_;
};

} // namespace binance_market_data::gateway::g11
