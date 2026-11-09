#pragma once

#include "publication/event_publication.hpp"
#include "recovery/recovery_coordinator.hpp"
#include "runtime/market_key.hpp"
#include "runtime/market_runtime.hpp"

#include <cstddef>
#include <vector>

namespace binance_market_data::gateway::runtime {

inline constexpr std::size_t kMaximumConfiguredProducts = 20U;

struct MarketServices final {
  MarketKey key;
  MarketRuntime *runtime{nullptr};
  recovery::RecoveryCoordinator *recovery{nullptr};
  publication::EventPublication *event_publication{nullptr};
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

} // namespace binance_market_data::gateway::runtime
