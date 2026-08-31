#include "market_registry.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace binance_market_data::gateway::g11 {

MarketKey spot_btcusdt_key() {
  return {common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, "BTCUSDT"};
}

MarketKey usdm_btcusdt_key() {
  return {common_wire::VENUE_BINANCE, common_wire::MARKET_USD_M_PERPETUAL,
          "BTCUSDT"};
}

MarketRuntimeRegistry::MarketRuntimeRegistry(MarketServices spot,
                                             MarketServices usdm)
    : entries_{std::move(spot), std::move(usdm)} {
  if (entries_[0].key != spot_btcusdt_key() ||
      entries_[1].key != usdm_btcusdt_key()) {
    throw std::invalid_argument{
        "G11 registry requires ordered Spot and USD-M BTCUSDT entries"};
  }
  for (const auto &entry : entries_) {
    if (entry.runtime == nullptr || entry.recovery == nullptr ||
        entry.event_publication == nullptr) {
      throw std::invalid_argument{"G11 registry services must be non-null"};
    }
  }
}

const MarketServices *
MarketRuntimeRegistry::find(const MarketKey &key) const noexcept {
  const auto found =
      std::find_if(entries_.begin(), entries_.end(),
                   [&key](const auto &entry) { return entry.key == key; });
  return found == entries_.end() ? nullptr : &*found;
}

const std::array<MarketServices, kFixedMarketCount> &
MarketRuntimeRegistry::entries() const noexcept {
  return entries_;
}

} // namespace binance_market_data::gateway::g11
