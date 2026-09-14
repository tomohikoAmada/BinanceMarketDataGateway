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

MarketRuntimeRegistry::MarketRuntimeRegistry(
    std::vector<MarketServices> entries)
    : entries_{[&entries] {
        if (entries.empty() || entries.size() > kMaximumConfiguredProducts) {
          throw std::invalid_argument{
              "configured registry requires between one and eight entries"};
        }
        std::sort(entries.begin(), entries.end(),
                  [](const auto &left, const auto &right) {
                    return MarketKeyLess{}(left.key, right.key);
                  });
        return std::move(entries);
      }()} {
  for (std::size_t index = 0U; index < entries_.size(); ++index) {
    const auto &entry = entries_[index];
    if (index != 0U && entries_[index - 1U].key == entry.key) {
      throw std::invalid_argument{"configured registry keys must be unique"};
    }
    if (entry.runtime == nullptr || entry.recovery == nullptr ||
        entry.event_publication == nullptr) {
      throw std::invalid_argument{
          "configured registry services must be non-null"};
    }
    for (std::size_t earlier = 0U; earlier < index; ++earlier) {
      if (entries_[earlier].runtime == entry.runtime ||
          entries_[earlier].recovery == entry.recovery ||
          entries_[earlier].event_publication == entry.event_publication) {
        throw std::invalid_argument{
            "configured registry services must not alias between products"};
      }
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

const std::vector<MarketServices> &
MarketRuntimeRegistry::entries() const noexcept {
  return entries_;
}

} // namespace binance_market_data::gateway::g11
