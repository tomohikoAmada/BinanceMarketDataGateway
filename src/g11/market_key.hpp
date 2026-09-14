#pragma once

#include <binance_market_data/common/v1/enums.pb.h>

#include <algorithm>
#include <string>

namespace binance_market_data::gateway::g11 {

namespace common_wire = ::binance_market_data::common::v1;

struct MarketKey final {
  common_wire::Venue venue{common_wire::VENUE_UNSPECIFIED};
  common_wire::Market market{common_wire::MARKET_UNSPECIFIED};
  std::string symbol;

  friend bool operator==(const MarketKey &, const MarketKey &) = default;
};

// Canonical configured-product order: venue, market, then exact symbol bytes.
struct MarketKeyLess final {
  [[nodiscard]] bool operator()(const MarketKey &left,
                                const MarketKey &right) const noexcept {
    if (left.venue != right.venue) {
      return left.venue < right.venue;
    }
    if (left.market != right.market) {
      return left.market < right.market;
    }
    return std::lexicographical_compare(
        left.symbol.begin(), left.symbol.end(), right.symbol.begin(),
        right.symbol.end(), [](char left_byte, char right_byte) {
          return static_cast<unsigned char>(left_byte) <
                 static_cast<unsigned char>(right_byte);
        });
  }
};

[[nodiscard]] MarketKey spot_btcusdt_key();
[[nodiscard]] MarketKey usdm_btcusdt_key();

} // namespace binance_market_data::gateway::g11
