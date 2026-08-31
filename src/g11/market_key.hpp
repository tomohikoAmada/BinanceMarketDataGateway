#pragma once

#include <binance_market_data/common/v1/enums.pb.h>

#include <string>

namespace binance_market_data::gateway::g11 {

namespace common_wire = ::binance_market_data::common::v1;

struct MarketKey final {
  common_wire::Venue venue{common_wire::VENUE_UNSPECIFIED};
  common_wire::Market market{common_wire::MARKET_UNSPECIFIED};
  std::string symbol;

  friend bool operator==(const MarketKey &, const MarketKey &) = default;
};

[[nodiscard]] MarketKey spot_btcusdt_key();
[[nodiscard]] MarketKey usdm_btcusdt_key();

} // namespace binance_market_data::gateway::g11
