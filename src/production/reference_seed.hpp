#pragma once

#include "runtime/clock.hpp"
#include "runtime/market_key.hpp"

#include <string>

namespace binance_market_data::gateway::production {

struct UsdMReferenceSeed final {
  runtime::MarketKey key;
  std::string exchange_info_body;
  runtime::ClockSample received_at;
};

} // namespace binance_market_data::gateway::production
