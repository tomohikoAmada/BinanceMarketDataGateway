#pragma once

#include "runtime/clock.hpp"
#include "runtime/market_key.hpp"

#include <binance_market_data/market/v1/reference_data.pb.h>

#include <memory>
#include <optional>
#include <string>

namespace binance_market_data::gateway::production {

struct UsdMReferenceSeed final {
  runtime::MarketKey key;
  std::string exchange_info_body;
  runtime::ClockSample received_at{};
  std::optional<binance_market_data::market::v1::ReferenceObservation>
      instrument{};
  std::shared_ptr<const std::string> shared_exchange_info_body{};
};

} // namespace binance_market_data::gateway::production
