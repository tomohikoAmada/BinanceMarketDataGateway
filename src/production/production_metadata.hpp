#pragma once

#include "spot_transport.hpp"
#include "usdm_transport.hpp"

#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <variant>

namespace binance_market_data::gateway::production {

enum class MetadataStage : std::uint8_t {
  SpotFetch,
  SpotParse,
  UsdMFetch,
  UsdMParse,
};

struct ProductionMetadata final {
  projection::v1::NumericSpec spot_numeric_spec;
  projection::v1::NumericSpec usdm_numeric_spec;
};

struct MetadataError final {
  MetadataStage stage{MetadataStage::SpotFetch};
  std::string message;
};

struct MetadataSources final {
  std::function<g4::ExchangeInfoResult()> spot_fetch;
  std::function<g4::ExchangeInfoResult()> usdm_fetch;
};

using ProductionMetadataResult =
    std::variant<ProductionMetadata, MetadataError>;

[[nodiscard]] ProductionMetadataResult
acquire_production_metadata(MetadataSources sources = {});
[[nodiscard]] std::string_view to_string(MetadataStage stage) noexcept;

} // namespace binance_market_data::gateway::production
