#pragma once

#include "multi_market_runtime.hpp"
#include "spot_transport.hpp"
#include "usdm_transport.hpp"

#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::production {

enum class MetadataStage : std::uint8_t {
  SpotFetch,
  SpotParse,
  UsdMFetch,
  UsdMParse,
};

struct ResolvedProductSpec final {
  g11::MarketKey key;
  projection::v1::NumericSpec numeric_spec;
};

struct ProductionMetadata final {
  std::vector<ResolvedProductSpec> products;
};

struct MetadataError final {
  MetadataStage stage{MetadataStage::SpotFetch};
  std::optional<g11::MarketKey> product;
  std::string message;
};

struct MetadataSources final {
  std::function<g4::ExchangeInfoResult()> spot_fetch;
  std::function<g4::ExchangeInfoResult()> usdm_fetch;
};

using ProductionMetadataResult =
    std::variant<ProductionMetadata, MetadataError>;

[[nodiscard]] ProductionMetadataResult
acquire_production_metadata(const std::vector<g11::MarketKey> &market_keys,
                            MetadataSources sources = {});
[[nodiscard]] std::vector<g11::ProductRuntimeSpec>
make_product_runtime_specs(ProductionMetadata metadata);
[[nodiscard]] std::string_view to_string(MetadataStage stage) noexcept;

} // namespace binance_market_data::gateway::production
