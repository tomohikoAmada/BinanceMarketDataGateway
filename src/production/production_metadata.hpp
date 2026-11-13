#pragma once

#include "composition/multi_market_runtime.hpp"
#include "production/reference_seed.hpp"
#include "transport/spot_transport.hpp"
#include "transport/usdm_transport.hpp"

#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
namespace composition = binance_market_data::gateway::composition;

namespace binance_market_data::gateway::production {

enum class MetadataStage : std::uint8_t {
  SpotFetch,
  SpotParse,
  UsdMFetch,
  UsdMParse,
};

struct ResolvedProductSpec final {
  runtime::MarketKey key;
  projection::v1::NumericSpec numeric_spec;
};

struct ProductionMetadata final {
  std::vector<ResolvedProductSpec> products;
  std::vector<UsdMReferenceSeed> usdm_references;
};

struct MetadataError final {
  MetadataStage stage{MetadataStage::SpotFetch};
  std::optional<runtime::MarketKey> product;
  std::string message;
};

struct MetadataSources final {
  std::function<transport::ExchangeInfoResult()> spot_fetch;
  std::function<transport::ExchangeInfoResult()> usdm_fetch;
};

using ProductionMetadataResult =
    std::variant<ProductionMetadata, MetadataError>;

[[nodiscard]] ProductionMetadataResult
acquire_production_metadata(const std::vector<runtime::MarketKey> &market_keys,
                            MetadataSources sources = {});
[[nodiscard]] std::vector<composition::ProductRuntimeSpec>
make_product_runtime_specs(ProductionMetadata metadata);
[[nodiscard]] std::string_view to_string(MetadataStage stage) noexcept;

} // namespace binance_market_data::gateway::production
