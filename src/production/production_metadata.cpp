#include "production/production_metadata.hpp"

#include <algorithm>
#include <optional>
#include <utility>
namespace composition = binance_market_data::gateway::composition;

namespace binance_market_data::gateway::production {

namespace {

[[nodiscard]] std::string
network_message(const transport::NetworkError &error) {
  return error.stage + ": " + error.message;
}

[[nodiscard]] std::string
protocol_message(const protocol::ProtocolError &error) {
  return error.field + ": " + error.message;
}

} // namespace

ProductionMetadataResult
acquire_production_metadata(const std::vector<runtime::MarketKey> &market_keys,
                            MetadataSources sources) {
  const auto has_market = [&market_keys](runtime::common_wire::Market market) {
    return std::any_of(
        market_keys.begin(), market_keys.end(),
        [market](const auto &key) { return key.market == market; });
  };
  if (!sources.spot_fetch) {
    sources.spot_fetch = transport::fetch_spot_exchange_info_set_https;
  }
  if (!sources.usdm_fetch) {
    sources.usdm_fetch = transport::fetch_usdm_exchange_info_https;
  }

  std::optional<std::string> spot_body;
  if (has_market(runtime::common_wire::MARKET_SPOT)) {
    const auto response = sources.spot_fetch();
    if (const auto *error = std::get_if<transport::NetworkError>(&response)) {
      return MetadataError{MetadataStage::SpotFetch, std::nullopt,
                           network_message(*error)};
    }
    spot_body = std::get<transport::ExchangeInfoResponse>(response).body;
  }

  std::optional<std::string> usdm_body;
  if (has_market(runtime::common_wire::MARKET_USD_M_PERPETUAL)) {
    const auto response = sources.usdm_fetch();
    if (const auto *error = std::get_if<transport::NetworkError>(&response)) {
      return MetadataError{MetadataStage::UsdMFetch, std::nullopt,
                           network_message(*error)};
    }
    usdm_body = std::get<transport::ExchangeInfoResponse>(response).body;
  }

  auto canonical_keys = market_keys;
  std::sort(canonical_keys.begin(), canonical_keys.end(),
            runtime::MarketKeyLess{});
  ProductionMetadata result;
  result.products.reserve(canonical_keys.size());
  for (const auto &key : canonical_keys) {
    if (key.venue != runtime::common_wire::VENUE_BINANCE) {
      return MetadataError{MetadataStage::SpotParse, key,
                           "unsupported configured venue"};
    }
    if (key.market == runtime::common_wire::MARKET_SPOT) {
      if (!transport::make_spot_transport_routes(key.symbol).has_value()) {
        return MetadataError{
            MetadataStage::SpotParse, key,
            "Spot symbol cannot be represented by a Binance route"};
      }
      const auto parsed = protocol::parse_exchange_info(*spot_body, key.symbol);
      if (const auto *failure = std::get_if<protocol::ProtocolError>(&parsed)) {
        return MetadataError{MetadataStage::SpotParse, key,
                             protocol_message(*failure)};
      }
      result.products.push_back(
          {key, std::get<protocol::SpotMetadata>(parsed).numeric_spec});
      continue;
    }
    if (key.market == runtime::common_wire::MARKET_USD_M_PERPETUAL) {
      if (!transport::make_usdm_transport_routes(key.symbol).has_value()) {
        return MetadataError{
            MetadataStage::UsdMParse, key,
            "USD-M symbol cannot be represented by a Binance route"};
      }
      const auto parsed =
          protocol::parse_usdm_exchange_info(*usdm_body, key.symbol);
      if (const auto *failure = std::get_if<protocol::ProtocolError>(&parsed)) {
        return MetadataError{MetadataStage::UsdMParse, key,
                             protocol_message(*failure)};
      }
      result.products.push_back(
          {key, std::get<protocol::UsdMMetadata>(parsed).numeric_spec});
      continue;
    }
    return MetadataError{MetadataStage::SpotParse, key,
                         "unsupported configured market"};
  }
  return result;
}

std::vector<composition::ProductRuntimeSpec>
make_product_runtime_specs(ProductionMetadata metadata) {
  std::sort(metadata.products.begin(), metadata.products.end(),
            [](const auto &left, const auto &right) {
              return runtime::MarketKeyLess{}(left.key, right.key);
            });
  std::vector<composition::ProductRuntimeSpec> specifications;
  specifications.reserve(metadata.products.size());
  for (auto &product : metadata.products) {
    specifications.push_back({std::move(product.key), product.numeric_spec,
                              composition::ProductRuntimeOptions{}});
  }
  return specifications;
}

std::string_view to_string(MetadataStage stage) noexcept {
  switch (stage) {
  case MetadataStage::SpotFetch:
    return "spot-fetch";
  case MetadataStage::SpotParse:
    return "spot-parse";
  case MetadataStage::UsdMFetch:
    return "usdm-fetch";
  case MetadataStage::UsdMParse:
    return "usdm-parse";
  }
  return "unknown";
}

} // namespace binance_market_data::gateway::production
