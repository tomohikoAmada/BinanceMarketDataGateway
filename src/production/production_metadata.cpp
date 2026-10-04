#include "production_metadata.hpp"

#include <algorithm>
#include <optional>
#include <utility>

namespace binance_market_data::gateway::production {

namespace {

[[nodiscard]] std::string network_message(const g4::NetworkError &error) {
  return error.stage + ": " + error.message;
}

[[nodiscard]] std::string protocol_message(const g4::ProtocolError &error) {
  return error.field + ": " + error.message;
}

} // namespace

ProductionMetadataResult
acquire_production_metadata(const std::vector<g11::MarketKey> &market_keys,
                            MetadataSources sources) {
  const auto has_market = [&market_keys](g11::common_wire::Market market) {
    return std::any_of(
        market_keys.begin(), market_keys.end(),
        [market](const auto &key) { return key.market == market; });
  };
  if (!sources.spot_fetch) {
    sources.spot_fetch = g4::fetch_spot_exchange_info_set_https;
  }
  if (!sources.usdm_fetch) {
    sources.usdm_fetch = g11::fetch_usdm_exchange_info_https;
  }

  std::optional<std::string> spot_body;
  if (has_market(g11::common_wire::MARKET_SPOT)) {
    const auto response = sources.spot_fetch();
    if (const auto *error = std::get_if<g4::NetworkError>(&response)) {
      return MetadataError{MetadataStage::SpotFetch, std::nullopt,
                           network_message(*error)};
    }
    spot_body = std::get<g4::ExchangeInfoResponse>(response).body;
  }

  std::optional<std::string> usdm_body;
  if (has_market(g11::common_wire::MARKET_USD_M_PERPETUAL)) {
    const auto response = sources.usdm_fetch();
    if (const auto *error = std::get_if<g4::NetworkError>(&response)) {
      return MetadataError{MetadataStage::UsdMFetch, std::nullopt,
                           network_message(*error)};
    }
    usdm_body = std::get<g4::ExchangeInfoResponse>(response).body;
  }

  auto canonical_keys = market_keys;
  std::sort(canonical_keys.begin(), canonical_keys.end(), g11::MarketKeyLess{});
  ProductionMetadata result;
  result.products.reserve(canonical_keys.size());
  for (const auto &key : canonical_keys) {
    if (key.venue != g11::common_wire::VENUE_BINANCE) {
      return MetadataError{MetadataStage::SpotParse, key,
                           "unsupported configured venue"};
    }
    if (key.market == g11::common_wire::MARKET_SPOT) {
      if (!g4::make_spot_transport_routes(key.symbol).has_value()) {
        return MetadataError{
            MetadataStage::SpotParse, key,
            "Spot symbol cannot be represented by a Binance route"};
      }
      const auto parsed = g4::parse_exchange_info(*spot_body, key.symbol);
      if (const auto *failure = std::get_if<g4::ProtocolError>(&parsed)) {
        return MetadataError{MetadataStage::SpotParse, key,
                             protocol_message(*failure)};
      }
      result.products.push_back(
          {key, std::get<g4::SpotMetadata>(parsed).numeric_spec});
      continue;
    }
    if (key.market == g11::common_wire::MARKET_USD_M_PERPETUAL) {
      if (!g11::make_usdm_transport_routes(key.symbol).has_value()) {
        return MetadataError{
            MetadataStage::UsdMParse, key,
            "USD-M symbol cannot be represented by a Binance route"};
      }
      const auto parsed = g11::parse_usdm_exchange_info(*usdm_body, key.symbol);
      if (const auto *failure = std::get_if<g4::ProtocolError>(&parsed)) {
        return MetadataError{MetadataStage::UsdMParse, key,
                             protocol_message(*failure)};
      }
      result.products.push_back(
          {key, std::get<g11::UsdMMetadata>(parsed).numeric_spec});
      continue;
    }
    return MetadataError{MetadataStage::SpotParse, key,
                         "unsupported configured market"};
  }
  return result;
}

std::vector<g11::ProductRuntimeSpec>
make_product_runtime_specs(ProductionMetadata metadata) {
  std::sort(metadata.products.begin(), metadata.products.end(),
            [](const auto &left, const auto &right) {
              return g11::MarketKeyLess{}(left.key, right.key);
            });
  std::vector<g11::ProductRuntimeSpec> specifications;
  specifications.reserve(metadata.products.size());
  for (auto &product : metadata.products) {
    specifications.push_back({std::move(product.key), product.numeric_spec,
                              g11::ProductRuntimeOptions{}});
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
