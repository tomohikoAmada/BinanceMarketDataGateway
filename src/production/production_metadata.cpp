#include "production/production_metadata.hpp"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string_view>
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
  std::vector<std::optional<ResolvedProductSpec>> resolved(
      canonical_keys.size());
  std::vector<std::size_t> spot_indices;
  std::vector<std::size_t> usdm_indices;
  for (std::size_t index = 0; index < canonical_keys.size(); ++index) {
    const auto &key = canonical_keys[index];
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
      spot_indices.push_back(index);
      continue;
    }
    if (key.market == runtime::common_wire::MARKET_USD_M_PERPETUAL) {
      if (!transport::make_usdm_transport_routes(key.symbol).has_value()) {
        return MetadataError{
            MetadataStage::UsdMParse, key,
            "USD-M symbol cannot be represented by a Binance route"};
      }
      usdm_indices.push_back(index);
      continue;
    }
    return MetadataError{MetadataStage::SpotParse, key,
                         "unsupported configured market"};
  }

  if (!spot_indices.empty()) {
    std::vector<std::string_view> requested;
    requested.reserve(spot_indices.size());
    for (const auto index : spot_indices) {
      requested.push_back(canonical_keys[index].symbol);
    }
    const auto parsed =
        protocol::parse_exchange_info_batch(*spot_body, requested);
    if (const auto *failure = std::get_if<protocol::ProtocolError>(&parsed)) {
      return MetadataError{MetadataStage::SpotParse,
                           canonical_keys[spot_indices.front()],
                           protocol_message(*failure)};
    }
    const auto &entries = std::get<protocol::SpotMetadataBatch>(parsed).symbols;
    for (std::size_t offset = 0; offset < entries.size(); ++offset) {
      const auto &entry = entries[offset];
      if (const auto *failure = std::get_if<protocol::ProtocolError>(&entry)) {
        return MetadataError{MetadataStage::SpotParse,
                             canonical_keys[spot_indices[offset]],
                             protocol_message(*failure)};
      }
      resolved[spot_indices[offset]] = ResolvedProductSpec{
          canonical_keys[spot_indices[offset]],
          std::get<protocol::SpotMetadata>(entry).numeric_spec};
    }
  }

  if (!usdm_indices.empty()) {
    std::vector<std::string_view> requested;
    requested.reserve(usdm_indices.size());
    for (const auto index : usdm_indices) {
      requested.push_back(canonical_keys[index].symbol);
    }
    const auto parsed =
        protocol::parse_usdm_exchange_info_batch(*usdm_body, requested);
    if (const auto *failure = std::get_if<protocol::ProtocolError>(&parsed)) {
      return MetadataError{MetadataStage::UsdMParse,
                           canonical_keys[usdm_indices.front()],
                           protocol_message(*failure)};
    }
    const auto &entries = std::get<protocol::UsdMMetadataBatch>(parsed).symbols;
    for (std::size_t offset = 0; offset < entries.size(); ++offset) {
      const auto &entry = entries[offset];
      if (const auto *failure = std::get_if<protocol::ProtocolError>(&entry)) {
        return MetadataError{MetadataStage::UsdMParse,
                             canonical_keys[usdm_indices[offset]],
                             protocol_message(*failure)};
      }
      resolved[usdm_indices[offset]] = ResolvedProductSpec{
          canonical_keys[usdm_indices[offset]],
          std::get<protocol::UsdMMetadata>(entry).numeric_spec};
    }
  }

  ProductionMetadata result;
  result.products.reserve(canonical_keys.size());
  for (auto &product : resolved) {
    result.products.push_back(std::move(*product));
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
