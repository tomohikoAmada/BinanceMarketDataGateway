#include "production_metadata.hpp"

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

ProductionMetadataResult acquire_production_metadata(MetadataSources sources) {
  if (!sources.spot_fetch) {
    sources.spot_fetch = g4::fetch_exchange_info_https;
  }
  if (!sources.usdm_fetch) {
    sources.usdm_fetch = g11::fetch_usdm_exchange_info_https;
  }

  const auto spot_response = sources.spot_fetch();
  if (const auto *error = std::get_if<g4::NetworkError>(&spot_response)) {
    return MetadataError{MetadataStage::SpotFetch, network_message(*error)};
  }
  const auto spot = g4::parse_exchange_info(
      std::get<g4::ExchangeInfoResponse>(spot_response).body);
  if (const auto *error = std::get_if<g4::ProtocolError>(&spot)) {
    return MetadataError{MetadataStage::SpotParse, protocol_message(*error)};
  }

  const auto usdm_response = sources.usdm_fetch();
  if (const auto *error = std::get_if<g4::NetworkError>(&usdm_response)) {
    return MetadataError{MetadataStage::UsdMFetch, network_message(*error)};
  }
  const auto usdm = g11::parse_usdm_exchange_info(
      std::get<g4::ExchangeInfoResponse>(usdm_response).body);
  if (const auto *error = std::get_if<g4::ProtocolError>(&usdm)) {
    return MetadataError{MetadataStage::UsdMParse, protocol_message(*error)};
  }

  return ProductionMetadata{std::get<g4::SpotMetadata>(spot).numeric_spec,
                            std::get<g11::UsdMMetadata>(usdm).numeric_spec};
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
