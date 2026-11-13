#pragma once

#include "protocol/spot_protocol.hpp"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::protocol {

namespace core = projection::v1;
namespace market = ::binance_market_data::market::v1;

struct UsdMMetadata final {
  std::string tick_size;
  std::string step_size;
  core::NumericSpec numeric_spec;
};

using UsdMMetadataResult = std::variant<UsdMMetadata, protocol::ProtocolError>;
struct UsdMMetadataBatch final {
  std::vector<UsdMMetadataResult> symbols;
};
using UsdMMetadataBatchResult =
    std::variant<UsdMMetadataBatch, protocol::ProtocolError>;
using UsdMDepthFrameResult = protocol::DepthFrameResult;
using UsdMDepthSnapshotResult = protocol::DepthSnapshotResult;
using UsdMPublicFrameResult = protocol::CombinedFrameResult;
using UsdMMarketFrameResult = protocol::CombinedFrameResult;

[[nodiscard]] UsdMMetadataResult
parse_usdm_exchange_info(std::string_view payload);

[[nodiscard]] UsdMMetadataBatchResult parse_usdm_exchange_info_batch(
    std::string_view payload,
    std::span<const std::string_view> requested_symbols);

[[nodiscard]] UsdMMetadataResult
parse_usdm_exchange_info(std::string_view payload,
                         std::string_view requested_symbol);

[[nodiscard]] UsdMDepthFrameResult
parse_usdm_depth_frame(std::string_view payload,
                       runtime::ClockSample received_at,
                       std::string_view connection_id);

[[nodiscard]] UsdMDepthFrameResult parse_usdm_depth_frame(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view connection_id, std::string_view requested_symbol);

[[nodiscard]] UsdMDepthSnapshotResult
parse_usdm_depth_snapshot(std::string_view payload,
                          runtime::ClockSample received_at,
                          std::string_view request_id);

[[nodiscard]] UsdMDepthSnapshotResult parse_usdm_depth_snapshot(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view request_id, std::string_view requested_symbol);

[[nodiscard]] UsdMPublicFrameResult parse_usdm_public_event_frame(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view connection_id, std::string_view requested_symbol);

[[nodiscard]] UsdMMarketFrameResult parse_usdm_market_event_frame(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view connection_id, std::string_view requested_symbol);

[[nodiscard]] std::optional<std::string>
usdm_stream_symbol(std::string_view canonical_symbol);

} // namespace binance_market_data::gateway::protocol
