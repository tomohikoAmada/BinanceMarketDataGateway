#pragma once

#include "spot_protocol.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace binance_market_data::gateway::g11 {

namespace core = projection::v1;
namespace market = ::binance_market_data::market::v1;

struct UsdMMetadata final {
  std::string tick_size;
  std::string step_size;
  core::NumericSpec numeric_spec;
};

using UsdMMetadataResult = std::variant<UsdMMetadata, g4::ProtocolError>;
using UsdMDepthFrameResult = g4::DepthFrameResult;
using UsdMDepthSnapshotResult = g4::DepthSnapshotResult;

[[nodiscard]] UsdMMetadataResult
parse_usdm_exchange_info(std::string_view payload);

[[nodiscard]] UsdMDepthFrameResult
parse_usdm_depth_frame(std::string_view payload, g3::ClockSample received_at,
                       std::string_view connection_id);

[[nodiscard]] UsdMDepthSnapshotResult
parse_usdm_depth_snapshot(std::string_view payload, g3::ClockSample received_at,
                          std::string_view request_id);

[[nodiscard]] std::optional<std::string>
usdm_stream_symbol(std::string_view canonical_symbol);

} // namespace binance_market_data::gateway::g11
