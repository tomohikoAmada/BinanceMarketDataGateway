#pragma once

#include "market_runtime.hpp"

#include <binance_market_data/market/v1/market_events.pb.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace binance_market_data::gateway::g4 {

namespace core = projection::v1;
namespace market = ::binance_market_data::market::v1;

enum class ProtocolErrorCode : std::uint8_t {
  InvalidJson,
  InvalidShape,
  InvalidField,
  WrongEvent,
  WrongSymbol,
  InvalidQuantum,
  InvalidMarketMetadata,
};

struct ProtocolError final {
  ProtocolErrorCode code;
  std::string field;
  std::string message;

  friend bool operator==(const ProtocolError &,
                         const ProtocolError &) = default;
};

struct SpotMetadata final {
  std::string tick_size;
  std::string step_size;
  core::NumericSpec numeric_spec;
};

struct ServerShutdown final {
  std::uint64_t exchange_event_time_ms;
};

using DecimalScaleResult = std::variant<core::DecimalScale, ProtocolError>;
using SpotMetadataResult = std::variant<SpotMetadata, ProtocolError>;
using DepthFrameResult =
    std::variant<market::DepthUpdate, ServerShutdown, ProtocolError>;
using NormalizedSpotEvent =
    std::variant<market::DepthUpdate, market::AggTrade, market::BookTicker>;
using CombinedFrameResult =
    std::variant<NormalizedSpotEvent, ServerShutdown, ProtocolError>;
using DepthSnapshotResult =
    std::variant<market::ExchangeDepthSnapshot, ProtocolError>;

[[nodiscard]] DecimalScaleResult
decimal_scale_from_quantum(std::string_view quantum);

[[nodiscard]] SpotMetadataResult parse_exchange_info(std::string_view payload);

[[nodiscard]] DepthFrameResult
parse_depth_frame(std::string_view payload, g3::ClockSample received_at,
                  std::string_view connection_id);

[[nodiscard]] CombinedFrameResult
parse_combined_event_frame(std::string_view payload,
                           g3::ClockSample received_at,
                           std::string_view connection_id);

[[nodiscard]] DepthSnapshotResult
parse_depth_snapshot(std::string_view payload, g3::ClockSample received_at,
                     std::string_view request_id);

[[nodiscard]] std::optional<std::string>
spot_stream_symbol(std::string_view canonical_symbol);

} // namespace binance_market_data::gateway::g4
