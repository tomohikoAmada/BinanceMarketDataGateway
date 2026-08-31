#include "usdm_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <exception>
#include <initializer_list>
#include <optional>
#include <string>
#include <utility>

namespace binance_market_data::gateway::g11 {

namespace {

namespace common_wire = ::binance_market_data::common::v1;
using Json = nlohmann::json;

[[nodiscard]] g4::ProtocolError error(g4::ProtocolErrorCode code,
                                      std::string field, std::string message) {
  return {code, std::move(field), std::move(message)};
}

[[nodiscard]] std::variant<Json, g4::ProtocolError>
parse_json(std::string_view payload) {
  try {
    auto parsed = Json::parse(payload.begin(), payload.end(), nullptr, false);
    if (parsed.is_discarded()) {
      return error(g4::ProtocolErrorCode::InvalidJson, "payload",
                   "payload is not valid JSON");
    }
    return parsed;
  } catch (const std::exception &) {
    return error(g4::ProtocolErrorCode::InvalidJson, "payload",
                 "payload could not be decoded");
  }
}

[[nodiscard]] std::optional<std::uint64_t>
unsigned_field(const Json &object, std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end() || !iterator->is_number_unsigned()) {
    return std::nullopt;
  }
  return iterator->get<std::uint64_t>();
}

[[nodiscard]] std::optional<std::string_view>
string_field(const Json &object, std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end() || !iterator->is_string()) {
    return std::nullopt;
  }
  return iterator->get_ref<const std::string &>();
}

[[nodiscard]] bool
has_only_keys(const Json &value,
              std::initializer_list<std::string_view> allowed) {
  if (!value.is_object()) {
    return false;
  }
  for (auto item = value.begin(); item != value.end(); ++item) {
    if (std::none_of(allowed.begin(), allowed.end(),
                     [&item](auto key) { return item.key() == key; })) {
      return false;
    }
  }
  return true;
}

template <typename RepeatedLevels>
[[nodiscard]] std::optional<g4::ProtocolError>
append_levels(const Json &levels, RepeatedLevels *output,
              std::string_view field_name) {
  if (!levels.is_array()) {
    return error(g4::ProtocolErrorCode::InvalidField, std::string{field_name},
                 "depth side must be an array");
  }
  for (const auto &level : levels) {
    if (!level.is_array() || level.size() != 2U || !level.at(0).is_string() ||
        !level.at(1).is_string()) {
      return error(g4::ProtocolErrorCode::InvalidField, std::string{field_name},
                   "each depth level must be exactly two strings");
    }
    auto *wire_level = output->Add();
    wire_level->set_price(level.at(0).get_ref<const std::string &>());
    wire_level->set_quantity(level.at(1).get_ref<const std::string &>());
  }
  return std::nullopt;
}

void populate_depth_metadata(market::DepthUpdate &event,
                             std::string_view connection_id,
                             g3::ClockSample received_at,
                             std::uint64_t exchange_event_time_ms,
                             std::optional<std::uint64_t> transaction_time_ms) {
  auto *metadata = event.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_USD_M_PERPETUAL);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g11-usdm");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::string{connection_id});
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(exchange_event_time_ms);
  if (transaction_time_ms.has_value()) {
    metadata->set_exchange_trade_time_ms(*transaction_time_ms);
  }
  metadata->set_receive_time_utc_ns(received_at.utc_ns);
  metadata->set_receive_monotonic_ns(received_at.monotonic_ns);
}

} // namespace

UsdMMetadataResult parse_usdm_exchange_info(std::string_view payload) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  const auto symbols = root.find("symbols");
  if (!root.is_object() || symbols == root.end() || !symbols->is_array()) {
    return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "symbols",
                 "exchangeInfo symbols must be an array");
  }

  const Json *selected = nullptr;
  for (const auto &candidate : *symbols) {
    if (!candidate.is_object()) {
      return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "symbols",
                   "each exchangeInfo symbol must be an object");
    }
    const auto symbol = string_field(candidate, "symbol");
    if (symbol.has_value() && *symbol == "BTCUSDT") {
      if (selected != nullptr) {
        return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                     "exchangeInfo contains duplicate BTCUSDT entries");
      }
      selected = &candidate;
    }
  }
  if (selected == nullptr) {
    return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                 "exchangeInfo must contain BTCUSDT");
  }

  const auto contract_type = string_field(*selected, "contractType");
  if (!contract_type.has_value() || *contract_type != "PERPETUAL") {
    return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "contractType",
                 "BTCUSDT contractType must be PERPETUAL");
  }
  const auto status = string_field(*selected, "status");
  if (!status.has_value() || *status != "TRADING") {
    return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "status",
                 "BTCUSDT USD-M perpetual must be TRADING");
  }

  const auto filters = selected->find("filters");
  if (filters == selected->end() || !filters->is_array()) {
    return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "filters",
                 "BTCUSDT filters must be an array");
  }
  std::optional<std::string> tick_size;
  std::optional<std::string> step_size;
  for (const auto &filter : *filters) {
    if (!filter.is_object()) {
      return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "filters",
                   "each symbol filter must be an object");
    }
    const auto type = string_field(filter, "filterType");
    if (!type.has_value()) {
      return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "filterType",
                   "each symbol filter needs a string filterType");
    }
    if (*type == "PRICE_FILTER") {
      const auto value = string_field(filter, "tickSize");
      if (tick_size.has_value() || !value.has_value()) {
        return error(g4::ProtocolErrorCode::InvalidMarketMetadata,
                     "PRICE_FILTER",
                     "exactly one valid PRICE_FILTER is required");
      }
      tick_size = std::string{*value};
    } else if (*type == "LOT_SIZE") {
      const auto value = string_field(filter, "stepSize");
      if (step_size.has_value() || !value.has_value()) {
        return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "LOT_SIZE",
                     "exactly one valid LOT_SIZE is required");
      }
      step_size = std::string{*value};
    }
  }
  if (!tick_size.has_value() || !step_size.has_value()) {
    return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "filters",
                 "PRICE_FILTER and LOT_SIZE are both required");
  }

  const auto price_scale = g4::decimal_scale_from_quantum(*tick_size);
  const auto quantity_scale = g4::decimal_scale_from_quantum(*step_size);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&price_scale)) {
    return *failure;
  }
  if (const auto *failure = std::get_if<g4::ProtocolError>(&quantity_scale)) {
    return *failure;
  }
  return UsdMMetadata{std::move(*tick_size),
                      std::move(*step_size),
                      {std::get<core::DecimalScale>(price_scale),
                       std::get<core::DecimalScale>(quantity_scale)}};
}

UsdMDepthFrameResult parse_usdm_depth_frame(std::string_view payload,
                                            g3::ClockSample received_at,
                                            std::string_view connection_id) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  const auto event = string_field(root, "e");
  if (!event.has_value()) {
    return error(g4::ProtocolErrorCode::InvalidField, "e",
                 "event type must be a string");
  }
  if (*event == "serverShutdown") {
    if (!root.is_object() || root.size() != 2U || !root.contains("E")) {
      return error(g4::ProtocolErrorCode::InvalidShape, "payload",
                   "serverShutdown has an unexpected shape");
    }
    const auto event_time = unsigned_field(root, "E");
    if (!event_time.has_value()) {
      return error(g4::ProtocolErrorCode::InvalidField, "E",
                   "serverShutdown event time must be uint64");
    }
    return g4::ServerShutdown{*event_time};
  }
  if (*event != "depthUpdate") {
    return error(g4::ProtocolErrorCode::WrongEvent, "e",
                 "USD-M raw stream payload is not a depthUpdate");
  }
  if (!has_only_keys(
          root, {"e", "E", "T", "s", "U", "u", "pu", "b", "a", "ps", "st"}) ||
      !root.contains("E") || !root.contains("s") || !root.contains("U") ||
      !root.contains("u") || !root.contains("b") || !root.contains("a")) {
    return error(g4::ProtocolErrorCode::InvalidShape, "payload",
                 "USD-M depthUpdate has an unexpected shape");
  }
  const auto symbol = string_field(root, "s");
  if (!symbol.has_value() || *symbol != "BTCUSDT") {
    return error(g4::ProtocolErrorCode::WrongSymbol, "s",
                 "USD-M depthUpdate symbol must be BTCUSDT");
  }
  if (root.contains("ps")) {
    const auto pair = string_field(root, "ps");
    if (!pair.has_value() || *pair != "BTCUSDT") {
      return error(g4::ProtocolErrorCode::WrongSymbol, "ps",
                   "USD-M depthUpdate pair must be BTCUSDT");
    }
  }
  if (root.contains("st")) {
    const auto symbol_type = unsigned_field(root, "st");
    if (!symbol_type.has_value() || *symbol_type != 1U) {
      return error(g4::ProtocolErrorCode::InvalidMarketMetadata, "st",
                   "USD-M depthUpdate symbol type must be 1");
    }
  }

  const auto event_time = unsigned_field(root, "E");
  const auto first = unsigned_field(root, "U");
  const auto final = unsigned_field(root, "u");
  if (!event_time.has_value() || !first.has_value() || !final.has_value()) {
    return error(g4::ProtocolErrorCode::InvalidField, "sequence",
                 "E, U, and u must be uint64 values");
  }
  std::optional<std::uint64_t> transaction_time;
  if (root.contains("T")) {
    transaction_time = unsigned_field(root, "T");
    if (!transaction_time.has_value()) {
      return error(g4::ProtocolErrorCode::InvalidField, "T",
                   "USD-M transaction time must be uint64 when present");
    }
  }
  std::optional<std::uint64_t> previous_final;
  if (root.contains("pu")) {
    previous_final = unsigned_field(root, "pu");
    if (!previous_final.has_value()) {
      return error(
          g4::ProtocolErrorCode::InvalidField, "pu",
          "USD-M previous final update ID must be uint64 when present");
    }
  }
  if (connection_id.empty()) {
    return error(g4::ProtocolErrorCode::InvalidField, "connection_id",
                 "connection_id must be non-empty");
  }

  market::DepthUpdate update;
  populate_depth_metadata(update, connection_id, received_at, *event_time,
                          transaction_time);
  update.set_first_update_id(*first);
  update.set_final_update_id(*final);
  if (previous_final.has_value()) {
    update.set_previous_final_update_id(*previous_final);
  }
  if (const auto failure =
          append_levels(root.at("b"), update.mutable_bids(), "b")) {
    return *failure;
  }
  if (const auto failure =
          append_levels(root.at("a"), update.mutable_asks(), "a")) {
    return *failure;
  }
  return update;
}

UsdMDepthSnapshotResult parse_usdm_depth_snapshot(std::string_view payload,
                                                  g3::ClockSample received_at,
                                                  std::string_view request_id) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  if (!has_only_keys(root, {"lastUpdateId", "E", "T", "bids", "asks"}) ||
      !root.contains("lastUpdateId") || !root.contains("bids") ||
      !root.contains("asks")) {
    return error(g4::ProtocolErrorCode::InvalidShape, "payload",
                 "USD-M depth snapshot has an unexpected shape");
  }
  const auto last_update_id = unsigned_field(root, "lastUpdateId");
  if (!last_update_id.has_value() || request_id.empty()) {
    return error(g4::ProtocolErrorCode::InvalidField, "lastUpdateId",
                 "lastUpdateId must be uint64 and request_id non-empty");
  }
  if (root.contains("E") && !unsigned_field(root, "E").has_value()) {
    return error(g4::ProtocolErrorCode::InvalidField, "E",
                 "snapshot message output time must be uint64 when present");
  }
  std::optional<std::uint64_t> transaction_time;
  if (root.contains("T")) {
    transaction_time = unsigned_field(root, "T");
    if (!transaction_time.has_value()) {
      return error(g4::ProtocolErrorCode::InvalidField, "T",
                   "snapshot transaction time must be uint64 when present");
    }
  }

  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_USD_M_PERPETUAL);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g11-usdm");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id(std::string{request_id});
  snapshot.set_last_update_id(*last_update_id);
  if (transaction_time.has_value()) {
    snapshot.set_exchange_transaction_time_ms(*transaction_time);
  }
  snapshot.set_receive_time_utc_ns(received_at.utc_ns);
  snapshot.set_receive_monotonic_ns(received_at.monotonic_ns);
  if (const auto failure =
          append_levels(root.at("bids"), snapshot.mutable_bids(), "bids")) {
    return *failure;
  }
  if (const auto failure =
          append_levels(root.at("asks"), snapshot.mutable_asks(), "asks")) {
    return *failure;
  }
  return snapshot;
}

std::optional<std::string>
usdm_stream_symbol(std::string_view canonical_symbol) {
  if (canonical_symbol != "BTCUSDT") {
    return std::nullopt;
  }
  return std::string{"btcusdt"};
}

} // namespace binance_market_data::gateway::g11
