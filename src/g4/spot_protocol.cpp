#include "spot_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <exception>
#include <initializer_list>
#include <string>
#include <utility>

namespace binance_market_data::gateway::g4 {

namespace {

namespace common_wire = ::binance_market_data::common::v1;
using Json = nlohmann::json;

[[nodiscard]] ProtocolError error(ProtocolErrorCode code, std::string field,
                                  std::string message) {
  return {code, std::move(field), std::move(message)};
}

[[nodiscard]] bool
has_exact_keys(const Json &value,
               std::initializer_list<std::string_view> expected) {
  if (!value.is_object() || value.size() != expected.size()) {
    return false;
  }
  return std::all_of(expected.begin(), expected.end(), [&value](auto key) {
    return value.contains(std::string{key});
  });
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

[[nodiscard]] std::optional<bool> boolean_field(const Json &object,
                                                std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end() || !iterator->is_boolean()) {
    return std::nullopt;
  }
  return iterator->get<bool>();
}

[[nodiscard]] bool plain_decimal(std::string_view value,
                                 bool allow_zero) noexcept {
  if (value.empty() || value.front() == '+' || value.front() == '-') {
    return false;
  }
  const auto decimal = value.find('.');
  if (decimal != std::string_view::npos &&
      value.find('.', decimal + 1U) != std::string_view::npos) {
    return false;
  }
  const auto whole = value.substr(0U, decimal);
  const auto fraction = decimal == std::string_view::npos
                            ? std::string_view{}
                            : value.substr(decimal + 1U);
  const auto digits = [](std::string_view text) {
    return std::all_of(text.begin(), text.end(),
                       [](char value) { return value >= '0' && value <= '9'; });
  };
  if (whole.empty() || !digits(whole) ||
      (decimal != std::string_view::npos && fraction.empty()) ||
      !digits(fraction)) {
    return false;
  }
  const auto nonzero = [](char value) { return value != '0'; };
  return allow_zero || std::any_of(value.begin(), value.end(), nonzero);
}

void populate_event_metadata(market::DepthUpdate &event,
                             std::string_view connection_id,
                             g3::ClockSample received_at,
                             std::uint64_t exchange_event_time_ms) {
  auto *metadata = event.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g4-spot");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::string{connection_id});
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(exchange_event_time_ms);
  metadata->set_receive_time_utc_ns(received_at.utc_ns);
  metadata->set_receive_monotonic_ns(received_at.monotonic_ns);
}

void populate_event_metadata(market::AggTrade &event,
                             std::string_view connection_id,
                             g3::ClockSample received_at,
                             std::uint64_t exchange_event_time_ms,
                             std::uint64_t exchange_trade_time_ms) {
  auto *metadata = event.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g4-spot");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::string{connection_id});
  metadata->set_stream(common_wire::STREAM_AGG_TRADE);
  metadata->set_schema_version("agg-trade.v1");
  metadata->set_exchange_event_time_ms(exchange_event_time_ms);
  metadata->set_exchange_trade_time_ms(exchange_trade_time_ms);
  metadata->set_receive_time_utc_ns(received_at.utc_ns);
  metadata->set_receive_monotonic_ns(received_at.monotonic_ns);
}

void populate_event_metadata(market::BookTicker &event,
                             std::string_view connection_id,
                             g3::ClockSample received_at) {
  auto *metadata = event.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g4-spot");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::string{connection_id});
  metadata->set_stream(common_wire::STREAM_BOOK_TICKER);
  metadata->set_schema_version("book-ticker.v1");
  metadata->set_receive_time_utc_ns(received_at.utc_ns);
  metadata->set_receive_monotonic_ns(received_at.monotonic_ns);
}

template <typename RepeatedLevels>
[[nodiscard]] std::optional<ProtocolError>
append_levels(const Json &levels, RepeatedLevels *output,
              std::string_view field_name) {
  if (!levels.is_array()) {
    return error(ProtocolErrorCode::InvalidField, std::string{field_name},
                 "depth side must be an array");
  }
  for (const auto &level : levels) {
    if (!level.is_array() || level.size() != 2U || !level.at(0).is_string() ||
        !level.at(1).is_string()) {
      return error(ProtocolErrorCode::InvalidField, std::string{field_name},
                   "each depth level must be exactly two strings");
    }
    auto *wire_level = output->Add();
    wire_level->set_price(level.at(0).get_ref<const std::string &>());
    wire_level->set_quantity(level.at(1).get_ref<const std::string &>());
  }
  return std::nullopt;
}

[[nodiscard]] std::variant<Json, ProtocolError>
parse_json(std::string_view payload) {
  try {
    auto parsed = Json::parse(payload.begin(), payload.end(), nullptr, false);
    if (parsed.is_discarded()) {
      return error(ProtocolErrorCode::InvalidJson, "payload",
                   "payload is not valid JSON");
    }
    return parsed;
  } catch (const std::exception &) {
    return error(ProtocolErrorCode::InvalidJson, "payload",
                 "payload could not be decoded");
  }
}

} // namespace

DecimalScaleResult decimal_scale_from_quantum(std::string_view quantum) {
  if (quantum.empty()) {
    return error(ProtocolErrorCode::InvalidQuantum, "quantum",
                 "decimal quantum is empty");
  }
  if (quantum.front() == '+' || quantum.front() == '-') {
    return error(ProtocolErrorCode::InvalidQuantum, "quantum",
                 "decimal quantum must not contain a sign");
  }

  const auto decimal = quantum.find('.');
  if (decimal != std::string_view::npos &&
      quantum.find('.', decimal + 1U) != std::string_view::npos) {
    return error(ProtocolErrorCode::InvalidQuantum, "quantum",
                 "decimal quantum has multiple decimal points");
  }
  const auto whole = quantum.substr(0U, decimal);
  const auto fraction = decimal == std::string_view::npos
                            ? std::string_view{}
                            : quantum.substr(decimal + 1U);
  const auto all_digits = [](std::string_view text) {
    return std::all_of(text.begin(), text.end(),
                       [](char value) { return value >= '0' && value <= '9'; });
  };
  if (whole.empty() || !all_digits(whole) ||
      (decimal != std::string_view::npos && fraction.empty()) ||
      !all_digits(fraction)) {
    return error(ProtocolErrorCode::InvalidQuantum, "quantum",
                 "decimal quantum must use plain decimal syntax");
  }

  const bool nonzero_whole = std::any_of(
      whole.begin(), whole.end(), [](char value) { return value != '0'; });
  const auto significant_end = fraction.find_last_not_of('0');
  if (!nonzero_whole && significant_end == std::string_view::npos) {
    return error(ProtocolErrorCode::InvalidQuantum, "quantum",
                 "decimal quantum must be greater than zero");
  }
  const std::size_t scale =
      significant_end == std::string_view::npos ? 0U : significant_end + 1U;
  const auto created =
      core::DecimalScale::create(static_cast<std::uint32_t>(scale));
  if (!created.has_value()) {
    return error(ProtocolErrorCode::InvalidQuantum, "quantum",
                 "decimal quantum exceeds Projection scale capacity");
  }
  return *created;
}

SpotMetadataResult parse_exchange_info(std::string_view payload) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  const auto symbols = root.find("symbols");
  if (!root.is_object() || symbols == root.end() || !symbols->is_array() ||
      symbols->size() != 1U || !symbols->at(0).is_object()) {
    return error(ProtocolErrorCode::InvalidMarketMetadata, "symbols",
                 "exchangeInfo must contain exactly one symbol object");
  }

  const auto &symbol = symbols->at(0);
  const auto symbol_name = string_field(symbol, "symbol");
  if (!symbol_name.has_value() || *symbol_name != "BTCUSDT") {
    return error(ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                 "exchangeInfo symbol must be BTCUSDT");
  }
  const auto status = string_field(symbol, "status");
  if (!status.has_value() || *status != "TRADING") {
    return error(ProtocolErrorCode::InvalidMarketMetadata, "status",
                 "BTCUSDT must be TRADING");
  }
  const auto spot_allowed = symbol.find("isSpotTradingAllowed");
  if (spot_allowed == symbol.end() || !spot_allowed->is_boolean() ||
      !spot_allowed->get<bool>()) {
    return error(ProtocolErrorCode::InvalidMarketMetadata,
                 "isSpotTradingAllowed",
                 "BTCUSDT Spot trading must be allowed");
  }

  const auto filters = symbol.find("filters");
  if (filters == symbol.end() || !filters->is_array()) {
    return error(ProtocolErrorCode::InvalidMarketMetadata, "filters",
                 "BTCUSDT filters must be an array");
  }
  std::optional<std::string> tick_size;
  std::optional<std::string> step_size;
  for (const auto &filter : *filters) {
    if (!filter.is_object()) {
      return error(ProtocolErrorCode::InvalidMarketMetadata, "filters",
                   "each symbol filter must be an object");
    }
    const auto type = string_field(filter, "filterType");
    if (!type.has_value()) {
      return error(ProtocolErrorCode::InvalidMarketMetadata, "filterType",
                   "each symbol filter needs a string filterType");
    }
    if (*type == "PRICE_FILTER") {
      const auto value = string_field(filter, "tickSize");
      if (tick_size.has_value() || !value.has_value()) {
        return error(ProtocolErrorCode::InvalidMarketMetadata, "PRICE_FILTER",
                     "exactly one valid PRICE_FILTER is required");
      }
      tick_size = std::string{*value};
    } else if (*type == "LOT_SIZE") {
      const auto value = string_field(filter, "stepSize");
      if (step_size.has_value() || !value.has_value()) {
        return error(ProtocolErrorCode::InvalidMarketMetadata, "LOT_SIZE",
                     "exactly one valid LOT_SIZE is required");
      }
      step_size = std::string{*value};
    }
  }
  if (!tick_size.has_value() || !step_size.has_value()) {
    return error(ProtocolErrorCode::InvalidMarketMetadata, "filters",
                 "PRICE_FILTER and LOT_SIZE are both required");
  }

  const auto price_scale = decimal_scale_from_quantum(*tick_size);
  const auto quantity_scale = decimal_scale_from_quantum(*step_size);
  if (const auto *failure = std::get_if<ProtocolError>(&price_scale)) {
    return *failure;
  }
  if (const auto *failure = std::get_if<ProtocolError>(&quantity_scale)) {
    return *failure;
  }
  return SpotMetadata{std::move(*tick_size),
                      std::move(*step_size),
                      {std::get<core::DecimalScale>(price_scale),
                       std::get<core::DecimalScale>(quantity_scale)}};
}

DepthFrameResult parse_depth_frame(std::string_view payload,
                                   g3::ClockSample received_at,
                                   std::string_view connection_id) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  const auto event = string_field(root, "e");
  if (!event.has_value()) {
    return error(ProtocolErrorCode::InvalidField, "e",
                 "event type must be a string");
  }
  if (*event == "serverShutdown") {
    if (!has_exact_keys(root, {"e", "E"})) {
      return error(ProtocolErrorCode::InvalidShape, "payload",
                   "serverShutdown has an unexpected shape");
    }
    const auto event_time = unsigned_field(root, "E");
    if (!event_time.has_value()) {
      return error(ProtocolErrorCode::InvalidField, "E",
                   "serverShutdown event time must be uint64");
    }
    return ServerShutdown{*event_time};
  }
  if (*event != "depthUpdate") {
    return error(ProtocolErrorCode::WrongEvent, "e",
                 "raw stream payload is not a depthUpdate");
  }
  if (!has_exact_keys(root, {"e", "E", "s", "U", "u", "b", "a"})) {
    return error(ProtocolErrorCode::InvalidShape, "payload",
                 "depthUpdate has an unexpected shape");
  }
  const auto symbol = string_field(root, "s");
  if (!symbol.has_value() || *symbol != "BTCUSDT") {
    return error(ProtocolErrorCode::WrongSymbol, "s",
                 "depthUpdate symbol must be BTCUSDT");
  }
  const auto event_time = unsigned_field(root, "E");
  const auto first = unsigned_field(root, "U");
  const auto final = unsigned_field(root, "u");
  if (!event_time.has_value() || !first.has_value() || !final.has_value()) {
    return error(ProtocolErrorCode::InvalidField, "sequence",
                 "E, U, and u must be uint64 values");
  }
  if (connection_id.empty()) {
    return error(ProtocolErrorCode::InvalidField, "connection_id",
                 "connection_id must be non-empty");
  }

  market::DepthUpdate update;
  populate_event_metadata(update, connection_id, received_at, *event_time);
  update.set_first_update_id(*first);
  update.set_final_update_id(*final);
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

CombinedFrameResult parse_combined_event_frame(std::string_view payload,
                                               g3::ClockSample received_at,
                                               std::string_view connection_id) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  if (!has_exact_keys(root, {"stream", "data"})) {
    return error(ProtocolErrorCode::InvalidShape, "payload",
                 "combined stream envelope has an unexpected shape");
  }
  const auto stream = string_field(root, "stream");
  const auto data = root.find("data");
  if (!stream.has_value() || data == root.end() || !data->is_object()) {
    return error(ProtocolErrorCode::InvalidField, "stream",
                 "combined stream and data must be valid");
  }
  if (connection_id.empty()) {
    return error(ProtocolErrorCode::InvalidField, "connection_id",
                 "connection_id must be non-empty");
  }

  if (*stream == "btcusdt@depth@100ms") {
    const auto nested =
        parse_depth_frame(data->dump(), received_at, connection_id);
    if (const auto *shutdown = std::get_if<ServerShutdown>(&nested)) {
      return *shutdown;
    }
    if (const auto *failure = std::get_if<ProtocolError>(&nested)) {
      return *failure;
    }
    return NormalizedSpotEvent{
        std::get<market::DepthUpdate>(std::move(nested))};
  }

  if (*stream == "btcusdt@aggTrade") {
    if (!has_exact_keys(
            *data, {"e", "E", "s", "a", "p", "q", "f", "l", "T", "m", "M"})) {
      return error(ProtocolErrorCode::InvalidShape, "data",
                   "aggTrade has an unexpected shape");
    }
    const auto event = string_field(*data, "e");
    const auto symbol = string_field(*data, "s");
    const auto aggregate_id = unsigned_field(*data, "a");
    const auto price = string_field(*data, "p");
    const auto quantity = string_field(*data, "q");
    const auto first_trade_id = unsigned_field(*data, "f");
    const auto last_trade_id = unsigned_field(*data, "l");
    const auto event_time = unsigned_field(*data, "E");
    const auto trade_time = unsigned_field(*data, "T");
    const auto buyer_is_maker = boolean_field(*data, "m");
    const auto best_match = boolean_field(*data, "M");
    if (!event.has_value() || *event != "aggTrade") {
      return error(ProtocolErrorCode::WrongEvent, "e",
                   "combined stream payload is not aggTrade");
    }
    if (!symbol.has_value() || *symbol != "BTCUSDT") {
      return error(ProtocolErrorCode::WrongSymbol, "s",
                   "aggTrade symbol must be BTCUSDT");
    }
    if (!aggregate_id.has_value() || !price.has_value() ||
        !plain_decimal(*price, false) || !quantity.has_value() ||
        !plain_decimal(*quantity, false) || !first_trade_id.has_value() ||
        !last_trade_id.has_value() || !event_time.has_value() ||
        !trade_time.has_value() || !buyer_is_maker.has_value() ||
        !best_match.has_value()) {
      return error(ProtocolErrorCode::InvalidField, "aggTrade",
                   "aggTrade fields have invalid types or decimal values");
    }
    market::AggTrade trade;
    populate_event_metadata(trade, connection_id, received_at, *event_time,
                            *trade_time);
    trade.set_aggregate_trade_id(*aggregate_id);
    trade.set_price(std::string{*price});
    trade.set_quantity(std::string{*quantity});
    trade.set_first_trade_id(*first_trade_id);
    trade.set_last_trade_id(*last_trade_id);
    trade.set_trade_time_ms(*trade_time);
    trade.set_buyer_is_maker(*buyer_is_maker);
    return NormalizedSpotEvent{std::move(trade)};
  }

  if (*stream == "btcusdt@bookTicker") {
    if (!has_exact_keys(*data, {"u", "s", "b", "B", "a", "A"})) {
      return error(ProtocolErrorCode::InvalidShape, "data",
                   "bookTicker has an unexpected shape");
    }
    const auto update_id = unsigned_field(*data, "u");
    const auto symbol = string_field(*data, "s");
    const auto bid_price = string_field(*data, "b");
    const auto bid_quantity = string_field(*data, "B");
    const auto ask_price = string_field(*data, "a");
    const auto ask_quantity = string_field(*data, "A");
    if (!symbol.has_value() || *symbol != "BTCUSDT") {
      return error(ProtocolErrorCode::WrongSymbol, "s",
                   "bookTicker symbol must be BTCUSDT");
    }
    if (!update_id.has_value() || !bid_price.has_value() ||
        !plain_decimal(*bid_price, false) || !bid_quantity.has_value() ||
        !plain_decimal(*bid_quantity, true) || !ask_price.has_value() ||
        !plain_decimal(*ask_price, false) || !ask_quantity.has_value() ||
        !plain_decimal(*ask_quantity, true)) {
      return error(ProtocolErrorCode::InvalidField, "bookTicker",
                   "bookTicker fields have invalid types or decimal values");
    }
    market::BookTicker ticker;
    populate_event_metadata(ticker, connection_id, received_at);
    ticker.set_update_id(*update_id);
    ticker.set_best_bid_price(std::string{*bid_price});
    ticker.set_best_bid_quantity(std::string{*bid_quantity});
    ticker.set_best_ask_price(std::string{*ask_price});
    ticker.set_best_ask_quantity(std::string{*ask_quantity});
    return NormalizedSpotEvent{std::move(ticker)};
  }

  return error(ProtocolErrorCode::WrongEvent, "stream",
               "combined stream is not configured for G9");
}

DepthSnapshotResult parse_depth_snapshot(std::string_view payload,
                                         g3::ClockSample received_at,
                                         std::string_view request_id) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  if (!has_exact_keys(root, {"lastUpdateId", "bids", "asks"})) {
    return error(ProtocolErrorCode::InvalidShape, "payload",
                 "depth snapshot has an unexpected shape");
  }
  const auto last_update_id = unsigned_field(root, "lastUpdateId");
  if (!last_update_id.has_value() || request_id.empty()) {
    return error(ProtocolErrorCode::InvalidField, "lastUpdateId",
                 "lastUpdateId must be uint64 and request_id non-empty");
  }

  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g4-spot");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id(std::string{request_id});
  snapshot.set_last_update_id(*last_update_id);
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
spot_stream_symbol(std::string_view canonical_symbol) {
  if (canonical_symbol != "BTCUSDT") {
    return std::nullopt;
  }
  return std::string{"btcusdt"};
}

} // namespace binance_market_data::gateway::g4
