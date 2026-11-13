#include "protocol/usdm_protocol.hpp"

#include "protocol/json_helpers.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace binance_market_data::gateway::protocol {

namespace {

namespace common_wire = ::binance_market_data::common::v1;
using detail::append_levels;
using detail::boolean_field;
using detail::error;
using detail::has_required_keys;
using detail::Json;
using detail::parse_json;
using detail::plain_decimal;
using detail::string_field;
using detail::unsigned_field;

template <typename Event>
void populate_metadata(Event &event, common_wire::Stream stream,
                       std::string_view schema_version, std::string_view symbol,
                       std::string_view connection_id,
                       runtime::ClockSample received_at,
                       std::uint64_t exchange_event_time_ms) {
  auto *metadata = event.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_USD_M_PERPETUAL);
  metadata->set_symbol(std::string{symbol});
  metadata->set_producer("gateway-g5-usdm");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::string{connection_id});
  metadata->set_stream(stream);
  metadata->set_schema_version(std::string{schema_version});
  metadata->set_exchange_event_time_ms(exchange_event_time_ms);
  metadata->set_receive_time_utc_ns(received_at.utc_ns);
  metadata->set_receive_monotonic_ns(received_at.monotonic_ns);
}

void populate_depth_metadata(market::DepthUpdate &event,
                             std::string_view symbol,
                             std::string_view connection_id,
                             runtime::ClockSample received_at,
                             std::uint64_t exchange_event_time_ms,
                             std::optional<std::uint64_t> transaction_time_ms) {
  auto *metadata = event.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_USD_M_PERPETUAL);
  metadata->set_symbol(std::string{symbol});
  metadata->set_producer("gateway-g11-usdm");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(std::string{connection_id});
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(exchange_event_time_ms);
  if (transaction_time_ms.has_value()) {
    metadata->set_exchange_transaction_time_ms(*transaction_time_ms);
  }
  metadata->set_receive_time_utc_ns(received_at.utc_ns);
  metadata->set_receive_monotonic_ns(received_at.monotonic_ns);
}

} // namespace

namespace {

[[nodiscard]] UsdMMetadataResult parse_selected_metadata(const Json &selected) {
  const auto contract_type = string_field(selected, "contractType");
  if (!contract_type.has_value() || *contract_type != "PERPETUAL") {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                 "contractType",
                 "requested USD-M symbol contractType must be PERPETUAL");
  }
  const auto status = string_field(selected, "status");
  if (!status.has_value() || *status != "TRADING") {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "status",
                 "requested USD-M perpetual symbol must be TRADING");
  }

  const auto filters = selected.find("filters");
  if (filters == selected.end() || !filters->is_array()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "filters",
                 "requested USD-M symbol filters must be an array");
  }
  std::optional<std::string> tick_size;
  std::optional<std::string> step_size;
  for (const auto &filter : *filters) {
    if (!filter.is_object()) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                   "filters", "each symbol filter must be an object");
    }
    const auto type = string_field(filter, "filterType");
    if (!type.has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                   "filterType",
                   "each symbol filter needs a string filterType");
    }
    if (*type == "PRICE_FILTER") {
      const auto value = string_field(filter, "tickSize");
      if (tick_size.has_value() || !value.has_value()) {
        return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                     "PRICE_FILTER",
                     "exactly one valid PRICE_FILTER is required");
      }
      tick_size = std::string{*value};
    } else if (*type == "LOT_SIZE") {
      const auto value = string_field(filter, "stepSize");
      if (step_size.has_value() || !value.has_value()) {
        return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                     "LOT_SIZE", "exactly one valid LOT_SIZE is required");
      }
      step_size = std::string{*value};
    }
  }
  if (!tick_size.has_value() || !step_size.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "filters",
                 "PRICE_FILTER and LOT_SIZE are both required");
  }

  const auto price_scale = protocol::decimal_scale_from_quantum(*tick_size);
  const auto quantity_scale = protocol::decimal_scale_from_quantum(*step_size);
  if (const auto *failure =
          std::get_if<protocol::ProtocolError>(&price_scale)) {
    return *failure;
  }
  if (const auto *failure =
          std::get_if<protocol::ProtocolError>(&quantity_scale)) {
    return *failure;
  }
  return UsdMMetadata{std::move(*tick_size),
                      std::move(*step_size),
                      {std::get<core::DecimalScale>(price_scale),
                       std::get<core::DecimalScale>(quantity_scale)}};
}

} // namespace

UsdMMetadataBatchResult parse_usdm_exchange_info_batch(
    std::string_view payload,
    std::span<const std::string_view> requested_symbols) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<protocol::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  if (!root.is_object()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbols",
                 "exchangeInfo symbols must be an array");
  }
  const auto symbols = root.find("symbols");
  if (symbols == root.end() || !symbols->is_array()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbols",
                 "exchangeInfo symbols must be an array");
  }

  std::unordered_map<std::string_view, std::size_t> requested;
  requested.reserve(requested_symbols.size());
  for (std::size_t index = 0; index < requested_symbols.size(); ++index) {
    if (!requested.emplace(requested_symbols[index], index).second) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                   "requested_symbols", "requested symbols must be unique");
    }
  }

  std::vector<const Json *> selected(requested_symbols.size(), nullptr);
  for (const auto &candidate : *symbols) {
    if (!candidate.is_object()) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata,
                   "symbols", "each exchangeInfo symbol must be an object");
    }
    const auto symbol = string_field(candidate, "symbol");
    if (!symbol.has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                   "each exchangeInfo symbol needs a string symbol");
    }
    const auto match = requested.find(*symbol);
    if (match == requested.end()) {
      continue;
    }
    auto &entry = selected[match->second];
    if (entry != nullptr) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                   "exchangeInfo contains a duplicate requested symbol");
    }
    entry = &candidate;
  }

  UsdMMetadataBatch result;
  result.symbols.reserve(requested_symbols.size());
  for (std::size_t index = 0; index < requested_symbols.size(); ++index) {
    if (!usdm_stream_symbol(requested_symbols[index]).has_value()) {
      result.symbols.push_back(
          error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                "requested USD-M symbol cannot be represented by a Binance "
                "route"));
      continue;
    }
    if (selected[index] == nullptr) {
      result.symbols.push_back(
          error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                "exchangeInfo must contain the requested USD-M symbol"));
      continue;
    }
    result.symbols.push_back(parse_selected_metadata(*selected[index]));
  }
  return result;
}

UsdMMetadataResult parse_usdm_exchange_info(std::string_view payload,
                                            std::string_view requested_symbol) {
  const std::array<std::string_view, 1> requested{requested_symbol};
  auto decoded = parse_usdm_exchange_info_batch(payload, requested);
  if (const auto *failure = std::get_if<protocol::ProtocolError>(&decoded)) {
    return *failure;
  }
  return std::move(std::get<UsdMMetadataBatch>(decoded).symbols.front());
}

UsdMMetadataResult parse_usdm_exchange_info(std::string_view payload) {
  return parse_usdm_exchange_info(payload, "BTCUSDT");
}

UsdMDepthFrameResult
parse_usdm_depth_object(const Json &root, runtime::ClockSample received_at,
                        std::string_view connection_id,
                        std::string_view requested_symbol) {
  if (!usdm_stream_symbol(requested_symbol).has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                 "requested USD-M symbol cannot be represented by a Binance "
                 "route");
  }
  const auto event = string_field(root, "e");
  if (!event.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "e",
                 "event type must be a string");
  }
  if (*event == "serverShutdown") {
    if (!has_required_keys(root, {"e", "E"})) {
      return error(protocol::ProtocolErrorCode::InvalidShape, "payload",
                   "serverShutdown has an unexpected shape");
    }
    const auto event_time = unsigned_field(root, "E");
    if (!event_time.has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidField, "E",
                   "serverShutdown event time must be uint64");
    }
    return protocol::ServerShutdown{*event_time};
  }
  if (*event != "depthUpdate") {
    return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                 "USD-M raw stream payload is not a depthUpdate");
  }
  if (!has_required_keys(root, {"e", "E", "s", "U", "u", "b", "a"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "payload",
                 "USD-M depthUpdate has an unexpected shape");
  }
  const auto symbol = string_field(root, "s");
  if (!symbol.has_value() || *symbol != requested_symbol) {
    return error(
        protocol::ProtocolErrorCode::WrongSymbol, "s",
        "USD-M depthUpdate symbol does not match the configured symbol");
  }
  if (root.contains("ps")) {
    if (!string_field(root, "ps").has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidField, "ps",
                   "USD-M depthUpdate pair must be a string when present");
    }
  }
  if (root.contains("st")) {
    const auto symbol_type = unsigned_field(root, "st");
    if (!symbol_type.has_value() || *symbol_type != 1U) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "st",
                   "USD-M depthUpdate symbol type must be 1");
    }
  }

  const auto event_time = unsigned_field(root, "E");
  const auto first = unsigned_field(root, "U");
  const auto final = unsigned_field(root, "u");
  if (!event_time.has_value() || !first.has_value() || !final.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "sequence",
                 "E, U, and u must be uint64 values");
  }
  std::optional<std::uint64_t> transaction_time;
  if (root.contains("T")) {
    transaction_time = unsigned_field(root, "T");
    if (!transaction_time.has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidField, "T",
                   "USD-M transaction time must be uint64 when present");
    }
  }
  std::optional<std::uint64_t> previous_final;
  if (root.contains("pu")) {
    previous_final = unsigned_field(root, "pu");
    if (!previous_final.has_value()) {
      return error(
          protocol::ProtocolErrorCode::InvalidField, "pu",
          "USD-M previous final update ID must be uint64 when present");
    }
  }
  if (connection_id.empty()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "connection_id",
                 "connection_id must be non-empty");
  }

  market::DepthUpdate update;
  populate_depth_metadata(update, requested_symbol, connection_id, received_at,
                          *event_time, transaction_time);
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

[[nodiscard]] UsdMMarketFrameResult
parse_book_ticker_object(const Json &data, runtime::ClockSample received_at,
                         std::string_view connection_id,
                         std::string_view requested_symbol) {
  if (!has_required_keys(data, {"e", "u", "s", "b", "B", "a", "A", "E", "T"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "data",
                 "USD-M bookTicker has an unexpected shape");
  }
  const auto event = string_field(data, "e");
  const auto symbol = string_field(data, "s");
  const auto update_id = unsigned_field(data, "u");
  const auto bid_price = string_field(data, "b");
  const auto bid_quantity = string_field(data, "B");
  const auto ask_price = string_field(data, "a");
  const auto ask_quantity = string_field(data, "A");
  const auto event_time = unsigned_field(data, "E");
  const auto transaction_time = unsigned_field(data, "T");
  if (!event.has_value() || *event != "bookTicker") {
    return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                 "USD-M public payload is not bookTicker");
  }
  if (!symbol.has_value() || *symbol != requested_symbol) {
    return error(protocol::ProtocolErrorCode::WrongSymbol, "s",
                 "bookTicker symbol does not match the configured symbol");
  }
  if (!update_id.has_value() || !event_time.has_value() ||
      !transaction_time.has_value() || !bid_price.has_value() ||
      !plain_decimal(*bid_price, false) || !bid_quantity.has_value() ||
      !plain_decimal(*bid_quantity, true) || !ask_price.has_value() ||
      !plain_decimal(*ask_price, false) || !ask_quantity.has_value() ||
      !plain_decimal(*ask_quantity, true)) {
    return error(protocol::ProtocolErrorCode::InvalidField, "bookTicker",
                 "bookTicker fields have invalid types or decimal values");
  }
  if (data.contains("st")) {
    const auto symbol_type = unsigned_field(data, "st");
    if (!symbol_type.has_value() || *symbol_type != 1U) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "st",
                   "bookTicker symbol type must be 1");
    }
  }
  market::BookTicker ticker;
  populate_metadata(ticker, common_wire::STREAM_BOOK_TICKER, "book-ticker.v1",
                    requested_symbol, connection_id, received_at, *event_time);
  ticker.mutable_metadata()->set_exchange_transaction_time_ms(
      *transaction_time);
  ticker.set_update_id(*update_id);
  ticker.set_best_bid_price(std::string{*bid_price});
  ticker.set_best_bid_quantity(std::string{*bid_quantity});
  ticker.set_best_ask_price(std::string{*ask_price});
  ticker.set_best_ask_quantity(std::string{*ask_quantity});
  return ticker;
}

[[nodiscard]] UsdMMarketFrameResult
parse_agg_trade_object(const Json &data, runtime::ClockSample received_at,
                       std::string_view connection_id,
                       std::string_view requested_symbol) {
  if (!has_required_keys(data,
                         {"e", "E", "s", "a", "p", "q", "f", "l", "T", "m"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "data",
                 "USD-M aggTrade has an unexpected shape");
  }
  const auto event = string_field(data, "e");
  const auto symbol = string_field(data, "s");
  const auto aggregate_id = unsigned_field(data, "a");
  const auto price = string_field(data, "p");
  const auto quantity = string_field(data, "q");
  const auto first_trade_id = unsigned_field(data, "f");
  const auto last_trade_id = unsigned_field(data, "l");
  const auto event_time = unsigned_field(data, "E");
  const auto trade_time = unsigned_field(data, "T");
  const auto buyer_is_maker = boolean_field(data, "m");
  if (!event.has_value() || *event != "aggTrade") {
    return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                 "USD-M market payload is not aggTrade");
  }
  if (!symbol.has_value() || *symbol != requested_symbol) {
    return error(protocol::ProtocolErrorCode::WrongSymbol, "s",
                 "aggTrade symbol does not match the configured symbol");
  }
  if (!aggregate_id.has_value() || !price.has_value() ||
      !plain_decimal(*price, false) || !quantity.has_value() ||
      !plain_decimal(*quantity, false) || !first_trade_id.has_value() ||
      !last_trade_id.has_value() || !event_time.has_value() ||
      !trade_time.has_value() || !buyer_is_maker.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "aggTrade",
                 "aggTrade fields have invalid types or decimal values");
  }
  if (*last_trade_id < *first_trade_id) {
    return error(protocol::ProtocolErrorCode::InvalidField, "l",
                 "aggTrade last trade id must not precede first trade id");
  }
  std::optional<std::string_view> normal_quantity;
  if (data.contains("nq")) {
    normal_quantity = string_field(data, "nq");
    if (!normal_quantity.has_value() ||
        !plain_decimal(*normal_quantity, true)) {
      return error(protocol::ProtocolErrorCode::InvalidField, "nq",
                   "aggTrade nq must be a plain decimal string when present");
    }
  }
  if (data.contains("st")) {
    const auto symbol_type = unsigned_field(data, "st");
    if (!symbol_type.has_value() || *symbol_type != 1U) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "st",
                   "aggTrade symbol type must be 1");
    }
  }
  market::AggTrade trade;
  populate_metadata(trade, common_wire::STREAM_AGG_TRADE, "agg-trade.v1",
                    requested_symbol, connection_id, received_at, *event_time);
  trade.mutable_metadata()->set_exchange_trade_time_ms(*trade_time);
  trade.set_aggregate_trade_id(*aggregate_id);
  trade.set_price(std::string{*price});
  trade.set_quantity(std::string{*quantity});
  trade.set_first_trade_id(*first_trade_id);
  trade.set_last_trade_id(*last_trade_id);
  trade.set_trade_time_ms(*trade_time);
  trade.set_buyer_is_maker(*buyer_is_maker);
  if (normal_quantity.has_value()) {
    trade.set_normal_quantity(std::string{*normal_quantity});
  }
  return trade;
}

[[nodiscard]] bool signed_decimal(std::string_view value) noexcept {
  if (!value.empty() && value.front() == '-') {
    value.remove_prefix(1U);
  }
  return plain_decimal(value, true);
}

[[nodiscard]] UsdMMarketFrameResult
parse_mark_price_object(const Json &data, runtime::ClockSample received_at,
                        std::string_view connection_id,
                        std::string_view requested_symbol) {
  if (!has_required_keys(data, {"e", "E", "s", "p", "i", "P", "r", "T"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "data",
                 "USD-M markPrice has an unexpected shape");
  }
  const auto event = string_field(data, "e");
  const auto symbol = string_field(data, "s");
  const auto mark_price = string_field(data, "p");
  const auto index_price = string_field(data, "i");
  const auto settle_price = string_field(data, "P");
  const auto funding_rate = string_field(data, "r");
  const auto event_time = unsigned_field(data, "E");
  const auto funding_time = unsigned_field(data, "T");
  if (!event.has_value() || *event != "markPriceUpdate") {
    return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                 "USD-M market payload is not markPriceUpdate");
  }
  if (!symbol.has_value() || *symbol != requested_symbol) {
    return error(protocol::ProtocolErrorCode::WrongSymbol, "s",
                 "markPrice symbol does not match the configured symbol");
  }
  if (!mark_price.has_value() || !plain_decimal(*mark_price, false) ||
      !index_price.has_value() || !plain_decimal(*index_price, false) ||
      !settle_price.has_value() || !plain_decimal(*settle_price, false) ||
      !funding_rate.has_value() || !signed_decimal(*funding_rate) ||
      !event_time.has_value() || !funding_time.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "markPrice",
                 "markPrice fields have invalid types or decimal values");
  }
  std::optional<std::string_view> moving_average;
  if (data.contains("ap")) {
    moving_average = string_field(data, "ap");
    if (!moving_average.has_value() || !plain_decimal(*moving_average, false)) {
      return error(protocol::ProtocolErrorCode::InvalidField, "ap",
                   "markPrice ap must be a positive decimal when present");
    }
  }
  if (data.contains("st")) {
    const auto symbol_type = unsigned_field(data, "st");
    if (!symbol_type.has_value() || *symbol_type != 1U) {
      return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "st",
                   "markPrice symbol type must be 1");
    }
  }
  market::MarkPrice price;
  populate_metadata(price, common_wire::STREAM_MARK_PRICE, "mark-price.v1",
                    requested_symbol, connection_id, received_at, *event_time);
  price.set_mark_price(std::string{*mark_price});
  price.set_index_price(std::string{*index_price});
  price.set_estimated_settle_price(std::string{*settle_price});
  price.set_funding_rate(std::string{*funding_rate});
  price.set_next_funding_time_ms(*funding_time);
  if (moving_average.has_value()) {
    price.set_mark_price_moving_average(std::string{*moving_average});
  }
  return price;
}

[[nodiscard]] UsdMMarketFrameResult
parse_liquidation_object(const Json &data, runtime::ClockSample received_at,
                         std::string_view connection_id,
                         std::string_view requested_symbol) {
  if (!has_required_keys(data, {"e", "E", "o"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "data",
                 "USD-M forceOrder has an unexpected shape");
  }
  const auto event = string_field(data, "e");
  if (!event.has_value() || *event != "forceOrder") {
    return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                 "USD-M market payload is not forceOrder");
  }
  const auto event_time = unsigned_field(data, "E");
  const auto &order = data.at("o");
  if (!event_time.has_value() || !order.is_object() ||
      !has_required_keys(order,
                         {"s", "S", "o", "f", "q", "p", "X", "l", "z", "T"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "o",
                 "forceOrder order has an unexpected shape");
  }
  const auto symbol = string_field(order, "s");
  const auto side = string_field(order, "S");
  const auto order_type = string_field(order, "o");
  const auto time_in_force = string_field(order, "f");
  const auto original_quantity = string_field(order, "q");
  const auto price = string_field(order, "p");
  const auto order_status = string_field(order, "X");
  const auto last_filled = string_field(order, "l");
  const auto accumulated = string_field(order, "z");
  const auto order_trade_time = unsigned_field(order, "T");
  if (!symbol.has_value() || *symbol != requested_symbol) {
    return error(protocol::ProtocolErrorCode::WrongSymbol, "o.s",
                 "forceOrder symbol does not match the configured symbol");
  }
  if (!side.has_value() || side->empty() || !order_type.has_value() ||
      order_type->empty() || !time_in_force.has_value() ||
      time_in_force->empty() || !original_quantity.has_value() ||
      !plain_decimal(*original_quantity, false) || !price.has_value() ||
      !plain_decimal(*price, false) || !order_status.has_value() ||
      order_status->empty() || !last_filled.has_value() ||
      !plain_decimal(*last_filled, true) || !accumulated.has_value() ||
      !plain_decimal(*accumulated, true) || !order_trade_time.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "forceOrder",
                 "forceOrder fields have invalid types or decimal values");
  }
  std::optional<std::string_view> average_price;
  if (order.contains("ap")) {
    average_price = string_field(order, "ap");
    if (!average_price.has_value() || !plain_decimal(*average_price, true)) {
      return error(protocol::ProtocolErrorCode::InvalidField, "o.ap",
                   "forceOrder average price must be a decimal when present");
    }
  }
  market::Liquidation liquidation;
  populate_metadata(liquidation, common_wire::STREAM_LIQUIDATION,
                    "liquidation.v1", requested_symbol, connection_id,
                    received_at, *event_time);
  liquidation.set_side(std::string{*side});
  liquidation.set_order_type(std::string{*order_type});
  liquidation.set_time_in_force(std::string{*time_in_force});
  liquidation.set_original_quantity(std::string{*original_quantity});
  liquidation.set_price(std::string{*price});
  if (average_price.has_value()) {
    liquidation.set_average_price(std::string{*average_price});
  }
  liquidation.set_order_status(std::string{*order_status});
  liquidation.set_last_filled_quantity(std::string{*last_filled});
  liquidation.set_accumulated_filled_quantity(std::string{*accumulated});
  liquidation.set_order_trade_time_ms(*order_trade_time);
  return liquidation;
}

[[nodiscard]] std::optional<protocol::ServerShutdown>
parse_server_shutdown(const Json &data) {
  if (!has_required_keys(data, {"e", "E"})) {
    return std::nullopt;
  }
  const auto event = string_field(data, "e");
  const auto event_time = unsigned_field(data, "E");
  if (!event.has_value() || *event != "serverShutdown" ||
      !event_time.has_value()) {
    return std::nullopt;
  }
  return protocol::ServerShutdown{*event_time};
}

[[nodiscard]] std::optional<std::string_view>
combined_stream_root(const Json &root) {
  if (!has_required_keys(root, {"stream", "data"}) ||
      !root.at("data").is_object()) {
    return std::nullopt;
  }
  return string_field(root, "stream");
}

UsdMDepthFrameResult parse_usdm_depth_frame(std::string_view payload,
                                            runtime::ClockSample received_at,
                                            std::string_view connection_id,
                                            std::string_view requested_symbol) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<protocol::ProtocolError>(&decoded)) {
    return *failure;
  }
  return parse_usdm_depth_object(std::get<Json>(decoded), received_at,
                                 connection_id, requested_symbol);
}

UsdMDepthFrameResult parse_usdm_depth_frame(std::string_view payload,
                                            runtime::ClockSample received_at,
                                            std::string_view connection_id) {
  return parse_usdm_depth_frame(payload, received_at, connection_id, "BTCUSDT");
}

UsdMDepthSnapshotResult parse_usdm_depth_snapshot(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view request_id, std::string_view requested_symbol) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<protocol::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  if (!usdm_stream_symbol(requested_symbol).has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                 "requested USD-M symbol cannot be represented by a Binance "
                 "route");
  }
  if (!has_required_keys(root, {"lastUpdateId", "bids", "asks"})) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "payload",
                 "USD-M depth snapshot has an unexpected shape");
  }
  const auto last_update_id = unsigned_field(root, "lastUpdateId");
  if (!last_update_id.has_value() || request_id.empty()) {
    return error(protocol::ProtocolErrorCode::InvalidField, "lastUpdateId",
                 "lastUpdateId must be uint64 and request_id non-empty");
  }
  std::optional<std::uint64_t> event_time;
  if (root.contains("E")) {
    event_time = unsigned_field(root, "E");
    if (!event_time.has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidField, "E",
                   "snapshot message output time must be uint64 when present");
    }
  }
  std::optional<std::uint64_t> transaction_time;
  if (root.contains("T")) {
    transaction_time = unsigned_field(root, "T");
    if (!transaction_time.has_value()) {
      return error(protocol::ProtocolErrorCode::InvalidField, "T",
                   "snapshot transaction time must be uint64 when present");
    }
  }

  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_USD_M_PERPETUAL);
  snapshot.set_symbol(std::string{requested_symbol});
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g11-usdm");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id(std::string{request_id});
  snapshot.set_last_update_id(*last_update_id);
  if (event_time.has_value()) {
    snapshot.set_exchange_event_time_ms(*event_time);
  }
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

UsdMDepthSnapshotResult
parse_usdm_depth_snapshot(std::string_view payload,
                          runtime::ClockSample received_at,
                          std::string_view request_id) {
  return parse_usdm_depth_snapshot(payload, received_at, request_id, "BTCUSDT");
}

UsdMPublicFrameResult parse_usdm_public_event_frame(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view connection_id, std::string_view requested_symbol) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<protocol::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  const auto stream = combined_stream_root(root);
  if (!stream.has_value() || connection_id.empty()) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "payload",
                 "USD-M combined stream envelope has an unexpected shape");
  }
  const auto &data = root.at("data");
  if (*stream == "!serverShutdown") {
    const auto shutdown = parse_server_shutdown(data);
    if (!shutdown.has_value()) {
      return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                   "server shutdown stream requires a serverShutdown event");
    }
    return *shutdown;
  }
  const auto encoded = usdm_stream_symbol(requested_symbol);
  if (!encoded.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                 "configured USD-M symbol cannot be represented as a stream");
  }
  const auto depth_stream = *encoded + "@depth@100ms";
  const auto book_ticker_stream = *encoded + "@bookTicker";
  if (*stream == depth_stream) {
    const auto nested = parse_usdm_depth_object(
        data, received_at, connection_id, requested_symbol);
    if (const auto *shutdown = std::get_if<ServerShutdown>(&nested)) {
      return *shutdown;
    }
    if (const auto *failure = std::get_if<ProtocolError>(&nested)) {
      return *failure;
    }
    return NormalizedMarketEvent{
        std::get<market::DepthUpdate>(std::move(nested))};
  }
  if (*stream == book_ticker_stream) {
    return parse_book_ticker_object(data, received_at, connection_id,
                                    requested_symbol);
  }
  return error(protocol::ProtocolErrorCode::WrongEvent, "stream",
               "USD-M public stream is not configured");
}

UsdMMarketFrameResult parse_usdm_market_event_frame(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view connection_id, std::string_view requested_symbol) {
  const auto decoded = parse_json(payload);
  if (const auto *failure = std::get_if<protocol::ProtocolError>(&decoded)) {
    return *failure;
  }
  const auto &root = std::get<Json>(decoded);
  const auto stream = combined_stream_root(root);
  if (!stream.has_value() || connection_id.empty()) {
    return error(protocol::ProtocolErrorCode::InvalidShape, "payload",
                 "USD-M combined stream envelope has an unexpected shape");
  }
  const auto &data = root.at("data");
  if (*stream == "!serverShutdown") {
    const auto shutdown = parse_server_shutdown(data);
    if (!shutdown.has_value()) {
      return error(protocol::ProtocolErrorCode::WrongEvent, "e",
                   "server shutdown stream requires a serverShutdown event");
    }
    return *shutdown;
  }
  const auto encoded = usdm_stream_symbol(requested_symbol);
  if (!encoded.has_value()) {
    return error(protocol::ProtocolErrorCode::InvalidMarketMetadata, "symbol",
                 "configured USD-M symbol cannot be represented as a stream");
  }
  const auto agg_trade_stream = *encoded + "@aggTrade";
  const auto mark_price_stream = *encoded + "@markPrice@1s";
  const auto force_order_stream = *encoded + "@forceOrder";
  if (*stream == agg_trade_stream) {
    return parse_agg_trade_object(data, received_at, connection_id,
                                  requested_symbol);
  }
  if (*stream == mark_price_stream) {
    return parse_mark_price_object(data, received_at, connection_id,
                                   requested_symbol);
  }
  if (*stream == force_order_stream) {
    return parse_liquidation_object(data, received_at, connection_id,
                                    requested_symbol);
  }
  return error(protocol::ProtocolErrorCode::WrongEvent, "stream",
               "USD-M market stream is not configured");
}

std::optional<std::string>
usdm_stream_symbol(std::string_view canonical_symbol) {
  return protocol::spot_stream_symbol(canonical_symbol);
}

} // namespace binance_market_data::gateway::protocol
