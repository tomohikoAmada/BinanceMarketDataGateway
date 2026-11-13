#include "transport/usdm_reference.hpp"

#include "protocol/json_helpers.hpp"
#include "protocol/spot_protocol.hpp"
#include "transport/usdm_transport.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <exception>
#include <limits>
#include <optional>
#include <unordered_map>
#include <utility>

namespace binance_market_data::gateway::transport {

namespace common_wire = ::binance_market_data::common::v1;
using Json = protocol::detail::Json;
using protocol::detail::non_negative_integer_field;
using protocol::detail::plain_decimal;
using protocol::detail::string_field;

namespace {

[[nodiscard]] UsdMReferenceParseError parse_error(std::string field,
                                                  std::string message) {
  return {std::move(field), std::move(message)};
}

[[nodiscard]] market::ReferenceObservation
base_observation(common_wire::ReferenceSourceKind source_kind,
                 runtime::ClockSample received_at, std::string_view request_id,
                 std::string_view symbol) {
  market::ReferenceObservation observation;
  observation.set_schema_version("reference-observation.v1");
  observation.set_venue(common_wire::VENUE_BINANCE);
  observation.set_market(common_wire::MARKET_USD_M_PERPETUAL);
  observation.set_symbol(std::string{symbol});
  observation.set_request_id(std::string{request_id});
  observation.set_source_kind(source_kind);
  observation.set_receive_time_utc_ns(received_at.utc_ns);
  observation.set_status(common_wire::REFERENCE_OBSERVATION_STATUS_VALID);
  return observation;
}

[[nodiscard]] UsdMReferenceParseResult
parse_periodic(UsdMReferenceKind kind, const Json &payload,
               runtime::ClockSample received_at, std::string_view request_id,
               std::string_view symbol) {
  const auto source_kind = usdm_reference_source_kind(kind);
  auto observation =
      base_observation(source_kind, received_at, request_id, symbol);
  const Json *entry = nullptr;
  if (payload.is_array()) {
    if (payload.empty()) {
      observation.set_status(common_wire::REFERENCE_OBSERVATION_STATUS_EMPTY);
      return observation;
    }
    entry = &payload.front();
  } else if (payload.is_object() && kind == UsdMReferenceKind::OpenInterest) {
    entry = &payload;
  }
  if (entry == nullptr || !entry->is_object()) {
    return parse_error("payload", "reference payload is not an object");
  }
  const auto payload_symbol = string_field(*entry, "symbol");
  if (payload_symbol.has_value() && *payload_symbol != symbol) {
    return parse_error("symbol", "reference symbol does not match");
  }

  switch (kind) {
  case UsdMReferenceKind::OpenInterest: {
    const auto value = string_field(*entry, "openInterest");
    const auto source_time = non_negative_integer_field(*entry, "time");
    if (!value.has_value() || !plain_decimal(*value, true)) {
      return parse_error("openInterest",
                         "openInterest must be a non-negative decimal");
    }
    auto *item = observation.mutable_open_interest();
    item->set_open_interest(std::string{*value});
    if (source_time.has_value()) {
      item->set_source_time_ms(*source_time);
      observation.set_source_time_ms(*source_time);
    }
    return observation;
  }
  case UsdMReferenceKind::TopLongShortAccountRatio:
  case UsdMReferenceKind::TopLongShortPositionRatio:
  case UsdMReferenceKind::GlobalLongShortAccountRatio: {
    const auto ratio = string_field(*entry, "longShortRatio");
    const auto long_account = string_field(*entry, "longAccount");
    const auto short_account = string_field(*entry, "shortAccount");
    const auto source_time = non_negative_integer_field(*entry, "timestamp");
    if (!ratio.has_value() || !plain_decimal(*ratio, true) ||
        !long_account.has_value() || !plain_decimal(*long_account, true) ||
        !short_account.has_value() || !plain_decimal(*short_account, true)) {
      return parse_error("ratio",
                         "longShortRatio/longAccount/shortAccount must be "
                         "non-negative decimals");
    }
    auto *item = observation.mutable_top_long_short_account_ratio();
    if (kind == UsdMReferenceKind::TopLongShortPositionRatio) {
      item = observation.mutable_top_long_short_position_ratio();
    } else if (kind == UsdMReferenceKind::GlobalLongShortAccountRatio) {
      item = observation.mutable_global_long_short_account_ratio();
    }
    item->set_long_short_ratio(std::string{*ratio});
    item->set_long_account(std::string{*long_account});
    item->set_short_account(std::string{*short_account});
    item->set_period("5m");
    if (source_time.has_value()) {
      item->set_source_time_ms(*source_time);
      observation.set_source_time_ms(*source_time);
    }
    return observation;
  }
  case UsdMReferenceKind::TakerBuySellVolume: {
    const auto ratio = string_field(*entry, "buySellRatio");
    const auto buy = string_field(*entry, "buyVol");
    const auto sell = string_field(*entry, "sellVol");
    const auto source_time = non_negative_integer_field(*entry, "timestamp");
    if (!ratio.has_value() || !plain_decimal(*ratio, true) ||
        !buy.has_value() || !plain_decimal(*buy, true) || !sell.has_value() ||
        !plain_decimal(*sell, true)) {
      return parse_error(
          "taker", "buySellRatio/buyVol/sellVol must be non-negative decimals");
    }
    auto *item = observation.mutable_taker_buy_sell_volume();
    item->set_buy_sell_ratio(std::string{*ratio});
    item->set_buy_volume(std::string{*buy});
    item->set_sell_volume(std::string{*sell});
    item->set_period("5m");
    if (source_time.has_value()) {
      item->set_source_time_ms(*source_time);
      observation.set_source_time_ms(*source_time);
    }
    return observation;
  }
  case UsdMReferenceKind::Instrument:
    break;
  }
  return parse_error("kind", "instrument kind is not periodic");
}

void append_filters(const Json &symbol_json,
                    market::InstrumentReference *reference) {
  const auto filters = symbol_json.find("filters");
  if (filters == symbol_json.end() || !filters->is_array()) {
    return;
  }
  for (const auto &filter : *filters) {
    if (!filter.is_object()) {
      continue;
    }
    const auto type = string_field(filter, "filterType");
    if (!type.has_value()) {
      continue;
    }
    auto *item = reference->add_filters();
    item->set_filter_type(std::string{*type});
    const auto set = [&filter, item](std::string_view name,
                                     void (market::InstrumentFilter::*setter)(
                                         const std::string &)) {
      const auto value = string_field(filter, name);
      if (value.has_value()) {
        (item->*setter)(std::string{*value});
      }
    };
    if (*type == "PRICE_FILTER") {
      set("tickSize", &market::InstrumentFilter::set_price);
      set("tickSize", &market::InstrumentFilter::set_tick_size);
      set("minPrice", &market::InstrumentFilter::set_min_price);
      set("maxPrice", &market::InstrumentFilter::set_max_price);
    } else if (*type == "LOT_SIZE" || *type == "MARKET_LOT_SIZE") {
      set("stepSize", &market::InstrumentFilter::set_quantity);
      set("stepSize", &market::InstrumentFilter::set_step_size);
      set("minQty", &market::InstrumentFilter::set_min_quantity);
      set("maxQty", &market::InstrumentFilter::set_max_quantity);
    } else if (*type == "MIN_NOTIONAL" || *type == "NOTIONAL") {
      set("notional", &market::InstrumentFilter::set_notional);
    } else if (*type == "MAX_NUM_ORDERS" || *type == "MAX_NUM_ALGO_ORDERS") {
      const auto value = non_negative_integer_field(filter, "limit");
      if (value.has_value()) {
        item->set_limit(*value);
      }
    } else if (*type == "PERCENT_PRICE" || *type == "PERCENT_PRICE_BY_SIDE") {
      set("multiplierUp", &market::InstrumentFilter::set_multiplier_up);
      set("multiplierDown", &market::InstrumentFilter::set_multiplier_down);
      set("multiplierDecimal",
          &market::InstrumentFilter::set_multiplier_decimal);
    } else if (*type == "POSITION_RISK_CONTROL") {
      const auto value = string_field(filter, "positionControlSide");
      if (value.has_value()) {
        item->set_position_control_side(std::string{*value});
      }
    }
  }
}

} // namespace

common_wire::ReferenceSourceKind
usdm_reference_source_kind(UsdMReferenceKind kind) noexcept {
  switch (kind) {
  case UsdMReferenceKind::OpenInterest:
    return common_wire::REFERENCE_SOURCE_KIND_OPEN_INTEREST;
  case UsdMReferenceKind::TopLongShortAccountRatio:
    return common_wire::REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO;
  case UsdMReferenceKind::TopLongShortPositionRatio:
    return common_wire::REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO;
  case UsdMReferenceKind::GlobalLongShortAccountRatio:
    return common_wire::REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO;
  case UsdMReferenceKind::TakerBuySellVolume:
    return common_wire::REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME;
  case UsdMReferenceKind::Instrument:
    return common_wire::REFERENCE_SOURCE_KIND_INSTRUMENT;
  }
  return common_wire::REFERENCE_SOURCE_KIND_UNSPECIFIED;
}

std::string_view usdm_reference_kind_name(UsdMReferenceKind kind) noexcept {
  switch (kind) {
  case UsdMReferenceKind::OpenInterest:
    return "open-interest";
  case UsdMReferenceKind::TopLongShortAccountRatio:
    return "top-long-short-account";
  case UsdMReferenceKind::TopLongShortPositionRatio:
    return "top-long-short-position";
  case UsdMReferenceKind::GlobalLongShortAccountRatio:
    return "global-long-short-account";
  case UsdMReferenceKind::TakerBuySellVolume:
    return "taker-buy-sell-volume";
  case UsdMReferenceKind::Instrument:
    return "instrument";
  }
  return "unknown";
}

std::string usdm_reference_target(UsdMReferenceKind kind,
                                  std::string_view symbol) {
  const std::string suffix = "?symbol=" + std::string{symbol};
  switch (kind) {
  case UsdMReferenceKind::OpenInterest:
    return "/fapi/v1/openInterest" + suffix;
  case UsdMReferenceKind::TopLongShortAccountRatio:
    return "/futures/data/topLongShortAccountRatio" + suffix +
           "&period=5m&limit=1";
  case UsdMReferenceKind::TopLongShortPositionRatio:
    return "/futures/data/topLongShortPositionRatio" + suffix +
           "&period=5m&limit=1";
  case UsdMReferenceKind::GlobalLongShortAccountRatio:
    return "/futures/data/globalLongShortAccountRatio" + suffix +
           "&period=5m&limit=1";
  case UsdMReferenceKind::TakerBuySellVolume:
    return "/futures/data/takerlongshortRatio" + suffix + "&period=5m&limit=1";
  case UsdMReferenceKind::Instrument:
    return "/fapi/v1/exchangeInfo";
  }
  return {};
}

UsdMReferenceParseResult
parse_usdm_reference_payload(UsdMReferenceKind kind, std::string_view payload,
                             runtime::ClockSample received_at,
                             std::string_view request_id,
                             std::string_view symbol) {
  if (kind == UsdMReferenceKind::Instrument) {
    return parse_error("kind", "use parse_usdm_instrument_definition");
  }
  Json decoded;
  try {
    decoded = Json::parse(payload.begin(), payload.end(), nullptr, false);
  } catch (const std::exception &) {
    return parse_error("payload", "reference response is not valid JSON");
  }
  if (decoded.is_discarded()) {
    return parse_error("payload", "reference response is not valid JSON");
  }
  return parse_periodic(kind, decoded, received_at, request_id, symbol);
}

namespace {

[[nodiscard]] std::vector<market::RateLimit>
extract_rate_limits(const Json &decoded) {
  std::vector<market::RateLimit> items;
  const auto rate_limits = decoded.find("rateLimits");
  if (rate_limits == decoded.end() || !rate_limits->is_array()) {
    return items;
  }
  for (const auto &limit : *rate_limits) {
    if (!limit.is_object()) {
      continue;
    }
    const auto type = string_field(limit, "rateLimitType");
    const auto interval = string_field(limit, "interval");
    const auto interval_num = non_negative_integer_field(limit, "intervalNum");
    const auto value = non_negative_integer_field(limit, "limit");
    if (!type.has_value() || !interval.has_value() ||
        !interval_num.has_value() || !value.has_value() ||
        *interval_num > static_cast<std::uint64_t>(
                            std::numeric_limits<std::int32_t>::max())) {
      continue;
    }
    market::RateLimit item;
    item.set_rate_limit_type(std::string{*type});
    item.set_interval(std::string{*interval});
    item.set_interval_num(static_cast<std::uint32_t>(*interval_num));
    item.set_limit(*value);
    items.push_back(std::move(item));
  }
  return items;
}

[[nodiscard]] UsdMReferenceParseResult build_instrument_observation(
    const Json &selected, std::optional<std::uint64_t> server_time,
    const std::vector<market::RateLimit> &rate_limits,
    runtime::ClockSample received_at, std::string_view request_id,
    std::string_view symbol) {
  auto observation =
      base_observation(common_wire::REFERENCE_SOURCE_KIND_INSTRUMENT,
                       received_at, request_id, symbol);
  auto *reference = observation.mutable_instrument();
  reference->set_symbol_present(true);
  const auto set_required = [&selected](std::string_view name,
                                        std::string *target) {
    const auto value = string_field(selected, name);
    if (value.has_value()) {
      *target = std::string{*value};
    }
    return value.has_value();
  };
  if (!set_required("contractType", reference->mutable_contract_type()) ||
      !set_required("status", reference->mutable_trading_status()) ||
      !set_required("baseAsset", reference->mutable_base_asset()) ||
      !set_required("quoteAsset", reference->mutable_quote_asset()) ||
      !set_required("marginAsset", reference->mutable_margin_asset())) {
    return parse_error("instrument",
                       "exchangeInfo symbol identity fields are required");
  }
  const auto price_precision =
      non_negative_integer_field(selected, "pricePrecision");
  const auto quantity_precision =
      non_negative_integer_field(selected, "quantityPrecision");
  if (!price_precision.has_value() || !quantity_precision.has_value() ||
      *price_precision > static_cast<std::uint64_t>(
                             std::numeric_limits<std::int32_t>::max()) ||
      *quantity_precision > static_cast<std::uint64_t>(
                                std::numeric_limits<std::int32_t>::max())) {
    return parse_error("precision",
                       "pricePrecision and quantityPrecision are required");
  }
  reference->set_price_precision(static_cast<std::int32_t>(*price_precision));
  reference->set_quantity_precision(
      static_cast<std::int32_t>(*quantity_precision));
  if (const auto onboard = non_negative_integer_field(selected, "onboardDate");
      onboard.has_value()) {
    reference->set_onboard_time_ms(*onboard);
  }
  if (const auto delivery =
          non_negative_integer_field(selected, "deliveryDate");
      delivery.has_value()) {
    reference->set_delivery_time_ms(*delivery);
  }
  if (server_time.has_value()) {
    reference->set_server_time_ms(*server_time);
    observation.set_source_time_ms(*server_time);
  }
  append_filters(selected, reference);
  for (const auto &limit : rate_limits) {
    *reference->add_rate_limits() = limit;
  }
  return observation;
}

} // namespace

UsdMInstrumentBatchResult parse_usdm_instrument_definitions_batch(
    std::string_view payload, runtime::ClockSample received_at,
    std::span<const UsdMInstrumentBatchRequest> requests) {
  Json decoded;
  try {
    decoded = Json::parse(payload.begin(), payload.end(), nullptr, false);
  } catch (const std::exception &) {
    return parse_error("payload", "exchangeInfo is not valid JSON");
  }
  if (decoded.is_discarded() || !decoded.is_object()) {
    return parse_error("payload", "exchangeInfo must be a JSON object");
  }
  const auto symbols = decoded.find("symbols");
  if (symbols == decoded.end() || !symbols->is_array()) {
    return parse_error("symbols", "exchangeInfo symbols must be an array");
  }

  std::unordered_map<std::string_view, std::vector<std::size_t>> requested;
  requested.reserve(requests.size());
  for (std::size_t index = 0; index < requests.size(); ++index) {
    requested[requests[index].symbol].push_back(index);
  }

  std::vector<const Json *> selected(requests.size(), nullptr);
  for (const auto &candidate : *symbols) {
    if (!candidate.is_object()) {
      continue;
    }
    const auto candidate_symbol = string_field(candidate, "symbol");
    if (!candidate_symbol.has_value()) {
      continue;
    }
    const auto match = requested.find(*candidate_symbol);
    if (match == requested.end()) {
      continue;
    }
    for (const auto index : match->second) {
      if (selected[index] == nullptr) {
        selected[index] = &candidate;
      }
    }
  }

  const auto server_time = non_negative_integer_field(decoded, "serverTime");
  const auto rate_limits = extract_rate_limits(decoded);

  UsdMInstrumentBatch batch;
  batch.symbols.reserve(requests.size());
  for (std::size_t index = 0; index < requests.size(); ++index) {
    if (selected[index] == nullptr) {
      batch.symbols.push_back(
          parse_error("symbol", "exchangeInfo does not contain symbol"));
      continue;
    }
    batch.symbols.push_back(build_instrument_observation(
        *selected[index], server_time, rate_limits, received_at,
        requests[index].request_id, requests[index].symbol));
  }
  return batch;
}

UsdMReferenceParseResult parse_usdm_instrument_definition(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view request_id, std::string_view symbol) {
  const std::array<UsdMInstrumentBatchRequest, 1> request{
      UsdMInstrumentBatchRequest{symbol, request_id}};
  auto batch =
      parse_usdm_instrument_definitions_batch(payload, received_at, request);
  if (const auto *failure = std::get_if<UsdMReferenceParseError>(&batch)) {
    return *failure;
  }
  return std::move(std::get<UsdMInstrumentBatch>(batch).symbols.front());
}

} // namespace binance_market_data::gateway::transport

namespace binance_market_data::gateway::transport {

namespace {

constexpr std::array<UsdMReferenceKind, 5> kPeriodicKinds{
    UsdMReferenceKind::OpenInterest,
    UsdMReferenceKind::TopLongShortAccountRatio,
    UsdMReferenceKind::TopLongShortPositionRatio,
    UsdMReferenceKind::GlobalLongShortAccountRatio,
    UsdMReferenceKind::TakerBuySellVolume,
};

[[nodiscard]] market::ReferenceObservation
make_error_observation(UsdMReferenceKind kind, runtime::ClockSample received_at,
                       std::string_view request_id, std::string_view symbol,
                       std::string error_code) {
  auto observation = base_observation(usdm_reference_source_kind(kind),
                                      received_at, request_id, symbol);
  observation.set_status(common_wire::REFERENCE_OBSERVATION_STATUS_ERROR);
  observation.set_error_code(std::move(error_code));
  return observation;
}

[[nodiscard]] std::string network_error_code(const NetworkError &error) {
  std::string code = error.stage;
  if (error.http_status.has_value()) {
    code += "-";
    code += std::to_string(*error.http_status);
  }
  return code;
}

[[nodiscard]] std::string_view
seed_body_view(const UsdMReferenceSeed &seed) noexcept {
  if (seed.shared_exchange_info_body != nullptr) {
    return *seed.shared_exchange_info_body;
  }
  return seed.exchange_info_body;
}

[[nodiscard]] std::optional<projection::v1::NumericSpec>
extract_numeric_spec_from_instrument(
    const market::InstrumentReference &reference) {
  std::optional<std::string_view> tick_size;
  std::optional<std::string_view> step_size;
  for (const auto &filter : reference.filters()) {
    if (filter.filter_type() == "PRICE_FILTER") {
      if (tick_size.has_value() || filter.tick_size().empty()) {
        return std::nullopt;
      }
      tick_size = filter.tick_size();
    } else if (filter.filter_type() == "LOT_SIZE") {
      if (step_size.has_value() || filter.step_size().empty()) {
        return std::nullopt;
      }
      step_size = filter.step_size();
    }
  }
  if (!tick_size.has_value() || !step_size.has_value()) {
    return std::nullopt;
  }
  const auto price_scale = protocol::decimal_scale_from_quantum(*tick_size);
  const auto quantity_scale = protocol::decimal_scale_from_quantum(*step_size);
  if (!std::holds_alternative<projection::v1::DecimalScale>(price_scale) ||
      !std::holds_alternative<projection::v1::DecimalScale>(quantity_scale)) {
    return std::nullopt;
  }
  return projection::v1::NumericSpec{
      std::get<projection::v1::DecimalScale>(price_scale),
      std::get<projection::v1::DecimalScale>(quantity_scale)};
}

[[nodiscard]] std::optional<std::string> check_incompatible_definition(
    const UsdMReferenceProduct &product,
    const std::optional<std::pair<std::int32_t, std::int32_t>>
        &baseline_precisions,
    const UsdMReferenceParseResult &parsed) {
  if (const auto *failure = std::get_if<UsdMReferenceParseError>(&parsed)) {
    if (failure->field == "symbol") {
      return std::string{"symbol_missing"};
    }
    return "instrument_parse_error:" + failure->field;
  }
  const auto &observation = std::get<market::ReferenceObservation>(parsed);
  if (!observation.has_instrument()) {
    return std::string{"missing_instrument_payload"};
  }
  const auto &instrument = observation.instrument();
  if (instrument.contract_type() != "PERPETUAL") {
    return "contract_type_changed:" + instrument.contract_type();
  }
  if (instrument.trading_status() != "TRADING") {
    return "trading_status_changed:" + instrument.trading_status();
  }
  if (baseline_precisions.has_value() &&
      (instrument.price_precision() != baseline_precisions->first ||
       instrument.quantity_precision() != baseline_precisions->second)) {
    return std::string{"precision_changed"};
  }
  if (product.baseline_numeric_spec.has_value()) {
    const auto current_spec = extract_numeric_spec_from_instrument(instrument);
    if (!current_spec.has_value()) {
      return std::string{"missing_or_invalid_numeric_filter"};
    }
    if (*current_spec != *product.baseline_numeric_spec) {
      return std::string{"numeric_spec_changed"};
    }
  }
  return std::nullopt;
}

} // namespace

struct UsdMReferenceService::Impl final {
  Impl(std::vector<UsdMReferenceProduct> products,
       std::vector<UsdMReferenceSeed> seeds, runtime::RuntimeClock clock,
       UsdMReferenceOptions options, UsdMReferenceSources sources)
      : products_{std::move(products)}, seeds_{std::move(seeds)},
        clock_{std::move(clock)}, options_{std::move(options)},
        sources_{std::move(sources)}, baseline_precisions_(products_.size()),
        quarantined_(products_.size(), false) {
    if (!clock_) {
      throw std::invalid_argument{"reference service clock is required"};
    }
    if (options_.periodic_period <= std::chrono::milliseconds::zero() ||
        options_.exchange_info_period <= std::chrono::milliseconds::zero()) {
      throw std::invalid_argument{"reference periods must be positive"};
    }
    if (!sources_.fetch) {
      sources_.fetch = [user_agent =
                            options_.user_agent](std::string_view target) {
        return fetch_https_json("fapi.binance.com", "443", std::string{target},
                                user_agent);
      };
    }
    if (!sources_.exchange_info_fetch) {
      sources_.exchange_info_fetch = fetch_usdm_exchange_info_https;
    }
  }

  ~Impl() { stop(); }

  [[nodiscard]] bool start() {
    std::lock_guard lock{mutex_};
    if (observation_.running) {
      return false;
    }
    observation_.running = true;
    stop_requested_ = false;
    thread_ = std::thread{[this] { run(); }};
    return true;
  }

  void stop() noexcept {
    {
      std::lock_guard lock{mutex_};
      if (!observation_.running && !thread_.joinable()) {
        return;
      }
      stop_requested_ = true;
      observation_.running = false;
    }
    condition_.notify_all();
    if (thread_.joinable()) {
      try {
        thread_.join();
      } catch (...) {
        std::terminate();
      }
    }
  }

  [[nodiscard]] UsdMReferenceObservation observe() const noexcept {
    std::lock_guard lock{mutex_};
    return observation_;
  }

  void poll_once_for_testing() {
    publish_seed_instruments();
    for (std::size_t index = 0; index < products_.size(); ++index) {
      for (const auto kind : kPeriodicKinds) {
        execute_periodic(index, kind);
      }
    }
    execute_exchange_info_refresh();
  }

private:
  struct Task final {
    std::size_t product{0U};
    UsdMReferenceKind kind{UsdMReferenceKind::OpenInterest};
    std::chrono::steady_clock::time_point due{};
  };

  [[nodiscard]] std::string next_request_id(UsdMReferenceKind kind,
                                            std::string_view symbol) {
    std::lock_guard lock{mutex_};
    const auto sequence = next_request_sequence_++;
    return "ref-" + std::string{symbol} + "-" +
           std::string{usdm_reference_kind_name(kind)} + "-" +
           std::to_string(sequence);
  }

  void record_request(bool failed, bool published) {
    std::lock_guard lock{mutex_};
    ++observation_.requests;
    if (failed) {
      ++observation_.failures;
    }
    if (published) {
      ++observation_.published;
    }
  }

  [[nodiscard]] bool publish(const UsdMReferenceProduct &product,
                             market::ReferenceObservation observation) {
    if (product.publication == nullptr) {
      return false;
    }
    const auto state = product.publication->observe();
    if (state.source_state != publication::EventSourceState::Open ||
        !state.source_generation.has_value()) {
      return false;
    }
    try {
      auto event = std::make_shared<const protocol::NormalizedMarketEvent>(
          std::move(observation));
      return product.publication->publish(event, *state.source_generation) ==
             publication::EventPublishResult::Published;
    } catch (...) {
      return false;
    }
  }

  void
  record_baseline_precision(std::size_t product_index,
                            const market::ReferenceObservation &observation) {
    if (!baseline_precisions_[product_index].has_value() &&
        observation.has_instrument()) {
      baseline_precisions_[product_index] =
          std::pair<std::int32_t, std::int32_t>{
              observation.instrument().price_precision(),
              observation.instrument().quantity_precision()};
    }
  }

  void publish_seed_instruments() {
    struct PendingSeed final {
      std::size_t product_index{0U};
      const UsdMReferenceSeed *seed{nullptr};
      std::string request_id;
    };
    std::vector<PendingSeed> pending;
    pending.reserve(seeds_.size());
    for (const auto &seed : seeds_) {
      const auto product = std::find_if(
          products_.begin(), products_.end(),
          [&seed](const auto &candidate) { return candidate.key == seed.key; });
      if (product == products_.end()) {
        continue;
      }
      const auto product_index =
          static_cast<std::size_t>(product - products_.begin());
      if (seed.instrument.has_value()) {
        auto observation = *seed.instrument;
        if (observation.request_id().empty()) {
          observation.set_request_id(
              next_request_id(UsdMReferenceKind::Instrument, seed.key.symbol));
        }
        record_baseline_precision(product_index, observation);
        const auto published = publish(*product, std::move(observation));
        record_request(false, published);
        continue;
      }
      if (seed_body_view(seed).empty()) {
        continue;
      }
      pending.push_back(
          {product_index, &seed,
           next_request_id(UsdMReferenceKind::Instrument, seed.key.symbol)});
    }
    std::vector<bool> processed(pending.size(), false);
    for (std::size_t i = 0; i < pending.size(); ++i) {
      if (processed[i]) {
        continue;
      }
      const auto body = seed_body_view(*pending[i].seed);
      const auto received_at = pending[i].seed->received_at;
      std::vector<std::size_t> group_indices;
      std::vector<UsdMInstrumentBatchRequest> requests;
      for (std::size_t j = i; j < pending.size(); ++j) {
        if (processed[j]) {
          continue;
        }
        const auto &candidate = *pending[j].seed;
        if (seed_body_view(candidate) == body &&
            candidate.received_at.utc_ns == received_at.utc_ns &&
            candidate.received_at.monotonic_ns == received_at.monotonic_ns) {
          processed[j] = true;
          group_indices.push_back(j);
          requests.push_back({products_[pending[j].product_index].key.symbol,
                              pending[j].request_id});
        }
      }
      const auto batch_result =
          parse_usdm_instrument_definitions_batch(body, received_at, requests);
      if (std::holds_alternative<UsdMReferenceParseError>(batch_result)) {
        for (std::size_t k = 0; k < group_indices.size(); ++k) {
          record_request(true, false);
        }
        continue;
      }
      const auto &batch = std::get<UsdMInstrumentBatch>(batch_result);
      for (std::size_t k = 0; k < group_indices.size(); ++k) {
        const auto product_index = pending[group_indices[k]].product_index;
        if (const auto *observation =
                std::get_if<market::ReferenceObservation>(&batch.symbols[k])) {
          record_baseline_precision(product_index, *observation);
          const auto published =
              publish(products_[product_index], *observation);
          record_request(false, published);
        } else {
          record_request(true, false);
        }
      }
    }
  }

  void execute_periodic(std::size_t index, UsdMReferenceKind kind) {
    if (index >= products_.size() || is_quarantined(index)) {
      return;
    }
    const auto &product = products_[index];
    const auto request_id = next_request_id(kind, product.key.symbol);
    const auto target = usdm_reference_target(kind, product.key.symbol);
    const auto response = sources_.fetch(target);
    if (const auto *failure = std::get_if<NetworkError>(&response)) {
      auto observation =
          make_error_observation(kind, clock_(), request_id, product.key.symbol,
                                 network_error_code(*failure));
      static_cast<void>(publish(product, std::move(observation)));
      record_request(true, false);
      return;
    }
    const auto &body = std::get<HttpsJsonResponse>(response);
    const auto parsed = parse_usdm_reference_payload(
        kind, body.body, body.received_at, request_id, product.key.symbol);
    if (const auto *observation =
            std::get_if<market::ReferenceObservation>(&parsed)) {
      const auto published = publish(product, *observation);
      record_request(false, published);
      return;
    }
    const auto &error = std::get<UsdMReferenceParseError>(parsed);
    auto observation =
        make_error_observation(kind, body.received_at, request_id,
                               product.key.symbol, "parse:" + error.field);
    static_cast<void>(publish(product, std::move(observation)));
    record_request(true, false);
  }

  [[nodiscard]] bool is_quarantined(std::size_t index) const noexcept {
    std::lock_guard lock{mutex_};
    return quarantined_[index];
  }

  void execute_exchange_info_refresh() {
    if (products_.empty()) {
      return;
    }
    const auto response = sources_.exchange_info_fetch();
    if (const auto *failure = std::get_if<NetworkError>(&response)) {
      // A failed refresh keeps the last published definitions and their
      // timestamps. It never fabricates a replacement observation.
      static_cast<void>(failure);
      record_request(true, false);
      return;
    }
    const auto &body = std::get<ExchangeInfoResponse>(response);
    const auto received_at =
        body.received_at.utc_ns == 0U ? clock_() : body.received_at;

    std::vector<std::size_t> active_indices;
    std::vector<std::string> request_ids;
    std::vector<UsdMInstrumentBatchRequest> requests;
    active_indices.reserve(products_.size());
    request_ids.reserve(products_.size());
    requests.reserve(products_.size());
    for (std::size_t i = 0; i < products_.size(); ++i) {
      if (is_quarantined(i)) {
        continue;
      }
      active_indices.push_back(i);
      request_ids.push_back(next_request_id(UsdMReferenceKind::Instrument,
                                            products_[i].key.symbol));
      requests.push_back({products_[i].key.symbol, request_ids.back()});
    }
    if (requests.empty()) {
      return;
    }

    const auto batch_result = parse_usdm_instrument_definitions_batch(
        body.body, received_at, requests);
    if (std::holds_alternative<UsdMReferenceParseError>(batch_result)) {
      for (std::size_t i = 0; i < active_indices.size(); ++i) {
        record_request(true, false);
      }
      return;
    }

    const auto &batch = std::get<UsdMInstrumentBatch>(batch_result);
    for (std::size_t offset = 0; offset < active_indices.size(); ++offset) {
      const auto product_index = active_indices[offset];
      const auto &product = products_[product_index];
      const auto &parsed = batch.symbols[offset];
      const bool guard_enabled =
          product.baseline_numeric_spec.has_value() ||
          static_cast<bool>(product.on_incompatible_definition);
      if (guard_enabled) {
        const auto reason = check_incompatible_definition(
            product, baseline_precisions_[product_index], parsed);
        if (reason.has_value()) {
          {
            std::lock_guard lock{mutex_};
            quarantined_[product_index] = true;
          }
          auto error_observation = make_error_observation(
              UsdMReferenceKind::Instrument, received_at, request_ids[offset],
              product.key.symbol, "incompatible:" + *reason);
          static_cast<void>(publish(product, std::move(error_observation)));
          record_request(true, false);
          if (product.on_incompatible_definition) {
            product.on_incompatible_definition(*reason);
          }
          continue;
        }
      }
      if (const auto *observation =
              std::get_if<market::ReferenceObservation>(&parsed)) {
        record_baseline_precision(product_index, *observation);
        const auto published = publish(product, *observation);
        record_request(false, published);
      } else {
        record_request(true, false);
      }
    }
  }

  void run() {
    publish_seed_instruments();
    std::vector<Task> tasks;
    tasks.reserve(products_.size() * kPeriodicKinds.size());
    const auto started = std::chrono::steady_clock::now();
    std::size_t ordinal = 0U;
    for (std::size_t product = 0; product < products_.size(); ++product) {
      for (const auto kind : kPeriodicKinds) {
        tasks.push_back(
            {product, kind, started + options_.initial_spacing * ordinal});
        ++ordinal;
      }
    }
    auto exchange_info_due = started + options_.exchange_info_period;
    std::unique_lock lock{mutex_};
    while (!stop_requested_) {
      auto selected = std::min_element(tasks.begin(), tasks.end(),
                                       [](const auto &left, const auto &right) {
                                         return left.due < right.due;
                                       });
      const auto exchange_info_first = exchange_info_due < selected->due;
      const auto due = exchange_info_first ? exchange_info_due : selected->due;
      if (condition_.wait_until(lock, due,
                                [this] { return stop_requested_; })) {
        break;
      }
      if (exchange_info_first) {
        lock.unlock();
        execute_exchange_info_refresh();
        lock.lock();
        exchange_info_due =
            std::chrono::steady_clock::now() + options_.exchange_info_period;
        continue;
      }
      const auto task = *selected;
      lock.unlock();
      execute_periodic(task.product, task.kind);
      lock.lock();
      selected->due =
          std::chrono::steady_clock::now() + options_.periodic_period;
    }
  }

  const std::vector<UsdMReferenceProduct> products_;
  const std::vector<UsdMReferenceSeed> seeds_;
  const runtime::RuntimeClock clock_;
  const UsdMReferenceOptions options_;
  UsdMReferenceSources sources_;

  std::vector<std::optional<std::pair<std::int32_t, std::int32_t>>>
      baseline_precisions_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  UsdMReferenceObservation observation_;
  std::vector<bool> quarantined_;
  std::uint64_t next_request_sequence_{1U};
  std::thread thread_;
  bool stop_requested_{false};
};

UsdMReferenceService::UsdMReferenceService(
    std::vector<UsdMReferenceProduct> products,
    std::vector<UsdMReferenceSeed> seeds, runtime::RuntimeClock clock,
    UsdMReferenceOptions options, UsdMReferenceSources sources)
    : impl_{std::make_unique<Impl>(std::move(products), std::move(seeds),
                                   std::move(clock), std::move(options),
                                   std::move(sources))} {}

UsdMReferenceService::~UsdMReferenceService() = default;

bool UsdMReferenceService::start() { return impl_->start(); }

void UsdMReferenceService::stop() noexcept { impl_->stop(); }

UsdMReferenceObservation UsdMReferenceService::observe() const noexcept {
  return impl_->observe();
}

void UsdMReferenceService::poll_once_for_testing() {
  impl_->poll_once_for_testing();
}

} // namespace binance_market_data::gateway::transport
