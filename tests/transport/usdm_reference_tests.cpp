#include "publication/event_publication.hpp"
#include "transport/public_api_budget.hpp"
#include "transport/usdm_reference.hpp"

#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace publication = binance_market_data::gateway::publication;
namespace protocol = binance_market_data::gateway::protocol;
namespace rt = binance_market_data::gateway::runtime;
namespace transport = binance_market_data::gateway::transport;
namespace common = binance_market_data::common::v1;
namespace market = binance_market_data::market::v1;
namespace gateway_wire = binance_market_data::gateway::v1;

namespace {

class TestFailure final : public std::exception {
public:
  explicit TestFailure(std::string message) : message_{std::move(message)} {}
  [[nodiscard]] const char *what() const noexcept override {
    return message_.c_str();
  }

private:
  std::string message_;
};

void require(bool condition, std::string_view expression) {
  if (!condition) {
    throw TestFailure{std::string{expression}};
  }
}

template <typename Actual, typename Expected>
void require_equal(const Actual &actual, const Expected &expected,
                   std::string_view expression) {
  if (!(actual == expected)) {
    throw TestFailure{std::string{expression}};
  }
}

#define REQUIRE(value) require((value), #value)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] rt::RuntimeClock advancing_clock() {
  auto next = std::make_shared<std::uint64_t>(1000U);
  return [next] {
    const auto value = (*next)++;
    return rt::ClockSample{value, value + 10000U};
  };
}

[[nodiscard]] transport::UsdMReferenceParseResult
parse_ratio(transport::UsdMReferenceKind kind, std::string_view body,
            std::uint64_t received = 500U) {
  return transport::parse_usdm_reference_payload(
      kind, body, rt::ClockSample{received, received + 10U}, "ref-test",
      "BTCUSDT");
}

[[nodiscard]] std::string exchange_info() {
  return R"json({
    "serverTime": 1700000000000,
    "rateLimits": [
      {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400}
    ],
    "symbols": [{
      "symbol":"BTCUSDT",
      "contractType":"PERPETUAL",
      "status":"TRADING",
      "baseAsset":"BTC",
      "quoteAsset":"USDT",
      "marginAsset":"USDT",
      "pricePrecision":2,
      "quantityPrecision":3,
      "onboardDate":1500000000000,
      "filters":[
        {"filterType":"PRICE_FILTER","tickSize":"0.10"},
        {"filterType":"LOT_SIZE","stepSize":"0.001"},
        {"filterType":"MIN_NOTIONAL","notional":"5"},
        {"filterType":"MAX_NUM_ORDERS","limit":200}
      ]
    }]
  })json";
}

void parses_typed_periodic_observations() {
  const auto open_interest = parse_ratio(
      transport::UsdMReferenceKind::OpenInterest,
      R"json({"openInterest":"12345.678","time":1700000008000})json");
  REQUIRE(std::holds_alternative<market::ReferenceObservation>(open_interest));
  const auto &oi = std::get<market::ReferenceObservation>(open_interest);
  REQUIRE_EQ(oi.schema_version(), "reference-observation.v1");
  REQUIRE_EQ(oi.source_kind(), common::REFERENCE_SOURCE_KIND_OPEN_INTEREST);
  REQUIRE_EQ(oi.status(), common::REFERENCE_OBSERVATION_STATUS_VALID);
  REQUIRE_EQ(oi.receive_time_utc_ns(), 500U);
  REQUIRE_EQ(oi.source_time_ms(), 1700000008000U);
  REQUIRE_EQ(oi.open_interest().open_interest(), "12345.678");

  const auto account = parse_ratio(
      transport::UsdMReferenceKind::TopLongShortAccountRatio,
      R"json([{"symbol":"BTCUSDT","longShortRatio":"1.9461","longAccount":"0.6606","shortAccount":"0.3394","timestamp":1700000008000}])json");
  REQUIRE(std::holds_alternative<market::ReferenceObservation>(account));
  const auto &ratios = std::get<market::ReferenceObservation>(account);
  REQUIRE_EQ(ratios.source_kind(),
             common::REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO);
  REQUIRE_EQ(ratios.top_long_short_account_ratio().long_short_ratio(),
             "1.9461");
  REQUIRE_EQ(ratios.top_long_short_account_ratio().period(), "5m");
  REQUIRE(!ratios.has_top_long_short_position_ratio());

  const auto taker = parse_ratio(
      transport::UsdMReferenceKind::TakerBuySellVolume,
      R"json([{"buySellRatio":"0.9004","buyVol":"290.309","sellVol":"322.429","timestamp":1700000008000}])json");
  REQUIRE(std::holds_alternative<market::ReferenceObservation>(taker));
  const auto &volume = std::get<market::ReferenceObservation>(taker);
  REQUIRE_EQ(volume.taker_buy_sell_volume().buy_volume(), "290.309");
  REQUIRE_EQ(volume.taker_buy_sell_volume().sell_volume(), "322.429");

  const auto empty = parse_ratio(
      transport::UsdMReferenceKind::GlobalLongShortAccountRatio, "[]");
  REQUIRE(std::holds_alternative<market::ReferenceObservation>(empty));
  REQUIRE_EQ(std::get<market::ReferenceObservation>(empty).status(),
             common::REFERENCE_OBSERVATION_STATUS_EMPTY);
  REQUIRE(!std::get<market::ReferenceObservation>(empty)
               .has_global_long_short_account_ratio());

  const auto invalid = parse_ratio(transport::UsdMReferenceKind::OpenInterest,
                                   R"json({"openInterest":""})json");
  REQUIRE(std::holds_alternative<transport::UsdMReferenceParseError>(invalid));
}

void parses_instrument_definition() {
  const auto parsed = transport::parse_usdm_instrument_definition(
      exchange_info(), rt::ClockSample{600U, 700U}, "instrument-test",
      "BTCUSDT");
  REQUIRE(std::holds_alternative<market::ReferenceObservation>(parsed));
  const auto &observation = std::get<market::ReferenceObservation>(parsed);
  REQUIRE_EQ(observation.source_kind(),
             common::REFERENCE_SOURCE_KIND_INSTRUMENT);
  REQUIRE(observation.instrument().symbol_present());
  REQUIRE_EQ(observation.instrument().contract_type(), "PERPETUAL");
  REQUIRE_EQ(observation.instrument().trading_status(), "TRADING");
  REQUIRE_EQ(observation.instrument().base_asset(), "BTC");
  REQUIRE_EQ(observation.instrument().price_precision(), 2);
  REQUIRE_EQ(observation.instrument().quantity_precision(), 3);
  REQUIRE_EQ(observation.instrument().onboard_time_ms(), 1500000000000U);
  REQUIRE_EQ(observation.instrument().filters_size(), 4);
  REQUIRE_EQ(observation.instrument().filters(0).filter_type(), "PRICE_FILTER");
  REQUIRE_EQ(observation.instrument().filters(0).price(), "0.10");
  REQUIRE_EQ(observation.instrument().filters(2).notional(), "5");
  REQUIRE_EQ(observation.instrument().filters(3).limit(), 200U);
  REQUIRE_EQ(observation.instrument().rate_limits_size(), 1);
  REQUIRE_EQ(observation.instrument().rate_limits(0).limit(), 2400U);
  REQUIRE_EQ(observation.instrument().server_time_ms(), 1700000000000U);
}

void service_caches_each_source_and_preserves_sequences() {
  const rt::MarketKey key{common::VENUE_BINANCE, common::MARKET_USD_M_PERPETUAL,
                          "BTCUSDT"};
  publication::EventPublication publication{"gw-reference", advancing_clock()};
  REQUIRE(publication.open_generation(1U));

  transport::UsdMReferenceSources sources;
  sources.fetch = [](std::string_view target) {
    const auto received = rt::ClockSample{9000U, 9100U};
    if (target.find("openInterest") != std::string_view::npos) {
      return transport::HttpsJsonResult{transport::HttpsJsonResponse{
          R"json({"openInterest":"10","time":8000})json", received, true}};
    }
    if (target.find("topLongShortAccountRatio") != std::string_view::npos) {
      return transport::HttpsJsonResult{transport::HttpsJsonResponse{
          R"json([{"longShortRatio":"1.1","longAccount":"0.5","shortAccount":"0.5","timestamp":8001}])json",
          received, true}};
    }
    if (target.find("topLongShortPositionRatio") != std::string_view::npos) {
      return transport::HttpsJsonResult{transport::HttpsJsonResponse{
          R"json([{"longShortRatio":"1.2","longAccount":"0.6","shortAccount":"0.4","timestamp":8002}])json",
          received, true}};
    }
    if (target.find("globalLongShortAccountRatio") != std::string_view::npos) {
      return transport::HttpsJsonResult{transport::HttpsJsonResponse{
          R"json([{"longShortRatio":"1.3","longAccount":"0.7","shortAccount":"0.3","timestamp":8003}])json",
          received, true}};
    }
    if (target.find("takerlongshortRatio") != std::string_view::npos) {
      return transport::HttpsJsonResult{transport::HttpsJsonResponse{
          R"json([{"buySellRatio":"0.9","buyVol":"2","sellVol":"3","timestamp":8004}])json",
          received, true}};
    }
    return transport::HttpsJsonResult{
        transport::NetworkError{transport::NetworkErrorCode::HttpStatus,
                                "reference-test",
                                "unexpected target",
                                500U,
                                {}}};
  };
  sources.exchange_info_fetch = [] {
    return transport::ExchangeInfoResult{transport::ExchangeInfoResponse{
        exchange_info(), true, rt::ClockSample{9000U, 9100U}}};
  };

  transport::UsdMReferenceOptions options;
  options.initial_spacing = std::chrono::milliseconds{0};
  transport::UsdMReferenceService service{{{key, &publication}},
                                          {},
                                          advancing_clock(),
                                          options,
                                          std::move(sources)};
  service.poll_once_for_testing();

  auto admitted =
      publication.admit({"ref-request", common::STREAM_REFERENCE_DATA,
                         publication::kReferenceObservationSchema});
  REQUIRE(
      std::holds_alternative<publication::AcceptedEventSubscription>(admitted));
  const auto channel =
      std::get<publication::AcceptedEventSubscription>(admitted).channel;

  std::vector<common::ReferenceSourceKind> kinds;
  std::uint64_t expected_sequence = 1U;
  for (;;) {
    auto record = channel->peek();
    if (!record.has_value()) {
      break;
    }
    REQUIRE(record.ordinary != nullptr);
    REQUIRE_EQ(record.ordinary->session_sequence, expected_sequence);
    ++expected_sequence;
    if (std::holds_alternative<gateway_wire::SubscriptionAccepted>(
            record.ordinary->payload)) {
      REQUIRE_EQ(channel->acknowledge(record),
                 publication::EventAcknowledgeResult::Acknowledged);
      continue;
    }
    const auto &event =
        std::get<std::shared_ptr<const protocol::NormalizedMarketEvent>>(
            record.ordinary->payload);
    if (const auto *reference =
            std::get_if<market::ReferenceObservation>(event.get())) {
      kinds.push_back(reference->source_kind());
      REQUIRE_EQ(reference->receive_time_utc_ns(), 9000U);
      REQUIRE(reference->has_source_time_ms());
    }
    REQUIRE_EQ(channel->acknowledge(record),
               publication::EventAcknowledgeResult::Acknowledged);
  }
  REQUIRE_EQ(kinds.size(), 6U);
  REQUIRE_EQ(kinds[0], common::REFERENCE_SOURCE_KIND_OPEN_INTEREST);
  REQUIRE_EQ(kinds[1],
             common::REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO);
  REQUIRE_EQ(kinds[2],
             common::REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO);
  REQUIRE_EQ(kinds[3],
             common::REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO);
  REQUIRE_EQ(kinds[4], common::REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME);
  REQUIRE_EQ(kinds[5], common::REFERENCE_SOURCE_KIND_INSTRUMENT);
  REQUIRE_EQ(expected_sequence, 8U);
}

void service_publishes_explicit_error_and_empty_states() {
  const rt::MarketKey key{common::VENUE_BINANCE, common::MARKET_USD_M_PERPETUAL,
                          "BTCUSDT"};
  publication::EventPublication publication{"gw-reference-errors",
                                            advancing_clock()};
  REQUIRE(publication.open_generation(1U));

  auto live_admitted =
      publication.admit({"ref-errors-live", common::STREAM_REFERENCE_DATA,
                         publication::kReferenceObservationSchema});
  REQUIRE(std::holds_alternative<publication::AcceptedEventSubscription>(
      live_admitted));
  const auto live_channel =
      std::get<publication::AcceptedEventSubscription>(live_admitted).channel;

  transport::UsdMReferenceSources sources;
  sources.fetch = [](std::string_view target) {
    if (target.find("openInterest") != std::string_view::npos) {
      return transport::HttpsJsonResult{
          transport::NetworkError{transport::NetworkErrorCode::HttpStatus,
                                  "depth-http-status", "failed", 429U, "5"}};
    }
    return transport::HttpsJsonResult{transport::HttpsJsonResponse{
        "[]", rt::ClockSample{9000U, 9100U}, true}};
  };
  sources.exchange_info_fetch = [] {
    return transport::ExchangeInfoResult{
        transport::NetworkError{transport::NetworkErrorCode::HttpStatus,
                                "exchange-info",
                                "refresh failed",
                                500U,
                                {}}};
  };
  transport::UsdMReferenceService service{
      {{key, &publication}}, {}, advancing_clock(), {}, std::move(sources)};
  service.poll_once_for_testing();

  {
    auto accepted = live_channel->peek();
    REQUIRE(accepted.has_value());
    REQUIRE(std::holds_alternative<gateway_wire::SubscriptionAccepted>(
        accepted.ordinary->payload));
    REQUIRE_EQ(live_channel->acknowledge(accepted),
               publication::EventAcknowledgeResult::Acknowledged);

    auto first = live_channel->peek();
    REQUIRE(first.has_value());
    const auto &error_event =
        std::get<std::shared_ptr<const protocol::NormalizedMarketEvent>>(
            first.ordinary->payload);
    REQUIRE(std::holds_alternative<market::ReferenceObservation>(*error_event));
    const auto &error = std::get<market::ReferenceObservation>(*error_event);
    REQUIRE_EQ(error.status(), common::REFERENCE_OBSERVATION_STATUS_ERROR);
    REQUIRE_EQ(error.error_code(), "depth-http-status-429");
    REQUIRE(!error.has_open_interest());
    REQUIRE_EQ(live_channel->acknowledge(first),
               publication::EventAcknowledgeResult::Acknowledged);

    auto second = live_channel->peek();
    REQUIRE(second.has_value());
    const auto &empty_event =
        std::get<std::shared_ptr<const protocol::NormalizedMarketEvent>>(
            second.ordinary->payload);
    REQUIRE_EQ(std::get<market::ReferenceObservation>(*empty_event).status(),
               common::REFERENCE_OBSERVATION_STATUS_EMPTY);
    REQUIRE(!std::get<market::ReferenceObservation>(*empty_event)
                 .has_global_long_short_account_ratio());
  }

  // ERROR-only source is never cached as initial value for late subscribers.
  auto late_admitted =
      publication.admit({"ref-errors-late", common::STREAM_REFERENCE_DATA,
                         publication::kReferenceObservationSchema});
  REQUIRE(std::holds_alternative<publication::AcceptedEventSubscription>(
      late_admitted));
  const auto late_channel =
      std::get<publication::AcceptedEventSubscription>(late_admitted).channel;
  auto accepted = late_channel->peek();
  REQUIRE(accepted.has_value());
  REQUIRE_EQ(late_channel->acknowledge(accepted),
             publication::EventAcknowledgeResult::Acknowledged);

  std::vector<common::ReferenceSourceKind> late_kinds;
  for (;;) {
    auto record = late_channel->peek();
    if (!record.has_value()) {
      break;
    }
    const auto &event =
        std::get<std::shared_ptr<const protocol::NormalizedMarketEvent>>(
            record.ordinary->payload);
    const auto &obs = std::get<market::ReferenceObservation>(*event);
    REQUIRE_EQ(obs.status(), common::REFERENCE_OBSERVATION_STATUS_EMPTY);
    late_kinds.push_back(obs.source_kind());
    REQUIRE_EQ(late_channel->acknowledge(record),
               publication::EventAcknowledgeResult::Acknowledged);
  }
  REQUIRE_EQ(late_kinds.size(), 4U);
  for (const auto kind : late_kinds) {
    REQUIRE(kind != common::REFERENCE_SOURCE_KIND_OPEN_INTEREST);
  }
}

void valid_then_error_preserves_cached_valid_for_new_subscriber() {
  const rt::MarketKey key{common::VENUE_BINANCE, common::MARKET_USD_M_PERPETUAL,
                          "BTCUSDT"};
  publication::EventPublication publication{"gw-ref-preserve",
                                            advancing_clock()};
  REQUIRE(publication.open_generation(1U));

  bool fail_open_interest = false;
  transport::UsdMReferenceSources sources;
  sources.fetch = [&fail_open_interest](std::string_view target) {
    const auto received = rt::ClockSample{9000U, 9100U};
    if (target.find("openInterest") != std::string_view::npos) {
      if (fail_open_interest) {
        return transport::HttpsJsonResult{
            transport::NetworkError{transport::NetworkErrorCode::HttpStatus,
                                    "oi-http",
                                    "failed",
                                    429U,
                                    {}}};
      }
      return transport::HttpsJsonResult{transport::HttpsJsonResponse{
          R"json({"openInterest":"42.5","time":8000})json", received, true}};
    }
    return transport::HttpsJsonResult{
        transport::HttpsJsonResponse{"[]", received, true}};
  };
  sources.exchange_info_fetch = [] {
    return transport::ExchangeInfoResult{transport::ExchangeInfoResponse{
        exchange_info(), true, rt::ClockSample{9000U, 9100U}}};
  };

  transport::UsdMReferenceService service{
      {{key, &publication}}, {}, advancing_clock(), {}, std::move(sources)};
  service.poll_once_for_testing();
  fail_open_interest = true;
  service.poll_once_for_testing();

  auto late_admitted =
      publication.admit({"ref-late-valid", common::STREAM_REFERENCE_DATA,
                         publication::kReferenceObservationSchema});
  REQUIRE(std::holds_alternative<publication::AcceptedEventSubscription>(
      late_admitted));
  const auto late_channel =
      std::get<publication::AcceptedEventSubscription>(late_admitted).channel;
  auto accepted = late_channel->peek();
  REQUIRE(accepted.has_value());
  REQUIRE_EQ(late_channel->acknowledge(accepted),
             publication::EventAcknowledgeResult::Acknowledged);

  bool saw_cached_valid_oi = false;
  for (;;) {
    auto record = late_channel->peek();
    if (!record.has_value()) {
      break;
    }
    const auto &event =
        std::get<std::shared_ptr<const protocol::NormalizedMarketEvent>>(
            record.ordinary->payload);
    const auto &obs = std::get<market::ReferenceObservation>(*event);
    if (obs.source_kind() == common::REFERENCE_SOURCE_KIND_OPEN_INTEREST) {
      REQUIRE_EQ(obs.status(), common::REFERENCE_OBSERVATION_STATUS_VALID);
      REQUIRE_EQ(obs.open_interest().open_interest(), "42.5");
      saw_cached_valid_oi = true;
    }
    REQUIRE_EQ(late_channel->acknowledge(record),
               publication::EventAcknowledgeResult::Acknowledged);
  }
  REQUIRE(saw_cached_valid_oi);
}

void parses_full_instrument_filter_fields() {
  const std::string payload = R"json({
    "serverTime": 1700000000000,
    "symbols": [{
      "symbol":"BTCUSDT",
      "contractType":"PERPETUAL",
      "status":"TRADING",
      "baseAsset":"BTC",
      "quoteAsset":"USDT",
      "marginAsset":"USDT",
      "pricePrecision":2,
      "quantityPrecision":3,
      "filters":[
        {"filterType":"PRICE_FILTER","minPrice":"556.80","maxPrice":"4529764","tickSize":"0.10"},
        {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
        {"filterType":"PERCENT_PRICE","multiplierUp":"1.1500","multiplierDown":"0.8500","multiplierDecimal":"4"},
        {"filterType":"POSITION_RISK_CONTROL","positionControlSide":"NONE"}
      ]
    }]
  })json";
  const auto parsed = transport::parse_usdm_instrument_definition(
      payload, rt::ClockSample{600U, 700U}, "full-filters", "BTCUSDT");
  REQUIRE(std::holds_alternative<market::ReferenceObservation>(parsed));
  const auto &inst =
      std::get<market::ReferenceObservation>(parsed).instrument();
  REQUIRE_EQ(inst.filters_size(), 4);
  REQUIRE_EQ(inst.filters(0).tick_size(), "0.10");
  REQUIRE_EQ(inst.filters(0).min_price(), "556.80");
  REQUIRE_EQ(inst.filters(0).max_price(), "4529764");
  REQUIRE_EQ(inst.filters(1).step_size(), "0.001");
  REQUIRE_EQ(inst.filters(1).min_quantity(), "0.001");
  REQUIRE_EQ(inst.filters(1).max_quantity(), "1000");
  REQUIRE_EQ(inst.filters(2).multiplier_up(), "1.1500");
  REQUIRE_EQ(inst.filters(2).multiplier_down(), "0.8500");
  REQUIRE_EQ(inst.filters(2).multiplier_decimal(), "4");
  REQUIRE_EQ(inst.filters(3).position_control_side(), "NONE");
}

void batch_parses_five_symbols_with_selected_precision_and_missing_error() {
  // Deliberately include top-level pricePrecision/quantityPrecision that differ
  // from per-symbol values to prove batch parse strictly uses *selected.
  const std::string payload = R"json({
    "serverTime": 1700000000000,
    "pricePrecision": 99,
    "quantityPrecision": 99,
    "symbols": [
      {"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":2,"quantityPrecision":3,"filters":[]},
      {"symbol":"ETHUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":2,"quantityPrecision":3,"filters":[]},
      {"symbol":"BNBUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BNB","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":2,"quantityPrecision":2,"filters":[]},
      {"symbol":"XRPUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"XRP","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":4,"quantityPrecision":1,"filters":[]},
      {"symbol":"ADAUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"ADA","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":5,"quantityPrecision":0,"filters":[]}
    ]
  })json";
  const std::array<transport::UsdMInstrumentBatchRequest, 6> requests{{
      {"BTCUSDT", "req-0"},
      {"ETHUSDT", "req-1"},
      {"BNBUSDT", "req-2"},
      {"XRPUSDT", "req-3"},
      {"ADAUSDT", "req-4"},
      {"SOLUSDT", "req-5"},
  }};
  const auto batch_result = transport::parse_usdm_instrument_definitions_batch(
      payload, rt::ClockSample{600U, 700U}, requests);
  REQUIRE(std::holds_alternative<transport::UsdMInstrumentBatch>(batch_result));
  const auto &batch = std::get<transport::UsdMInstrumentBatch>(batch_result);
  REQUIRE_EQ(batch.symbols.size(), 6U);
  const std::array<std::pair<int, int>, 5> expected_precisions{
      {{2, 3}, {2, 3}, {2, 2}, {4, 1}, {5, 0}}};
  for (std::size_t i = 0; i < 5U; ++i) {
    REQUIRE(
        std::holds_alternative<market::ReferenceObservation>(batch.symbols[i]));
    const auto &obs = std::get<market::ReferenceObservation>(batch.symbols[i]);
    REQUIRE_EQ(obs.symbol(), requests[i].symbol);
    REQUIRE_EQ(obs.request_id(), requests[i].request_id);
    REQUIRE_EQ(obs.instrument().price_precision(),
               expected_precisions[i].first);
    REQUIRE_EQ(obs.instrument().quantity_precision(),
               expected_precisions[i].second);
  }
  REQUIRE(std::holds_alternative<transport::UsdMReferenceParseError>(
      batch.symbols[5]));
  REQUIRE_EQ(
      std::get<transport::UsdMReferenceParseError>(batch.symbols[5]).field,
      "symbol");
}

void quarantines_only_incompatible_usdm_product() {
  const rt::MarketKey btc_key{common::VENUE_BINANCE,
                              common::MARKET_USD_M_PERPETUAL, "BTCUSDT"};
  const rt::MarketKey eth_key{common::VENUE_BINANCE,
                              common::MARKET_USD_M_PERPETUAL, "ETHUSDT"};
  publication::EventPublication btc_pub{"gw-q-btc", advancing_clock()};
  publication::EventPublication eth_pub{"gw-q-eth", advancing_clock()};
  REQUIRE(btc_pub.open_generation(1U));
  REQUIRE(eth_pub.open_generation(1U));

  const auto price_scale =
      binance_market_data::projection::v1::DecimalScale::create(1U);
  const auto qty_scale =
      binance_market_data::projection::v1::DecimalScale::create(3U);
  REQUIRE(price_scale.has_value() && qty_scale.has_value());
  const binance_market_data::projection::v1::NumericSpec baseline_spec{
      *price_scale, *qty_scale};

  std::string btc_reason;
  std::string eth_reason;
  int phase = 0;
  transport::UsdMReferenceSources sources;
  sources.fetch = [](std::string_view) {
    return transport::HttpsJsonResult{transport::HttpsJsonResponse{
        "[]", rt::ClockSample{9000U, 9100U}, true}};
  };
  sources.exchange_info_fetch = [&phase] {
    const std::string btc_tick = phase == 0 ? "0.10" : "0.01";
    const std::string body =
        R"json({"serverTime":1700000001000,"symbols":[
          {"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":2,"quantityPrecision":3,"filters":[{"filterType":"PRICE_FILTER","tickSize":")json" +
        btc_tick +
        R"json("},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},
          {"symbol":"ETHUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":2,"quantityPrecision":3,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}
        ]})json";
    return transport::ExchangeInfoResult{transport::ExchangeInfoResponse{
        body, true, rt::ClockSample{9500U, 9600U}}};
  };

  transport::UsdMReferenceService service{
      {{btc_key, &btc_pub, baseline_spec,
        [&btc_reason](std::string_view r) { btc_reason = std::string{r}; }},
       {eth_key, &eth_pub, baseline_spec,
        [&eth_reason](std::string_view r) { eth_reason = std::string{r}; }}},
      {},
      advancing_clock(),
      {},
      std::move(sources)};

  service.poll_once_for_testing();
  REQUIRE(btc_reason.empty());
  REQUIRE(eth_reason.empty());

  phase = 1;
  service.poll_once_for_testing();
  REQUIRE_EQ(btc_reason, "numeric_spec_changed");
  REQUIRE(eth_reason.empty());
}

void reference_budget_counts_zero_weight_attempts() {
  transport::detail::PublicApiBudget budget;
  const auto zero = transport::detail::PublicApiBudget::Clock::time_point{};
  for (unsigned index = 0; index < 200U; ++index) {
    REQUIRE_EQ(budget.admit(true, false, 0U, zero),
               transport::detail::PublicApiBudget::Duration::zero());
  }
  REQUIRE(budget.admit(true, false, 0U, zero) >
          transport::detail::PublicApiBudget::Duration::zero());
  REQUIRE_EQ(transport::detail::public_rest_weight(
                 "fapi.binance.com", "/futures/data/topLongShortAccountRatio"),
             0U);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"TYPED_PERIODIC_OBSERVATIONS", parses_typed_periodic_observations},
      {"INSTRUMENT_DEFINITION", parses_instrument_definition},
      {"SERVICE_CACHE_AND_SEQUENCES",
       service_caches_each_source_and_preserves_sequences},
      {"SERVICE_ERROR_AND_EMPTY_STATES",
       service_publishes_explicit_error_and_empty_states},
      {"VALID_THEN_ERROR_PRESERVES_CACHE",
       valid_then_error_preserves_cached_valid_for_new_subscriber},
      {"FULL_INSTRUMENT_FILTER_FIELDS", parses_full_instrument_filter_fields},
      {"BATCH_FIVE_SYMBOLS_AND_MISSING_ERROR",
       batch_parses_five_symbols_with_selected_precision_and_missing_error},
      {"QUARANTINES_ONLY_INCOMPATIBLE_PRODUCT",
       quarantines_only_incompatible_usdm_product},
      {"ZERO_WEIGHT_REQUEST_BUDGET",
       reference_budget_counts_zero_weight_attempts},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &failure) {
      std::cerr << name << "=FAIL " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
