#include "protocol/usdm_protocol.hpp"
#include "runtime/market_runtime.hpp"
#include "support/usdm_time_fixture.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <nlohmann/json.hpp>

#include <exception>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
namespace protocol = binance_market_data::gateway::protocol;
namespace rt = binance_market_data::gateway::runtime;

namespace {

namespace common_wire = binance_market_data::common::v1;

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

#define REQUIRE(condition) require((condition), #condition)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] std::string
target_symbol(std::string_view contract_type = "PERPETUAL",
              std::string_view status = "TRADING",
              std::string_view filters = R"json(
      {"filterType":"MAX_NUM_ORDERS","limit":200},
      {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
      {"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000","tickSize":"0.10"}
    )json",
              std::string_view symbol = "BTCUSDT") {
  return std::string{R"json({"symbol":")json"} + std::string{symbol} +
         R"json(","pair":")json" + std::string{symbol} +
         R"json(","contractType":")json" + std::string{contract_type} +
         R"json(","status":")json" + std::string{status} +
         R"json(","pricePrecision":17,"quantityPrecision":19,"filters":[)json" +
         std::string{filters} + "]}";
}

[[nodiscard]] std::string exchange_info_for(std::string target,
                                            std::string_view unrelated_symbol) {
  return std::string{R"json({"timezone":"UTC","symbols":[{"symbol":")json"} +
         std::string{unrelated_symbol} +
         R"json(","contractType":"PERPETUAL","status":"TRADING","filters":[]},)json" +
         std::move(target) +
         R"json(,{"symbol":"BTCUSDC","contractType":"PERPETUAL","status":"TRADING","filters":[]}]})json";
}

[[nodiscard]] std::string exchange_info(std::string target) {
  return exchange_info_for(std::move(target), "ETHUSDT");
}

[[nodiscard]] const protocol::ProtocolError &
require_metadata_error(const protocol::UsdMMetadataResult &result) {
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(result));
  return std::get<protocol::ProtocolError>(result);
}

void exchange_info_selects_exact_perpetual() {
  const auto parsed =
      protocol::parse_usdm_exchange_info(exchange_info(target_symbol()));
  REQUIRE(std::holds_alternative<protocol::UsdMMetadata>(parsed));
  const auto &metadata = std::get<protocol::UsdMMetadata>(parsed);
  REQUIRE_EQ(metadata.tick_size, "0.10");
  REQUIRE_EQ(metadata.step_size, "0.001");
  REQUIRE_EQ(metadata.numeric_spec.price_scale.value(), 1U);
  REQUIRE_EQ(metadata.numeric_spec.quantity_scale.value(), 3U);
  REQUIRE(metadata.numeric_spec.price_scale.value() != 17U);
  REQUIRE(metadata.numeric_spec.quantity_scale.value() != 19U);

  REQUIRE_EQ(
      require_metadata_error(protocol::parse_usdm_exchange_info(exchange_info(
                                 target_symbol("CURRENT_QUARTER"))))
          .field,
      "contractType");
  REQUIRE_EQ(
      require_metadata_error(protocol::parse_usdm_exchange_info(exchange_info(
                                 target_symbol("PERPETUAL", "SETTLING"))))
          .field,
      "status");
}

void exact_symbol_metadata_selection() {
  const auto eth = protocol::parse_usdm_exchange_info(
      exchange_info_for(target_symbol("PERPETUAL", "TRADING",
                                      R"json(
      {"filterType":"LOT_SIZE","stepSize":"0.001"},
      {"filterType":"PRICE_FILTER","tickSize":"0.10"}
    )json",
                                      "ETHUSDT"),
                        "BTCUSDT"),
      "ETHUSDT");
  REQUIRE(std::holds_alternative<protocol::UsdMMetadata>(eth));
  const auto &eth_metadata = std::get<protocol::UsdMMetadata>(eth);
  REQUIRE_EQ(eth_metadata.numeric_spec.price_scale.value(), 1U);
  REQUIRE_EQ(eth_metadata.numeric_spec.quantity_scale.value(), 3U);

  const auto btc = protocol::parse_usdm_exchange_info(
      exchange_info(target_symbol()), "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::UsdMMetadata>(btc));
  REQUIRE_EQ(
      std::get<protocol::UsdMMetadata>(btc).numeric_spec.price_scale.value(),
      1U);

  REQUIRE_EQ(
      require_metadata_error(protocol::parse_usdm_exchange_info(
                                 exchange_info(target_symbol()), "XRPUSDT"))
          .code,
      protocol::ProtocolErrorCode::InvalidMarketMetadata);
}

void batch_exchange_info_selection() {
  const auto body = [] {
    return std::string{R"json({"timezone":"UTC","symbols":[)json"} +
           target_symbol(
               "PERPETUAL", "TRADING",
               R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"})json",
               "ETHUSDT") +
           "," +
           target_symbol(
               "PERPETUAL", "TRADING",
               R"json({"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.00001"})json",
               "BTCUSDT") +
           "]}";
  }();
  const std::vector<std::string_view> requested{"BTCUSDT", "ETHUSDT"};
  const auto parsed = protocol::parse_usdm_exchange_info_batch(body, requested);
  REQUIRE(std::holds_alternative<protocol::UsdMMetadataBatch>(parsed));
  const auto &entries = std::get<protocol::UsdMMetadataBatch>(parsed).symbols;
  REQUIRE_EQ(entries.size(), 2U);
  REQUIRE(std::holds_alternative<protocol::UsdMMetadata>(entries[0]));
  REQUIRE(std::holds_alternative<protocol::UsdMMetadata>(entries[1]));
  REQUIRE_EQ(std::get<protocol::UsdMMetadata>(entries[0])
                 .numeric_spec.price_scale.value(),
             2U);
  REQUIRE_EQ(std::get<protocol::UsdMMetadata>(entries[0])
                 .numeric_spec.quantity_scale.value(),
             5U);
  REQUIRE_EQ(std::get<protocol::UsdMMetadata>(entries[1])
                 .numeric_spec.price_scale.value(),
             1U);
  REQUIRE_EQ(std::get<protocol::UsdMMetadata>(entries[1])
                 .numeric_spec.quantity_scale.value(),
             3U);

  const std::vector<std::string_view> partially_missing{"BTCUSDT", "XRPUSDT"};
  const auto missing =
      protocol::parse_usdm_exchange_info_batch(body, partially_missing);
  REQUIRE(std::holds_alternative<protocol::UsdMMetadataBatch>(missing));
  const auto &missing_entries =
      std::get<protocol::UsdMMetadataBatch>(missing).symbols;
  REQUIRE(std::holds_alternative<protocol::UsdMMetadata>(missing_entries[0]));
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(missing_entries[1]));

  const std::vector<std::string_view> duplicated{"BTCUSDT", "BTCUSDT"};
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(
      protocol::parse_usdm_exchange_info_batch(body, duplicated)));
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(
      protocol::parse_usdm_exchange_info_batch("not-json", requested)));
}

void exchange_info_filter_validation() {
  static_cast<void>(require_metadata_error(
      protocol::parse_usdm_exchange_info(exchange_info(target_symbol(
          "PERPETUAL", "TRADING",
          R"json({"filterType":"LOT_SIZE","stepSize":"0.001"})json")))));
  static_cast<void>(require_metadata_error(
      protocol::parse_usdm_exchange_info(exchange_info(target_symbol(
          "PERPETUAL", "TRADING",
          R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"})json")))));
  static_cast<void>(
      require_metadata_error(protocol::parse_usdm_exchange_info(exchange_info(
          target_symbol("PERPETUAL", "TRADING",
                        R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"},
                  {"filterType":"PRICE_FILTER","tickSize":"0.01"},
                  {"filterType":"LOT_SIZE","stepSize":"0.001"})json")))));
  static_cast<void>(
      require_metadata_error(protocol::parse_usdm_exchange_info(exchange_info(
          target_symbol("PERPETUAL", "TRADING",
                        R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"},
                  {"filterType":"LOT_SIZE","stepSize":"0.001"},
                  {"filterType":"LOT_SIZE","stepSize":"0.01"})json")))));
}

void diff_depth_present_and_absent_pu() {
  constexpr auto received_at =
      binance_market_data::gateway::runtime::ClockSample{1700000000000000000ULL,
                                                         9000000000000ULL};
  const auto present = protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1770000000001,"T":1770000000000,"s":"BTCUSDT","U":501,"u":504,"pu":499,"b":[["100.1","2.500"]],"a":[["100.2","0"]],"ps":"BTCUSDT","st":1})json",
      received_at, "binance-usdm-btcusdt-g1-1700000000000000000");
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(present));
  const auto &update = std::get<protocol::market::DepthUpdate>(present);
  REQUIRE_EQ(update.metadata().venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(update.metadata().market(), common_wire::MARKET_USD_M_PERPETUAL);
  REQUIRE_EQ(update.metadata().symbol(), "BTCUSDT");
  REQUIRE_EQ(update.metadata().stream(), common_wire::STREAM_DIFF_DEPTH);
  REQUIRE_EQ(update.metadata().exchange_event_time_ms(), 1770000000001ULL);
  REQUIRE(update.metadata().has_exchange_transaction_time_ms());
  REQUIRE_EQ(update.metadata().exchange_transaction_time_ms(),
             1770000000000ULL);
  REQUIRE(!update.metadata().has_exchange_trade_time_ms());
  REQUIRE_EQ(update.first_update_id(), 501U);
  REQUIRE_EQ(update.final_update_id(), 504U);
  REQUIRE(update.has_previous_final_update_id());
  REQUIRE_EQ(update.previous_final_update_id(), 499U);
  REQUIRE_EQ(update.bids(0).price(), "100.1");
  REQUIRE_EQ(update.asks(0).quantity(), "0");

  const auto absent = protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":900,"u":901,"b":[],"a":[]})json",
      {3U, 4U}, "usdm-connection");
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(absent));
  REQUIRE(!std::get<protocol::market::DepthUpdate>(absent)
               .has_previous_final_update_id());
}

void exact_symbol_events_and_snapshot_identity() {
  constexpr auto received_at =
      binance_market_data::gateway::runtime::ClockSample{10U, 20U};
  const auto parsed = protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1,"T":2,"s":"ETHUSDT","U":501,"u":504,"pu":499,"b":[],"a":[],"ps":"BTCUSDT","st":1})json",
      received_at, "eth-usdm-connection", "ETHUSDT");
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(parsed));
  const auto &update = std::get<protocol::market::DepthUpdate>(parsed);
  REQUIRE_EQ(update.metadata().symbol(), "ETHUSDT");
  REQUIRE(update.has_previous_final_update_id());
  REQUIRE_EQ(update.previous_final_update_id(), 499U);

  REQUIRE(std::holds_alternative<
          protocol::ProtocolError>(protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})json",
      received_at, "eth-usdm-connection", "ETHUSDT")));
  const auto invalid_pair = protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1,"s":"ETHUSDT","U":1,"u":2,"pu":0,"b":[],"a":[],"ps":123})json",
      received_at, "eth-usdm-connection", "ETHUSDT");
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(invalid_pair));
  const auto &invalid_pair_error =
      std::get<protocol::ProtocolError>(invalid_pair);
  REQUIRE_EQ(invalid_pair_error.code,
             protocol::ProtocolErrorCode::InvalidField);
  REQUIRE_EQ(invalid_pair_error.field, "ps");

  const auto snapshot = protocol::parse_usdm_depth_snapshot(
      R"json({"lastUpdateId":504,"E":1,"T":2,"bids":[],"asks":[]})json",
      received_at, "eth-usdm-snapshot", "ETHUSDT");
  REQUIRE(std::holds_alternative<protocol::market::ExchangeDepthSnapshot>(
      snapshot));
  REQUIRE_EQ(
      std::get<protocol::market::ExchangeDepthSnapshot>(snapshot).symbol(),
      "ETHUSDT");
}

void diff_depth_has_no_prior_u_classifier() {
  const auto first = protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":100,"u":110,"pu":7,"b":[],"a":[]})json",
      {1U, 2U}, "usdm-connection");
  const auto second = protocol::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":500,"u":400,"pu":999999,"b":[],"a":[]})json",
      {3U, 4U}, "usdm-connection");
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(first));
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(second));
  REQUIRE_EQ(std::get<protocol::market::DepthUpdate>(second)
                 .previous_final_update_id(),
             999999U);
}

void diff_depth_identity_and_type_rejection() {
  const std::vector<std::string_view> invalid{
      R"json({"e":"depthUpdate","E":1,"s":"ETHUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"pu":"0","b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"T":"1","s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[],"st":2})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[["1"]],"a":[]})json",
      R"json({"e":"aggTrade","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]})json",
      R"json(not-json)json",
  };
  for (const auto payload : invalid) {
    REQUIRE(std::holds_alternative<protocol::ProtocolError>(
        protocol::parse_usdm_depth_frame(payload, {1U, 2U}, "connection")));
  }
}

void snapshot_parse_and_validation() {
  const auto parsed = protocol::parse_usdm_depth_snapshot(
      R"json({"lastUpdateId":1027024,"E":1770000000001,"T":1770000000000,"bids":[["100.1","2.0"]],"asks":[["100.2","3.0"]]})json",
      {10U, 20U}, "g11-usdm-depth-request-1");
  REQUIRE(
      std::holds_alternative<protocol::market::ExchangeDepthSnapshot>(parsed));
  const auto &snapshot =
      std::get<protocol::market::ExchangeDepthSnapshot>(parsed);
  REQUIRE_EQ(snapshot.venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(snapshot.market(), common_wire::MARKET_USD_M_PERPETUAL);
  REQUIRE_EQ(snapshot.symbol(), "BTCUSDT");
  REQUIRE_EQ(snapshot.last_update_id(), 1027024U);
  REQUIRE_EQ(snapshot.exchange_transaction_time_ms(), 1770000000000ULL);
  REQUIRE_EQ(snapshot.bids(0).quantity(), "2.0");
  REQUIRE_EQ(snapshot.asks(0).price(), "100.2");

  for (const auto invalid : {
           R"json({"lastUpdateId":"1","bids":[],"asks":[]})json",
           R"json({"lastUpdateId":1,"E":"2","bids":[],"asks":[]})json",
           R"json({"lastUpdateId":1,"bids":[[1,"2"]],"asks":[]})json",
           R"json({"lastUpdateId":1,"bids":[]})json",
       }) {
    REQUIRE(std::holds_alternative<protocol::ProtocolError>(
        protocol::parse_usdm_depth_snapshot(invalid, {1U, 2U}, "request")));
  }
}

void stream_symbol_and_shutdown() {
  REQUIRE_EQ(protocol::usdm_stream_symbol("BTCUSDT"),
             std::optional<std::string>{"btcusdt"});
  REQUIRE_EQ(protocol::usdm_stream_symbol("ETHUSDT"),
             std::optional<std::string>{"ethusdt"});
  REQUIRE(!protocol::usdm_stream_symbol("btcusdt").has_value());
  REQUIRE(!protocol::usdm_stream_symbol("ETH-USDT").has_value());

  const auto shutdown = protocol::parse_usdm_depth_frame(
      R"json({"e":"serverShutdown","E":1770123456789})json", {1U, 2U},
      "connection");
  REQUIRE(std::holds_alternative<protocol::ServerShutdown>(shutdown));
}

void unrelated_extensions_preserve_strict_fields() {
  using Json = nlohmann::json;
  auto depth = Json::parse(
      R"({"e":"depthUpdate","E":1,"T":2,"s":"BTCUSDT","U":501,"u":504,"pu":499,"b":[],"a":[],"ps":"BTCUSDT","st":1,"future_extension":{"nested":true}})");
  const auto parsed =
      protocol::parse_usdm_depth_frame(depth.dump(), {1U, 2U}, "source");
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(parsed));
  const auto &update = std::get<protocol::market::DepthUpdate>(parsed);
  REQUIRE_EQ(update.first_update_id(), 501U);
  REQUIRE_EQ(update.final_update_id(), 504U);
  REQUIRE_EQ(update.previous_final_update_id(), 499U);
  for (const auto key : {"e", "E", "s", "U", "u", "b", "a"}) {
    auto missing = depth;
    missing.erase(key);
    REQUIRE(std::holds_alternative<protocol::ProtocolError>(
        protocol::parse_usdm_depth_frame(missing.dump(), {1U, 2U}, "source")));
  }
  for (const auto key :
       {"e", "E", "T", "s", "U", "u", "pu", "b", "a", "ps", "st"}) {
    auto malformed = depth;
    malformed[key] = nullptr;
    REQUIRE(std::holds_alternative<protocol::ProtocolError>(
        protocol::parse_usdm_depth_frame(malformed.dump(), {1U, 2U},
                                         "source")));
  }
  depth["s"] = "ETHUSDT";
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(
      protocol::parse_usdm_depth_frame(depth.dump(), {1U, 2U}, "source")));
  auto snapshot = Json::parse(
      R"({"lastUpdateId":504,"E":1,"T":2,"bids":[],"asks":[],"future_extension":null})");
  REQUIRE(std::holds_alternative<protocol::market::ExchangeDepthSnapshot>(
      protocol::parse_usdm_depth_snapshot(snapshot.dump(), {1U, 2U},
                                          "request")));
  snapshot["lastUpdateId"] = "504";
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(
      protocol::parse_usdm_depth_snapshot(snapshot.dump(), {1U, 2U},
                                          "request")));
  REQUIRE(std::holds_alternative<protocol::ServerShutdown>(
      protocol::parse_usdm_depth_frame(
          R"({"e":"serverShutdown","E":1,"future_extension":[]})", {1U, 2U},
          "source")));
}

void extensions_do_not_bypass_projection_sequence_checks() {
  namespace core = binance_market_data::projection::v1;
  const core::NumericSpec spec{*core::DecimalScale::create(2U),
                               *core::DecimalScale::create(3U)};
  for (
      const auto payload :
      {R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":102,"u":102,"pu":999,"b":[],"a":[],"future_extension":true})",
       R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":103,"u":102,"pu":101,"b":[],"a":[],"future_extension":true})",
       R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":102,"u":102,"b":[],"a":[],"future_extension":true})"}) {
    rt::MarketRuntime runtime{
        {8U, 8U},
        [] { return rt::ClockSample{1U, 2U}; },
        spec,
        rt::adapter::ExpectedIdentity{"BTCUSDT",
                                      core::SequencePolicyKind::UsdMPerpetual}};
    REQUIRE_EQ(runtime.start(), rt::StartResult::Started);
    auto snapshot = protocol::parse_usdm_depth_snapshot(
        R"({"lastUpdateId":100,"bids":[["100.00","1.000"]],"asks":[["101.00","1.000"]],"future_extension":true})",
        {1U, 2U}, "request");
    REQUIRE_EQ(runtime.submit_snapshot(
                   std::get<protocol::market::ExchangeDepthSnapshot>(
                       std::move(snapshot))),
               rt::AdmissionResult::Accepted);
    auto bridge = protocol::parse_usdm_depth_frame(
        R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":100,"u":101,"pu":99,"b":[],"a":[],"future_extension":true})",
        {1U, 2U}, "source");
    REQUIRE_EQ(runtime.submit_depth_update(
                   std::get<protocol::market::DepthUpdate>(std::move(bridge))),
               rt::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.observe().state, rt::RuntimeState::Live);
    auto invalid =
        protocol::parse_usdm_depth_frame(payload, {1U, 2U}, "source");
    REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(invalid));
    REQUIRE_EQ(runtime.submit_depth_update(
                   std::get<protocol::market::DepthUpdate>(std::move(invalid))),
               rt::AdmissionResult::Accepted);
    const auto rejected = runtime.observe();
    REQUIRE(rejected.state == rt::RuntimeState::NeedsResync ||
            rejected.state == rt::RuntimeState::Faulted);
    REQUIRE_EQ(rejected.last_update_id, std::optional<std::uint64_t>{101U});
  }
}

void contracts_c0_source_time_oracle() {
  namespace fixtures = binance_market_data::gateway::test_support;
  const auto fixture = fixtures::usdm_time_fixture();
  for (const auto &test_case : fixture.at("cases")) {
    const auto source = fixtures::source_case(fixture, test_case);
    const auto parsed = protocol::parse_usdm_depth_frame(
        source.dump(), {10U, 20U}, "c0-source");
    REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(parsed));
    const auto &update = std::get<protocol::market::DepthUpdate>(parsed);
    const auto &metadata = update.metadata();
    const auto &expected = test_case.at("expected_metadata");
    REQUIRE_EQ(metadata.exchange_event_time_ms(),
               expected.at("exchange_event_time_ms").get<std::uint64_t>());
    REQUIRE(!metadata.has_exchange_trade_time_ms());
    REQUIRE_EQ(metadata.has_exchange_transaction_time_ms(),
               !expected.at("exchange_transaction_time_ms").is_null());
    if (metadata.has_exchange_transaction_time_ms()) {
      REQUIRE_EQ(
          metadata.exchange_transaction_time_ms(),
          expected.at("exchange_transaction_time_ms").get<std::uint64_t>());
    }
    REQUIRE_EQ(update.first_update_id(), 2001U);
    REQUIRE_EQ(update.final_update_id(), 2003U);
    REQUIRE_EQ(update.previous_final_update_id(), 1999U);
    REQUIRE_EQ(update.bids(0).price(), "29600.00");
    REQUIRE_EQ(update.bids(0).quantity(), "3.00000000");
    REQUIRE_EQ(update.asks(0).quantity(), "0.00000000");
    protocol::market::DepthUpdate decoded;
    REQUIRE(decoded.ParseFromString(update.SerializeAsString()));
    REQUIRE_EQ(decoded.metadata().SerializeAsString(),
               metadata.SerializeAsString());
    for (const auto &wrong : fixture.at("invalid_metadata")) {
      if (metadata.has_exchange_transaction_time_ms() &&
          metadata.exchange_transaction_time_ms() != 0U) {
        REQUIRE(wrong != expected);
      }
    }
    nlohmann::json snapshot{{"lastUpdateId", 2000U},
                            {"bids", source.at("b")},
                            {"asks", source.at("a")}};
    if (source.contains("T")) {
      snapshot["T"] = source.at("T");
    }
    const auto baseline = protocol::parse_usdm_depth_snapshot(
        snapshot.dump(), {10U, 20U}, "c0-request");
    REQUIRE(std::holds_alternative<protocol::market::ExchangeDepthSnapshot>(
        baseline));
    const auto &book =
        std::get<protocol::market::ExchangeDepthSnapshot>(baseline);
    REQUIRE_EQ(book.has_exchange_transaction_time_ms(),
               metadata.has_exchange_transaction_time_ms());
    REQUIRE_EQ(book.exchange_transaction_time_ms(),
               metadata.exchange_transaction_time_ms());
  }
  for (const auto &patch : fixture.at("invalid_source_patches")) {
    auto source = fixture.at("raw");
    source.update(patch);
    const auto parsed = protocol::parse_usdm_depth_frame(
        source.dump(), {10U, 20U}, "c0-source");
    REQUIRE(std::holds_alternative<protocol::ProtocolError>(parsed));
    REQUIRE_EQ(std::get<protocol::ProtocolError>(parsed).field, "T");
  }
}

void usdm_public_and_market_event_payloads() {
  const rt::ClockSample received{1700000000000000000ULL, 5000U};
  const auto public_depth = protocol::parse_usdm_public_event_frame(
      R"json({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000000,"T":1700000000001,"s":"BTCUSDT","U":100,"u":102,"pu":99,"b":[["100.0","1.0"]],"a":[["101.0","0.0"]]}})json",
      received, "usdm-public", "BTCUSDT");
  REQUIRE(
      std::holds_alternative<protocol::NormalizedMarketEvent>(public_depth));
  const auto &public_depth_event =
      std::get<protocol::NormalizedMarketEvent>(public_depth);
  REQUIRE(std::holds_alternative<protocol::market::DepthUpdate>(
      public_depth_event));
  const auto &depth =
      std::get<protocol::market::DepthUpdate>(public_depth_event);
  REQUIRE_EQ(depth.metadata().stream(), common_wire::STREAM_DIFF_DEPTH);
  REQUIRE_EQ(depth.previous_final_update_id(), 99U);
  REQUIRE_EQ(depth.metadata().exchange_event_time_ms(), 1700000000000U);
  REQUIRE_EQ(depth.metadata().exchange_transaction_time_ms(), 1700000000001U);

  const auto ticker = protocol::parse_usdm_public_event_frame(
      R"json({"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000","T":1568014460893,"E":1568014460894,"st":1}})json",
      received, "usdm-public", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::NormalizedMarketEvent>(ticker));
  const auto &ticker_event = std::get<protocol::NormalizedMarketEvent>(ticker);
  REQUIRE(std::holds_alternative<protocol::market::BookTicker>(ticker_event));
  const auto &book = std::get<protocol::market::BookTicker>(ticker_event);
  REQUIRE_EQ(book.metadata().market(), common_wire::MARKET_USD_M_PERPETUAL);
  REQUIRE_EQ(book.metadata().exchange_event_time_ms(), 1568014460894U);
  REQUIRE_EQ(book.metadata().exchange_transaction_time_ms(), 1568014460893U);
  REQUIRE_EQ(book.best_bid_price(), "25.35190000");
  REQUIRE_EQ(book.best_bid_quantity(), "31.21000000");
  REQUIRE_EQ(book.best_ask_price(), "25.36520000");

  const auto trade = protocol::parse_usdm_market_event_frame(
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1568014460893,"s":"BTCUSDT","a":12345,"p":"9910","q":"0.014","f":100,"l":105,"T":1568014460892,"m":true,"nq":"0.013","st":1}})json",
      received, "usdm-market", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::NormalizedMarketEvent>(trade));
  const auto &trade_event = std::get<protocol::NormalizedMarketEvent>(trade);
  REQUIRE(std::holds_alternative<protocol::market::AggTrade>(trade_event));
  const auto &agg = std::get<protocol::market::AggTrade>(trade_event);
  REQUIRE_EQ(agg.metadata().stream(), common_wire::STREAM_AGG_TRADE);
  REQUIRE_EQ(agg.metadata().exchange_trade_time_ms(), 1568014460892U);
  REQUIRE_EQ(agg.normal_quantity(), "0.013");
  REQUIRE(agg.has_normal_quantity());

  const auto no_nq = protocol::parse_usdm_market_event_frame(
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"T":1,"m":false}})json",
      received, "usdm-market", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::NormalizedMarketEvent>(no_nq));
  REQUIRE(!std::get<protocol::market::AggTrade>(
               std::get<protocol::NormalizedMarketEvent>(no_nq))
               .has_normal_quantity());

  const auto mark = protocol::parse_usdm_market_event_frame(
      R"json({"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"11794.15000000","i":"11784.62659091","P":"11784.25641265","r":"-0.00038167","ap":"11794.15000000","T":1562306400000,"st":1}})json",
      received, "usdm-market", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::NormalizedMarketEvent>(mark));
  const auto &mark_event = std::get<protocol::NormalizedMarketEvent>(mark);
  REQUIRE(std::holds_alternative<protocol::market::MarkPrice>(mark_event));
  const auto &price = std::get<protocol::market::MarkPrice>(mark_event);
  REQUIRE_EQ(price.funding_rate(), "-0.00038167");
  REQUIRE_EQ(price.next_funding_time_ms(), 1562306400000U);
  REQUIRE(price.has_mark_price_moving_average());
  REQUIRE_EQ(price.mark_price_moving_average(), "11794.15000000");
  REQUIRE(!price.metadata().has_exchange_transaction_time_ms());
  REQUIRE(!price.metadata().has_exchange_trade_time_ms());

  const auto liquidation = protocol::parse_usdm_market_event_frame(
      R"json({"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1568014460893,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"9910","ap":"9910","X":"FILLED","l":"0.014","z":"0.014","T":1568014460893}}})json",
      received, "usdm-market", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::NormalizedMarketEvent>(liquidation));
  const auto &liquidation_event =
      std::get<protocol::NormalizedMarketEvent>(liquidation);
  REQUIRE(
      std::holds_alternative<protocol::market::Liquidation>(liquidation_event));
  const auto &order =
      std::get<protocol::market::Liquidation>(liquidation_event);
  REQUIRE_EQ(order.side(), "SELL");
  REQUIRE_EQ(order.order_type(), "LIMIT");
  REQUIRE_EQ(order.time_in_force(), "IOC");
  REQUIRE_EQ(order.original_quantity(), "0.014");
  REQUIRE_EQ(order.average_price(), "9910");
  REQUIRE_EQ(order.order_trade_time_ms(), 1568014460893U);
  REQUIRE_EQ(order.metadata().exchange_event_time_ms(), 1568014460893U);

  const auto wrong_stream = protocol::parse_usdm_market_event_frame(
      R"json({"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1","T":1,"E":1}})json",
      received, "usdm-market", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(wrong_stream));
  REQUIRE_EQ(std::get<protocol::ProtocolError>(wrong_stream).code,
             protocol::ProtocolErrorCode::WrongEvent);

  const auto wrong_symbol = protocol::parse_usdm_public_event_frame(
      R"json({"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"ETHUSDT","b":"1","B":"1","a":"2","A":"1","T":1,"E":1}})json",
      received, "usdm-public", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::ProtocolError>(wrong_symbol));
  REQUIRE_EQ(std::get<protocol::ProtocolError>(wrong_symbol).code,
             protocol::ProtocolErrorCode::WrongSymbol);

  const auto snapshot = protocol::parse_usdm_depth_snapshot(
      R"json({"lastUpdateId":100,"E":1700000000000,"T":1699999999999,"bids":[["100.0","1.0"]],"asks":[]})json",
      received, "snapshot-request", "BTCUSDT");
  REQUIRE(std::holds_alternative<protocol::market::ExchangeDepthSnapshot>(
      snapshot));
  const auto &baseline =
      std::get<protocol::market::ExchangeDepthSnapshot>(snapshot);
  REQUIRE(baseline.has_exchange_event_time_ms());
  REQUIRE_EQ(baseline.exchange_event_time_ms(), 1700000000000U);
  REQUIRE_EQ(baseline.exchange_transaction_time_ms(), 1699999999999U);

  const auto missing_funding_time = protocol::parse_usdm_market_event_frame(
      R"json({"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1,"s":"BTCUSDT","p":"1","i":"1","P":"1","r":"0"}})json",
      received, "usdm-market", "BTCUSDT");
  REQUIRE(
      std::holds_alternative<protocol::ProtocolError>(missing_funding_time));
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"EXCHANGEINFO_SELECTS_EXACT_PERPETUAL",
       exchange_info_selects_exact_perpetual},
      {"EXACT_SYMBOL_METADATA_SELECTION", exact_symbol_metadata_selection},
      {"BATCH_EXCHANGEINFO_SELECTION", batch_exchange_info_selection},
      {"EXCHANGEINFO_FILTER_VALIDATION", exchange_info_filter_validation},
      {"DIFF_DEPTH_PRESENT_AND_ABSENT_PU", diff_depth_present_and_absent_pu},
      {"CONTRACTS_C0_SOURCE_TIME_ORACLE", contracts_c0_source_time_oracle},
      {"EXACT_SYMBOL_EVENTS_AND_SNAPSHOT_IDENTITY",
       exact_symbol_events_and_snapshot_identity},
      {"DIFF_DEPTH_HAS_NO_PRIOR_U_CLASSIFIER",
       diff_depth_has_no_prior_u_classifier},
      {"DIFF_DEPTH_IDENTITY_AND_TYPE_REJECTION",
       diff_depth_identity_and_type_rejection},
      {"SNAPSHOT_PARSE_AND_VALIDATION", snapshot_parse_and_validation},
      {"STREAM_SYMBOL_AND_SHUTDOWN", stream_symbol_and_shutdown},
      {"UNRELATED_EXTENSIONS_PRESERVE_STRICT_FIELDS",
       unrelated_extensions_preserve_strict_fields},
      {"EXTENSIONS_PRESERVE_PROJECTION_SEQUENCE_CHECKS",
       extensions_do_not_bypass_projection_sequence_checks},
      {"USDM_PUBLIC_AND_MARKET_EVENT_PAYLOADS",
       usdm_public_and_market_event_payloads},
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
