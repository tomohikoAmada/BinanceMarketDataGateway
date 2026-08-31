#include "usdm_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <exception>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace g4 = binance_market_data::gateway::g4;
namespace g11 = binance_market_data::gateway::g11;

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
    )json") {
  return std::string{
             R"json({"symbol":"BTCUSDT","pair":"BTCUSDT","contractType":")json"} +
         std::string{contract_type} + R"json(","status":")json" +
         std::string{status} +
         R"json(","pricePrecision":17,"quantityPrecision":19,"filters":[)json" +
         std::string{filters} + "]}";
}

[[nodiscard]] std::string exchange_info(std::string target) {
  return std::string{R"json({"timezone":"UTC","symbols":[
    {"symbol":"ETHUSDT","contractType":"PERPETUAL","status":"TRADING","filters":[]},
  )json"} +
         std::move(target) +
         R"json(,
    {"symbol":"BTCUSDC","contractType":"PERPETUAL","status":"TRADING","filters":[]}
  ]})json";
}

[[nodiscard]] const g4::ProtocolError &
require_metadata_error(const g11::UsdMMetadataResult &result) {
  REQUIRE(std::holds_alternative<g4::ProtocolError>(result));
  return std::get<g4::ProtocolError>(result);
}

void exchange_info_selects_exact_perpetual() {
  const auto parsed =
      g11::parse_usdm_exchange_info(exchange_info(target_symbol()));
  REQUIRE(std::holds_alternative<g11::UsdMMetadata>(parsed));
  const auto &metadata = std::get<g11::UsdMMetadata>(parsed);
  REQUIRE_EQ(metadata.tick_size, "0.10");
  REQUIRE_EQ(metadata.step_size, "0.001");
  REQUIRE_EQ(metadata.numeric_spec.price_scale.value(), 1U);
  REQUIRE_EQ(metadata.numeric_spec.quantity_scale.value(), 3U);
  REQUIRE(metadata.numeric_spec.price_scale.value() != 17U);
  REQUIRE(metadata.numeric_spec.quantity_scale.value() != 19U);

  REQUIRE_EQ(require_metadata_error(g11::parse_usdm_exchange_info(exchange_info(
                                        target_symbol("CURRENT_QUARTER"))))
                 .field,
             "contractType");
  REQUIRE_EQ(
      require_metadata_error(g11::parse_usdm_exchange_info(exchange_info(
                                 target_symbol("PERPETUAL", "SETTLING"))))
          .field,
      "status");
}

void exchange_info_filter_validation() {
  static_cast<void>(require_metadata_error(
      g11::parse_usdm_exchange_info(exchange_info(target_symbol(
          "PERPETUAL", "TRADING",
          R"json({"filterType":"LOT_SIZE","stepSize":"0.001"})json")))));
  static_cast<void>(require_metadata_error(
      g11::parse_usdm_exchange_info(exchange_info(target_symbol(
          "PERPETUAL", "TRADING",
          R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"})json")))));
  static_cast<void>(
      require_metadata_error(g11::parse_usdm_exchange_info(exchange_info(
          target_symbol("PERPETUAL", "TRADING",
                        R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"},
                  {"filterType":"PRICE_FILTER","tickSize":"0.01"},
                  {"filterType":"LOT_SIZE","stepSize":"0.001"})json")))));
  static_cast<void>(
      require_metadata_error(g11::parse_usdm_exchange_info(exchange_info(
          target_symbol("PERPETUAL", "TRADING",
                        R"json({"filterType":"PRICE_FILTER","tickSize":"0.10"},
                  {"filterType":"LOT_SIZE","stepSize":"0.001"},
                  {"filterType":"LOT_SIZE","stepSize":"0.01"})json")))));
}

void diff_depth_present_and_absent_pu() {
  constexpr auto received_at = binance_market_data::gateway::g3::ClockSample{
      1700000000000000000ULL, 9000000000000ULL};
  const auto present = g11::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1770000000001,"T":1770000000000,"s":"BTCUSDT","U":501,"u":504,"pu":499,"b":[["100.1","2.500"]],"a":[["100.2","0"]],"ps":"BTCUSDT","st":1})json",
      received_at, "binance-usdm-btcusdt-g1-1700000000000000000");
  REQUIRE(std::holds_alternative<g11::market::DepthUpdate>(present));
  const auto &update = std::get<g11::market::DepthUpdate>(present);
  REQUIRE_EQ(update.metadata().venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(update.metadata().market(), common_wire::MARKET_USD_M_PERPETUAL);
  REQUIRE_EQ(update.metadata().symbol(), "BTCUSDT");
  REQUIRE_EQ(update.metadata().stream(), common_wire::STREAM_DIFF_DEPTH);
  REQUIRE_EQ(update.metadata().exchange_event_time_ms(), 1770000000001ULL);
  REQUIRE_EQ(update.metadata().exchange_trade_time_ms(), 1770000000000ULL);
  REQUIRE_EQ(update.first_update_id(), 501U);
  REQUIRE_EQ(update.final_update_id(), 504U);
  REQUIRE(update.has_previous_final_update_id());
  REQUIRE_EQ(update.previous_final_update_id(), 499U);
  REQUIRE_EQ(update.bids(0).price(), "100.1");
  REQUIRE_EQ(update.asks(0).quantity(), "0");

  const auto absent = g11::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":900,"u":901,"b":[],"a":[]})json",
      {3U, 4U}, "usdm-connection");
  REQUIRE(std::holds_alternative<g11::market::DepthUpdate>(absent));
  REQUIRE(!std::get<g11::market::DepthUpdate>(absent)
               .has_previous_final_update_id());
}

void diff_depth_has_no_prior_u_classifier() {
  const auto first = g11::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":100,"u":110,"pu":7,"b":[],"a":[]})json",
      {1U, 2U}, "usdm-connection");
  const auto second = g11::parse_usdm_depth_frame(
      R"json({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":500,"u":400,"pu":999999,"b":[],"a":[]})json",
      {3U, 4U}, "usdm-connection");
  REQUIRE(std::holds_alternative<g11::market::DepthUpdate>(first));
  REQUIRE(std::holds_alternative<g11::market::DepthUpdate>(second));
  REQUIRE_EQ(
      std::get<g11::market::DepthUpdate>(second).previous_final_update_id(),
      999999U);
}

void diff_depth_identity_and_type_rejection() {
  const std::vector<std::string_view> invalid{
      R"json({"e":"depthUpdate","E":1,"s":"ETHUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"pu":"0","b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"T":"1","s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[],"ps":"BTCUSD"})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[],"st":2})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[["1"]],"a":[]})json",
      R"json({"e":"aggTrade","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]})json",
      R"json(not-json)json",
  };
  for (const auto payload : invalid) {
    REQUIRE(std::holds_alternative<g4::ProtocolError>(
        g11::parse_usdm_depth_frame(payload, {1U, 2U}, "connection")));
  }
}

void snapshot_parse_and_validation() {
  const auto parsed = g11::parse_usdm_depth_snapshot(
      R"json({"lastUpdateId":1027024,"E":1770000000001,"T":1770000000000,"bids":[["100.1","2.0"]],"asks":[["100.2","3.0"]]})json",
      {10U, 20U}, "g11-usdm-depth-request-1");
  REQUIRE(std::holds_alternative<g11::market::ExchangeDepthSnapshot>(parsed));
  const auto &snapshot = std::get<g11::market::ExchangeDepthSnapshot>(parsed);
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
    REQUIRE(std::holds_alternative<g4::ProtocolError>(
        g11::parse_usdm_depth_snapshot(invalid, {1U, 2U}, "request")));
  }
}

void stream_symbol_and_shutdown() {
  REQUIRE_EQ(g11::usdm_stream_symbol("BTCUSDT"),
             std::optional<std::string>{"btcusdt"});
  REQUIRE(!g11::usdm_stream_symbol("btcusdt").has_value());
  REQUIRE(!g11::usdm_stream_symbol("ETHUSDT").has_value());

  const auto shutdown = g11::parse_usdm_depth_frame(
      R"json({"e":"serverShutdown","E":1770123456789})json", {1U, 2U},
      "connection");
  REQUIRE(std::holds_alternative<g4::ServerShutdown>(shutdown));
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"EXCHANGEINFO_SELECTS_EXACT_PERPETUAL",
       exchange_info_selects_exact_perpetual},
      {"EXCHANGEINFO_FILTER_VALIDATION", exchange_info_filter_validation},
      {"DIFF_DEPTH_PRESENT_AND_ABSENT_PU", diff_depth_present_and_absent_pu},
      {"DIFF_DEPTH_HAS_NO_PRIOR_U_CLASSIFIER",
       diff_depth_has_no_prior_u_classifier},
      {"DIFF_DEPTH_IDENTITY_AND_TYPE_REJECTION",
       diff_depth_identity_and_type_rejection},
      {"SNAPSHOT_PARSE_AND_VALIDATION", snapshot_parse_and_validation},
      {"STREAM_SYMBOL_AND_SHUTDOWN", stream_symbol_and_shutdown},
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
