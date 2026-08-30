#include "spot_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <cstdint>
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
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;

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

[[nodiscard]] std::string exchange_info(std::string_view symbol = "BTCUSDT",
                                        std::string_view status = "TRADING",
                                        std::string_view spot_allowed = "true",
                                        std::string_view filters = R"json(
      {"filterType":"PRICE_FILTER","minPrice":"0.01000000","maxPrice":"1000000.00000000","tickSize":"0.01000000"},
      {"filterType":"LOT_SIZE","minQty":"0.00001000","maxQty":"9000.00000000","stepSize":"0.00001000"},
      {"filterType":"MARKET_LOT_SIZE","stepSize":"0.12500000"}
    )json") {
  return std::string{R"json({"timezone":"UTC","symbols":[{
    "symbol":")json"} +
         std::string{symbol} + R"json(","status":")json" + std::string{status} +
         R"json(","baseAssetPrecision":1,"quotePrecision":17,
    "isSpotTradingAllowed":)json" +
         std::string{spot_allowed} + R"json(,"filters":[)json" +
         std::string{filters} + R"json(]}]})json";
}

[[nodiscard]] const g4::ProtocolError &
require_error(const g4::SpotMetadataResult &result) {
  REQUIRE(std::holds_alternative<g4::ProtocolError>(result));
  return std::get<g4::ProtocolError>(result);
}

void numeric_spec_from_filters() {
  const std::vector<std::pair<std::string_view, std::uint8_t>> cases{
      {"0.01000000", 2U},
      {"0.00001000", 5U},
      {"1.00000000", 0U},
      {"0.12500000", 3U},
  };
  for (const auto &[quantum, expected] : cases) {
    const auto parsed = g4::decimal_scale_from_quantum(quantum);
    REQUIRE(std::holds_alternative<g4::core::DecimalScale>(parsed));
    REQUIRE_EQ(std::get<g4::core::DecimalScale>(parsed).value(), expected);
  }

  for (const auto invalid : {"", "0", "0.000", "-0.01", "+0.01", "1e-8", ".01",
                             "1.", "1..0", "one", "0.0000000000000000001"}) {
    REQUIRE(std::holds_alternative<g4::ProtocolError>(
        g4::decimal_scale_from_quantum(invalid)));
  }
}

void exchange_info_validation_and_precision_sources() {
  const auto parsed = g4::parse_exchange_info(exchange_info());
  REQUIRE(std::holds_alternative<g4::SpotMetadata>(parsed));
  const auto &metadata = std::get<g4::SpotMetadata>(parsed);
  REQUIRE_EQ(metadata.tick_size, "0.01000000");
  REQUIRE_EQ(metadata.step_size, "0.00001000");
  REQUIRE_EQ(metadata.numeric_spec.price_scale.value(), 2U);
  REQUIRE_EQ(metadata.numeric_spec.quantity_scale.value(), 5U);
  // baseAssetPrecision=1, quotePrecision=17, and MARKET_LOT_SIZE scale=3 are
  // intentionally not NumericSpec authority.
  REQUIRE(metadata.numeric_spec.price_scale.value() != 17U);
  REQUIRE(metadata.numeric_spec.quantity_scale.value() != 1U);
  REQUIRE(metadata.numeric_spec.quantity_scale.value() != 3U);

  REQUIRE_EQ(
      require_error(g4::parse_exchange_info(exchange_info("ETHUSDT"))).code,
      g4::ProtocolErrorCode::InvalidMarketMetadata);
  static_cast<void>(require_error(
      g4::parse_exchange_info(exchange_info("BTCUSDT", "BREAK"))));
  static_cast<void>(require_error(
      g4::parse_exchange_info(exchange_info("BTCUSDT", "TRADING", "false"))));
  static_cast<void>(require_error(g4::parse_exchange_info(exchange_info(
      "BTCUSDT", "TRADING", "true",
      R"json({"filterType":"LOT_SIZE","stepSize":"0.001"})json"))));
  static_cast<void>(require_error(g4::parse_exchange_info(
      exchange_info("BTCUSDT", "TRADING", "true",
                    R"json({"filterType":"PRICE_FILTER","tickSize":"0.01"},
             {"filterType":"PRICE_FILTER","tickSize":"0.01"},
             {"filterType":"LOT_SIZE","stepSize":"0.001"})json"))));
  static_cast<void>(require_error(g4::parse_exchange_info(
      exchange_info("BTCUSDT", "TRADING", "true",
                    R"json({"filterType":"PRICE_FILTER","tickSize":0.01},
             {"filterType":"LOT_SIZE","stepSize":"0.001"})json"))));
  static_cast<void>(require_error(g4::parse_exchange_info(
      exchange_info("BTCUSDT", "TRADING", "true",
                    R"json({"filterType":"PRICE_FILTER","tickSize":"0"},
             {"filterType":"LOT_SIZE","stepSize":"0.001"})json"))));
}

void depth_frame_parse() {
  constexpr std::string_view payload = R"json({
    "e":"depthUpdate","E":1672515782136,"s":"BTCUSDT",
    "U":157,"u":160,
    "b":[["0.00240000","10.00000000"]],
    "a":[["0.00260000","100.00000000"]]
  })json";
  const auto parsed =
      g4::parse_depth_frame(payload, {1700000000123456000ULL, 9000000000999ULL},
                            "binance-spot-btcusdt-g1-1700000000");
  REQUIRE(std::holds_alternative<g4::market::DepthUpdate>(parsed));
  const auto &update = std::get<g4::market::DepthUpdate>(parsed);
  REQUIRE_EQ(update.metadata().venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(update.metadata().market(), common_wire::MARKET_SPOT);
  REQUIRE_EQ(update.metadata().symbol(), "BTCUSDT");
  REQUIRE_EQ(update.metadata().stream(), common_wire::STREAM_DIFF_DEPTH);
  REQUIRE_EQ(update.metadata().exchange_event_time_ms(), 1672515782136ULL);
  REQUIRE_EQ(update.metadata().receive_time_utc_ns(), 1700000000123456000ULL);
  REQUIRE_EQ(update.metadata().receive_monotonic_ns(), 9000000000999ULL);
  REQUIRE_EQ(update.metadata().connection_id(),
             "binance-spot-btcusdt-g1-1700000000");
  REQUIRE_EQ(update.first_update_id(), 157U);
  REQUIRE_EQ(update.final_update_id(), 160U);
  REQUIRE(!update.has_previous_final_update_id());
  REQUIRE_EQ(update.bids_size(), 1);
  REQUIRE_EQ(update.bids(0).price(), "0.00240000");
  REQUIRE_EQ(update.bids(0).quantity(), "10.00000000");
  REQUIRE_EQ(update.asks_size(), 1);
}

void wrong_symbol_and_malformed_depth() {
  const std::vector<std::string_view> invalid{
      R"json({"e":"depthUpdate","E":1,"s":"ETHUSDT","U":1,"u":1,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":"1","s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[["1"]],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[[1,"2"]],"a":[]})json",
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":18446744073709551616,"u":1,"b":[],"a":[]})json",
      R"json({"e":"trade","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})json",
      R"json(not-json)json",
  };
  for (const auto payload : invalid) {
    const auto parsed = g4::parse_depth_frame(payload, {1U, 2U}, "connection");
    REQUIRE(std::holds_alternative<g4::ProtocolError>(parsed));
  }
}

void snapshot_json_parse() {
  constexpr std::string_view payload = R"json({
    "lastUpdateId":1027024,
    "bids":[["4.00000000","431.00000000"]],
    "asks":[["4.00000200","12.00000000"]]
  })json";
  const auto parsed = g4::parse_depth_snapshot(
      payload, {1700000001U, 9000000001U}, "g4-depth-request-1");
  REQUIRE(std::holds_alternative<g4::market::ExchangeDepthSnapshot>(parsed));
  const auto &snapshot = std::get<g4::market::ExchangeDepthSnapshot>(parsed);
  REQUIRE_EQ(snapshot.venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(snapshot.market(), common_wire::MARKET_SPOT);
  REQUIRE_EQ(snapshot.symbol(), "BTCUSDT");
  REQUIRE_EQ(snapshot.request_id(), "g4-depth-request-1");
  REQUIRE_EQ(snapshot.last_update_id(), 1027024U);
  REQUIRE_EQ(snapshot.receive_time_utc_ns(), 1700000001U);
  REQUIRE_EQ(snapshot.receive_monotonic_ns(), 9000000001U);
  REQUIRE(!snapshot.has_exchange_transaction_time_ms());
  REQUIRE_EQ(snapshot.bids(0).price(), "4.00000000");
  REQUIRE_EQ(snapshot.asks(0).quantity(), "12.00000000");

  for (const auto invalid :
       {R"json({"lastUpdateId":"1","bids":[],"asks":[]})json",
        R"json({"lastUpdateId":1,"bids":[["1",2]],"asks":[]})json",
        R"json({"lastUpdateId":1,"bids":[]})json"}) {
    REQUIRE(std::holds_alternative<g4::ProtocolError>(
        g4::parse_depth_snapshot(invalid, {1U, 2U}, "request")));
  }
}

void stream_name_and_server_shutdown() {
  REQUIRE_EQ(g4::spot_stream_symbol("BTCUSDT"),
             std::optional<std::string>{"btcusdt"});
  REQUIRE(!g4::spot_stream_symbol("btcusdt").has_value());
  REQUIRE(!g4::spot_stream_symbol("ETHUSDT").has_value());

  const auto shutdown = g4::parse_depth_frame(
      R"json({"e":"serverShutdown","E":1770123456789})json", {1U, 2U},
      "connection");
  REQUIRE(std::holds_alternative<g4::ServerShutdown>(shutdown));
  REQUIRE_EQ(std::get<g4::ServerShutdown>(shutdown).exchange_event_time_ms,
             1770123456789ULL);
}

void combined_event_frames() {
  const auto received_at =
      g3::ClockSample{1700000000123456000ULL, 9000000000999ULL};
  const auto depth = g4::parse_combined_event_frame(
      R"json({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1672515782136,"s":"BTCUSDT","U":157,"u":160,"b":[["100.00","1.000"]],"a":[]}})json",
      received_at, "combined-g1");
  REQUIRE(std::holds_alternative<g4::NormalizedSpotEvent>(depth));
  const auto &depth_event = std::get<g4::NormalizedSpotEvent>(depth);
  REQUIRE(std::holds_alternative<g4::market::DepthUpdate>(depth_event));
  REQUIRE_EQ(std::get<g4::market::DepthUpdate>(depth_event).final_update_id(),
             160U);

  const auto trade = g4::parse_combined_event_frame(
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1672515782136,"s":"BTCUSDT","a":5933014,"p":"100.25","q":"0.125","f":100,"l":105,"T":1672515782137,"m":true,"M":true}})json",
      received_at, "combined-g1");
  REQUIRE(std::holds_alternative<g4::NormalizedSpotEvent>(trade));
  const auto &trade_event = std::get<g4::NormalizedSpotEvent>(trade);
  REQUIRE(std::holds_alternative<g4::market::AggTrade>(trade_event));
  const auto &normalized_trade = std::get<g4::market::AggTrade>(trade_event);
  REQUIRE_EQ(normalized_trade.aggregate_trade_id(), 5933014U);
  REQUIRE_EQ(normalized_trade.price(), "100.25");
  REQUIRE_EQ(normalized_trade.quantity(), "0.125");
  REQUIRE_EQ(normalized_trade.first_trade_id(), 100U);
  REQUIRE_EQ(normalized_trade.last_trade_id(), 105U);
  REQUIRE_EQ(normalized_trade.trade_time_ms(), 1672515782137ULL);
  REQUIRE(normalized_trade.buyer_is_maker());
  REQUIRE_EQ(normalized_trade.metadata().stream(),
             common_wire::STREAM_AGG_TRADE);
  REQUIRE_EQ(normalized_trade.metadata().schema_version(), "agg-trade.v1");
  REQUIRE_EQ(normalized_trade.metadata().exchange_event_time_ms(),
             1672515782136ULL);
  REQUIRE_EQ(normalized_trade.metadata().exchange_trade_time_ms(),
             1672515782137ULL);

  const auto ticker = g4::parse_combined_event_frame(
      R"json({"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"100.00","B":"1.500","a":"100.01","A":"0.000"}})json",
      received_at, "combined-g1");
  REQUIRE(std::holds_alternative<g4::NormalizedSpotEvent>(ticker));
  const auto &ticker_event = std::get<g4::NormalizedSpotEvent>(ticker);
  REQUIRE(std::holds_alternative<g4::market::BookTicker>(ticker_event));
  const auto &normalized_ticker =
      std::get<g4::market::BookTicker>(ticker_event);
  REQUIRE_EQ(normalized_ticker.update_id(), 400900217U);
  REQUIRE_EQ(normalized_ticker.best_bid_price(), "100.00");
  REQUIRE_EQ(normalized_ticker.best_ask_quantity(), "0.000");
  REQUIRE_EQ(normalized_ticker.metadata().stream(),
             common_wire::STREAM_BOOK_TICKER);
  REQUIRE_EQ(normalized_ticker.metadata().schema_version(), "book-ticker.v1");
  REQUIRE(!normalized_ticker.metadata().has_exchange_event_time_ms());
  REQUIRE(!normalized_ticker.metadata().has_exchange_trade_time_ms());
}

void malformed_combined_event_frames() {
  const std::vector<std::string_view> invalid{
      R"json(not-json)json",
      R"json({"stream":"btcusdt@aggTrade"})json",
      R"json({"stream":"ethusdt@aggTrade","data":{}})json",
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"ETHUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"T":1,"m":true,"M":true}})json",
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"trade","E":1,"s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"T":1,"m":true,"M":true}})json",
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"1e2","q":"1","f":1,"l":1,"T":1,"m":true,"M":true}})json",
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":"1","p":"1","q":"1","f":1,"l":1,"T":1,"m":true,"M":true}})json",
      R"json({"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"T":1,"m":1,"M":true}})json",
      R"json({"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"0","B":"1","a":"2","A":"1"}})json",
      R"json({"stream":"btcusdt@bookTicker","data":{"u":1,"s":"ETHUSDT","b":"1","B":"1","a":"2","A":"1"}})json",
      R"json({"stream":"btcusdt@bookTicker","data":{"u":"1","s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1"}})json",
      R"json({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[],"pu":0}})json",
  };
  for (const auto payload : invalid) {
    REQUIRE(std::holds_alternative<g4::ProtocolError>(
        g4::parse_combined_event_frame(payload, {1U, 2U}, "connection")));
  }

  // The legacy raw depth parser remains a separate unchanged framing path.
  REQUIRE(std::holds_alternative<g4::market::DepthUpdate>(g4::parse_depth_frame(
      R"json({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})json",
      {1U, 2U}, "connection")));
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"NUMERIC_SPEC_FROM_FILTERS", numeric_spec_from_filters},
      {"EXCHANGEINFO_VALIDATION_AND_WRONG_PRECISION_SOURCES",
       exchange_info_validation_and_precision_sources},
      {"DEPTH_FRAME_PARSE", depth_frame_parse},
      {"WRONG_SYMBOL_AND_MALFORMED_DEPTH", wrong_symbol_and_malformed_depth},
      {"SNAPSHOT_JSON_PARSE", snapshot_json_parse},
      {"STREAM_NAME_AND_SERVER_SHUTDOWN", stream_name_and_server_shutdown},
      {"COMBINED_EVENT_FRAMES", combined_event_frames},
      {"MALFORMED_COMBINED_EVENT_FRAMES", malformed_combined_event_frames},
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
