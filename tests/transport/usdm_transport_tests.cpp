#include "transport/usdm_transport.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace protocol = binance_market_data::gateway::protocol;
namespace transport = binance_market_data::gateway::transport;
namespace rt = binance_market_data::gateway::runtime;

namespace {

namespace core = binance_market_data::projection::v1;

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

#define REQUIRE(condition) require((condition), #condition)

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(1U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    std::abort();
  }
  return {*price, *quantity};
}

[[nodiscard]] rt::RuntimeClock deterministic_clock() {
  return [] { return rt::ClockSample{1700000000000000000ULL, 5000U}; };
}

void official_routes_are_frozen() {
  REQUIRE(transport::kUsdMTransportRoutes.rest_host == "fapi.binance.com");
  REQUIRE(transport::kUsdMTransportRoutes.rest_port == "443");
  REQUIRE(transport::kUsdMTransportRoutes.exchange_info_target ==
          "/fapi/v1/exchangeInfo");
  REQUIRE(transport::kUsdMTransportRoutes.depth_target ==
          "/fapi/v1/depth?symbol=BTCUSDT&limit=1000");
  REQUIRE(transport::kUsdMTransportRoutes.websocket_host ==
          "fstream.binance.com");
  REQUIRE(transport::kUsdMTransportRoutes.websocket_port == "443");
  REQUIRE(transport::kUsdMTransportRoutes.websocket_target ==
          "/public/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker");
  REQUIRE(transport::kUsdMTransportRoutes.market_websocket_target ==
          "/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/"
          "btcusdt@forceOrder");
  REQUIRE(transport::kUsdMTransportRoutes.raw_websocket_target ==
          "/public/ws/btcusdt@depth@100ms");
  REQUIRE(transport::kUsdMTransportRoutes.websocket_target !=
          "/ws/btcusdt@depth@100ms");
  REQUIRE(transport::kUsdMTransportRoutes.diff_depth_stream ==
          "btcusdt@depth@100ms");
  REQUIRE(transport::kUsdMTransportRoutes.snapshot_limit == 1000U);
}

void exact_usdm_routes_are_product_bound() {
  const auto btc = transport::make_usdm_transport_routes("BTCUSDT");
  const auto eth = transport::make_usdm_transport_routes("ETHUSDT");
  REQUIRE(btc.has_value());
  REQUIRE(eth.has_value());
  REQUIRE(btc->depth_target == "/fapi/v1/depth?symbol=BTCUSDT&limit=1000");
  REQUIRE(eth->depth_target == "/fapi/v1/depth?symbol=ETHUSDT&limit=1000");
  REQUIRE(btc->raw_websocket_target == "/public/ws/btcusdt@depth@100ms");
  REQUIRE(eth->raw_websocket_target == "/public/ws/ethusdt@depth@100ms");
  REQUIRE(btc->websocket_target ==
          "/public/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker");
  REQUIRE(eth->websocket_target ==
          "/public/stream?streams=ethusdt@depth@100ms/ethusdt@bookTicker");
  REQUIRE(btc->market_websocket_target ==
          "/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/"
          "btcusdt@forceOrder");
  REQUIRE(eth->market_websocket_target ==
          "/market/stream?streams=ethusdt@aggTrade/ethusdt@markPrice@1s/"
          "ethusdt@forceOrder");
  REQUIRE(btc->diff_depth_stream == "btcusdt@depth@100ms");
  REQUIRE(eth->diff_depth_stream == "ethusdt@depth@100ms");
  REQUIRE(btc->connection_id_prefix != eth->connection_id_prefix);
  REQUIRE(btc->snapshot_request_id != eth->snapshot_request_id);
  REQUIRE(!transport::make_usdm_transport_routes("ethusdt").has_value());
}

void clean_stop_preserves_shared_network_semantics() {
  const auto clock = deterministic_clock();
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  REQUIRE(runtime.start() == rt::StartResult::Started);

  transport::detail::TransportTestOptions test_options;
  test_options.start_operation =
      [](const transport::detail::TransportSeam &seam) {
        seam.mark_ready();
        return std::function<void()>{};
      };
  transport::detail::BinanceTransport transport{
      runtime, clock, 7U,
      transport::detail::make_usdm_transport_config("BTCUSDT", {}),
      std::move(test_options)};
  REQUIRE(transport.start() == transport::TransportStartResult::Started);
  transport.stop();
  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  runtime.stop();

  REQUIRE(observation.stopped);
  REQUIRE(!observation.running);
  REQUIRE(!observation.terminal_error.has_value());
  REQUIRE(observation.connection_generation == 7U);
  REQUIRE(observation.connection_id ==
          "binance-usdm-btcusdt-g7-1700000000000000000");
  REQUIRE(observation.profile == transport::BinanceTransportProfile::DepthOnly);
  REQUIRE(!runtime_observation.fault_reason.has_value());
}

void event_profile_is_dual_websocket_publication() {
  const auto clock = deterministic_clock();
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  REQUIRE(runtime.start() == rt::StartResult::Started);

  transport::detail::TransportTestOptions test_options;
  test_options.start_operation =
      [](const transport::detail::TransportSeam &seam) {
        seam.mark_ready();
        return std::function<void()>{};
      };
  transport::UsdMTransportOptions options;
  options.normalized_event_sink =
      [](std::shared_ptr<const protocol::NormalizedMarketEvent>, std::uint64_t
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
         ,
         binance_market_data::gateway::performance::TraceToken
#endif
      ) { return transport::NormalizedEventSinkResult::Continue; };
  transport::detail::BinanceTransport transport{
      runtime, clock, 2U,
      transport::detail::make_usdm_transport_config("BTCUSDT",
                                                    std::move(options)),
      std::move(test_options)};
  REQUIRE(transport.start() == transport::TransportStartResult::Started);
  const auto active = transport.observe();
  transport.stop();
  runtime.stop();

  REQUIRE(active.profile ==
          transport::BinanceTransportProfile::UsdMCombinedEvents);
  REQUIRE(active.websocket_handshake);
  REQUIRE(active.market_websocket_handshake);
  REQUIRE(active.connection_id != active.market_connection_id);
  REQUIRE(active.agg_trade_frame_count == 0U);
  REQUIRE(active.book_ticker_frame_count == 0U);
  REQUIRE(active.mark_price_frame_count == 0U);
  REQUIRE(active.liquidation_frame_count == 0U);
}

void eth_transport_identity_is_deterministic() {
  const auto clock = deterministic_clock();
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  REQUIRE(runtime.start() == rt::StartResult::Started);

  transport::detail::TransportTestOptions test_options;
  test_options.start_operation =
      [](const transport::detail::TransportSeam &seam) {
        seam.mark_ready();
        return std::function<void()>{};
      };
  transport::detail::BinanceTransport transport{
      runtime, clock, 9U,
      transport::detail::make_usdm_transport_config("ETHUSDT", {}),
      std::move(test_options)};
  REQUIRE(transport.start() == transport::TransportStartResult::Started);
  const auto observation = transport.observe();
  transport.stop();
  runtime.stop();

  REQUIRE(observation.connection_id ==
          "binance-usdm-ethusdt-g9-1700000000000000000");
  REQUIRE(observation.connection_generation == 9U);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"OFFICIAL_ROUTES_ARE_FROZEN", official_routes_are_frozen},
      {"EXACT_USDM_ROUTES_ARE_PRODUCT_BOUND",
       exact_usdm_routes_are_product_bound},
      {"CLEAN_STOP_PRESERVES_SHARED_NETWORK_SEMANTICS",
       clean_stop_preserves_shared_network_semantics},
      {"EVENT_PROFILE_IS_DUAL_WEBSOCKET_PUBLICATION",
       event_profile_is_dual_websocket_publication},
      {"ETH_TRANSPORT_IDENTITY_IS_DETERMINISTIC",
       eth_transport_identity_is_deterministic},
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
