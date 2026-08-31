#include "usdm_transport.hpp"

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

namespace {

namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
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

#define REQUIRE(condition) require((condition), #condition)

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(1U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    std::abort();
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock deterministic_clock() {
  return [] { return g3::ClockSample{1700000000000000000ULL, 5000U}; };
}

void official_routes_are_frozen() {
  REQUIRE(g11::kUsdMTransportRoutes.rest_host == "fapi.binance.com");
  REQUIRE(g11::kUsdMTransportRoutes.rest_port == "443");
  REQUIRE(g11::kUsdMTransportRoutes.exchange_info_target ==
          "/fapi/v1/exchangeInfo");
  REQUIRE(g11::kUsdMTransportRoutes.depth_target ==
          "/fapi/v1/depth?symbol=BTCUSDT&limit=1000");
  REQUIRE(g11::kUsdMTransportRoutes.websocket_host == "fstream.binance.com");
  REQUIRE(g11::kUsdMTransportRoutes.websocket_port == "443");
  REQUIRE(g11::kUsdMTransportRoutes.websocket_target ==
          "/public/ws/btcusdt@depth@100ms");
  REQUIRE(g11::kUsdMTransportRoutes.websocket_target !=
          "/ws/btcusdt@depth@100ms");
  REQUIRE(g11::kUsdMTransportRoutes.diff_depth_stream == "btcusdt@depth@100ms");
  REQUIRE(g11::kUsdMTransportRoutes.snapshot_limit == 1000U);
}

void clean_stop_preserves_shared_network_semantics() {
  const auto clock = deterministic_clock();
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  REQUIRE(runtime.start() == g3::StartResult::Started);

  g4::detail::TransportTestOptions test_options;
  test_options.stop_cut_mode =
      g4::detail::TransportStopCutTestMode::CleanCancellation;
  g11::UsdMTransport transport{runtime, clock, 7U, {}, std::move(test_options)};
  REQUIRE(transport.start() == g4::TransportStartResult::Started);
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
  REQUIRE(observation.profile == g4::BinanceTransportProfile::DepthOnly);
  REQUIRE(!runtime_observation.fault_reason.has_value());
}

void event_profile_is_closed_depth_only_publication() {
  const auto clock = deterministic_clock();
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  REQUIRE(runtime.start() == g3::StartResult::Started);

  g4::detail::TransportTestOptions test_options;
  test_options.stop_cut_mode =
      g4::detail::TransportStopCutTestMode::CleanCancellation;
  g11::UsdMTransportOptions options;
  options.normalized_event_sink =
      [](std::shared_ptr<const g4::NormalizedMarketEvent>, std::uint64_t) {
        return g4::NormalizedEventSinkResult::Continue;
      };
  g11::UsdMTransport transport{runtime, clock, 2U, std::move(options),
                               std::move(test_options)};
  REQUIRE(transport.start() == g4::TransportStartResult::Started);
  const auto active = transport.observe();
  transport.stop();
  runtime.stop();

  REQUIRE(active.profile == g4::BinanceTransportProfile::DepthWithEvents);
  REQUIRE(active.agg_trade_frame_count == 0U);
  REQUIRE(active.book_ticker_frame_count == 0U);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"OFFICIAL_ROUTES_ARE_FROZEN", official_routes_are_frozen},
      {"CLEAN_STOP_PRESERVES_SHARED_NETWORK_SEMANTICS",
       clean_stop_preserves_shared_network_semantics},
      {"EVENT_PROFILE_IS_CLOSED_DEPTH_ONLY_PUBLICATION",
       event_profile_is_closed_depth_only_publication},
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
