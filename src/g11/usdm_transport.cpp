#include "usdm_transport.hpp"

#include <chrono>
#include <stdexcept>
#include <string>
#include <utility>

namespace binance_market_data::gateway::g11 {

namespace {

constexpr auto kStageTimeout = std::chrono::seconds{10};

[[nodiscard]] g4::detail::BinanceTransportConfig
make_usdm_transport_config(std::string exact_symbol,
                           UsdMTransportOptions options) {
  const auto routes = make_usdm_transport_routes(exact_symbol);
  if (!routes.has_value()) {
    throw std::invalid_argument{
        "USD-M symbol cannot be represented by a Binance route"};
  }
  const std::string symbol{exact_symbol};
  g4::detail::BinanceTransportConfig config;
  config.rest_host = routes->rest_host;
  config.rest_port = routes->rest_port;
  config.depth_target = routes->depth_target;
  config.websocket_host = routes->websocket_host;
  config.websocket_port = routes->websocket_port;
  config.websocket_target = routes->websocket_target;
  config.connection_id_prefix = routes->connection_id_prefix;
  config.snapshot_request_id = routes->snapshot_request_id;
  config.user_agent = "bmd-gateway-g11-usdm/1.0.0";
  config.profile = options.normalized_event_sink
                       ? g4::BinanceTransportProfile::DepthWithEvents
                       : g4::BinanceTransportProfile::DepthOnly;
  config.normalized_event_sink = std::move(options.normalized_event_sink);
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  config.performance_baseline = std::move(options.performance_baseline);
#endif
  config.depth_frame_parser = [symbol](std::string_view payload,
                                       g3::ClockSample received_at,
                                       std::string_view connection_id) {
    return parse_usdm_depth_frame(payload, received_at, connection_id, symbol);
  };
  config.depth_snapshot_parser = [symbol](std::string_view payload,
                                          g3::ClockSample received_at,
                                          std::string_view request_id) {
    return parse_usdm_depth_snapshot(payload, received_at, request_id, symbol);
  };
  return config;
}

} // namespace

std::optional<UsdMTransportRoutes>
make_usdm_transport_routes(std::string_view exact_symbol) {
  const auto encoded_symbol = usdm_stream_symbol(exact_symbol);
  if (!encoded_symbol.has_value()) {
    return std::nullopt;
  }

  UsdMTransportRoutes routes;
  routes.rest_host = kUsdMTransportRoutes.rest_host;
  routes.rest_port = kUsdMTransportRoutes.rest_port;
  routes.exchange_info_target = kUsdMTransportRoutes.exchange_info_target;
  routes.depth_target =
      "/fapi/v1/depth?symbol=" + std::string{exact_symbol} + "&limit=1000";
  routes.websocket_host = kUsdMTransportRoutes.websocket_host;
  routes.websocket_port = kUsdMTransportRoutes.websocket_port;
  routes.websocket_target = "/public/ws/" + *encoded_symbol + "@depth@100ms";
  routes.diff_depth_stream = *encoded_symbol + "@depth@100ms";
  routes.snapshot_limit = 1000U;
  routes.connection_id_prefix = "binance-usdm-" + *encoded_symbol + "-g";
  routes.snapshot_request_id =
      "g11-usdm-" + *encoded_symbol + "-depth-request-1";
  return routes;
}

g4::ExchangeInfoResult fetch_usdm_exchange_info_https() {
  return g4::detail::fetch_exchange_info_https(
      {std::string{kUsdMTransportRoutes.rest_host},
       std::string{kUsdMTransportRoutes.rest_port},
       std::string{kUsdMTransportRoutes.exchange_info_target}, kStageTimeout});
}

UsdMTransport::UsdMTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                             std::uint64_t connection_generation,
                             UsdMTransportOptions options,
                             g4::detail::TransportTestOptions test_options)
    : UsdMTransport(runtime, std::move(clock), std::string{"BTCUSDT"},
                    connection_generation, std::move(options),
                    std::move(test_options)) {}

UsdMTransport::UsdMTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                             std::string exact_symbol,
                             std::uint64_t connection_generation,
                             UsdMTransportOptions options,
                             g4::detail::TransportTestOptions test_options)
    : transport_{std::make_unique<g4::detail::BinanceTransport>(
          runtime, std::move(clock), connection_generation,
          make_usdm_transport_config(std::move(exact_symbol),
                                     std::move(options)),
          std::move(test_options))} {}

UsdMTransport::~UsdMTransport() = default;

g4::TransportStartResult UsdMTransport::start() { return transport_->start(); }

void UsdMTransport::stop() noexcept { transport_->stop(); }

g4::TransportObservation UsdMTransport::observe() const {
  return transport_->observe();
}

} // namespace binance_market_data::gateway::g11
