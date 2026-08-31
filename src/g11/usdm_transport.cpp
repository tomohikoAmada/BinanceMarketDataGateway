#include "usdm_transport.hpp"

#include <chrono>
#include <string>
#include <utility>

namespace binance_market_data::gateway::g11 {

namespace {

constexpr auto kStageTimeout = std::chrono::seconds{10};

[[nodiscard]] g4::detail::BinanceTransportConfig
make_usdm_transport_config(UsdMTransportOptions options) {
  g4::detail::BinanceTransportConfig config;
  config.rest_host = kUsdMTransportRoutes.rest_host;
  config.rest_port = kUsdMTransportRoutes.rest_port;
  config.depth_target = kUsdMTransportRoutes.depth_target;
  config.websocket_host = kUsdMTransportRoutes.websocket_host;
  config.websocket_port = kUsdMTransportRoutes.websocket_port;
  config.websocket_target = kUsdMTransportRoutes.websocket_target;
  config.connection_id_prefix = "binance-usdm-btcusdt-g";
  config.snapshot_request_id = "g11-usdm-depth-request-1";
  config.user_agent = "bmd-gateway-g11-usdm/1.0.0";
  config.profile = options.normalized_event_sink
                       ? g4::BinanceTransportProfile::DepthWithEvents
                       : g4::BinanceTransportProfile::DepthOnly;
  config.normalized_event_sink = std::move(options.normalized_event_sink);
  config.depth_frame_parser = parse_usdm_depth_frame;
  config.depth_snapshot_parser = parse_usdm_depth_snapshot;
  return config;
}

} // namespace

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
    : transport_{std::make_unique<g4::detail::BinanceTransport>(
          runtime, std::move(clock), connection_generation,
          make_usdm_transport_config(std::move(options)),
          std::move(test_options))} {}

UsdMTransport::~UsdMTransport() = default;

g4::TransportStartResult UsdMTransport::start() { return transport_->start(); }

void UsdMTransport::stop() noexcept { transport_->stop(); }

g4::TransportObservation UsdMTransport::observe() const {
  return transport_->observe();
}

} // namespace binance_market_data::gateway::g11
