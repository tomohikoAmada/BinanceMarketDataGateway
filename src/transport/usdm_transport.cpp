#include "transport/usdm_transport.hpp"

#include <chrono>
#include <stdexcept>
#include <string>
#include <utility>

namespace binance_market_data::gateway::transport {

using protocol::parse_usdm_depth_frame;
using protocol::parse_usdm_depth_snapshot;
using protocol::parse_usdm_market_event_frame;
using protocol::parse_usdm_public_event_frame;
using protocol::usdm_stream_symbol;

namespace detail {

[[nodiscard]] transport::detail::BinanceTransportConfig
make_usdm_transport_config(std::string exact_symbol,
                           UsdMTransportOptions options) {
  const auto routes = make_usdm_transport_routes(exact_symbol);
  if (!routes.has_value()) {
    throw std::invalid_argument{
        "USD-M symbol cannot be represented by a Binance route"};
  }
  const std::string symbol{exact_symbol};
  transport::detail::BinanceTransportConfig config;
  config.rest_host = routes->rest_host;
  config.rest_port = routes->rest_port;
  config.depth_target = routes->depth_target;
  config.websocket_host = routes->websocket_host;
  config.websocket_port = routes->websocket_port;
  config.websocket_target = routes->websocket_target;
  config.connection_id_prefix = routes->connection_id_prefix;
  config.snapshot_request_id = routes->snapshot_request_id;
  config.user_agent = "bmdg-usdm/1.0.0";
  config.websocket_idle_timeout = std::chrono::seconds{240};
  config.use_keep_alive_pings = true;
  config.defer_depth_request = options.defer_depth_request;
  if (options.normalized_event_sink) {
    config.profile = transport::BinanceTransportProfile::UsdMCombinedEvents;
    const auto &streams = options.enabled_streams;
    config.market_only = !streams.orderbook;
    if (streams.orderbook || streams.diff_depth || streams.book_ticker) {
      std::string target = "/public/stream?streams=";
      bool first = true;
      auto append = [&](const std::string &name) {
        if (!first) {
          target += '/';
        }
        first = false;
        target += name;
      };
      if (streams.orderbook || streams.diff_depth) {
        append(routes->diff_depth_stream);
      }
      if (streams.book_ticker) {
        append(routes->book_ticker_stream);
      }
      config.websocket_target = std::move(target);
    } else {
      config.websocket_target.clear();
    }
    if (streams.agg_trade || streams.mark_price || streams.liquidation) {
      config.market_websocket_host = routes->websocket_host;
      config.market_websocket_port = routes->websocket_port;
      std::string market_target = "/market/stream?streams=";
      bool first = true;
      auto append = [&](const std::string &name) {
        if (!first) {
          market_target += '/';
        }
        first = false;
        market_target += name;
      };
      if (streams.agg_trade) {
        append(routes->agg_trade_stream);
      }
      if (streams.mark_price) {
        append(routes->mark_price_stream);
      }
      if (streams.liquidation) {
        append(routes->force_order_stream);
      }
      config.market_websocket_target = std::move(market_target);
    }
    config.combined_frame_parser = [symbol](std::string_view payload,
                                            runtime::ClockSample received_at,
                                            std::string_view connection_id) {
      return parse_usdm_public_event_frame(payload, received_at, connection_id,
                                           symbol);
    };
    config.market_frame_parser = [symbol](std::string_view payload,
                                          runtime::ClockSample received_at,
                                          std::string_view connection_id) {
      return parse_usdm_market_event_frame(payload, received_at, connection_id,
                                           symbol);
    };
  } else {
    config.profile = transport::BinanceTransportProfile::DepthOnly;
    config.websocket_target = routes->raw_websocket_target;
  }
  config.normalized_event_sink = std::move(options.normalized_event_sink);
  config.reference_event_sink = std::move(options.reference_event_sink);
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
  config.performance_baseline = std::move(options.performance_baseline);
#endif
  config.depth_frame_parser = [symbol](std::string_view payload,
                                       runtime::ClockSample received_at,
                                       std::string_view connection_id) {
    return parse_usdm_depth_frame(payload, received_at, connection_id, symbol);
  };
  config.depth_snapshot_parser = [symbol](std::string_view payload,
                                          runtime::ClockSample received_at,
                                          std::string_view request_id) {
    return parse_usdm_depth_snapshot(payload, received_at, request_id, symbol);
  };
  return config;
}

} // namespace detail

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
  routes.raw_websocket_target =
      "/public/ws/" + *encoded_symbol + "@depth@100ms";
  routes.diff_depth_stream = *encoded_symbol + "@depth@100ms";
  routes.book_ticker_stream = *encoded_symbol + "@bookTicker";
  routes.agg_trade_stream = *encoded_symbol + "@aggTrade";
  routes.mark_price_stream = *encoded_symbol + "@markPrice@1s";
  routes.force_order_stream = *encoded_symbol + "@forceOrder";
  routes.websocket_target =
      "/public/stream?streams=" + routes.diff_depth_stream + "/" +
      routes.book_ticker_stream;
  routes.market_websocket_target =
      "/market/stream?streams=" + routes.agg_trade_stream + "/" +
      routes.mark_price_stream + "/" + routes.force_order_stream;
  routes.snapshot_limit = 1000U;
  routes.connection_id_prefix = "binance-usdm-" + *encoded_symbol + "-g";
  routes.snapshot_request_id =
      "g11-usdm-" + *encoded_symbol + "-depth-request-1";
  return routes;
}

transport::ExchangeInfoResult fetch_usdm_exchange_info_https() {
  return fetch_usdm_exchange_info_https_cancellable({});
}

transport::ExchangeInfoResult
fetch_usdm_exchange_info_https_cancellable(std::stop_token stop_token) {
  auto fetched = transport::fetch_https_json_cancellable(
      std::string{kUsdMTransportRoutes.rest_host},
      std::string{kUsdMTransportRoutes.rest_port},
      std::string{kUsdMTransportRoutes.exchange_info_target},
      "bmdg-usdm/1.0.0", stop_token);
  if (const auto *failure = std::get_if<transport::NetworkError>(&fetched)) {
    return *failure;
  }
  auto ok = std::get<transport::HttpsJsonResponse>(std::move(fetched));
  if (!ok.tls_verified) {
    return transport::NetworkError{transport::NetworkErrorCode::HttpStatus,
                                   "exchange-info-http-status",
                                   "Binance exchangeInfo returned non-200",
                                   std::nullopt, std::nullopt};
  }
  return transport::ExchangeInfoResponse{std::move(ok.body), true,
                                         ok.received_at};
}

UsdMTransport::UsdMTransport(runtime::MarketRuntime &runtime,
                             runtime::RuntimeClock clock,
                             std::uint64_t connection_generation,
                             UsdMTransportOptions options)
    : UsdMTransport(runtime, std::move(clock), std::string{"BTCUSDT"},
                    connection_generation, std::move(options)) {}

UsdMTransport::UsdMTransport(runtime::MarketRuntime &runtime,
                             runtime::RuntimeClock clock,
                             std::string exact_symbol,
                             std::uint64_t connection_generation,
                             UsdMTransportOptions options)
    : transport_{std::make_unique<transport::detail::BinanceTransport>(
          runtime, std::move(clock), connection_generation,
          detail::make_usdm_transport_config(std::move(exact_symbol),
                                             std::move(options)))} {}

UsdMTransport::~UsdMTransport() = default;

transport::TransportStartResult UsdMTransport::start() {
  return transport_->start();
}

void UsdMTransport::begin_depth_bootstrap() {
  transport_->begin_depth_bootstrap();
}

void UsdMTransport::stop() noexcept { transport_->stop(); }

transport::TransportObservation UsdMTransport::observe() const {
  return transport_->observe();
}

} // namespace binance_market_data::gateway::transport
