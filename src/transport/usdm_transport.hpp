#pragma once

#include "protocol/usdm_protocol.hpp"
#include "transport/spot_transport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace binance_market_data::gateway::transport {

struct UsdMTransportRoutes final {
  std::string rest_host;
  std::string rest_port;
  std::string exchange_info_target;
  std::string depth_target;
  std::string websocket_host;
  std::string websocket_port;
  std::string websocket_target;
  std::string raw_websocket_target;
  std::string market_websocket_target;
  std::string diff_depth_stream;
  std::string book_ticker_stream;
  std::string agg_trade_stream;
  std::string mark_price_stream;
  std::string force_order_stream;
  std::size_t snapshot_limit{1000U};
  std::string connection_id_prefix;
  std::string snapshot_request_id;
};

inline const UsdMTransportRoutes kUsdMTransportRoutes{
    "fapi.binance.com",
    "443",
    "/fapi/v1/exchangeInfo",
    "/fapi/v1/depth?symbol=BTCUSDT&limit=1000",
    "fstream.binance.com",
    "443",
    "/public/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker",
    "/public/ws/btcusdt@depth@100ms",
    "/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/"
    "btcusdt@forceOrder",
    "btcusdt@depth@100ms",
    "btcusdt@bookTicker",
    "btcusdt@aggTrade",
    "btcusdt@markPrice@1s",
    "btcusdt@forceOrder",
    1000U,
    "binance-usdm-btcusdt-g",
    "g11-usdm-btcusdt-depth-request-1",
};

[[nodiscard]] std::optional<UsdMTransportRoutes>
make_usdm_transport_routes(std::string_view exact_symbol);

[[nodiscard]] transport::ExchangeInfoResult fetch_usdm_exchange_info_https();

struct UsdMTransportOptions final {
  transport::NormalizedEventSink normalized_event_sink;
  transport::NormalizedEventSink reference_event_sink;
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline;
#endif
};

class UsdMTransport final {
public:
  UsdMTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::uint64_t connection_generation = 1U,
                UsdMTransportOptions options = {},
                transport::detail::TransportTestOptions test_options = {});
  UsdMTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::string exact_symbol,
                std::uint64_t connection_generation = 1U,
                UsdMTransportOptions options = {},
                transport::detail::TransportTestOptions test_options = {});
  ~UsdMTransport();

  UsdMTransport(const UsdMTransport &) = delete;
  UsdMTransport &operator=(const UsdMTransport &) = delete;
  UsdMTransport(UsdMTransport &&) = delete;
  UsdMTransport &operator=(UsdMTransport &&) = delete;

  [[nodiscard]] transport::TransportStartResult start();
  void stop() noexcept;
  [[nodiscard]] transport::TransportObservation observe() const;

private:
  std::unique_ptr<transport::detail::BinanceTransport> transport_;
};

} // namespace binance_market_data::gateway::transport
