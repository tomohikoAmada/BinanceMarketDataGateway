#pragma once

#include "spot_transport.hpp"
#include "usdm_protocol.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace binance_market_data::gateway::g11 {

struct UsdMTransportRoutes final {
  std::string_view rest_host;
  std::string_view rest_port;
  std::string_view exchange_info_target;
  std::string_view depth_target;
  std::string_view websocket_host;
  std::string_view websocket_port;
  std::string_view websocket_target;
  std::string_view diff_depth_stream;
  std::size_t snapshot_limit;
};

inline constexpr UsdMTransportRoutes kUsdMTransportRoutes{
    "fapi.binance.com",
    "443",
    "/fapi/v1/exchangeInfo",
    "/fapi/v1/depth?symbol=BTCUSDT&limit=1000",
    "fstream.binance.com",
    "443",
    "/public/ws/btcusdt@depth@100ms",
    "btcusdt@depth@100ms",
    1000U,
};

[[nodiscard]] g4::ExchangeInfoResult fetch_usdm_exchange_info_https();

struct UsdMTransportOptions final {
  g4::NormalizedEventSink normalized_event_sink;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline;
#endif
};

class UsdMTransport final {
public:
  UsdMTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                std::uint64_t connection_generation = 1U,
                UsdMTransportOptions options = {},
                g4::detail::TransportTestOptions test_options = {});
  ~UsdMTransport();

  UsdMTransport(const UsdMTransport &) = delete;
  UsdMTransport &operator=(const UsdMTransport &) = delete;
  UsdMTransport(UsdMTransport &&) = delete;
  UsdMTransport &operator=(UsdMTransport &&) = delete;

  [[nodiscard]] g4::TransportStartResult start();
  void stop() noexcept;
  [[nodiscard]] g4::TransportObservation observe() const;

private:
  std::unique_ptr<g4::detail::BinanceTransport> transport_;
};

} // namespace binance_market_data::gateway::g11
