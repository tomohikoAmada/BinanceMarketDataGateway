#pragma once

#include "protocol/spot_protocol.hpp"
#include "runtime/market_runtime.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace binance_market_data::gateway::transport {

using protocol::CombinedFrameResult;
using protocol::DepthFrameResult;
using protocol::DepthSnapshotResult;
using protocol::NormalizedMarketEvent;
using protocol::ProtocolError;
using protocol::ServerShutdown;
namespace market = protocol::market;

enum class NetworkErrorCode : std::uint8_t {
  Timeout,
  Dns,
  Tcp,
  Tls,
  WebSocketHandshake,
  WebSocketRead,
  HttpWrite,
  HttpRead,
  HttpStatus,
  Protocol,
  RuntimeAdmission,
  ServerShutdown,
  Internal,
};

struct NetworkError final {
  NetworkErrorCode code;
  std::string stage;
  std::string message;
  std::optional<unsigned> http_status;
  std::optional<std::string> retry_after;
};

struct ExchangeInfoResponse final {
  std::string body;
  bool tls_verified{false};
  runtime::ClockSample received_at{};
};

using ExchangeInfoResult = std::variant<ExchangeInfoResponse, NetworkError>;

struct HttpsJsonResponse final {
  std::string body;
  runtime::ClockSample received_at;
  bool tls_verified{false};
};

using HttpsJsonResult = std::variant<HttpsJsonResponse, NetworkError>;

// All strings in this route description own their storage. The exact symbol
// remains the caller's MarketKey identity; lowercase text is only Binance
// transport encoding.
struct SpotTransportRoutes final {
  std::string rest_host;
  std::string rest_port;
  std::string exchange_info_target;
  std::string depth_target;
  std::string websocket_host;
  std::string websocket_port;
  std::string websocket_target;
  std::string combined_websocket_target;
  std::string diff_depth_stream;
  std::string agg_trade_stream;
  std::string book_ticker_stream;
  std::size_t snapshot_limit{5000U};
  std::string connection_id_prefix;
  std::string snapshot_request_id;
};

[[nodiscard]] std::optional<SpotTransportRoutes>
make_spot_transport_routes(std::string_view exact_symbol);

[[nodiscard]] ExchangeInfoResult fetch_exchange_info_https();
[[nodiscard]] ExchangeInfoResult
fetch_exchange_info_https(std::string_view exact_symbol);
// G12-C set-level metadata acquisition. This intentionally fetches the full
// Spot exchangeInfo document once for all configured Spot products.
[[nodiscard]] ExchangeInfoResult fetch_spot_exchange_info_set_https();
[[nodiscard]] runtime::ClockSample sample_real_clock() noexcept;

// Blocking one-shot GET for low-frequency reference polling. It reuses the
// shared TLS/HTTP implementation and public API budget; callers run it on their
// product-owned reference thread, never on a WebSocket I/O thread.
[[nodiscard]] HttpsJsonResult fetch_https_json(std::string host,
                                               std::string port,
                                               std::string target,
                                               std::string user_agent);

enum class TransportStartResult : std::uint8_t {
  Started,
  AlreadyStarted,
  Failed,
  Stopped,
};

enum class BinanceTransportProfile : std::uint8_t {
  DepthOnly,
  G9CombinedEvents,
  SpotCombinedEvents = G9CombinedEvents,
  DepthWithEvents,
  UsdMCombinedEvents,
};

using SpotTransportProfile = BinanceTransportProfile;

enum class NormalizedEventSinkResult : std::uint8_t {
  Continue,
  InvariantFailure,
};

using NormalizedEventSink = std::function<NormalizedEventSinkResult(
    std::shared_ptr<const NormalizedMarketEvent>, std::uint64_t
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
    ,
    performance::TraceToken
#endif
    )>;

struct SpotTransportOptions final {
  SpotTransportProfile profile{SpotTransportProfile::DepthOnly};
  NormalizedEventSink normalized_event_sink;
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline;
#endif
};

struct TransportObservation final {
  bool started{false};
  bool running{false};
  bool stopped{false};
  bool tls_verified{false};
  bool websocket_handshake{false};
  bool market_websocket_handshake{false};
  bool rest_depth_fetched{false};
  bool server_ping_observed{false};
  bool server_shutdown_observed{false};
  std::size_t depth_frame_count{0U};
  std::size_t agg_trade_frame_count{0U};
  std::size_t book_ticker_frame_count{0U};
  std::size_t mark_price_frame_count{0U};
  std::size_t liquidation_frame_count{0U};
  std::optional<std::uint64_t> last_event_utc_ns;
  SpotTransportProfile profile{SpotTransportProfile::DepthOnly};
  std::string connection_id;
  std::string market_connection_id;
  std::uint64_t connection_generation{1U};
  std::optional<NetworkError> terminal_error;
};

// Internal G4 policy and deterministic test seams. This header is exposed only
// by the opt-in G4 build target; it is not an installed API.
namespace detail {

inline constexpr auto kWebSocketIdleTimeout = std::chrono::seconds{90};
inline constexpr bool kWebSocketKeepAlivePings = false;

struct TransportSeam final {
  std::function<void()> mark_ready;
  std::function<void(NetworkError failure, bool snapshot_failure)> fail;
};

struct TransportTestOptions final {
  std::function<std::function<void()>(const TransportSeam &seam)>
      start_operation;
  std::function<void()> stop_winner_gate;
  std::function<void()> stop_waiter_entered;
};

struct ExchangeInfoEndpoint final {
  std::string host;
  std::string port;
  std::string target;
  std::chrono::steady_clock::duration stage_timeout;
};

[[nodiscard]] ExchangeInfoEndpoint spot_exchange_info_set_endpoint();

[[nodiscard]] ExchangeInfoResult
fetch_exchange_info_https(const ExchangeInfoEndpoint &endpoint);

using DepthFrameParser = std::function<DepthFrameResult(
    std::string_view, runtime::ClockSample, std::string_view)>;
using CombinedFrameParser = std::function<CombinedFrameResult(
    std::string_view, runtime::ClockSample, std::string_view)>;
using DepthSnapshotParser = std::function<DepthSnapshotResult(
    std::string_view, runtime::ClockSample, std::string_view)>;

// Product-specific routes and parsers supplied to the shared Binance network
// mechanism. It contains no sequence or Projection policy.
struct BinanceTransportConfig final {
  std::string rest_host;
  std::string rest_port;
  std::string depth_target;
  std::string websocket_host;
  std::string websocket_port;
  std::string websocket_target;
  std::string market_websocket_host;
  std::string market_websocket_port;
  std::string market_websocket_target;
  std::string connection_id_prefix;
  std::string snapshot_request_id;
  std::string user_agent;
  BinanceTransportProfile profile{BinanceTransportProfile::DepthOnly};
  NormalizedEventSink normalized_event_sink;
  NormalizedEventSink reference_event_sink;
  DepthFrameParser depth_frame_parser;
  CombinedFrameParser combined_frame_parser;
  CombinedFrameParser market_frame_parser;
  DepthSnapshotParser depth_snapshot_parser;
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline;
#endif
};

[[nodiscard]] BinanceTransportConfig
spot_transport_config(std::string exact_symbol, SpotTransportOptions options);

class BinanceTransport final {
public:
  BinanceTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                   std::uint64_t connection_generation,
                   BinanceTransportConfig config,
                   TransportTestOptions test_options = {});
  ~BinanceTransport();

  BinanceTransport(const BinanceTransport &) = delete;
  BinanceTransport &operator=(const BinanceTransport &) = delete;
  BinanceTransport(BinanceTransport &&) = delete;
  BinanceTransport &operator=(BinanceTransport &&) = delete;

  [[nodiscard]] TransportStartResult start();
  void stop() noexcept;
  [[nodiscard]] TransportObservation observe() const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] bool
live_acceptance_ready(const TransportObservation &transport,
                      const runtime::RuntimeObservation &runtime) noexcept;

} // namespace detail

// One concrete Binance Spot transport. start(), stop(), and destruction are
// coordinated by the same external lifecycle owner as MarketRuntime.
class SpotTransport final {
public:
  SpotTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock);
  SpotTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::uint64_t connection_generation);
  SpotTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::uint64_t connection_generation,
                SpotTransportOptions options);
  SpotTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::string exact_symbol);
  SpotTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::string exact_symbol, std::uint64_t connection_generation);
  SpotTransport(runtime::MarketRuntime &runtime, runtime::RuntimeClock clock,
                std::string exact_symbol, std::uint64_t connection_generation,
                SpotTransportOptions options);
  ~SpotTransport();

  SpotTransport(const SpotTransport &) = delete;
  SpotTransport &operator=(const SpotTransport &) = delete;
  SpotTransport(SpotTransport &&) = delete;
  SpotTransport &operator=(SpotTransport &&) = delete;

  [[nodiscard]] TransportStartResult start();
  void stop() noexcept;
  [[nodiscard]] TransportObservation observe() const;

private:
  std::unique_ptr<detail::BinanceTransport> transport_;
};

} // namespace binance_market_data::gateway::transport
