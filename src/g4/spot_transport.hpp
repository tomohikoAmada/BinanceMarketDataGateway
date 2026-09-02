#pragma once

#include "market_runtime.hpp"
#include "spot_protocol.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace binance_market_data::gateway::g4 {

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
};

using ExchangeInfoResult = std::variant<ExchangeInfoResponse, NetworkError>;

[[nodiscard]] ExchangeInfoResult fetch_exchange_info_https();
[[nodiscard]] g3::ClockSample sample_real_clock() noexcept;

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
};

using SpotTransportProfile = BinanceTransportProfile;

enum class NormalizedEventSinkResult : std::uint8_t {
  Continue,
  InvariantFailure,
};

using NormalizedEventSink = std::function<NormalizedEventSinkResult(
    std::shared_ptr<const NormalizedMarketEvent>, std::uint64_t
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    ,
    performance::TraceToken
#endif
    )>;

struct SpotTransportOptions final {
  SpotTransportProfile profile{SpotTransportProfile::DepthOnly};
  NormalizedEventSink normalized_event_sink;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline;
#endif
};

struct TransportObservation final {
  bool started{false};
  bool running{false};
  bool stopped{false};
  bool tls_verified{false};
  bool websocket_handshake{false};
  bool rest_depth_fetched{false};
  bool server_ping_observed{false};
  bool server_shutdown_observed{false};
  std::size_t depth_frame_count{0U};
  std::size_t agg_trade_frame_count{0U};
  std::size_t book_ticker_frame_count{0U};
  std::optional<std::uint64_t> last_event_utc_ns;
  SpotTransportProfile profile{SpotTransportProfile::DepthOnly};
  std::string connection_id;
  std::uint64_t connection_generation{1U};
  std::optional<NetworkError> terminal_error;
};

// Internal G4 policy and deterministic test seams. This header is exposed only
// by the opt-in G4 build target; it is not an installed API.
namespace detail {

inline constexpr auto kWebSocketIdleTimeout = std::chrono::seconds{90};
inline constexpr bool kWebSocketKeepAlivePings = false;

enum class TransportStopCutTestMode : std::uint8_t {
  None,
  CleanCancellation,
  PreexistingFailure,
  StartPendingUntilStop,
};

struct TransportTestOptions final {
  TransportStopCutTestMode stop_cut_mode{TransportStopCutTestMode::None};
  std::function<void()> start_pending;
  std::function<void()> stop_winner_gate;
  std::function<void()> stop_waiter_entered;
};

struct ExchangeInfoEndpoint final {
  std::string host;
  std::string port;
  std::string target;
  std::chrono::steady_clock::duration stage_timeout;
};

[[nodiscard]] ExchangeInfoResult
fetch_exchange_info_https(const ExchangeInfoEndpoint &endpoint);

using DepthFrameParser = std::function<DepthFrameResult(
    std::string_view, g3::ClockSample, std::string_view)>;
using CombinedFrameParser = std::function<CombinedFrameResult(
    std::string_view, g3::ClockSample, std::string_view)>;
using DepthSnapshotParser = std::function<DepthSnapshotResult(
    std::string_view, g3::ClockSample, std::string_view)>;

// Product-specific routes and parsers supplied to the shared Binance network
// mechanism. It contains no sequence or Projection policy.
struct BinanceTransportConfig final {
  std::string rest_host;
  std::string rest_port;
  std::string depth_target;
  std::string websocket_host;
  std::string websocket_port;
  std::string websocket_target;
  std::string connection_id_prefix;
  std::string snapshot_request_id;
  std::string user_agent;
  BinanceTransportProfile profile{BinanceTransportProfile::DepthOnly};
  NormalizedEventSink normalized_event_sink;
  DepthFrameParser depth_frame_parser;
  CombinedFrameParser combined_frame_parser;
  DepthSnapshotParser depth_snapshot_parser;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline;
#endif
};

class BinanceTransport final {
public:
  BinanceTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
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
                      const g3::RuntimeObservation &runtime) noexcept;

} // namespace detail

// One concrete Binance Spot BTCUSDT transport. start(), stop(), and destruction
// are coordinated by the same external lifecycle owner as MarketRuntime.
class SpotTransport final {
public:
  SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                detail::TransportTestOptions test_options = {});
  SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                std::uint64_t connection_generation,
                detail::TransportTestOptions test_options = {});
  SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                std::uint64_t connection_generation,
                SpotTransportOptions options,
                detail::TransportTestOptions test_options = {});
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

} // namespace binance_market_data::gateway::g4
