#pragma once

#include "market_runtime.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
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
  std::string connection_id;
  std::uint64_t connection_generation{1U};
  std::optional<NetworkError> terminal_error;
};

// Internal G4 policy and deterministic test seams. This header is exposed only
// by the opt-in G4 build target; it is not an installed API.
namespace detail {

inline constexpr auto kWebSocketIdleTimeout = std::chrono::seconds{90};
inline constexpr bool kWebSocketKeepAlivePings = false;

struct ExchangeInfoEndpoint final {
  std::string host;
  std::string port;
  std::string target;
  std::chrono::steady_clock::duration stage_timeout;
};

[[nodiscard]] ExchangeInfoResult
fetch_exchange_info_https(const ExchangeInfoEndpoint &endpoint);

[[nodiscard]] bool
live_acceptance_ready(const TransportObservation &transport,
                      const g3::RuntimeObservation &runtime) noexcept;

} // namespace detail

// One concrete Binance Spot BTCUSDT transport. start(), stop(), and destruction
// are coordinated by the same external lifecycle owner as MarketRuntime.
class SpotTransport final {
public:
  SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock);
  ~SpotTransport();

  SpotTransport(const SpotTransport &) = delete;
  SpotTransport &operator=(const SpotTransport &) = delete;
  SpotTransport(SpotTransport &&) = delete;
  SpotTransport &operator=(SpotTransport &&) = delete;

  [[nodiscard]] TransportStartResult start();
  void stop() noexcept;
  [[nodiscard]] TransportObservation observe() const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace binance_market_data::gateway::g4
