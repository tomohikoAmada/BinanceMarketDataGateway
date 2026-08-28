#include "spot_transport.hpp"

#include "spot_protocol.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace binance_market_data::gateway::g4 {

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;
using ErrorCode = boost::system::error_code;

constexpr std::string_view kRestHost = "api.binance.com";
constexpr std::string_view kRestPort = "443";
constexpr std::string_view kExchangeInfoTarget =
    "/api/v3/exchangeInfo?symbol=BTCUSDT";
constexpr std::string_view kDepthTarget =
    "/api/v3/depth?symbol=BTCUSDT&limit=5000";
constexpr std::string_view kWebSocketHost = "stream.binance.com";
constexpr std::string_view kWebSocketPort = "9443";
constexpr std::string_view kWebSocketTarget = "/ws/btcusdt@depth@100ms";
constexpr auto kStageTimeout = std::chrono::seconds{10};

[[nodiscard]] NetworkError
network_error(NetworkErrorCode code, std::string stage, std::string message,
              std::optional<unsigned> status = std::nullopt,
              std::optional<std::string> retry_after = std::nullopt) {
  return {code, std::move(stage), std::move(message), status,
          std::move(retry_after)};
}

[[nodiscard]] NetworkError error_from_code(NetworkErrorCode code,
                                           std::string stage,
                                           const ErrorCode &failure) {
  return network_error(code, std::move(stage), failure.message());
}

[[nodiscard]] std::optional<std::string>
retry_after_from(const http::response<http::string_body> &response) {
  const auto value = response.find(http::field::retry_after);
  if (value == response.end()) {
    return std::nullopt;
  }
  return std::string{value->value()};
}

[[nodiscard]] std::optional<NetworkError>
configure_tls_stream(SSL *native_handle, std::string_view host) {
  const std::string host_text{host};
  if (SSL_set_tlsext_host_name(native_handle, host_text.c_str()) != 1) {
    const auto openssl_error = ERR_get_error();
    return network_error(NetworkErrorCode::Tls, "sni",
                         openssl_error == 0U
                             ? "failed to set TLS SNI"
                             : ERR_error_string(openssl_error, nullptr));
  }
  return std::nullopt;
}

void configure_ssl_context(ssl::context &context) {
  context.set_verify_mode(ssl::verify_peer);
  context.set_default_verify_paths();
#if defined(__APPLE__)
  // Conan's relocatable OpenSSL package compiles in a package-local default
  // CA path that has no bundle. This is macOS's system-provided CA bundle, not
  // a pinned server or development certificate.
  context.load_verify_file("/etc/ssl/cert.pem");
#endif
}

[[nodiscard]] std::variant<tcp::resolver::results_type, NetworkError>
resolve_with_timeout(asio::io_context &context, std::string_view host,
                     std::string_view port) {
  tcp::resolver resolver{context};
  asio::steady_timer timer{context};
  std::optional<tcp::resolver::results_type> resolved;
  std::optional<NetworkError> failure;
  bool timed_out = false;

  resolver.async_resolve(
      std::string{host}, std::string{port},
      [&](const ErrorCode &error_code, tcp::resolver::results_type results) {
        timer.cancel();
        if (error_code) {
          if (!timed_out) {
            failure = error_from_code(NetworkErrorCode::Dns, "dns", error_code);
          }
          return;
        }
        resolved = std::move(results);
      });
  timer.expires_after(kStageTimeout);
  timer.async_wait([&](const ErrorCode &error_code) {
    if (!error_code && !resolved.has_value() && !failure.has_value()) {
      timed_out = true;
      resolver.cancel();
      failure = network_error(NetworkErrorCode::Timeout, "dns",
                              "DNS resolution timed out");
    }
  });
  context.run();
  context.restart();
  if (failure.has_value()) {
    return std::move(*failure);
  }
  if (!resolved.has_value()) {
    return network_error(NetworkErrorCode::Dns, "dns",
                         "DNS resolution completed without results");
  }
  return std::move(*resolved);
}

[[nodiscard]] std::variant<http::response<http::string_body>, NetworkError>
blocking_https_get(std::string_view target) {
  try {
    asio::io_context context;
    ssl::context tls_context{ssl::context::tls_client};
    configure_ssl_context(tls_context);
    const auto resolved = resolve_with_timeout(context, kRestHost, kRestPort);
    if (const auto *failure = std::get_if<NetworkError>(&resolved)) {
      return *failure;
    }

    beast::ssl_stream<beast::tcp_stream> stream{context, tls_context};
    stream.set_verify_callback(
        ssl::host_name_verification(std::string{kRestHost}));
    if (const auto failure =
            configure_tls_stream(stream.native_handle(), kRestHost)) {
      return *failure;
    }

    ErrorCode error_code;
    beast::get_lowest_layer(stream).expires_after(kStageTimeout);
    beast::get_lowest_layer(stream).connect(
        std::get<tcp::resolver::results_type>(resolved), error_code);
    if (error_code) {
      return error_from_code(NetworkErrorCode::Tcp, "tcp-connect", error_code);
    }

    beast::get_lowest_layer(stream).expires_after(kStageTimeout);
    stream.handshake(ssl::stream_base::client, error_code);
    if (error_code) {
      return error_from_code(NetworkErrorCode::Tls, "tls-handshake",
                             error_code);
    }

    http::request<http::empty_body> request{http::verb::get,
                                            std::string{target}, 11};
    request.set(http::field::host, kRestHost);
    request.set(http::field::user_agent, "bmd-gateway-g4/1.0.0");
    request.set(http::field::accept, "application/json");
    request.set(http::field::connection, "close");
    beast::get_lowest_layer(stream).expires_after(kStageTimeout);
    http::write(stream, request, error_code);
    if (error_code) {
      return error_from_code(NetworkErrorCode::HttpWrite, "http-write",
                             error_code);
    }

    beast::flat_buffer buffer;
    http::response_parser<http::string_body> parser;
    parser.body_limit(16U * 1024U * 1024U);
    beast::get_lowest_layer(stream).expires_after(kStageTimeout);
    http::read(stream, buffer, parser, error_code);
    if (error_code) {
      return error_from_code(NetworkErrorCode::HttpRead, "http-read",
                             error_code);
    }
    return parser.release();
  } catch (const std::exception &failure) {
    return network_error(NetworkErrorCode::Internal, "https", failure.what());
  }
}

class AsyncHttpsGet final : public std::enable_shared_from_this<AsyncHttpsGet> {
public:
  using Result = std::variant<
      std::pair<http::response<http::string_body>, g3::ClockSample>,
      NetworkError>;
  using Completion = std::function<void(Result)>;

  AsyncHttpsGet(asio::io_context &context, ssl::context &tls_context,
                g3::RuntimeClock clock, Completion completion)
      : resolver_{context}, stream_{context, tls_context}, timer_{context},
        clock_{std::move(clock)}, completion_{std::move(completion)} {
    request_.method(http::verb::get);
    request_.target(kDepthTarget);
    request_.version(11);
    request_.set(http::field::host, kRestHost);
    request_.set(http::field::user_agent, "bmd-gateway-g4/1.0.0");
    request_.set(http::field::accept, "application/json");
    request_.set(http::field::connection, "close");
    parser_.body_limit(16U * 1024U * 1024U);
  }

  void start() {
    stream_.set_verify_callback(
        ssl::host_name_verification(std::string{kRestHost}));
    if (const auto failure =
            configure_tls_stream(stream_.native_handle(), kRestHost)) {
      finish(*failure);
      return;
    }
    arm_timeout("depth-dns");
    resolver_.async_resolve(
        std::string{kRestHost}, std::string{kRestPort},
        [self = shared_from_this()](const ErrorCode &error_code,
                                    tcp::resolver::results_type results) {
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::Dns, "depth-dns",
                                         error_code));
            return;
          }
          self->connect(std::move(results));
        });
  }

  void cancel() noexcept {
    done_ = true;
    resolver_.cancel();
    timer_.cancel();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
  }

private:
  void connect(tcp::resolver::results_type results) {
    arm_timeout("depth-tcp-connect");
    beast::get_lowest_layer(stream_).async_connect(
        results, [self = shared_from_this()](const ErrorCode &error_code,
                                             const tcp::endpoint &) {
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::Tcp,
                                         "depth-tcp-connect", error_code));
            return;
          }
          self->handshake();
        });
  }

  void handshake() {
    arm_timeout("depth-tls-handshake");
    stream_.async_handshake(
        ssl::stream_base::client,
        [self = shared_from_this()](const ErrorCode &error_code) {
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::Tls,
                                         "depth-tls-handshake", error_code));
            return;
          }
          self->write_request();
        });
  }

  void write_request() {
    arm_timeout("depth-http-write");
    http::async_write(
        stream_, request_,
        [self = shared_from_this()](const ErrorCode &error_code, std::size_t) {
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::HttpWrite,
                                         "depth-http-write", error_code));
            return;
          }
          self->read_response();
        });
  }

  void read_response() {
    arm_timeout("depth-http-read");
    http::async_read(
        stream_, buffer_, parser_,
        [self = shared_from_this()](const ErrorCode &error_code, std::size_t) {
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::HttpRead,
                                         "depth-http-read", error_code));
            return;
          }
          g3::ClockSample received_at{};
          try {
            received_at = self->clock_();
          } catch (const std::exception &failure) {
            self->finish(network_error(NetworkErrorCode::Internal,
                                       "depth-receive-clock", failure.what()));
            return;
          } catch (...) {
            self->finish(network_error(NetworkErrorCode::Internal,
                                       "depth-receive-clock",
                                       "receive clock failed"));
            return;
          }
          auto response = self->parser_.release();
          if (response.result_int() != 200) {
            self->finish(network_error(
                NetworkErrorCode::HttpStatus, "depth-http-status",
                "Binance depth request returned non-200", response.result_int(),
                retry_after_from(response)));
            return;
          }
          self->finish(std::make_pair(std::move(response), received_at));
        });
  }

  void arm_timeout(std::string stage) {
    ++timeout_generation_;
    const auto generation = timeout_generation_;
    timer_.expires_after(kStageTimeout);
    timer_.async_wait([self = shared_from_this(), generation,
                       stage = std::move(stage)](const ErrorCode &error_code) {
      if (!error_code && !self->done_ &&
          generation == self->timeout_generation_) {
        self->finish(network_error(NetworkErrorCode::Timeout, stage,
                                   "network stage timed out"));
      }
    });
  }

  void disarm_timeout() noexcept {
    ++timeout_generation_;
    timer_.cancel();
  }

  void finish(Result result) {
    if (done_) {
      return;
    }
    done_ = true;
    disarm_timeout();
    completion_(std::move(result));
  }

  tcp::resolver resolver_;
  beast::ssl_stream<beast::tcp_stream> stream_;
  asio::steady_timer timer_;
  beast::flat_buffer buffer_;
  http::request<http::empty_body> request_;
  http::response_parser<http::string_body> parser_;
  g3::RuntimeClock clock_;
  Completion completion_;
  std::uint64_t timeout_generation_{0U};
  bool done_{false};
};

struct WebSocketCallbacks final {
  std::function<void()> tls_verified;
  std::function<void()> active;
  std::function<void()> ping;
  std::function<bool(std::string, g3::ClockSample)> message;
  std::function<void(NetworkError)> failure;
};

class AsyncWebSocket final
    : public std::enable_shared_from_this<AsyncWebSocket> {
public:
  AsyncWebSocket(asio::io_context &context, ssl::context &tls_context,
                 g3::RuntimeClock clock, WebSocketCallbacks callbacks)
      : resolver_{context}, stream_{context, tls_context}, timer_{context},
        clock_{std::move(clock)}, callbacks_{std::move(callbacks)} {}

  void start() {
    stream_.next_layer().set_verify_callback(
        ssl::host_name_verification(std::string{kWebSocketHost}));
    if (const auto failure = configure_tls_stream(
            stream_.next_layer().native_handle(), kWebSocketHost)) {
      fail(*failure);
      return;
    }
    arm_timeout("websocket-dns");
    resolver_.async_resolve(
        std::string{kWebSocketHost}, std::string{kWebSocketPort},
        [self = shared_from_this()](const ErrorCode &error_code,
                                    tcp::resolver::results_type results) {
          self->disarm_timeout();
          if (error_code) {
            self->fail(error_from_code(NetworkErrorCode::Dns, "websocket-dns",
                                       error_code));
            return;
          }
          self->connect(std::move(results));
        });
  }

  void cancel() noexcept {
    done_ = true;
    resolver_.cancel();
    timer_.cancel();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
  }

private:
  void connect(tcp::resolver::results_type results) {
    arm_timeout("websocket-tcp-connect");
    beast::get_lowest_layer(stream_).async_connect(
        results, [self = shared_from_this()](const ErrorCode &error_code,
                                             const tcp::endpoint &) {
          self->disarm_timeout();
          if (error_code) {
            self->fail(error_from_code(NetworkErrorCode::Tcp,
                                       "websocket-tcp-connect", error_code));
            return;
          }
          self->tls_handshake();
        });
  }

  void tls_handshake() {
    arm_timeout("websocket-tls-handshake");
    stream_.next_layer().async_handshake(
        ssl::stream_base::client,
        [self = shared_from_this()](const ErrorCode &error_code) {
          self->disarm_timeout();
          if (error_code) {
            self->fail(error_from_code(NetworkErrorCode::Tls,
                                       "websocket-tls-handshake", error_code));
            return;
          }
          self->callbacks_.tls_verified();
          self->websocket_handshake();
        });
  }

  void websocket_handshake() {
    websocket::stream_base::timeout timeouts;
    timeouts.handshake_timeout = kStageTimeout;
    timeouts.idle_timeout = websocket::stream_base::none();
    timeouts.keep_alive_pings = false;
    stream_.set_option(timeouts);
    stream_.set_option(
        websocket::stream_base::decorator([](websocket::request_type &request) {
          request.set(http::field::user_agent, "bmd-gateway-g4/1.0.0");
        }));
    arm_timeout("websocket-handshake");
    stream_.async_handshake(
        std::string{kWebSocketHost}, std::string{kWebSocketTarget},
        [self = shared_from_this()](const ErrorCode &error_code) {
          self->disarm_timeout();
          if (error_code) {
            self->fail(error_from_code(NetworkErrorCode::WebSocketHandshake,
                                       "websocket-handshake", error_code));
            return;
          }
          self->stream_.control_callback(
              [weak = std::weak_ptr<AsyncWebSocket>{self}](
                  websocket::frame_type kind, beast::string_view) {
                if (kind == websocket::frame_type::ping) {
                  if (const auto locked = weak.lock()) {
                    locked->callbacks_.ping();
                  }
                }
              });
          self->read();
          self->callbacks_.active();
        });
  }

  void read() {
    if (done_) {
      return;
    }
    stream_.async_read(buffer_, [self = shared_from_this()](
                                    const ErrorCode &error_code, std::size_t) {
      if (error_code) {
        self->fail(error_from_code(NetworkErrorCode::WebSocketRead,
                                   "websocket-read", error_code));
        return;
      }
      g3::ClockSample received_at{};
      try {
        received_at = self->clock_();
      } catch (const std::exception &failure) {
        self->fail(network_error(NetworkErrorCode::Internal,
                                 "websocket-receive-clock", failure.what()));
        return;
      } catch (...) {
        self->fail(network_error(NetworkErrorCode::Internal,
                                 "websocket-receive-clock",
                                 "receive clock failed"));
        return;
      }
      if (!self->stream_.got_text()) {
        self->fail(network_error(NetworkErrorCode::Protocol, "websocket-frame",
                                 "Binance raw stream returned binary data"));
        return;
      }
      auto payload = beast::buffers_to_string(self->buffer_.data());
      self->buffer_.consume(self->buffer_.size());
      if (!self->callbacks_.message(std::move(payload), received_at)) {
        self->cancel();
        return;
      }
      self->read();
    });
  }

  void arm_timeout(std::string stage) {
    ++timeout_generation_;
    const auto generation = timeout_generation_;
    timer_.expires_after(kStageTimeout);
    timer_.async_wait([self = shared_from_this(), generation,
                       stage = std::move(stage)](const ErrorCode &error_code) {
      if (!error_code && !self->done_ &&
          generation == self->timeout_generation_) {
        self->fail(network_error(NetworkErrorCode::Timeout, stage,
                                 "network stage timed out"));
      }
    });
  }

  void disarm_timeout() noexcept {
    ++timeout_generation_;
    timer_.cancel();
  }

  void fail(NetworkError failure) {
    if (done_) {
      return;
    }
    done_ = true;
    resolver_.cancel();
    disarm_timeout();
    callbacks_.failure(std::move(failure));
  }

  tcp::resolver resolver_;
  websocket::stream<beast::ssl_stream<beast::tcp_stream>> stream_;
  asio::steady_timer timer_;
  beast::flat_buffer buffer_;
  g3::RuntimeClock clock_;
  WebSocketCallbacks callbacks_;
  std::uint64_t timeout_generation_{0U};
  bool done_{false};
};

} // namespace

g3::ClockSample sample_real_clock() noexcept {
  const auto utc = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  const auto monotonic =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count();
  const auto safe = [](auto value) {
    if (value <= 0) {
      return std::uint64_t{0U};
    }
    using Value = decltype(value);
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if constexpr (sizeof(Value) > sizeof(std::uint64_t)) {
      if (value > static_cast<Value>(maximum)) {
        return maximum;
      }
    }
    return static_cast<std::uint64_t>(value);
  };
  return {safe(utc), safe(monotonic)};
}

ExchangeInfoResult fetch_exchange_info_https() {
  auto response = blocking_https_get(kExchangeInfoTarget);
  if (const auto *failure = std::get_if<NetworkError>(&response)) {
    return *failure;
  }
  auto successful =
      std::get<http::response<http::string_body>>(std::move(response));
  if (successful.result_int() != 200) {
    return network_error(NetworkErrorCode::HttpStatus,
                         "exchange-info-http-status",
                         "Binance exchangeInfo returned non-200",
                         successful.result_int(), retry_after_from(successful));
  }
  return ExchangeInfoResponse{std::move(successful.body()), true};
}

class SpotTransport::Impl final {
public:
  Impl(g3::MarketRuntime &runtime, g3::RuntimeClock clock)
      : runtime_{runtime}, clock_{std::move(clock)},
        tls_context_{ssl::context::tls_client} {
    if (!clock_) {
      throw std::invalid_argument{"G4 transport clock must be injected"};
    }
    configure_ssl_context(tls_context_);
    const auto opened_at = clock_();
    observation_.connection_id =
        "binance-spot-btcusdt-g1-" + std::to_string(opened_at.utc_ns);
  }

  ~Impl() { stop(); }

  [[nodiscard]] TransportStartResult start() {
    std::unique_lock lock{mutex_};
    if (observation_.stopped) {
      return TransportStartResult::Stopped;
    }
    if (observation_.started) {
      return TransportStartResult::AlreadyStarted;
    }
    observation_.started = true;
    work_guard_.emplace(asio::make_work_guard(context_));
    network_thread_ = std::thread{[this] { context_.run(); }};
    asio::post(context_, [this] { start_websocket(); });
    condition_.wait(lock, [this] {
      return observation_.websocket_handshake ||
             observation_.terminal_error.has_value();
    });
    return observation_.websocket_handshake ? TransportStartResult::Started
                                            : TransportStartResult::Failed;
  }

  void stop() noexcept {
    {
      std::lock_guard lock{mutex_};
      if (observation_.stopped) {
        return;
      }
      user_stopping_ = true;
      observation_.running = false;
    }
    if (observation_.started) {
      asio::post(context_, [this] {
        if (websocket_) {
          websocket_->cancel();
        }
        if (depth_request_) {
          depth_request_->cancel();
        }
        if (work_guard_.has_value()) {
          work_guard_->reset();
        }
        context_.stop();
      });
      if (network_thread_.joinable()) {
        try {
          network_thread_.join();
        } catch (...) {
          std::terminate();
        }
      }
    }
    {
      std::lock_guard lock{mutex_};
      observation_.stopped = true;
      observation_.running = false;
    }
    condition_.notify_all();
  }

  [[nodiscard]] TransportObservation observe() const {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  void start_websocket() {
    WebSocketCallbacks callbacks;
    callbacks.tls_verified = [this] {
      std::lock_guard lock{mutex_};
      observation_.tls_verified = true;
    };
    callbacks.active = [this] { websocket_active(); };
    callbacks.ping = [this] {
      std::lock_guard lock{mutex_};
      observation_.server_ping_observed = true;
      condition_.notify_all();
    };
    callbacks.message = [this](std::string payload,
                               g3::ClockSample received_at) {
      return receive_websocket_message(std::move(payload), received_at);
    };
    callbacks.failure = [this](NetworkError failure) {
      terminal_failure(std::move(failure), false);
    };
    websocket_ = std::make_shared<AsyncWebSocket>(context_, tls_context_,
                                                  clock_, std::move(callbacks));
    websocket_->start();
  }

  void websocket_active() {
    {
      std::lock_guard lock{mutex_};
      if (user_stopping_) {
        return;
      }
      observation_.websocket_handshake = true;
      observation_.running = true;
    }
    condition_.notify_all();
    depth_request_ = std::make_shared<AsyncHttpsGet>(
        context_, tls_context_, clock_, [this](AsyncHttpsGet::Result result) {
          receive_depth_response(std::move(result));
        });
    depth_request_->start();
  }

  [[nodiscard]] bool receive_websocket_message(std::string payload,
                                               g3::ClockSample received_at) {
    auto parsed =
        parse_depth_frame(payload, received_at, observe().connection_id);
    if (std::holds_alternative<ServerShutdown>(parsed)) {
      {
        std::lock_guard lock{mutex_};
        observation_.server_shutdown_observed = true;
      }
      terminal_failure(network_error(NetworkErrorCode::ServerShutdown,
                                     "websocket-server-shutdown",
                                     "Binance announced serverShutdown"),
                       false);
      return false;
    }
    if (const auto *failure = std::get_if<ProtocolError>(&parsed)) {
      terminal_failure(network_error(NetworkErrorCode::Protocol,
                                     "websocket-json", failure->message),
                       false);
      return false;
    }

    const auto admitted = runtime_.submit_depth_update(
        std::get<market::DepthUpdate>(std::move(parsed)));
    if (admitted != g3::AdmissionResult::Accepted) {
      terminal_failure(network_error(NetworkErrorCode::RuntimeAdmission,
                                     "websocket-runtime-admission",
                                     "MarketRuntime rejected a depth frame"),
                       false);
      return false;
    }
    {
      std::lock_guard lock{mutex_};
      ++observation_.depth_frame_count;
    }
    condition_.notify_all();
    return true;
  }

  void receive_depth_response(AsyncHttpsGet::Result result) {
    if (const auto *failure = std::get_if<NetworkError>(&result)) {
      terminal_failure(*failure, true);
      return;
    }
    auto [response, received_at] =
        std::get<std::pair<http::response<http::string_body>, g3::ClockSample>>(
            std::move(result));
    auto parsed = parse_depth_snapshot(response.body(), received_at,
                                       "g4-depth-request-1");
    if (const auto *failure = std::get_if<ProtocolError>(&parsed)) {
      terminal_failure(network_error(NetworkErrorCode::Protocol,
                                     "depth-snapshot-json", failure->message),
                       true);
      return;
    }
    const auto admitted = runtime_.submit_snapshot(
        std::get<market::ExchangeDepthSnapshot>(std::move(parsed)));
    if (admitted != g3::AdmissionResult::Accepted) {
      terminal_failure(
          network_error(NetworkErrorCode::RuntimeAdmission,
                        "snapshot-runtime-admission",
                        "MarketRuntime rejected the REST snapshot"),
          true);
      return;
    }
    {
      std::lock_guard lock{mutex_};
      observation_.rest_depth_fetched = true;
    }
    condition_.notify_all();
  }

  void terminal_failure(NetworkError failure, bool snapshot_failure) {
    {
      std::lock_guard lock{mutex_};
      if (user_stopping_ || observation_.terminal_error.has_value()) {
        return;
      }
      observation_.terminal_error = std::move(failure);
      observation_.running = false;
    }
    if (snapshot_failure) {
      static_cast<void>(runtime_.submit_snapshot_failure());
    } else {
      static_cast<void>(runtime_.submit_transport_failure());
    }
    if (websocket_) {
      websocket_->cancel();
    }
    if (depth_request_) {
      depth_request_->cancel();
    }
    if (work_guard_.has_value()) {
      work_guard_->reset();
    }
    condition_.notify_all();
  }

  g3::MarketRuntime &runtime_;
  g3::RuntimeClock clock_;
  asio::io_context context_;
  ssl::context tls_context_;
  std::optional<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard_;
  std::shared_ptr<AsyncWebSocket> websocket_;
  std::shared_ptr<AsyncHttpsGet> depth_request_;
  std::thread network_thread_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  TransportObservation observation_;
  bool user_stopping_{false};
};

SpotTransport::SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock)
    : impl_{std::make_unique<Impl>(runtime, std::move(clock))} {}

SpotTransport::~SpotTransport() = default;

TransportStartResult SpotTransport::start() { return impl_->start(); }

void SpotTransport::stop() noexcept { impl_->stop(); }

TransportObservation SpotTransport::observe() const { return impl_->observe(); }

} // namespace binance_market_data::gateway::g4
