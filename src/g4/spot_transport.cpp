#include "spot_transport.hpp"

#include "spot_protocol.hpp"

#include <boost/asio.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
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

[[nodiscard]] bool is_clean_cancellation(const ErrorCode &error_code,
                                         bool clean_cancel_requested) {
  return clean_cancel_requested && error_code == asio::error::operation_aborted;
}

class AsyncExchangeInfoGet final
    : public std::enable_shared_from_this<AsyncExchangeInfoGet> {
public:
  using Completion = std::function<void(ExchangeInfoResult)>;

  AsyncExchangeInfoGet(asio::io_context &context, ssl::context &tls_context,
                       detail::ExchangeInfoEndpoint endpoint,
                       Completion completion)
      : resolver_{context}, stream_{context, tls_context}, timer_{context},
        endpoint_{std::move(endpoint)}, completion_{std::move(completion)} {
    request_.method(http::verb::get);
    request_.target(endpoint_.target);
    request_.version(11);
    request_.set(http::field::host, endpoint_.host);
    request_.set(http::field::user_agent, "bmd-gateway-g4/1.0.0");
    request_.set(http::field::accept, "application/json");
    request_.set(http::field::connection, "close");
    parser_.body_limit(16U * 1024U * 1024U);
  }

  void start() {
    stream_.set_verify_callback(ssl::host_name_verification(endpoint_.host));
    if (const auto failure =
            configure_tls_stream(stream_.native_handle(), endpoint_.host)) {
      finish(*failure);
      return;
    }
    arm_timeout("exchange-info-dns");
    resolver_.async_resolve(
        endpoint_.host, endpoint_.port,
        [self = shared_from_this()](const ErrorCode &error_code,
                                    tcp::resolver::results_type results) {
          if (self->done_) {
            return;
          }
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::Dns,
                                         "exchange-info-dns", error_code));
            return;
          }
          self->connect(std::move(results));
        });
  }

private:
  void connect(tcp::resolver::results_type results) {
    arm_timeout("exchange-info-tcp-connect");
    beast::get_lowest_layer(stream_).async_connect(
        results, [self = shared_from_this()](const ErrorCode &error_code,
                                             const tcp::endpoint &) {
          if (self->done_) {
            return;
          }
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::Tcp,
                                         "exchange-info-tcp-connect",
                                         error_code));
            return;
          }
          self->handshake();
        });
  }

  void handshake() {
    arm_timeout("exchange-info-tls-handshake");
    stream_.async_handshake(
        ssl::stream_base::client,
        [self = shared_from_this()](const ErrorCode &error_code) {
          if (self->done_) {
            return;
          }
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::Tls,
                                         "exchange-info-tls-handshake",
                                         error_code));
            return;
          }
          self->write_request();
        });
  }

  void write_request() {
    arm_timeout("exchange-info-http-write");
    http::async_write(
        stream_, request_,
        [self = shared_from_this()](const ErrorCode &error_code, std::size_t) {
          if (self->done_) {
            return;
          }
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::HttpWrite,
                                         "exchange-info-http-write",
                                         error_code));
            return;
          }
          self->read_response();
        });
  }

  void read_response() {
    arm_timeout("exchange-info-http-read");
    http::async_read(
        stream_, buffer_, parser_,
        [self = shared_from_this()](const ErrorCode &error_code, std::size_t) {
          if (self->done_) {
            return;
          }
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::HttpRead,
                                         "exchange-info-http-read",
                                         error_code));
            return;
          }
          auto response = self->parser_.release();
          if (response.result_int() != 200) {
            self->finish(network_error(
                NetworkErrorCode::HttpStatus, "exchange-info-http-status",
                "Binance exchangeInfo returned non-200", response.result_int(),
                retry_after_from(response)));
            return;
          }
          self->finish(ExchangeInfoResponse{std::move(response.body()), true});
        });
  }

  void arm_timeout(std::string stage) {
    ++timeout_generation_;
    const auto generation = timeout_generation_;
    timer_.expires_after(endpoint_.stage_timeout);
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

  void finish(ExchangeInfoResult result) {
    if (done_) {
      return;
    }
    done_ = true;
    disarm_timeout();
    resolver_.cancel();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
    completion_(std::move(result));
  }

  tcp::resolver resolver_;
  beast::ssl_stream<beast::tcp_stream> stream_;
  asio::steady_timer timer_;
  beast::flat_buffer buffer_;
  http::request<http::empty_body> request_;
  http::response_parser<http::string_body> parser_;
  detail::ExchangeInfoEndpoint endpoint_;
  Completion completion_;
  std::uint64_t timeout_generation_{0U};
  bool done_{false};
};

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
    io_pending_ = true;
    resolver_.async_resolve(
        std::string{kRestHost}, std::string{kRestPort},
        [self = shared_from_this()](const ErrorCode &error_code,
                                    tcp::resolver::results_type results) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::Dns,
                                        "depth-dns")) {
            return;
          }
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
    if (done_ || cancel_requested_) {
      return;
    }
    cancel_requested_ = true;
    cancellation_completions_pending_ =
        static_cast<std::size_t>(io_pending_) +
        static_cast<std::size_t>(timer_pending_);
    resolver_.cancel();
    if (timer_pending_) {
      timer_.cancel();
    }
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
    if (cancellation_completions_pending_ == 0U) {
      retire_cleanly();
    }
  }

private:
  void connect(tcp::resolver::results_type results) {
    arm_timeout("depth-tcp-connect");
    io_pending_ = true;
    beast::get_lowest_layer(stream_).async_connect(
        results, [self = shared_from_this()](const ErrorCode &error_code,
                                             const tcp::endpoint &) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::Tcp,
                                        "depth-tcp-connect")) {
            return;
          }
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
    io_pending_ = true;
    stream_.async_handshake(
        ssl::stream_base::client,
        [self = shared_from_this()](const ErrorCode &error_code) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::Tls,
                                        "depth-tls-handshake")) {
            return;
          }
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
    io_pending_ = true;
    http::async_write(
        stream_, request_,
        [self = shared_from_this()](const ErrorCode &error_code, std::size_t) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::HttpWrite,
                                        "depth-http-write")) {
            return;
          }
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
    io_pending_ = true;
    http::async_read(
        stream_, buffer_, parser_,
        [self = shared_from_this()](const ErrorCode &error_code, std::size_t) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->cancel_requested_) {
            if (error_code &&
                !is_clean_cancellation(error_code, self->cancel_requested_)) {
              self->finish(error_from_code(NetworkErrorCode::HttpRead,
                                           "depth-http-read", error_code));
              return;
            }
            if (!error_code) {
              self->cancelled_result_.emplace(self->response_result());
            }
            self->complete_cancelled_piece();
            return;
          }
          self->disarm_timeout();
          if (error_code) {
            self->finish(error_from_code(NetworkErrorCode::HttpRead,
                                         "depth-http-read", error_code));
            return;
          }
          self->finish(self->response_result());
        });
  }

  [[nodiscard]] Result response_result() {
    g3::ClockSample received_at{};
    try {
      received_at = clock_();
    } catch (const std::exception &failure) {
      return network_error(NetworkErrorCode::Internal, "depth-receive-clock",
                           failure.what());
    } catch (...) {
      return network_error(NetworkErrorCode::Internal, "depth-receive-clock",
                           "receive clock failed");
    }
    auto response = parser_.release();
    if (response.result_int() != 200) {
      return network_error(NetworkErrorCode::HttpStatus, "depth-http-status",
                           "Binance depth request returned non-200",
                           response.result_int(), retry_after_from(response));
    }
    return std::make_pair(std::move(response), received_at);
  }

  [[nodiscard]] bool handle_cancelled_io(const ErrorCode &error_code,
                                         NetworkErrorCode code,
                                         std::string stage) {
    if (!cancel_requested_) {
      return false;
    }
    if (error_code && !is_clean_cancellation(error_code, cancel_requested_)) {
      finish(error_from_code(code, std::move(stage), error_code));
      return true;
    }
    complete_cancelled_piece();
    return true;
  }

  void arm_timeout(std::string stage) {
    ++timeout_generation_;
    const auto generation = timeout_generation_;
    timer_pending_ = true;
    timer_.expires_after(kStageTimeout);
    timer_.async_wait([self = shared_from_this(), generation,
                       stage = std::move(stage)](const ErrorCode &error_code) {
      if (generation != self->timeout_generation_) {
        return;
      }
      self->timer_pending_ = false;
      if (self->done_) {
        return;
      }
      if (self->cancel_requested_) {
        if (!error_code) {
          self->finish(network_error(NetworkErrorCode::Timeout, stage,
                                     "network stage timed out"));
        } else if (is_clean_cancellation(error_code, self->cancel_requested_)) {
          self->complete_cancelled_piece();
        } else {
          self->finish(error_from_code(NetworkErrorCode::Internal,
                                       stage + "-timer", error_code));
        }
        return;
      }
      if (!error_code) {
        self->finish(network_error(NetworkErrorCode::Timeout, stage,
                                   "network stage timed out"));
      }
    });
  }

  void disarm_timeout() noexcept {
    ++timeout_generation_;
    timer_pending_ = false;
    timer_.cancel();
  }

  void complete_cancelled_piece() {
    if (cancellation_completions_pending_ > 0U) {
      --cancellation_completions_pending_;
    }
    if (cancellation_completions_pending_ != 0U || done_) {
      return;
    }
    if (cancelled_result_.has_value()) {
      auto result = std::move(*cancelled_result_);
      cancelled_result_.reset();
      finish(std::move(result));
      return;
    }
    retire_cleanly();
  }

  void retire_cleanly() noexcept {
    if (done_) {
      return;
    }
    done_ = true;
    resolver_.cancel();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
  }

  void finish(Result result) {
    if (done_) {
      return;
    }
    done_ = true;
    cancellation_completions_pending_ = 0U;
    disarm_timeout();
    resolver_.cancel();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
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
  std::optional<Result> cancelled_result_;
  std::uint64_t timeout_generation_{0U};
  std::size_t cancellation_completions_pending_{0U};
  bool io_pending_{false};
  bool timer_pending_{false};
  bool cancel_requested_{false};
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
    io_pending_ = true;
    resolver_.async_resolve(
        std::string{kWebSocketHost}, std::string{kWebSocketPort},
        [self = shared_from_this()](const ErrorCode &error_code,
                                    tcp::resolver::results_type results) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::Dns,
                                        "websocket-dns")) {
            return;
          }
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
    if (done_ || cancel_requested_) {
      return;
    }
    cancel_requested_ = true;
    cancellation_completions_pending_ =
        static_cast<std::size_t>(io_pending_) +
        static_cast<std::size_t>(timer_pending_);
    resolver_.cancel();
    if (timer_pending_) {
      timer_.cancel();
    }
    read_cancellation_.emit(asio::cancellation_type::terminal);
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
    if (cancellation_completions_pending_ == 0U) {
      retire_cleanly();
    }
  }

private:
  void connect(tcp::resolver::results_type results) {
    arm_timeout("websocket-tcp-connect");
    io_pending_ = true;
    beast::get_lowest_layer(stream_).async_connect(
        results, [self = shared_from_this()](const ErrorCode &error_code,
                                             const tcp::endpoint &) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::Tcp,
                                        "websocket-tcp-connect")) {
            return;
          }
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
    io_pending_ = true;
    stream_.next_layer().async_handshake(
        ssl::stream_base::client,
        [self = shared_from_this()](const ErrorCode &error_code) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code, NetworkErrorCode::Tls,
                                        "websocket-tls-handshake")) {
            return;
          }
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
    timeouts.idle_timeout = detail::kWebSocketIdleTimeout;
    timeouts.keep_alive_pings = detail::kWebSocketKeepAlivePings;
    stream_.set_option(timeouts);
    stream_.set_option(
        websocket::stream_base::decorator([](websocket::request_type &request) {
          request.set(http::field::user_agent, "bmd-gateway-g4/1.0.0");
        }));
    arm_timeout("websocket-handshake");
    io_pending_ = true;
    stream_.async_handshake(
        std::string{kWebSocketHost}, std::string{kWebSocketTarget},
        [self = shared_from_this()](const ErrorCode &error_code) {
          self->io_pending_ = false;
          if (self->done_) {
            return;
          }
          if (self->handle_cancelled_io(error_code,
                                        NetworkErrorCode::WebSocketHandshake,
                                        "websocket-handshake")) {
            return;
          }
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
    io_pending_ = true;
    stream_.async_read(
        buffer_,
        asio::bind_cancellation_slot(
            read_cancellation_.slot(),
            [self = shared_from_this()](const ErrorCode &error_code,
                                        std::size_t) {
              self->io_pending_ = false;
              if (self->done_) {
                return;
              }
              if (error_code) {
                if (is_clean_cancellation(error_code,
                                          self->cancel_requested_)) {
                  self->complete_cancelled_piece();
                } else if (error_code == beast::error::timeout) {
                  self->fail(error_from_code(NetworkErrorCode::Timeout,
                                             "websocket-idle", error_code));
                } else {
                  self->fail(error_from_code(NetworkErrorCode::WebSocketRead,
                                             "websocket-read", error_code));
                }
                return;
              }
              const bool retire_after_message = self->cancel_requested_;
              g3::ClockSample received_at{};
              try {
                received_at = self->clock_();
              } catch (const std::exception &failure) {
                self->fail(network_error(NetworkErrorCode::Internal,
                                         "websocket-receive-clock",
                                         failure.what()));
                return;
              } catch (...) {
                self->fail(network_error(NetworkErrorCode::Internal,
                                         "websocket-receive-clock",
                                         "receive clock failed"));
                return;
              }
              if (!self->stream_.got_text()) {
                self->fail(
                    network_error(NetworkErrorCode::Protocol, "websocket-frame",
                                  "Binance raw stream returned binary data"));
                return;
              }
              auto payload = beast::buffers_to_string(self->buffer_.data());
              self->buffer_.consume(self->buffer_.size());
              if (!self->callbacks_.message(std::move(payload), received_at)) {
                self->retire_cleanly();
                return;
              }
              if (retire_after_message) {
                self->complete_cancelled_piece();
                return;
              }
              self->read();
            }));
  }

  [[nodiscard]] bool handle_cancelled_io(const ErrorCode &error_code,
                                         NetworkErrorCode code,
                                         std::string stage) {
    if (!cancel_requested_) {
      return false;
    }
    if (error_code && !is_clean_cancellation(error_code, cancel_requested_)) {
      fail(error_from_code(code, std::move(stage), error_code));
      return true;
    }
    complete_cancelled_piece();
    return true;
  }

  void arm_timeout(std::string stage) {
    ++timeout_generation_;
    const auto generation = timeout_generation_;
    timer_pending_ = true;
    timer_.expires_after(kStageTimeout);
    timer_.async_wait([self = shared_from_this(), generation,
                       stage = std::move(stage)](const ErrorCode &error_code) {
      if (generation != self->timeout_generation_) {
        return;
      }
      self->timer_pending_ = false;
      if (self->done_) {
        return;
      }
      if (self->cancel_requested_) {
        if (!error_code) {
          self->fail(network_error(NetworkErrorCode::Timeout, stage,
                                   "network stage timed out"));
        } else if (is_clean_cancellation(error_code, self->cancel_requested_)) {
          self->complete_cancelled_piece();
        } else {
          self->fail(error_from_code(NetworkErrorCode::Internal,
                                     stage + "-timer", error_code));
        }
        return;
      }
      if (!error_code) {
        self->fail(network_error(NetworkErrorCode::Timeout, stage,
                                 "network stage timed out"));
      }
    });
  }

  void disarm_timeout() noexcept {
    ++timeout_generation_;
    timer_pending_ = false;
    timer_.cancel();
  }

  void complete_cancelled_piece() noexcept {
    if (cancellation_completions_pending_ > 0U) {
      --cancellation_completions_pending_;
    }
    if (cancellation_completions_pending_ == 0U) {
      retire_cleanly();
    }
  }

  void retire_cleanly() noexcept {
    if (done_) {
      return;
    }
    done_ = true;
    resolver_.cancel();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
  }

  void fail(NetworkError failure) {
    if (done_) {
      return;
    }
    done_ = true;
    cancellation_completions_pending_ = 0U;
    resolver_.cancel();
    disarm_timeout();
    ErrorCode ignored;
    beast::get_lowest_layer(stream_).socket().cancel(ignored);
    beast::get_lowest_layer(stream_).socket().close(ignored);
    callbacks_.failure(std::move(failure));
  }

  tcp::resolver resolver_;
  websocket::stream<beast::ssl_stream<beast::tcp_stream>> stream_;
  asio::steady_timer timer_;
  beast::flat_buffer buffer_;
  g3::RuntimeClock clock_;
  WebSocketCallbacks callbacks_;
  asio::cancellation_signal read_cancellation_;
  std::uint64_t timeout_generation_{0U};
  std::size_t cancellation_completions_pending_{0U};
  bool io_pending_{false};
  bool timer_pending_{false};
  bool cancel_requested_{false};
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

ExchangeInfoResult
detail::fetch_exchange_info_https(const ExchangeInfoEndpoint &endpoint) {
  if (endpoint.host.empty() || endpoint.port.empty() ||
      endpoint.target.empty() ||
      endpoint.stage_timeout <= std::chrono::steady_clock::duration::zero()) {
    return network_error(NetworkErrorCode::Internal, "exchange-info-config",
                         "exchangeInfo endpoint and timeout must be finite");
  }
  try {
    asio::io_context context;
    ssl::context tls_context{ssl::context::tls_client};
    configure_ssl_context(tls_context);
    std::optional<ExchangeInfoResult> result;
    const auto operation = std::make_shared<AsyncExchangeInfoGet>(
        context, tls_context, endpoint,
        [&result](ExchangeInfoResult completed) {
          if (!result.has_value()) {
            result.emplace(std::move(completed));
          }
        });
    operation->start();
    context.run();
    if (!result.has_value()) {
      return network_error(NetworkErrorCode::Internal, "exchange-info-https",
                           "exchangeInfo operation completed without a result");
    }
    return std::move(*result);
  } catch (const std::exception &failure) {
    return network_error(NetworkErrorCode::Internal, "exchange-info-https",
                         failure.what());
  } catch (...) {
    return network_error(NetworkErrorCode::Internal, "exchange-info-https",
                         "exchangeInfo operation failed");
  }
}

ExchangeInfoResult fetch_exchange_info_https() {
  return detail::fetch_exchange_info_https(
      {std::string{kRestHost}, std::string{kRestPort},
       std::string{kExchangeInfoTarget}, kStageTimeout});
}

bool detail::live_acceptance_ready(
    const TransportObservation &transport,
    const g3::RuntimeObservation &runtime) noexcept {
  return transport.stopped && !transport.running &&
         !transport.terminal_error.has_value() && transport.tls_verified &&
         transport.websocket_handshake && transport.rest_depth_fetched &&
         transport.depth_frame_count > 0U &&
         runtime.state == g3::RuntimeState::Live &&
         runtime.projection_status == core::ProjectionStatus::Synchronized &&
         runtime.last_update_id.has_value() &&
         !runtime.fault_reason.has_value();
}

class SpotTransport::Impl final {
public:
  Impl(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
       std::uint64_t connection_generation,
       detail::TransportTestOptions test_options)
      : runtime_{runtime}, clock_{std::move(clock)},
        tls_context_{ssl::context::tls_client}, test_options_{test_options} {
    if (!clock_) {
      throw std::invalid_argument{"G4 transport clock must be injected"};
    }
    if (connection_generation == 0U) {
      throw std::invalid_argument{"G4 connection generation must be nonzero"};
    }
    configure_ssl_context(tls_context_);
    const auto opened_at = clock_();
    observation_.connection_generation = connection_generation;
    observation_.connection_id = "binance-spot-btcusdt-g" +
                                 std::to_string(connection_generation) + "-" +
                                 std::to_string(opened_at.utc_ns);
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
    asio::post(context_, [this] {
      if (test_options_.stop_cut_mode ==
          detail::TransportStopCutTestMode::None) {
        start_websocket();
      } else {
        start_stop_cut_test_transport();
      }
    });
    condition_.wait(lock, [this] {
      return observation_.websocket_handshake ||
             observation_.terminal_error.has_value() || observation_.stopped ||
             stop_requested_;
    });
    if (observation_.stopped || stop_requested_) {
      return TransportStartResult::Stopped;
    }
    return observation_.websocket_handshake ? TransportStartResult::Started
                                            : TransportStartResult::Failed;
  }

  void stop() noexcept {
    bool started = false;
    {
      std::unique_lock lock{mutex_};
      if (observation_.stopped) {
        return;
      }
      if (stop_in_progress_) {
        if (test_options_.stop_waiter_entered) {
          test_options_.stop_waiter_entered();
        }
        condition_.wait(lock, [this] { return observation_.stopped; });
        return;
      }
      stop_in_progress_ = true;
      stop_requested_ = true;
      observation_.running = false;
      started = observation_.started;
    }
    condition_.notify_all();
    if (test_options_.stop_winner_gate) {
      test_options_.stop_winner_gate();
    }
    if (started) {
      asio::post(context_, [this] { begin_clean_shutdown(); });
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
      stop_in_progress_ = false;
    }
    condition_.notify_all();
  }

  [[nodiscard]] TransportObservation observe() const {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  void start_stop_cut_test_transport() {
    test_cancel_timer_.emplace(context_);
    test_cancel_timer_->expires_after(std::chrono::hours{24});
    test_cancel_timer_->async_wait([this](const ErrorCode &error_code) {
      if (is_clean_cancellation(error_code, network_clean_cancel_requested_)) {
        return;
      }
      terminal_failure(error_from_code(NetworkErrorCode::Internal,
                                       "test-clean-cancellation", error_code),
                       false);
    });
    if (test_options_.stop_cut_mode ==
        detail::TransportStopCutTestMode::StartPendingUntilStop) {
      if (test_options_.start_pending) {
        test_options_.start_pending();
      }
      return;
    }
    {
      std::lock_guard lock{mutex_};
      observation_.tls_verified = true;
      observation_.websocket_handshake = true;
      observation_.rest_depth_fetched = true;
      observation_.depth_frame_count = 1U;
      observation_.running = true;
    }
    condition_.notify_all();

    if (test_options_.stop_cut_mode !=
        detail::TransportStopCutTestMode::PreexistingFailure) {
      return;
    }

    {
      std::unique_lock lock{mutex_};
      condition_.wait(lock, [this] { return stop_requested_; });
    }
    // This models an I/O failure already ready at the stop boundary while
    // deterministically exercising the adverse order: clean cancellation is
    // established first in the network domain, then the genuine completion is
    // delivered. terminal_failure() must still preserve it.
    begin_clean_shutdown();
    terminal_failure(
        network_error(NetworkErrorCode::WebSocketRead,
                      "test-preexisting-network-failure",
                      "genuine failure was ready before the stop cut"),
        false);
  }

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
      if (stop_requested_) {
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
      if (observation_.terminal_error.has_value()) {
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
    cancel_network_operations();
    release_network_work();
    condition_.notify_all();
  }

  void begin_clean_shutdown() noexcept {
    if (network_clean_cancel_requested_) {
      return;
    }
    network_clean_cancel_requested_ = true;
    clean_shutdown_barrier_.emplace(context_);
    clean_shutdown_barrier_->expires_at(std::chrono::steady_clock::now());
    clean_shutdown_barrier_->async_wait([this](const ErrorCode &) {
      // Boost 1.91's reactor appends ready socket completions before timers and
      // dequeues every already-due steady timer into one scheduler batch. This
      // extra reactor turn is the network-domain shutdown cut: a read failure
      // or Beast idle timeout already ready before the cut runs before terminal
      // read cancellation can complete. Beast time_out() therefore sets
      // timed_out first, and the read's check_stop_now() must surface
      // beast::error::timeout. No websocket timeout option is changed.
      cancel_network_operations();
      release_network_work();
    });
  }

  void cancel_network_operations() noexcept {
    // Each composed operation retains its own shared Async* owner through its
    // completion. Dropping these parent references after requesting
    // cancellation lets AsyncWebSocket destruction cancel Beast's still-future
    // internal idle wait, so io_context drains without changing timeout policy.
    auto websocket = std::move(websocket_);
    if (websocket) {
      websocket->cancel();
    }
    auto depth_request = std::move(depth_request_);
    if (depth_request) {
      depth_request->cancel();
    }
    if (test_cancel_timer_.has_value()) {
      test_cancel_timer_->cancel();
    }
  }

  void release_network_work() noexcept {
    if (work_guard_.has_value()) {
      work_guard_->reset();
    }
  }

  g3::MarketRuntime &runtime_;
  g3::RuntimeClock clock_;
  asio::io_context context_;
  ssl::context tls_context_;
  std::optional<asio::executor_work_guard<asio::io_context::executor_type>>
      work_guard_;
  std::shared_ptr<AsyncWebSocket> websocket_;
  std::shared_ptr<AsyncHttpsGet> depth_request_;
  std::optional<asio::steady_timer> clean_shutdown_barrier_;
  std::optional<asio::steady_timer> test_cancel_timer_;
  std::thread network_thread_;
  detail::TransportTestOptions test_options_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  TransportObservation observation_;
  bool stop_requested_{false};
  bool stop_in_progress_{false};
  bool network_clean_cancel_requested_{false};
};

SpotTransport::SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                             detail::TransportTestOptions test_options)
    : SpotTransport(runtime, std::move(clock), 1U, test_options) {}

SpotTransport::SpotTransport(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                             std::uint64_t connection_generation,
                             detail::TransportTestOptions test_options)
    : impl_{std::make_unique<Impl>(runtime, std::move(clock),
                                   connection_generation, test_options)} {}

SpotTransport::~SpotTransport() = default;

TransportStartResult SpotTransport::start() { return impl_->start(); }

void SpotTransport::stop() noexcept { impl_->stop(); }

TransportObservation SpotTransport::observe() const { return impl_->observe(); }

} // namespace binance_market_data::gateway::g4
