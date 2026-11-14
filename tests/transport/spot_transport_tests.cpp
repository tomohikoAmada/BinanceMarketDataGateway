#include "transport/public_api_budget.hpp"
#include "transport/spot_transport.hpp"

#include <boost/asio.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <variant>
namespace protocol = binance_market_data::gateway::protocol;
namespace transport = binance_market_data::gateway::transport;
namespace rt = binance_market_data::gateway::runtime;

namespace {

namespace core = binance_market_data::projection::v1;
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;
using ErrorCode = boost::system::error_code;

class LocalWebSocketPair final {
public:
  LocalWebSocketPair()
      : acceptor_{server_context_, {tcp::v4(), 0U}},
        client_{std::make_unique<Stream>(client_context_)},
        server_{std::make_unique<Stream>(server_context_)} {}

  [[nodiscard]] bool handshake() {
    ErrorCode client_error;
    client_->next_layer().connect(acceptor_.local_endpoint(), client_error);
    if (client_error) {
      return false;
    }

    ErrorCode server_error;
    acceptor_.accept(server_->next_layer(), server_error);
    if (server_error) {
      return false;
    }

    std::thread server_handshake{
        [this, &server_error] { server_->accept(server_error); }};
    client_->handshake("localhost", "/", client_error);
    server_handshake.join();
    return !client_error && !server_error;
  }

  using Stream = websocket::stream<tcp::socket>;

  asio::io_context client_context_;
  asio::io_context server_context_;
  tcp::acceptor acceptor_;
  std::unique_ptr<Stream> client_;
  std::unique_ptr<Stream> server_;
};

void set_idle_timeout(LocalWebSocketPair::Stream &stream,
                      std::chrono::steady_clock::duration idle_timeout) {
  websocket::stream_base::timeout timeouts;
  timeouts.handshake_timeout = websocket::stream_base::none();
  timeouts.idle_timeout = idle_timeout;
  timeouts.keep_alive_pings = false;
  stream.set_option(timeouts);
}

[[nodiscard]] bool outstanding_websocket_read_is_explicitly_cancelled() {
  LocalWebSocketPair pair;
  if (!pair.handshake()) {
    return false;
  }
  set_idle_timeout(*pair.client_, transport::detail::kWebSocketIdleTimeout);

  asio::cancellation_signal cancellation;
  beast::flat_buffer buffer;
  ErrorCode read_result;
  std::size_t completion_count = 0U;
  pair.client_->async_read(
      buffer,
      asio::bind_cancellation_slot(
          cancellation.slot(), [&pair, &read_result, &completion_count](
                                   const ErrorCode &error_code, std::size_t) {
            read_result = error_code;
            ++completion_count;
            // Production releases its owning reference at the cancellation
            // cut. Destruction after the composed read completion cancels
            // Beast's still-future internal idle wait without changing policy.
            pair.client_.reset();
          }));

  const auto started_at = std::chrono::steady_clock::now();
  cancellation.emit(asio::cancellation_type::terminal);
  ErrorCode ignored;
  pair.client_->next_layer().cancel(ignored);
  pair.client_->next_layer().close(ignored);
  static_cast<void>(pair.client_context_.run());
  const auto elapsed = std::chrono::steady_clock::now() - started_at;

  return completion_count == 1U &&
         read_result == asio::error::operation_aborted && !pair.client_ &&
         pair.client_context_.stopped() && elapsed < std::chrono::seconds{1};
}

[[nodiscard]] bool idle_timeout_ready_before_stop_cut_is_preserved() {
  LocalWebSocketPair pair;
  if (!pair.handshake()) {
    return false;
  }
  // A zero test-only duration makes Beast's authoritative idle timer due as
  // soon as async_read arms it. The context is deliberately not run yet, so
  // the timer completion is ready but has not executed.
  set_idle_timeout(*pair.client_, std::chrono::steady_clock::duration::zero());

  asio::cancellation_signal cancellation;
  beast::flat_buffer buffer;
  ErrorCode read_result;
  std::size_t completion_count = 0U;
  bool shutdown_cut_executed = false;
  pair.client_->async_read(
      buffer,
      asio::bind_cancellation_slot(
          cancellation.slot(),
          [&pair, &read_result, &completion_count,
           &shutdown_cut_executed](const ErrorCode &error_code, std::size_t) {
            read_result = error_code;
            ++completion_count;
            if (shutdown_cut_executed) {
              pair.client_.reset();
            }
          }));

  std::optional<asio::steady_timer> shutdown_barrier;
  asio::post(pair.client_context_, [&] {
    // Match SpotTransport's network-domain sequencing: the stop request first
    // reaches the io_context and then arms an immediate barrier, forcing a
    // reactor turn before cancellation is emitted.
    shutdown_barrier.emplace(pair.client_context_);
    shutdown_barrier->expires_at(std::chrono::steady_clock::now());
    shutdown_barrier->async_wait([&](const ErrorCode &error_code) {
      if (error_code) {
        return;
      }
      shutdown_cut_executed = true;
      if (completion_count == 0U) {
        cancellation.emit(asio::cancellation_type::terminal);
        ErrorCode ignored;
        pair.client_->next_layer().cancel(ignored);
        pair.client_->next_layer().close(ignored);
      } else {
        // AsyncWebSocket::cancel() observes done_ and skips emission before
        // SpotTransport releases its owning reference at this same cut.
        pair.client_.reset();
      }
    });
  });

  static_cast<void>(pair.client_context_.run());
  return shutdown_cut_executed && completion_count == 1U &&
         read_result == beast::error::timeout && !pair.client_ &&
         pair.client_context_.stopped();
}

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(5U);
  if (!price.has_value() || !quantity.has_value()) {
    std::abort();
  }
  return {*price, *quantity};
}

[[nodiscard]] bool websocket_policy_is_conservative() {
  return transport::detail::kWebSocketIdleTimeout == std::chrono::seconds{90} &&
         !transport::detail::kWebSocketKeepAlivePings;
}

[[nodiscard]] bool exact_spot_routes_are_product_bound() {
  const auto btc = transport::make_spot_transport_routes("BTCUSDT");
  const auto eth = transport::make_spot_transport_routes("ETHUSDT");
  if (!btc.has_value() || !eth.has_value()) {
    return false;
  }
  return btc->exchange_info_target == "/api/v3/exchangeInfo?symbol=BTCUSDT" &&
         eth->exchange_info_target == "/api/v3/exchangeInfo?symbol=ETHUSDT" &&
         btc->depth_target == "/api/v3/depth?symbol=BTCUSDT&limit=5000" &&
         eth->depth_target == "/api/v3/depth?symbol=ETHUSDT&limit=5000" &&
         btc->websocket_target == "/ws/btcusdt@depth@100ms" &&
         eth->websocket_target == "/ws/ethusdt@depth@100ms" &&
         btc->combined_websocket_target ==
             "/stream?streams=btcusdt@depth@100ms/btcusdt@aggTrade/"
             "btcusdt@bookTicker" &&
         eth->combined_websocket_target ==
             "/stream?streams=ethusdt@depth@100ms/ethusdt@aggTrade/"
             "ethusdt@bookTicker" &&
         btc->connection_id_prefix != eth->connection_id_prefix &&
         btc->snapshot_request_id != eth->snapshot_request_id &&
         !transport::make_spot_transport_routes("ethusdt").has_value();
}

[[nodiscard]] bool set_metadata_endpoint_is_full_exchange_info() {
  const auto endpoint = transport::detail::spot_exchange_info_set_endpoint();
  return endpoint.host == "api.binance.com" && endpoint.port == "443" &&
         endpoint.target == "/api/v3/exchangeInfo" &&
         endpoint.target.find("?symbol=") == std::string::npos;
}

[[nodiscard]] bool exchange_info_tls_stall_times_out() {
  asio::io_context server_context;
  tcp::acceptor acceptor{server_context, {tcp::v4(), 0U}};
  const auto port = acceptor.local_endpoint().port();
  std::thread server{[&acceptor, &server_context] {
    tcp::socket socket{server_context};
    boost::system::error_code error_code;
    acceptor.accept(socket, error_code);
    if (!error_code) {
      std::this_thread::sleep_for(std::chrono::milliseconds{250});
      socket.close(error_code);
    }
  }};

  const auto started_at = std::chrono::steady_clock::now();
  const auto result = transport::detail::fetch_exchange_info_https(
      {"localhost", std::to_string(port), "/exchangeInfo",
       std::chrono::milliseconds{50}});
  const auto elapsed = std::chrono::steady_clock::now() - started_at;
  server.join();

  const auto *failure = std::get_if<transport::NetworkError>(&result);
  return failure != nullptr &&
         failure->code == transport::NetworkErrorCode::Timeout &&
         failure->stage == "exchange-info-tls-handshake" &&
         elapsed < std::chrono::seconds{1};
}

[[nodiscard]] bool final_acceptance_rejects_failures() {
  transport::TransportObservation transport;
  transport.stopped = true;
  transport.tls_verified = true;
  transport.websocket_handshake = true;
  transport.rest_depth_fetched = true;
  transport.depth_frame_count = 1U;

  rt::RuntimeObservation runtime;
  runtime.state = rt::RuntimeState::Live;
  runtime.projection_status = core::ProjectionStatus::Synchronized;
  runtime.last_update_id = 1U;
  if (!transport::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }

  transport.stopped = false;
  if (transport::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  transport.stopped = true;

  transport.terminal_error = transport::NetworkError{
      transport::NetworkErrorCode::Timeout, "websocket-idle",
      "The socket was closed due to a timeout", std::nullopt, std::nullopt};
  if (transport::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  transport.terminal_error.reset();

  runtime.state = rt::RuntimeState::NeedsResync;
  if (transport::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  runtime.state = rt::RuntimeState::Faulted;
  runtime.fault_reason = rt::FaultReason::TransportFailure;
  if (transport::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  runtime.state = rt::RuntimeState::Live;
  runtime.fault_reason.reset();
  runtime.projection_status = core::ProjectionStatus::NeedsResync;
  return !transport::detail::live_acceptance_ready(transport, runtime);
}

[[nodiscard]] bool
clean_started_stop_has_no_false_failure(const rt::RuntimeClock &clock) {
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != rt::StartResult::Started) {
    return false;
  }
  bool cancelled = false;
  transport::detail::TransportTestOptions options;
  options.start_operation =
      [&cancelled](const transport::detail::TransportSeam &seam) {
        seam.mark_ready();
        return [&cancelled] { cancelled = true; };
      };
  transport::detail::BinanceTransport transport{
      runtime, clock, 1U,
      transport::detail::spot_transport_config("BTCUSDT", {}),
      std::move(options)};
  if (transport.start() != transport::TransportStartResult::Started) {
    transport.stop();
    runtime.stop();
    return false;
  }
  transport.stop();
  transport.stop();
  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  runtime.stop();
  return cancelled && observation.stopped && !observation.running &&
         !observation.terminal_error.has_value() &&
         !runtime_observation.fault_reason.has_value();
}

[[nodiscard]] bool
preexisting_failure_survives_stop_cut(const rt::RuntimeClock &clock) {
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != rt::StartResult::Started) {
    return false;
  }
  transport::detail::TransportTestOptions options;
  options.start_operation = [](const transport::detail::TransportSeam &seam) {
    seam.mark_ready();
    return [fail = seam.fail] {
      // Models a genuine read failure delivered on the network domain after
      // begin_clean_shutdown() has already established clean-cut state.
      fail(
          transport::NetworkError{
              transport::NetworkErrorCode::WebSocketRead,
              "test-preexisting-network-failure",
              "genuine failure was ready before the stop cut", std::nullopt,
              std::nullopt},
          false);
    };
  };
  transport::detail::BinanceTransport transport{
      runtime, clock, 1U,
      transport::detail::spot_transport_config("BTCUSDT", {}),
      std::move(options)};
  if (transport.start() != transport::TransportStartResult::Started) {
    transport.stop();
    runtime.stop();
    return false;
  }
  transport.stop();
  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  const bool rejected = !transport::detail::live_acceptance_ready(
      observation, runtime_observation);
  runtime.stop();

  return observation.stopped && !observation.running &&
         observation.terminal_error.has_value() &&
         observation.terminal_error->code ==
             transport::NetworkErrorCode::WebSocketRead &&
         observation.terminal_error->stage ==
             "test-preexisting-network-failure" &&
         runtime_observation.state == rt::RuntimeState::Faulted &&
         runtime_observation.fault_reason ==
             rt::FaultReason::TransportFailure &&
         rejected;
}

[[nodiscard]] bool
concurrent_stop_has_one_winner_and_waiter(const rt::RuntimeClock &clock) {
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != rt::StartResult::Started) {
    return false;
  }

  std::promise<void> winner_entered;
  auto winner_future = winner_entered.get_future();
  std::promise<void> waiter_entered;
  auto waiter_future = waiter_entered.get_future();
  std::promise<void> release_winner;
  auto release_future = release_winner.get_future().share();
  transport::detail::TransportTestOptions options;
  options.start_operation = [](const transport::detail::TransportSeam &seam) {
    seam.mark_ready();
    return std::function<void()>{};
  };
  options.stop_winner_gate = [&winner_entered, release_future] {
    winner_entered.set_value();
    release_future.wait();
  };
  options.stop_waiter_entered = [&waiter_entered] {
    waiter_entered.set_value();
  };

  transport::detail::BinanceTransport transport{
      runtime, clock, 1U,
      transport::detail::spot_transport_config("BTCUSDT", {}),
      std::move(options)};
  if (transport.start() != transport::TransportStartResult::Started) {
    transport.stop();
    runtime.stop();
    return false;
  }

  std::barrier callers_ready{3};
  std::atomic<unsigned> returned{0U};
  std::thread first{[&] {
    callers_ready.arrive_and_wait();
    transport.stop();
    ++returned;
  }};
  std::thread second{[&] {
    callers_ready.arrive_and_wait();
    transport.stop();
    ++returned;
  }};
  callers_ready.arrive_and_wait();
  winner_future.wait();
  waiter_future.wait();
  release_winner.set_value();
  first.join();
  second.join();

  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  runtime.stop();
  return returned.load() == 2U && observation.stopped && !observation.running &&
         !observation.terminal_error.has_value() &&
         !runtime_observation.fault_reason.has_value();
}

[[nodiscard]] bool
start_pending_is_woken_by_stop(const rt::RuntimeClock &clock) {
  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != rt::StartResult::Started) {
    return false;
  }

  std::promise<void> start_pending;
  auto pending_future = start_pending.get_future();
  transport::detail::TransportTestOptions options;
  options.start_operation =
      [&start_pending](const transport::detail::TransportSeam &) {
        start_pending.set_value();
        return std::function<void()>{};
      };
  transport::detail::BinanceTransport transport{
      runtime, clock, 1U,
      transport::detail::spot_transport_config("BTCUSDT", {}),
      std::move(options)};

  std::promise<transport::TransportStartResult> start_result;
  auto result_future = start_result.get_future();
  std::thread starter{[&] { start_result.set_value(transport.start()); }};
  pending_future.wait();
  std::thread stopper{[&] { transport.stop(); }};
  starter.join();
  stopper.join();

  const auto result = result_future.get();
  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  runtime.stop();
  return result == transport::TransportStartResult::Stopped &&
         observation.started && observation.stopped && !observation.running &&
         !observation.websocket_handshake &&
         !observation.terminal_error.has_value() &&
         !runtime_observation.fault_reason.has_value();
}

} // namespace

int main() {
  if (!websocket_policy_is_conservative()) {
    return EXIT_FAILURE;
  }
  if (!exact_spot_routes_are_product_bound()) {
    return EXIT_FAILURE;
  }
  if (!set_metadata_endpoint_is_full_exchange_info()) {
    return EXIT_FAILURE;
  }
  if (!exchange_info_tls_stall_times_out()) {
    return EXIT_FAILURE;
  }
  if (!final_acceptance_rejects_failures()) {
    return EXIT_FAILURE;
  }
  if (!outstanding_websocket_read_is_explicitly_cancelled()) {
    return EXIT_FAILURE;
  }
  if (!idle_timeout_ready_before_stop_cut_is_preserved()) {
    return EXIT_FAILURE;
  }

  rt::RuntimeClock clock = [] {
    return rt::ClockSample{1700000000123456000ULL, 9000000000999ULL};
  };
  if (!clean_started_stop_has_no_false_failure(clock)) {
    return EXIT_FAILURE;
  }
  if (!preexisting_failure_survives_stop_cut(clock)) {
    return EXIT_FAILURE;
  }
  if (!concurrent_stop_has_one_winner_and_waiter(clock)) {
    return EXIT_FAILURE;
  }
  if (!start_pending_is_woken_by_stop(clock)) {
    return EXIT_FAILURE;
  }

  rt::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  transport::SpotTransport transport{runtime, clock};
  transport::SpotTransport next_transport{runtime, clock, 2U};
  transport::SpotTransportOptions combined_options;
  combined_options.profile = transport::SpotTransportProfile::G9CombinedEvents;
  combined_options.normalized_event_sink =
      [](std::shared_ptr<const protocol::NormalizedSpotEvent>, std::uint64_t
#if defined(BMDG_PERFORMANCE_BASELINE_ENABLED)
         ,
         binance_market_data::gateway::performance::TraceToken
#endif
      ) { return transport::NormalizedEventSinkResult::Continue; };
  transport::SpotTransport combined_transport{runtime, clock, 3U,
                                              std::move(combined_options)};

  const auto before = transport.observe();
  if (before.started || before.running || before.stopped ||
      before.connection_generation != 1U || before.connection_id.empty()) {
    return EXIT_FAILURE;
  }
  const auto next_before = next_transport.observe();
  if (next_before.connection_generation != 2U ||
      next_before.connection_id.empty() ||
      next_before.connection_id == before.connection_id) {
    return EXIT_FAILURE;
  }
  const auto combined_before = combined_transport.observe();
  if (before.profile != transport::SpotTransportProfile::DepthOnly ||
      next_before.profile != transport::SpotTransportProfile::DepthOnly ||
      combined_before.profile !=
          transport::SpotTransportProfile::G9CombinedEvents ||
      combined_before.connection_generation != 3U) {
    return EXIT_FAILURE;
  }
  transport.stop();
  next_transport.stop();
  combined_transport.stop();
  transport.stop();
  const auto after = transport.observe();
  if (!after.stopped || after.running || after.started ||
      after.connection_generation != 1U ||
      after.connection_id != before.connection_id ||
      after.terminal_error.has_value()) {
    return EXIT_FAILURE;
  }
  runtime.stop();

  // Real transport's budget timer must drain on stop without DNS/TCP/HTTP.
  // All destinations are loopback even if this admission assertion regresses.
  transport::detail::shared_public_api_budget().reject(false, 418U, "86400");
  rt::MarketRuntime budget_runtime{{4U, 4U}, clock, numeric_spec()};
  transport::detail::BinanceTransportConfig config;
  config.rest_host = config.websocket_host = "127.0.0.1";
  config.rest_port = config.websocket_port = "1";
  config.depth_target = "/api/v3/depth?limit=5000";
  config.websocket_target = "/ws/btcusdt@depth";
  config.connection_id_prefix = "budget-test-";
  config.snapshot_request_id = "budget-test-snapshot";
  config.user_agent = "budget-offline-test";
  config.depth_frame_parser = [](auto payload, auto at, auto id) {
    return protocol::parse_depth_frame(payload, at, id);
  };
  config.depth_snapshot_parser = [](auto payload, auto at, auto id) {
    return protocol::parse_depth_snapshot(payload, at, id);
  };
  transport::detail::BinanceTransport waiting{budget_runtime, clock, 1U,
                                              std::move(config)};
  const auto before_wait = std::chrono::steady_clock::now();
  auto start = std::async(std::launch::async, [&] { return waiting.start(); });
  if (start.wait_for(std::chrono::milliseconds{30}) !=
      std::future_status::timeout) {
    return EXIT_FAILURE;
  }
  waiting.stop();
  if (start.get() != transport::TransportStartResult::Stopped ||
      waiting.observe().terminal_error.has_value() ||
      std::chrono::steady_clock::now() - before_wait >
          std::chrono::seconds{1}) {
    return EXIT_FAILURE;
  }
  const auto metadata = transport::detail::fetch_exchange_info_https(
      {"127.0.0.1", "1", "/api/v3/exchangeInfo",
       std::chrono::milliseconds{20}});
  const auto *blocked = std::get_if<transport::NetworkError>(&metadata);
  if (blocked == nullptr ||
      blocked->code != transport::NetworkErrorCode::RuntimeAdmission ||
      std::chrono::steady_clock::now() - before_wait >
          std::chrono::seconds{1}) {
    return EXIT_FAILURE;
  }
  std::cout << "BUDGET_WAIT_STOP_AND_METADATA_BOUND=PASS\n";
  std::cout << "EXCHANGE_INFO_ASYNC_TIMEOUT=PASS\n"
               "SPOT_SET_METADATA_FULL_EXCHANGE_INFO=PASS\n"
               "WEBSOCKET_IDLE_POLICY=PASS\n"
               "FINAL_ACCEPTANCE_REJECTION=PASS\n"
               "OUTSTANDING_WEBSOCKET_READ_CANCEL=PASS\n"
               "IDLE_TIMEOUT_STOP_BOUNDARY=PASS\n"
               "IO_CONTEXT_NATURAL_DRAIN=PASS\n"
               "CLEAN_STARTED_STOP_NO_FALSE_FAILURE=PASS\n"
               "PREEXISTING_FAILURE_SURVIVES_STOP_CUT=PASS\n"
               "CONCURRENT_STOP_WINNER_WAITER=PASS\n"
               "START_PENDING_UNTIL_STOP=PASS\n"
               "EXPLICIT_GENERATION_IDENTITY=PASS\n"
               "DEPTH_ONLY_AND_G9_COMBINED_PROFILES=PASS\n"
               "CLEAN_TRANSPORT_STOP_CORE=PASS\n";
  return EXIT_SUCCESS;
}
