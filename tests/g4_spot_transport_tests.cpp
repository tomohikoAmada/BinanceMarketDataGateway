#include "spot_transport.hpp"

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

namespace {

namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
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
  set_idle_timeout(*pair.client_, g4::detail::kWebSocketIdleTimeout);

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
  return g4::detail::kWebSocketIdleTimeout == std::chrono::seconds{90} &&
         !g4::detail::kWebSocketKeepAlivePings;
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
  const auto result = g4::detail::fetch_exchange_info_https(
      {"localhost", std::to_string(port), "/exchangeInfo",
       std::chrono::milliseconds{50}});
  const auto elapsed = std::chrono::steady_clock::now() - started_at;
  server.join();

  const auto *failure = std::get_if<g4::NetworkError>(&result);
  return failure != nullptr && failure->code == g4::NetworkErrorCode::Timeout &&
         failure->stage == "exchange-info-tls-handshake" &&
         elapsed < std::chrono::seconds{1};
}

[[nodiscard]] bool final_acceptance_rejects_failures() {
  g4::TransportObservation transport;
  transport.stopped = true;
  transport.tls_verified = true;
  transport.websocket_handshake = true;
  transport.rest_depth_fetched = true;
  transport.depth_frame_count = 1U;

  g3::RuntimeObservation runtime;
  runtime.state = g3::RuntimeState::Live;
  runtime.projection_status = core::ProjectionStatus::Synchronized;
  runtime.last_update_id = 1U;
  if (!g4::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }

  transport.stopped = false;
  if (g4::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  transport.stopped = true;

  transport.terminal_error = g4::NetworkError{
      g4::NetworkErrorCode::Timeout, "websocket-idle",
      "The socket was closed due to a timeout", std::nullopt, std::nullopt};
  if (g4::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  transport.terminal_error.reset();

  runtime.state = g3::RuntimeState::NeedsResync;
  if (g4::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  runtime.state = g3::RuntimeState::Faulted;
  runtime.fault_reason = g3::FaultReason::TransportFailure;
  if (g4::detail::live_acceptance_ready(transport, runtime)) {
    return false;
  }
  runtime.state = g3::RuntimeState::Live;
  runtime.fault_reason.reset();
  runtime.projection_status = core::ProjectionStatus::NeedsResync;
  return !g4::detail::live_acceptance_ready(transport, runtime);
}

[[nodiscard]] bool
clean_started_stop_has_no_false_failure(const g3::RuntimeClock &clock) {
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != g3::StartResult::Started) {
    return false;
  }
  g4::detail::TransportTestOptions options;
  options.stop_cut_mode =
      g4::detail::TransportStopCutTestMode::CleanCancellation;
  g4::SpotTransport transport{runtime, clock, std::move(options)};
  if (transport.start() != g4::TransportStartResult::Started) {
    transport.stop();
    runtime.stop();
    return false;
  }
  transport.stop();
  transport.stop();
  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  runtime.stop();
  return observation.stopped && !observation.running &&
         !observation.terminal_error.has_value() &&
         !runtime_observation.fault_reason.has_value();
}

[[nodiscard]] bool
preexisting_failure_survives_stop_cut(const g3::RuntimeClock &clock) {
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != g3::StartResult::Started) {
    return false;
  }
  g4::detail::TransportTestOptions options;
  options.stop_cut_mode =
      g4::detail::TransportStopCutTestMode::PreexistingFailure;
  g4::SpotTransport transport{runtime, clock, std::move(options)};
  if (transport.start() != g4::TransportStartResult::Started) {
    transport.stop();
    runtime.stop();
    return false;
  }
  transport.stop();
  const auto observation = transport.observe();
  const auto runtime_observation = runtime.observe();
  const bool rejected =
      !g4::detail::live_acceptance_ready(observation, runtime_observation);
  runtime.stop();

  return observation.stopped && !observation.running &&
         observation.terminal_error.has_value() &&
         observation.terminal_error->code ==
             g4::NetworkErrorCode::WebSocketRead &&
         observation.terminal_error->stage ==
             "test-preexisting-network-failure" &&
         runtime_observation.state == g3::RuntimeState::Faulted &&
         runtime_observation.fault_reason ==
             g3::FaultReason::TransportFailure &&
         rejected;
}

[[nodiscard]] bool
concurrent_stop_has_one_winner_and_waiter(const g3::RuntimeClock &clock) {
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != g3::StartResult::Started) {
    return false;
  }

  std::promise<void> winner_entered;
  auto winner_future = winner_entered.get_future();
  std::promise<void> waiter_entered;
  auto waiter_future = waiter_entered.get_future();
  std::promise<void> release_winner;
  auto release_future = release_winner.get_future().share();
  g4::detail::TransportTestOptions options;
  options.stop_cut_mode =
      g4::detail::TransportStopCutTestMode::CleanCancellation;
  options.stop_winner_gate = [&winner_entered, release_future] {
    winner_entered.set_value();
    release_future.wait();
  };
  options.stop_waiter_entered = [&waiter_entered] {
    waiter_entered.set_value();
  };

  g4::SpotTransport transport{runtime, clock, std::move(options)};
  if (transport.start() != g4::TransportStartResult::Started) {
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
start_pending_is_woken_by_stop(const g3::RuntimeClock &clock) {
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  if (runtime.start() != g3::StartResult::Started) {
    return false;
  }

  std::promise<void> start_pending;
  auto pending_future = start_pending.get_future();
  g4::detail::TransportTestOptions options;
  options.stop_cut_mode =
      g4::detail::TransportStopCutTestMode::StartPendingUntilStop;
  options.start_pending = [&start_pending] { start_pending.set_value(); };
  g4::SpotTransport transport{runtime, clock, std::move(options)};

  std::promise<g4::TransportStartResult> start_result;
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
  return result == g4::TransportStartResult::Stopped && observation.started &&
         observation.stopped && !observation.running &&
         !observation.websocket_handshake &&
         !observation.terminal_error.has_value() &&
         !runtime_observation.fault_reason.has_value();
}

} // namespace

int main() {
  if (!websocket_policy_is_conservative()) {
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

  g3::RuntimeClock clock = [] {
    return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL};
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

  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  g4::SpotTransport transport{runtime, clock};
  g4::SpotTransport next_transport{runtime, clock, 2U};
  g4::SpotTransportOptions combined_options;
  combined_options.profile = g4::SpotTransportProfile::G9CombinedEvents;
  combined_options.normalized_event_sink =
      [](std::shared_ptr<const g4::NormalizedSpotEvent>, std::uint64_t) {
        return g4::NormalizedEventSinkResult::Continue;
      };
  g4::SpotTransport combined_transport{runtime, clock, 3U,
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
  if (before.profile != g4::SpotTransportProfile::DepthOnly ||
      next_before.profile != g4::SpotTransportProfile::DepthOnly ||
      combined_before.profile != g4::SpotTransportProfile::G9CombinedEvents ||
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
  std::cout << "EXCHANGE_INFO_ASYNC_TIMEOUT=PASS\n"
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
