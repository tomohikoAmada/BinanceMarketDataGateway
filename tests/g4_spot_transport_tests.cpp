#include "spot_transport.hpp"

#include <boost/asio.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <variant>

namespace {

namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;

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
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

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

  g3::RuntimeClock clock = [] {
    return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL};
  };
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  g4::SpotTransport transport{runtime, clock};

  const auto before = transport.observe();
  if (before.started || before.running || before.stopped ||
      before.connection_generation != 1U || before.connection_id.empty()) {
    return EXIT_FAILURE;
  }
  transport.stop();
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
               "CLEAN_TRANSPORT_STOP_CORE=PASS\n";
  return EXIT_SUCCESS;
}
