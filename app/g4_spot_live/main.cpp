#include "spot_protocol.hpp"
#include "spot_transport.hpp"

#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>
#include <variant>

namespace {

namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;

[[nodiscard]] std::string_view
projection_status(core::ProjectionStatus status) {
  switch (status) {
  case core::ProjectionStatus::AwaitingBaseline:
    return "AwaitingBaseline";
  case core::ProjectionStatus::AwaitingBridge:
    return "AwaitingBridge";
  case core::ProjectionStatus::Synchronized:
    return "Synchronized";
  case core::ProjectionStatus::NeedsResync:
    return "NeedsResync";
  }
  return "Unknown";
}

[[nodiscard]] std::string_view runtime_state(g3::RuntimeState state) {
  switch (state) {
  case g3::RuntimeState::Constructed:
    return "Constructed";
  case g3::RuntimeState::Buffering:
    return "Buffering";
  case g3::RuntimeState::AwaitingBridge:
    return "AwaitingBridge";
  case g3::RuntimeState::Live:
    return "Live";
  case g3::RuntimeState::NeedsResync:
    return "NeedsResync";
  case g3::RuntimeState::Faulted:
    return "Faulted";
  case g3::RuntimeState::Stopping:
    return "Stopping";
  case g3::RuntimeState::Stopped:
    return "Stopped";
  }
  return "Unknown";
}

void print_network_error(const g4::NetworkError &error) {
  std::cerr << "NETWORK_ERROR stage=" << error.stage
            << " message=" << error.message;
  if (error.http_status.has_value()) {
    std::cerr << " http_status=" << *error.http_status;
  }
  if (error.retry_after.has_value()) {
    std::cerr << " retry_after=" << *error.retry_after;
  }
  std::cerr << '\n';
}

} // namespace

int main() {
  const auto exchange_info = g4::fetch_exchange_info_https();
  if (const auto *failure = std::get_if<g4::NetworkError>(&exchange_info)) {
    print_network_error(*failure);
    return EXIT_FAILURE;
  }
  const auto &exchange_response =
      std::get<g4::ExchangeInfoResponse>(exchange_info);
  const auto metadata = g4::parse_exchange_info(exchange_response.body);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&metadata)) {
    std::cerr << "EXCHANGE_INFO_PARSE=FAIL field=" << failure->field
              << " message=" << failure->message << '\n';
    return EXIT_FAILURE;
  }
  const auto &spot = std::get<g4::SpotMetadata>(metadata);
  std::cout << "EXCHANGE_INFO_FETCH=PASS\n"
               "BTCUSDT_EXISTS=YES\n"
               "BTCUSDT_STATUS=TRADING\n"
               "BTCUSDT_SPOT_ALLOWED=YES\n"
            << "PRICE_FILTER_TICK_SIZE=" << spot.tick_size << '\n'
            << "LOT_SIZE_STEP_SIZE=" << spot.step_size << '\n'
            << "DERIVED_PRICE_SCALE="
            << static_cast<unsigned>(spot.numeric_spec.price_scale.value())
            << '\n'
            << "DERIVED_QUANTITY_SCALE="
            << static_cast<unsigned>(spot.numeric_spec.quantity_scale.value())
            << '\n';

  g3::RuntimeClock clock = g4::sample_real_clock;
  g3::MarketRuntime runtime{{256U, 256U}, clock, spot.numeric_spec};
  if (runtime.start() != g3::StartResult::Started) {
    std::cerr << "MARKET_RUNTIME_START=FAIL\n";
    return EXIT_FAILURE;
  }
  g4::SpotTransport transport{runtime, clock};
  if (transport.start() != g4::TransportStartResult::Started) {
    const auto observation = transport.observe();
    if (observation.terminal_error.has_value()) {
      print_network_error(*observation.terminal_error);
    }
    transport.stop();
    runtime.stop();
    return EXIT_FAILURE;
  }

  constexpr auto kBootstrapTimeout = std::chrono::seconds{20};
  const auto bootstrap_deadline =
      std::chrono::steady_clock::now() + kBootstrapTimeout;
  g3::RuntimeObservation runtime_observation;
  bool live = false;
  while (std::chrono::steady_clock::now() < bootstrap_deadline) {
    runtime_observation = runtime.observe();
    const auto transport_observation = transport.observe();
    if (transport_observation.terminal_error.has_value() ||
        runtime_observation.state == g3::RuntimeState::Faulted ||
        runtime_observation.state == g3::RuntimeState::NeedsResync) {
      break;
    }
    if (runtime_observation.state == g3::RuntimeState::Live) {
      live = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{25});
  }
  if (!live || !runtime_observation.last_update_id.has_value()) {
    const auto transport_observation = transport.observe();
    if (transport_observation.terminal_error.has_value()) {
      print_network_error(*transport_observation.terminal_error);
    }
    std::cerr << "REAL_BOOTSTRAP=FAIL projection_status="
              << projection_status(runtime_observation.projection_status)
              << " runtime_state=" << runtime_state(runtime_observation.state)
              << '\n';
    transport.stop();
    runtime.stop();
    return EXIT_FAILURE;
  }

  const auto bootstrap_last_update_id = *runtime_observation.last_update_id;
  bool post_live_update = false;
  const auto post_live_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{10};
  while (std::chrono::steady_clock::now() < post_live_deadline) {
    runtime_observation = runtime.observe();
    const auto transport_observation = transport.observe();
    if (transport_observation.terminal_error.has_value() ||
        runtime_observation.state != g3::RuntimeState::Live) {
      break;
    }
    if (runtime_observation.last_update_id.has_value() &&
        *runtime_observation.last_update_id > bootstrap_last_update_id) {
      post_live_update = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{25});
  }
  if (!post_live_update) {
    std::cerr << "POST_LIVE_REAL_UPDATE=FAIL\n";
    transport.stop();
    runtime.stop();
    return EXIT_FAILURE;
  }

  const auto ping_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{25};
  while (std::chrono::steady_clock::now() < ping_deadline) {
    const auto observation = transport.observe();
    if (observation.server_ping_observed ||
        observation.terminal_error.has_value()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
  }

  const auto captured = runtime.capture_snapshot();
  if (!std::holds_alternative<g3::CapturedSnapshot>(captured)) {
    std::cerr << "REAL_OWNER_SNAPSHOT_CAPTURE=FAIL\n";
    transport.stop();
    runtime.stop();
    return EXIT_FAILURE;
  }
  const auto &snapshot = std::get<g3::CapturedSnapshot>(captured).snapshot;
  if (!snapshot.synchronized() || snapshot.last_update_id() == 0U ||
      snapshot.symbol() != "BTCUSDT" || snapshot.bids().empty() ||
      snapshot.asks().empty()) {
    std::cerr << "REAL_OWNER_SNAPSHOT_CAPTURE=FAIL invalid_snapshot\n";
    transport.stop();
    runtime.stop();
    return EXIT_FAILURE;
  }

  runtime_observation = runtime.observe();
  const auto final_transport = transport.observe();
  std::cout << "TLS_VERIFY="
            << (exchange_response.tls_verified && final_transport.tls_verified
                    ? "PASS"
                    : "FAIL")
            << '\n'
            << "WS_HANDSHAKE="
            << (final_transport.websocket_handshake ? "PASS" : "FAIL") << '\n'
            << "WS_STREAM=btcusdt@depth@100ms\n"
            << "REAL_DEPTH_FRAMES_RECEIVED="
            << final_transport.depth_frame_count << '\n'
            << "CONNECTION_GENERATION=" << final_transport.connection_generation
            << '\n'
            << "CONNECTION_ID=" << final_transport.connection_id << '\n'
            << "REST_DEPTH_FETCH="
            << (final_transport.rest_depth_fetched ? "PASS" : "FAIL") << '\n'
            << "REST_DEPTH_LIMIT=5000\n"
            << "REAL_BOOTSTRAP=PASS\n"
            << "FINAL_PROJECTION_STATUS="
            << projection_status(runtime_observation.projection_status) << '\n'
            << "FINAL_RUNTIME_STATE="
            << runtime_state(runtime_observation.state) << '\n'
            << "POST_LIVE_REAL_UPDATE=PASS\n"
            << "REAL_OWNER_SNAPSHOT_CAPTURE=PASS\n"
            << "OWNER_SNAPSHOT_LAST_UPDATE_ID=" << snapshot.last_update_id()
            << '\n'
            << "SERVER_PING_OBSERVED="
            << (final_transport.server_ping_observed ? "YES" : "NO") << '\n';

  transport.stop();
  runtime.stop();
  return EXIT_SUCCESS;
}
