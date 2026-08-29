#include "planned_rotation.hpp"
#include "spot_protocol.hpp"

#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string_view>
#include <thread>
#include <variant>

namespace {

namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;

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

[[nodiscard]] bool wait_for_later_real_update(g3::MarketRuntime &runtime,
                                              std::uint64_t initial) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{10};
  while (std::chrono::steady_clock::now() < deadline) {
    const auto observation = runtime.observe();
    if (observation.state != g3::RuntimeState::Live) {
      return false;
    }
    if (observation.last_update_id.has_value() &&
        *observation.last_update_id > initial) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{25});
  }
  return false;
}

[[nodiscard]] bool valid_owner_snapshot(const g3::SnapshotResult &result) {
  const auto *captured = std::get_if<g3::CapturedSnapshot>(&result);
  return captured != nullptr && captured->snapshot.synchronized() &&
         captured->snapshot.last_update_id() != 0U &&
         captured->snapshot.symbol() == "BTCUSDT" &&
         !captured->snapshot.bids().empty() &&
         !captured->snapshot.asks().empty() &&
         captured->captured_on_thread != std::this_thread::get_id();
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

  g3::RuntimeClock clock = g4::sample_real_clock;
  g3::MarketRuntime runtime{{256U, 256U}, clock, spot.numeric_spec};
  const g5::PlannedRotationPolicy acceptance_policy{std::chrono::seconds{5}};
  g5::SpotRecovery recovery{runtime, clock, acceptance_policy};
  if (recovery.start() != g5::RecoveryStartResult::Started) {
    std::cerr << "G6_ROTATION_START=FAIL\n";
    return EXIT_FAILURE;
  }

  const auto initial = recovery.wait_for_generation_live(1U);
  if (initial.state != g5::RecoveryState::Live || initial.terminal ||
      initial.connection_generation != 1U) {
    if (initial.terminal_error.has_value()) {
      print_network_error(*initial.terminal_error);
    }
    recovery.stop();
    return EXIT_FAILURE;
  }
  auto runtime_observation = runtime.observe();
  if (!runtime_observation.last_update_id.has_value()) {
    recovery.stop();
    return EXIT_FAILURE;
  }
  const auto initial_snapshot = runtime.capture_snapshot();
  const bool initial_post_live =
      wait_for_later_real_update(runtime, *runtime_observation.last_update_id);
  if (!valid_owner_snapshot(initial_snapshot) || !initial_post_live) {
    std::cerr << "REAL_INITIAL_GENERATION=FAIL\n";
    recovery.stop();
    return EXIT_FAILURE;
  }

  const auto rotated = recovery.wait_for_generation_live(2U);
  if (rotated.state != g5::RecoveryState::Live || rotated.terminal ||
      rotated.connection_generation != 2U ||
      rotated.connection_id == initial.connection_id ||
      rotated.planned_rotation_count < 1U ||
      rotated.consecutive_recovery_attempts != 0U ||
      !rotated.last_planned_rotation_cut.has_value()) {
    if (rotated.terminal_error.has_value()) {
      print_network_error(*rotated.terminal_error);
    }
    recovery.stop();
    return EXIT_FAILURE;
  }
  const auto &rotation_cut = *rotated.last_planned_rotation_cut;
  const bool old_generation_clean =
      rotation_cut.generation == 1U && rotation_cut.transport.stopped &&
      !rotation_cut.transport.running &&
      !rotation_cut.transport.terminal_error.has_value() &&
      rotation_cut.runtime.state == g3::RuntimeState::Live &&
      rotation_cut.runtime.projection_status ==
          core::ProjectionStatus::Synchronized &&
      !rotation_cut.runtime.fault_reason.has_value();
  if (!old_generation_clean) {
    std::cerr << "REAL_OLD_GENERATION_CUT=FAIL\n";
    recovery.stop();
    return EXIT_FAILURE;
  }

  runtime_observation = runtime.observe();
  if (!runtime_observation.last_update_id.has_value()) {
    recovery.stop();
    return EXIT_FAILURE;
  }
  const auto rotated_at = *runtime_observation.last_update_id;
  const bool rotated_post_live =
      wait_for_later_real_update(runtime, rotated_at);
  const auto final_cut =
      rotated_post_live ? recovery.quiesce_for_acceptance() : std::nullopt;
  if (!final_cut.has_value()) {
    std::cerr << "REAL_PLANNED_ROTATION=FAIL\n";
    recovery.stop();
    return EXIT_FAILURE;
  }

  const auto &final_transport = final_cut->transport;
  const auto &final_runtime = final_cut->runtime;
  const bool final_transport_clean =
      final_transport.stopped && !final_transport.running &&
      !final_transport.terminal_error.has_value() &&
      final_transport.connection_generation == 2U &&
      final_transport.connection_id == rotated.connection_id &&
      final_transport.tls_verified && final_transport.websocket_handshake &&
      final_transport.rest_depth_fetched &&
      final_transport.depth_frame_count > 0U;
  const bool final_runtime_live =
      final_runtime.state == g3::RuntimeState::Live &&
      final_runtime.projection_status == core::ProjectionStatus::Synchronized &&
      !final_runtime.fault_reason.has_value() &&
      final_runtime.last_update_id.has_value();
  if (!final_transport_clean || !final_runtime_live) {
    std::cerr << "REAL_PLANNED_ROTATION=FAIL\n";
    if (final_transport.terminal_error.has_value()) {
      print_network_error(*final_transport.terminal_error);
    }
    recovery.stop();
    return EXIT_FAILURE;
  }

  const auto rotated_snapshot = runtime.capture_snapshot();
  const bool rotated_snapshot_valid = valid_owner_snapshot(rotated_snapshot);
  recovery.stop();
  if (!rotated_snapshot_valid) {
    std::cerr << "REAL_PLANNED_ROTATION=FAIL\n";
    return EXIT_FAILURE;
  }

  std::cout << "EXCHANGE_INFO_FETCH=PASS\n"
            << "TLS_VERIFY="
            << (exchange_response.tls_verified ? "PASS" : "FAIL") << '\n'
            << "REAL_INITIAL_GENERATION=1\n"
            << "REAL_INITIAL_LIVE=PASS\n"
            << "REAL_INITIAL_CONNECTION_ID=" << initial.connection_id << '\n'
            << "REAL_INITIAL_POST_LIVE_UPDATE=PASS\n"
            << "REAL_INITIAL_OWNER_SNAPSHOT=PASS\n"
            << "REAL_PLANNED_ROTATION_TRIGGERED=YES\n"
            << "REAL_ROTATION_CAUSE=PlannedRotation\n"
            << "REAL_OLD_GENERATION_STOPPED=YES\n"
            << "REAL_OLD_GENERATION_TERMINAL_ERROR=NO\n"
            << "REAL_PRE_RESET_RUNTIME_STATE="
            << runtime_state(rotation_cut.runtime.state) << '\n'
            << "REAL_PRE_RESET_PROJECTION_STATUS="
            << projection_status(rotation_cut.runtime.projection_status) << '\n'
            << "REAL_PRE_RESET_FAULT=NONE\n"
            << "REAL_ROTATED_GENERATION=2\n"
            << "REAL_ROTATED_CONNECTION_ID=" << rotated.connection_id << '\n'
            << "REAL_CONNECTION_ID_CHANGED=YES\n"
            << "REAL_ROTATED_BOOTSTRAP=PASS\n"
            << "REAL_ROTATED_LIVE=PASS\n"
            << "REAL_POST_ROTATION_UPDATE=PASS\n"
            << "REAL_POST_ROTATION_OWNER_SNAPSHOT=PASS\n"
            << "REAL_PLANNED_ROTATION_COUNT=" << rotated.planned_rotation_count
            << '\n'
            << "REAL_PLANNED_ROTATION_RECOVERY_ATTEMPTS="
            << rotated.consecutive_recovery_attempts << '\n'
            << "FINAL_ACCEPTANCE_TRANSPORT_STOPPED=YES\n"
            << "FINAL_ACCEPTANCE_TERMINAL_ERROR=NO\n"
            << "FINAL_ACCEPTANCE_RUNTIME_STATE="
            << runtime_state(final_runtime.state) << '\n'
            << "FINAL_ACCEPTANCE_PROJECTION_STATUS="
            << projection_status(final_runtime.projection_status) << '\n'
            << "FINAL_ACCEPTANCE_FAULT=NONE\n"
            << "FINAL_ACCEPTANCE_CREATED_LATER_GENERATION=NO\n"
            << "MAX_ACTIVE_TRANSPORTS=" << rotated.max_active_transport_count
            << '\n'
            << "REAL_RATE_LIMIT_ABUSE_ATTEMPTED=NO\n";
  return EXIT_SUCCESS;
}
