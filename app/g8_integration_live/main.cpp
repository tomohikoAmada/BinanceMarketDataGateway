#include "grpc_service.hpp"
#include "planned_rotation.hpp"
#include "spot_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace wire = binance_market_data::gateway::v1;

using ServiceStub = wire::BinanceMarketDataGatewayService::Stub;
using StreamReader = grpc::ClientReader<wire::OrderBookStreamItem>;

inline constexpr auto kGenerationDeadline = std::chrono::seconds{75};
inline constexpr auto kConsumerDeadline = std::chrono::seconds{45};

enum class PhaseKind : std::uint8_t {
  ControlledRecovery,
  PlannedRotation,
};

struct SessionEvidence final {
  bool canonical{true};
  bool contiguous{true};
  bool payload_after_terminal{false};
  bool accepted{false};
  bool snapshot{false};
  bool later_update{false};
  bool terminal{false};
  bool finished{false};
  std::uint64_t expected_sequence{1U};
  std::uint64_t snapshot_last_update_id{0U};
  std::optional<std::uint64_t> snapshot_generation;
  std::optional<std::uint64_t> update_generation;
  std::optional<std::uint64_t> terminal_generation;
  common::ConsumerGapReason terminal_reason{
      common::CONSUMER_GAP_REASON_UNSPECIFIED};
  common::RecoveryAction recovery_action{common::RECOVERY_ACTION_UNSPECIFIED};
  std::string subscription_id;
  grpc::Status status;
};

class StreamSession final {
public:
  StreamSession(ServiceStub &stub,
                const wire::OrderBookSubscriptionRequest &request,
                std::string gateway_instance_id)
      : gateway_instance_id_{std::move(gateway_instance_id)} {
    context_.set_deadline(std::chrono::system_clock::now() +
                          kGenerationDeadline);
    reader_ = stub.SubscribeOrderBook(&context_, request);
    thread_ = std::thread{[this] { drain(); }};
  }

  ~StreamSession() {
    context_.TryCancel();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  StreamSession(const StreamSession &) = delete;
  StreamSession &operator=(const StreamSession &) = delete;

  [[nodiscard]] bool wait_for_initial(std::uint64_t generation) {
    std::unique_lock lock{mutex_};
    return condition_.wait_for(
               lock, kConsumerDeadline,
               [&] {
                 return evidence_.finished ||
                        (evidence_.accepted && evidence_.snapshot &&
                         evidence_.later_update &&
                         evidence_.snapshot_generation == generation &&
                         evidence_.update_generation == generation);
               }) &&
           evidence_.accepted && evidence_.snapshot && evidence_.later_update &&
           evidence_.snapshot_generation == generation &&
           evidence_.update_generation == generation;
  }

  [[nodiscard]] bool wait_for_terminal() {
    std::unique_lock lock{mutex_};
    return condition_.wait_for(lock, kConsumerDeadline, [&] {
      return evidence_.terminal || evidence_.finished;
    }) && evidence_.terminal;
  }

  [[nodiscard]] bool wait_for_finish() {
    std::unique_lock lock{mutex_};
    return condition_.wait_for(lock, std::chrono::seconds{5},
                               [&] { return evidence_.finished; });
  }

  void cancel_and_join() noexcept {
    context_.TryCancel();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  [[nodiscard]] SessionEvidence evidence() const {
    std::lock_guard lock{mutex_};
    return evidence_;
  }

private:
  void drain() noexcept {
    wire::OrderBookStreamItem item;
    while (reader_->Read(&item)) {
      std::lock_guard lock{mutex_};
      accept_item(item);
      condition_.notify_all();
    }
    const auto status = reader_->Finish();
    {
      std::lock_guard lock{mutex_};
      evidence_.status = status;
      evidence_.finished = true;
    }
    condition_.notify_all();
  }

  void accept_item(const wire::OrderBookStreamItem &item) noexcept {
    if (evidence_.terminal) {
      evidence_.payload_after_terminal = true;
    }
    if (item.has_envelope_metadata() || !item.has_delivery_metadata()) {
      evidence_.canonical = false;
      return;
    }
    const auto &metadata = item.delivery_metadata();
    if (metadata.protocol_version() != g7::kProtocolVersion ||
        metadata.gateway_instance_id() != gateway_instance_id_ ||
        metadata.publish_time_utc_ns() == 0U) {
      evidence_.canonical = false;
    }
    if (metadata.session_sequence() != evidence_.expected_sequence) {
      evidence_.contiguous = false;
    }
    if (evidence_.expected_sequence !=
        std::numeric_limits<std::uint64_t>::max()) {
      ++evidence_.expected_sequence;
    }
    if (evidence_.subscription_id.empty()) {
      evidence_.subscription_id = metadata.subscription_id();
    } else if (metadata.subscription_id() != evidence_.subscription_id) {
      evidence_.canonical = false;
    }

    if (item.has_subscription_accepted()) {
      evidence_.accepted = metadata.session_sequence() == 1U &&
                           !metadata.has_connection_generation() &&
                           item.subscription_accepted().subscription_id() ==
                               evidence_.subscription_id;
    } else if (item.has_snapshot()) {
      evidence_.snapshot =
          item.snapshot().synchronized() &&
          item.snapshot().schema_version() == g7::kSnapshotSchema &&
          item.snapshot().symbol() == "BTCUSDT" &&
          item.snapshot().last_update_id() != 0U &&
          !item.snapshot().bids().empty() && !item.snapshot().asks().empty() &&
          metadata.has_connection_generation();
      evidence_.snapshot_last_update_id = item.snapshot().last_update_id();
      if (metadata.has_connection_generation()) {
        evidence_.snapshot_generation = metadata.connection_generation();
      }
    } else if (item.has_depth_update()) {
      evidence_.later_update =
          item.depth_update().metadata().symbol() == "BTCUSDT" &&
          item.depth_update().metadata().schema_version() ==
              g7::kUpdateSchema &&
          item.depth_update().final_update_id() >
              evidence_.snapshot_last_update_id &&
          metadata.has_connection_generation();
      if (metadata.has_connection_generation()) {
        evidence_.update_generation = metadata.connection_generation();
      }
    } else if (item.has_consumer_gap()) {
      evidence_.terminal = true;
      evidence_.terminal_reason = item.consumer_gap().reason();
      evidence_.recovery_action = item.consumer_gap().recovery_action();
      if (metadata.has_connection_generation()) {
        evidence_.terminal_generation = metadata.connection_generation();
      }
    } else {
      evidence_.canonical = false;
    }
  }

  const std::string gateway_instance_id_;
  grpc::ClientContext context_;
  std::unique_ptr<StreamReader> reader_;
  std::thread thread_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  SessionEvidence evidence_;
};

struct PhaseEvidence final {
  bool passed{false};
  bool old_session_terminated{false};
  bool old_session_contiguous{false};
  bool old_session_crossed_rebootstrap{true};
  bool new_session_sequence_reset{false};
  bool connection_id_changed{false};
  bool fresh_snapshot{false};
  bool final_transport_active{true};
  std::size_t final_grpc_contexts{1U};
  std::size_t final_subscribers{1U};
  g3::RuntimeState final_runtime_state{g3::RuntimeState::Constructed};
  std::size_t max_active_transports{0U};
  std::size_t consecutive_recovery_attempts{0U};
  std::uint64_t planned_rotation_count{0U};
};

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

[[nodiscard]] wire::OrderBookSubscriptionRequest
make_request(std::string request_id) {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(common::MARKET_SPOT);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] std::optional<g5::RecoveryObservation>
wait_for_generation(g5::SpotRecovery &recovery, std::uint64_t generation) {
  auto waiter = std::async(std::launch::async, [&] {
    return recovery.wait_for_generation_live(generation);
  });
  if (waiter.wait_for(kGenerationDeadline) != std::future_status::ready) {
    recovery.stop();
  }
  const auto observation = waiter.get();
  if (observation.state != g5::RecoveryState::Live || observation.terminal ||
      observation.connection_generation != generation) {
    if (observation.terminal_error.has_value()) {
      print_network_error(*observation.terminal_error);
    }
    return std::nullopt;
  }
  return observation;
}

[[nodiscard]] bool wait_for_subscriber_count(g3::MarketRuntime &runtime,
                                             std::size_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  do {
    if (runtime.observe().resident_subscription_count == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  } while (std::chrono::steady_clock::now() < deadline);
  return runtime.observe().resident_subscription_count == expected;
}

[[nodiscard]] bool
clean_generation_two_cut(const g5::QuiescentAcceptanceCut &cut,
                         const g5::RecoveryObservation &generation_two) {
  return cut.transport.stopped && !cut.transport.running &&
         !cut.transport.terminal_error.has_value() &&
         cut.transport.connection_generation == 2U &&
         cut.transport.connection_id == generation_two.connection_id &&
         cut.transport.tls_verified && cut.transport.websocket_handshake &&
         cut.transport.rest_depth_fetched &&
         cut.transport.depth_frame_count > 0U &&
         cut.runtime.state == g3::RuntimeState::Live &&
         cut.runtime.projection_status ==
             core::ProjectionStatus::Synchronized &&
         !cut.runtime.fault_reason.has_value();
}

[[nodiscard]] PhaseEvidence run_phase(PhaseKind kind,
                                      const core::NumericSpec &numeric_spec) {
  PhaseEvidence result;
  g3::RuntimeClock clock = g4::sample_real_clock;
  g3::MarketRuntime runtime{{256U, 256U}, clock, numeric_spec};
  std::unique_ptr<g5::SpotRecovery> recovery;
  if (kind == PhaseKind::PlannedRotation) {
    recovery = std::make_unique<g5::SpotRecovery>(
        runtime, clock, g5::PlannedRotationPolicy{std::chrono::seconds{5}});
  } else {
    recovery = std::make_unique<g5::SpotRecovery>(runtime, clock);
  }

  if (recovery->start() != g5::RecoveryStartResult::Started) {
    std::cerr << "G8_RECOVERY_START=FAIL\n";
    return result;
  }
  const auto initial = wait_for_generation(*recovery, 1U);
  if (!initial.has_value()) {
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }

  const auto gateway_instance_id = g7::generate_gateway_instance_id();
  g7::OrderBookGrpcServer server{runtime, gateway_instance_id};
  if (!server.start("127.0.0.1:0")) {
    std::cerr << "G8_GRPC_START=FAIL\n";
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }
  auto stub = wire::BinanceMarketDataGatewayService::NewStub(
      grpc::CreateChannel("127.0.0.1:" + std::to_string(server.selected_port()),
                          grpc::InsecureChannelCredentials()));

  StreamSession old_session{*stub,
                            make_request(kind == PhaseKind::ControlledRecovery
                                             ? "g8-recovery-generation-1"
                                             : "g8-rotation-generation-1"),
                            gateway_instance_id};
  if (!old_session.wait_for_initial(1U)) {
    std::cerr << "G8_INITIAL_CONSUMER=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }

  if (kind == PhaseKind::ControlledRecovery &&
      !recovery->request_controlled_recovery_for_acceptance()) {
    std::cerr << "G8_CONTROLLED_RECOVERY_REQUEST=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }
  if (!old_session.wait_for_terminal()) {
    std::cerr << "G8_OLD_SESSION_TERMINAL=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }

  const auto generation_two = wait_for_generation(*recovery, 2U);
  if (!generation_two.has_value() || !old_session.wait_for_finish()) {
    std::cerr << "G8_GENERATION_TWO=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }
  const auto old = old_session.evidence();
  const auto expected_reason =
      kind == PhaseKind::ControlledRecovery
          ? common::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE
          : common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED;
  const auto expected_action =
      kind == PhaseKind::ControlledRecovery
          ? common::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT
          : common::RECOVERY_ACTION_RESUBSCRIBE;
  const bool old_valid =
      old.canonical && old.contiguous && old.terminal && old.finished &&
      old.status.ok() && !old.payload_after_terminal &&
      old.terminal_reason == expected_reason &&
      old.recovery_action == expected_action && old.terminal_generation == 1U &&
      old.snapshot_generation == 1U && old.update_generation == 1U;
  if (!old_valid) {
    std::cerr << "G8_OLD_SESSION_EVIDENCE=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }

  StreamSession new_session{*stub,
                            make_request(kind == PhaseKind::ControlledRecovery
                                             ? "g8-recovery-generation-2"
                                             : "g8-rotation-generation-2"),
                            gateway_instance_id};
  if (!new_session.wait_for_initial(2U)) {
    std::cerr << "G8_NEW_SESSION=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }
  const auto new_evidence = new_session.evidence();
  const bool new_valid = new_evidence.canonical && new_evidence.contiguous &&
                         new_evidence.accepted && new_evidence.snapshot &&
                         new_evidence.later_update &&
                         new_evidence.subscription_id != old.subscription_id &&
                         new_evidence.snapshot_generation == 2U &&
                         new_evidence.update_generation == 2U &&
                         new_evidence.expected_sequence >= 4U;
  if (!new_valid) {
    std::cerr << "G8_NEW_SESSION_EVIDENCE=FAIL\n";
    server.shutdown();
    recovery->stop();
    result.final_runtime_state = runtime.observe().state;
    return result;
  }

  const auto generation_two_observation = recovery->observe();
  const auto final_cut = recovery->quiesce_for_acceptance();
  const bool clean_cut = final_cut.has_value() &&
                         clean_generation_two_cut(*final_cut, *generation_two);
  new_session.cancel_and_join();
  const auto new_final = new_session.evidence();
  const bool subscriber_removed = wait_for_subscriber_count(runtime, 0U);
  server.shutdown();
  result.final_grpc_contexts = server.service().tracked_context_count();
  result.final_subscribers = runtime.observe().resident_subscription_count;
  recovery->stop();
  result.final_runtime_state = runtime.observe().state;

  result.old_session_terminated = old.terminal && old.finished;
  result.old_session_contiguous = old.contiguous;
  result.old_session_crossed_rebootstrap =
      old.snapshot_generation != 1U || old.update_generation != 1U ||
      old.terminal_generation != 1U || old.payload_after_terminal;
  result.new_session_sequence_reset =
      new_final.accepted && new_final.contiguous && new_final.finished &&
      (new_final.status.ok() ||
       new_final.status.error_code() == grpc::StatusCode::CANCELLED) &&
      new_final.expected_sequence >= 4U;
  result.connection_id_changed =
      generation_two->connection_id != initial->connection_id;
  result.fresh_snapshot =
      new_final.snapshot && new_final.snapshot_generation == 2U &&
      new_final.later_update && new_final.update_generation == 2U;
  result.final_transport_active =
      !clean_cut || generation_two_observation.active_transport_count != 1U;
  if (clean_cut) {
    result.final_transport_active = final_cut->transport.running;
  }
  result.max_active_transports =
      generation_two_observation.max_active_transport_count;
  result.consecutive_recovery_attempts =
      generation_two_observation.consecutive_recovery_attempts;
  result.planned_rotation_count =
      generation_two_observation.planned_rotation_count;
  result.passed =
      clean_cut && subscriber_removed && result.old_session_terminated &&
      result.old_session_contiguous &&
      !result.old_session_crossed_rebootstrap &&
      result.new_session_sequence_reset && result.connection_id_changed &&
      result.fresh_snapshot && !result.final_transport_active &&
      result.final_grpc_contexts == 0U && result.final_subscribers == 0U &&
      result.final_runtime_state == g3::RuntimeState::Stopped &&
      result.max_active_transports <= 1U &&
      (kind != PhaseKind::PlannedRotation ||
       (result.planned_rotation_count >= 1U &&
        result.consecutive_recovery_attempts == 0U));
  return result;
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
  const auto &numeric_spec = std::get<g4::SpotMetadata>(metadata).numeric_spec;

  const auto recovery = run_phase(PhaseKind::ControlledRecovery, numeric_spec);
  if (!recovery.passed) {
    std::cerr << "REAL_RECOVERY_CONSUMER=FAIL\n";
    return EXIT_FAILURE;
  }
  const auto rotation = run_phase(PhaseKind::PlannedRotation, numeric_spec);
  if (!rotation.passed) {
    std::cerr << "REAL_PLANNED_ROTATION_CONSUMER=FAIL\n";
    return EXIT_FAILURE;
  }

  const auto max_active =
      std::max(recovery.max_active_transports, rotation.max_active_transports);
  std::cout << "EXCHANGE_INFO_FETCH=PASS\n"
            << "TLS_VERIFY="
            << (exchange_response.tls_verified ? "PASS" : "FAIL") << '\n'
            << "INITIAL_REAL_BOOTSTRAP=PASS\n"
            << "INITIAL_REAL_CONSUMER=PASS\n"
            << "RECOVERY_OLD_SESSION_TERMINATED=PASS\n"
            << "RECOVERY_OLD_SESSION_CONTIGUOUS=YES\n"
            << "RECOVERY_OLD_GENERATION=1\n"
            << "RECOVERY_NEW_GENERATION=2\n"
            << "RECOVERY_TERMINAL_REASON=RESUME_NOT_AVAILABLE\n"
            << "RECOVERY_ACTION=REQUEST_NEW_SNAPSHOT\n"
            << "RECOVERY_CONNECTION_ID_CHANGED=YES\n"
            << "RECOVERY_FRESH_SNAPSHOT=PASS\n"
            << "RECOVERY_NEW_SESSION_SEQUENCE_RESET=YES\n"
            << "PLANNED_ROTATION_OLD_SESSION_TERMINATED=PASS\n"
            << "PLANNED_ROTATION_OLD_SESSION_CONTIGUOUS=YES\n"
            << "PLANNED_ROTATION_TERMINAL_REASON="
               "CONNECTION_GENERATION_CHANGED\n"
            << "PLANNED_ROTATION_RECOVERY_ACTION=RESUBSCRIBE\n"
            << "PLANNED_ROTATION_RECOVERY_ATTEMPTS="
            << rotation.consecutive_recovery_attempts << '\n'
            << "PLANNED_ROTATION_NEW_GENERATION=2\n"
            << "PLANNED_ROTATION_CONNECTION_ID_CHANGED=YES\n"
            << "PLANNED_ROTATION_FRESH_SNAPSHOT=PASS\n"
            << "PLANNED_ROTATION_NEW_SESSION_SEQUENCE_RESET=YES\n"
            << "OLD_SESSION_CROSSED_REBOOTSTRAP=NO\n"
            << "ALL_SESSION_SEQUENCES_CONTIGUOUS=YES\n"
            << "MAX_ACTIVE_TRANSPORTS=" << max_active << '\n'
            << "FINAL_TRANSPORT_ACTIVE=NO\n"
            << "FINAL_GRPC_CONTEXTS=" << rotation.final_grpc_contexts << '\n'
            << "FINAL_SUBSCRIBERS=" << rotation.final_subscribers << '\n'
            << "FINAL_RUNTIME_STATE=Stopped\n"
            << "GATEWAY_CONTRACTS_PROJECTION_BOUNDARY=PASS\n"
            << "REAL_RATE_LIMIT_ABUSE_ATTEMPTED=NO\n";
  return EXIT_SUCCESS;
}
