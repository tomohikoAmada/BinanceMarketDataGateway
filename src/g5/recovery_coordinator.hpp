#pragma once

#include "spot_transport.hpp"

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>

namespace binance_market_data::gateway::g5 {

enum class RecoveryState : std::uint8_t {
  Starting,
  Live,
  Rotating,
  Backoff,
  Recovering,
  Exhausted,
  Stopping,
  Stopped,
};

struct PlannedRotationPolicy final {
  std::chrono::nanoseconds age;
};

struct PlannedRotationCut final {
  std::uint64_t generation{0U};
  g4::TransportObservation transport;
  g3::RuntimeObservation runtime;
};

enum class SourceGenerationCloseOutcome : std::uint8_t {
  Replacement,
  PermanentFailure,
  GlobalShutdown,
};

struct SourceGenerationLifecycle final {
  std::function<bool(std::uint64_t)> open;
  std::function<bool(std::uint64_t)> quiesce;
  std::function<bool(std::uint64_t, SourceGenerationCloseOutcome)> close;
};

enum class RecoveryCause : std::uint8_t {
  NeedsResync,
  TransportFailure,
  SnapshotFailure,
  IngressOverflow,
  BootstrapBufferOverflow,
  Protocol,
  ServerShutdown,
  Http429,
  Http418,
  Http5xx,
  TerminalHttp4xx,
  InternalFailure,
};

enum class RecoveryStartResult : std::uint8_t {
  Started,
  AlreadyStarted,
  Stopped,
  RuntimeStartFailed,
};

namespace detail {

inline constexpr std::size_t kMaximumRecoveryAttempts = 6U;
inline constexpr std::size_t kRecoveryFailureHistoryCapacity =
    kMaximumRecoveryAttempts + 1U;

} // namespace detail

struct RecoveryFailureDiagnostic final {
  std::uint64_t connection_generation{0U};
  RecoveryCause cause{RecoveryCause::InternalFailure};
  std::optional<g4::NetworkError> network_error;
  g3::RuntimeState runtime_state{g3::RuntimeState::Constructed};
  g3::core::ProjectionStatus projection_status{
      g3::core::ProjectionStatus::AwaitingBaseline};
  std::optional<g3::core::GapInfo> last_gap;
  std::optional<g3::FaultReason> fault_reason;
  std::optional<g3::adapter::AdapterError> adapter_error;
};

struct RecoveryObservation final {
  RecoveryState state{RecoveryState::Starting};
  std::uint64_t connection_generation{0U};
  std::string connection_id;
  std::optional<std::uint64_t> last_event_utc_ns;
  std::size_t consecutive_recovery_attempts{0U};
  std::uint64_t total_recovery_count{0U};
  std::uint64_t planned_rotation_count{0U};
  std::optional<std::uint64_t> last_rotation_generation;
  std::optional<PlannedRotationCut> last_planned_rotation_cut;
  std::optional<std::uint64_t> generation_started_monotonic_ns;
  std::optional<RecoveryCause> last_recovery_cause;
  std::chrono::seconds last_requested_delay{0};
  bool in_backoff{false};
  bool terminal{false};
  bool exhausted{false};
  std::size_t active_transport_count{0U};
  std::size_t max_active_transport_count{0U};
  std::optional<g4::NetworkError> terminal_error;
  // The most recent classified unplanned failure cuts are retained oldest to
  // newest. Once full, recording a new cut discards only the oldest one.
  std::array<RecoveryFailureDiagnostic, detail::kRecoveryFailureHistoryCapacity>
      failure_history{};
  std::size_t failure_history_size{0U};
};

struct QuiescentAcceptanceCut final {
  g4::TransportObservation transport;
  g3::RuntimeObservation runtime;
};

namespace detail {

class RecoveryAttempt {
public:
  virtual ~RecoveryAttempt() = default;
  [[nodiscard]] virtual g4::TransportStartResult start() = 0;
  virtual void stop() noexcept = 0;
  [[nodiscard]] virtual g4::TransportObservation observe() const = 0;
};

using AttemptFactory = std::function<std::unique_ptr<RecoveryAttempt>(
    g3::MarketRuntime &, const g3::RuntimeClock &, std::uint64_t)>;
using BackoffWaiter =
    std::function<bool(std::chrono::seconds, std::stop_token)>;
using RotationWaiter = std::function<g3::TimedRecoveryWaitResult(
    g3::MarketRuntime &, std::chrono::nanoseconds, std::stop_token)>;

struct RecoveryTestOptions final {
  AttemptFactory attempt_factory;
  BackoffWaiter backoff_waiter;
  RotationWaiter rotation_waiter;
  std::function<void()> lifecycle_shutdown_established;
  std::function<void()> before_backoff_commit;
  std::function<void()> before_planned_reset;
};

[[nodiscard]] std::optional<std::chrono::seconds>
parse_retry_after(std::optional<std::string> value) noexcept;

[[nodiscard]] std::chrono::seconds
normal_backoff_delay(std::size_t recovery_attempt);

} // namespace detail

struct RecoveryCoordinatorOptions final {
  detail::AttemptFactory attempt_factory;
  SourceGenerationLifecycle source_generation_lifecycle;
};

// Product-independent G5/G6 lifecycle authority. Each instance owns only one
// single-product MarketRuntime lifecycle and consumes one product attempt
// factory; it never switches products or shares state with another instance.
class RecoveryCoordinator final {
public:
  RecoveryCoordinator(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                      RecoveryCoordinatorOptions options,
                      detail::RecoveryTestOptions test_options = {});
  RecoveryCoordinator(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                      PlannedRotationPolicy planned_rotation,
                      RecoveryCoordinatorOptions options,
                      detail::RecoveryTestOptions test_options = {});
  ~RecoveryCoordinator();

  RecoveryCoordinator(const RecoveryCoordinator &) = delete;
  RecoveryCoordinator &operator=(const RecoveryCoordinator &) = delete;
  RecoveryCoordinator(RecoveryCoordinator &&) = delete;
  RecoveryCoordinator &operator=(RecoveryCoordinator &&) = delete;

  [[nodiscard]] RecoveryStartResult start();
  void stop() noexcept;
  [[nodiscard]] RecoveryObservation observe() const;
  [[nodiscard]] RecoveryObservation
  wait_for_generation_live(std::uint64_t generation);
  [[nodiscard]] RecoveryObservation wait_until_terminal();
  [[nodiscard]] bool request_controlled_recovery_for_acceptance();
  [[nodiscard]] std::optional<QuiescentAcceptanceCut> quiesce_for_acceptance();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace binance_market_data::gateway::g5
