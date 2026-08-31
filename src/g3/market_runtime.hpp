#pragma once

#include "order_book_publication.hpp"
#include "source_provenance.hpp"

#include <binance_market_data/market/v1/market_events.pb.h>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>
#include <binance_market_data/projection/v1/snapshots.pb.h>
#include <binance_market_data/projection_adapter/v1/proto_adapter.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <thread>
#include <utility>
#include <variant>

namespace binance_market_data::gateway::g3 {

namespace adapter = projection_adapter::v1;
namespace core = projection::v1;
namespace market = ::binance_market_data::market::v1;

struct RuntimeLimits final {
  constexpr RuntimeLimits(std::size_t ingress_capacity_value = 64U,
                          std::size_t bootstrap_capacity_value = 64U,
                          g7::PublicationLimits publication_value = {}) noexcept
      : ingress_capacity{ingress_capacity_value},
        bootstrap_capacity{bootstrap_capacity_value},
        publication{publication_value} {}

  std::size_t ingress_capacity;
  std::size_t bootstrap_capacity;
  g7::PublicationLimits publication;
};

struct ClockSample final {
  std::uint64_t utc_ns;
  std::uint64_t monotonic_ns;

  friend constexpr bool operator==(ClockSample, ClockSample) noexcept = default;
};

using RuntimeClock = std::function<ClockSample()>;

enum class RuntimeState : std::uint8_t {
  Constructed,
  Buffering,
  AwaitingBridge,
  Live,
  NeedsResync,
  Faulted,
  Stopping,
  Stopped,
};

enum class FaultReason : std::uint8_t {
  IngressOverflow,
  BootstrapBufferOverflow,
  AdapterError,
  TransportFailure,
  SnapshotFailure,
  ProjectionRejected,
  ClockError,
  InternalError,
};

enum class StartResult : std::uint8_t {
  Started,
  AlreadyStarted,
  Stopped,
};

enum class AdmissionResult : std::uint8_t {
  Accepted,
  Full,
  NotStarted,
  Stopping,
  Stopped,
  Faulted,
};

struct IngressObservation final {
  std::size_t occupancy;
  std::size_t capacity;
};

struct RuntimeObservation final {
  RuntimeState state{RuntimeState::Constructed};
  core::ProjectionStatus projection_status{
      core::ProjectionStatus::AwaitingBaseline};
  std::optional<std::uint64_t> last_update_id;
  std::optional<core::GapInfo> last_gap;
  std::optional<core::InstallResult> last_install;
  std::optional<core::ApplyResult> last_apply;
  std::optional<adapter::AdapterError> adapter_error;
  std::optional<FaultReason> fault_reason;
  std::size_t ingress_occupancy{0U};
  std::size_t ingress_capacity{0U};
  std::size_t bootstrap_occupancy{0U};
  std::size_t bootstrap_capacity{0U};
  std::thread::id owner_thread_id;
  std::thread::id last_publication_thread_id;
  std::thread::id last_reset_thread_id;
  std::uint64_t reset_count{0U};
  std::uint64_t last_admitted_ticket{0U};
  std::uint64_t processed_ticket{0U};
  std::optional<std::uint64_t> current_projection_generation;
  std::size_t resident_subscription_count{0U};
  std::size_t pending_admission_count{0U};
  bool publication_admission_open{false};
  bool publication_shutdown{false};
  bool owner_joined{false};
};

enum class RebootstrapResetResult : std::uint8_t {
  Reset,
  NotStarted,
  InvalidState,
  Busy,
  Stopping,
  Stopped,
  InternalError,
};

enum class PlannedRebootstrapResetResult : std::uint8_t {
  Reset,
  NotStarted,
  InvalidState,
  Busy,
  Stopping,
  Stopped,
  InternalError,
};

enum class TimedRecoveryWaitResult : std::uint8_t {
  RecoveryRequired,
  DeadlineReached,
  Stopped,
};

enum class SnapshotRequestError : std::uint8_t {
  NotStarted,
  NotLive,
  Busy,
  Faulted,
  Stopping,
  Stopped,
  ClockError,
  InternalError,
};

struct CapturedSnapshot final {
  core::LocalOrderBookSnapshot snapshot;
  ClockSample generated_at;
  std::thread::id captured_on_thread;
};

using SnapshotResult =
    std::variant<CapturedSnapshot, adapter::AdapterError, SnapshotRequestError>;

enum class SubscriptionAdmissionError : std::uint8_t {
  NotStarted,
  NotLive,
  PendingLimit,
  ActiveLimit,
  InvalidDepthLimit,
  ShuttingDown,
  ClockError,
  IdExhausted,
  InternalError,
  Stopped,
};

struct AcceptedSubscription final {
  std::shared_ptr<g7::SubscriberChannel> channel;
  std::thread::id admitted_on_thread;
};

using SubscriptionAdmissionResult =
    std::variant<AcceptedSubscription, SubscriptionAdmissionError>;

enum class PublicationShutdownResult : std::uint8_t {
  ShutDown,
  AlreadyShutDown,
  NotStarted,
  Stopped,
};

struct RuntimeTestOptions final {
  RuntimeTestOptions(
      bool owner_starts_paused_value = false,
      std::function<void()> admission_enqueued_value = {},
      std::function<void()> before_admission_processing_value = {})
      : owner_starts_paused{owner_starts_paused_value},
        admission_enqueued{std::move(admission_enqueued_value)},
        before_admission_processing{
            std::move(before_admission_processing_value)} {}

  bool owner_starts_paused;
  std::function<void()> admission_enqueued;
  std::function<void()> before_admission_processing;
};

// Internal single-product runtime. This header is exposed only by the opt-in
// G3 build target; it is not an installed API.
// start(), stop(), and destruction must be coordinated by one external
// lifecycle owner. Concurrent lifecycle operations are not supported.
// RuntimeClock runs on the serialized owner thread and must not call or
// re-enter MarketRuntime; in particular, it must never call stop().
class MarketRuntime final {
public:
  explicit MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
                         core::NumericSpec numeric_spec,
                         RuntimeTestOptions test_options = {});
  MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
                core::NumericSpec numeric_spec,
                adapter::ExpectedIdentity expected_identity,
                RuntimeTestOptions test_options = {});
  ~MarketRuntime();

  MarketRuntime(const MarketRuntime &) = delete;
  MarketRuntime &operator=(const MarketRuntime &) = delete;
  MarketRuntime(MarketRuntime &&) = delete;
  MarketRuntime &operator=(MarketRuntime &&) = delete;

  [[nodiscard]] StartResult start();
  void stop() noexcept;

  [[nodiscard]] AdmissionResult submit_depth_update(market::DepthUpdate update);
  [[nodiscard]] AdmissionResult
  submit_depth_update(market::DepthUpdate update, SourceProvenance provenance);
  [[nodiscard]] AdmissionResult
  submit_snapshot(market::ExchangeDepthSnapshot snapshot);
  [[nodiscard]] AdmissionResult
  submit_snapshot(market::ExchangeDepthSnapshot snapshot,
                  SourceProvenance provenance);
  [[nodiscard]] AdmissionResult submit_transport_failure();
  [[nodiscard]] AdmissionResult submit_snapshot_failure();

  // Both operations establish a FIFO barrier behind all earlier admitted
  // source events. Returned Projection-derived state is an owning copy.
  [[nodiscard]] RuntimeObservation observe();
  [[nodiscard]] SnapshotResult capture_snapshot();

  // G7 request admission is serialized onto the Projection owner through a
  // distinct bounded target-ticket mailbox. The returned channel is owning.
  [[nodiscard]] SubscriptionAdmissionResult admit_order_book_subscription(
      g7::ValidatedOrderBookSubscription subscription);
  void notify_subscriber_closed() noexcept;

  // Closing the gate is nonblocking and shares the owner-commit mutex. The
  // reserved shutdown control then runs on the owner and wakes all channels.
  void close_publication_admission() noexcept;
  [[nodiscard]] PublicationShutdownResult shutdown_publication() noexcept;

  // The caller must first establish a no-future-submission cut for the old
  // source generation. In G5 that means stopping and joining SpotTransport,
  // then establishing an observe() barrier. Projection reset executes only on
  // the existing serialized owner thread. Lifetime ticket counters remain
  // monotonic.
  [[nodiscard]] RebootstrapResetResult reset_for_rebootstrap();

  // The caller must first stop and join the healthy source generation and
  // establish an observe() FIFO barrier. This separate owner command accepts
  // only Live/Synchronized/no-fault state; it never manufactures a recovery
  // fault. Lifetime ticket counters remain monotonic.
  [[nodiscard]] PlannedRebootstrapResetResult
  reset_live_for_planned_rebootstrap();

  // Blocking state waits used by the G5 lifecycle coordinator. A requested
  // stop returns std::nullopt without manufacturing a runtime fault.
  [[nodiscard]] std::optional<RuntimeObservation>
  wait_until_live_or_recovery_required(std::stop_token stop_token);
  [[nodiscard]] std::optional<RuntimeObservation>
  wait_until_recovery_required(std::stop_token stop_token);
  [[nodiscard]] TimedRecoveryWaitResult
  wait_until_recovery_required_for(std::stop_token stop_token,
                                   std::chrono::nanoseconds duration);

  [[nodiscard]] IngressObservation ingress_observation() const noexcept;

  // A deterministic test seam: production construction leaves the owner
  // released. Stop always overrides this gate, including with a full ingress.
  void release_owner_for_testing() noexcept;
  void pause_owner_for_testing() noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace binance_market_data::gateway::g3
