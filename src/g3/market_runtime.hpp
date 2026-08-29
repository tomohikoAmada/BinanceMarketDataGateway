#pragma once

#include <binance_market_data/market/v1/market_events.pb.h>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>
#include <binance_market_data/projection/v1/snapshots.pb.h>
#include <binance_market_data/projection_adapter/v1/proto_adapter.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <thread>
#include <variant>

namespace binance_market_data::gateway::g3 {

namespace adapter = projection_adapter::v1;
namespace core = projection::v1;
namespace market = ::binance_market_data::market::v1;

struct RuntimeLimits final {
  std::size_t ingress_capacity{64U};
  std::size_t bootstrap_capacity{64U};
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
  std::thread::id last_reset_thread_id;
  std::uint64_t reset_count{0U};
  std::uint64_t last_admitted_ticket{0U};
  std::uint64_t processed_ticket{0U};
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

struct RuntimeTestOptions final {
  bool owner_starts_paused{false};
};

// Internal G3 runtime for exactly BINANCE + SPOT + BTCUSDT. This header is
// exposed only by the opt-in G3 build target; it is not an installed API.
// start(), stop(), and destruction must be coordinated by one external
// lifecycle owner. Concurrent lifecycle operations are not supported.
// RuntimeClock runs on the serialized owner thread and must not call or
// re-enter MarketRuntime; in particular, it must never call stop().
class MarketRuntime final {
public:
  explicit MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
                         core::NumericSpec numeric_spec,
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
  submit_snapshot(market::ExchangeDepthSnapshot snapshot);
  [[nodiscard]] AdmissionResult submit_transport_failure();
  [[nodiscard]] AdmissionResult submit_snapshot_failure();

  // Both operations establish a FIFO barrier behind all earlier admitted
  // source events. Returned Projection-derived state is an owning copy.
  [[nodiscard]] RuntimeObservation observe();
  [[nodiscard]] SnapshotResult capture_snapshot();

  // The caller must first establish a no-future-submission cut for the old
  // source generation. In G5 that means stopping and joining SpotTransport,
  // then establishing an observe() barrier. Projection reset executes only on
  // the existing serialized owner thread. Lifetime ticket counters remain
  // monotonic.
  [[nodiscard]] RebootstrapResetResult reset_for_rebootstrap();

  // Blocking state waits used by the G5 lifecycle coordinator. A requested
  // stop returns std::nullopt without manufacturing a runtime fault.
  [[nodiscard]] std::optional<RuntimeObservation>
  wait_until_live_or_recovery_required(std::stop_token stop_token);
  [[nodiscard]] std::optional<RuntimeObservation>
  wait_until_recovery_required(std::stop_token stop_token);

  [[nodiscard]] IngressObservation ingress_observation() const noexcept;

  // A deterministic test seam: production construction leaves the owner
  // released. Stop always overrides this gate, including with a full ingress.
  void release_owner_for_testing() noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace binance_market_data::gateway::g3
