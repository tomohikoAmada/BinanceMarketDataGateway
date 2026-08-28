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
  bool owner_joined{false};
};

enum class SnapshotRequestError : std::uint8_t {
  NotStarted,
  NotLive,
  Busy,
  Faulted,
  Stopping,
  Stopped,
  ClockError,
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
class MarketRuntime final {
public:
  explicit MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
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

  [[nodiscard]] IngressObservation ingress_observation() const noexcept;

  // A deterministic test seam: production construction leaves the owner
  // released. Stop always overrides this gate, including with a full ingress.
  void release_owner_for_testing() noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace binance_market_data::gateway::g3
