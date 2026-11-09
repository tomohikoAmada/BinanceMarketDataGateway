#pragma once

#include "publication/event_publication.hpp"
#include "recovery/spot_recovery.hpp"
#include "runtime/market_runtime.hpp"
#if defined(BMD_GATEWAY_G11_ENABLED)
#include "composition/market_registry.hpp"
#endif

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_messages.pb.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>

namespace binance_market_data::gateway::status {

namespace common_wire = ::binance_market_data::common::v1;
namespace gateway_wire = ::binance_market_data::gateway::v1;

inline constexpr char kStatusRequestSchema[] = "gateway-status-request.v1";
inline constexpr char kStatusSnapshotSchema[] = "gateway-status-snapshot.v1";

enum class StatusSnapshotError : std::uint8_t {
  StartBaselineUnavailable,
  ClockError,
  MonotonicClockRegression,
  InvalidObservation,
  ObservationTimeout,
};

using StatusSnapshotResult =
    std::variant<gateway_wire::GatewayStatusSnapshot, StatusSnapshotError>;

[[nodiscard]] std::optional<common_wire::StreamLifecycleState>
map_runtime_state(runtime::RuntimeState state) noexcept;

// Synchronous G10 status assembler. It observes the existing runtime
// components but does not own or schedule any of them.
class GatewayStatusAssembler final {
public:
  GatewayStatusAssembler(runtime::MarketRuntime &runtime,
                         recovery::SpotRecovery &recovery,
                         publication::EventPublication &event_publication,
                         runtime::RuntimeClock clock,
                         const std::string &gateway_instance_id);
#if defined(BMD_GATEWAY_G11_ENABLED)
  GatewayStatusAssembler(const runtime::MarketRuntimeRegistry &registry,
                         runtime::RuntimeClock clock,
                         const std::string &gateway_instance_id);
#endif

  GatewayStatusAssembler(const GatewayStatusAssembler &) = delete;
  GatewayStatusAssembler &operator=(const GatewayStatusAssembler &) = delete;

  // Called immediately before the server's BuildAndStart() attempt. A failed
  // attempt must clear this provisional baseline before it is retried.
  [[nodiscard]] bool prepare_start_baseline() noexcept;
  void clear_start_baseline() noexcept;

  [[nodiscard]] StatusSnapshotResult
  collect(std::chrono::steady_clock::time_point deadline =
              std::chrono::steady_clock::now() + runtime::kObservationTimeout,
          std::stop_token stop_token = {}) const;

private:
  runtime::MarketRuntime *runtime_{nullptr};
  recovery::SpotRecovery *recovery_{nullptr};
  publication::EventPublication *event_publication_{nullptr};
#if defined(BMD_GATEWAY_G11_ENABLED)
  const runtime::MarketRuntimeRegistry *registry_{nullptr};
#endif
  runtime::RuntimeClock clock_;
  const std::string gateway_instance_id_;

  mutable std::mutex baseline_mutex_;
  std::optional<std::uint64_t> start_baseline_monotonic_ns_;
};

} // namespace binance_market_data::gateway::status
