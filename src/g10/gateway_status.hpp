#pragma once

#include "event_publication.hpp"
#include "market_runtime.hpp"
#include "spot_recovery.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_messages.pb.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>

namespace binance_market_data::gateway::g10 {

namespace common_wire = ::binance_market_data::common::v1;
namespace gateway_wire = ::binance_market_data::gateway::v1;

inline constexpr char kStatusRequestSchema[] = "gateway-status-request.v1";
inline constexpr char kStatusSnapshotSchema[] = "gateway-status-snapshot.v1";

enum class StatusSnapshotError : std::uint8_t {
  StartBaselineUnavailable,
  ClockError,
  MonotonicClockRegression,
  InvalidObservation,
};

using StatusSnapshotResult =
    std::variant<gateway_wire::GatewayStatusSnapshot, StatusSnapshotError>;

[[nodiscard]] std::optional<common_wire::StreamLifecycleState>
map_runtime_state(g3::RuntimeState state) noexcept;

// Synchronous G10 status assembler. It observes the existing runtime
// components but does not own or schedule any of them.
class GatewayStatusAssembler final {
public:
  GatewayStatusAssembler(g3::MarketRuntime &runtime, g5::SpotRecovery &recovery,
                         g9::EventPublication &event_publication,
                         g3::RuntimeClock clock,
                         const std::string &gateway_instance_id);

  GatewayStatusAssembler(const GatewayStatusAssembler &) = delete;
  GatewayStatusAssembler &operator=(const GatewayStatusAssembler &) = delete;

  // Called immediately before the server's BuildAndStart() attempt. A failed
  // attempt must clear this provisional baseline before it is retried.
  [[nodiscard]] bool prepare_start_baseline() noexcept;
  void clear_start_baseline() noexcept;

  [[nodiscard]] StatusSnapshotResult collect() const;

private:
  g3::MarketRuntime &runtime_;
  g5::SpotRecovery &recovery_;
  g9::EventPublication &event_publication_;
  g3::RuntimeClock clock_;
  const std::string gateway_instance_id_;

  mutable std::mutex baseline_mutex_;
  std::optional<std::uint64_t> start_baseline_monotonic_ns_;
};

} // namespace binance_market_data::gateway::g10
