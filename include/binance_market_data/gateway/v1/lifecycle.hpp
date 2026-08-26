#pragma once

#include "binance_market_data/gateway/v1/config.hpp"

#include <cstdint>
#include <variant>

namespace binance_market_data::gateway::v1 {

enum class LifecycleState : std::uint8_t {
  Constructed,
  Running,
  Stopped,
};

enum class LifecycleErrorCode : std::uint8_t {
  AlreadyRunning,
  AlreadyStopped,
  NotRunning,
};

struct LifecycleError final {
  LifecycleErrorCode code;
  LifecycleState state;

  friend constexpr bool operator==(const LifecycleError &,
                                   const LifecycleError &) = default;
};

using LifecycleResult = std::variant<LifecycleState, LifecycleError>;

// A synchronous lifecycle seam. It owns no threads, sockets, queues, clocks, or
// callbacks.
class Foundation final {
public:
  explicit Foundation(ValidatedGatewayConfig config);

  Foundation(Foundation &&) noexcept = default;
  Foundation &operator=(Foundation &&) noexcept = default;
  Foundation(const Foundation &) = delete;
  Foundation &operator=(const Foundation &) = delete;
  ~Foundation() = default;

  [[nodiscard]] LifecycleResult start() noexcept;
  [[nodiscard]] LifecycleResult stop() noexcept;
  [[nodiscard]] LifecycleState state() const noexcept { return state_; }
  [[nodiscard]] const GatewayConfig &config() const noexcept { return config_; }

private:
  GatewayConfig config_;
  LifecycleState state_{LifecycleState::Constructed};
};

} // namespace binance_market_data::gateway::v1
