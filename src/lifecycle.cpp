#include "binance_market_data/gateway/v1/lifecycle.hpp"

#include <utility>

namespace binance_market_data::gateway::v1 {

Foundation::Foundation(ValidatedGatewayConfig config)
    : config_{std::move(config).take()} {}

LifecycleResult Foundation::start() noexcept {
  if (state_ == LifecycleState::Running) {
    return LifecycleError{LifecycleErrorCode::AlreadyRunning, state_};
  }
  if (state_ == LifecycleState::Stopped) {
    return LifecycleError{LifecycleErrorCode::AlreadyStopped, state_};
  }
  state_ = LifecycleState::Running;
  return state_;
}

LifecycleResult Foundation::stop() noexcept {
  if (state_ == LifecycleState::Constructed) {
    return LifecycleError{LifecycleErrorCode::NotRunning, state_};
  }
  if (state_ == LifecycleState::Stopped) {
    return LifecycleError{LifecycleErrorCode::AlreadyStopped, state_};
  }
  state_ = LifecycleState::Stopped;
  return state_;
}

} // namespace binance_market_data::gateway::v1
