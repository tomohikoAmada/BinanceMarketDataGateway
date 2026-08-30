#include "planned_rotation.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g6 = binance_market_data::gateway::g6;
namespace market_wire = binance_market_data::market::v1;

class TestFailure final : public std::exception {
public:
  explicit TestFailure(std::string message) : message_{std::move(message)} {}
  [[nodiscard]] const char *what() const noexcept override {
    return message_.c_str();
  }

private:
  std::string message_;
};

void require(bool condition, std::string_view expression) {
  if (!condition) {
    throw TestFailure{std::string{expression}};
  }
}

template <typename Actual, typename Expected>
void require_equal(const Actual &actual, const Expected &expected,
                   std::string_view expression) {
  if (!(actual == expected)) {
    throw TestFailure{std::string{expression}};
  }
}

#define REQUIRE(condition) require((condition), #condition)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] g3::RuntimeClock fixed_clock() {
  return [] { return g3::ClockSample{1700000000123456000ULL, 1000ULL}; };
}

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid NumericSpec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] market_wire::DepthUpdate make_update(std::uint64_t first,
                                                   std::uint64_t final,
                                                   std::uint64_t generation) {
  market_wire::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g6-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g6-script-g" + std::to_string(generation));
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(1700000000002ULL);
  metadata->set_receive_time_utc_ns(1700000000002000000ULL);
  metadata->set_receive_monotonic_ns(9000000000002ULL);
  update.set_first_update_id(first);
  update.set_final_update_id(final);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("4.000");
  return update;
}

[[nodiscard]] market_wire::ExchangeDepthSnapshot
make_snapshot(std::uint64_t last_update_id, std::uint64_t generation) {
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g6-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g6-snapshot-g" + std::to_string(generation));
  snapshot.set_last_update_id(last_update_id);
  snapshot.set_exchange_transaction_time_ms(1700000000001ULL);
  snapshot.set_receive_time_utc_ns(1700000000001000000ULL);
  snapshot.set_receive_monotonic_ns(9000000000001ULL);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("2.500");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("3.000");
  return snapshot;
}

enum class Action {
  Live,
  FailOnStop,
  NeedsResyncOnStop,
  StartFailure,
  BlockStart,
};

struct ScriptState final {
  std::mutex mutex;
  std::condition_variable condition;
  std::vector<Action> actions;
  std::vector<std::uint64_t> created;
  std::vector<std::uint64_t> started;
  std::vector<std::uint64_t> stopped;
  std::vector<std::uint64_t> reset_count_at_creation;
  std::vector<std::chrono::seconds> delays;
  std::map<std::uint64_t, g4::NetworkError> external_errors;
  std::size_t active{0U};
  std::size_t max_active{0U};
};

class RotationGate final {
public:
  [[nodiscard]] g3::TimedRecoveryWaitResult
  wait(g3::MarketRuntime &, std::chrono::nanoseconds duration,
       std::stop_token stop_token) {
    std::unique_lock lock{mutex_};
    durations_.push_back(duration);
    ++calls_;
    condition_.notify_all();
    const auto ready =
        condition_.wait(lock, stop_token, [this] { return !results_.empty(); });
    if (!ready || stop_token.stop_requested()) {
      return g3::TimedRecoveryWaitResult::Stopped;
    }
    const auto result = results_.front();
    results_.pop_front();
    return result;
  }

  void trigger(g3::TimedRecoveryWaitResult result) {
    {
      std::lock_guard lock{mutex_};
      results_.push_back(result);
    }
    condition_.notify_all();
  }

  void wait_for_calls(std::size_t count) {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this, count] { return calls_ >= count; });
  }

  [[nodiscard]] std::size_t calls() const {
    std::lock_guard lock{mutex_};
    return calls_;
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable_any condition_;
  std::deque<g3::TimedRecoveryWaitResult> results_;
  std::vector<std::chrono::nanoseconds> durations_;
  std::size_t calls_{0U};
};

class ScriptAttempt final : public g5::detail::RecoveryAttempt {
public:
  ScriptAttempt(g3::MarketRuntime &runtime, std::uint64_t generation,
                Action action, std::shared_ptr<ScriptState> state)
      : runtime_{runtime}, generation_{generation}, action_{action},
        state_{std::move(state)} {
    observation_.connection_generation = generation_;
    observation_.connection_id = "g6-script-g" + std::to_string(generation_);
    std::lock_guard lock{state_->mutex};
    state_->created.push_back(generation_);
    state_->reset_count_at_creation.push_back(runtime_.observe().reset_count);
    ++state_->active;
    state_->max_active = std::max(state_->max_active, state_->active);
    state_->condition.notify_all();
  }

  ~ScriptAttempt() override { stop(); }

  [[nodiscard]] g4::TransportStartResult start() override {
    {
      std::lock_guard lock{state_->mutex};
      state_->started.push_back(generation_);
      state_->condition.notify_all();
    }
    {
      std::lock_guard lock{observation_mutex_};
      observation_.started = true;
      observation_.running = true;
      observation_.tls_verified = true;
      observation_.websocket_handshake = true;
    }
    if (action_ == Action::BlockStart) {
      std::unique_lock lock{state_->mutex};
      state_->condition.wait(lock, [this] { return stopped_.load(); });
      return g4::TransportStartResult::Failed;
    }
    if (action_ == Action::StartFailure) {
      static_cast<void>(runtime_.submit_transport_failure());
      std::lock_guard lock{observation_mutex_};
      observation_.terminal_error =
          network_error(g4::NetworkErrorCode::WebSocketHandshake);
      observation_.running = false;
      return g4::TransportStartResult::Failed;
    }

    const auto base = generation_ * 100U;
    static_cast<void>(runtime_.submit_depth_update(
        make_update(base + 1U, base + 1U, generation_)));
    static_cast<void>(
        runtime_.submit_snapshot(make_snapshot(base, generation_)));
    std::lock_guard lock{observation_mutex_};
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
    return g4::TransportStartResult::Started;
  }

  void stop() noexcept override {
    if (stopped_.exchange(true)) {
      return;
    }
    {
      std::lock_guard lock{observation_mutex_};
      if (action_ == Action::FailOnStop) {
        observation_.terminal_error =
            network_error(g4::NetworkErrorCode::WebSocketRead);
      }
      observation_.stopped = true;
      observation_.running = false;
    }
    if (action_ == Action::NeedsResyncOnStop) {
      const auto base = generation_ * 100U;
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 3U, base + 3U, generation_)));
    }
    {
      std::lock_guard lock{state_->mutex};
      state_->stopped.push_back(generation_);
      --state_->active;
      state_->condition.notify_all();
    }
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
    g4::TransportObservation result;
    {
      std::lock_guard lock{observation_mutex_};
      result = observation_;
    }
    std::lock_guard lock{state_->mutex};
    const auto found = state_->external_errors.find(generation_);
    if (found != state_->external_errors.end()) {
      result.terminal_error = found->second;
      result.running = false;
    }
    return result;
  }

private:
  [[nodiscard]] static g4::NetworkError
  network_error(g4::NetworkErrorCode code) {
    return {code, "g6-script", "scripted failure", std::nullopt, std::nullopt};
  }

  g3::MarketRuntime &runtime_;
  std::uint64_t generation_;
  Action action_;
  std::shared_ptr<ScriptState> state_;
  mutable std::mutex observation_mutex_;
  g4::TransportObservation observation_;
  std::atomic<bool> stopped_{false};
};

[[nodiscard]] std::shared_ptr<ScriptState>
make_script(std::initializer_list<Action> actions) {
  auto state = std::make_shared<ScriptState>();
  state->actions.assign(actions);
  return state;
}

[[nodiscard]] g5::detail::RecoveryTestOptions
script_options(const std::shared_ptr<ScriptState> &state,
               const std::shared_ptr<RotationGate> &rotation) {
  g5::detail::RecoveryTestOptions options;
  options.attempt_factory = [state](g3::MarketRuntime &runtime,
                                    const g3::RuntimeClock &,
                                    std::uint64_t generation)
      -> std::unique_ptr<g5::detail::RecoveryAttempt> {
    Action action = Action::StartFailure;
    {
      std::lock_guard lock{state->mutex};
      const auto index = static_cast<std::size_t>(generation - 1U);
      if (index < state->actions.size()) {
        action = state->actions[index];
      }
    }
    return std::make_unique<ScriptAttempt>(runtime, generation, action, state);
  };
  options.backoff_waiter = [state](std::chrono::seconds delay,
                                   std::stop_token stop_token) {
    std::lock_guard lock{state->mutex};
    state->delays.push_back(delay);
    return !stop_token.stop_requested();
  };
  options.rotation_waiter = [rotation](g3::MarketRuntime &runtime,
                                       std::chrono::nanoseconds duration,
                                       std::stop_token stop_token) {
    return rotation->wait(runtime, duration, stop_token);
  };
  return options;
}

[[nodiscard]] g5::PlannedRotationPolicy short_policy() {
  return {std::chrono::nanoseconds{100}};
}

void inject_failure(const std::shared_ptr<ScriptState> &state,
                    g3::MarketRuntime &runtime, std::uint64_t generation,
                    g4::NetworkErrorCode code) {
  {
    std::lock_guard lock{state->mutex};
    state->external_errors.emplace(
        generation,
        g4::NetworkError{code, "g6-script", "external scripted failure",
                         std::nullopt, std::nullopt});
  }
  REQUIRE_EQ(runtime.submit_transport_failure(), g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
}

void no_early_rotation_and_stop_before_deadline() {
  auto state = make_script({Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(1U);
  rotation->wait_for_calls(1U);
  REQUIRE_EQ(live.connection_generation, 1U);
  REQUIRE_EQ(recovery.observe().planned_rotation_count, 0U);
  recovery.stop();
  std::lock_guard lock{state->mutex};
  REQUIRE_EQ(state->created, (std::vector<std::uint64_t>{1U}));
  REQUIRE_EQ(state->active, 0U);
}

void planned_rotation_order_owner_and_counters() {
  auto state = make_script({Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  auto options = script_options(state, rotation);
  std::atomic<bool> clean_cut_before_reset{false};
  options.before_planned_reset = [&] {
    std::lock_guard lock{state->mutex};
    clean_cut_before_reset = state->stopped == std::vector<std::uint64_t>{1U} &&
                             runtime.observe().reset_count == 0U;
  };
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            std::move(options)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto first = recovery.wait_for_generation_live(1U);
  const auto before = runtime.observe();
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto second = recovery.wait_for_generation_live(2U);
  const auto after = runtime.observe();

  REQUIRE(clean_cut_before_reset.load());
  REQUIRE_EQ(second.connection_generation, 2U);
  REQUIRE(first.connection_id != second.connection_id);
  REQUIRE_EQ(second.planned_rotation_count, 1U);
  REQUIRE_EQ(second.last_rotation_generation, 1U);
  REQUIRE_EQ(second.consecutive_recovery_attempts, 0U);
  REQUIRE_EQ(second.total_recovery_count, 0U);
  REQUIRE_EQ(second.max_active_transport_count, 1U);
  REQUIRE_EQ(after.last_reset_thread_id, after.owner_thread_id);
  REQUIRE(after.last_reset_thread_id != std::this_thread::get_id());
  REQUIRE(after.last_admitted_ticket > before.last_admitted_ticket);
  REQUIRE(after.processed_ticket > before.processed_ticket);
  REQUIRE_EQ(second.generation_started_monotonic_ns, 1000U);
  REQUIRE(second.last_planned_rotation_cut.has_value());
  REQUIRE_EQ(second.last_planned_rotation_cut->generation, 1U);
  REQUIRE(second.last_planned_rotation_cut->transport.stopped);
  REQUIRE(!second.last_planned_rotation_cut->transport.running);
  REQUIRE(
      !second.last_planned_rotation_cut->transport.terminal_error.has_value());
  REQUIRE_EQ(second.last_planned_rotation_cut->runtime.state,
             g3::RuntimeState::Live);
  REQUIRE_EQ(second.last_planned_rotation_cut->runtime.projection_status,
             core::ProjectionStatus::Synchronized);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->max_active, 1U);
    REQUIRE_EQ(state->stopped.front(), 1U);
    REQUIRE_EQ(state->reset_count_at_creation.at(1), 1U);
    REQUIRE(state->delays.empty());
  }
  recovery.stop();
}

void repeated_rotation() {
  auto state = make_script({Action::Live, Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  static_cast<void>(recovery.wait_for_generation_live(2U));
  rotation->wait_for_calls(2U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto third = recovery.wait_for_generation_live(3U);
  REQUIRE_EQ(third.planned_rotation_count, 2U);
  REQUIRE_EQ(third.last_rotation_generation, 2U);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->created, (std::vector<std::uint64_t>{1U, 2U, 3U}));
    REQUIRE_EQ(state->max_active, 1U);
    REQUIRE(state->delays.empty());
  }
  recovery.stop();
}

void failure_before_deadline_uses_g5() {
  auto state = make_script({Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  inject_failure(state, runtime, 1U, g4::NetworkErrorCode::WebSocketRead);
  rotation->trigger(g3::TimedRecoveryWaitResult::RecoveryRequired);
  const auto recovered = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(recovered.planned_rotation_count, 0U);
  REQUIRE_EQ(recovered.total_recovery_count, 1U);
  REQUIRE_EQ(recovered.last_recovery_cause,
             g5::RecoveryCause::TransportFailure);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->delays,
               (std::vector<std::chrono::seconds>{std::chrono::seconds{1}}));
  }
  recovery.stop();
}

void failure_at_rotation_cut_uses_g5() {
  auto state = make_script({Action::FailOnStop, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto recovered = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(recovered.planned_rotation_count, 0U);
  REQUIRE_EQ(recovered.total_recovery_count, 1U);
  REQUIRE_EQ(recovered.last_recovery_cause,
             g5::RecoveryCause::TransportFailure);
  REQUIRE_EQ(runtime.observe().reset_count, 1U);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->delays.front(), std::chrono::seconds{1});
  }
  recovery.stop();
}

void needs_resync_at_rotation_cut_uses_g5() {
  auto state = make_script({Action::NeedsResyncOnStop, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto recovered = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(recovered.planned_rotation_count, 0U);
  REQUIRE_EQ(recovered.last_recovery_cause, g5::RecoveryCause::NeedsResync);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->delays.front(), std::chrono::seconds{1});
  }
  recovery.stop();
}

void stop_after_deadline_before_reset() {
  auto state = make_script({Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  std::promise<void> before_reset;
  auto before_reset_future = before_reset.get_future();
  std::promise<void> release_reset;
  auto release_reset_future = release_reset.get_future().share();
  std::promise<void> shutdown_established;
  auto shutdown_future = shutdown_established.get_future();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  auto options = script_options(state, rotation);
  options.before_planned_reset = [&] {
    before_reset.set_value();
    release_reset_future.wait();
  };
  options.lifecycle_shutdown_established = [&] {
    shutdown_established.set_value();
  };
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            std::move(options)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  before_reset_future.wait();
  std::thread stopper{[&] { recovery.stop(); }};
  shutdown_future.wait();
  release_reset.set_value();
  stopper.join();
  REQUIRE_EQ(runtime.observe().reset_count, 0U);
  REQUIRE_EQ(recovery.observe().planned_rotation_count, 0U);
  std::lock_guard lock{state->mutex};
  REQUIRE_EQ(state->created, (std::vector<std::uint64_t>{1U}));
}

void stop_during_new_generation_start() {
  auto state = make_script({Action::Live, Action::BlockStart});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  {
    std::unique_lock lock{state->mutex};
    state->condition.wait(lock, [&] { return state->started.size() >= 2U; });
  }
  recovery.stop();
  std::lock_guard lock{state->mutex};
  REQUIRE_EQ(state->active, 0U);
  REQUIRE_EQ(state->max_active, 1U);
  REQUIRE_EQ(state->stopped, (std::vector<std::uint64_t>{1U, 2U}));
}

void new_generation_failure_uses_g5_backoff() {
  auto state = make_script({Action::Live, Action::StartFailure, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto third = recovery.wait_for_generation_live(3U);
  REQUIRE_EQ(third.planned_rotation_count, 1U);
  REQUIRE_EQ(third.total_recovery_count, 1U);
  REQUIRE_EQ(third.last_recovery_cause, g5::RecoveryCause::TransportFailure);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->delays,
               (std::vector<std::chrono::seconds>{std::chrono::seconds{1}}));
  }
  recovery.stop();
}

void server_shutdown_remains_g5() {
  auto state = make_script({Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  inject_failure(state, runtime, 1U, g4::NetworkErrorCode::ServerShutdown);
  rotation->trigger(g3::TimedRecoveryWaitResult::RecoveryRequired);
  const auto recovered = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(recovered.last_recovery_cause, g5::RecoveryCause::ServerShutdown);
  REQUIRE_EQ(recovered.planned_rotation_count, 0U);
  recovery.stop();
}

void production_policy_is_frozen() {
  REQUIRE_EQ(g6::kPlannedRotationAge,
             std::chrono::hours{23} + std::chrono::minutes{50});
  REQUIRE_EQ(g6::production_policy().age,
             std::chrono::duration_cast<std::chrono::nanoseconds>(
                 std::chrono::hours{23} + std::chrono::minutes{50}));
}

void monotonic_clock_failure_quiesces_attempt() {
  auto state = make_script({Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  std::atomic<unsigned> calls{0U};
  g3::RuntimeClock regressing_clock = [&] {
    const auto call = calls.fetch_add(1U);
    return g3::ClockSample{1700000000123456000ULL, call == 0U ? 1000U : 999U};
  };
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, std::move(regressing_clock),
                            short_policy(), script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto terminal = recovery.wait_until_terminal();
  REQUIRE(terminal.terminal);
  REQUIRE_EQ(terminal.last_recovery_cause, g5::RecoveryCause::InternalFailure);
  REQUIRE_EQ(terminal.active_transport_count, 0U);
  {
    std::lock_guard lock{state->mutex};
    REQUIRE_EQ(state->created, (std::vector<std::uint64_t>{1U}));
    REQUIRE_EQ(state->stopped, (std::vector<std::uint64_t>{1U}));
    REQUIRE_EQ(state->active, 0U);
  }
  recovery.stop();
}

void event_generation_planned_rotation_two_phase_cut() {
  auto state = make_script({Action::Live, Action::Live});
  auto rotation = std::make_shared<RotationGate>();
  auto lifecycle_steps = std::make_shared<std::vector<std::string>>();
  auto lifecycle_mutex = std::make_shared<std::mutex>();
  g5::RecoveryOptions recovery_options;
  recovery_options.source_generation_lifecycle.open =
      [lifecycle_steps, lifecycle_mutex](std::uint64_t generation) {
        std::lock_guard lock{*lifecycle_mutex};
        lifecycle_steps->push_back("open-" + std::to_string(generation));
        return true;
      };
  recovery_options.source_generation_lifecycle.quiesce =
      [lifecycle_steps, lifecycle_mutex](std::uint64_t generation) {
        std::lock_guard lock{*lifecycle_mutex};
        lifecycle_steps->push_back("quiesce-" + std::to_string(generation));
        return true;
      };
  recovery_options.source_generation_lifecycle.close =
      [lifecycle_steps, lifecycle_mutex](
          std::uint64_t generation, g5::SourceGenerationCloseOutcome outcome) {
        std::lock_guard lock{*lifecycle_mutex};
        lifecycle_steps->push_back(
            std::string{
                outcome == g5::SourceGenerationCloseOutcome::Replacement
                    ? "replacement-"
                : outcome == g5::SourceGenerationCloseOutcome::PermanentFailure
                    ? "permanent-"
                    : "shutdown-"} +
            std::to_string(generation));
        return true;
      };

  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), short_policy(),
                            std::move(recovery_options),
                            script_options(state, rotation)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  rotation->wait_for_calls(1U);
  rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto second = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(second.planned_rotation_count, 1U);
  REQUIRE_EQ(second.total_recovery_count, 0U);
  REQUIRE_EQ(second.max_active_transport_count, 1U);
  recovery.stop();
  std::lock_guard lock{*lifecycle_mutex};
  REQUIRE_EQ(*lifecycle_steps,
             (std::vector<std::string>{"open-1", "quiesce-1", "replacement-1",
                                       "open-2", "quiesce-2", "shutdown-2"}));
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"PRODUCTION_POLICY_23H50M", production_policy_is_frozen},
      {"NO_EARLY_ROTATION_AND_STOP_BEFORE_DEADLINE",
       no_early_rotation_and_stop_before_deadline},
      {"PLANNED_ROTATION_BREAK_BEFORE_MAKE_OWNER_RESET_COUNTERS",
       planned_rotation_order_owner_and_counters},
      {"REPEATED_ROTATION", repeated_rotation},
      {"FAILURE_BEFORE_DEADLINE_USES_G5", failure_before_deadline_uses_g5},
      {"FAILURE_AT_ROTATION_CUT_USES_G5", failure_at_rotation_cut_uses_g5},
      {"NEEDS_RESYNC_AT_ROTATION_CUT_USES_G5",
       needs_resync_at_rotation_cut_uses_g5},
      {"STOP_AFTER_DEADLINE_BEFORE_RESET", stop_after_deadline_before_reset},
      {"STOP_DURING_NEW_GENERATION_START", stop_during_new_generation_start},
      {"NEW_GENERATION_FAILURE_USES_G5_BACKOFF",
       new_generation_failure_uses_g5_backoff},
      {"SERVER_SHUTDOWN_REMAINS_G5", server_shutdown_remains_g5},
      {"MONOTONIC_CLOCK_FAILURE_QUIESCES_ATTEMPT",
       monotonic_clock_failure_quiesces_attempt},
      {"EVENT_GENERATION_PLANNED_ROTATION_TWO_PHASE_CUT",
       event_generation_planned_rotation_two_phase_cut},
  };

  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &error) {
      std::cerr << name << "=FAIL " << error.what() << '\n';
      return 1;
    }
  }
  return 0;
}
