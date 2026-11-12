#include "recovery/spot_recovery.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>
namespace transport = binance_market_data::gateway::transport;
namespace recovery = binance_market_data::gateway::recovery;
namespace rt = binance_market_data::gateway::runtime;

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
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

[[nodiscard]] rt::RuntimeClock fixed_clock() {
  return
      [] { return rt::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
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
                                                   std::uint64_t generation,
                                                   bool wrong_symbol = false) {
  market_wire::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol(wrong_symbol ? "ETHUSDT" : "BTCUSDT");
  metadata->set_producer("gateway-g5-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g5-script-g" + std::to_string(generation));
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
  snapshot.set_producer("gateway-g5-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g5-snapshot-g" + std::to_string(generation));
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
  LiveWithTerminalBeforeRuntimeFault,
  LiveWithFailureOnStop,
  TransportFailure,
  SnapshotFailure,
  ProtocolFailure,
  NeedsResync,
  IngressOverflow,
  BootstrapOverflow,
  AdapterFailure,
  Http429,
  Http429Long,
  Http429Missing,
  Http429Malformed,
  Http418,
  Http418Overflow,
  Http5xx,
  Http403,
  Http404,
};

struct ScriptState final {
  std::mutex mutex;
  std::vector<Action> actions;
  std::vector<std::uint64_t> created_generations;
  std::vector<std::uint64_t> started_generations;
  std::vector<std::uint64_t> stopped_generations;
  std::vector<std::uint64_t> reset_count_at_creation;
  std::vector<std::chrono::seconds> delays;
  std::size_t active{0U};
  std::size_t max_active{0U};
};

class ScriptAttempt final : public recovery::detail::RecoveryAttempt {
public:
  ScriptAttempt(rt::MarketRuntime &runtime, std::uint64_t generation,
                Action action, std::shared_ptr<ScriptState> state)
      : runtime_{runtime}, generation_{generation}, action_{action},
        state_{std::move(state)} {
    observation_.connection_generation = generation_;
    observation_.connection_id =
        "script-g" + std::to_string(generation_) + "-same-clock";
    std::lock_guard lock{state_->mutex};
    state_->created_generations.push_back(generation_);
    state_->reset_count_at_creation.push_back(runtime_.observe().reset_count);
    ++state_->active;
    state_->max_active = std::max(state_->max_active, state_->active);
  }

  ~ScriptAttempt() override { stop(); }

  [[nodiscard]] transport::TransportStartResult start() override {
    std::lock_guard observation_lock{observation_mutex_};
    {
      std::lock_guard state_lock{state_->mutex};
      state_->started_generations.push_back(generation_);
    }
    observation_.started = true;
    observation_.running = true;
    observation_.tls_verified = true;
    observation_.websocket_handshake = true;
    const auto base = generation_ * 100U;
    switch (action_) {
    case Action::Live:
    case Action::LiveWithFailureOnStop:
      bootstrap(base);
      return transport::TransportStartResult::Started;
    case Action::LiveWithTerminalBeforeRuntimeFault:
      bootstrap(base);
      static_cast<void>(runtime_.observe());
      set_error(transport::NetworkErrorCode::WebSocketRead, std::nullopt,
                std::nullopt);
      return transport::TransportStartResult::Started;
    case Action::NeedsResync:
      bootstrap(base);
      static_cast<void>(runtime_.observe());
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 3U, base + 3U, generation_)));
      return transport::TransportStartResult::Started;
    case Action::IngressOverflow:
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 1U, base + 1U, generation_)));
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 2U, base + 2U, generation_)));
      runtime_.release_owner_for_testing();
      set_error(transport::NetworkErrorCode::RuntimeAdmission, std::nullopt,
                std::nullopt);
      return transport::TransportStartResult::Failed;
    case Action::BootstrapOverflow:
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 1U, base + 1U, generation_)));
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 2U, base + 2U, generation_)));
      set_error(transport::NetworkErrorCode::RuntimeAdmission, std::nullopt,
                std::nullopt);
      return transport::TransportStartResult::Started;
    case Action::AdapterFailure:
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 1U, base + 1U, generation_, true)));
      static_cast<void>(
          runtime_.submit_snapshot(make_snapshot(base, generation_)));
      return transport::TransportStartResult::Started;
    case Action::SnapshotFailure:
      static_cast<void>(runtime_.submit_snapshot_failure());
      set_error(transport::NetworkErrorCode::HttpRead, std::nullopt,
                std::nullopt);
      return transport::TransportStartResult::Failed;
    case Action::ProtocolFailure:
      static_cast<void>(runtime_.submit_transport_failure());
      set_error(transport::NetworkErrorCode::Protocol, std::nullopt,
                std::nullopt);
      return transport::TransportStartResult::Failed;
    case Action::Http429:
      return http_failure(429U, std::string{"17"});
    case Action::Http429Long:
      return http_failure(429U, std::string{"1234"});
    case Action::Http429Missing:
      return http_failure(429U, std::nullopt);
    case Action::Http429Malformed:
      return http_failure(429U, std::string{"1.5"});
    case Action::Http418:
      return http_failure(418U, std::string{"19"});
    case Action::Http418Overflow:
      return http_failure(418U, std::string{"184467440737095516160"});
    case Action::Http5xx:
      return http_failure(503U, std::nullopt);
    case Action::Http403:
      return http_failure(403U, std::nullopt);
    case Action::Http404:
      return http_failure(404U, std::nullopt);
    case Action::TransportFailure:
      static_cast<void>(runtime_.submit_transport_failure());
      set_error(transport::NetworkErrorCode::WebSocketRead, std::nullopt,
                std::nullopt);
      return transport::TransportStartResult::Failed;
    }
    return transport::TransportStartResult::Failed;
  }

  void stop() noexcept override {
    if (stopped_.exchange(true)) {
      return;
    }
    {
      std::lock_guard observation_lock{observation_mutex_};
      if (action_ == Action::LiveWithFailureOnStop) {
        set_error(transport::NetworkErrorCode::WebSocketRead, std::nullopt,
                  std::nullopt);
      }
      observation_.stopped = true;
      observation_.running = false;
    }
    if (action_ == Action::LiveWithFailureOnStop ||
        action_ == Action::LiveWithTerminalBeforeRuntimeFault) {
      static_cast<void>(runtime_.submit_transport_failure());
    }
    std::lock_guard lock{state_->mutex};
    state_->stopped_generations.push_back(generation_);
    --state_->active;
  }

  [[nodiscard]] transport::TransportObservation observe() const override {
    std::lock_guard observation_lock{observation_mutex_};
    return observation_;
  }

private:
  void bootstrap(std::uint64_t base) {
    static_cast<void>(runtime_.submit_depth_update(
        make_update(base + 1U, base + 1U, generation_)));
    static_cast<void>(runtime_.observe());
    static_cast<void>(
        runtime_.submit_snapshot(make_snapshot(base, generation_)));
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
  }

  [[nodiscard]] transport::TransportStartResult
  http_failure(unsigned status, std::optional<std::string> retry_after) {
    static_cast<void>(runtime_.submit_snapshot_failure());
    set_error(transport::NetworkErrorCode::HttpStatus, status,
              std::move(retry_after));
    return transport::TransportStartResult::Failed;
  }

  void set_error(transport::NetworkErrorCode code,
                 std::optional<unsigned> status,
                 std::optional<std::string> retry_after) {
    observation_.terminal_error = transport::NetworkError{
        code, "script", "scripted failure", status, std::move(retry_after)};
    observation_.running = false;
  }

  rt::MarketRuntime &runtime_;
  std::uint64_t generation_;
  Action action_;
  std::shared_ptr<ScriptState> state_;
  transport::TransportObservation observation_;
  mutable std::mutex observation_mutex_;
  std::atomic<bool> stopped_{false};
};

[[nodiscard]] recovery::detail::RecoveryTestOptions
script_options(const std::shared_ptr<ScriptState> &state) {
  recovery::detail::RecoveryTestOptions options;
  options.attempt_factory = [state](rt::MarketRuntime &runtime,
                                    const rt::RuntimeClock &,
                                    std::uint64_t generation)
      -> std::unique_ptr<recovery::detail::RecoveryAttempt> {
    Action action = Action::TransportFailure;
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
  return options;
}

[[nodiscard]] std::shared_ptr<ScriptState>
make_script(std::initializer_list<Action> actions) {
  auto state = std::make_shared<ScriptState>();
  state->actions.assign(actions);
  return state;
}

void initial_live() {
  static_assert(recovery::detail::kRecoveryBackoffStages == 6U);
  static_assert(recovery::detail::kRecoveryFailureHistoryCapacity == 7U);
  auto script = make_script({Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(1U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live.connection_generation, 1U);
  REQUIRE_EQ(live.total_recovery_count, 0U);
  REQUIRE_EQ(live.max_active_transport_count, 1U);
  recovery.stop();
}

void failure_diagnostic_survives_live_replacement() {
  auto script = make_script({Action::Http429, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE(!live.terminal_error.has_value());
  REQUIRE_EQ(live.failure_history_size, 1U);
  const auto &failure = live.failure_history[0];
  REQUIRE_EQ(failure.connection_generation, 1U);
  REQUIRE_EQ(failure.cause, recovery::RecoveryCause::Http429);
  REQUIRE_EQ(failure.runtime_state, rt::RuntimeState::Faulted);
  REQUIRE_EQ(failure.fault_reason, rt::FaultReason::SnapshotFailure);
  REQUIRE(failure.network_error.has_value());
  REQUIRE_EQ(failure.network_error->code,
             transport::NetworkErrorCode::HttpStatus);
  REQUIRE_EQ(failure.network_error->stage, std::string{"script"});
  REQUIRE_EQ(failure.network_error->message, std::string{"scripted failure"});
  REQUIRE_EQ(failure.network_error->http_status, std::optional<unsigned>{429U});
  REQUIRE_EQ(failure.network_error->retry_after,
             std::optional<std::string>{"17"});
  recovery.stop();
}

void two_failure_diagnostics_survive_generation_three() {
  auto script =
      make_script({Action::ProtocolFailure, Action::Http5xx, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(3U);
  REQUIRE_EQ(live.failure_history_size, 2U);
  REQUIRE_EQ(live.failure_history[0].connection_generation, 1U);
  REQUIRE_EQ(live.failure_history[0].cause, recovery::RecoveryCause::Protocol);
  REQUIRE(live.failure_history[0].network_error.has_value());
  REQUIRE_EQ(live.failure_history[0].network_error->code,
             transport::NetworkErrorCode::Protocol);
  REQUIRE_EQ(live.failure_history[1].connection_generation, 2U);
  REQUIRE_EQ(live.failure_history[1].cause, recovery::RecoveryCause::Http5xx);
  REQUIRE(live.failure_history[1].network_error.has_value());
  REQUIRE_EQ(live.failure_history[1].network_error->code,
             transport::NetworkErrorCode::HttpStatus);
  REQUIRE_EQ(live.failure_history[1].network_error->http_status,
             std::optional<unsigned>{503U});
  recovery.stop();
}

void failure_diagnostic_history_retains_newest_seven() {
  auto script = make_script({Action::Live, Action::Live, Action::Live,
                             Action::Live, Action::Live, Action::Live,
                             Action::Live, Action::Live, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  for (std::uint64_t failed_generation = 1U; failed_generation <= 8U;
       ++failed_generation) {
    REQUIRE(recovery.request_controlled_recovery_for_acceptance());
    const auto live = recovery.wait_for_generation_live(failed_generation + 1U);
    REQUIRE(live.failure_history_size <=
            recovery::detail::kRecoveryFailureHistoryCapacity);
  }
  const auto retained = recovery.observe();
  REQUIRE_EQ(retained.failure_history_size,
             recovery::detail::kRecoveryFailureHistoryCapacity);
  for (std::size_t index = 0U; index < retained.failure_history_size; ++index) {
    REQUIRE_EQ(retained.failure_history[index].connection_generation,
               static_cast<std::uint64_t>(index + 2U));
    REQUIRE_EQ(retained.failure_history[index].cause,
               recovery::RecoveryCause::TransportFailure);
  }
  recovery.stop();
}

void nonrecoverable_failure_diagnostic_is_retained() {
  auto script = make_script({Action::Http403});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto terminal = recovery.wait_until_terminal();
  REQUIRE(terminal.terminal);
  REQUIRE_EQ(terminal.failure_history_size, 1U);
  const auto &failure = terminal.failure_history[0];
  REQUIRE_EQ(failure.connection_generation, 1U);
  REQUIRE_EQ(failure.cause, recovery::RecoveryCause::TerminalHttp4xx);
  REQUIRE(failure.network_error.has_value());
  REQUIRE_EQ(failure.network_error->http_status, std::optional<unsigned>{403U});
  recovery.stop();
}

void runtime_only_adapter_failure_diagnostic_is_retained() {
  auto script = make_script({Action::AdapterFailure});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto terminal = recovery.wait_until_terminal();
  REQUIRE(terminal.terminal);
  REQUIRE_EQ(terminal.failure_history_size, 1U);
  const auto &failure = terminal.failure_history[0];
  REQUIRE_EQ(failure.cause, recovery::RecoveryCause::InternalFailure);
  REQUIRE_EQ(failure.runtime_state, rt::RuntimeState::Faulted);
  REQUIRE_EQ(failure.fault_reason, rt::FaultReason::AdapterError);
  REQUIRE(!failure.network_error.has_value());
  REQUIRE(failure.adapter_error.has_value());
  REQUIRE_EQ(failure.adapter_error->code,
             rt::adapter::AdapterErrorCode::IdentityMismatch);
  REQUIRE_EQ(failure.adapter_error->field, rt::adapter::AdapterField::Symbol);
  recovery.stop();
}

void transport_failure_recovery_and_cut() {
  auto script = make_script({Action::Live, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto first = recovery.wait_for_generation_live(1U);
  REQUIRE(recovery.request_controlled_recovery_for_acceptance());
  const auto second = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(second.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(second.connection_generation, 2U);
  REQUIRE(first.connection_id != second.connection_id);
  REQUIRE_EQ(second.total_recovery_count, 1U);
  const auto runtime_observation = runtime.observe();
  REQUIRE_EQ(runtime_observation.reset_count, 1U);
  REQUIRE_EQ(runtime_observation.last_reset_thread_id,
             runtime_observation.owner_thread_id);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->max_active, 1U);
    REQUIRE_EQ(script->stopped_generations.front(), 1U);
    REQUIRE_EQ(script->reset_count_at_creation.at(1), 1U);
  }
  recovery.stop();
}

void needs_resync_direct_recovery() {
  auto script = make_script({Action::NeedsResync, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live.last_recovery_cause, recovery::RecoveryCause::NeedsResync);
  REQUIRE_EQ(live.failure_history_size, 1U);
  REQUIRE_EQ(live.failure_history[0].projection_status,
             core::ProjectionStatus::NeedsResync);
  REQUIRE(live.failure_history[0].last_gap.has_value());
  REQUIRE_EQ(runtime.observe().reset_count, 1U);
  recovery.stop();
}

void recoverable_failure_matrix() {
  const std::vector<std::pair<Action, recovery::RecoveryCause>> cases{
      {Action::SnapshotFailure, recovery::RecoveryCause::SnapshotFailure},
      {Action::ProtocolFailure, recovery::RecoveryCause::Protocol},
      {Action::BootstrapOverflow,
       recovery::RecoveryCause::BootstrapBufferOverflow},
      {Action::Http5xx, recovery::RecoveryCause::Http5xx},
  };
  for (const auto &[action, cause] : cases) {
    auto script = make_script({action, Action::Live});
    rt::MarketRuntime runtime{{8U, 1U}, fixed_clock(), numeric_spec()};
    recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                    script_options(script)};
    REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
    const auto live = recovery.wait_for_generation_live(2U);
    REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
    REQUIRE_EQ(live.last_recovery_cause, cause);
    REQUIRE_EQ(live.total_recovery_count, 1U);
    recovery.stop();
  }
}

void ingress_overflow_recovery() {
  auto script = make_script({Action::IngressOverflow, Action::Live});
  rt::MarketRuntime runtime{{1U, 8U}, fixed_clock(), numeric_spec(), {true}};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live.last_recovery_cause,
             recovery::RecoveryCause::IngressOverflow);
  recovery.stop();
}

void persistent_recoverable_failures_recover_after_seven() {
  auto script = make_script({Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(8U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live.connection_generation, 8U);
  REQUIRE_EQ(live.total_recovery_count, 7U);
  REQUIRE_EQ(live.consecutive_recovery_attempts, 0U);
  REQUIRE_EQ(live.failure_history_size,
             recovery::detail::kRecoveryFailureHistoryCapacity);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->delays,
               (std::vector<std::chrono::seconds>{
                   std::chrono::seconds{1}, std::chrono::seconds{2},
                   std::chrono::seconds{4}, std::chrono::seconds{8},
                   std::chrono::seconds{16}, std::chrono::seconds{30},
                   std::chrono::seconds{30}}));
    REQUIRE_EQ(script->created_generations,
               (std::vector<std::uint64_t>{1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U}));
    REQUIRE_EQ(script->stopped_generations,
               (std::vector<std::uint64_t>{1U, 2U, 3U, 4U, 5U, 6U, 7U}));
    REQUIRE_EQ(script->max_active, 1U);
  }
  REQUIRE_EQ(recovery::detail::normal_backoff_delay(7U),
             std::chrono::seconds{30});
  REQUIRE_EQ(recovery::detail::normal_backoff_delay(100U),
             std::chrono::seconds{30});
  recovery.stop();
}

void stop_during_long_retry_after_backoff() {
  auto script = make_script({Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::Http429Long,
                             Action::Live});
  std::mutex wait_mutex;
  std::condition_variable_any wait_condition;
  std::vector<std::chrono::seconds> delays;
  bool entered_long_wait = false;
  auto options = script_options(script);
  options.backoff_waiter = [&](std::chrono::seconds delay,
                               std::stop_token stop_token) {
    {
      std::lock_guard lock{wait_mutex};
      delays.push_back(delay);
      if (delay < std::chrono::seconds{1234}) {
        return !stop_token.stop_requested();
      }
      entered_long_wait = true;
    }
    wait_condition.notify_all();
    std::unique_lock lock{wait_mutex};
    if (!stop_token.stop_requested()) {
      static_cast<void>(
          wait_condition.wait(lock, stop_token, [] { return false; }));
    }
    return false;
  };
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(), std::move(options)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  {
    std::unique_lock lock{wait_mutex};
    wait_condition.wait(lock, [&] { return entered_long_wait; });
  }
  recovery.stop();
  REQUIRE_EQ(recovery.observe().state, recovery::RecoveryState::Stopped);
  {
    std::lock_guard lock{wait_mutex};
    REQUIRE_EQ(delays,
               (std::vector<std::chrono::seconds>{
                   std::chrono::seconds{1}, std::chrono::seconds{2},
                   std::chrono::seconds{4}, std::chrono::seconds{8},
                   std::chrono::seconds{16}, std::chrono::seconds{30},
                   std::chrono::seconds{30}, std::chrono::seconds{1234}}));
  }
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->created_generations,
               (std::vector<std::uint64_t>{1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U}));
  }
}

void success_resets_incident_counter() {
  auto script = make_script({Action::TransportFailure, Action::TransportFailure,
                             Action::Live, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(3U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live.consecutive_recovery_attempts, 0U);
  REQUIRE(recovery.request_controlled_recovery_for_acceptance());
  const auto live_again = recovery.wait_for_generation_live(4U);
  REQUIRE_EQ(live_again.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live_again.consecutive_recovery_attempts, 0U);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->delays,
               (std::vector<std::chrono::seconds>{std::chrono::seconds{1},
                                                  std::chrono::seconds{2},
                                                  std::chrono::seconds{1}}));
  }
  recovery.stop();
}

void stale_live_does_not_reset_incident_counter() {
  auto script =
      make_script({Action::TransportFailure,
                   Action::LiveWithTerminalBeforeRuntimeFault, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
  REQUIRE_EQ(live.connection_generation, 3U);
  REQUIRE_EQ(live.consecutive_recovery_attempts, 0U);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->delays,
               (std::vector<std::chrono::seconds>{std::chrono::seconds{1},
                                                  std::chrono::seconds{2}}));
    REQUIRE_EQ(script->created_generations,
               (std::vector<std::uint64_t>{1U, 2U, 3U}));
  }
  recovery.stop();
}

void create_vs_ordinary_stop_quiesces_factory_attempt() {
  auto script = make_script({Action::Live});
  std::promise<void> factory_entered;
  auto factory_entered_future = factory_entered.get_future();
  std::promise<void> release_factory;
  auto release_factory_future = release_factory.get_future().share();
  std::promise<void> shutdown_established;
  auto shutdown_established_future = shutdown_established.get_future();

  auto options = script_options(script);
  options.attempt_factory = [script, &factory_entered,
                             release_factory_future](rt::MarketRuntime &runtime,
                                                     const rt::RuntimeClock &,
                                                     std::uint64_t generation)
      -> std::unique_ptr<recovery::detail::RecoveryAttempt> {
    factory_entered.set_value();
    release_factory_future.wait();
    return std::make_unique<ScriptAttempt>(runtime, generation, Action::Live,
                                           script);
  };
  options.lifecycle_shutdown_established = [&shutdown_established] {
    shutdown_established.set_value();
  };

  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(), std::move(options)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  factory_entered_future.wait();

  std::thread stopper{[&recovery] { recovery.stop(); }};
  shutdown_established_future.wait();
  release_factory.set_value();
  stopper.join();

  const auto stopped = recovery.observe();
  REQUIRE_EQ(stopped.state, recovery::RecoveryState::Stopped);
  REQUIRE_EQ(stopped.active_transport_count, 0U);
  REQUIRE_EQ(runtime.observe().state, rt::RuntimeState::Stopped);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->created_generations, (std::vector<std::uint64_t>{1U}));
    REQUIRE(script->started_generations.empty());
    REQUIRE_EQ(script->stopped_generations, (std::vector<std::uint64_t>{1U}));
    REQUIRE_EQ(script->active, 0U);
  }
}

void acceptance_gate_wins_before_backoff_commit() {
  auto script = make_script({Action::Live, Action::Live});
  std::promise<void> before_backoff;
  auto before_backoff_future = before_backoff.get_future();
  std::promise<void> release_backoff;
  auto release_backoff_future = release_backoff.get_future().share();
  std::promise<void> shutdown_established;
  auto shutdown_established_future = shutdown_established.get_future();

  auto options = script_options(script);
  options.before_backoff_commit = [&before_backoff, release_backoff_future] {
    before_backoff.set_value();
    release_backoff_future.wait();
  };
  options.lifecycle_shutdown_established = [&shutdown_established] {
    shutdown_established.set_value();
  };

  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(), std::move(options)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  REQUIRE_EQ(recovery.wait_for_generation_live(1U).state,
             recovery::RecoveryState::Live);
  REQUIRE(recovery.request_controlled_recovery_for_acceptance());
  before_backoff_future.wait();

  std::optional<recovery::QuiescentAcceptanceCut> cut;
  std::thread accepter{[&] { cut = recovery.quiesce_for_acceptance(); }};
  shutdown_established_future.wait();
  release_backoff.set_value();
  accepter.join();

  REQUIRE(!cut.has_value());
  REQUIRE_EQ(recovery.observe().state, recovery::RecoveryState::Stopping);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->created_generations, (std::vector<std::uint64_t>{1U}));
    REQUIRE_EQ(script->stopped_generations, (std::vector<std::uint64_t>{1U}));
    REQUIRE(script->delays.empty());
    REQUIRE_EQ(script->active, 0U);
  }

  recovery.stop();
  REQUIRE_EQ(recovery.observe().state, recovery::RecoveryState::Stopped);
  REQUIRE_EQ(runtime.observe().state, rt::RuntimeState::Stopped);
}

void final_acceptance_failure_race_is_rejected() {
  auto script = make_script({Action::LiveWithFailureOnStop});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  REQUIRE_EQ(recovery.wait_for_generation_live(1U).state,
             recovery::RecoveryState::Live);

  const auto cut = recovery.quiesce_for_acceptance();
  REQUIRE(cut.has_value());
  REQUIRE(cut->transport.stopped);
  REQUIRE(!cut->transport.running);
  REQUIRE(cut->transport.terminal_error.has_value());
  REQUIRE_EQ(cut->runtime.state, rt::RuntimeState::Faulted);
  REQUIRE_EQ(cut->runtime.fault_reason, rt::FaultReason::TransportFailure);
  recovery.stop();
}

void final_acceptance_clean_cut_and_full_stop() {
  auto script = make_script({Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  REQUIRE_EQ(recovery.wait_for_generation_live(1U).state,
             recovery::RecoveryState::Live);

  const auto cut = recovery.quiesce_for_acceptance();
  REQUIRE(cut.has_value());
  REQUIRE(cut->transport.stopped);
  REQUIRE(!cut->transport.running);
  REQUIRE(!cut->transport.terminal_error.has_value());
  REQUIRE(cut->transport.websocket_handshake);
  REQUIRE(cut->transport.rest_depth_fetched);
  REQUIRE_EQ(cut->transport.connection_generation, 1U);
  REQUIRE_EQ(cut->runtime.state, rt::RuntimeState::Live);
  REQUIRE_EQ(cut->runtime.projection_status,
             core::ProjectionStatus::Synchronized);
  REQUIRE(!cut->runtime.fault_reason.has_value());
  REQUIRE(cut->runtime.last_update_id.has_value());
  REQUIRE(
      std::holds_alternative<rt::CapturedSnapshot>(runtime.capture_snapshot()));

  recovery.stop();
  REQUIRE_EQ(recovery.observe().state, recovery::RecoveryState::Stopped);
  REQUIRE_EQ(runtime.observe().state, rt::RuntimeState::Stopped);
}

void rate_limit_policy() {
  for (const auto &[action, expected_delay, expected_cause] : std::vector<
           std::tuple<Action, std::chrono::seconds, recovery::RecoveryCause>>{
           {Action::Http429, std::chrono::seconds{17},
            recovery::RecoveryCause::Http429},
           {Action::Http418, std::chrono::seconds{19},
            recovery::RecoveryCause::Http418}}) {
    auto script = make_script({action, Action::Live});
    rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                    script_options(script)};
    REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
    const auto live = recovery.wait_for_generation_live(2U);
    REQUIRE_EQ(live.state, recovery::RecoveryState::Live);
    REQUIRE_EQ(live.last_recovery_cause, expected_cause);
    {
      std::lock_guard lock{script->mutex};
      REQUIRE_EQ(script->delays.front(), expected_delay);
    }
    recovery.stop();
  }

  REQUIRE_EQ(recovery::detail::parse_retry_after(std::string{"0"}),
             std::chrono::seconds{0});
  REQUIRE(!recovery::detail::parse_retry_after(std::nullopt).has_value());
  REQUIRE(!recovery::detail::parse_retry_after(std::string{}).has_value());
  REQUIRE(!recovery::detail::parse_retry_after(std::string{"-1"}).has_value());
  REQUIRE(!recovery::detail::parse_retry_after(std::string{"1.5"}).has_value());
  REQUIRE(
      !recovery::detail::parse_retry_after(std::string{"184467440737095516160"})
           .has_value());
}

void terminal_http_and_bad_retry_after() {
  for (const auto action : {Action::Http429Missing, Action::Http429Malformed,
                            Action::Http418Overflow, Action::Http403,
                            Action::Http404, Action::AdapterFailure}) {
    auto script = make_script({action, Action::Live});
    rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                    script_options(script)};
    REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
    const auto terminal = recovery.wait_until_terminal();
    REQUIRE(terminal.terminal);
    REQUIRE(!terminal.exhausted);
    {
      std::lock_guard lock{script->mutex};
      REQUIRE_EQ(script->created_generations.size(), 1U);
      REQUIRE(script->delays.empty());
    }
    recovery.stop();
  }
}

void stop_during_backoff() {
  auto script = make_script({Action::TransportFailure, Action::Live});
  std::promise<void> entered;
  auto entered_future = entered.get_future();
  std::atomic<bool> signalled{false};
  auto options = script_options(script);
  options.backoff_waiter = [&entered, &signalled](std::chrono::seconds,
                                                  std::stop_token token) {
    if (!signalled.exchange(true)) {
      entered.set_value();
    }
    std::mutex mutex;
    std::condition_variable_any condition;
    std::unique_lock lock{mutex};
    static_cast<void>(condition.wait(lock, token, [] { return false; }));
    return false;
  };
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(), std::move(options)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  entered_future.wait();
  recovery.stop();
  const auto stopped = recovery.observe();
  REQUIRE_EQ(stopped.state, recovery::RecoveryState::Stopped);
  std::lock_guard lock{script->mutex};
  REQUIRE_EQ(script->created_generations.size(), 1U);
}

void stop_during_active_connection() {
  auto script = make_script({Action::Live, Action::Live});
  rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                  script_options(script)};
  REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  recovery.stop();
  const auto stopped = recovery.observe();
  REQUIRE_EQ(stopped.state, recovery::RecoveryState::Stopped);
  REQUIRE_EQ(stopped.total_recovery_count, 0U);
  std::lock_guard lock{script->mutex};
  REQUIRE_EQ(script->created_generations.size(), 1U);
  REQUIRE_EQ(script->active, 0U);
}

struct SourceLifecycleTrace final {
  std::mutex mutex;
  std::vector<std::string> steps;
};

[[nodiscard]] recovery::RecoveryOptions
lifecycle_options(const std::shared_ptr<SourceLifecycleTrace> &trace) {
  recovery::RecoveryOptions options;
  options.source_generation_lifecycle.open = [trace](std::uint64_t generation) {
    std::lock_guard lock{trace->mutex};
    trace->steps.push_back("open-" + std::to_string(generation));
    return true;
  };
  options.source_generation_lifecycle.quiesce =
      [trace](std::uint64_t generation) {
        std::lock_guard lock{trace->mutex};
        trace->steps.push_back("quiesce-" + std::to_string(generation));
        return true;
      };
  options.source_generation_lifecycle.close =
      [trace](std::uint64_t generation,
              recovery::SourceGenerationCloseOutcome outcome) {
        const auto name =
            outcome == recovery::SourceGenerationCloseOutcome::Replacement
                ? "replacement-"
            : outcome ==
                    recovery::SourceGenerationCloseOutcome::PermanentFailure
                ? "permanent-"
                : "shutdown-";
        std::lock_guard lock{trace->mutex};
        trace->steps.push_back(name + std::to_string(generation));
        return true;
      };
  return options;
}

void event_generation_two_phase_recovery_and_permanent_cut() {
  {
    auto script = make_script({Action::Live, Action::Live});
    auto trace = std::make_shared<SourceLifecycleTrace>();
    rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                    lifecycle_options(trace),
                                    script_options(script)};
    REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
    static_cast<void>(recovery.wait_for_generation_live(1U));
    REQUIRE(recovery.request_controlled_recovery_for_acceptance());
    static_cast<void>(recovery.wait_for_generation_live(2U));
    recovery.stop();
    std::lock_guard lock{trace->mutex};
    REQUIRE_EQ(trace->steps,
               (std::vector<std::string>{"open-1", "quiesce-1", "replacement-1",
                                         "open-2", "quiesce-2", "shutdown-2"}));
  }

  {
    auto script = make_script({Action::Live});
    auto trace = std::make_shared<SourceLifecycleTrace>();
    rt::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    recovery::SpotRecovery recovery{runtime, fixed_clock(),
                                    lifecycle_options(trace),
                                    script_options(script)};
    REQUIRE_EQ(recovery.start(), recovery::RecoveryStartResult::Started);
    static_cast<void>(recovery.wait_for_generation_live(1U));
    REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, 102U, 1U, true)),
               rt::AdmissionResult::Accepted);
    const auto terminal = recovery.wait_until_terminal();
    REQUIRE(terminal.terminal);
    REQUIRE_EQ(terminal.last_recovery_cause,
               recovery::RecoveryCause::InternalFailure);
    recovery.stop();
    std::lock_guard lock{trace->mutex};
    REQUIRE_EQ(trace->steps, (std::vector<std::string>{"open-1", "quiesce-1",
                                                       "permanent-1"}));
  }
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"INITIAL_LIVE", initial_live},
      {"FAILURE_DIAGNOSTIC_SURVIVES_LIVE_REPLACEMENT",
       failure_diagnostic_survives_live_replacement},
      {"TWO_FAILURE_DIAGNOSTICS_SURVIVE_GENERATION_THREE",
       two_failure_diagnostics_survive_generation_three},
      {"FAILURE_DIAGNOSTIC_HISTORY_RETAINS_NEWEST_SEVEN",
       failure_diagnostic_history_retains_newest_seven},
      {"NONRECOVERABLE_FAILURE_DIAGNOSTIC_RETAINED",
       nonrecoverable_failure_diagnostic_is_retained},
      {"RUNTIME_ONLY_ADAPTER_FAILURE_DIAGNOSTIC_RETAINED",
       runtime_only_adapter_failure_diagnostic_is_retained},
      {"TRANSPORT_FAILURE_RECOVERY_AND_OLD_GENERATION_CUT",
       transport_failure_recovery_and_cut},
      {"PROJECTION_NEEDS_RESYNC_DIRECT_RECOVERY", needs_resync_direct_recovery},
      {"RECOVERABLE_FAILURE_MATRIX", recoverable_failure_matrix},
      {"INGRESS_OVERFLOW_RECOVERY", ingress_overflow_recovery},
      {"PERSISTENT_RECOVERABLE_FAILURES_RECOVER_AFTER_SEVEN",
       persistent_recoverable_failures_recover_after_seven},
      {"STOP_DURING_LONG_RETRY_AFTER_BACKOFF",
       stop_during_long_retry_after_backoff},
      {"SUCCESS_RESETS_INCIDENT_COUNTER", success_resets_incident_counter},
      {"STALE_LIVE_DOES_NOT_RESET_INCIDENT_COUNTER",
       stale_live_does_not_reset_incident_counter},
      {"CREATE_VS_ORDINARY_STOP_QUIESCES_FACTORY_ATTEMPT",
       create_vs_ordinary_stop_quiesces_factory_attempt},
      {"ACCEPTANCE_GATE_WINS_BEFORE_BACKOFF_COMMIT",
       acceptance_gate_wins_before_backoff_commit},
      {"FINAL_ACCEPTANCE_FAILURE_RACE_REJECTED",
       final_acceptance_failure_race_is_rejected},
      {"FINAL_ACCEPTANCE_CLEAN_CUT_AND_FULL_STOP",
       final_acceptance_clean_cut_and_full_stop},
      {"HTTP_429_418_RETRY_AFTER", rate_limit_policy},
      {"BAD_RETRY_AFTER_HTTP_4XX_INTERNAL_FAIL_CLOSED",
       terminal_http_and_bad_retry_after},
      {"STOP_DURING_BACKOFF", stop_during_backoff},
      {"STOP_DURING_ACTIVE_CONNECTION", stop_during_active_connection},
      {"EVENT_GENERATION_TWO_PHASE_RECOVERY_AND_PERMANENT_CUT",
       event_generation_two_phase_recovery_and_permanent_cut},
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
