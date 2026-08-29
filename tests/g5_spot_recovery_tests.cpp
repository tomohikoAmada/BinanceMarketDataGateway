#include "spot_recovery.hpp"

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
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
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
  return
      [] { return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
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
  TransportFailure,
  SnapshotFailure,
  ProtocolFailure,
  NeedsResync,
  IngressOverflow,
  BootstrapOverflow,
  AdapterFailure,
  Http429,
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
  std::vector<std::uint64_t> stopped_generations;
  std::vector<std::uint64_t> reset_count_at_creation;
  std::vector<std::chrono::seconds> delays;
  std::size_t active{0U};
  std::size_t max_active{0U};
};

class ScriptAttempt final : public g5::detail::RecoveryAttempt {
public:
  ScriptAttempt(g3::MarketRuntime &runtime, std::uint64_t generation,
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

  [[nodiscard]] g4::TransportStartResult start() override {
    std::lock_guard observation_lock{observation_mutex_};
    observation_.started = true;
    observation_.running = true;
    observation_.tls_verified = true;
    observation_.websocket_handshake = true;
    const auto base = generation_ * 100U;
    switch (action_) {
    case Action::Live:
      bootstrap(base);
      return g4::TransportStartResult::Started;
    case Action::NeedsResync:
      bootstrap(base);
      static_cast<void>(runtime_.observe());
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 3U, base + 3U, generation_)));
      return g4::TransportStartResult::Started;
    case Action::IngressOverflow:
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 1U, base + 1U, generation_)));
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 2U, base + 2U, generation_)));
      runtime_.release_owner_for_testing();
      set_error(g4::NetworkErrorCode::RuntimeAdmission, std::nullopt,
                std::nullopt);
      return g4::TransportStartResult::Failed;
    case Action::BootstrapOverflow:
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 1U, base + 1U, generation_)));
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 2U, base + 2U, generation_)));
      set_error(g4::NetworkErrorCode::RuntimeAdmission, std::nullopt,
                std::nullopt);
      return g4::TransportStartResult::Started;
    case Action::AdapterFailure:
      static_cast<void>(runtime_.submit_depth_update(
          make_update(base + 1U, base + 1U, generation_, true)));
      static_cast<void>(
          runtime_.submit_snapshot(make_snapshot(base, generation_)));
      set_error(g4::NetworkErrorCode::RuntimeAdmission, std::nullopt,
                std::nullopt);
      return g4::TransportStartResult::Started;
    case Action::SnapshotFailure:
      static_cast<void>(runtime_.submit_snapshot_failure());
      set_error(g4::NetworkErrorCode::HttpRead, std::nullopt, std::nullopt);
      return g4::TransportStartResult::Failed;
    case Action::ProtocolFailure:
      static_cast<void>(runtime_.submit_transport_failure());
      set_error(g4::NetworkErrorCode::Protocol, std::nullopt, std::nullopt);
      return g4::TransportStartResult::Failed;
    case Action::Http429:
      return http_failure(429U, std::string{"17"});
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
      set_error(g4::NetworkErrorCode::WebSocketRead, std::nullopt,
                std::nullopt);
      return g4::TransportStartResult::Failed;
    }
    return g4::TransportStartResult::Failed;
  }

  void stop() noexcept override {
    if (stopped_.exchange(true)) {
      return;
    }
    {
      std::lock_guard observation_lock{observation_mutex_};
      observation_.stopped = true;
      observation_.running = false;
    }
    std::lock_guard lock{state_->mutex};
    state_->stopped_generations.push_back(generation_);
    --state_->active;
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
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

  [[nodiscard]] g4::TransportStartResult
  http_failure(unsigned status, std::optional<std::string> retry_after) {
    static_cast<void>(runtime_.submit_snapshot_failure());
    set_error(g4::NetworkErrorCode::HttpStatus, status, std::move(retry_after));
    return g4::TransportStartResult::Failed;
  }

  void set_error(g4::NetworkErrorCode code, std::optional<unsigned> status,
                 std::optional<std::string> retry_after) {
    observation_.terminal_error = g4::NetworkError{
        code, "script", "scripted failure", status, std::move(retry_after)};
    observation_.running = false;
  }

  g3::MarketRuntime &runtime_;
  std::uint64_t generation_;
  Action action_;
  std::shared_ptr<ScriptState> state_;
  g4::TransportObservation observation_;
  mutable std::mutex observation_mutex_;
  std::atomic<bool> stopped_{false};
};

[[nodiscard]] g5::detail::RecoveryTestOptions
script_options(const std::shared_ptr<ScriptState> &state) {
  g5::detail::RecoveryTestOptions options;
  options.attempt_factory = [state](g3::MarketRuntime &runtime,
                                    const g3::RuntimeClock &,
                                    std::uint64_t generation)
      -> std::unique_ptr<g5::detail::RecoveryAttempt> {
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
  auto script = make_script({Action::Live});
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(1U);
  REQUIRE_EQ(live.state, g5::RecoveryState::Live);
  REQUIRE_EQ(live.connection_generation, 1U);
  REQUIRE_EQ(live.total_recovery_count, 0U);
  REQUIRE_EQ(live.max_active_transport_count, 1U);
  recovery.stop();
}

void transport_failure_recovery_and_cut() {
  auto script = make_script({Action::Live, Action::Live});
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto first = recovery.wait_for_generation_live(1U);
  REQUIRE(recovery.request_controlled_recovery_for_acceptance());
  const auto second = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(second.state, g5::RecoveryState::Live);
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
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(live.state, g5::RecoveryState::Live);
  REQUIRE_EQ(live.last_recovery_cause, g5::RecoveryCause::NeedsResync);
  REQUIRE_EQ(runtime.observe().reset_count, 1U);
  recovery.stop();
}

void recoverable_failure_matrix() {
  const std::vector<std::pair<Action, g5::RecoveryCause>> cases{
      {Action::SnapshotFailure, g5::RecoveryCause::SnapshotFailure},
      {Action::ProtocolFailure, g5::RecoveryCause::Protocol},
      {Action::BootstrapOverflow, g5::RecoveryCause::BootstrapBufferOverflow},
      {Action::Http5xx, g5::RecoveryCause::Http5xx},
  };
  for (const auto &[action, cause] : cases) {
    auto script = make_script({action, Action::Live});
    g3::MarketRuntime runtime{{8U, 1U}, fixed_clock(), numeric_spec()};
    g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
    REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
    const auto live = recovery.wait_for_generation_live(2U);
    REQUIRE_EQ(live.state, g5::RecoveryState::Live);
    REQUIRE_EQ(live.last_recovery_cause, cause);
    REQUIRE_EQ(live.total_recovery_count, 1U);
    recovery.stop();
  }
}

void ingress_overflow_recovery() {
  auto script = make_script({Action::IngressOverflow, Action::Live});
  g3::MarketRuntime runtime{{1U, 8U}, fixed_clock(), numeric_spec(), {true}};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(2U);
  REQUIRE_EQ(live.state, g5::RecoveryState::Live);
  REQUIRE_EQ(live.last_recovery_cause, g5::RecoveryCause::IngressOverflow);
  recovery.stop();
}

void backoff_and_exhaustion() {
  auto script = make_script({Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure, Action::TransportFailure,
                             Action::TransportFailure});
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto exhausted = recovery.wait_until_terminal();
  REQUIRE(exhausted.exhausted);
  REQUIRE_EQ(exhausted.consecutive_recovery_attempts, 6U);
  REQUIRE_EQ(exhausted.connection_generation, 7U);
  {
    std::lock_guard lock{script->mutex};
    REQUIRE_EQ(script->delays,
               (std::vector<std::chrono::seconds>{
                   std::chrono::seconds{1}, std::chrono::seconds{2},
                   std::chrono::seconds{4}, std::chrono::seconds{8},
                   std::chrono::seconds{16}, std::chrono::seconds{30}}));
    REQUIRE_EQ(script->created_generations.size(), 7U);
  }
  recovery.stop();
}

void success_resets_incident_counter() {
  auto script = make_script({Action::TransportFailure, Action::TransportFailure,
                             Action::Live, Action::Live});
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  const auto live = recovery.wait_for_generation_live(3U);
  REQUIRE_EQ(live.state, g5::RecoveryState::Live);
  REQUIRE_EQ(live.consecutive_recovery_attempts, 0U);
  REQUIRE(recovery.request_controlled_recovery_for_acceptance());
  const auto live_again = recovery.wait_for_generation_live(4U);
  REQUIRE_EQ(live_again.state, g5::RecoveryState::Live);
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

void rate_limit_policy() {
  for (const auto &[action, expected_delay, expected_cause] :
       std::vector<std::tuple<Action, std::chrono::seconds, g5::RecoveryCause>>{
           {Action::Http429, std::chrono::seconds{17},
            g5::RecoveryCause::Http429},
           {Action::Http418, std::chrono::seconds{19},
            g5::RecoveryCause::Http418}}) {
    auto script = make_script({action, Action::Live});
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
    REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
    const auto live = recovery.wait_for_generation_live(2U);
    REQUIRE_EQ(live.state, g5::RecoveryState::Live);
    REQUIRE_EQ(live.last_recovery_cause, expected_cause);
    {
      std::lock_guard lock{script->mutex};
      REQUIRE_EQ(script->delays.front(), expected_delay);
    }
    recovery.stop();
  }

  REQUIRE_EQ(g5::detail::parse_retry_after(std::string{"0"}),
             std::chrono::seconds{0});
  REQUIRE(!g5::detail::parse_retry_after(std::nullopt).has_value());
  REQUIRE(!g5::detail::parse_retry_after(std::string{}).has_value());
  REQUIRE(!g5::detail::parse_retry_after(std::string{"-1"}).has_value());
  REQUIRE(!g5::detail::parse_retry_after(std::string{"1.5"}).has_value());
  REQUIRE(!g5::detail::parse_retry_after(std::string{"184467440737095516160"})
               .has_value());
}

void terminal_http_and_bad_retry_after() {
  for (const auto action : {Action::Http429Missing, Action::Http429Malformed,
                            Action::Http418Overflow, Action::Http403,
                            Action::Http404, Action::AdapterFailure}) {
    auto script = make_script({action, Action::Live});
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
    REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
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
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), std::move(options)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  entered_future.wait();
  recovery.stop();
  const auto stopped = recovery.observe();
  REQUIRE_EQ(stopped.state, g5::RecoveryState::Stopped);
  std::lock_guard lock{script->mutex};
  REQUIRE_EQ(script->created_generations.size(), 1U);
}

void stop_during_active_connection() {
  auto script = make_script({Action::Live, Action::Live});
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), script_options(script)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  static_cast<void>(recovery.wait_for_generation_live(1U));
  recovery.stop();
  const auto stopped = recovery.observe();
  REQUIRE_EQ(stopped.state, g5::RecoveryState::Stopped);
  REQUIRE_EQ(stopped.total_recovery_count, 0U);
  std::lock_guard lock{script->mutex};
  REQUIRE_EQ(script->created_generations.size(), 1U);
  REQUIRE_EQ(script->active, 0U);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"INITIAL_LIVE", initial_live},
      {"TRANSPORT_FAILURE_RECOVERY_AND_OLD_GENERATION_CUT",
       transport_failure_recovery_and_cut},
      {"PROJECTION_NEEDS_RESYNC_DIRECT_RECOVERY", needs_resync_direct_recovery},
      {"RECOVERABLE_FAILURE_MATRIX", recoverable_failure_matrix},
      {"INGRESS_OVERFLOW_RECOVERY", ingress_overflow_recovery},
      {"NORMAL_BACKOFF_AND_MAX_ATTEMPT_EXHAUSTION", backoff_and_exhaustion},
      {"SUCCESS_RESETS_INCIDENT_COUNTER", success_resets_incident_counter},
      {"HTTP_429_418_RETRY_AFTER", rate_limit_policy},
      {"BAD_RETRY_AFTER_HTTP_4XX_INTERNAL_FAIL_CLOSED",
       terminal_http_and_bad_retry_after},
      {"STOP_DURING_BACKOFF", stop_during_backoff},
      {"STOP_DURING_ACTIVE_CONNECTION", stop_during_active_connection},
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
