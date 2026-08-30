#include "gateway_status.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace market = binance_market_data::market::v1;

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

#define REQUIRE(value) require((value), #value)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid numeric spec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock fixed_clock() {
  return [] { return g3::ClockSample{1700000000000000000ULL, 10U}; };
}

struct ClockSequence final {
  std::mutex mutex;
  std::vector<g3::ClockSample> samples;
  std::size_t next{0U};
  bool throw_on_call{false};

  [[nodiscard]] g3::ClockSample sample() {
    std::lock_guard lock{mutex};
    if (throw_on_call) {
      throw std::runtime_error{"clock failure"};
    }
    if (samples.empty()) {
      throw std::runtime_error{"clock sequence is empty"};
    }
    const auto index = std::min(next, samples.size() - 1U);
    ++next;
    return samples[index];
  }
};

[[nodiscard]] g3::RuntimeClock
sequence_clock(const std::shared_ptr<ClockSequence> &sequence) {
  return [sequence] { return sequence->sample(); };
}

void runtime_state_mapping() {
  const std::vector<std::pair<g3::RuntimeState, common::StreamLifecycleState>>
      expected{
          {g3::RuntimeState::Constructed,
           common::STREAM_LIFECYCLE_STATE_ACCEPTED},
          {g3::RuntimeState::Buffering,
           common::STREAM_LIFECYCLE_STATE_SNAPSHOT_PENDING},
          {g3::RuntimeState::AwaitingBridge,
           common::STREAM_LIFECYCLE_STATE_SNAPSHOT_PENDING},
          {g3::RuntimeState::Live, common::STREAM_LIFECYCLE_STATE_LIVE},
          {g3::RuntimeState::NeedsResync,
           common::STREAM_LIFECYCLE_STATE_RESYNC_IN_PROGRESS},
          {g3::RuntimeState::Faulted, common::STREAM_LIFECYCLE_STATE_DEGRADED},
          {g3::RuntimeState::Stopping, common::STREAM_LIFECYCLE_STATE_CLOSING},
          {g3::RuntimeState::Stopped, common::STREAM_LIFECYCLE_STATE_CLOSED},
      };
  for (const auto &[runtime_state, stream_state] : expected) {
    const auto mapped = g10::map_runtime_state(runtime_state);
    REQUIRE(mapped.has_value());
    REQUIRE_EQ(*mapped, stream_state);
  }
}

void basic_snapshot_and_uptime() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock()};
  g9::EventPublication events{"gw-status", fixed_clock()};
  auto sequence = std::make_shared<ClockSequence>();
  sequence->samples = {{0U, 1'000'000'000U}, {0U, 1'999'999'999U}};
  g10::GatewayStatusAssembler assembler{runtime, recovery, events,
                                        sequence_clock(sequence), "gw-status"};

  REQUIRE(assembler.prepare_start_baseline());
  const auto result = assembler.collect();
  const auto *snapshot =
      std::get_if<g10::gateway_wire::GatewayStatusSnapshot>(&result);
  REQUIRE(snapshot != nullptr);
  REQUIRE_EQ(snapshot->schema_version(), g10::kStatusSnapshotSchema);
  REQUIRE_EQ(snapshot->gateway_instance_id(), "gw-status");
  REQUIRE_EQ(snapshot->observed_time_utc_ns(), 0U);
  REQUIRE_EQ(snapshot->uptime_seconds(), 0U);
  REQUIRE_EQ(snapshot->markets_size(), 1);
  REQUIRE_EQ(snapshot->markets(0).venue(), common::VENUE_BINANCE);
  REQUIRE_EQ(snapshot->markets(0).market(), common::MARKET_SPOT);
  REQUIRE_EQ(snapshot->markets(0).symbol(), "BTCUSDT");
  REQUIRE_EQ(snapshot->markets(0).state(),
             common::STREAM_LIFECYCLE_STATE_ACCEPTED);
  REQUIRE(!snapshot->markets(0).has_last_event_utc_ns());
  REQUIRE(!snapshot->markets(0).has_connection_generation());
  REQUIRE_EQ(snapshot->total_active_subscriptions(), 0U);

  sequence->samples.push_back({123U, 2'000'000'000U});
  const auto exact = assembler.collect();
  const auto *exact_snapshot =
      std::get_if<g10::gateway_wire::GatewayStatusSnapshot>(&exact);
  REQUIRE(exact_snapshot != nullptr);
  REQUIRE_EQ(exact_snapshot->observed_time_utc_ns(), 123U);
  REQUIRE_EQ(exact_snapshot->uptime_seconds(), 1U);
}

void assembler_errors_and_counts() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock()};
  g9::EventPublication events{"gw-counts", fixed_clock()};
  auto sequence = std::make_shared<ClockSequence>();
  sequence->samples = {{10U, 1'000'000'000U}, {20U, 2'000'000'000U}};
  g10::GatewayStatusAssembler assembler{runtime, recovery, events,
                                        sequence_clock(sequence), "gw-counts"};

  REQUIRE_EQ(std::get<g10::StatusSnapshotError>(assembler.collect()),
             g10::StatusSnapshotError::StartBaselineUnavailable);
  REQUIRE(assembler.prepare_start_baseline());

  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("g10-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g10-snapshot");
  snapshot.set_last_update_id(100U);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("1.000");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("1.000");
  REQUIRE_EQ(runtime.submit_snapshot(std::move(snapshot)),
             g3::AdmissionResult::Accepted);
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("g10-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g10-source");
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(g7::kUpdateSchema);
  update.set_first_update_id(101U);
  update.set_final_update_id(101U);
  auto *update_bid = update.add_bids();
  update_bid->set_price("100.00");
  update_bid->set_quantity("2.000");
  REQUIRE_EQ(runtime.submit_depth_update(std::move(update)),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Live);

  REQUIRE(events.open_generation(1U));
  REQUIRE(std::holds_alternative<g9::AcceptedEventSubscription>(events.admit(
      {"event-1", common::STREAM_AGG_TRADE, g9::kAggTradeEventSchema})));
  const g7::ValidatedOrderBookSubscription order_book_subscription{
      "order-book-1", "gw-counts", std::nullopt};
  REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(
      runtime.admit_order_book_subscription(order_book_subscription)));

  sequence->samples.push_back({20U, 2'000'000'000U});
  const auto result = assembler.collect();
  const auto *status =
      std::get_if<g10::gateway_wire::GatewayStatusSnapshot>(&result);
  REQUIRE(status != nullptr);
  REQUIRE_EQ(status->markets(0).active_subscription_count(), 2U);
  REQUIRE_EQ(status->total_active_subscriptions(), 2U);
  REQUIRE(status->markets(0).has_connection_generation() == false);
  runtime.stop();
  events.shutdown();
}

void uptime_regression_and_clock_failure() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock()};
  g9::EventPublication events{"gw-clock", fixed_clock()};
  auto sequence = std::make_shared<ClockSequence>();
  sequence->samples = {{1U, 1'000U}, {2U, 999U}};
  g10::GatewayStatusAssembler assembler{runtime, recovery, events,
                                        sequence_clock(sequence), "gw-clock"};
  REQUIRE(assembler.prepare_start_baseline());
  REQUIRE_EQ(std::get<g10::StatusSnapshotError>(assembler.collect()),
             g10::StatusSnapshotError::MonotonicClockRegression);

  sequence->throw_on_call = true;
  REQUIRE(!assembler.prepare_start_baseline());
  REQUIRE_EQ(std::get<g10::StatusSnapshotError>(assembler.collect()),
             g10::StatusSnapshotError::StartBaselineUnavailable);
}

struct FakeAttemptState final {
  mutable std::mutex mutex;
  g4::TransportObservation observation;
};

class FakeAttempt final : public g5::detail::RecoveryAttempt {
public:
  FakeAttempt(g3::MarketRuntime &runtime, std::uint64_t generation,
              std::shared_ptr<FakeAttemptState> state)
      : runtime_{runtime}, generation_{generation}, state_{std::move(state)} {
    std::lock_guard lock{state_->mutex};
    state_->observation.connection_generation = generation_;
    state_->observation.connection_id =
        "g10-fake-" + std::to_string(generation_);
  }

  ~FakeAttempt() override { stop(); }

  [[nodiscard]] g4::TransportStartResult start() override {
    {
      std::lock_guard lock{state_->mutex};
      state_->observation.started = true;
      state_->observation.running = true;
      state_->observation.tls_verified = true;
      state_->observation.websocket_handshake = true;
      state_->observation.rest_depth_fetched = true;
    }
    market::ExchangeDepthSnapshot snapshot;
    snapshot.set_venue(common::VENUE_BINANCE);
    snapshot.set_market(common::MARKET_SPOT);
    snapshot.set_symbol("BTCUSDT");
    snapshot.set_schema_version("exchange-depth-snapshot.v1");
    snapshot.set_producer("g10-fake");
    snapshot.set_producer_version("1.0.0");
    snapshot.set_request_id("fake-snapshot-" + std::to_string(generation_));
    snapshot.set_last_update_id(generation_ * 100U);
    auto *bid = snapshot.add_bids();
    bid->set_price("100.00");
    bid->set_quantity("1.000");
    auto *ask = snapshot.add_asks();
    ask->set_price("101.00");
    ask->set_quantity("1.000");
    static_cast<void>(runtime_.submit_snapshot(
        std::move(snapshot), g3::SourceProvenance{generation_}));

    market::DepthUpdate update;
    auto *metadata = update.mutable_metadata();
    metadata->set_venue(common::VENUE_BINANCE);
    metadata->set_market(common::MARKET_SPOT);
    metadata->set_symbol("BTCUSDT");
    metadata->set_producer("g10-fake");
    metadata->set_producer_version("1.0.0");
    metadata->set_connection_id("g10-fake-" + std::to_string(generation_));
    metadata->set_stream(common::STREAM_DIFF_DEPTH);
    metadata->set_schema_version(g7::kUpdateSchema);
    update.set_first_update_id(generation_ * 100U + 1U);
    update.set_final_update_id(generation_ * 100U + 1U);
    auto *update_bid = update.add_bids();
    update_bid->set_price("100.00");
    update_bid->set_quantity("2.000");
    static_cast<void>(runtime_.submit_depth_update(
        std::move(update), g3::SourceProvenance{generation_}));
    return g4::TransportStartResult::Started;
  }

  void stop() noexcept override {
    if (stopped_.exchange(true)) {
      return;
    }
    std::lock_guard lock{state_->mutex};
    state_->observation.running = false;
    state_->observation.stopped = true;
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
    std::lock_guard lock{state_->mutex};
    return state_->observation;
  }

private:
  g3::MarketRuntime &runtime_;
  std::uint64_t generation_;
  std::shared_ptr<FakeAttemptState> state_;
  std::atomic<bool> stopped_{false};
};

struct FakeAttempts final {
  std::mutex mutex;
  std::vector<std::shared_ptr<FakeAttemptState>> states;
};

void wait_for_attempt_count(const std::shared_ptr<FakeAttempts> &attempts,
                            std::size_t expected) {
  for (std::size_t tries = 0U; tries < 200U; ++tries) {
    {
      std::lock_guard lock{attempts->mutex};
      if (attempts->states.size() >= expected) {
        return;
      }
    }
    std::this_thread::yield();
  }
  throw TestFailure{"timed out waiting for fake recovery attempt"};
}

void active_event_observation_persists_in_source_order() {
  auto attempts = std::make_shared<FakeAttempts>();
  g5::detail::RecoveryTestOptions options;
  options.attempt_factory = [attempts](g3::MarketRuntime &runtime,
                                       const g3::RuntimeClock &,
                                       std::uint64_t generation) {
    auto state = std::make_shared<FakeAttemptState>();
    {
      std::lock_guard lock{attempts->mutex};
      attempts->states.push_back(state);
    }
    return std::unique_ptr<g5::detail::RecoveryAttempt>{
        std::make_unique<FakeAttempt>(runtime, generation, std::move(state))};
  };
  options.backoff_waiter = [](std::chrono::seconds, std::stop_token token) {
    return !token.stop_requested();
  };

  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, fixed_clock(), std::move(options)};
  REQUIRE_EQ(recovery.start(), g5::RecoveryStartResult::Started);
  REQUIRE_EQ(recovery.wait_for_generation_live(1U).state,
             g5::RecoveryState::Live);
  wait_for_attempt_count(attempts, 1U);
  std::shared_ptr<FakeAttemptState> first;
  {
    std::lock_guard lock{attempts->mutex};
    first = attempts->states[0];
  }
  {
    std::lock_guard lock{first->mutex};
    first->observation.last_event_utc_ns = 100U;
  }
  REQUIRE_EQ(recovery.observe().last_event_utc_ns,
             std::optional<std::uint64_t>{100U});

  REQUIRE(recovery.request_controlled_recovery_for_acceptance());
  REQUIRE_EQ(recovery.wait_for_generation_live(2U).state,
             g5::RecoveryState::Live);
  wait_for_attempt_count(attempts, 2U);
  REQUIRE_EQ(recovery.observe().last_event_utc_ns,
             std::optional<std::uint64_t>{100U});
  std::shared_ptr<FakeAttemptState> second;
  {
    std::lock_guard lock{attempts->mutex};
    second = attempts->states[1];
  }
  {
    std::lock_guard lock{second->mutex};
    second->observation.last_event_utc_ns = 90U;
  }
  REQUIRE_EQ(recovery.observe().last_event_utc_ns,
             std::optional<std::uint64_t>{90U});
  recovery.stop();
  REQUIRE_EQ(recovery.observe().last_event_utc_ns,
             std::optional<std::uint64_t>{90U});
}

} // namespace

int main() {
  try {
    runtime_state_mapping();
    basic_snapshot_and_uptime();
    assembler_errors_and_counts();
    uptime_regression_and_clock_failure();
    active_event_observation_persists_in_source_order();
  } catch (const std::exception &error) {
    return error.what() == nullptr ? 1 : 1;
  }
  return 0;
}
