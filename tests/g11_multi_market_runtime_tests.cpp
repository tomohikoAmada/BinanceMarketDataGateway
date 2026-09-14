#include "multi_market_runtime.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/market/v1/market_events.pb.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace market_wire = binance_market_data::market::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g11 = binance_market_data::gateway::g11;

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

[[nodiscard]] g11::MarketKey fixed_key_for(common_wire::Market market) {
  return market == common_wire::MARKET_SPOT ? g11::spot_btcusdt_key()
                                            : g11::usdm_btcusdt_key();
}

[[nodiscard]] market_wire::DepthUpdate make_update(const g11::MarketKey &key,
                                                   std::uint64_t generation) {
  const auto market = key.market;
  market_wire::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(market);
  metadata->set_symbol(key.symbol);
  metadata->set_producer("gateway-g11-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(
      (market == common_wire::MARKET_SPOT ? "spot-g" : "usdm-g") + key.symbol +
      "-" + std::to_string(generation));
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(1700000000002ULL);
  metadata->set_receive_time_utc_ns(1700000000002000000ULL);
  metadata->set_receive_monotonic_ns(9000000000002ULL);
  update.set_first_update_id(
      market == common_wire::MARKET_USD_M_PERPETUAL ? 100U : 101U);
  update.set_final_update_id(101U);
  if (market == common_wire::MARKET_USD_M_PERPETUAL) {
    update.set_previous_final_update_id(100U);
  }
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("4.000");
  return update;
}

[[nodiscard]] market_wire::ExchangeDepthSnapshot
make_snapshot(const g11::MarketKey &key, std::uint64_t generation) {
  const auto market = key.market;
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(market);
  snapshot.set_symbol(key.symbol);
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g11-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id((market == common_wire::MARKET_SPOT
                               ? "spot-snapshot-g"
                               : "usdm-snapshot-g") +
                          std::to_string(generation));
  snapshot.set_last_update_id(100U);
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

class SyntheticLiveAttempt final : public g5::detail::RecoveryAttempt {
public:
  SyntheticLiveAttempt(g3::MarketRuntime &runtime, g11::MarketKey key,
                       std::uint64_t generation)
      : runtime_{runtime}, key_{std::move(key)}, generation_{generation} {
    observation_.connection_generation = generation;
    observation_.connection_id =
        (key_.market == common_wire::MARKET_SPOT ? "spot-test-g"
                                                 : "usdm-test-g") +
        key_.symbol + "-" + std::to_string(generation);
  }

  [[nodiscard]] g4::TransportStartResult start() override {
    if (runtime_.submit_depth_update(make_update(key_, generation_),
                                     g3::SourceProvenance{generation_}) !=
            g3::AdmissionResult::Accepted ||
        runtime_.submit_snapshot(make_snapshot(key_, generation_),
                                 g3::SourceProvenance{generation_}) !=
            g3::AdmissionResult::Accepted) {
      return g4::TransportStartResult::Failed;
    }
    std::lock_guard lock{mutex_};
    observation_.started = true;
    observation_.running = true;
    observation_.tls_verified = true;
    observation_.websocket_handshake = true;
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
    observation_.last_event_utc_ns = 1700000000002000000ULL + generation_;
    return g4::TransportStartResult::Started;
  }

  void stop() noexcept override {
    std::lock_guard lock{mutex_};
    observation_.running = false;
    observation_.stopped = true;
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  g3::MarketRuntime &runtime_;
  const g11::MarketKey key_;
  const std::uint64_t generation_;
  mutable std::mutex mutex_;
  g4::TransportObservation observation_;
};

class TerminalAttempt final : public g5::detail::RecoveryAttempt {
public:
  explicit TerminalAttempt(std::uint64_t generation) {
    observation_.connection_generation = generation;
    observation_.connection_id = "usdm-terminal-g" + std::to_string(generation);
    observation_.terminal_error =
        g4::NetworkError{g4::NetworkErrorCode::Internal, "usdm-test-terminal",
                         "terminal USD-M failure", std::nullopt, std::nullopt};
  }

  [[nodiscard]] g4::TransportStartResult start() override {
    std::lock_guard lock{mutex_};
    observation_.started = true;
    return g4::TransportStartResult::Failed;
  }

  void stop() noexcept override {
    std::lock_guard lock{mutex_};
    observation_.stopped = true;
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  mutable std::mutex mutex_;
  g4::TransportObservation observation_;
};

class RotationGate final {
public:
  [[nodiscard]] g3::TimedRecoveryWaitResult wait(g3::MarketRuntime &,
                                                 std::chrono::nanoseconds,
                                                 std::stop_token stop_token) {
    std::unique_lock lock{mutex_};
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

  void wait_for_calls(std::size_t expected) {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this, expected] { return calls_ >= expected; });
  }

  void trigger(g3::TimedRecoveryWaitResult result) {
    {
      std::lock_guard lock{mutex_};
      results_.push_back(result);
    }
    condition_.notify_all();
  }

private:
  std::mutex mutex_;
  std::condition_variable_any condition_;
  std::deque<g3::TimedRecoveryWaitResult> results_;
  std::size_t calls_{0U};
};

[[nodiscard]] g5::detail::RecoveryTestOptions
live_test_options(g11::MarketKey key) {
  g5::detail::RecoveryTestOptions options;
  options.attempt_factory = [key = std::move(key)](g3::MarketRuntime &runtime,
                                                   const g3::RuntimeClock &,
                                                   std::uint64_t generation) {
    return std::make_unique<SyntheticLiveAttempt>(runtime, key, generation);
  };
  options.backoff_waiter = [](std::chrono::seconds,
                              std::stop_token stop_token) {
    return !stop_token.stop_requested();
  };
  return options;
}

[[nodiscard]] g5::detail::RecoveryTestOptions
live_test_options(common_wire::Market market) {
  return live_test_options(fixed_key_for(market));
}

[[nodiscard]] g11::TwoProductRuntimeOptions live_two_product_options() {
  g11::TwoProductRuntimeOptions options;
  options.spot.recovery_test = live_test_options(common_wire::MARKET_SPOT);
  options.usdm.recovery_test =
      live_test_options(common_wire::MARKET_USD_M_PERPETUAL);
  return options;
}

[[nodiscard]] g11::ProductRuntimeSpec resolved_spec(g11::MarketKey key,
                                                    bool live = false) {
  g11::ProductRuntimeOptions options;
  if (live) {
    options.recovery_test = live_test_options(key);
  }
  return {std::move(key), numeric_spec(), std::move(options)};
}

[[nodiscard]] bool
rejects_configured_set(std::vector<g11::ProductRuntimeSpec> specifications) {
  try {
    g11::ConfiguredProductRuntimeSet products{std::move(specifications),
                                              fixed_clock(), "gw-invalid-set"};
    return false;
  } catch (const std::invalid_argument &) {
    return true;
  }
}

void configured_set_cardinality_duplicates_order_and_stability() {
  REQUIRE(rejects_configured_set({}));

  g11::ConfiguredProductRuntimeSet one{
      std::vector<g11::ProductRuntimeSpec>{resolved_spec(
          {common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, "ETHUSDT"})},
      fixed_clock(), "gw-one"};
  REQUIRE_EQ(one.size(), 1U);
  REQUIRE_EQ(one.registry().entries().size(), 1U);
  const auto rejects_registry = [](std::vector<g11::MarketServices> entries) {
    try {
      g11::MarketRuntimeRegistry registry{std::move(entries)};
      return false;
    } catch (const std::invalid_argument &) {
      return true;
    }
  };
  auto null_services = one.registry().entries().front();
  null_services.runtime = nullptr;
  REQUIRE(rejects_registry({null_services}));
  auto aliased_services = one.registry().entries().front();
  auto second_alias = aliased_services;
  second_alias.key = g11::usdm_btcusdt_key();
  REQUIRE(rejects_registry({aliased_services, second_alias}));

  std::vector<g11::ProductRuntimeSpec> eight_specs;
  for (const auto *symbol : {"A", "B", "C", "D", "E", "F", "G", "H"}) {
    eight_specs.push_back(resolved_spec(
        {common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, symbol}));
  }
  g11::ConfiguredProductRuntimeSet eight{std::move(eight_specs), fixed_clock(),
                                         "gw-eight"};
  REQUIRE_EQ(eight.size(), 8U);
  REQUIRE_EQ(eight.registry().entries().size(), 8U);

  auto nine_specs = std::vector<g11::ProductRuntimeSpec>{};
  for (const auto *symbol : {"A", "B", "C", "D", "E", "F", "G", "H", "I"}) {
    nine_specs.push_back(resolved_spec(
        {common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, symbol}));
  }
  REQUIRE(rejects_configured_set(std::move(nine_specs)));

  const g11::MarketKey duplicate_key{common_wire::VENUE_BINANCE,
                                     common_wire::MARKET_SPOT, "ETHUSDT"};
  std::vector<g11::ProductRuntimeSpec> duplicates;
  duplicates.push_back(resolved_spec(duplicate_key));
  duplicates.push_back(resolved_spec(duplicate_key));
  REQUIRE(rejects_configured_set(std::move(duplicates)));

  const g11::MarketKey spot_btc = g11::spot_btcusdt_key();
  const g11::MarketKey spot_eth{common_wire::VENUE_BINANCE,
                                common_wire::MARKET_SPOT, "ETHUSDT"};
  const g11::MarketKey usdm_btc = g11::usdm_btcusdt_key();
  const g11::MarketKey usdm_eth{common_wire::VENUE_BINANCE,
                                common_wire::MARKET_USD_M_PERPETUAL, "ETHUSDT"};
  std::vector<g11::ProductRuntimeSpec> scrambled;
  scrambled.push_back(resolved_spec(usdm_eth));
  scrambled.push_back(resolved_spec(spot_eth));
  scrambled.push_back(resolved_spec(usdm_btc));
  scrambled.push_back(resolved_spec(spot_btc));
  g11::ConfiguredProductRuntimeSet four{std::move(scrambled), fixed_clock(),
                                        "gw-four"};
  const auto &entries = four.registry().entries();
  REQUIRE_EQ(entries.size(), 4U);
  REQUIRE_EQ(entries[0].key, spot_btc);
  REQUIRE_EQ(entries[1].key, spot_eth);
  REQUIRE_EQ(entries[2].key, usdm_btc);
  REQUIRE_EQ(entries[3].key, usdm_eth);
  for (std::size_t index = 0U; index < entries.size(); ++index) {
    auto *owner = four.find(entries[index].key);
    REQUIRE(owner != nullptr);
    REQUIRE(owner == four.products()[index].get());
    REQUIRE(entries[index].runtime == &owner->runtime());
    REQUIRE(entries[index].recovery == &owner->recovery());
    REQUIRE(entries[index].event_publication == &owner->event_publication());
  }
  REQUIRE(four.find({common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT,
                     "UNCONFIGURED"}) == nullptr);
  REQUIRE(four.registry().find(spot_btc) != nullptr);
  REQUIRE(four.registry().find(usdm_btc) != nullptr);
  REQUIRE(four.registry().find(spot_btc) != four.registry().find(usdm_btc));
}

void two_product_ownership_and_projection_policy() {
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), fixed_clock(),
                                  "gw-g11", live_two_product_options()};
  const auto started = products.start();
  REQUIRE(started.spot == g5::RecoveryStartResult::Started);
  REQUIRE(started.usdm == g5::RecoveryStartResult::Started);
  const auto spot_live =
      products.spot().recovery().wait_for_generation_live(1U);
  const auto usdm_live =
      products.usdm().recovery().wait_for_generation_live(1U);
  const auto spot_runtime = products.spot().runtime().observe();
  const auto usdm_runtime = products.usdm().runtime().observe();

  REQUIRE(products.spot().key() == g11::spot_btcusdt_key());
  REQUIRE(products.usdm().key() == g11::usdm_btcusdt_key());
  REQUIRE(spot_live.state == g5::RecoveryState::Live);
  REQUIRE(usdm_live.state == g5::RecoveryState::Live);
  REQUIRE(spot_runtime.state == g3::RuntimeState::Live);
  REQUIRE(usdm_runtime.state == g3::RuntimeState::Live);
  REQUIRE(spot_runtime.projection_status ==
          core::ProjectionStatus::Synchronized);
  REQUIRE(usdm_runtime.projection_status ==
          core::ProjectionStatus::Synchronized);
  REQUIRE(spot_runtime.owner_thread_id != std::thread::id{});
  REQUIRE(usdm_runtime.owner_thread_id != std::thread::id{});
  REQUIRE(spot_runtime.owner_thread_id != usdm_runtime.owner_thread_id);
  REQUIRE_EQ(spot_runtime.last_update_id, std::optional<std::uint64_t>{101U});
  REQUIRE_EQ(usdm_runtime.last_update_id, std::optional<std::uint64_t>{101U});
  products.stop();
}

void recovery_and_generation_are_isolated() {
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), fixed_clock(),
                                  "gw-isolation", live_two_product_options()};
  static_cast<void>(products.start());
  static_cast<void>(products.spot().recovery().wait_for_generation_live(1U));
  static_cast<void>(products.usdm().recovery().wait_for_generation_live(1U));

  const auto usdm_before = products.usdm().runtime().observe();
  REQUIRE(
      products.spot().recovery().request_controlled_recovery_for_acceptance());
  const auto spot_generation_two =
      products.spot().recovery().wait_for_generation_live(2U);
  const auto usdm_after_spot_recovery = products.usdm().runtime().observe();
  const auto usdm_recovery_after_spot = products.usdm().recovery().observe();
  REQUIRE_EQ(spot_generation_two.connection_generation, 2U);
  REQUIRE_EQ(spot_generation_two.failure_history_size, 1U);
  REQUIRE_EQ(spot_generation_two.failure_history[0].connection_generation, 1U);
  REQUIRE_EQ(usdm_recovery_after_spot.failure_history_size, 0U);
  REQUIRE_EQ(usdm_recovery_after_spot.connection_generation, 1U);
  REQUIRE_EQ(usdm_after_spot_recovery.reset_count, usdm_before.reset_count);
  REQUIRE_EQ(usdm_after_spot_recovery.owner_thread_id,
             usdm_before.owner_thread_id);
  REQUIRE_EQ(usdm_after_spot_recovery.last_update_id,
             usdm_before.last_update_id);

  const auto spot_before_usdm_recovery = products.spot().runtime().observe();
  REQUIRE(
      products.usdm().recovery().request_controlled_recovery_for_acceptance());
  const auto usdm_generation_two =
      products.usdm().recovery().wait_for_generation_live(2U);
  const auto spot_after_usdm_recovery = products.spot().runtime().observe();
  const auto spot_recovery_after_usdm = products.spot().recovery().observe();
  REQUIRE_EQ(usdm_generation_two.connection_generation, 2U);
  REQUIRE_EQ(usdm_generation_two.failure_history_size, 1U);
  REQUIRE_EQ(usdm_generation_two.failure_history[0].connection_generation, 1U);
  REQUIRE_EQ(spot_recovery_after_usdm.connection_generation, 2U);
  REQUIRE_EQ(spot_after_usdm_recovery.reset_count,
             spot_before_usdm_recovery.reset_count);
  REQUIRE_EQ(spot_after_usdm_recovery.owner_thread_id,
             spot_before_usdm_recovery.owner_thread_id);
  REQUIRE_EQ(spot_generation_two.max_active_transport_count, 1U);
  REQUIRE_EQ(usdm_generation_two.max_active_transport_count, 1U);
  products.stop();
}

void planned_rotation_is_product_local() {
  auto options = live_two_product_options();
  auto spot_rotation = std::make_shared<RotationGate>();
  options.spot.planned_rotation = {std::chrono::nanoseconds{1}};
  options.spot.recovery_test.rotation_waiter =
      [spot_rotation](g3::MarketRuntime &runtime,
                      std::chrono::nanoseconds duration,
                      std::stop_token stop_token) {
        return spot_rotation->wait(runtime, duration, stop_token);
      };
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), fixed_clock(),
                                  "gw-rotation", std::move(options)};
  static_cast<void>(products.start());
  static_cast<void>(products.spot().recovery().wait_for_generation_live(1U));
  static_cast<void>(products.usdm().recovery().wait_for_generation_live(1U));
  const auto usdm_before = products.usdm().runtime().observe();

  spot_rotation->wait_for_calls(1U);
  spot_rotation->trigger(g3::TimedRecoveryWaitResult::DeadlineReached);
  const auto spot_rotated =
      products.spot().recovery().wait_for_generation_live(2U);
  const auto usdm_after = products.usdm().runtime().observe();
  const auto usdm_recovery = products.usdm().recovery().observe();

  REQUIRE_EQ(spot_rotated.connection_generation, 2U);
  REQUIRE_EQ(spot_rotated.planned_rotation_count, 1U);
  REQUIRE_EQ(spot_rotated.total_recovery_count, 0U);
  REQUIRE_EQ(usdm_recovery.connection_generation, 1U);
  REQUIRE_EQ(usdm_recovery.planned_rotation_count, 0U);
  REQUIRE_EQ(usdm_after.reset_count, usdm_before.reset_count);
  REQUIRE_EQ(usdm_after.owner_thread_id, usdm_before.owner_thread_id);
  REQUIRE_EQ(usdm_after.last_update_id, usdm_before.last_update_id);
  products.stop();
}

void one_market_terminal_failure_does_not_stop_other() {
  auto options = live_two_product_options();
  options.usdm.recovery_test.attempt_factory = [](g3::MarketRuntime &,
                                                  const g3::RuntimeClock &,
                                                  std::uint64_t generation) {
    return std::make_unique<TerminalAttempt>(generation);
  };
  g11::TwoProductRuntime products{numeric_spec(), numeric_spec(), fixed_clock(),
                                  "gw-terminal", std::move(options)};
  const auto started = products.start();
  REQUIRE(started.spot == g5::RecoveryStartResult::Started);
  REQUIRE(started.usdm == g5::RecoveryStartResult::Started);
  const auto spot_live =
      products.spot().recovery().wait_for_generation_live(1U);
  const auto usdm_terminal = products.usdm().recovery().wait_until_terminal();
  const auto spot_after = products.spot().recovery().observe();
  const auto spot_runtime_after = products.spot().runtime().observe();

  REQUIRE(spot_live.state == g5::RecoveryState::Live);
  REQUIRE(usdm_terminal.terminal);
  REQUIRE(usdm_terminal.state == g5::RecoveryState::Exhausted);
  REQUIRE_EQ(usdm_terminal.failure_history_size, 1U);
  REQUIRE_EQ(usdm_terminal.failure_history[0].connection_generation, 1U);
  REQUIRE(usdm_terminal.failure_history[0].network_error.has_value());
  REQUIRE(spot_after.state == g5::RecoveryState::Live);
  REQUIRE_EQ(spot_after.connection_generation, 1U);
  REQUIRE_EQ(spot_after.active_transport_count, 1U);
  REQUIRE(spot_runtime_after.state == g3::RuntimeState::Live);
  REQUIRE(spot_runtime_after.projection_status ==
          core::ProjectionStatus::Synchronized);
  products.stop();
}

void product_runtime_uses_exact_market_key_identity() {
  const g11::MarketKey spot_eth{common_wire::VENUE_BINANCE,
                                common_wire::MARKET_SPOT, "ETHUSDT"};
  const g11::MarketKey usdm_eth{common_wire::VENUE_BINANCE,
                                common_wire::MARKET_USD_M_PERPETUAL, "ETHUSDT"};
  g11::ProductRuntimeOptions spot_options;
  spot_options.recovery_test = live_test_options(spot_eth);
  g11::ProductRuntimeOptions usdm_options;
  usdm_options.recovery_test = live_test_options(usdm_eth);
  g11::ProductRuntime spot{spot_eth, numeric_spec(), fixed_clock(),
                           "gw-spot-eth", std::move(spot_options)};
  g11::ProductRuntime usdm{usdm_eth, numeric_spec(), fixed_clock(),
                           "gw-usdm-eth", std::move(usdm_options)};

  REQUIRE(spot.key() == spot_eth);
  REQUIRE(usdm.key() == usdm_eth);
  REQUIRE(spot.kind() == g11::ProductKind::Spot);
  REQUIRE(usdm.kind() == g11::ProductKind::UsdMPerpetual);
  REQUIRE(spot.start() == g5::RecoveryStartResult::Started);
  REQUIRE(usdm.start() == g5::RecoveryStartResult::Started);

  const auto spot_live = spot.recovery().wait_for_generation_live(1U);
  const auto usdm_live = usdm.recovery().wait_for_generation_live(1U);
  const auto spot_observation = spot.runtime().observe();
  const auto usdm_observation = usdm.runtime().observe();
  REQUIRE(spot_live.state == g5::RecoveryState::Live);
  REQUIRE(usdm_live.state == g5::RecoveryState::Live);
  REQUIRE(spot_observation.state == g3::RuntimeState::Live);
  REQUIRE(usdm_observation.state == g3::RuntimeState::Live);
  REQUIRE_EQ(spot_observation.last_update_id,
             std::optional<std::uint64_t>{101U});
  REQUIRE_EQ(usdm_observation.last_update_id,
             std::optional<std::uint64_t>{101U});
  spot.stop();
  usdm.stop();

  const auto rejects = [](g11::MarketKey key) {
    try {
      g11::ProductRuntime runtime{std::move(key), numeric_spec(), fixed_clock(),
                                  "invalid"};
      return false;
    } catch (const std::invalid_argument &) {
      return true;
    }
  };
  REQUIRE(rejects(
      {common_wire::VENUE_UNSPECIFIED, common_wire::MARKET_SPOT, "ETHUSDT"}));
  REQUIRE(rejects({common_wire::VENUE_BINANCE, common_wire::MARKET_UNSPECIFIED,
                   "ETHUSDT"}));
  REQUIRE(rejects({common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, ""}));
  REQUIRE(rejects(
      {common_wire::VENUE_BINANCE, common_wire::MARKET_SPOT, "ethusdt"}));
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"TWO_PRODUCT_OWNERSHIP_AND_PROJECTION_POLICY",
       two_product_ownership_and_projection_policy},
      {"RECOVERY_AND_GENERATION_ARE_ISOLATED",
       recovery_and_generation_are_isolated},
      {"PLANNED_ROTATION_IS_PRODUCT_LOCAL", planned_rotation_is_product_local},
      {"ONE_MARKET_TERMINAL_FAILURE_DOES_NOT_STOP_OTHER",
       one_market_terminal_failure_does_not_stop_other},
      {"PRODUCT_RUNTIME_USES_EXACT_MARKET_KEY_IDENTITY",
       product_runtime_uses_exact_market_key_identity},
      {"CONFIGURED_SET_CARDINALITY_DUPLICATES_ORDER_AND_STABILITY",
       configured_set_cardinality_duplicates_order_and_stability},
  };

  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &failure) {
      std::cerr << name << "=FAIL " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
