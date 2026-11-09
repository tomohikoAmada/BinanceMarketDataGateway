#pragma once

#include "production/production_gateway.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/market/v1/market_events.pb.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>
namespace composition = binance_market_data::gateway::composition;
namespace transport = binance_market_data::gateway::transport;
namespace recovery = binance_market_data::gateway::recovery;
namespace production = binance_market_data::gateway::production;
namespace rt = binance_market_data::gateway::runtime;

namespace binance_market_data::gateway::production::test_support {

namespace common = ::binance_market_data::common::v1;
namespace market = ::binance_market_data::market::v1;

enum class AttemptMode : std::uint8_t {
  Live,
  Terminal,
  NeverLive,
};

class TestAttempt;

struct AttemptState final {
  std::atomic<std::size_t> active{0U};
  std::atomic<std::size_t> maximum_active{0U};
  std::atomic<std::size_t> starts{0U};
  std::atomic<TestAttempt *> active_attempt{nullptr};
  std::mutex start_mutex;
  std::condition_variable start_condition;
  std::size_t completed_starts{0U};

  void mark_start_completed() {
    {
      std::lock_guard lock{start_mutex};
      ++completed_starts;
    }
    start_condition.notify_all();
  }

  void wait_for_first_start_completion() {
    std::unique_lock lock{start_mutex};
    start_condition.wait(lock, [this] { return completed_starts != 0U; });
  }
};

[[nodiscard]] inline rt::RuntimeClock fixed_clock() {
  return
      [] { return rt::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
}

[[nodiscard]] inline projection::v1::NumericSpec numeric_spec() {
  const auto price = projection::v1::DecimalScale::create(2U);
  const auto quantity = projection::v1::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw std::runtime_error{"invalid test NumericSpec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] inline market::DepthUpdate
make_update(const rt::MarketKey &key, std::uint64_t generation,
            std::uint64_t final_id = 101U, bool wrong_symbol = false) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(key.venue);
  metadata->set_market(key.market);
  metadata->set_symbol(wrong_symbol ? key.symbol + "-wrong" : key.symbol);
  metadata->set_producer("gateway-production-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(
      (key.market == common::MARKET_SPOT ? "spot-test-g" : "usdm-test-g") +
      std::to_string(generation));
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(1700000000002ULL + final_id);
  metadata->set_receive_time_utc_ns(1700000000002000000ULL + final_id);
  metadata->set_receive_monotonic_ns(9000000000002ULL + final_id);
  update.set_first_update_id(
      key.market == common::MARKET_USD_M_PERPETUAL ? final_id - 1U : final_id);
  update.set_final_update_id(final_id);
  if (key.market == common::MARKET_USD_M_PERPETUAL) {
    update.set_previous_final_update_id(final_id - 1U);
  }
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("4.000");
  return update;
}

[[nodiscard]] inline market::ExchangeDepthSnapshot
make_snapshot(const rt::MarketKey &key, std::uint64_t generation) {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(key.venue);
  snapshot.set_market(key.market);
  snapshot.set_symbol(key.symbol);
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-production-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id((key.market == common::MARKET_SPOT
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

[[nodiscard]] inline market::DepthUpdate
make_update(common::Market product, std::uint64_t generation,
            std::uint64_t final_id = 101U, bool wrong_symbol = false) {
  const auto key = product == common::MARKET_SPOT ? rt::spot_btcusdt_key()
                                                  : rt::usdm_btcusdt_key();
  return make_update(key, generation, final_id, wrong_symbol);
}

class TestAttempt final : public recovery::detail::RecoveryAttempt {
public:
  TestAttempt(rt::MarketRuntime &runtime, rt::MarketKey key,
              std::uint64_t generation, AttemptMode mode,
              std::shared_ptr<AttemptState> state)
      : runtime_{runtime}, key_{std::move(key)}, generation_{generation},
        mode_{mode}, state_{std::move(state)} {
    state_->active_attempt.store(this);
    observation_.connection_generation = generation_;
    observation_.connection_id =
        (key_.market == common::MARKET_SPOT ? "spot-test-g" : "usdm-test-g") +
        std::to_string(generation_);
    const auto active = state_->active.fetch_add(1U) + 1U;
    auto maximum = state_->maximum_active.load();
    while (maximum < active &&
           !state_->maximum_active.compare_exchange_weak(maximum, active)) {
    }
  }

  ~TestAttempt() override { stop(); }

  [[nodiscard]] transport::TransportStartResult start() override {
    state_->starts.fetch_add(1U);
    {
      std::lock_guard lock{mutex_};
      observation_.started = true;
      observation_.running = mode_ != AttemptMode::Terminal;
      observation_.tls_verified = true;
      observation_.websocket_handshake = true;
    }
    if (mode_ == AttemptMode::Terminal) {
      std::lock_guard lock{mutex_};
      observation_.terminal_error = transport::NetworkError{
          transport::NetworkErrorCode::Internal,
          key_.market == common::MARKET_SPOT ? "spot-test-terminal"
                                             : "usdm-test-terminal",
          "deterministic terminal startup failure", std::nullopt, std::nullopt};
      state_->mark_start_completed();
      return transport::TransportStartResult::Failed;
    }
    if (mode_ == AttemptMode::NeverLive) {
      state_->mark_start_completed();
      return transport::TransportStartResult::Started;
    }
    if (runtime_.submit_depth_update(make_update(key_, generation_),
                                     rt::SourceProvenance{generation_}) !=
            rt::AdmissionResult::Accepted ||
        runtime_.submit_snapshot(make_snapshot(key_, generation_),
                                 rt::SourceProvenance{generation_}) !=
            rt::AdmissionResult::Accepted) {
      state_->mark_start_completed();
      return transport::TransportStartResult::Failed;
    }
    std::lock_guard lock{mutex_};
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
    observation_.last_event_utc_ns = 1700000000002000000ULL + generation_;
    state_->mark_start_completed();
    return transport::TransportStartResult::Started;
  }

  [[nodiscard]] bool make_live_for_testing() {
    std::lock_guard lock{mutex_};
    if (mode_ != AttemptMode::NeverLive || stopped_.load() ||
        made_live_.exchange(true)) {
      return false;
    }
    if (runtime_.submit_depth_update(make_update(key_, generation_),
                                     rt::SourceProvenance{generation_}) !=
            rt::AdmissionResult::Accepted ||
        runtime_.submit_snapshot(make_snapshot(key_, generation_),
                                 rt::SourceProvenance{generation_}) !=
            rt::AdmissionResult::Accepted) {
      return false;
    }
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
    observation_.last_event_utc_ns = 1700000000002000000ULL + generation_;
    return true;
  }

  void stop() noexcept override {
    if (stopped_.exchange(true)) {
      return;
    }
    {
      std::lock_guard lock{mutex_};
      observation_.running = false;
      observation_.stopped = true;
    }
    auto *expected = this;
    static_cast<void>(
        state_->active_attempt.compare_exchange_strong(expected, nullptr));
    state_->active.fetch_sub(1U);
  }

  [[nodiscard]] bool inject_recoverable_failure() {
    {
      std::lock_guard lock{mutex_};
      if (stopped_.load()) {
        return false;
      }
      observation_.running = false;
      observation_.terminal_error = transport::NetworkError{
          transport::NetworkErrorCode::WebSocketRead,
          "spot-test\nstage\\source",
          std::string{"recovery\nmessage\\\t\x01"} + std::string(300U, 'x'),
          std::nullopt, std::nullopt};
    }
    return runtime_.submit_transport_failure() == rt::AdmissionResult::Accepted;
  }

  [[nodiscard]] transport::TransportObservation observe() const override {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  rt::MarketRuntime &runtime_;
  const rt::MarketKey key_;
  const std::uint64_t generation_;
  const AttemptMode mode_;
  std::shared_ptr<AttemptState> state_;
  mutable std::mutex mutex_;
  transport::TransportObservation observation_;
  std::atomic<bool> stopped_{false};
  std::atomic<bool> made_live_{false};
};

[[nodiscard]] inline recovery::detail::RecoveryTestOptions
attempt_options(rt::MarketKey key, AttemptMode mode,
                const std::shared_ptr<AttemptState> &state) {
  recovery::detail::RecoveryTestOptions options;
  options.attempt_factory = [key = std::move(key), mode,
                             state](rt::MarketRuntime &runtime,
                                    const rt::RuntimeClock &,
                                    std::uint64_t generation) {
    return std::make_unique<TestAttempt>(runtime, key, generation, mode, state);
  };
  options.backoff_waiter = [](std::chrono::seconds,
                              std::stop_token stop_token) {
    return !stop_token.stop_requested();
  };
  return options;
}

[[nodiscard]] inline composition::ProductRuntimeSpec
make_test_product_spec(rt::MarketKey key, AttemptMode mode,
                       const std::shared_ptr<AttemptState> &state) {
  composition::ProductRuntimeOptions options;
  options.recovery_test = attempt_options(key, mode, state);
  return {std::move(key), numeric_spec(), std::move(options)};
}

struct LiveConfiguration final {
  GatewayOptions gateway;
  std::vector<composition::ProductRuntimeSpec> specifications;
  std::shared_ptr<AttemptState> spot{std::make_shared<AttemptState>()};
  std::shared_ptr<AttemptState> usdm{std::make_shared<AttemptState>()};
};

[[nodiscard]] inline LiveConfiguration
gateway_products(AttemptMode spot_mode = AttemptMode::Live,
                 AttemptMode usdm_mode = AttemptMode::Live) {
  LiveConfiguration result;
  result.gateway.initial_startup_timeout = std::chrono::seconds{2};
  result.gateway.allow_ephemeral_listen_for_testing = true;
  result.specifications.push_back(
      make_test_product_spec(rt::spot_btcusdt_key(), spot_mode, result.spot));
  result.specifications.push_back(
      make_test_product_spec(rt::usdm_btcusdt_key(), usdm_mode, result.usdm));
  return result;
}

} // namespace binance_market_data::gateway::production::test_support
