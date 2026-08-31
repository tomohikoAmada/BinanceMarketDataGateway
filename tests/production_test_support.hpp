#pragma once

#include "production_gateway.hpp"
#include "production_metadata.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/market/v1/market_events.pb.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>

namespace binance_market_data::gateway::production::test_support {

namespace common = ::binance_market_data::common::v1;
namespace market = ::binance_market_data::market::v1;

enum class AttemptMode : std::uint8_t {
  Live,
  Terminal,
  NeverLive,
};

struct AttemptState final {
  std::atomic<std::size_t> active{0U};
  std::atomic<std::size_t> maximum_active{0U};
  std::atomic<std::size_t> starts{0U};
};

[[nodiscard]] inline g3::RuntimeClock fixed_clock() {
  return
      [] { return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
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
make_update(common::Market product, std::uint64_t generation,
            std::uint64_t final_id = 101U, bool wrong_symbol = false) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(product);
  metadata->set_symbol(wrong_symbol ? "ETHUSDT" : "BTCUSDT");
  metadata->set_producer("gateway-production-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id(
      (product == common::MARKET_SPOT ? "spot-test-g" : "usdm-test-g") +
      std::to_string(generation));
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(1700000000002ULL + final_id);
  metadata->set_receive_time_utc_ns(1700000000002000000ULL + final_id);
  metadata->set_receive_monotonic_ns(9000000000002ULL + final_id);
  update.set_first_update_id(
      product == common::MARKET_USD_M_PERPETUAL ? final_id - 1U : final_id);
  update.set_final_update_id(final_id);
  if (product == common::MARKET_USD_M_PERPETUAL) {
    update.set_previous_final_update_id(final_id - 1U);
  }
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("4.000");
  return update;
}

[[nodiscard]] inline market::ExchangeDepthSnapshot
make_snapshot(common::Market product, std::uint64_t generation) {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(product);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-production-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id(
      (product == common::MARKET_SPOT ? "spot-snapshot-g" : "usdm-snapshot-g") +
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

class TestAttempt final : public g5::detail::RecoveryAttempt {
public:
  TestAttempt(g3::MarketRuntime &runtime, common::Market product,
              std::uint64_t generation, AttemptMode mode,
              std::shared_ptr<AttemptState> state)
      : runtime_{runtime}, product_{product}, generation_{generation},
        mode_{mode}, state_{std::move(state)} {
    observation_.connection_generation = generation_;
    observation_.connection_id =
        (product_ == common::MARKET_SPOT ? "spot-test-g" : "usdm-test-g") +
        std::to_string(generation_);
    const auto active = state_->active.fetch_add(1U) + 1U;
    auto maximum = state_->maximum_active.load();
    while (maximum < active &&
           !state_->maximum_active.compare_exchange_weak(maximum, active)) {
    }
  }

  ~TestAttempt() override { stop(); }

  [[nodiscard]] g4::TransportStartResult start() override {
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
      observation_.terminal_error = g4::NetworkError{
          g4::NetworkErrorCode::Internal,
          product_ == common::MARKET_SPOT ? "spot-test-terminal"
                                          : "usdm-test-terminal",
          "deterministic terminal startup failure", std::nullopt, std::nullopt};
      return g4::TransportStartResult::Failed;
    }
    if (mode_ == AttemptMode::NeverLive) {
      return g4::TransportStartResult::Started;
    }
    if (runtime_.submit_depth_update(make_update(product_, generation_),
                                     g3::SourceProvenance{generation_}) !=
            g3::AdmissionResult::Accepted ||
        runtime_.submit_snapshot(make_snapshot(product_, generation_),
                                 g3::SourceProvenance{generation_}) !=
            g3::AdmissionResult::Accepted) {
      return g4::TransportStartResult::Failed;
    }
    std::lock_guard lock{mutex_};
    observation_.rest_depth_fetched = true;
    observation_.depth_frame_count = 1U;
    observation_.last_event_utc_ns = 1700000000002000000ULL + generation_;
    return g4::TransportStartResult::Started;
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
    state_->active.fetch_sub(1U);
  }

  [[nodiscard]] g4::TransportObservation observe() const override {
    std::lock_guard lock{mutex_};
    return observation_;
  }

private:
  g3::MarketRuntime &runtime_;
  const common::Market product_;
  const std::uint64_t generation_;
  const AttemptMode mode_;
  std::shared_ptr<AttemptState> state_;
  mutable std::mutex mutex_;
  g4::TransportObservation observation_;
  std::atomic<bool> stopped_{false};
};

[[nodiscard]] inline g5::detail::RecoveryTestOptions
attempt_options(common::Market product, AttemptMode mode,
                const std::shared_ptr<AttemptState> &state) {
  g5::detail::RecoveryTestOptions options;
  options.attempt_factory = [product, mode, state](g3::MarketRuntime &runtime,
                                                   const g3::RuntimeClock &,
                                                   std::uint64_t generation) {
    return std::make_unique<TestAttempt>(runtime, product, generation, mode,
                                         state);
  };
  options.backoff_waiter = [](std::chrono::seconds,
                              std::stop_token stop_token) {
    return !stop_token.stop_requested();
  };
  return options;
}

struct LiveOptions final {
  GatewayOptions gateway;
  std::shared_ptr<AttemptState> spot{std::make_shared<AttemptState>()};
  std::shared_ptr<AttemptState> usdm{std::make_shared<AttemptState>()};
};

[[nodiscard]] inline LiveOptions
gateway_options(AttemptMode spot_mode = AttemptMode::Live,
                AttemptMode usdm_mode = AttemptMode::Live) {
  LiveOptions result;
  result.gateway.initial_startup_timeout = std::chrono::seconds{2};
  result.gateway.allow_ephemeral_listen_for_testing = true;
  result.gateway.products.spot.recovery_test =
      attempt_options(common::MARKET_SPOT, spot_mode, result.spot);
  result.gateway.products.usdm.recovery_test =
      attempt_options(common::MARKET_USD_M_PERPETUAL, usdm_mode, result.usdm);
  return result;
}

[[nodiscard]] inline ProductionMetadata metadata() {
  return {numeric_spec(), numeric_spec()};
}

} // namespace binance_market_data::gateway::production::test_support
