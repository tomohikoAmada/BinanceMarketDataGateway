#pragma once

#include "production/daemon_config.hpp"
#include "production/production_metadata.hpp"
#include "protocol/usdm_protocol.hpp"
#include "support/production_test_support.hpp"

#include <nlohmann/json.hpp>

#include <array>
#include <fstream>
#include <limits>
namespace composition = binance_market_data::gateway::composition;
namespace protocol = binance_market_data::gateway::protocol;
namespace transport = binance_market_data::gateway::transport;
namespace recovery = binance_market_data::gateway::recovery;
namespace production = binance_market_data::gateway::production;
namespace rt = binance_market_data::gateway::runtime;

namespace binance_market_data::gateway::production::four_product_test {
namespace support = test_support;
using Json = nlohmann::json;

inline void check(bool condition) {
  if (!condition) {
    throw std::runtime_error{"four-product fixture invariant failed"};
  }
}

struct Sample final {
  std::string bid;
  std::string ask;
  std::string quantity;
  std::string tick;
  std::string step;
};

inline Sample sample(const rt::MarketKey &key) {
  if (key.market == support::common::MARKET_SPOT) {
    return key.symbol == "BTCUSDT"
               ? Sample{"101.23", "102.34", "1.234", "0.01", "0.001"}
               : Sample{"201.2", "202.3", "2.34", "0.1", "0.01"};
  }
  return key.symbol == "BTCUSDT"
             ? Sample{"301.2", "302.3", "3.456", "0.1", "0.001"}
             : Sample{"401.23", "402.34", "4.56", "0.01", "0.01"};
}

inline std::string exchange_info(support::common::Market market) {
  Json symbols = Json::array();
  for (const auto symbol : {"BTCUSDT", "ETHUSDT"}) {
    const auto values =
        sample({support::common::VENUE_BINANCE, market, symbol});
    symbols.push_back(
        {{"symbol", symbol},
         {"pair", symbol},
         {"status", "TRADING"},
         {"contractType", "PERPETUAL"},
         {"isSpotTradingAllowed", true},
         {"filters",
          Json::array(
              {{{"filterType", "PRICE_FILTER"}, {"tickSize", values.tick}},
               {{"filterType", "LOT_SIZE"}, {"stepSize", values.step}}})}});
  }
  return Json{{"symbols", symbols}}.dump();
}

inline support::market::DepthUpdate depth(const rt::MarketKey &key,
                                          std::uint64_t generation,
                                          std::uint64_t id,
                                          std::optional<std::uint64_t> pu = {},
                                          std::optional<Sample> selected = {}) {
  const auto values = selected.value_or(sample(key));
  Json source{{"e", "depthUpdate"},
              {"E", 1700000000000ULL + id},
              {"s", key.symbol},
              {"U", id},
              {"u", id},
              {"b", Json::array({{values.bid, values.quantity}})},
              {"a", Json::array()}};
  const auto connection = key.symbol + "-g" + std::to_string(generation);
  const rt::ClockSample received{1700000000000000000ULL + id, 9000000ULL + id};
  if (key.market == support::common::MARKET_SPOT) {
    const auto routes = transport::make_spot_transport_routes(key.symbol);
    check(routes.has_value());
    check(routes->depth_target ==
          "/api/v3/depth?symbol=" + key.symbol + "&limit=5000");
    const auto combined =
        Json{{"stream", routes->diff_depth_stream}, {"data", source}}.dump();
    auto parsed = protocol::parse_combined_event_frame(combined, received,
                                                       connection, key.symbol);
    check(std::holds_alternative<protocol::NormalizedMarketEvent>(parsed));
    return std::get<support::market::DepthUpdate>(
        std::get<protocol::NormalizedMarketEvent>(std::move(parsed)));
  }
  const auto routes = transport::make_usdm_transport_routes(key.symbol);
  check(routes.has_value());
  check(routes->raw_websocket_target ==
        "/public/ws/" + *protocol::usdm_stream_symbol(key.symbol) +
            "@depth@100ms");
  source["U"] = id - 1U;
  source["T"] = 1700000000001ULL + id;
  source["pu"] = pu.value_or(id - 1U);
  auto parsed = protocol::parse_usdm_depth_frame(source.dump(), received,
                                                 connection, key.symbol);
  check(std::holds_alternative<support::market::DepthUpdate>(parsed));
  return std::get<support::market::DepthUpdate>(std::move(parsed));
}

inline support::market::ExchangeDepthSnapshot
snapshot(const rt::MarketKey &key, std::uint64_t generation,
         std::optional<Sample> selected = {}) {
  const auto values = selected.value_or(sample(key));
  Json source{{"lastUpdateId", 100U},
              {"bids", Json::array({{values.bid, values.quantity}})},
              {"asks", Json::array({{values.ask, values.quantity}})}};
  const rt::ClockSample received{1700000000000000100ULL, 9000100ULL};
  const auto request = key.symbol + "-snapshot-g" + std::to_string(generation);
  auto parsed = key.market == support::common::MARKET_SPOT
                    ? protocol::parse_depth_snapshot(source.dump(), received,
                                                     request, key.symbol)
                    : protocol::parse_usdm_depth_snapshot(
                          source.dump(), received, request, key.symbol);
  check(std::holds_alternative<support::market::ExchangeDepthSnapshot>(parsed));
  return std::get<support::market::ExchangeDepthSnapshot>(std::move(parsed));
}

// Only the external I/O boundary is replaced. Production config, metadata,
// route/parser functions, owner/Projection, recovery and RPC handlers run
// normally.
class Attempt final : public recovery::detail::RecoveryAttempt {
public:
  Attempt(rt::MarketRuntime &runtime, rt::MarketKey key,
          std::uint64_t generation, support::AttemptMode mode,
          std::shared_ptr<support::AttemptState> state,
          std::optional<Sample> selected = {})
      : runtime_{runtime}, key_{std::move(key)}, generation_{generation},
        mode_{mode}, state_{std::move(state)}, selected_{std::move(selected)} {
    const auto active = state_->active.fetch_add(1U) + 1U;
    auto maximum = state_->maximum_active.load();
    while (maximum < active &&
           !state_->maximum_active.compare_exchange_weak(maximum, active)) {
    }
  }
  ~Attempt() override { stop(); }
  transport::TransportStartResult start() override {
    state_->starts.fetch_add(1U);
    if (mode_ == support::AttemptMode::Terminal) {
      return transport::TransportStartResult::Failed;
    }
    if (mode_ == support::AttemptMode::Live) {
      check(runtime_.submit_depth_update(
                depth(key_, generation_, 101U, {}, selected_), {generation_}) ==
            rt::AdmissionResult::Accepted);
      check(runtime_.submit_snapshot(snapshot(key_, generation_, selected_),
                                     {generation_}) ==
            rt::AdmissionResult::Accepted);
    }
    return transport::TransportStartResult::Started;
  }
  void stop() noexcept override {
    if (!stopped_.exchange(true)) {
      state_->active.fetch_sub(1U);
    }
  }
  transport::TransportObservation observe() const override {
    transport::TransportObservation result;
    result.started = true;
    result.running =
        !stopped_.load() && mode_ != support::AttemptMode::Terminal;
    result.stopped = stopped_.load();
    result.tls_verified = true;
    result.websocket_handshake = true;
    result.rest_depth_fetched = mode_ == support::AttemptMode::Live;
    result.depth_frame_count = result.rest_depth_fetched ? 1U : 0U;
    result.connection_id = key_.symbol + "-g" + std::to_string(generation_);
    result.connection_generation = generation_;
    if (mode_ == support::AttemptMode::Terminal) {
      result.terminal_error =
          transport::NetworkError{transport::NetworkErrorCode::Internal,
                                  "four-product",
                                  "startup failure",
                                  {},
                                  {}};
    }
    return result;
  }

private:
  rt::MarketRuntime &runtime_;
  const rt::MarketKey key_;
  const std::uint64_t generation_;
  const support::AttemptMode mode_;
  const std::shared_ptr<support::AttemptState> state_;
  const std::optional<Sample> selected_;
  std::atomic<bool> stopped_{false};
};

struct Configuration final {
  DaemonConfig config;
  std::vector<composition::ProductRuntimeSpec> specs;
  std::vector<std::shared_ptr<support::AttemptState>> attempts;
};

inline Configuration configure_with_metadata(
    DaemonConfig config, const std::string &spot_body,
    const std::string &usdm_body, const std::vector<Sample> &samples,
    std::size_t fail_index = std::numeric_limits<std::size_t>::max(),
    support::AttemptMode mode = support::AttemptMode::Terminal) {
  Configuration result;
  result.config = std::move(config);
  int spot_fetches = 0;
  int usdm_fetches = 0;
  auto metadata = acquire_production_metadata(
      result.config.market_keys,
      {[&] {
         ++spot_fetches;
         return transport::ExchangeInfoResult{
             transport::ExchangeInfoResponse{spot_body, true}};
       },
       [&] {
         ++usdm_fetches;
         return transport::ExchangeInfoResult{
             transport::ExchangeInfoResponse{usdm_body, true}};
       }});
  check(spot_fetches == 1 && usdm_fetches == 1);
  check(std::holds_alternative<ProductionMetadata>(metadata));
  result.specs = make_product_runtime_specs(
      std::get<ProductionMetadata>(std::move(metadata)));
  check(result.specs.size() == result.config.market_keys.size());
  check(samples.empty() || samples.size() == result.specs.size());
  result.attempts.resize(result.specs.size());
  for (std::size_t i = 0; i < result.specs.size(); ++i) {
    auto state = std::make_shared<support::AttemptState>();
    result.attempts[i] = state;
    auto &spec = result.specs[i];
    spec.options.recovery_test.attempt_factory =
        [key = spec.key, state,
         sample = samples.empty() ? std::optional<Sample>{} : samples[i],
         selected = i == fail_index ? mode : support::AttemptMode::Live](
            rt::MarketRuntime &runtime, const rt::RuntimeClock &,
            std::uint64_t generation) {
          return std::make_unique<Attempt>(runtime, key, generation, selected,
                                           state, sample);
        };
    spec.options.recovery_test.backoff_waiter = [](std::chrono::seconds,
                                                   std::stop_token token) {
      return !token.stop_requested();
    };
  }
  return result;
}

inline Configuration
configure(DaemonConfig config, std::size_t fail_index = 4U,
          support::AttemptMode mode = support::AttemptMode::Terminal) {
  return configure_with_metadata(
      std::move(config), exchange_info(support::common::MARKET_SPOT),
      exchange_info(support::common::MARKET_USD_M_PERPETUAL), {}, fail_index,
      mode);
}

inline Configuration configure_file(const std::string &path) {
  auto config = load_daemon_config(path);
  check(std::holds_alternative<DaemonConfig>(config));
  return configure(std::get<DaemonConfig>(std::move(config)));
}
} // namespace binance_market_data::gateway::production::four_product_test
