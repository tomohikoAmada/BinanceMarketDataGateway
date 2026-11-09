#pragma once

#include "support/four_product_test_support.hpp"
namespace production = binance_market_data::gateway::production;
namespace rt = binance_market_data::gateway::runtime;

namespace binance_market_data::gateway::production::p1_test {
namespace fixture = four_product_test;

// Test data arithmetic only. Prices and quantities never pass through float.
inline std::string multiple(const std::string &unit, std::uint64_t count) {
  auto digits = unit;
  const auto point = digits.find('.');
  const auto scale =
      point == std::string::npos ? 0U : digits.size() - point - 1U;
  if (point != std::string::npos)
    digits.erase(point, 1U);
  auto value = std::to_string(std::stoull(digits) * count);
  if (scale != 0U) {
    if (value.size() <= scale)
      value.insert(0U, scale + 1U - value.size(), '0');
    value.insert(value.size() - scale, 1U, '.');
  }
  return value;
}

struct Workload final {
  rt::MarketKey key;
  std::size_t depth;
  std::size_t batch;
  std::size_t count;
  std::size_t interval;
  std::string tick;
  std::string step;
  std::uint64_t base_ticks;
  std::uint64_t ask_gap{10U};

  std::string bid(std::size_t i) const {
    return multiple(tick, base_ticks + depth - i);
  }
  std::string ask(std::size_t i) const {
    return multiple(tick, base_ticks + depth + ask_gap + i);
  }
  fixture::Sample sample() const {
    return {bid(0U), ask(0U), step, tick, step};
  }
};

struct Profile final {
  fixture::Json source;
  std::vector<Workload> products;

  fixture::Configuration
  configure(const std::string &config_path,
            std::size_t fail_index = std::numeric_limits<std::size_t>::max(),
            fixture::support::AttemptMode mode =
                fixture::support::AttemptMode::Terminal) const {
    auto parsed = load_daemon_config(config_path);
    fixture::check(std::holds_alternative<DaemonConfig>(parsed));
    auto config = std::get<DaemonConfig>(std::move(parsed));
    fixture::check(config.market_keys.size() == products.size());
    std::vector<fixture::Sample> samples;
    for (std::size_t i = 0U; i < products.size(); ++i) {
      fixture::check(config.market_keys[i] == products[i].key);
      samples.push_back(products[i].sample());
    }
    return fixture::configure_with_metadata(
        std::move(config), source.at("spot_exchange_info").dump(),
        source.at("usdm_exchange_info").dump(), samples, fail_index, mode);
  }
};

inline Profile load(const std::string &path, const std::string &scenario) {
  std::ifstream input{path};
  Profile result;
  input >> result.source;
  for (const auto &p : result.source.at("products")) {
    const auto &w = p.at("workloads").at(scenario);
    result.products.push_back(
        {{fixture::support::common::VENUE_BINANCE,
          p.at("market") == "SPOT"
              ? fixture::support::common::MARKET_SPOT
              : fixture::support::common::MARKET_USD_M_PERPETUAL,
          p.at("symbol").get<std::string>()},
         w.at("depth"),
         w.at("batch"),
         w.at("count"),
         w.at("interval_us"),
         p.at("tick"),
         p.at("step"),
         p.at("base_ticks")});
  }
  fixture::check(result.products.size() == 20U);
  return result;
}
} // namespace binance_market_data::gateway::production::p1_test
