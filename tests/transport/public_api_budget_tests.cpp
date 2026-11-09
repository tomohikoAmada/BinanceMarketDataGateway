#include "transport/public_api_budget.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
namespace transport = binance_market_data::gateway::transport;

namespace detail = binance_market_data::gateway::transport::detail;
using Budget = detail::PublicApiBudget;
using namespace std::chrono_literals;

void require(bool value) {
  if (!value) {
    throw std::runtime_error{"public API budget assertion failed"};
  }
}

// Virtual-clock probe used by the Recorder's two-process joint proof. It uses
// the exact production policy/route weights; it does not contact an endpoint.
int trace() {
  struct Operation {
    std::int64_t at;
    unsigned id;
    bool usdm;
    char kind;
    unsigned status;
    std::string retry;
  };
  const auto later = [](const Operation &a, const Operation &b) {
    return a.at == b.at ? a.id > b.id : a.at > b.at;
  };
  std::priority_queue<Operation, std::vector<Operation>, decltype(later)> work{
      later};
  Operation op{};
  while (std::cin >> op.at >> op.id >> op.usdm >> op.kind >> op.status >>
         op.retry) {
    work.push(op);
  }
  Budget budget;
  while (!work.empty()) {
    op = work.top();
    work.pop();
    const auto now =
        Budget::Clock::time_point{std::chrono::milliseconds{op.at}};
    const auto host = op.usdm ? "fapi.binance.com" : "api.binance.com";
    const auto target = op.kind == 'D' ? (op.usdm ? "/fapi/v1/depth?limit=1000"
                                                  : "/api/v3/depth?limit=5000")
                                       : (op.usdm ? "/fapi/v1/exchangeInfo"
                                                  : "/api/v3/exchangeInfo");
    const auto cost =
        op.kind == 'W' ? 1U : detail::public_rest_weight(host, target);
    const auto delay = budget.admit(op.usdm, op.kind == 'W', cost, now);
    if (delay > Budget::Duration::zero()) {
      op.at += std::chrono::ceil<std::chrono::milliseconds>(delay).count();
      work.push(op);
      continue;
    }
    std::cout << op.id << ' ' << op.at << ' ' << op.usdm << ' ' << op.kind
              << ' ' << cost << '\n';
    budget.reject(op.usdm, op.status, op.retry, now);
  }
  return 0;
}

int main(int argc, char **argv) {
  if (argc == 2 && std::string{argv[1]} == "--trace") {
    return trace();
  }
  const Budget::Clock::time_point zero{};
  Budget budget;
  for (unsigned i = 0; i < 12; ++i) {
    require(budget.admit(false, false, 250, zero) == 0s);
  }
  require(budget.admit(false, false, 20, zero + 59999ms) == 1ms);
  require(budget.admit(false, false, 20, zero + 60s) == 0s);
  require(budget.admit(true, false, 20, zero) == 0s);
  for (unsigned i = 0; i < 60; ++i) {
    require(budget.admit(false, true, 1, zero) == 0s);
  }
  require(budget.admit(false, true, 1, zero + 299s) == 1s);
  require(budget.admit(false, true, 1, zero + 300s) == 0s);
  budget.reject(true, 418, "42", zero);
  budget.reject(true, 429, "1", zero);
  require(budget.admit(true, false, 20, zero + 41s) == 1s);
  require(budget.admit(true, false, 20, zero + 42s) == 0s);
  budget.reject(true, 418, "", zero + 50s);
  require(budget.admit(true, false, 1, zero + 51s) == 86399s);
  bool invalid = false;
  try {
    static_cast<void>(budget.admit(false, false, 3001, zero));
  } catch (const std::invalid_argument &) {
    invalid = true;
  }
  require(invalid);

  Budget concurrent;
  std::atomic<unsigned> admitted{0};
  std::vector<std::thread> threads;
  for (unsigned i = 0; i < 20; ++i) {
    threads.emplace_back([&] {
      if (concurrent.admit(false, false, 250, zero) == 0s) {
        ++admitted;
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  require(admitted == 12U);
  std::cout << "ROLLING_WEIGHT_AND_ATTEMPT_BOUNDARIES=PASS\n"
               "MARKET_ISOLATION_AND_429_418_COOLDOWN=PASS\n"
               "CONCURRENT_OWNER_ADMISSION=PASS\n";
}
