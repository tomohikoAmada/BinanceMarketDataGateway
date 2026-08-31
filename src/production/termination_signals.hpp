#pragma once

#include <condition_variable>
#include <csignal>
#include <mutex>
#include <thread>

namespace binance_market_data::gateway::production {

// SIGINT/SIGTERM are blocked before any runtime thread is created. A single
// sigwait loop records delivery and has an explicit teardown wake signal; no
// asynchronous handler runs.
class TerminationSignals final {
public:
  TerminationSignals();
  ~TerminationSignals();

  TerminationSignals(const TerminationSignals &) = delete;
  TerminationSignals &operator=(const TerminationSignals &) = delete;

  [[nodiscard]] bool requested() const noexcept;
  [[nodiscard]] int wait() noexcept;

private:
  void loop(std::stop_token stop_token) noexcept;

  sigset_t signal_set_{};
  sigset_t previous_mask_{};
  std::jthread waiter_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  bool requested_{false};
  int signal_{0};
};

} // namespace binance_market_data::gateway::production
