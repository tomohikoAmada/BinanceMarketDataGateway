#include "termination_signals.hpp"

#include <pthread.h>
#include <system_error>

namespace binance_market_data::gateway::production {

TerminationSignals::TerminationSignals() {
  sigemptyset(&signal_set_);
  sigaddset(&signal_set_, SIGINT);
  sigaddset(&signal_set_, SIGTERM);
  sigaddset(&signal_set_, SIGUSR1);
  const auto result = pthread_sigmask(SIG_BLOCK, &signal_set_, &previous_mask_);
  if (result != 0) {
    throw std::system_error{result, std::generic_category(),
                            "failed to block termination signals"};
  }
  try {
    waiter_ =
        std::jthread{[this](std::stop_token stop_token) { loop(stop_token); }};
  } catch (...) {
    static_cast<void>(pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr));
    throw;
  }
}

TerminationSignals::~TerminationSignals() {
  waiter_.request_stop();
  if (waiter_.joinable()) {
    static_cast<void>(pthread_kill(waiter_.native_handle(), SIGUSR1));
    waiter_.join();
  }
  static_cast<void>(pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr));
}

bool TerminationSignals::requested() const noexcept {
  std::lock_guard lock{mutex_};
  return requested_;
}

int TerminationSignals::wait() noexcept {
  std::unique_lock lock{mutex_};
  condition_.wait(lock, [this] { return requested_; });
  return signal_;
}

void TerminationSignals::loop(std::stop_token stop_token) noexcept {
  while (!stop_token.stop_requested()) {
    int received = 0;
    const auto result = sigwait(&signal_set_, &received);
    if (result == 0 && (received == SIGINT || received == SIGTERM)) {
      {
        std::lock_guard lock{mutex_};
        if (!requested_) {
          requested_ = true;
          signal_ = received;
        }
      }
      condition_.notify_all();
      continue;
    }
    if (result == 0 && received == SIGUSR1) {
      continue;
    }
    if (result != 0) {
      {
        std::lock_guard lock{mutex_};
        requested_ = true;
        signal_ = -1;
      }
      condition_.notify_all();
      return;
    }
  }
}

} // namespace binance_market_data::gateway::production
