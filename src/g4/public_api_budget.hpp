#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace binance_market_data::gateway::g4::detail {

// Fixed Gateway share alongside ONE Recorder. Rolling windows count failed
// attempts too; reconnect/Live transitions do not reset them. Deployment must
// exclude overlapping processes and allow a full quiet window after restart.
class PublicApiBudget final {
public:
  using Clock = std::chrono::steady_clock;
  using Duration = Clock::duration;

  [[nodiscard]] Duration admit(bool usdm, bool websocket, unsigned cost,
                               Clock::time_point now = Clock::now()) {
    std::lock_guard lock{mutex_};
    auto &market = markets_[usdm ? 1U : 0U];
    auto &window = websocket ? market.connections : market.weight;
    const auto delay = admission_delay(market, websocket, cost, now);
    if (delay > Duration::zero()) {
      return delay;
    }
    window.events.emplace_back(now, cost);
    return Duration::zero();
  }

  [[nodiscard]] Duration delay(bool usdm, bool websocket, unsigned cost,
                               Clock::time_point now = Clock::now()) {
    std::lock_guard lock{mutex_};
    return admission_delay(markets_[usdm ? 1U : 0U], websocket, cost, now);
  }

  void reject(bool usdm, unsigned status, std::string_view retry_after,
              Clock::time_point now = Clock::now()) {
    if (status != 429U && status != 418U) {
      return;
    }
    std::uint64_t seconds = status == 418U ? 86400U : 60U;
    std::uint64_t parsed{};
    const auto result = std::from_chars(
        retry_after.data(), retry_after.data() + retry_after.size(), parsed);
    if (result.ec == std::errc{} &&
        result.ptr == retry_after.data() + retry_after.size()) {
      seconds = parsed;
    }
    const auto maximum = std::chrono::duration_cast<std::chrono::seconds>(
                             Clock::time_point::max() - now)
                             .count();
    const auto until = seconds >= static_cast<std::uint64_t>(maximum)
                           ? Clock::time_point::max()
                           : now + std::chrono::seconds{seconds};
    std::lock_guard lock{mutex_};
    auto &blocked = markets_[usdm ? 1U : 0U].blocked_until;
    blocked = std::max(blocked, until);
  }

private:
  struct Window {
    unsigned capacity;
    std::chrono::seconds period;
    std::deque<std::pair<Clock::time_point, unsigned>> events;

    [[nodiscard]] Duration delay(unsigned cost, Clock::time_point now) {
      if (cost == 0U || cost > capacity) {
        throw std::invalid_argument{"request cost exceeds Gateway share"};
      }
      while (!events.empty() && events.front().first + period <= now) {
        events.pop_front();
      }
      unsigned total = cost;
      for (const auto &[at, value] : events) {
        static_cast<void>(at);
        total += value;
      }
      for (const auto &[at, value] : events) {
        if (total <= capacity) {
          break;
        }
        total -= value;
        if (total <= capacity) {
          return at + period - now;
        }
      }
      return Duration::zero();
    }
  };
  struct Market {
    Window weight;
    Window connections{60U, std::chrono::seconds{300}, {}};
    Clock::time_point blocked_until{};
  };
  [[nodiscard]] static Duration admission_delay(Market &market, bool websocket,
                                                unsigned cost,
                                                Clock::time_point now) {
    auto &window = websocket ? market.connections : market.weight;
    const auto block = market.blocked_until > now ? market.blocked_until - now
                                                  : Duration::zero();
    return std::max(window.delay(cost, now), block);
  }
  std::mutex mutex_;
  std::array<Market, 2> markets_{
      Market{Window{3000U, std::chrono::seconds{60}, {}},
             Window{60U, std::chrono::seconds{300}, {}},
             {}},
      Market{Window{600U, std::chrono::seconds{60}, {}},
             Window{60U, std::chrono::seconds{300}, {}},
             {}}};
};

[[nodiscard]] inline PublicApiBudget &shared_public_api_budget() {
  static PublicApiBudget budget;
  return budget;
}

[[nodiscard]] inline bool is_usdm_host(std::string_view host) {
  return host == "fapi.binance.com" || host == "fstream.binance.com";
}

[[nodiscard]] inline unsigned public_rest_weight(std::string_view host,
                                                 std::string_view target) {
  if (target.starts_with("/api/v3/depth")) {
    return 250U; // Gateway's fixed limit=5000.
  }
  if (target.starts_with("/fapi/v1/depth")) {
    return 20U; // Gateway's fixed limit=1000.
  }
  return is_usdm_host(host) ? 1U : 20U; // exchangeInfo.
}

} // namespace binance_market_data::gateway::g4::detail
