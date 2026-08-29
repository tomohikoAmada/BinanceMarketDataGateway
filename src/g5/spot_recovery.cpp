#include "spot_recovery.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <condition_variable>
#include <limits>
#include <stdexcept>
#include <thread>
#include <utility>

namespace binance_market_data::gateway::g5 {

namespace {

namespace core = binance_market_data::projection::v1;

class LiveSpotAttempt final : public detail::RecoveryAttempt {
public:
  LiveSpotAttempt(g3::MarketRuntime &runtime, const g3::RuntimeClock &clock,
                  std::uint64_t generation)
      : transport_{runtime, clock, generation} {}

  [[nodiscard]] g4::TransportStartResult start() override {
    return transport_.start();
  }

  void stop() noexcept override { transport_.stop(); }

  [[nodiscard]] g4::TransportObservation observe() const override {
    return transport_.observe();
  }

private:
  g4::SpotTransport transport_;
};

struct RecoveryDecision final {
  bool recoverable{false};
  RecoveryCause cause{RecoveryCause::InternalFailure};
  std::optional<std::chrono::seconds> minimum_delay;
};

[[nodiscard]] RecoveryDecision
classify_failure(const g3::RuntimeObservation &runtime,
                 const g4::TransportObservation &transport) noexcept {
  RecoveryDecision decision;

  if (runtime.state == g3::RuntimeState::NeedsResync) {
    decision.recoverable = true;
    decision.cause = RecoveryCause::NeedsResync;
  } else if (runtime.state == g3::RuntimeState::Faulted &&
             runtime.fault_reason.has_value()) {
    switch (*runtime.fault_reason) {
    case g3::FaultReason::TransportFailure:
      decision = {true, RecoveryCause::TransportFailure, std::nullopt};
      break;
    case g3::FaultReason::SnapshotFailure:
      decision = {true, RecoveryCause::SnapshotFailure, std::nullopt};
      break;
    case g3::FaultReason::IngressOverflow:
      decision = {true, RecoveryCause::IngressOverflow, std::nullopt};
      break;
    case g3::FaultReason::BootstrapBufferOverflow:
      decision = {true, RecoveryCause::BootstrapBufferOverflow, std::nullopt};
      break;
    case g3::FaultReason::AdapterError:
    case g3::FaultReason::ProjectionRejected:
    case g3::FaultReason::ClockError:
    case g3::FaultReason::InternalError:
      return decision;
    }
  } else {
    return decision;
  }

  if (!transport.terminal_error.has_value()) {
    return decision;
  }
  const auto &error = *transport.terminal_error;
  if (error.code == g4::NetworkErrorCode::Internal) {
    return {};
  }
  if (error.code == g4::NetworkErrorCode::Protocol) {
    decision.cause = RecoveryCause::Protocol;
    return decision;
  }
  if (error.code == g4::NetworkErrorCode::ServerShutdown) {
    decision.cause = RecoveryCause::ServerShutdown;
    return decision;
  }
  if (error.code != g4::NetworkErrorCode::HttpStatus) {
    return decision;
  }
  if (!error.http_status.has_value()) {
    return {};
  }

  const auto status = *error.http_status;
  if (status == 429U || status == 418U) {
    const auto retry_after = detail::parse_retry_after(error.retry_after);
    if (!retry_after.has_value()) {
      return {};
    }
    decision.recoverable = true;
    decision.cause =
        status == 429U ? RecoveryCause::Http429 : RecoveryCause::Http418;
    decision.minimum_delay = retry_after;
    return decision;
  }
  if (status >= 500U && status <= 599U) {
    decision.recoverable = true;
    decision.cause = RecoveryCause::Http5xx;
    return decision;
  }
  if (status >= 400U && status <= 499U) {
    decision.recoverable = false;
    decision.cause = RecoveryCause::TerminalHttp4xx;
    return decision;
  }
  return {};
}

[[nodiscard]] g4::NetworkError internal_error(std::string message) {
  return {g4::NetworkErrorCode::Internal, "g5-recovery", std::move(message),
          std::nullopt, std::nullopt};
}

} // namespace

std::optional<std::chrono::seconds>
detail::parse_retry_after(std::optional<std::string> value) noexcept {
  if (!value.has_value() || value->empty()) {
    return std::nullopt;
  }
  std::uint64_t seconds = 0U;
  const auto *begin = value->data();
  const auto *end = begin + value->size();
  const auto [position, error] = std::from_chars(begin, end, seconds);
  if (error != std::errc{} || position != end ||
      seconds > static_cast<std::uint64_t>(
                    std::numeric_limits<std::chrono::seconds::rep>::max())) {
    return std::nullopt;
  }
  return std::chrono::seconds{static_cast<std::chrono::seconds::rep>(seconds)};
}

std::chrono::seconds
detail::normal_backoff_delay(std::size_t recovery_attempt) {
  constexpr std::array delays{
      std::chrono::seconds{1},  std::chrono::seconds{2},
      std::chrono::seconds{4},  std::chrono::seconds{8},
      std::chrono::seconds{16}, std::chrono::seconds{30}};
  if (recovery_attempt == 0U || recovery_attempt > delays.size()) {
    throw std::out_of_range{"G5 recovery attempt is outside bounded policy"};
  }
  return delays[recovery_attempt - 1U];
}

class SpotRecovery::Impl final {
public:
  Impl(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
       detail::RecoveryTestOptions test_options)
      : runtime_{runtime}, clock_{std::move(clock)},
        attempt_factory_{std::move(test_options.attempt_factory)},
        backoff_waiter_{std::move(test_options.backoff_waiter)} {
    if (!clock_) {
      throw std::invalid_argument{"G5 recovery clock must be injected"};
    }
    if (!attempt_factory_) {
      attempt_factory_ = [](g3::MarketRuntime &attempt_runtime,
                            const g3::RuntimeClock &attempt_clock,
                            std::uint64_t generation) {
        return std::make_unique<LiveSpotAttempt>(attempt_runtime, attempt_clock,
                                                 generation);
      };
    }
  }

  ~Impl() { stop(); }

  [[nodiscard]] RecoveryStartResult start() {
    {
      std::lock_guard lock{mutex_};
      if (stopped_) {
        return RecoveryStartResult::Stopped;
      }
      if (started_) {
        return RecoveryStartResult::AlreadyStarted;
      }
      started_ = true;
      observation_.state = RecoveryState::Starting;
    }

    if (runtime_.start() != g3::StartResult::Started) {
      std::lock_guard lock{mutex_};
      observation_.terminal = true;
      observation_.state = RecoveryState::Exhausted;
      observation_.terminal_error =
          internal_error("MarketRuntime did not start for G5");
      condition_.notify_all();
      return RecoveryStartResult::RuntimeStartFailed;
    }

    coordinator_ = std::jthread{
        [this](std::stop_token stop_token) { coordinator_loop(stop_token); }};
    return RecoveryStartResult::Started;
  }

  void stop() noexcept {
    {
      std::lock_guard lock{mutex_};
      if (stopped_) {
        return;
      }
      if (!started_) {
        stopped_ = true;
        observation_.state = RecoveryState::Stopped;
        condition_.notify_all();
        return;
      }
      observation_.state = RecoveryState::Stopping;
      observation_.in_backoff = false;
    }
    condition_.notify_all();
    coordinator_.request_stop();
    backoff_condition_.notify_all();

    std::shared_ptr<detail::RecoveryAttempt> attempt;
    {
      std::lock_guard lock{attempt_mutex_};
      attempt = active_attempt_;
    }
    if (attempt) {
      attempt->stop();
    }
    if (coordinator_.joinable()) {
      try {
        coordinator_.join();
      } catch (...) {
        std::terminate();
      }
    }
    runtime_.stop();
    {
      std::lock_guard lock{mutex_};
      stopped_ = true;
      observation_.state = RecoveryState::Stopped;
      observation_.in_backoff = false;
      observation_.active_transport_count = 0U;
    }
    condition_.notify_all();
  }

  [[nodiscard]] RecoveryObservation observe() const {
    std::lock_guard lock{mutex_};
    return observation_;
  }

  [[nodiscard]] RecoveryObservation
  wait_for_generation_live(std::uint64_t generation) {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this, generation] {
      return (observation_.state == RecoveryState::Live &&
              observation_.connection_generation >= generation) ||
             observation_.terminal || stopped_;
    });
    return observation_;
  }

  [[nodiscard]] RecoveryObservation wait_until_terminal() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return observation_.terminal || stopped_; });
    return observation_;
  }

  [[nodiscard]] bool request_controlled_recovery_for_acceptance() {
    {
      std::lock_guard lock{mutex_};
      if (observation_.state != RecoveryState::Live || stopped_) {
        return false;
      }
    }
    return runtime_.submit_transport_failure() == g3::AdmissionResult::Accepted;
  }

  [[nodiscard]] std::optional<QuiescentAcceptanceCut> quiesce_for_acceptance() {
    std::shared_ptr<detail::RecoveryAttempt> attempt;
    {
      std::lock_guard lock{attempt_mutex_};
      attempt = active_attempt_;
    }
    {
      std::lock_guard lock{mutex_};
      if (!attempt || !started_ || stopped_ || observation_.terminal ||
          observation_.state != RecoveryState::Live) {
        return std::nullopt;
      }
      observation_.state = RecoveryState::Stopping;
      observation_.in_backoff = false;
    }
    condition_.notify_all();
    coordinator_.request_stop();
    backoff_condition_.notify_all();

    // A requested coordinator stop transfers active-attempt quiescence to the
    // lifecycle caller, so this is the only stop owner for the retained source.
    attempt->stop();
    if (coordinator_.joinable()) {
      coordinator_.join();
    }

    // The coordinator is joined, so no later generation can be created. The
    // retained owning reference keeps the final transport observation alive.
    const auto transport = attempt->observe();
    {
      std::lock_guard lock{attempt_mutex_};
      if (active_attempt_ == attempt) {
        active_attempt_.reset();
      }
    }
    {
      std::lock_guard lock{mutex_};
      observation_.active_transport_count = 0U;
      observation_.connection_generation = transport.connection_generation;
      observation_.connection_id = transport.connection_id;
    }
    condition_.notify_all();

    // This owner FIFO barrier follows both source and coordinator quiescence.
    // MarketRuntime intentionally remains alive for acceptance snapshot
    // capture.
    return QuiescentAcceptanceCut{transport, runtime_.observe()};
  }

private:
  void coordinator_loop(std::stop_token stop_token) noexcept {
    std::uint64_t generation = 1U;
    try {
      for (;;) {
        if (stop_token.stop_requested()) {
          break;
        }
        auto attempt = create_attempt(generation);
        if (!attempt) {
          set_terminal(RecoveryCause::InternalFailure,
                       internal_error("attempt factory returned null"), false);
          return;
        }

        const auto start_result = attempt->start();
        if (stop_token.stop_requested()) {
          break;
        }

        std::optional<g3::RuntimeObservation> runtime_observation;
        if (start_result == g4::TransportStartResult::Started) {
          runtime_observation =
              runtime_.wait_until_live_or_recovery_required(stop_token);
          if (!runtime_observation.has_value()) {
            break;
          }
          if (runtime_observation->state == g3::RuntimeState::Live) {
            // terminal_failure() publishes the transport error before admitting
            // the runtime fault. The FIFO barrier covers every earlier runtime
            // admission; the following transport observation covers a terminal
            // condition visible after that barrier. Anything later is a new
            // post-cut failure.
            const auto qualified_runtime = runtime_.observe();
            const auto transport = attempt->observe();
            if (try_mark_live(qualified_runtime, transport, generation,
                              stop_token)) {
              runtime_observation =
                  runtime_.wait_until_recovery_required(stop_token);
              if (!runtime_observation.has_value()) {
                break;
              }
            }
          }
        }

        const auto transport_observation = quiesce_attempt(attempt);
        // This owner barrier is deliberately after the transport network thread
        // has joined. No old-generation producer can submit beyond this cut.
        const auto final_runtime = runtime_.observe();
        const auto decision =
            classify_failure(final_runtime, transport_observation);
        if (!decision.recoverable) {
          set_terminal(decision.cause,
                       transport_observation.terminal_error.value_or(
                           internal_error("nonrecoverable G5 failure")),
                       false);
          return;
        }

        std::size_t recovery_attempt = 0U;
        {
          std::lock_guard lock{mutex_};
          if (observation_.consecutive_recovery_attempts >=
              detail::kMaximumRecoveryAttempts) {
            observation_.last_recovery_cause = decision.cause;
            observation_.state = RecoveryState::Exhausted;
            observation_.terminal = true;
            observation_.exhausted = true;
            observation_.in_backoff = false;
            observation_.terminal_error = transport_observation.terminal_error;
            condition_.notify_all();
            return;
          }
          recovery_attempt = ++observation_.consecutive_recovery_attempts;
          observation_.last_recovery_cause = decision.cause;
          auto delay = detail::normal_backoff_delay(recovery_attempt);
          if (decision.minimum_delay.has_value()) {
            delay = std::max(delay, *decision.minimum_delay);
          }
          observation_.last_requested_delay = delay;
          observation_.state = RecoveryState::Backoff;
          observation_.in_backoff = true;
        }
        condition_.notify_all();

        const auto delay = observe().last_requested_delay;
        if (!wait_backoff(delay, stop_token)) {
          break;
        }
        {
          std::lock_guard lock{mutex_};
          observation_.state = RecoveryState::Recovering;
          observation_.in_backoff = false;
        }
        condition_.notify_all();

        if (runtime_.reset_for_rebootstrap() !=
            g3::RebootstrapResetResult::Reset) {
          set_terminal(RecoveryCause::InternalFailure,
                       internal_error("owner-domain rebootstrap reset failed"),
                       false);
          return;
        }
        ++generation;
        {
          std::lock_guard lock{mutex_};
          ++observation_.total_recovery_count;
        }
      }
    } catch (const std::exception &error) {
      set_terminal(RecoveryCause::InternalFailure, internal_error(error.what()),
                   false);
    } catch (...) {
      set_terminal(RecoveryCause::InternalFailure,
                   internal_error("unknown coordinator exception"), false);
    }
  }

  [[nodiscard]] std::shared_ptr<detail::RecoveryAttempt>
  create_attempt(std::uint64_t generation) {
    auto unique = attempt_factory_(runtime_, clock_, generation);
    if (!unique) {
      return {};
    }
    std::shared_ptr<detail::RecoveryAttempt> attempt{std::move(unique)};
    {
      std::lock_guard lock{attempt_mutex_};
      active_attempt_ = attempt;
    }
    const auto transport = attempt->observe();
    {
      std::lock_guard lock{mutex_};
      observation_.connection_generation = generation;
      observation_.connection_id = transport.connection_id;
      observation_.active_transport_count = 1U;
      observation_.max_active_transport_count =
          std::max(observation_.max_active_transport_count,
                   observation_.active_transport_count);
    }
    condition_.notify_all();
    return attempt;
  }

  [[nodiscard]] g4::TransportObservation quiesce_attempt(
      const std::shared_ptr<detail::RecoveryAttempt> &attempt) noexcept {
    attempt->stop();
    const auto transport = attempt->observe();
    {
      std::lock_guard lock{attempt_mutex_};
      if (active_attempt_ == attempt) {
        active_attempt_.reset();
      }
    }
    {
      std::lock_guard lock{mutex_};
      observation_.active_transport_count = 0U;
      observation_.connection_generation = transport.connection_generation;
      observation_.connection_id = transport.connection_id;
    }
    condition_.notify_all();
    return transport;
  }

  [[nodiscard]] bool try_mark_live(const g3::RuntimeObservation &runtime,
                                   const g4::TransportObservation &transport,
                                   std::uint64_t expected_generation,
                                   std::stop_token stop_token) {
    if (runtime.state != g3::RuntimeState::Live ||
        runtime.projection_status != core::ProjectionStatus::Synchronized ||
        runtime.fault_reason.has_value() || !transport.running ||
        transport.stopped || transport.terminal_error.has_value() ||
        !transport.websocket_handshake || !transport.rest_depth_fetched ||
        transport.connection_generation != expected_generation ||
        stop_token.stop_requested()) {
      return false;
    }
    std::lock_guard lock{mutex_};
    if (stopped_ || observation_.terminal ||
        observation_.state == RecoveryState::Stopping ||
        stop_token.stop_requested()) {
      return false;
    }
    observation_.state = RecoveryState::Live;
    observation_.connection_generation = transport.connection_generation;
    observation_.connection_id = transport.connection_id;
    observation_.consecutive_recovery_attempts = 0U;
    observation_.in_backoff = false;
    observation_.terminal_error.reset();
    condition_.notify_all();
    return true;
  }

  [[nodiscard]] bool wait_backoff(std::chrono::seconds delay,
                                  std::stop_token stop_token) {
    if (backoff_waiter_) {
      return backoff_waiter_(delay, stop_token) && !stop_token.stop_requested();
    }
    std::unique_lock lock{backoff_mutex_};
    static_cast<void>(backoff_condition_.wait_for(lock, stop_token, delay,
                                                  [] { return false; }));
    return !stop_token.stop_requested();
  }

  void set_terminal(RecoveryCause cause, g4::NetworkError error,
                    bool exhausted) noexcept {
    std::lock_guard lock{mutex_};
    observation_.last_recovery_cause = cause;
    observation_.state = RecoveryState::Exhausted;
    observation_.terminal = true;
    observation_.exhausted = exhausted;
    observation_.in_backoff = false;
    observation_.terminal_error = std::move(error);
    condition_.notify_all();
  }

  g3::MarketRuntime &runtime_;
  g3::RuntimeClock clock_;
  detail::AttemptFactory attempt_factory_;
  detail::BackoffWaiter backoff_waiter_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  RecoveryObservation observation_;
  std::jthread coordinator_;
  bool started_{false};
  bool stopped_{false};

  std::mutex attempt_mutex_;
  std::shared_ptr<detail::RecoveryAttempt> active_attempt_;
  std::mutex backoff_mutex_;
  std::condition_variable_any backoff_condition_;
};

SpotRecovery::SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                           detail::RecoveryTestOptions test_options)
    : impl_{std::make_unique<Impl>(runtime, std::move(clock),
                                   std::move(test_options))} {}

SpotRecovery::~SpotRecovery() = default;

RecoveryStartResult SpotRecovery::start() { return impl_->start(); }

void SpotRecovery::stop() noexcept { impl_->stop(); }

RecoveryObservation SpotRecovery::observe() const { return impl_->observe(); }

RecoveryObservation
SpotRecovery::wait_for_generation_live(std::uint64_t generation) {
  return impl_->wait_for_generation_live(generation);
}

RecoveryObservation SpotRecovery::wait_until_terminal() {
  return impl_->wait_until_terminal();
}

bool SpotRecovery::request_controlled_recovery_for_acceptance() {
  return impl_->request_controlled_recovery_for_acceptance();
}

std::optional<QuiescentAcceptanceCut> SpotRecovery::quiesce_for_acceptance() {
  return impl_->quiesce_for_acceptance();
}

} // namespace binance_market_data::gateway::g5
