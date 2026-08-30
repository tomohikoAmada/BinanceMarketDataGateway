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
                  std::uint64_t generation, g4::SpotTransportOptions options)
      : transport_{runtime, clock, generation, std::move(options)} {}

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
  } else if (transport.terminal_error.has_value()) {
    decision.recoverable = true;
    decision.cause = RecoveryCause::TransportFailure;
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
       std::optional<PlannedRotationPolicy> planned_rotation,
       RecoveryOptions options, detail::RecoveryTestOptions test_options)
      : runtime_{runtime}, clock_{std::move(clock)},
        planned_rotation_{planned_rotation},
        transport_options_{std::move(options.transport)},
        source_generation_lifecycle_{
            std::move(options.source_generation_lifecycle)},
        attempt_factory_{std::move(test_options.attempt_factory)},
        backoff_waiter_{std::move(test_options.backoff_waiter)},
        rotation_waiter_{std::move(test_options.rotation_waiter)},
        lifecycle_shutdown_established_{
            std::move(test_options.lifecycle_shutdown_established)},
        before_backoff_commit_{std::move(test_options.before_backoff_commit)},
        before_planned_reset_{std::move(test_options.before_planned_reset)} {
    if (!clock_) {
      throw std::invalid_argument{"G5 recovery clock must be injected"};
    }
    if (planned_rotation_.has_value() &&
        planned_rotation_->age <= std::chrono::nanoseconds::zero()) {
      throw std::invalid_argument{"G6 planned rotation age must be positive"};
    }
    const auto lifecycle_callback_count =
        static_cast<unsigned>(
            static_cast<bool>(source_generation_lifecycle_.open)) +
        static_cast<unsigned>(
            static_cast<bool>(source_generation_lifecycle_.quiesce)) +
        static_cast<unsigned>(
            static_cast<bool>(source_generation_lifecycle_.close));
    if (lifecycle_callback_count != 0U && lifecycle_callback_count != 3U) {
      throw std::invalid_argument{
          "G9 source generation lifecycle callbacks must be all-or-none"};
    }
    if (!attempt_factory_) {
      attempt_factory_ = [transport_options = transport_options_](
                             g3::MarketRuntime &attempt_runtime,
                             const g3::RuntimeClock &attempt_clock,
                             std::uint64_t generation) {
        return std::make_unique<LiveSpotAttempt>(attempt_runtime, attempt_clock,
                                                 generation, transport_options);
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
    bool newly_requested = false;
    {
      std::lock_guard lock{mutex_};
      if (stopped_) {
        return;
      }
      newly_requested = !shutdown_requested_;
      shutdown_requested_ = true;
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
    if (newly_requested && lifecycle_shutdown_established_) {
      lifecycle_shutdown_established_();
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

    // The coordinator owns every attempt it creates, but this post-join take is
    // the lifecycle safety net for an attempt published after the initial
    // snapshot. No future attempt can be published after the join.
    std::shared_ptr<detail::RecoveryAttempt> final_attempt;
    {
      std::lock_guard lock{attempt_mutex_};
      final_attempt = std::move(active_attempt_);
    }
    if (final_attempt) {
      final_attempt->stop();
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
      if (observation_.state != RecoveryState::Live || stopped_ ||
          shutdown_requested_) {
        return false;
      }
    }
    return runtime_.submit_transport_failure() == g3::AdmissionResult::Accepted;
  }

  [[nodiscard]] std::optional<QuiescentAcceptanceCut> quiesce_for_acceptance() {
    std::shared_ptr<detail::RecoveryAttempt> retained_attempt;
    {
      std::lock_guard lock{mutex_};
      if (!started_ || stopped_ || observation_.terminal ||
          observation_.state != RecoveryState::Live || shutdown_requested_) {
        return std::nullopt;
      }
      shutdown_requested_ = true;
      observation_.state = RecoveryState::Stopping;
      observation_.in_backoff = false;
      // Retain the Live source before the stop request can wake the
      // coordinator and let its concurrent quiescence clear active_attempt_.
      std::lock_guard attempt_lock{attempt_mutex_};
      retained_attempt = active_attempt_;
    }
    condition_.notify_all();
    coordinator_.request_stop();
    backoff_condition_.notify_all();

    if (lifecycle_shutdown_established_) {
      lifecycle_shutdown_established_();
    }
    if (retained_attempt) {
      retained_attempt->stop();
    }
    if (coordinator_.joinable()) {
      try {
        coordinator_.join();
      } catch (...) {
        std::terminate();
      }
    }

    // The coordinator is joined, so this protected take is the final source
    // ownership cut. A different attempt means the retained generation cannot
    // be accepted, even though that unexpected source is still stopped here.
    std::shared_ptr<detail::RecoveryAttempt> final_attempt;
    {
      std::lock_guard lock{attempt_mutex_};
      final_attempt = std::move(active_attempt_);
    }
    if (final_attempt && final_attempt != retained_attempt) {
      final_attempt->stop();
      {
        std::lock_guard lock{mutex_};
        observation_.active_transport_count = 0U;
      }
      condition_.notify_all();
      return std::nullopt;
    }
    if (!retained_attempt) {
      return std::nullopt;
    }

    // The owning reference survives active-attempt clearing, so the final
    // stopped transport observation is retained for the acceptance proof.
    const auto transport = retained_attempt->observe();
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
        if (stop_token.stop_requested() || shutdown_requested()) {
          break;
        }
        auto attempt = create_attempt(generation);
        if (!attempt) {
          set_terminal(RecoveryCause::InternalFailure,
                       internal_error("attempt factory returned null"), false);
          return;
        }
        if (stop_token.stop_requested() || shutdown_requested()) {
          static_cast<void>(quiesce_attempt(attempt));
          close_source_generation(SourceGenerationCloseOutcome::GlobalShutdown);
          break;
        }

        std::optional<std::uint64_t> generation_started;
        std::optional<std::uint64_t> rotation_deadline;
        if (planned_rotation_.has_value()) {
          try {
            generation_started = clock_().monotonic_ns;
            rotation_deadline = planned_rotation_deadline(*generation_started);
          } catch (...) {
            static_cast<void>(quiesce_attempt(attempt));
            throw;
          }
        }
        {
          std::lock_guard lock{mutex_};
          observation_.generation_started_monotonic_ns = generation_started;
        }
        condition_.notify_all();

        const auto start_result = attempt->start();
        if (stop_token.stop_requested() || shutdown_requested()) {
          static_cast<void>(quiesce_attempt(attempt));
          close_source_generation(SourceGenerationCloseOutcome::GlobalShutdown);
          break;
        }

        bool planned_rotation_due = false;
        std::optional<g3::RuntimeObservation> runtime_observation;
        if (start_result == g4::TransportStartResult::Started) {
          open_source_generation(attempt->observe(), generation);
          runtime_observation =
              runtime_.wait_until_live_or_recovery_required(stop_token);
          if (!runtime_observation.has_value()) {
            static_cast<void>(quiesce_attempt(attempt));
            close_source_generation(
                SourceGenerationCloseOutcome::GlobalShutdown);
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
              if (rotation_deadline.has_value()) {
                g3::TimedRecoveryWaitResult wait_result;
                try {
                  wait_result = wait_for_rotation_or_recovery(
                      *generation_started, *rotation_deadline, stop_token);
                } catch (...) {
                  static_cast<void>(quiesce_attempt(attempt));
                  close_source_generation(
                      SourceGenerationCloseOutcome::PermanentFailure);
                  throw;
                }
                if (wait_result == g3::TimedRecoveryWaitResult::Stopped) {
                  static_cast<void>(quiesce_attempt(attempt));
                  close_source_generation(
                      SourceGenerationCloseOutcome::GlobalShutdown);
                  break;
                }
                planned_rotation_due =
                    wait_result == g3::TimedRecoveryWaitResult::DeadlineReached;
                if (planned_rotation_due) {
                  bool shutdown_won = false;
                  {
                    std::lock_guard lock{mutex_};
                    shutdown_won =
                        shutdown_requested_ || stop_token.stop_requested();
                    if (!shutdown_won) {
                      observation_.state = RecoveryState::Rotating;
                    }
                  }
                  condition_.notify_all();
                  if (shutdown_won) {
                    static_cast<void>(quiesce_attempt(attempt));
                    close_source_generation(
                        SourceGenerationCloseOutcome::GlobalShutdown);
                    break;
                  }
                }
              } else {
                runtime_observation =
                    runtime_.wait_until_recovery_required(stop_token);
                if (!runtime_observation.has_value()) {
                  static_cast<void>(quiesce_attempt(attempt));
                  close_source_generation(
                      SourceGenerationCloseOutcome::GlobalShutdown);
                  break;
                }
              }
            }
          }
        }

        const auto transport_observation = quiesce_attempt(attempt);
        if (stop_token.stop_requested() || shutdown_requested()) {
          close_source_generation(SourceGenerationCloseOutcome::GlobalShutdown);
          break;
        }
        // This owner barrier is deliberately after the transport network thread
        // has joined. No old-generation producer can submit beyond this cut.
        auto final_runtime = runtime_.observe();
        // SpotTransport normally admits its matching runtime fault before its
        // terminal error becomes observable. Preserve a genuine terminal-only
        // race at the planned cut by reflecting that real source failure
        // through the ordinary runtime fault boundary before classification.
        if (transport_observation.terminal_error.has_value() &&
            final_runtime.state == g3::RuntimeState::Live &&
            !final_runtime.fault_reason.has_value()) {
          if (runtime_.submit_transport_failure() !=
              g3::AdmissionResult::Accepted) {
            close_source_generation(
                SourceGenerationCloseOutcome::PermanentFailure);
            set_terminal(
                RecoveryCause::InternalFailure,
                internal_error("terminal transport fault admission failed"),
                false);
            return;
          }
          final_runtime = runtime_.observe();
        }
        if (planned_rotation_due &&
            is_clean_planned_cut(final_runtime, transport_observation)) {
          if (before_planned_reset_) {
            before_planned_reset_();
          }
          g3::PlannedRebootstrapResetResult reset_result;
          {
            // The lifecycle gate makes shutdown and the healthy owner reset a
            // single ordering decision after source quiescence.
            std::lock_guard lock{mutex_};
            if (shutdown_requested_ || stop_token.stop_requested()) {
              close_source_generation(
                  SourceGenerationCloseOutcome::GlobalShutdown);
              break;
            }
            close_source_generation(SourceGenerationCloseOutcome::Replacement);
            reset_result = runtime_.reset_live_for_planned_rebootstrap();
            if (reset_result == g3::PlannedRebootstrapResetResult::Reset) {
              observation_.last_rotation_generation = generation;
              observation_.last_planned_rotation_cut = PlannedRotationCut{
                  generation, transport_observation, final_runtime};
              ++observation_.planned_rotation_count;
              ++generation;
              observation_.state = RecoveryState::Starting;
            }
          }
          condition_.notify_all();
          if (reset_result != g3::PlannedRebootstrapResetResult::Reset) {
            set_terminal(
                RecoveryCause::InternalFailure,
                internal_error("owner-domain planned rebootstrap reset failed"),
                false);
            return;
          }
          continue;
        }
        const auto decision =
            classify_failure(final_runtime, transport_observation);
        if (!decision.recoverable) {
          close_source_generation(
              SourceGenerationCloseOutcome::PermanentFailure);
          set_terminal(decision.cause,
                       transport_observation.terminal_error.value_or(
                           internal_error("nonrecoverable G5 failure")),
                       false);
          return;
        }
        if (before_backoff_commit_) {
          before_backoff_commit_();
        }

        std::size_t recovery_attempt = 0U;
        {
          std::lock_guard lock{mutex_};
          if (shutdown_requested_ || stop_token.stop_requested()) {
            close_source_generation(
                SourceGenerationCloseOutcome::GlobalShutdown);
            break;
          }
          if (observation_.consecutive_recovery_attempts >=
              detail::kMaximumRecoveryAttempts) {
            close_source_generation(
                SourceGenerationCloseOutcome::PermanentFailure);
            observation_.last_recovery_cause = decision.cause;
            observation_.state = RecoveryState::Exhausted;
            observation_.terminal = true;
            observation_.exhausted = true;
            observation_.in_backoff = false;
            observation_.terminal_error = transport_observation.terminal_error;
            condition_.notify_all();
            return;
          }
          close_source_generation(SourceGenerationCloseOutcome::Replacement);
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
          if (shutdown_requested_ || stop_token.stop_requested()) {
            break;
          }
          observation_.state = RecoveryState::Recovering;
          observation_.in_backoff = false;
        }
        condition_.notify_all();

        g3::RebootstrapResetResult reset_result;
        {
          // Holding the lifecycle gate through reset makes reset-start and
          // shutdown entry a single ordering decision.
          std::lock_guard lock{mutex_};
          if (shutdown_requested_ || stop_token.stop_requested()) {
            break;
          }
          reset_result = runtime_.reset_for_rebootstrap();
          if (reset_result == g3::RebootstrapResetResult::Reset) {
            ++generation;
            ++observation_.total_recovery_count;
          }
        }
        if (reset_result != g3::RebootstrapResetResult::Reset) {
          set_terminal(RecoveryCause::InternalFailure,
                       internal_error("owner-domain rebootstrap reset failed"),
                       false);
          return;
        }
      }
    } catch (const std::exception &error) {
      close_source_after_exception();
      set_terminal(RecoveryCause::InternalFailure, internal_error(error.what()),
                   false);
    } catch (...) {
      close_source_after_exception();
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

  [[nodiscard]] std::optional<std::uint64_t>
  planned_rotation_deadline(std::uint64_t generation_started) const {
    if (!planned_rotation_.has_value()) {
      return std::nullopt;
    }
    const auto age = planned_rotation_->age.count();
    const auto unsigned_age = static_cast<std::uint64_t>(age);
    if (generation_started >
        std::numeric_limits<std::uint64_t>::max() - unsigned_age) {
      throw std::overflow_error{"G6 planned rotation deadline overflow"};
    }
    return generation_started + unsigned_age;
  }

  [[nodiscard]] g3::TimedRecoveryWaitResult
  wait_for_rotation_or_recovery(std::uint64_t generation_started,
                                std::uint64_t deadline,
                                std::stop_token stop_token) {
    const auto now = clock_().monotonic_ns;
    if (now < generation_started) {
      throw std::runtime_error{"G6 monotonic clock moved backwards"};
    }
    if (now >= deadline) {
      return g3::TimedRecoveryWaitResult::DeadlineReached;
    }
    const auto remaining = deadline - now;
    const auto duration = std::chrono::nanoseconds{
        static_cast<std::chrono::nanoseconds::rep>(remaining)};
    if (rotation_waiter_) {
      return rotation_waiter_(runtime_, duration, stop_token);
    }
    return runtime_.wait_until_recovery_required_for(stop_token, duration);
  }

  [[nodiscard]] static bool
  is_clean_planned_cut(const g3::RuntimeObservation &runtime,
                       const g4::TransportObservation &transport) noexcept {
    return transport.stopped && !transport.running &&
           !transport.terminal_error.has_value() &&
           runtime.state == g3::RuntimeState::Live &&
           runtime.projection_status == core::ProjectionStatus::Synchronized &&
           !runtime.fault_reason.has_value();
  }

  [[nodiscard]] g4::TransportObservation
  quiesce_attempt(const std::shared_ptr<detail::RecoveryAttempt> &attempt) {
    attempt->stop();
    const auto transport = attempt->observe();
    if (source_generation_open_.has_value() &&
        *source_generation_open_ == transport.connection_generation &&
        !source_generation_quiesced_) {
      if (!source_generation_lifecycle_.quiesce(
              transport.connection_generation)) {
        throw std::runtime_error{
            "G9 source generation quiesce invariant failed"};
      }
      source_generation_quiesced_ = true;
    }
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

  void open_source_generation(const g4::TransportObservation &transport,
                              std::uint64_t expected_generation) {
    if (!source_generation_lifecycle_.open) {
      return;
    }
    if (source_generation_open_.has_value() || source_generation_quiesced_ ||
        !transport.started || !transport.running || transport.stopped ||
        !transport.websocket_handshake ||
        transport.connection_generation != expected_generation ||
        !source_generation_lifecycle_.open(expected_generation)) {
      throw std::runtime_error{"G9 source generation open invariant failed"};
    }
    source_generation_open_ = expected_generation;
  }

  void close_source_generation(SourceGenerationCloseOutcome outcome) {
    if (!source_generation_lifecycle_.close ||
        !source_generation_open_.has_value()) {
      return;
    }
    if (!source_generation_quiesced_ ||
        !source_generation_lifecycle_.close(*source_generation_open_,
                                            outcome)) {
      throw std::runtime_error{"G9 source generation close invariant failed"};
    }
    source_generation_open_.reset();
    source_generation_quiesced_ = false;
  }

  void close_source_after_exception() noexcept {
    if (!source_generation_open_.has_value()) {
      return;
    }
    if (!source_generation_quiesced_) {
      std::shared_ptr<detail::RecoveryAttempt> attempt;
      {
        std::lock_guard lock{attempt_mutex_};
        attempt = active_attempt_;
      }
      if (attempt != nullptr) {
        try {
          static_cast<void>(quiesce_attempt(attempt));
        } catch (...) {
          return;
        }
      }
    }
    try {
      close_source_generation(SourceGenerationCloseOutcome::PermanentFailure);
    } catch (...) {
    }
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
    if (stopped_ || shutdown_requested_ || observation_.terminal ||
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
    if (shutdown_requested_) {
      return;
    }
    observation_.last_recovery_cause = cause;
    observation_.state = RecoveryState::Exhausted;
    observation_.terminal = true;
    observation_.exhausted = exhausted;
    observation_.in_backoff = false;
    observation_.terminal_error = std::move(error);
    condition_.notify_all();
  }

  [[nodiscard]] bool shutdown_requested() const {
    std::lock_guard lock{mutex_};
    return shutdown_requested_;
  }

  g3::MarketRuntime &runtime_;
  g3::RuntimeClock clock_;
  std::optional<PlannedRotationPolicy> planned_rotation_;
  g4::SpotTransportOptions transport_options_;
  SourceGenerationLifecycle source_generation_lifecycle_;
  std::optional<std::uint64_t> source_generation_open_;
  bool source_generation_quiesced_{false};
  detail::AttemptFactory attempt_factory_;
  detail::BackoffWaiter backoff_waiter_;
  detail::RotationWaiter rotation_waiter_;
  std::function<void()> lifecycle_shutdown_established_;
  std::function<void()> before_backoff_commit_;
  std::function<void()> before_planned_reset_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  RecoveryObservation observation_;
  std::jthread coordinator_;
  bool started_{false};
  bool stopped_{false};
  bool shutdown_requested_{false};

  std::mutex attempt_mutex_;
  std::shared_ptr<detail::RecoveryAttempt> active_attempt_;
  std::mutex backoff_mutex_;
  std::condition_variable_any backoff_condition_;
};

SpotRecovery::SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                           detail::RecoveryTestOptions test_options)
    : SpotRecovery(runtime, std::move(clock), RecoveryOptions{},
                   std::move(test_options)) {}

SpotRecovery::SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                           RecoveryOptions options,
                           detail::RecoveryTestOptions test_options)
    : impl_{std::make_unique<Impl>(runtime, std::move(clock), std::nullopt,
                                   std::move(options),
                                   std::move(test_options))} {}

SpotRecovery::SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                           PlannedRotationPolicy planned_rotation,
                           detail::RecoveryTestOptions test_options)
    : SpotRecovery(runtime, std::move(clock), planned_rotation,
                   RecoveryOptions{}, std::move(test_options)) {}

SpotRecovery::SpotRecovery(g3::MarketRuntime &runtime, g3::RuntimeClock clock,
                           PlannedRotationPolicy planned_rotation,
                           RecoveryOptions options,
                           detail::RecoveryTestOptions test_options)
    : impl_{std::make_unique<Impl>(runtime, std::move(clock), planned_rotation,
                                   std::move(options),
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
