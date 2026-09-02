#include "market_runtime.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace binance_market_data::gateway::g3 {

namespace {

namespace common_wire = ::binance_market_data::common::v1;

[[nodiscard]] adapter::ExpectedIdentity make_expected_identity() {
  return {"BTCUSDT", core::SequencePolicyKind::Spot};
}

enum class InjectedFailure : std::uint8_t {
  Transport,
  Snapshot,
};

struct DepthUpdateInput final {
  market::DepthUpdate update;
  SourceProvenance provenance;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  performance::TraceToken trace;
#endif
};

struct SnapshotInput final {
  market::ExchangeDepthSnapshot snapshot;
  SourceProvenance provenance;
};

using RuntimeInput =
    std::variant<DepthUpdateInput, SnapshotInput, InjectedFailure>;

struct IngressItem final {
  std::uint64_t ticket;
  RuntimeInput input;
};

[[nodiscard]] RuntimeState
state_for_projection(core::ProjectionStatus status) noexcept {
  switch (status) {
  case core::ProjectionStatus::AwaitingBaseline:
    return RuntimeState::Buffering;
  case core::ProjectionStatus::AwaitingBridge:
    return RuntimeState::AwaitingBridge;
  case core::ProjectionStatus::Synchronized:
    return RuntimeState::Live;
  case core::ProjectionStatus::NeedsResync:
    return RuntimeState::NeedsResync;
  }
  return RuntimeState::Faulted;
}

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
[[nodiscard]] performance::T3Disposition
measurement_disposition(core::ApplyDisposition disposition) noexcept {
  switch (disposition) {
  case core::ApplyDisposition::Applied:
    return performance::T3Disposition::Applied;
  case core::ApplyDisposition::IgnoredStale:
    return performance::T3Disposition::IgnoredStale;
  case core::ApplyDisposition::IgnoredDuplicate:
    return performance::T3Disposition::IgnoredDuplicate;
  case core::ApplyDisposition::GapDetected:
    return performance::T3Disposition::GapDetected;
  case core::ApplyDisposition::RejectedWrongState:
    return performance::T3Disposition::RejectedWrongState;
  }
  return performance::T3Disposition::InternalFailure;
}
#endif

} // namespace

class MarketRuntime::Impl final {
public:
  Impl(RuntimeLimits limits, RuntimeClock clock, core::NumericSpec numeric_spec,
       adapter::ExpectedIdentity expected_identity,
       RuntimeTestOptions test_options)
      : limits_{limits}, clock_{std::move(clock)},
        expected_identity_{std::move(expected_identity)},
        projection_{numeric_spec, expected_identity_.policy},
        admission_enqueued_{std::move(test_options.admission_enqueued)},
        before_admission_processing_{
            std::move(test_options.before_admission_processing)},
        owner_paused_{test_options.owner_starts_paused} {
    if (limits_.ingress_capacity == 0U || limits_.bootstrap_capacity == 0U) {
      throw std::invalid_argument{"G3 runtime capacities must be nonzero"};
    }
    if (!clock_) {
      throw std::invalid_argument{"G3 runtime clock must be injected"};
    }
    if (expected_identity_.symbol.empty()) {
      throw std::invalid_argument{"MarketRuntime identity must have a symbol"};
    }
    if (limits_.publication.maximum_active_subscriptions == 0U ||
        limits_.publication.ordinary_queue_capacity == 0U ||
        limits_.publication.pending_admission_capacity == 0U) {
      throw std::invalid_argument{"G7 publication capacities must be nonzero"};
    }
    subscribers_.reserve(limits_.publication.maximum_active_subscriptions);
    observation_.ingress_capacity = limits_.ingress_capacity;
    observation_.bootstrap_capacity = limits_.bootstrap_capacity;
  }

  ~Impl() { stop(); }

  [[nodiscard]] StartResult start() {
    std::unique_lock lock{mutex_};
    if (stopped_) {
      return StartResult::Stopped;
    }
    if (started_) {
      return StartResult::AlreadyStarted;
    }

    started_ = true;
    accepting_ = true;
    publication_admission_open_ = !publication_shutdown_completed_;
    observation_.publication_admission_open = publication_admission_open_;
    owner_ = std::thread{[this] { owner_loop(); }};
    condition_.wait(lock, [this] { return owner_ready_; });
    return StartResult::Started;
  }

  void stop() noexcept {
    {
      std::lock_guard lock{mutex_};
      if (stopped_) {
        return;
      }
      if (!started_) {
        accepting_ = false;
        stopped_ = true;
        observation_.state = RuntimeState::Stopped;
        observation_.owner_joined = true;
        return;
      }
      accepting_ = false;
      publication_admission_open_ = false;
      observation_.publication_admission_open = false;
      publication_shutdown_requested_ = true;
      stop_requested_ = true;
      owner_paused_ = false;
      if (observation_.state != RuntimeState::Faulted &&
          observation_.state != RuntimeState::NeedsResync) {
        observation_.state = RuntimeState::Stopping;
      }
    }
    condition_.notify_all();

    if (owner_.joinable()) {
      try {
        owner_.join();
      } catch (...) {
        std::terminate();
      }
    }

    {
      std::lock_guard lock{mutex_};
      stopped_ = true;
      observation_.state = RuntimeState::Stopped;
      observation_.owner_joined = true;
      if (snapshot_request_.has_value() &&
          !snapshot_request_->result.has_value()) {
        snapshot_request_->result = SnapshotRequestError::Stopped;
      }
      complete_all_admissions_locked(SubscriptionAdmissionError::Stopped);
    }
    condition_.notify_all();
  }

  [[nodiscard]] AdmissionResult submit(RuntimeInput input) {
    std::lock_guard lock{mutex_};
    if (stopped_) {
      return AdmissionResult::Stopped;
    }
    if (!started_) {
      return AdmissionResult::NotStarted;
    }
    if (stop_requested_) {
      return AdmissionResult::Stopping;
    }
    if (!accepting_) {
      return AdmissionResult::Faulted;
    }
    if (ingress_.size() == limits_.ingress_capacity) {
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      if (limits_.performance_baseline != nullptr) {
        limits_.performance_baseline->record_ingress_full();
      }
#endif
      accepting_ = false;
      pending_fault_ = FaultReason::IngressOverflow;
      condition_.notify_all();
      return AdmissionResult::Full;
    }

    ++last_admitted_ticket_;
    ingress_.push_back(IngressItem{last_admitted_ticket_, std::move(input)});
    observation_.ingress_occupancy = ingress_.size();
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    if (limits_.performance_baseline != nullptr) {
      if (auto *depth = std::get_if<DepthUpdateInput>(&ingress_.back().input)) {
        limits_.performance_baseline->record_q(depth->trace, ingress_.size());
      }
    }
#endif
    condition_.notify_all();
    return AdmissionResult::Accepted;
  }

  [[nodiscard]] RuntimeObservation observe() {
    std::unique_lock lock{mutex_};
    if (started_ && !stopped_) {
      const auto target = last_admitted_ticket_;
      condition_.wait(lock, [this, target] {
        return processed_ticket_ >= target || stopped_;
      });
    }
    observation_.ingress_occupancy = ingress_.size();
    publish_tickets_locked();
    return observation_;
  }

  [[nodiscard]] RebootstrapResetResult reset_for_rebootstrap() {
    std::unique_lock lock{mutex_};
    if (stopped_) {
      return RebootstrapResetResult::Stopped;
    }
    if (!started_) {
      return RebootstrapResetResult::NotStarted;
    }
    if (stop_requested_) {
      return RebootstrapResetResult::Stopping;
    }
    if (observation_.state != RuntimeState::Faulted &&
        observation_.state != RuntimeState::NeedsResync) {
      return RebootstrapResetResult::InvalidState;
    }
    if (reset_request_.has_value()) {
      return RebootstrapResetResult::Busy;
    }

    reset_request_.emplace(last_admitted_ticket_, ResetKind::Recovery);
    condition_.notify_all();
    condition_.wait(lock, [this] {
      return reset_request_->result.has_value() || stopped_;
    });
    if (!reset_request_->result.has_value()) {
      reset_request_.reset();
      return RebootstrapResetResult::Stopped;
    }
    const auto result = *reset_request_->result;
    reset_request_.reset();
    condition_.notify_all();
    return result;
  }

  [[nodiscard]] PlannedRebootstrapResetResult
  reset_live_for_planned_rebootstrap() {
    std::unique_lock lock{mutex_};
    if (stopped_) {
      return PlannedRebootstrapResetResult::Stopped;
    }
    if (!started_) {
      return PlannedRebootstrapResetResult::NotStarted;
    }
    if (stop_requested_) {
      return PlannedRebootstrapResetResult::Stopping;
    }
    if (observation_.state != RuntimeState::Live ||
        observation_.projection_status !=
            core::ProjectionStatus::Synchronized ||
        observation_.fault_reason.has_value()) {
      return PlannedRebootstrapResetResult::InvalidState;
    }
    if (reset_request_.has_value()) {
      return PlannedRebootstrapResetResult::Busy;
    }

    reset_request_.emplace(last_admitted_ticket_, ResetKind::Planned);
    condition_.notify_all();
    condition_.wait(lock, [this] {
      return reset_request_->result.has_value() || stopped_;
    });
    if (!reset_request_->result.has_value()) {
      reset_request_.reset();
      return PlannedRebootstrapResetResult::Stopped;
    }
    const auto result = to_planned_reset_result(*reset_request_->result);
    reset_request_.reset();
    condition_.notify_all();
    return result;
  }

  [[nodiscard]] std::optional<RuntimeObservation>
  wait_until_live_or_recovery_required(std::stop_token stop_token) {
    std::unique_lock lock{mutex_};
    const auto ready = condition_.wait(lock, stop_token, [this] {
      return observation_.state == RuntimeState::Live ||
             recovery_required_locked() || stop_requested_ || stopped_;
    });
    if (!ready || stop_token.stop_requested()) {
      return std::nullopt;
    }
    publish_tickets_locked();
    return observation_;
  }

  [[nodiscard]] std::optional<RuntimeObservation>
  wait_until_recovery_required(std::stop_token stop_token) {
    std::unique_lock lock{mutex_};
    const auto ready = condition_.wait(lock, stop_token, [this] {
      return recovery_required_locked() || stop_requested_ || stopped_;
    });
    if (!ready || stop_token.stop_requested()) {
      return std::nullopt;
    }
    publish_tickets_locked();
    return observation_;
  }

  [[nodiscard]] TimedRecoveryWaitResult
  wait_until_recovery_required_for(std::stop_token stop_token,
                                   std::chrono::nanoseconds duration) {
    std::unique_lock lock{mutex_};
    const auto ready = condition_.wait_for(lock, stop_token, duration, [this] {
      return recovery_required_locked() || stop_requested_ || stopped_;
    });
    if (stop_token.stop_requested() || stop_requested_ || stopped_) {
      return TimedRecoveryWaitResult::Stopped;
    }
    return ready ? TimedRecoveryWaitResult::RecoveryRequired
                 : TimedRecoveryWaitResult::DeadlineReached;
  }

  [[nodiscard]] SnapshotResult capture_snapshot() {
    std::unique_lock lock{mutex_};
    if (stopped_) {
      return SnapshotRequestError::Stopped;
    }
    if (!started_) {
      return SnapshotRequestError::NotStarted;
    }
    if (stop_requested_) {
      return SnapshotRequestError::Stopping;
    }
    if (observation_.state == RuntimeState::Faulted ||
        observation_.state == RuntimeState::NeedsResync) {
      return SnapshotRequestError::Faulted;
    }
    if (snapshot_request_.has_value()) {
      return SnapshotRequestError::Busy;
    }

    snapshot_request_.emplace(last_admitted_ticket_);
    condition_.notify_all();
    condition_.wait(lock, [this] {
      return snapshot_request_->result.has_value() || stopped_;
    });
    if (!snapshot_request_->result.has_value()) {
      snapshot_request_.reset();
      return SnapshotRequestError::Stopped;
    }
    auto result = std::move(*snapshot_request_->result);
    snapshot_request_.reset();
    condition_.notify_all();
    return result;
  }

  [[nodiscard]] SubscriptionAdmissionResult admit_order_book_subscription(
      g7::ValidatedOrderBookSubscription subscription) {
    std::unique_lock lock{mutex_};
    if (stopped_) {
      return SubscriptionAdmissionError::Stopped;
    }
    if (!started_) {
      return SubscriptionAdmissionError::NotStarted;
    }
    if (!publication_admission_open_ || publication_shutdown_requested_ ||
        publication_shutdown_completed_ || stop_requested_) {
      return SubscriptionAdmissionError::ShuttingDown;
    }
    if (observation_.state != RuntimeState::Live ||
        observation_.projection_status !=
            core::ProjectionStatus::Synchronized ||
        observation_.fault_reason.has_value()) {
      return SubscriptionAdmissionError::NotLive;
    }
    if (pending_admission_count_ ==
        limits_.publication.pending_admission_capacity) {
      return SubscriptionAdmissionError::PendingLimit;
    }

    std::shared_ptr<AdmissionRequest> request;
    try {
      request = std::make_shared<AdmissionRequest>(last_admitted_ticket_,
                                                   std::move(subscription));
      admission_requests_.push_back(request);
    } catch (...) {
      return SubscriptionAdmissionError::InternalError;
    }
    ++pending_admission_count_;
    observation_.pending_admission_count = pending_admission_count_;
    if (admission_enqueued_) {
      try {
        admission_enqueued_();
      } catch (...) {
        // Test instrumentation must not affect owner-mailbox correctness.
      }
    }
    condition_.notify_all();
    condition_.wait(lock, [&request, this] {
      return request->result.has_value() || stopped_;
    });
    if (!request->result.has_value()) {
      return SubscriptionAdmissionError::Stopped;
    }
    return std::move(*request->result);
  }

  void notify_subscriber_closed() noexcept {
    {
      std::lock_guard lock{mutex_};
      publication_removal_pending_ = true;
    }
    condition_.notify_all();
  }

  void close_publication_admission() noexcept {
    {
      std::lock_guard lock{mutex_};
      publication_admission_open_ = false;
      observation_.publication_admission_open = false;
    }
    condition_.notify_all();
  }

  [[nodiscard]] PublicationShutdownResult shutdown_publication() noexcept {
    std::unique_lock lock{mutex_};
    publication_admission_open_ = false;
    observation_.publication_admission_open = false;
    if (publication_shutdown_completed_) {
      return PublicationShutdownResult::AlreadyShutDown;
    }
    if (stopped_) {
      return PublicationShutdownResult::Stopped;
    }
    if (!started_) {
      publication_shutdown_requested_ = true;
      publication_shutdown_completed_ = true;
      observation_.publication_shutdown = true;
      return PublicationShutdownResult::NotStarted;
    }
    publication_shutdown_requested_ = true;
    condition_.notify_all();
    condition_.wait(
        lock, [this] { return publication_shutdown_completed_ || stopped_; });
    return publication_shutdown_completed_ ? PublicationShutdownResult::ShutDown
                                           : PublicationShutdownResult::Stopped;
  }

  [[nodiscard]] IngressObservation ingress_observation() const noexcept {
    std::lock_guard lock{mutex_};
    return {ingress_.size(), limits_.ingress_capacity};
  }

  void release_owner_for_testing() noexcept {
    {
      std::lock_guard lock{mutex_};
      owner_paused_ = false;
    }
    condition_.notify_all();
  }

  void pause_owner_for_testing() noexcept {
    std::lock_guard lock{mutex_};
    if (started_ && !stop_requested_ && !stopped_) {
      owner_paused_ = true;
    }
  }

private:
  enum class ResetKind : std::uint8_t {
    Recovery,
    Planned,
  };

  struct AdmissionRequest final {
    AdmissionRequest(std::uint64_t target_value,
                     g7::ValidatedOrderBookSubscription subscription_value)
        : target{target_value}, subscription{std::move(subscription_value)} {}

    std::uint64_t target;
    g7::ValidatedOrderBookSubscription subscription;
    std::optional<SubscriptionAdmissionResult> result;
  };

  struct SnapshotRequest final {
    explicit SnapshotRequest(std::uint64_t target_value)
        : target{target_value} {}

    std::uint64_t target;
    std::optional<SnapshotResult> result;
  };

  struct ResetRequest final {
    ResetRequest(std::uint64_t target_value, ResetKind reset_kind)
        : target{target_value}, kind{reset_kind} {}

    std::uint64_t target;
    ResetKind kind;
    std::optional<RebootstrapResetResult> result;
  };

  void complete_all_admissions_locked(SubscriptionAdmissionError error) {
    for (const auto &request : admission_requests_) {
      if (!request->result.has_value()) {
        request->result = error;
      }
    }
    admission_requests_.clear();
    pending_admission_count_ = 0U;
    observation_.pending_admission_count = 0U;
  }

  [[nodiscard]] static PlannedRebootstrapResetResult
  to_planned_reset_result(RebootstrapResetResult result) noexcept {
    switch (result) {
    case RebootstrapResetResult::Reset:
      return PlannedRebootstrapResetResult::Reset;
    case RebootstrapResetResult::NotStarted:
      return PlannedRebootstrapResetResult::NotStarted;
    case RebootstrapResetResult::InvalidState:
      return PlannedRebootstrapResetResult::InvalidState;
    case RebootstrapResetResult::Busy:
      return PlannedRebootstrapResetResult::Busy;
    case RebootstrapResetResult::Stopping:
      return PlannedRebootstrapResetResult::Stopping;
    case RebootstrapResetResult::Stopped:
      return PlannedRebootstrapResetResult::Stopped;
    case RebootstrapResetResult::InternalError:
      return PlannedRebootstrapResetResult::InternalError;
    }
    return PlannedRebootstrapResetResult::InternalError;
  }

  void owner_loop() noexcept {
    {
      std::lock_guard lock{mutex_};
      observation_.owner_thread_id = std::this_thread::get_id();
      observation_.state = RuntimeState::Buffering;
      refresh_projection_observation_locked();
      owner_ready_ = true;
    }
    condition_.notify_all();

    for (;;) {
      std::optional<IngressItem> item;
      std::shared_ptr<AdmissionRequest> admission;
      bool perform_snapshot = false;
      bool perform_reset = false;
      bool perform_publication_shutdown = false;
      bool perform_removal_sweep = false;
      std::optional<FaultReason> pending_fault;
      {
        std::unique_lock lock{mutex_};
        condition_.wait(lock, [this] {
          if (stop_requested_) {
            return true;
          }
          if (publication_shutdown_requested_ &&
              !publication_shutdown_completed_) {
            return true;
          }
          if (owner_paused_) {
            return false;
          }
          return pending_fault_.has_value() || reset_ready_locked() ||
                 admission_ready_locked() || snapshot_ready_locked() ||
                 publication_removal_pending_ || !ingress_.empty();
        });

        if (!stop_requested_ &&
            !(publication_shutdown_requested_ &&
              !publication_shutdown_completed_) &&
            owner_paused_) {
          continue;
        }
        if (publication_shutdown_requested_ &&
            !publication_shutdown_completed_) {
          perform_publication_shutdown = true;
        } else if (pending_fault_.has_value()) {
          pending_fault = pending_fault_;
          pending_fault_.reset();
        } else if (reset_ready_locked()) {
          perform_reset = true;
        } else if (admission_ready_locked()) {
          admission = admission_requests_.front();
          admission_requests_.pop_front();
        } else if (snapshot_ready_locked()) {
          perform_snapshot = true;
        } else if (publication_removal_pending_) {
          publication_removal_pending_ = false;
          perform_removal_sweep = true;
        } else if (!ingress_.empty()) {
          item.emplace(std::move(ingress_.front()));
          ingress_.pop_front();
          observation_.ingress_occupancy = ingress_.size();
        } else if (stop_requested_) {
          break;
        }
      }

      if (perform_publication_shutdown) {
        perform_publication_shutdown_control();
        continue;
      }
      if (pending_fault.has_value()) {
        transition_to_fault(*pending_fault, std::nullopt);
        continue;
      }
      if (perform_reset) {
        perform_reset_request();
        continue;
      }
      if (admission != nullptr) {
        perform_admission_request(admission);
        continue;
      }
      if (perform_snapshot) {
        perform_snapshot_request();
        continue;
      }
      if (perform_removal_sweep) {
        sweep_closed_subscribers();
        continue;
      }
      if (!item.has_value()) {
        continue;
      }

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      if (limits_.performance_baseline != nullptr) {
        if (auto *depth = std::get_if<DepthUpdateInput>(&item->input)) {
          limits_.performance_baseline->record_t2(depth->trace);
        }
      }
#endif
      try {
        process_input(item->input);
      } catch (...) {
        transition_to_fault(FaultReason::InternalError, std::nullopt);
      }
      finish_ticket(item->ticket);
    }

    {
      std::lock_guard lock{mutex_};
      refresh_projection_observation_locked();
      observation_.state = RuntimeState::Stopped;
      if (snapshot_request_.has_value() &&
          !snapshot_request_->result.has_value()) {
        snapshot_request_->result = SnapshotRequestError::Stopped;
      }
      if (reset_request_.has_value() && !reset_request_->result.has_value()) {
        reset_request_->result = RebootstrapResetResult::Stopped;
      }
      complete_all_admissions_locked(SubscriptionAdmissionError::Stopped);
    }
    close_all_subscribers_for_shutdown();
    condition_.notify_all();
  }

  [[nodiscard]] bool snapshot_ready_locked() const noexcept {
    return snapshot_request_.has_value() &&
           !snapshot_request_->result.has_value() &&
           processed_ticket_ >= snapshot_request_->target;
  }

  [[nodiscard]] bool reset_ready_locked() const noexcept {
    return reset_request_.has_value() && !reset_request_->result.has_value() &&
           processed_ticket_ >= reset_request_->target;
  }

  [[nodiscard]] bool admission_ready_locked() const noexcept {
    return !admission_requests_.empty() &&
           processed_ticket_ >= admission_requests_.front()->target;
  }

  [[nodiscard]] bool recovery_required_locked() const noexcept {
    return observation_.state == RuntimeState::Faulted ||
           observation_.state == RuntimeState::NeedsResync ||
           observation_.state == RuntimeState::Stopping ||
           observation_.state == RuntimeState::Stopped;
  }

  void publish_tickets_locked() noexcept {
    observation_.last_admitted_ticket = last_admitted_ticket_;
    observation_.processed_ticket = processed_ticket_;
  }

  [[nodiscard]] std::optional<g7::PublicationTime>
  sample_publication_time() noexcept {
    try {
      const auto sample = clock_();
      return g7::PublicationTime{sample.utc_ns, sample.monotonic_ns};
    } catch (...) {
      return std::nullopt;
    }
  }

  void
  complete_admission_request(const std::shared_ptr<AdmissionRequest> &request,
                             SubscriptionAdmissionResult result) noexcept {
    {
      std::lock_guard lock{mutex_};
      if (!request->result.has_value()) {
        request->result = std::move(result);
        if (pending_admission_count_ != 0U) {
          --pending_admission_count_;
        }
        observation_.pending_admission_count = pending_admission_count_;
      }
    }
    condition_.notify_all();
  }

  void perform_admission_request(
      const std::shared_ptr<AdmissionRequest> &request) noexcept {
    if (before_admission_processing_) {
      try {
        before_admission_processing_();
      } catch (...) {
        // Test instrumentation must not affect owner-domain correctness.
      }
    }
    sweep_closed_subscribers();
    {
      std::lock_guard lock{mutex_};
      if (!publication_admission_open_ || publication_shutdown_requested_ ||
          stop_requested_ || stopped_) {
        complete_admission_request_unlocked(
            request, SubscriptionAdmissionError::ShuttingDown);
        condition_.notify_all();
        return;
      }
      if (observation_.state != RuntimeState::Live ||
          observation_.projection_status !=
              core::ProjectionStatus::Synchronized ||
          observation_.fault_reason.has_value()) {
        complete_admission_request_unlocked(
            request, SubscriptionAdmissionError::NotLive);
        condition_.notify_all();
        return;
      }
    }
    if (subscribers_.size() >=
        limits_.publication.maximum_active_subscriptions) {
      complete_admission_request(request,
                                 SubscriptionAdmissionError::ActiveLimit);
      return;
    }

    adapter::SnapshotOptions options;
    if (request->subscription.depth_limit.has_value()) {
      const auto depth =
          adapter::DepthLimit::create(*request->subscription.depth_limit);
      if (std::holds_alternative<adapter::AdapterError>(depth)) {
        complete_admission_request(
            request, SubscriptionAdmissionError::InvalidDepthLimit);
        return;
      }
      options.depth_limit = std::get<adapter::DepthLimit>(depth);
    }

    const auto published_at = sample_publication_time();
    if (!published_at.has_value()) {
      complete_admission_request(request,
                                 SubscriptionAdmissionError::ClockError);
      return;
    }
    if (next_subscription_id_ == std::numeric_limits<std::uint64_t>::max()) {
      complete_admission_request(request,
                                 SubscriptionAdmissionError::IdExhausted);
      return;
    }

    try {
      adapter::SnapshotContext context{expected_identity_,
                                       "gateway-g7-publication",
                                       "1.0.0",
                                       adapter::SnapshotOrigin::GatewayLive,
                                       published_at->utc_ns,
                                       published_at->monotonic_ns,
                                       std::nullopt};
      auto snapshot = adapter::make_local_order_book_snapshot(projection_,
                                                              context, options);
      if (std::holds_alternative<adapter::AdapterError>(snapshot)) {
        const auto &failure = std::get<adapter::AdapterError>(snapshot);
        complete_admission_request(
            request,
            failure.code == adapter::AdapterErrorCode::InvalidDepthLimit
                ? SubscriptionAdmissionError::InvalidDepthLimit
                : SubscriptionAdmissionError::InternalError);
        return;
      }

      const auto subscription_id =
          std::string{"ob-"} + std::to_string(next_subscription_id_);
      auto channel = std::make_shared<g7::SubscriberChannel>(
          subscription_id, request->subscription.gateway_instance_id,
          limits_.publication.ordinary_queue_capacity
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
          ,
          next_subscription_id_, limits_.performance_baseline.get()
#endif
      );
      auto accepted = g7::make_accepted_record(
          request->subscription.request_id, subscription_id,
          request->subscription.gateway_instance_id, *published_at);
      auto initial_snapshot = g7::make_snapshot_record(
          std::get<core::LocalOrderBookSnapshot>(std::move(snapshot)),
          current_projection_generation_, *published_at);
      if (!channel->stage_initial(std::move(accepted),
                                  std::move(initial_snapshot))) {
        complete_admission_request(request,
                                   SubscriptionAdmissionError::ActiveLimit);
        return;
      }

      {
        std::lock_guard lock{mutex_};
        if (!publication_admission_open_ || publication_shutdown_requested_ ||
            stop_requested_ || stopped_) {
          complete_admission_request_unlocked(
              request, SubscriptionAdmissionError::ShuttingDown);
          condition_.notify_all();
          return;
        }
        if (observation_.state != RuntimeState::Live ||
            observation_.projection_status !=
                core::ProjectionStatus::Synchronized ||
            observation_.fault_reason.has_value()) {
          complete_admission_request_unlocked(
              request, SubscriptionAdmissionError::NotLive);
          condition_.notify_all();
          return;
        }
        subscribers_.push_back(channel);
        ++next_subscription_id_;
        observation_.resident_subscription_count = subscribers_.size();
        observation_.last_publication_thread_id = std::this_thread::get_id();
        complete_admission_request_unlocked(
            request, AcceptedSubscription{channel, std::this_thread::get_id()});
      }
      condition_.notify_all();
    } catch (...) {
      complete_admission_request(request,
                                 SubscriptionAdmissionError::InternalError);
    }
  }

  void complete_admission_request_unlocked(
      const std::shared_ptr<AdmissionRequest> &request,
      SubscriptionAdmissionResult result) noexcept {
    if (request->result.has_value()) {
      return;
    }
    request->result = std::move(result);
    if (pending_admission_count_ != 0U) {
      --pending_admission_count_;
    }
    observation_.pending_admission_count = pending_admission_count_;
  }

  void sweep_closed_subscribers() noexcept {
    std::erase_if(subscribers_, [](const auto &subscriber) {
      return subscriber->state() == g7::SubscriberState::Closed;
    });
    std::lock_guard lock{mutex_};
    observation_.resident_subscription_count = subscribers_.size();
  }

  void close_all_subscribers_for_shutdown() noexcept {
    for (const auto &subscriber : subscribers_) {
      subscriber->close_from_owner();
    }
    subscribers_.clear();
    std::lock_guard lock{mutex_};
    observation_.resident_subscription_count = 0U;
  }

  void perform_publication_shutdown_control() noexcept {
    close_all_subscribers_for_shutdown();
    {
      std::lock_guard lock{mutex_};
      publication_admission_open_ = false;
      complete_all_admissions_locked(SubscriptionAdmissionError::ShuttingDown);
      publication_shutdown_completed_ = true;
      observation_.publication_admission_open = false;
      observation_.publication_shutdown = true;
    }
    condition_.notify_all();
  }

  void terminalize_active_subscribers(
      common_wire::ConsumerGapReason reason,
      common_wire::RecoveryAction recovery_action,
      std::optional<std::uint64_t> connection_generation) noexcept {
    sweep_closed_subscribers();
    const auto sampled = sample_publication_time();
    const auto published_at = sampled.value_or(g7::PublicationTime{});
    {
      std::lock_guard lock{mutex_};
      observation_.last_publication_thread_id = std::this_thread::get_id();
    }
    for (const auto &subscriber : subscribers_) {
      static_cast<void>(subscriber->terminalize(
          reason, recovery_action, connection_generation, published_at));
    }
  }

  void perform_reset_request() noexcept {
    auto result = RebootstrapResetResult::Reset;
    ResetKind reset_kind = ResetKind::Recovery;
    {
      std::lock_guard lock{mutex_};
      if (!reset_request_.has_value()) {
        return;
      }
      reset_kind = reset_request_->kind;
      const bool valid_recovery =
          observation_.state == RuntimeState::Faulted ||
          observation_.state == RuntimeState::NeedsResync;
      const bool valid_planned = observation_.state == RuntimeState::Live &&
                                 observation_.projection_status ==
                                     core::ProjectionStatus::Synchronized &&
                                 !observation_.fault_reason.has_value();
      if ((reset_kind == ResetKind::Recovery && !valid_recovery) ||
          (reset_kind == ResetKind::Planned && !valid_planned)) {
        reset_request_->result = RebootstrapResetResult::InvalidState;
        condition_.notify_all();
        return;
      }
    }
    try {
      if (reset_kind == ResetKind::Planned) {
        terminalize_active_subscribers(
            common_wire::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED,
            common_wire::RECOVERY_ACTION_RESUBSCRIBE,
            current_projection_generation_);
      } else {
        terminalize_active_subscribers(
            common_wire::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE,
            common_wire::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT,
            current_projection_generation_);
      }
      projection_.reset();
      bootstrap_.clear();
      current_projection_generation_.reset();
      std::lock_guard lock{mutex_};
      observation_.last_gap.reset();
      observation_.last_install.reset();
      observation_.last_apply.reset();
      observation_.adapter_error.reset();
      observation_.fault_reason.reset();
      observation_.last_update_id.reset();
      observation_.current_projection_generation.reset();
      observation_.bootstrap_occupancy = 0U;
      observation_.last_reset_thread_id = std::this_thread::get_id();
      ++observation_.reset_count;
      refresh_projection_observation_locked();
      observation_.state = RuntimeState::Buffering;
      accepting_ = true;
      publish_tickets_locked();
    } catch (...) {
      result = RebootstrapResetResult::InternalError;
      std::lock_guard lock{mutex_};
      accepting_ = false;
      observation_.fault_reason = FaultReason::InternalError;
      observation_.state = RuntimeState::Faulted;
    }
    {
      std::lock_guard lock{mutex_};
      if (reset_request_.has_value()) {
        reset_request_->result = result;
      }
    }
    condition_.notify_all();
  }

  void process_input(RuntimeInput &input) {
    std::visit(
        [this](auto &value) {
          using Value = std::remove_cvref_t<decltype(value)>;
          if constexpr (std::is_same_v<Value, DepthUpdateInput>) {
            process_depth_update(value);
          } else if constexpr (std::is_same_v<Value, SnapshotInput>) {
            process_snapshot(value);
          } else {
            transition_to_fault(value == InjectedFailure::Transport
                                    ? FaultReason::TransportFailure
                                    : FaultReason::SnapshotFailure,
                                std::nullopt);
          }
        },
        input);
  }

  void process_depth_update(const DepthUpdateInput &input) {
    if (projection_.status() == core::ProjectionStatus::AwaitingBaseline) {
      if (bootstrap_.size() == limits_.bootstrap_capacity) {
        transition_to_fault(FaultReason::BootstrapBufferOverflow, std::nullopt);
        return;
      }
      bootstrap_.push_back(input);
      publish_projection_state();
      return;
    }
    apply_update(input);
  }

  void process_snapshot(const SnapshotInput &input) {
    auto adapted = adapter::adapt_exchange_depth_snapshot(
        input.snapshot, projection_.numeric_spec(), expected_identity_);
    if (std::holds_alternative<adapter::AdapterError>(adapted)) {
      transition_to_fault(FaultReason::AdapterError,
                          std::get<adapter::AdapterError>(adapted));
      return;
    }

    const auto installed =
        std::get<adapter::AdaptedBookBaseline>(adapted).install_into(
            projection_);
    if (std::holds_alternative<adapter::AdapterError>(installed)) {
      transition_to_fault(FaultReason::AdapterError,
                          std::get<adapter::AdapterError>(installed));
      return;
    }

    const auto install_result = std::get<core::InstallResult>(installed);
    {
      std::lock_guard lock{mutex_};
      observation_.last_install = install_result;
    }
    if (install_result.disposition != core::InstallDisposition::Installed) {
      transition_to_fault(FaultReason::ProjectionRejected, std::nullopt);
      return;
    }
    current_projection_generation_ = input.provenance.connection_generation;
    {
      std::lock_guard lock{mutex_};
      observation_.current_projection_generation =
          current_projection_generation_;
    }

    while (!bootstrap_.empty()) {
      auto update = std::move(bootstrap_.front());
      bootstrap_.pop_front();
      apply_update(update);
      if (projection_.status() == core::ProjectionStatus::NeedsResync ||
          is_faulted()) {
        bootstrap_.clear();
        break;
      }
    }
    publish_projection_state();
  }

  void apply_update(const DepthUpdateInput &input) {
    if (current_projection_generation_.has_value() &&
        input.provenance.connection_generation.has_value() &&
        current_projection_generation_ !=
            input.provenance.connection_generation) {
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      if (limits_.performance_baseline != nullptr) {
        limits_.performance_baseline->record_t3(
            input.trace, performance::T3Disposition::InternalFailure);
      }
#endif
      transition_to_fault(FaultReason::InternalError, std::nullopt);
      return;
    }
    auto adapted = adapter::adapt_depth_update(
        input.update, projection_.numeric_spec(), expected_identity_);
    if (std::holds_alternative<adapter::AdapterError>(adapted)) {
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      if (limits_.performance_baseline != nullptr) {
        limits_.performance_baseline->record_t3(
            input.trace, performance::T3Disposition::AdapterFailure);
      }
#endif
      transition_to_fault(FaultReason::AdapterError,
                          std::get<adapter::AdapterError>(adapted));
      return;
    }

    const auto applied =
        std::get<adapter::AdaptedDepthBatch>(adapted).apply_to(projection_);
    if (std::holds_alternative<adapter::AdapterError>(applied)) {
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      if (limits_.performance_baseline != nullptr) {
        limits_.performance_baseline->record_t3(
            input.trace, performance::T3Disposition::AdapterFailure);
      }
#endif
      transition_to_fault(FaultReason::AdapterError,
                          std::get<adapter::AdapterError>(applied));
      return;
    }
    const auto apply_result = std::get<core::ApplyResult>(applied);
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    if (limits_.performance_baseline != nullptr) {
      limits_.performance_baseline->record_t3(
          input.trace, measurement_disposition(apply_result.disposition));
    }
#endif
    {
      std::lock_guard lock{mutex_};
      observation_.last_apply = apply_result;
    }
    if (apply_result.disposition == core::ApplyDisposition::Applied) {
      publish_applied_update(input.update, input.provenance
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                             ,
                             input.trace
#endif
      );
    } else if (apply_result.disposition ==
               core::ApplyDisposition::GapDetected) {
      terminalize_active_subscribers(
          common_wire::CONSUMER_GAP_REASON_UPSTREAM_SEQUENCE_GAP,
          common_wire::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT,
          input.provenance.connection_generation);
    }
    publish_projection_state();
  }

  void publish_applied_update(const market::DepthUpdate &update,
                              SourceProvenance provenance
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                              ,
                              performance::TraceToken trace
#endif
  ) {
    if (publication_shutdown_completed_) {
      return;
    }
    sweep_closed_subscribers();
    if (subscribers_.empty()) {
      return;
    }
    const auto published_at = sample_publication_time();
    if (!published_at.has_value()) {
      transition_to_fault(FaultReason::ClockError, std::nullopt);
      return;
    }

    std::shared_ptr<const market::DepthUpdate> shared_update;
    try {
      shared_update = std::make_shared<const market::DepthUpdate>(update);
    } catch (...) {
      transition_to_fault(FaultReason::InternalError, std::nullopt);
      return;
    }
    {
      std::lock_guard lock{mutex_};
      observation_.last_publication_thread_id = std::this_thread::get_id();
    }
    for (const auto &subscriber : subscribers_) {
      const auto admitted = subscriber->admit_update(
          shared_update, provenance.connection_generation, *published_at
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
          ,
          trace
#endif
      );
      if (admitted == g7::OrdinaryAdmissionResult::AllocationFailure) {
        transition_to_fault(FaultReason::InternalError, std::nullopt);
        return;
      }
    }
  }

  void publish_projection_state() {
    const auto status = projection_.status();
    {
      std::lock_guard lock{mutex_};
      refresh_projection_observation_locked();
      observation_.bootstrap_occupancy = bootstrap_.size();
      if (observation_.state != RuntimeState::Faulted &&
          observation_.state != RuntimeState::Stopping &&
          observation_.state != RuntimeState::Stopped) {
        observation_.state = state_for_projection(status);
      }
      if (status == core::ProjectionStatus::NeedsResync) {
        accepting_ = false;
        discard_ingress_locked();
      }
    }
    condition_.notify_all();
  }

  void refresh_projection_observation_locked() {
    observation_.projection_status = projection_.status();
    const auto last_update_id = projection_.last_update_id();
    observation_.last_update_id =
        last_update_id.has_value()
            ? std::optional<std::uint64_t>{last_update_id->value()}
            : std::nullopt;
    observation_.last_gap = projection_.last_gap();
    observation_.current_projection_generation = current_projection_generation_;
  }

  [[nodiscard]] bool is_faulted() const noexcept {
    std::lock_guard lock{mutex_};
    return observation_.state == RuntimeState::Faulted;
  }

  void transition_to_fault(FaultReason reason,
                           std::optional<adapter::AdapterError> adapter_error) {
    terminalize_active_subscribers(
        common_wire::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE,
        common_wire::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT,
        current_projection_generation_);
    {
      std::lock_guard lock{mutex_};
      accepting_ = false;
      bootstrap_.clear();
      observation_.bootstrap_occupancy = 0U;
      observation_.fault_reason = reason;
      observation_.adapter_error = adapter_error;
      refresh_projection_observation_locked();
      observation_.state = RuntimeState::Faulted;
      discard_ingress_locked();
      if (snapshot_request_.has_value() &&
          !snapshot_request_->result.has_value()) {
        snapshot_request_->result = SnapshotRequestError::Faulted;
      }
    }
    condition_.notify_all();
  }

  void discard_ingress_locked() noexcept {
    if (!ingress_.empty()) {
      processed_ticket_ = std::max(processed_ticket_, ingress_.back().ticket);
      ingress_.clear();
      observation_.ingress_occupancy = 0U;
    }
  }

  void finish_ticket(std::uint64_t ticket) noexcept {
    {
      std::lock_guard lock{mutex_};
      processed_ticket_ = std::max(processed_ticket_, ticket);
      observation_.bootstrap_occupancy = bootstrap_.size();
    }
    condition_.notify_all();
  }

  void perform_snapshot_request() noexcept {
    SnapshotResult result{SnapshotRequestError::NotLive};
    ClockSample sample{};
    bool live = false;
    {
      std::lock_guard lock{mutex_};
      live = observation_.state == RuntimeState::Live;
    }
    if (live) {
      try {
        sample = clock_();
      } catch (...) {
        complete_snapshot_request(SnapshotRequestError::ClockError);
        return;
      }

      try {
        adapter::SnapshotContext context{expected_identity_,
                                         "gateway-g3-runtime",
                                         "1.0.0",
                                         adapter::SnapshotOrigin::GatewayLive,
                                         sample.utc_ns,
                                         sample.monotonic_ns,
                                         std::nullopt};
        auto snapshot = adapter::make_local_order_book_snapshot(
            projection_, context, adapter::SnapshotOptions{});
        if (std::holds_alternative<adapter::AdapterError>(snapshot)) {
          result = std::get<adapter::AdapterError>(snapshot);
        } else {
          result = CapturedSnapshot{
              std::get<core::LocalOrderBookSnapshot>(std::move(snapshot)),
              sample, std::this_thread::get_id()};
        }
      } catch (...) {
        result = SnapshotRequestError::InternalError;
      }
    }

    complete_snapshot_request(std::move(result));
  }

  void complete_snapshot_request(SnapshotResult result) noexcept {
    {
      std::lock_guard lock{mutex_};
      if (snapshot_request_.has_value()) {
        snapshot_request_->result = std::move(result);
      }
    }
    condition_.notify_all();
  }

  RuntimeLimits limits_;
  RuntimeClock clock_;
  adapter::ExpectedIdentity expected_identity_;
  core::BookProjection projection_;
  std::deque<DepthUpdateInput> bootstrap_;
  std::optional<std::uint64_t> current_projection_generation_;
  std::vector<std::shared_ptr<g7::SubscriberChannel>> subscribers_;
  std::uint64_t next_subscription_id_{1U};

  mutable std::mutex mutex_;
  std::condition_variable_any condition_;
  std::deque<IngressItem> ingress_;
  std::deque<std::shared_ptr<AdmissionRequest>> admission_requests_;
  std::optional<FaultReason> pending_fault_;
  std::optional<SnapshotRequest> snapshot_request_;
  std::optional<ResetRequest> reset_request_;
  RuntimeObservation observation_;
  std::thread owner_;
  std::uint64_t last_admitted_ticket_{0U};
  std::uint64_t processed_ticket_{0U};
  std::size_t pending_admission_count_{0U};
  std::function<void()> admission_enqueued_;
  std::function<void()> before_admission_processing_;
  bool started_{false};
  bool accepting_{false};
  bool publication_admission_open_{false};
  bool publication_shutdown_requested_{false};
  bool publication_shutdown_completed_{false};
  bool publication_removal_pending_{false};
  bool stop_requested_{false};
  bool stopped_{false};
  bool owner_ready_{false};
  bool owner_paused_{false};
};

MarketRuntime::MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
                             core::NumericSpec numeric_spec,
                             RuntimeTestOptions test_options)
    : MarketRuntime(limits, std::move(clock), numeric_spec,
                    make_expected_identity(), std::move(test_options)) {}

MarketRuntime::MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
                             core::NumericSpec numeric_spec,
                             adapter::ExpectedIdentity expected_identity,
                             RuntimeTestOptions test_options)
    : impl_{std::make_unique<Impl>(limits, std::move(clock), numeric_spec,
                                   std::move(expected_identity),
                                   std::move(test_options))} {}

MarketRuntime::~MarketRuntime() = default;

StartResult MarketRuntime::start() { return impl_->start(); }

void MarketRuntime::stop() noexcept { impl_->stop(); }

AdmissionResult MarketRuntime::submit_depth_update(market::DepthUpdate update) {
  return submit_depth_update(std::move(update), SourceProvenance{});
}

AdmissionResult
MarketRuntime::submit_depth_update(market::DepthUpdate update,
                                   SourceProvenance provenance) {
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  return submit_depth_update(std::move(update), std::move(provenance), {});
#else
  return impl_->submit(
      DepthUpdateInput{std::move(update), std::move(provenance)});
#endif
}

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
AdmissionResult
MarketRuntime::submit_depth_update(market::DepthUpdate update,
                                   SourceProvenance provenance,
                                   performance::TraceToken trace) {
  return impl_->submit(
      DepthUpdateInput{std::move(update), std::move(provenance), trace});
}
#endif

AdmissionResult
MarketRuntime::submit_snapshot(market::ExchangeDepthSnapshot snapshot) {
  return submit_snapshot(std::move(snapshot), SourceProvenance{});
}

AdmissionResult
MarketRuntime::submit_snapshot(market::ExchangeDepthSnapshot snapshot,
                               SourceProvenance provenance) {
  return impl_->submit(
      SnapshotInput{std::move(snapshot), std::move(provenance)});
}

AdmissionResult MarketRuntime::submit_transport_failure() {
  return impl_->submit(InjectedFailure::Transport);
}

AdmissionResult MarketRuntime::submit_snapshot_failure() {
  return impl_->submit(InjectedFailure::Snapshot);
}

RuntimeObservation MarketRuntime::observe() { return impl_->observe(); }

SnapshotResult MarketRuntime::capture_snapshot() {
  return impl_->capture_snapshot();
}

SubscriptionAdmissionResult MarketRuntime::admit_order_book_subscription(
    g7::ValidatedOrderBookSubscription subscription) {
  return impl_->admit_order_book_subscription(std::move(subscription));
}

void MarketRuntime::notify_subscriber_closed() noexcept {
  impl_->notify_subscriber_closed();
}

void MarketRuntime::close_publication_admission() noexcept {
  impl_->close_publication_admission();
}

PublicationShutdownResult MarketRuntime::shutdown_publication() noexcept {
  return impl_->shutdown_publication();
}

RebootstrapResetResult MarketRuntime::reset_for_rebootstrap() {
  return impl_->reset_for_rebootstrap();
}

PlannedRebootstrapResetResult
MarketRuntime::reset_live_for_planned_rebootstrap() {
  return impl_->reset_live_for_planned_rebootstrap();
}

std::optional<RuntimeObservation>
MarketRuntime::wait_until_live_or_recovery_required(
    std::stop_token stop_token) {
  return impl_->wait_until_live_or_recovery_required(stop_token);
}

std::optional<RuntimeObservation>
MarketRuntime::wait_until_recovery_required(std::stop_token stop_token) {
  return impl_->wait_until_recovery_required(stop_token);
}

TimedRecoveryWaitResult MarketRuntime::wait_until_recovery_required_for(
    std::stop_token stop_token, std::chrono::nanoseconds duration) {
  return impl_->wait_until_recovery_required_for(stop_token, duration);
}

IngressObservation MarketRuntime::ingress_observation() const noexcept {
  return impl_->ingress_observation();
}

void MarketRuntime::release_owner_for_testing() noexcept {
  impl_->release_owner_for_testing();
}

void MarketRuntime::pause_owner_for_testing() noexcept {
  impl_->pause_owner_for_testing();
}

} // namespace binance_market_data::gateway::g3
