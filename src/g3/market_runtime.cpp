#include "market_runtime.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace binance_market_data::gateway::g3 {

namespace {

[[nodiscard]] core::NumericSpec make_numeric_spec() {
  const auto price_scale = core::DecimalScale::create(2U);
  const auto quantity_scale = core::DecimalScale::create(3U);
  if (!price_scale.has_value() || !quantity_scale.has_value()) {
    std::abort();
  }
  return {*price_scale, *quantity_scale};
}

[[nodiscard]] adapter::ExpectedIdentity make_expected_identity() {
  return {"BTCUSDT", core::SequencePolicyKind::Spot};
}

enum class InjectedFailure : std::uint8_t {
  Transport,
  Snapshot,
};

using RuntimeInput =
    std::variant<market::DepthUpdate, market::ExchangeDepthSnapshot,
                 InjectedFailure>;

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

} // namespace

class MarketRuntime::Impl final {
public:
  Impl(RuntimeLimits limits, RuntimeClock clock,
       RuntimeTestOptions test_options)
      : limits_{limits}, clock_{std::move(clock)},
        expected_identity_{make_expected_identity()},
        projection_{make_numeric_spec(), core::SequencePolicyKind::Spot},
        owner_paused_{test_options.owner_starts_paused} {
    if (limits_.ingress_capacity == 0U || limits_.bootstrap_capacity == 0U) {
      throw std::invalid_argument{"G3 runtime capacities must be nonzero"};
    }
    if (!clock_) {
      throw std::invalid_argument{"G3 runtime clock must be injected"};
    }
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
      accepting_ = false;
      pending_fault_ = FaultReason::IngressOverflow;
      condition_.notify_all();
      return AdmissionResult::Full;
    }

    ++last_admitted_ticket_;
    ingress_.push_back(IngressItem{last_admitted_ticket_, std::move(input)});
    observation_.ingress_occupancy = ingress_.size();
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
    return observation_;
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

private:
  struct SnapshotRequest final {
    explicit SnapshotRequest(std::uint64_t target_value)
        : target{target_value} {}

    std::uint64_t target;
    std::optional<SnapshotResult> result;
  };

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
      bool perform_snapshot = false;
      std::optional<FaultReason> pending_fault;
      {
        std::unique_lock lock{mutex_};
        condition_.wait(lock, [this] {
          if (stop_requested_) {
            return true;
          }
          if (owner_paused_) {
            return false;
          }
          return pending_fault_.has_value() || snapshot_ready_locked() ||
                 !ingress_.empty();
        });

        if (!stop_requested_ && owner_paused_) {
          continue;
        }
        if (pending_fault_.has_value()) {
          pending_fault = pending_fault_;
          pending_fault_.reset();
        } else if (snapshot_ready_locked()) {
          perform_snapshot = true;
        } else if (!ingress_.empty()) {
          item.emplace(std::move(ingress_.front()));
          ingress_.pop_front();
          observation_.ingress_occupancy = ingress_.size();
        } else if (stop_requested_) {
          break;
        }
      }

      if (pending_fault.has_value()) {
        transition_to_fault(*pending_fault, std::nullopt);
        continue;
      }
      if (perform_snapshot) {
        perform_snapshot_request();
        continue;
      }
      if (!item.has_value()) {
        continue;
      }

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
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool snapshot_ready_locked() const noexcept {
    return snapshot_request_.has_value() &&
           !snapshot_request_->result.has_value() &&
           processed_ticket_ >= snapshot_request_->target;
  }

  void process_input(RuntimeInput &input) {
    std::visit(
        [this](auto &value) {
          using Value = std::remove_cvref_t<decltype(value)>;
          if constexpr (std::is_same_v<Value, market::DepthUpdate>) {
            process_depth_update(value);
          } else if constexpr (std::is_same_v<Value,
                                              market::ExchangeDepthSnapshot>) {
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

  void process_depth_update(const market::DepthUpdate &update) {
    if (projection_.status() == core::ProjectionStatus::AwaitingBaseline) {
      if (bootstrap_.size() == limits_.bootstrap_capacity) {
        transition_to_fault(FaultReason::BootstrapBufferOverflow, std::nullopt);
        return;
      }
      bootstrap_.push_back(update);
      publish_projection_state();
      return;
    }
    apply_update(update);
  }

  void process_snapshot(const market::ExchangeDepthSnapshot &snapshot) {
    auto adapted = adapter::adapt_exchange_depth_snapshot(
        snapshot, projection_.numeric_spec(), expected_identity_);
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

  void apply_update(const market::DepthUpdate &update) {
    auto adapted = adapter::adapt_depth_update(
        update, projection_.numeric_spec(), expected_identity_);
    if (std::holds_alternative<adapter::AdapterError>(adapted)) {
      transition_to_fault(FaultReason::AdapterError,
                          std::get<adapter::AdapterError>(adapted));
      return;
    }

    const auto applied =
        std::get<adapter::AdaptedDepthBatch>(adapted).apply_to(projection_);
    if (std::holds_alternative<adapter::AdapterError>(applied)) {
      transition_to_fault(FaultReason::AdapterError,
                          std::get<adapter::AdapterError>(applied));
      return;
    }
    {
      std::lock_guard lock{mutex_};
      observation_.last_apply = std::get<core::ApplyResult>(applied);
    }
    publish_projection_state();
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
  }

  [[nodiscard]] bool is_faulted() const noexcept {
    std::lock_guard lock{mutex_};
    return observation_.state == RuntimeState::Faulted;
  }

  void transition_to_fault(FaultReason reason,
                           std::optional<adapter::AdapterError> adapter_error) {
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
    try {
      bool live = false;
      {
        std::lock_guard lock{mutex_};
        live = observation_.state == RuntimeState::Live;
      }
      if (live) {
        sample = clock_();
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
      }
    } catch (...) {
      result = SnapshotRequestError::ClockError;
    }

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
  std::deque<market::DepthUpdate> bootstrap_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<IngressItem> ingress_;
  std::optional<FaultReason> pending_fault_;
  std::optional<SnapshotRequest> snapshot_request_;
  RuntimeObservation observation_;
  std::thread owner_;
  std::uint64_t last_admitted_ticket_{0U};
  std::uint64_t processed_ticket_{0U};
  bool started_{false};
  bool accepting_{false};
  bool stop_requested_{false};
  bool stopped_{false};
  bool owner_ready_{false};
  bool owner_paused_{false};
};

MarketRuntime::MarketRuntime(RuntimeLimits limits, RuntimeClock clock,
                             RuntimeTestOptions test_options)
    : impl_{std::make_unique<Impl>(limits, std::move(clock), test_options)} {}

MarketRuntime::~MarketRuntime() = default;

StartResult MarketRuntime::start() { return impl_->start(); }

void MarketRuntime::stop() noexcept { impl_->stop(); }

AdmissionResult MarketRuntime::submit_depth_update(market::DepthUpdate update) {
  return impl_->submit(std::move(update));
}

AdmissionResult
MarketRuntime::submit_snapshot(market::ExchangeDepthSnapshot snapshot) {
  return impl_->submit(std::move(snapshot));
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

IngressObservation MarketRuntime::ingress_observation() const noexcept {
  return impl_->ingress_observation();
}

void MarketRuntime::release_owner_for_testing() noexcept {
  impl_->release_owner_for_testing();
}

} // namespace binance_market_data::gateway::g3
