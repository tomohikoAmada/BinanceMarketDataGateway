#include "event_publication.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace binance_market_data::gateway::g9 {

namespace {

[[nodiscard]] std::shared_ptr<const EventPublicationRecord>
make_accepted_record(const ValidatedEventSubscription &subscription,
                     const std::string &subscription_id,
                     const std::string &gateway_instance_id,
                     EventPublicationTime published_at) {
  gateway_wire::SubscriptionAccepted accepted;
  accepted.set_request_id(subscription.request_id);
  accepted.set_subscription_id(subscription_id);
  accepted.set_schema_version(kEventSubscriptionAcceptedSchema);
  accepted.set_gateway_instance_id(gateway_instance_id);
  accepted.set_accepted_time_utc_ns(published_at.utc_ns);
  accepted.add_negotiated_payload_schema_versions(
      subscription.negotiated_payload_schema_version);
  return std::make_shared<const EventPublicationRecord>(EventPublicationRecord{
      1U, std::nullopt, published_at, std::move(accepted)});
}

} // namespace

common_wire::Stream
normalized_event_stream(const g4::NormalizedMarketEvent &event) noexcept {
  if (std::holds_alternative<g4::market::DepthUpdate>(event)) {
    return common_wire::STREAM_DIFF_DEPTH;
  }
  if (std::holds_alternative<g4::market::AggTrade>(event)) {
    return common_wire::STREAM_AGG_TRADE;
  }
  return common_wire::STREAM_BOOK_TICKER;
}

EventSubscriberChannel::EventSubscriberChannel(std::string subscription_id,
                                               std::string gateway_instance_id,
                                               common_wire::Stream stream,
                                               std::uint64_t source_generation,
                                               std::size_t ordinary_capacity)
    : subscription_id_{std::move(subscription_id)},
      gateway_instance_id_{std::move(gateway_instance_id)}, stream_{stream},
      source_generation_{source_generation},
      ordinary_capacity_{ordinary_capacity}, ordinary_ring_(ordinary_capacity) {
  if (ordinary_capacity_ == 0U || source_generation_ == 0U ||
      stream_ == common_wire::STREAM_UNSPECIFIED) {
    throw std::invalid_argument{"invalid event subscriber channel"};
  }
}

bool EventSubscriberChannel::stage_accepted(
    std::shared_ptr<const EventPublicationRecord> accepted) noexcept {
  if (accepted == nullptr || accepted->session_sequence != 1U ||
      accepted->connection_generation.has_value() ||
      !std::holds_alternative<gateway_wire::SubscriptionAccepted>(
          accepted->payload)) {
    return false;
  }
  std::lock_guard lock{mutex_};
  if (state_ != EventChannelState::Active || size_ != 0U ||
      !ring_push_locked(std::move(accepted))) {
    return false;
  }
  next_session_sequence_ = 2U;
  condition_.notify_one();
  return true;
}

EventAdmissionResult EventSubscriberChannel::admit_event(
    const std::shared_ptr<const g4::NormalizedMarketEvent> &event,
    EventPublicationTime published_at) noexcept {
  std::lock_guard lock{mutex_};
  if (state_ != EventChannelState::Active) {
    return EventAdmissionResult::Inactive;
  }
  if (event == nullptr || normalized_event_stream(*event) != stream_) {
    return EventAdmissionResult::AllocationFailure;
  }
  if (size_ == ordinary_capacity_) {
    terminal_.emplace(EventTerminalDescriptor{
        next_session_sequence_, source_generation_, published_at,
        common_wire::CONSUMER_GAP_REASON_SLOW_CONSUMER,
        common_wire::RECOVERY_ACTION_RESUBSCRIBE});
    if (next_session_sequence_ != std::numeric_limits<std::uint64_t>::max()) {
      ++next_session_sequence_;
    }
    state_ = EventChannelState::TerminalGap;
    condition_.notify_one();
    return EventAdmissionResult::Terminalized;
  }
  if (next_session_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
    return EventAdmissionResult::AllocationFailure;
  }
  try {
    auto record =
        std::make_shared<const EventPublicationRecord>(EventPublicationRecord{
            next_session_sequence_, source_generation_, published_at, event});
    if (!ring_push_locked(std::move(record))) {
      return EventAdmissionResult::AllocationFailure;
    }
  } catch (...) {
    return EventAdmissionResult::AllocationFailure;
  }
  ++next_session_sequence_;
  condition_.notify_one();
  return EventAdmissionResult::Admitted;
}

bool EventSubscriberChannel::terminalize_replacement(
    EventPublicationTime published_at) noexcept {
  std::lock_guard lock{mutex_};
  if (state_ != EventChannelState::Active || terminal_.has_value()) {
    return false;
  }
  terminal_.emplace(EventTerminalDescriptor{
      next_session_sequence_, source_generation_, published_at,
      common_wire::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED,
      common_wire::RECOVERY_ACTION_RESUBSCRIBE});
  if (next_session_sequence_ != std::numeric_limits<std::uint64_t>::max()) {
    ++next_session_sequence_;
  }
  state_ = EventChannelState::TerminalGap;
  condition_.notify_one();
  return true;
}

bool EventSubscriberChannel::close_unavailable() noexcept {
  std::lock_guard lock{mutex_};
  if (state_ != EventChannelState::Active) {
    return false;
  }
  state_ = EventChannelState::TerminalUnavailable;
  condition_.notify_one();
  return true;
}

PeekedEventPublication EventSubscriberChannel::peek() const {
  std::lock_guard lock{mutex_};
  if (state_ == EventChannelState::Closed) {
    return {};
  }
  if (size_ != 0U) {
    return {ordinary_ring_[head_], std::nullopt};
  }
  if (terminal_.has_value()) {
    return {nullptr, terminal_};
  }
  return {};
}

EventAcknowledgeResult EventSubscriberChannel::acknowledge(
    const PeekedEventPublication &publication) noexcept {
  std::lock_guard lock{mutex_};
  if (state_ == EventChannelState::Closed) {
    return EventAcknowledgeResult::Closed;
  }
  if (publication.ordinary != nullptr) {
    if (size_ == 0U || ordinary_ring_[head_] != publication.ordinary) {
      return EventAcknowledgeResult::Mismatch;
    }
    ring_pop_locked();
    condition_.notify_one();
    return EventAcknowledgeResult::Acknowledged;
  }
  if (publication.terminal.has_value() && size_ == 0U &&
      terminal_.has_value() && *terminal_ == *publication.terminal) {
    terminal_.reset();
    state_ = EventChannelState::Closed;
    condition_.notify_all();
    return EventAcknowledgeResult::Closed;
  }
  return EventAcknowledgeResult::Mismatch;
}

void EventSubscriberChannel::wait_for_change(
    std::chrono::milliseconds duration) const {
  std::unique_lock lock{mutex_};
  condition_.wait_for(lock, duration, [this] {
    return state_ != EventChannelState::Active || size_ != 0U;
  });
}

void EventSubscriberChannel::close_from_owner() noexcept {
  {
    std::lock_guard lock{mutex_};
    state_ = EventChannelState::Closed;
  }
  condition_.notify_all();
}

void EventSubscriberChannel::close_from_writer() noexcept {
  {
    std::lock_guard lock{mutex_};
    state_ = EventChannelState::Closed;
    clear_from_writer_locked();
  }
  condition_.notify_all();
}

EventChannelState EventSubscriberChannel::state() const noexcept {
  std::lock_guard lock{mutex_};
  return state_;
}

std::size_t EventSubscriberChannel::ordinary_size() const noexcept {
  std::lock_guard lock{mutex_};
  return size_;
}

bool EventSubscriberChannel::terminal_reserved() const noexcept {
  std::lock_guard lock{mutex_};
  return terminal_.has_value();
}

std::uint64_t EventSubscriberChannel::next_session_sequence() const noexcept {
  std::lock_guard lock{mutex_};
  return next_session_sequence_;
}

const std::string &EventSubscriberChannel::subscription_id() const noexcept {
  return subscription_id_;
}

const std::string &
EventSubscriberChannel::gateway_instance_id() const noexcept {
  return gateway_instance_id_;
}

common_wire::Stream EventSubscriberChannel::stream() const noexcept {
  return stream_;
}

std::uint64_t EventSubscriberChannel::source_generation() const noexcept {
  return source_generation_;
}

bool EventSubscriberChannel::ring_push_locked(
    std::shared_ptr<const EventPublicationRecord> record) noexcept {
  if (size_ == ordinary_capacity_ || record == nullptr) {
    return false;
  }
  const auto tail = (head_ + size_) % ordinary_capacity_;
  ordinary_ring_[tail] = std::move(record);
  ++size_;
  return true;
}

void EventSubscriberChannel::ring_pop_locked() noexcept {
  ordinary_ring_[head_].reset();
  head_ = (head_ + 1U) % ordinary_capacity_;
  --size_;
}

void EventSubscriberChannel::clear_from_writer_locked() noexcept {
  while (size_ != 0U) {
    ring_pop_locked();
  }
  terminal_.reset();
}

EventPublication::EventPublication(std::string gateway_instance_id,
                                   g3::RuntimeClock clock,
                                   EventPublicationLimits limits)
    : gateway_instance_id_{std::move(gateway_instance_id)},
      clock_{std::move(clock)}, limits_{limits} {
  if (gateway_instance_id_.empty() || !clock_ ||
      limits_.maximum_active_subscriptions == 0U ||
      limits_.maximum_active_subscriptions > kMaximumActiveEventSubscriptions ||
      limits_.ordinary_queue_capacity == 0U ||
      limits_.ordinary_queue_capacity > kEventOrdinaryQueueCapacity) {
    throw std::invalid_argument{"invalid G9 event publication configuration"};
  }
  channels_.reserve(limits_.maximum_active_subscriptions);
}

bool EventPublication::open_generation(std::uint64_t generation) noexcept {
  std::lock_guard lock{mutex_};
  if (generation == 0U || source_state_ == EventSourceState::Shutdown ||
      source_state_ == EventSourceState::PermanentlyClosed ||
      source_state_ == EventSourceState::Open ||
      source_state_ == EventSourceState::Quiesced ||
      (source_generation_.has_value() && generation <= *source_generation_)) {
    return false;
  }
  source_generation_ = generation;
  source_state_ = EventSourceState::Open;
  return true;
}

bool EventPublication::quiesce_generation(std::uint64_t generation) noexcept {
  std::lock_guard lock{mutex_};
  if (source_state_ != EventSourceState::Open ||
      source_generation_ != generation) {
    return false;
  }
  source_state_ = EventSourceState::Quiesced;
  return true;
}

bool EventPublication::close_generation_replaced(
    std::uint64_t generation) noexcept {
  const auto published_at = sample_time();
  if (!published_at.has_value()) {
    return false;
  }
  std::lock_guard lock{mutex_};
  if (source_state_ != EventSourceState::Quiesced ||
      source_generation_ != generation) {
    return false;
  }
  if (!terminalize_replaced_locked(generation, *published_at)) {
    return false;
  }
  source_state_ = EventSourceState::Closed;
  return true;
}

bool EventPublication::close_generation_permanently(
    std::uint64_t generation) noexcept {
  std::lock_guard lock{mutex_};
  if (source_state_ != EventSourceState::Quiesced ||
      source_generation_ != generation) {
    return false;
  }
  for (const auto &channel : channels_) {
    if (channel != nullptr && channel->source_generation() == generation) {
      static_cast<void>(channel->close_unavailable());
    }
  }
  source_state_ = EventSourceState::PermanentlyClosed;
  return true;
}

EventSubscriptionAdmission
EventPublication::admit(const ValidatedEventSubscription &subscription) {
  const auto published_at = sample_time();
  if (!published_at.has_value()) {
    return EventSubscriptionAdmissionError::ClockError;
  }
  std::lock_guard lock{mutex_};
  if (source_state_ == EventSourceState::Shutdown) {
    return EventSubscriptionAdmissionError::ShuttingDown;
  }
  if (source_state_ != EventSourceState::Open ||
      !source_generation_.has_value()) {
    return EventSubscriptionAdmissionError::SourceUnavailable;
  }
  if (channels_.size() == limits_.maximum_active_subscriptions) {
    return EventSubscriptionAdmissionError::ActiveLimit;
  }
  if (next_subscription_id_ == std::numeric_limits<std::uint64_t>::max()) {
    return EventSubscriptionAdmissionError::IdExhausted;
  }
  const auto subscription_id = "ev-" + std::to_string(next_subscription_id_);
  try {
    auto channel = std::make_shared<EventSubscriberChannel>(
        subscription_id, gateway_instance_id_, subscription.stream,
        *source_generation_, limits_.ordinary_queue_capacity);
    auto accepted = make_accepted_record(subscription, subscription_id,
                                         gateway_instance_id_, *published_at);
    if (!channel->stage_accepted(std::move(accepted))) {
      return EventSubscriptionAdmissionError::InternalError;
    }
    channels_.push_back(channel);
    ++next_subscription_id_;
    return AcceptedEventSubscription{std::move(channel)};
  } catch (...) {
    return EventSubscriptionAdmissionError::InternalError;
  }
}

EventPublishResult EventPublication::publish(
    const std::shared_ptr<const g4::NormalizedMarketEvent> &event,
    std::uint64_t generation) noexcept {
  if (event == nullptr) {
    return EventPublishResult::InvariantFailure;
  }
  const auto published_at = sample_time();
  if (!published_at.has_value()) {
    return EventPublishResult::InvariantFailure;
  }
  std::lock_guard lock{mutex_};
  if (source_state_ == EventSourceState::Shutdown ||
      (source_state_ == EventSourceState::Closed &&
       (!source_generation_.has_value() || generation > *source_generation_))) {
    return EventPublishResult::IgnoredBeforeOpen;
  }
  if (source_state_ != EventSourceState::Open ||
      source_generation_ != generation) {
    return EventPublishResult::InvariantFailure;
  }
  const auto stream = normalized_event_stream(*event);
  for (const auto &channel : channels_) {
    if (channel == nullptr || channel->stream() != stream ||
        channel->source_generation() != generation) {
      continue;
    }
    const auto result = channel->admit_event(event, *published_at);
    if (result == EventAdmissionResult::AllocationFailure) {
      return EventPublishResult::InvariantFailure;
    }
  }
  return EventPublishResult::Published;
}

void EventPublication::remove(
    const std::shared_ptr<EventSubscriberChannel> &channel) noexcept {
  if (channel == nullptr) {
    return;
  }
  std::lock_guard lock{mutex_};
  const auto found = std::find(channels_.begin(), channels_.end(), channel);
  if (found != channels_.end()) {
    channels_.erase(found);
  }
}

void EventPublication::shutdown() noexcept {
  std::lock_guard lock{mutex_};
  if (source_state_ == EventSourceState::Shutdown) {
    return;
  }
  source_state_ = EventSourceState::Shutdown;
  for (const auto &channel : channels_) {
    if (channel != nullptr) {
      channel->close_from_owner();
    }
  }
}

EventPublicationObservation EventPublication::observe() const noexcept {
  std::lock_guard lock{mutex_};
  return {source_state_, source_generation_, channels_.size()};
}

const std::string &EventPublication::gateway_instance_id() const noexcept {
  return gateway_instance_id_;
}

std::optional<EventPublicationTime> EventPublication::sample_time() noexcept {
  try {
    const auto sampled = clock_();
    return EventPublicationTime{sampled.utc_ns, sampled.monotonic_ns};
  } catch (...) {
    return std::nullopt;
  }
}

bool EventPublication::terminalize_replaced_locked(
    std::uint64_t generation, EventPublicationTime published_at) noexcept {
  for (const auto &channel : channels_) {
    if (channel != nullptr && channel->source_generation() == generation) {
      static_cast<void>(channel->terminalize_replacement(published_at));
    }
  }
  return true;
}

} // namespace binance_market_data::gateway::g9
