#include "order_book_publication.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace binance_market_data::gateway::g7 {

PublicationPayloadKind PublicationRecord::kind() const noexcept {
  if (std::holds_alternative<gateway_wire::SubscriptionAccepted>(payload)) {
    return PublicationPayloadKind::SubscriptionAccepted;
  }
  if (std::holds_alternative<projection_wire::LocalOrderBookSnapshot>(
          payload)) {
    return PublicationPayloadKind::Snapshot;
  }
  return PublicationPayloadKind::DepthUpdate;
}

SubscriberChannel::SubscriberChannel(std::string subscription_id,
                                     std::string gateway_instance_id,
                                     std::size_t ordinary_capacity)
    : subscription_id_{std::move(subscription_id)},
      gateway_instance_id_{std::move(gateway_instance_id)},
      ordinary_capacity_{ordinary_capacity}, ordinary_ring_(ordinary_capacity) {
  if (ordinary_capacity_ == 0U) {
    throw std::invalid_argument{"subscriber ordinary capacity must be nonzero"};
  }
}

bool SubscriberChannel::stage_initial(
    std::shared_ptr<const PublicationRecord> accepted,
    std::shared_ptr<const PublicationRecord> snapshot) noexcept {
  if (accepted == nullptr || snapshot == nullptr ||
      accepted->session_sequence != 1U || snapshot->session_sequence != 2U ||
      accepted->kind() != PublicationPayloadKind::SubscriptionAccepted ||
      snapshot->kind() != PublicationPayloadKind::Snapshot) {
    return false;
  }

  std::lock_guard lock{mutex_};
  if (state_ != SubscriberState::Active || size_ != 0U ||
      ordinary_capacity_ < 2U) {
    return false;
  }
  if (!ring_push_locked(std::move(accepted)) ||
      !ring_push_locked(std::move(snapshot))) {
    clear_from_writer_locked();
    state_ = SubscriberState::Active;
    next_session_sequence_ = 1U;
    return false;
  }
  next_session_sequence_ = 3U;
  condition_.notify_one();
  return true;
}

OrdinaryAdmissionResult SubscriberChannel::admit_update(
    const std::shared_ptr<const market_wire::DepthUpdate> &update,
    std::optional<std::uint64_t> connection_generation,
    PublicationTime published_at) noexcept {
  std::lock_guard lock{mutex_};
  if (state_ != SubscriberState::Active) {
    return OrdinaryAdmissionResult::Inactive;
  }
  if (size_ == ordinary_capacity_) {
    terminal_.emplace(
        TerminalDescriptor{next_session_sequence_, std::nullopt, published_at,
                           common_wire::CONSUMER_GAP_REASON_SLOW_CONSUMER,
                           common_wire::RECOVERY_ACTION_RESUBSCRIBE});
    if (next_session_sequence_ != std::numeric_limits<std::uint64_t>::max()) {
      ++next_session_sequence_;
    }
    state_ = SubscriberState::TerminalGap;
    condition_.notify_one();
    return OrdinaryAdmissionResult::Terminalized;
  }
  if (next_session_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
    return OrdinaryAdmissionResult::AllocationFailure;
  }
  try {
    auto record = std::make_shared<const PublicationRecord>(PublicationRecord{
        next_session_sequence_, connection_generation, published_at, update});
    if (!ring_push_locked(std::move(record))) {
      return OrdinaryAdmissionResult::AllocationFailure;
    }
  } catch (...) {
    return OrdinaryAdmissionResult::AllocationFailure;
  }
  ++next_session_sequence_;
  condition_.notify_one();
  return OrdinaryAdmissionResult::Admitted;
}

bool SubscriberChannel::terminalize(
    common_wire::ConsumerGapReason reason,
    common_wire::RecoveryAction recovery_action,
    std::optional<std::uint64_t> connection_generation,
    PublicationTime published_at) noexcept {
  std::lock_guard lock{mutex_};
  if (state_ != SubscriberState::Active || terminal_.has_value()) {
    return false;
  }
  terminal_.emplace(TerminalDescriptor{next_session_sequence_,
                                       connection_generation, published_at,
                                       reason, recovery_action});
  if (next_session_sequence_ != std::numeric_limits<std::uint64_t>::max()) {
    ++next_session_sequence_;
  }
  state_ = SubscriberState::TerminalGap;
  condition_.notify_one();
  return true;
}

PeekedPublication SubscriberChannel::peek() const {
  std::lock_guard lock{mutex_};
  if (state_ == SubscriberState::Closed) {
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

AcknowledgeResult
SubscriberChannel::acknowledge(const PeekedPublication &publication) noexcept {
  std::lock_guard lock{mutex_};
  if (state_ == SubscriberState::Closed) {
    return AcknowledgeResult::Closed;
  }
  if (publication.ordinary != nullptr) {
    if (size_ == 0U || ordinary_ring_[head_] != publication.ordinary) {
      return AcknowledgeResult::Mismatch;
    }
    ring_pop_locked();
    condition_.notify_one();
    return AcknowledgeResult::Acknowledged;
  }
  if (publication.terminal.has_value() && size_ == 0U &&
      terminal_.has_value() && *terminal_ == *publication.terminal) {
    terminal_.reset();
    state_ = SubscriberState::Closed;
    condition_.notify_all();
    return AcknowledgeResult::Closed;
  }
  return AcknowledgeResult::Mismatch;
}

void SubscriberChannel::wait_for_change(
    std::chrono::milliseconds duration) const {
  std::unique_lock lock{mutex_};
  condition_.wait_for(lock, duration, [this] {
    return state_ != SubscriberState::Active || size_ != 0U;
  });
}

void SubscriberChannel::close_from_owner() noexcept {
  {
    std::lock_guard lock{mutex_};
    state_ = SubscriberState::Closed;
  }
  condition_.notify_all();
}

void SubscriberChannel::close_from_writer() noexcept {
  {
    std::lock_guard lock{mutex_};
    state_ = SubscriberState::Closed;
    clear_from_writer_locked();
  }
  condition_.notify_all();
}

SubscriberState SubscriberChannel::state() const noexcept {
  std::lock_guard lock{mutex_};
  return state_;
}

std::size_t SubscriberChannel::ordinary_size() const noexcept {
  std::lock_guard lock{mutex_};
  return size_;
}

std::size_t SubscriberChannel::ordinary_capacity() const noexcept {
  return ordinary_capacity_;
}

bool SubscriberChannel::terminal_reserved() const noexcept {
  std::lock_guard lock{mutex_};
  return terminal_.has_value();
}

std::uint64_t SubscriberChannel::next_session_sequence() const noexcept {
  std::lock_guard lock{mutex_};
  return next_session_sequence_;
}

const std::string &SubscriberChannel::subscription_id() const noexcept {
  return subscription_id_;
}

const std::string &SubscriberChannel::gateway_instance_id() const noexcept {
  return gateway_instance_id_;
}

bool SubscriberChannel::ring_push_locked(
    std::shared_ptr<const PublicationRecord> record) noexcept {
  if (size_ == ordinary_capacity_ || record == nullptr) {
    return false;
  }
  const auto tail = (head_ + size_) % ordinary_capacity_;
  ordinary_ring_[tail] = std::move(record);
  ++size_;
  return true;
}

void SubscriberChannel::ring_pop_locked() noexcept {
  ordinary_ring_[head_].reset();
  head_ = (head_ + 1U) % ordinary_capacity_;
  --size_;
}

void SubscriberChannel::clear_from_writer_locked() noexcept {
  while (size_ != 0U) {
    ring_pop_locked();
  }
  terminal_.reset();
}

std::shared_ptr<const PublicationRecord> make_accepted_record(
    const std::string &request_id, const std::string &subscription_id,
    const std::string &gateway_instance_id, PublicationTime published_at) {
  gateway_wire::SubscriptionAccepted accepted;
  accepted.set_request_id(request_id);
  accepted.set_subscription_id(subscription_id);
  accepted.set_schema_version(kSubscriptionAcceptedSchema);
  accepted.set_gateway_instance_id(gateway_instance_id);
  accepted.set_accepted_time_utc_ns(published_at.utc_ns);
  accepted.add_negotiated_payload_schema_versions(kSnapshotSchema);
  accepted.add_negotiated_payload_schema_versions(kUpdateSchema);
  return std::make_shared<const PublicationRecord>(
      PublicationRecord{1U, std::nullopt, published_at, std::move(accepted)});
}

std::shared_ptr<const PublicationRecord>
make_snapshot_record(projection_wire::LocalOrderBookSnapshot snapshot,
                     std::optional<std::uint64_t> connection_generation,
                     PublicationTime published_at) {
  return std::make_shared<const PublicationRecord>(PublicationRecord{
      2U, connection_generation, published_at, std::move(snapshot)});
}

} // namespace binance_market_data::gateway::g7
