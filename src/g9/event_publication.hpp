#pragma once

#include "spot_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_messages.pb.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::g9 {

namespace common_wire = ::binance_market_data::common::v1;
namespace gateway_wire = ::binance_market_data::gateway::v1;

inline constexpr std::size_t kMaximumActiveEventSubscriptions = 8U;
inline constexpr std::size_t kEventOrdinaryQueueCapacity = 64U;
inline constexpr std::size_t kEventTerminalControlCapacity = 1U;
inline constexpr char kEventRequestSchema[] = "event-subscription-request.v1";
inline constexpr char kEventSubscriptionAcceptedSchema[] =
    "subscription-accepted.v1";
inline constexpr char kDepthEventSchema[] = "depth-update.v1";
inline constexpr char kAggTradeEventSchema[] = "agg-trade.v1";
inline constexpr char kBookTickerEventSchema[] = "book-ticker.v1";

struct EventPublicationLimits final {
  std::size_t maximum_active_subscriptions{kMaximumActiveEventSubscriptions};
  std::size_t ordinary_queue_capacity{kEventOrdinaryQueueCapacity};
};

struct ValidatedEventSubscription final {
  std::string request_id;
  common_wire::Stream stream{common_wire::STREAM_UNSPECIFIED};
  std::string negotiated_payload_schema_version;
};

struct EventPublicationTime final {
  std::uint64_t utc_ns{0U};
  std::optional<std::uint64_t> monotonic_ns;

  friend constexpr bool operator==(EventPublicationTime,
                                   EventPublicationTime) noexcept = default;
};

using EventOrdinaryPayload =
    std::variant<gateway_wire::SubscriptionAccepted,
                 std::shared_ptr<const g4::NormalizedMarketEvent>>;

struct EventPublicationRecord final {
  std::uint64_t session_sequence{0U};
  std::optional<std::uint64_t> connection_generation;
  EventPublicationTime published_at;
  EventOrdinaryPayload payload;
};

struct EventTerminalDescriptor final {
  std::uint64_t session_sequence{0U};
  std::uint64_t connection_generation{0U};
  EventPublicationTime published_at;
  common_wire::ConsumerGapReason reason{
      common_wire::CONSUMER_GAP_REASON_UNSPECIFIED};
  common_wire::RecoveryAction recovery_action{
      common_wire::RECOVERY_ACTION_UNSPECIFIED};

  friend constexpr bool
  operator==(const EventTerminalDescriptor &,
             const EventTerminalDescriptor &) noexcept = default;
};

enum class EventChannelState : std::uint8_t {
  Active,
  TerminalGap,
  TerminalUnavailable,
  Closed,
};

enum class EventAdmissionResult : std::uint8_t {
  Admitted,
  Terminalized,
  Inactive,
  AllocationFailure,
};

struct PeekedEventPublication final {
  std::shared_ptr<const EventPublicationRecord> ordinary;
  std::optional<EventTerminalDescriptor> terminal;

  [[nodiscard]] bool has_value() const noexcept {
    return ordinary != nullptr || terminal.has_value();
  }
  [[nodiscard]] bool is_terminal() const noexcept {
    return terminal.has_value();
  }
};

enum class EventAcknowledgeResult : std::uint8_t {
  Acknowledged,
  Closed,
  Mismatch,
};

class EventSubscriberChannel final {
public:
  EventSubscriberChannel(std::string subscription_id,
                         std::string gateway_instance_id,
                         common_wire::Stream stream,
                         std::uint64_t source_generation,
                         std::size_t ordinary_capacity);

  EventSubscriberChannel(const EventSubscriberChannel &) = delete;
  EventSubscriberChannel &operator=(const EventSubscriberChannel &) = delete;

  [[nodiscard]] bool stage_accepted(
      std::shared_ptr<const EventPublicationRecord> accepted) noexcept;
  [[nodiscard]] EventAdmissionResult
  admit_event(const std::shared_ptr<const g4::NormalizedMarketEvent> &event,
              EventPublicationTime published_at) noexcept;
  [[nodiscard]] bool
  terminalize_replacement(EventPublicationTime published_at) noexcept;
  [[nodiscard]] bool close_unavailable() noexcept;

  [[nodiscard]] PeekedEventPublication peek() const;
  [[nodiscard]] EventAcknowledgeResult
  acknowledge(const PeekedEventPublication &publication) noexcept;
  void wait_for_change(std::chrono::milliseconds duration) const;
  void close_from_owner() noexcept;
  void close_from_writer() noexcept;

  [[nodiscard]] EventChannelState state() const noexcept;
  [[nodiscard]] std::size_t ordinary_size() const noexcept;
  [[nodiscard]] bool terminal_reserved() const noexcept;
  [[nodiscard]] std::uint64_t next_session_sequence() const noexcept;
  [[nodiscard]] const std::string &subscription_id() const noexcept;
  [[nodiscard]] const std::string &gateway_instance_id() const noexcept;
  [[nodiscard]] common_wire::Stream stream() const noexcept;
  [[nodiscard]] std::uint64_t source_generation() const noexcept;

private:
  [[nodiscard]] bool ring_push_locked(
      std::shared_ptr<const EventPublicationRecord> record) noexcept;
  void ring_pop_locked() noexcept;
  void clear_from_writer_locked() noexcept;

  const std::string subscription_id_;
  const std::string gateway_instance_id_;
  const common_wire::Stream stream_;
  const std::uint64_t source_generation_;
  const std::size_t ordinary_capacity_;
  std::vector<std::shared_ptr<const EventPublicationRecord>> ordinary_ring_;

  mutable std::mutex mutex_;
  mutable std::condition_variable condition_;
  std::size_t head_{0U};
  std::size_t size_{0U};
  std::optional<EventTerminalDescriptor> terminal_;
  EventChannelState state_{EventChannelState::Active};
  std::uint64_t next_session_sequence_{1U};
};

enum class EventSourceState : std::uint8_t {
  Closed,
  Open,
  Quiesced,
  PermanentlyClosed,
  Shutdown,
};

enum class EventSubscriptionAdmissionError : std::uint8_t {
  SourceUnavailable,
  ActiveLimit,
  ClockError,
  IdExhausted,
  InternalError,
  ShuttingDown,
};

struct AcceptedEventSubscription final {
  std::shared_ptr<EventSubscriberChannel> channel;
};

using EventSubscriptionAdmission =
    std::variant<AcceptedEventSubscription, EventSubscriptionAdmissionError>;

enum class EventPublishResult : std::uint8_t {
  Published,
  IgnoredBeforeOpen,
  InvariantFailure,
};

struct EventPublicationObservation final {
  EventSourceState source_state{EventSourceState::Closed};
  std::optional<std::uint64_t> source_generation;
  std::size_t active_subscriptions{0U};
};

class EventPublication final {
public:
  EventPublication(std::string gateway_instance_id, g3::RuntimeClock clock,
                   EventPublicationLimits limits = {});

  EventPublication(const EventPublication &) = delete;
  EventPublication &operator=(const EventPublication &) = delete;

  [[nodiscard]] bool open_generation(std::uint64_t generation) noexcept;
  [[nodiscard]] bool quiesce_generation(std::uint64_t generation) noexcept;
  [[nodiscard]] bool
  close_generation_replaced(std::uint64_t generation) noexcept;
  [[nodiscard]] bool
  close_generation_permanently(std::uint64_t generation) noexcept;

  [[nodiscard]] EventSubscriptionAdmission
  admit(const ValidatedEventSubscription &subscription);
  [[nodiscard]] EventPublishResult
  publish(const std::shared_ptr<const g4::NormalizedMarketEvent> &event,
          std::uint64_t generation) noexcept;
  void remove(const std::shared_ptr<EventSubscriberChannel> &channel) noexcept;
  void shutdown() noexcept;

  [[nodiscard]] EventPublicationObservation observe() const noexcept;
  [[nodiscard]] const std::string &gateway_instance_id() const noexcept;

private:
  [[nodiscard]] std::optional<EventPublicationTime> sample_time() noexcept;
  [[nodiscard]] bool
  terminalize_replaced_locked(std::uint64_t generation,
                              EventPublicationTime published_at) noexcept;

  const std::string gateway_instance_id_;
  g3::RuntimeClock clock_;
  const EventPublicationLimits limits_;

  mutable std::mutex mutex_;
  EventSourceState source_state_{EventSourceState::Closed};
  std::optional<std::uint64_t> source_generation_;
  std::uint64_t next_subscription_id_{1U};
  std::vector<std::shared_ptr<EventSubscriberChannel>> channels_;
};

[[nodiscard]] common_wire::Stream
normalized_event_stream(const g4::NormalizedMarketEvent &event) noexcept;

} // namespace binance_market_data::gateway::g9
