#pragma once

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
#include "performance_baseline.hpp"
#endif

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_messages.pb.h>
#include <binance_market_data/market/v1/market_events.pb.h>
#include <binance_market_data/projection/v1/snapshots.pb.h>

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

namespace binance_market_data::gateway::g7 {

namespace common_wire = ::binance_market_data::common::v1;
namespace gateway_wire = ::binance_market_data::gateway::v1;
namespace market_wire = ::binance_market_data::market::v1;
namespace projection_wire = ::binance_market_data::projection::v1;

inline constexpr std::size_t kMaximumActiveSubscriptions = 8U;
inline constexpr std::size_t kOrdinaryQueueCapacity = 64U;
inline constexpr std::size_t kTerminalControlCapacity = 1U;
inline constexpr std::size_t kPendingAdmissionCapacity = 8U;
inline constexpr auto kIdleClientCancellationCheckInterval =
    std::chrono::milliseconds{250};

inline constexpr char kProtocolVersion[] = "gateway-stream.v1";
inline constexpr char kOrderBookRequestSchema[] =
    "order-book-subscription-request.v1";
inline constexpr char kSubscriptionAcceptedSchema[] =
    "subscription-accepted.v1";
inline constexpr char kSnapshotSchema[] = "local-order-book-snapshot.v1";
inline constexpr char kUpdateSchema[] = "depth-update.v1";
inline constexpr char kConsumerGapSchema[] = "consumer-gap-notice.v1";

struct PublicationLimits final {
  std::size_t maximum_active_subscriptions{kMaximumActiveSubscriptions};
  std::size_t ordinary_queue_capacity{kOrdinaryQueueCapacity};
  std::size_t pending_admission_capacity{kPendingAdmissionCapacity};
};

struct PublicationTime final {
  std::uint64_t utc_ns{0U};
  std::optional<std::uint64_t> monotonic_ns;

  friend constexpr bool operator==(PublicationTime,
                                   PublicationTime) noexcept = default;
};

struct ValidatedOrderBookSubscription final {
  std::string request_id;
  std::string gateway_instance_id;
  std::optional<std::int32_t> depth_limit;
};

enum class PublicationPayloadKind : std::uint8_t {
  SubscriptionAccepted,
  Snapshot,
  DepthUpdate,
};

using OrdinaryPayload =
    std::variant<gateway_wire::SubscriptionAccepted,
                 projection_wire::LocalOrderBookSnapshot,
                 std::shared_ptr<const market_wire::DepthUpdate>>;

struct PublicationRecord final {
  std::uint64_t session_sequence{0U};
  std::optional<std::uint64_t> connection_generation;
  PublicationTime published_at;
  OrdinaryPayload payload;

  [[nodiscard]] PublicationPayloadKind kind() const noexcept;
};

struct TerminalDescriptor final {
  std::uint64_t session_sequence{0U};
  std::optional<std::uint64_t> connection_generation;
  PublicationTime published_at;
  common_wire::ConsumerGapReason reason{
      common_wire::CONSUMER_GAP_REASON_UNSPECIFIED};
  common_wire::RecoveryAction recovery_action{
      common_wire::RECOVERY_ACTION_UNSPECIFIED};

  friend constexpr bool
  operator==(const TerminalDescriptor &,
             const TerminalDescriptor &) noexcept = default;
};

enum class SubscriberState : std::uint8_t {
  Active,
  TerminalGap,
  Closed,
};

enum class OrdinaryAdmissionResult : std::uint8_t {
  Admitted,
  Terminalized,
  Inactive,
  AllocationFailure,
};

struct PeekedPublication final {
  std::shared_ptr<const PublicationRecord> ordinary;
  std::optional<TerminalDescriptor> terminal;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  performance::DeliveryToken delivery;
#endif

  [[nodiscard]] bool has_value() const noexcept {
    return ordinary != nullptr || terminal.has_value();
  }
  [[nodiscard]] bool is_terminal() const noexcept {
    return terminal.has_value();
  }
};

enum class AcknowledgeResult : std::uint8_t {
  Acknowledged,
  Closed,
  Mismatch,
};

// One concrete fixed-capacity subscriber egress channel. The MarketRuntime
// owner is the only producer. Exactly one RPC handler is the consumer/writer.
class SubscriberChannel final {
public:
  SubscriberChannel(
      std::string subscription_id, std::string gateway_instance_id,
      std::size_t ordinary_capacity
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      ,
      std::uint64_t subscriber_ordinal = 0U,
      performance::ProductTraceBuffer *performance_baseline = nullptr
#endif
  );

  SubscriberChannel(const SubscriberChannel &) = delete;
  SubscriberChannel &operator=(const SubscriberChannel &) = delete;
  SubscriberChannel(SubscriberChannel &&) = delete;
  SubscriberChannel &operator=(SubscriberChannel &&) = delete;

  // This is used only while the channel is private to owner admission. Both
  // mandatory records commit together or neither becomes visible.
  [[nodiscard]] bool
  stage_initial(std::shared_ptr<const PublicationRecord> accepted,
                std::shared_ptr<const PublicationRecord> snapshot) noexcept;

  [[nodiscard]] OrdinaryAdmissionResult
  admit_update(const std::shared_ptr<const market_wire::DepthUpdate> &update,
               std::optional<std::uint64_t> connection_generation,
               PublicationTime published_at
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
               ,
               performance::TraceToken trace = {}
#endif
               ) noexcept;

  // First terminal reason wins. The terminal descriptor has its own reserved
  // slot and never consumes ordinary ring capacity.
  [[nodiscard]] bool
  terminalize(common_wire::ConsumerGapReason reason,
              common_wire::RecoveryAction recovery_action,
              std::optional<std::uint64_t> connection_generation,
              PublicationTime published_at) noexcept;

  [[nodiscard]] PeekedPublication peek() const;
  [[nodiscard]] AcknowledgeResult
  acknowledge(const PeekedPublication &publication) noexcept;
  void wait_for_change(std::chrono::milliseconds duration) const;

  // Owner closure preserves the ring/front while a writer may have it in
  // flight. The writer is the only domain that clears queued records.
  void close_from_owner() noexcept;
  void close_from_writer() noexcept;

  [[nodiscard]] SubscriberState state() const noexcept;
  [[nodiscard]] std::size_t ordinary_size() const noexcept;
  [[nodiscard]] std::size_t ordinary_capacity() const noexcept;
  [[nodiscard]] bool terminal_reserved() const noexcept;
  [[nodiscard]] std::uint64_t next_session_sequence() const noexcept;
  [[nodiscard]] const std::string &subscription_id() const noexcept;
  [[nodiscard]] const std::string &gateway_instance_id() const noexcept;

private:
  [[nodiscard]] bool
  ring_push_locked(std::shared_ptr<const PublicationRecord> record) noexcept;
  void ring_pop_locked() noexcept;
  void clear_from_writer_locked() noexcept;

  const std::string subscription_id_;
  const std::string gateway_instance_id_;
  const std::size_t ordinary_capacity_;
  std::vector<std::shared_ptr<const PublicationRecord>> ordinary_ring_;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  const std::uint64_t subscriber_ordinal_;
  performance::ProductTraceBuffer *const performance_baseline_;
  std::vector<performance::DeliveryToken> delivery_ring_;
#endif

  mutable std::mutex mutex_;
  mutable std::condition_variable condition_;
  std::size_t head_{0U};
  std::size_t size_{0U};
  std::optional<TerminalDescriptor> terminal_;
  SubscriberState state_{SubscriberState::Active};
  std::uint64_t next_session_sequence_{1U};
};

[[nodiscard]] std::shared_ptr<const PublicationRecord> make_accepted_record(
    const std::string &request_id, const std::string &subscription_id,
    const std::string &gateway_instance_id, PublicationTime published_at);

[[nodiscard]] std::shared_ptr<const PublicationRecord>
make_snapshot_record(projection_wire::LocalOrderBookSnapshot snapshot,
                     std::optional<std::uint64_t> connection_generation,
                     PublicationTime published_at);

} // namespace binance_market_data::gateway::g7
