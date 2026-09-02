#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <vector>

namespace binance_market_data::gateway::performance {

inline constexpr std::size_t kDefaultTraceCapacity = 16'384U;
inline constexpr std::size_t kDefaultDeliveryCapacityPerKind = 32'768U;

enum class Product : std::uint8_t {
  Spot,
  UsdMPerpetual,
};

enum class T3Disposition : std::uint8_t {
  Unobserved,
  Applied,
  IgnoredStale,
  IgnoredDuplicate,
  GapDetected,
  RejectedWrongState,
  AdapterFailure,
  InternalFailure,
};

enum class DeliveryKind : std::uint8_t {
  OrderBook,
  Event,
};

struct PerformanceBaselineLimits final {
  std::size_t trace_capacity{kDefaultTraceCapacity};
  std::size_t delivery_capacity_per_kind{kDefaultDeliveryCapacityPerKind};
};

using MonotonicClock = std::function<std::uint64_t()>;

class ProductTraceBuffer;

struct TraceToken final {
  ProductTraceBuffer *owner{nullptr};
  std::uint32_t row{0U};
  std::uint64_t trace_id{0U};

  [[nodiscard]] bool valid() const noexcept {
    return owner != nullptr && trace_id != 0U;
  }
};

struct DeliveryToken final {
  ProductTraceBuffer *owner{nullptr};
  DeliveryKind kind{DeliveryKind::OrderBook};
  std::uint32_t row{0U};

  [[nodiscard]] bool valid() const noexcept { return owner != nullptr; }
};

struct TraceEvidence final {
  std::uint64_t trace_id{0U};
  std::uint64_t t0_ns{0U};
  std::uint64_t t1_ns{0U};
  std::uint64_t q_ns{0U};
  std::uint64_t t2_ns{0U};
  std::uint64_t t3_ns{0U};
  std::size_t ingress_occupancy_q{0U};
  T3Disposition t3_disposition{T3Disposition::Unobserved};
  std::uint8_t stage_mask{0U};
};

struct DeliveryEvidence final {
  std::uint64_t trace_id{0U};
  std::uint64_t subscriber_ordinal{0U};
  std::uint64_t session_sequence{0U};
  std::uint64_t t4_ns{0U};
  std::uint64_t t5_ns{0U};
  std::size_t queue_occupancy_t4{0U};
  bool t5_observed{false};
};

struct QueueEvidence final {
  std::size_t ingress_max_occupancy{0U};
  std::uint64_t ingress_full_count{0U};
  std::size_t order_book_max_occupancy{0U};
  std::uint64_t order_book_full_terminalization_count{0U};
  std::size_t event_max_occupancy{0U};
  std::uint64_t event_full_terminalization_count{0U};
};

// One instance is owned by exactly one product. Production has one network
// producer, one Projection owner, and separate single-writer subscriber
// delivery slots. Evidence is read/exported only after all of those domains
// have stopped.
class ProductTraceBuffer final {
public:
  ProductTraceBuffer(Product product, MonotonicClock clock,
                     PerformanceBaselineLimits limits = {});

  ProductTraceBuffer(const ProductTraceBuffer &) = delete;
  ProductTraceBuffer &operator=(const ProductTraceBuffer &) = delete;

  [[nodiscard]] std::uint64_t sample_monotonic() noexcept;
  [[nodiscard]] TraceToken begin_trace(std::uint64_t t0_ns,
                                       std::uint64_t t1_ns) noexcept;
  void record_q(TraceToken trace, std::size_t occupancy) noexcept;
  void record_t2(TraceToken trace) noexcept;
  void record_t3(TraceToken trace, T3Disposition disposition) noexcept;

  [[nodiscard]] DeliveryToken record_t4(TraceToken trace, DeliveryKind kind,
                                        std::uint64_t subscriber_ordinal,
                                        std::uint64_t session_sequence,
                                        std::size_t occupancy) noexcept;
  void record_t5(DeliveryToken delivery) noexcept;

  void record_ingress_full() noexcept;
  void record_delivery_full(DeliveryKind kind) noexcept;

  // These read-only seams are valid only after the product graph and all RPC
  // writers have stopped. They intentionally perform no synchronization.
  [[nodiscard]] Product product() const noexcept;
  [[nodiscard]] std::size_t trace_count() const noexcept;
  [[nodiscard]] std::size_t trace_capacity() const noexcept;
  [[nodiscard]] std::size_t delivery_capacity_per_kind() const noexcept;
  [[nodiscard]] std::uint64_t trace_storage_overflow() const noexcept;
  [[nodiscard]] std::uint64_t delivery_storage_overflow() const noexcept;
  [[nodiscard]] std::uint64_t clock_error_count() const noexcept;
  [[nodiscard]] const TraceEvidence &trace(std::size_t index) const;
  [[nodiscard]] const std::vector<DeliveryEvidence> &
  deliveries(DeliveryKind kind) const noexcept;
  [[nodiscard]] std::size_t delivery_count(DeliveryKind kind) const noexcept;
  [[nodiscard]] QueueEvidence queue_evidence() const noexcept;
  [[nodiscard]] bool evidence_valid() const noexcept;

  void write_json_lines(std::ostream &output) const;

private:
  [[nodiscard]] TraceEvidence *resolve(TraceToken trace) noexcept;
  [[nodiscard]] std::vector<DeliveryEvidence> &
  delivery_storage(DeliveryKind kind) noexcept;
  [[nodiscard]] std::size_t &delivery_size(DeliveryKind kind) noexcept;

  const Product product_;
  MonotonicClock clock_;
  const PerformanceBaselineLimits limits_;
  std::vector<TraceEvidence> traces_;
  std::vector<DeliveryEvidence> order_book_deliveries_;
  std::vector<DeliveryEvidence> event_deliveries_;
  std::size_t trace_count_{0U};
  std::size_t order_book_delivery_count_{0U};
  std::size_t event_delivery_count_{0U};
  std::uint64_t trace_storage_overflow_{0U};
  std::uint64_t order_book_delivery_overflow_{0U};
  std::uint64_t event_delivery_overflow_{0U};
  std::atomic<std::uint64_t> clock_error_count_{0U};
  QueueEvidence queue_evidence_;
};

[[nodiscard]] const char *to_string(Product product) noexcept;
[[nodiscard]] const char *to_string(T3Disposition disposition) noexcept;
[[nodiscard]] const char *to_string(DeliveryKind kind) noexcept;

} // namespace binance_market_data::gateway::performance
