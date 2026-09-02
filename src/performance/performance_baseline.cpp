#include "performance_baseline.hpp"

#include <algorithm>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <string_view>

namespace binance_market_data::gateway::performance {

namespace {

inline constexpr std::uint8_t kT0 = 1U << 0U;
inline constexpr std::uint8_t kT1 = 1U << 1U;
inline constexpr std::uint8_t kQ = 1U << 2U;
inline constexpr std::uint8_t kT2 = 1U << 3U;
inline constexpr std::uint8_t kT3 = 1U << 4U;

[[nodiscard]] const char *missing_trace_stage(const TraceEvidence &trace) {
  if ((trace.stage_mask & kT0) == 0U) {
    return "t0_not_observed";
  }
  if ((trace.stage_mask & kT1) == 0U) {
    return "transport_parse_not_completed";
  }
  if ((trace.stage_mask & kQ) == 0U) {
    return "runtime_enqueue_not_accepted";
  }
  if ((trace.stage_mask & kT2) == 0U) {
    return "owner_not_dequeued";
  }
  if ((trace.stage_mask & kT3) == 0U) {
    return "projection_not_completed_before_shutdown_or_reset";
  }
  return "none";
}

[[nodiscard]] const char *missing_book_delivery(const TraceEvidence &trace,
                                                std::size_t delivery_count,
                                                bool delivery_evidence_lost) {
  if (delivery_count != 0U) {
    return "none";
  }
  if ((trace.stage_mask & kT3) == 0U) {
    return "projection_not_completed";
  }
  if (trace.t3_disposition != T3Disposition::Applied) {
    return "projection_disposition_not_applied";
  }
  return delivery_evidence_lost
             ? "no_recorded_delivery_or_delivery_storage_overflow"
             : "no_matching_subscriber";
}

[[nodiscard]] const char *missing_event_delivery(std::size_t delivery_count,
                                                 bool delivery_evidence_lost) {
  if (delivery_count != 0U) {
    return "none";
  }
  return delivery_evidence_lost
             ? "no_recorded_delivery_or_delivery_storage_overflow"
             : "no_matching_subscriber";
}

void write_optional_time(std::ostream &output, std::string_view name,
                         std::uint64_t value, bool present) {
  output << ",\"" << name << "\":";
  if (present) {
    output << value;
  } else {
    output << "null";
  }
}

} // namespace

ProductTraceBuffer::ProductTraceBuffer(Product product, MonotonicClock clock,
                                       PerformanceBaselineLimits limits)
    : product_{product}, clock_{std::move(clock)}, limits_{limits},
      traces_(limits.trace_capacity),
      order_book_deliveries_(limits.delivery_capacity_per_kind),
      event_deliveries_(limits.delivery_capacity_per_kind) {
  if (!clock_ || limits_.trace_capacity == 0U ||
      limits_.delivery_capacity_per_kind == 0U ||
      limits_.trace_capacity > std::numeric_limits<std::uint32_t>::max() ||
      limits_.delivery_capacity_per_kind >
          std::numeric_limits<std::uint32_t>::max()) {
    throw std::invalid_argument{"invalid performance baseline limits"};
  }
}

std::uint64_t ProductTraceBuffer::sample_monotonic() noexcept {
  try {
    return clock_();
  } catch (...) {
    clock_error_count_.fetch_add(1U, std::memory_order_relaxed);
    return 0U;
  }
}

TraceToken ProductTraceBuffer::begin_trace(std::uint64_t t0_ns,
                                           std::uint64_t t1_ns) noexcept {
  if (trace_count_ == traces_.size()) {
    ++trace_storage_overflow_;
    return {};
  }
  const auto row = trace_count_++;
  const auto trace_id = static_cast<std::uint64_t>(row) + 1U;
  traces_[row] = TraceEvidence{trace_id,
                               t0_ns,
                               t1_ns,
                               0U,
                               0U,
                               0U,
                               0U,
                               T3Disposition::Unobserved,
                               static_cast<std::uint8_t>(kT0 | kT1)};
  return {this, static_cast<std::uint32_t>(row), trace_id};
}

void ProductTraceBuffer::record_q(TraceToken trace,
                                  std::size_t occupancy) noexcept {
  auto *evidence = resolve(trace);
  if (evidence == nullptr) {
    return;
  }
  evidence->q_ns = sample_monotonic();
  evidence->ingress_occupancy_q = occupancy;
  evidence->stage_mask = static_cast<std::uint8_t>(evidence->stage_mask | kQ);
  queue_evidence_.ingress_max_occupancy =
      std::max(queue_evidence_.ingress_max_occupancy, occupancy);
}

void ProductTraceBuffer::record_t2(TraceToken trace) noexcept {
  auto *evidence = resolve(trace);
  if (evidence == nullptr) {
    return;
  }
  evidence->t2_ns = sample_monotonic();
  evidence->stage_mask = static_cast<std::uint8_t>(evidence->stage_mask | kT2);
}

void ProductTraceBuffer::record_t3(TraceToken trace,
                                   T3Disposition disposition) noexcept {
  auto *evidence = resolve(trace);
  if (evidence == nullptr) {
    return;
  }
  evidence->t3_ns = sample_monotonic();
  evidence->t3_disposition = disposition;
  evidence->stage_mask = static_cast<std::uint8_t>(evidence->stage_mask | kT3);
}

DeliveryToken ProductTraceBuffer::record_t4(TraceToken trace, DeliveryKind kind,
                                            std::uint64_t subscriber_ordinal,
                                            std::uint64_t session_sequence,
                                            std::size_t occupancy) noexcept {
  if (resolve(trace) == nullptr) {
    return {};
  }
  auto &size = delivery_size(kind);
  auto &storage = delivery_storage(kind);
  if (size == storage.size()) {
    if (kind == DeliveryKind::OrderBook) {
      ++order_book_delivery_overflow_;
    } else {
      ++event_delivery_overflow_;
    }
    return {};
  }
  const auto row = size++;
  storage[row] = DeliveryEvidence{trace.trace_id,
                                  subscriber_ordinal,
                                  session_sequence,
                                  sample_monotonic(),
                                  0U,
                                  occupancy,
                                  false};
  if (kind == DeliveryKind::OrderBook) {
    queue_evidence_.order_book_max_occupancy =
        std::max(queue_evidence_.order_book_max_occupancy, occupancy);
  } else {
    queue_evidence_.event_max_occupancy =
        std::max(queue_evidence_.event_max_occupancy, occupancy);
  }
  return {this, kind, static_cast<std::uint32_t>(row)};
}

void ProductTraceBuffer::record_t5(DeliveryToken delivery) noexcept {
  if (!delivery.valid() || delivery.owner != this) {
    return;
  }
  auto &storage = delivery_storage(delivery.kind);
  if (delivery.row >= storage.size() || storage[delivery.row].trace_id == 0U) {
    return;
  }
  auto &evidence = storage[delivery.row];
  evidence.t5_ns = sample_monotonic();
  evidence.t5_observed = true;
}

void ProductTraceBuffer::record_ingress_full() noexcept {
  ++queue_evidence_.ingress_full_count;
}

void ProductTraceBuffer::record_delivery_full(DeliveryKind kind) noexcept {
  if (kind == DeliveryKind::OrderBook) {
    ++queue_evidence_.order_book_full_terminalization_count;
  } else {
    ++queue_evidence_.event_full_terminalization_count;
  }
}

Product ProductTraceBuffer::product() const noexcept { return product_; }

std::size_t ProductTraceBuffer::trace_count() const noexcept {
  return trace_count_;
}

std::size_t ProductTraceBuffer::trace_capacity() const noexcept {
  return traces_.size();
}

std::size_t ProductTraceBuffer::delivery_capacity_per_kind() const noexcept {
  return limits_.delivery_capacity_per_kind;
}

std::uint64_t ProductTraceBuffer::trace_storage_overflow() const noexcept {
  return trace_storage_overflow_;
}

std::uint64_t ProductTraceBuffer::delivery_storage_overflow() const noexcept {
  return order_book_delivery_overflow_ + event_delivery_overflow_;
}

std::uint64_t ProductTraceBuffer::clock_error_count() const noexcept {
  return clock_error_count_.load(std::memory_order_relaxed);
}

const TraceEvidence &ProductTraceBuffer::trace(std::size_t index) const {
  if (index >= trace_count_) {
    throw std::out_of_range{"performance trace row is unavailable"};
  }
  return traces_[index];
}

const std::vector<DeliveryEvidence> &
ProductTraceBuffer::deliveries(DeliveryKind kind) const noexcept {
  return kind == DeliveryKind::OrderBook ? order_book_deliveries_
                                         : event_deliveries_;
}

std::size_t
ProductTraceBuffer::delivery_count(DeliveryKind kind) const noexcept {
  return kind == DeliveryKind::OrderBook ? order_book_delivery_count_
                                         : event_delivery_count_;
}

QueueEvidence ProductTraceBuffer::queue_evidence() const noexcept {
  return queue_evidence_;
}

bool ProductTraceBuffer::evidence_valid() const noexcept {
  return trace_storage_overflow_ == 0U && delivery_storage_overflow() == 0U &&
         clock_error_count() == 0U;
}

void ProductTraceBuffer::write_json_lines(std::ostream &output) const {
  const auto queue = queue_evidence();
  output << "{\"record\":\"campaign\",\"schema\":\"bmd-gateway-"
            "performance-baseline.v1\",\"product\":\""
         << to_string(product_) << "\",\"trace_capacity\":" << trace_capacity()
         << ",\"delivery_capacity_per_kind\":" << delivery_capacity_per_kind()
         << ",\"trace_count\":" << trace_count()
         << ",\"trace_storage_overflow\":" << trace_storage_overflow()
         << ",\"delivery_storage_overflow\":" << delivery_storage_overflow()
         << ",\"clock_error_count\":" << clock_error_count()
         << ",\"evidence_valid\":" << (evidence_valid() ? "true" : "false")
         << ",\"ingress_max_occupancy\":" << queue.ingress_max_occupancy
         << ",\"ingress_full_count\":" << queue.ingress_full_count
         << ",\"order_book_max_occupancy\":" << queue.order_book_max_occupancy
         << ",\"order_book_full_terminalization_count\":"
         << queue.order_book_full_terminalization_count
         << ",\"event_max_occupancy\":" << queue.event_max_occupancy
         << ",\"event_full_terminalization_count\":"
         << queue.event_full_terminalization_count << "}\n";

  std::vector<std::size_t> book_counts(trace_count_, 0U);
  std::vector<std::size_t> event_counts(trace_count_, 0U);
  const auto count_deliveries = [&](DeliveryKind kind, auto &counts) {
    const auto &storage = deliveries(kind);
    for (std::size_t index = 0U; index < delivery_count(kind); ++index) {
      const auto trace_id = storage[index].trace_id;
      if (trace_id != 0U && trace_id <= counts.size()) {
        ++counts[static_cast<std::size_t>(trace_id - 1U)];
      }
    }
  };
  count_deliveries(DeliveryKind::OrderBook, book_counts);
  count_deliveries(DeliveryKind::Event, event_counts);

  for (std::size_t index = 0U; index < trace_count_; ++index) {
    const auto &evidence = traces_[index];
    output << "{\"record\":\"trace\",\"product\":\"" << to_string(product_)
           << "\",\"trace_id\":" << evidence.trace_id;
    write_optional_time(output, "t0_ns", evidence.t0_ns,
                        (evidence.stage_mask & kT0) != 0U);
    write_optional_time(output, "t1_ns", evidence.t1_ns,
                        (evidence.stage_mask & kT1) != 0U);
    write_optional_time(output, "q_ns", evidence.q_ns,
                        (evidence.stage_mask & kQ) != 0U);
    write_optional_time(output, "t2_ns", evidence.t2_ns,
                        (evidence.stage_mask & kT2) != 0U);
    write_optional_time(output, "t3_ns", evidence.t3_ns,
                        (evidence.stage_mask & kT3) != 0U);
    output << ",\"t3_disposition\":\"" << to_string(evidence.t3_disposition)
           << "\",\"ingress_occupancy_q\":" << evidence.ingress_occupancy_q
           << ",\"book_delivery_count\":" << book_counts[index]
           << ",\"event_delivery_count\":" << event_counts[index]
           << ",\"missing_book_delivery_reason\":\""
           << missing_book_delivery(evidence, book_counts[index],
                                    order_book_delivery_overflow_ != 0U)
           << "\",\"missing_event_delivery_reason\":\""
           << missing_event_delivery(event_counts[index],
                                     event_delivery_overflow_ != 0U)
           << "\",\"missing_stage_reason\":\"" << missing_trace_stage(evidence)
           << "\"}\n";
  }

  const auto write_deliveries = [&](DeliveryKind kind) {
    const auto &storage = deliveries(kind);
    for (std::size_t index = 0U; index < delivery_count(kind); ++index) {
      const auto &evidence = storage[index];
      output << "{\"record\":\"delivery\",\"product\":\"" << to_string(product_)
             << "\",\"kind\":\"" << to_string(kind)
             << "\",\"trace_id\":" << evidence.trace_id
             << ",\"subscriber_ordinal\":" << evidence.subscriber_ordinal
             << ",\"session_sequence\":" << evidence.session_sequence
             << ",\"t4_ns\":" << evidence.t4_ns;
      write_optional_time(output, "t5_ns", evidence.t5_ns,
                          evidence.t5_observed);
      output << ",\"queue_occupancy_t4\":" << evidence.queue_occupancy_t4
             << ",\"missing_stage_reason\":\""
             << (evidence.t5_observed ? "none" : "grpc_write_not_completed")
             << "\"}\n";
    }
  };
  write_deliveries(DeliveryKind::OrderBook);
  write_deliveries(DeliveryKind::Event);
}

TraceEvidence *ProductTraceBuffer::resolve(TraceToken trace) noexcept {
  if (!trace.valid() || trace.owner != this || trace.row >= traces_.size()) {
    return nullptr;
  }
  auto &evidence = traces_[trace.row];
  return evidence.trace_id == trace.trace_id ? &evidence : nullptr;
}

std::vector<DeliveryEvidence> &
ProductTraceBuffer::delivery_storage(DeliveryKind kind) noexcept {
  return kind == DeliveryKind::OrderBook ? order_book_deliveries_
                                         : event_deliveries_;
}

std::size_t &ProductTraceBuffer::delivery_size(DeliveryKind kind) noexcept {
  return kind == DeliveryKind::OrderBook ? order_book_delivery_count_
                                         : event_delivery_count_;
}

const char *to_string(Product product) noexcept {
  switch (product) {
  case Product::Spot:
    return "BINANCE/SPOT/BTCUSDT";
  case Product::UsdMPerpetual:
    return "BINANCE/USD_M_PERPETUAL/BTCUSDT";
  }
  return "UNKNOWN";
}

const char *to_string(T3Disposition disposition) noexcept {
  switch (disposition) {
  case T3Disposition::Unobserved:
    return "unobserved";
  case T3Disposition::Applied:
    return "applied";
  case T3Disposition::IgnoredStale:
    return "ignored_stale";
  case T3Disposition::IgnoredDuplicate:
    return "ignored_duplicate";
  case T3Disposition::GapDetected:
    return "gap_detected";
  case T3Disposition::RejectedWrongState:
    return "rejected_wrong_state";
  case T3Disposition::AdapterFailure:
    return "adapter_failure";
  case T3Disposition::InternalFailure:
    return "internal_failure";
  }
  return "unknown";
}

const char *to_string(DeliveryKind kind) noexcept {
  switch (kind) {
  case DeliveryKind::OrderBook:
    return "order_book";
  case DeliveryKind::Event:
    return "event";
  }
  return "unknown";
}

} // namespace binance_market_data::gateway::performance
