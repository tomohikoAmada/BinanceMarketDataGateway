#include "event_publication.hpp"
#include "grpc_service.hpp"
#include "market_runtime.hpp"
#include "performance_baseline.hpp"
#include "production_test_support.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace market = binance_market_data::market::v1;
namespace performance = binance_market_data::gateway::performance;
namespace production = binance_market_data::gateway::production;
namespace support = production::test_support;
namespace wire = binance_market_data::gateway::v1;

static_assert(std::is_const_v<std::remove_reference_t<
                  decltype(*std::declval<g7::PeekedPublication>().ordinary)>>);
static_assert(
    std::is_const_v<std::remove_reference_t<
        decltype(*std::declval<g9::PeekedEventPublication>().ordinary)>>);

class TestFailure final : public std::exception {
public:
  explicit TestFailure(std::string message) : message_{std::move(message)} {}
  [[nodiscard]] const char *what() const noexcept override {
    return message_.c_str();
  }

private:
  std::string message_;
};

void require(bool condition, std::string_view expression) {
  if (!condition) {
    throw TestFailure{std::string{expression}};
  }
}

template <typename Actual, typename Expected>
void require_equal(const Actual &actual, const Expected &expected,
                   std::string_view expression) {
  if (!(actual == expected)) {
    throw TestFailure{std::string{expression}};
  }
}

#define REQUIRE(value) require((value), #value)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

class StepClock final {
public:
  [[nodiscard]] std::uint64_t sample() noexcept {
    return next_.fetch_add(10U, std::memory_order_relaxed);
  }

  [[nodiscard]] g3::RuntimeClock runtime_clock() {
    return [this] {
      const auto monotonic = sample();
      return g3::ClockSample{1'700'000'000'000'000'000ULL + monotonic,
                             monotonic};
    };
  }

  [[nodiscard]] performance::MonotonicClock monotonic_clock() {
    return [this] { return sample(); };
  }

private:
  std::atomic<std::uint64_t> next_{1'000U};
};

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid numeric spec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] market::ExchangeDepthSnapshot make_snapshot() {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("performance-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("snapshot-1");
  snapshot.set_last_update_id(100U);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("1.000");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("1.000");
  return snapshot;
}

[[nodiscard]] market::DepthUpdate make_update(std::uint64_t first,
                                              std::uint64_t final) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("performance-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("performance-source");
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(g7::kUpdateSchema);
  update.set_first_update_id(first);
  update.set_final_update_id(final);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("2.000");
  return update;
}

[[nodiscard]] std::shared_ptr<const g4::NormalizedMarketEvent>
make_event(std::uint64_t id) {
  return std::make_shared<const g4::NormalizedMarketEvent>(make_update(id, id));
}

[[nodiscard]] g3::RuntimeLimits
runtime_limits(const std::shared_ptr<performance::ProductTraceBuffer> &baseline,
               std::size_t ingress_capacity = 16U) {
  g3::RuntimeLimits limits{ingress_capacity, 8U};
  limits.performance_baseline = baseline;
  return limits;
}

void bootstrap(g3::MarketRuntime &runtime) {
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot(), g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U),
                                         g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Live);
}

[[nodiscard]] wire::OrderBookSubscriptionRequest book_request() {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id("performance-book");
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(common::MARKET_SPOT);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] wire::EventSubscriptionRequest event_request() {
  wire::EventSubscriptionRequest request;
  request.set_request_id("performance-event");
  request.set_schema_version(g9::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(common::MARKET_SPOT);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(common::STREAM_DIFF_DEPTH);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(g9::kDepthEventSchema);
  return request;
}

[[nodiscard]] std::unique_ptr<wire::BinanceMarketDataGatewayService::Stub>
make_stub(int port) {
  return wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
}

template <typename Predicate>
void wait_until(std::condition_variable &condition, std::mutex &mutex,
                Predicate predicate) {
  std::unique_lock lock{mutex};
  REQUIRE(condition.wait_for(lock, std::chrono::seconds{5}, predicate));
}

void assert_public_wire_has_no_trace_field(
    const google::protobuf::Message &message) {
  REQUIRE(message.GetDescriptor()->FindFieldByName("trace_id") == nullptr);
  REQUIRE(message.GetDescriptor()->FindFieldByName("performance_trace_id") ==
          nullptr);
}

void correlated_branches_and_successful_t5() {
  StepClock step;
  auto baseline = std::make_shared<performance::ProductTraceBuffer>(
      performance::Product::Spot, step.monotonic_clock(),
      performance::PerformanceBaselineLimits{32U, 64U});
  auto clock = step.runtime_clock();
  g3::MarketRuntime runtime{runtime_limits(baseline), clock, numeric_spec()};
  bootstrap(runtime);
  g9::EventPublication events{"gw-performance", clock, {}, baseline};
  REQUIRE(events.open_generation(1U));

  std::mutex mutex;
  std::condition_variable condition;
  std::vector<wire::OrderBookStreamItem> book_items;
  std::vector<wire::GatewayEventEnvelope> event_items;
  g7::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.write_override = [&](auto &, const auto &item) {
    {
      std::lock_guard lock{mutex};
      book_items.push_back(item);
    }
    condition.notify_all();
    return true;
  };
  options.event_write_override = [&](auto &, const auto &item) {
    {
      std::lock_guard lock{mutex};
      event_items.push_back(item);
    }
    condition.notify_all();
    return true;
  };
  g7::OrderBookGrpcServer server{runtime, events, "gw-performance",
                                 std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext book_context;
  grpc::ClientContext event_context;
  auto book_reader = stub->SubscribeOrderBook(&book_context, book_request());
  auto event_reader = stub->SubscribeEvents(&event_context, event_request());
  wait_until(condition, mutex, [&] {
    return book_items.size() >= 2U && event_items.size() >= 1U;
  });

  runtime.pause_owner_for_testing();
  const auto trace = baseline->begin_trace(100U, 200U);
  REQUIRE(trace.valid());
  const auto event = make_event(102U);
  REQUIRE_EQ(events.publish(event, 1U, trace),
             g9::EventPublishResult::Published);
  REQUIRE_EQ(runtime.submit_depth_update(std::get<market::DepthUpdate>(*event),
                                         g3::SourceProvenance{1U}, trace),
             g3::AdmissionResult::Accepted);
  runtime.release_owner_for_testing();
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{102U});
  wait_until(condition, mutex, [&] {
    return book_items.size() >= 3U && event_items.size() >= 2U;
  });

  book_context.TryCancel();
  event_context.TryCancel();
  static_cast<void>(book_reader->Finish());
  static_cast<void>(event_reader->Finish());
  server.shutdown();
  runtime.stop();

  REQUIRE_EQ(baseline->trace_count(), 1U);
  const auto &row = baseline->trace(0U);
  REQUIRE_EQ(row.trace_id, trace.trace_id);
  REQUIRE_EQ(row.t0_ns, 100U);
  REQUIRE_EQ(row.t1_ns, 200U);
  REQUIRE(row.q_ns > row.t1_ns);
  REQUIRE(row.t2_ns > row.q_ns);
  REQUIRE(row.t3_ns > row.t2_ns);
  REQUIRE_EQ(row.t3_disposition, performance::T3Disposition::Applied);

  REQUIRE_EQ(baseline->delivery_count(performance::DeliveryKind::OrderBook),
             1U);
  REQUIRE_EQ(baseline->delivery_count(performance::DeliveryKind::Event), 1U);
  const auto &book =
      baseline->deliveries(performance::DeliveryKind::OrderBook).front();
  const auto &event_delivery =
      baseline->deliveries(performance::DeliveryKind::Event).front();
  REQUIRE_EQ(book.trace_id, trace.trace_id);
  REQUIRE_EQ(event_delivery.trace_id, trace.trace_id);
  REQUIRE(event_delivery.t4_ns < row.q_ns);
  REQUIRE(book.t4_ns > row.t3_ns);
  REQUIRE(book.t5_observed && book.t5_ns > book.t4_ns);
  REQUIRE(event_delivery.t5_observed &&
          event_delivery.t5_ns > event_delivery.t4_ns);

  REQUIRE(book_items.back().has_depth_update());
  REQUIRE(event_items.back().has_depth_update());
  assert_public_wire_has_no_trace_field(book_items.back());
  assert_public_wire_has_no_trace_field(book_items.back().delivery_metadata());
  assert_public_wire_has_no_trace_field(event_items.back());
  assert_public_wire_has_no_trace_field(event_items.back().delivery_metadata());

  const auto queue = baseline->queue_evidence();
  REQUIRE(queue.ingress_max_occupancy >= 1U);
  REQUIRE(queue.order_book_max_occupancy >= 1U);
  REQUIRE(queue.event_max_occupancy >= 1U);
}

void non_applied_and_absent_subscribers_have_no_fake_delivery() {
  StepClock step;
  auto baseline = std::make_shared<performance::ProductTraceBuffer>(
      performance::Product::Spot, step.monotonic_clock(),
      performance::PerformanceBaselineLimits{16U, 16U});
  auto clock = step.runtime_clock();
  g3::MarketRuntime runtime{runtime_limits(baseline), clock, numeric_spec()};
  bootstrap(runtime);
  auto admission = runtime.admit_order_book_subscription(
      {"non-applied", "gw-performance", std::nullopt});
  REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(admission));
  const auto channel =
      std::get<g3::AcceptedSubscription>(std::move(admission)).channel;
  for (int index = 0; index < 2; ++index) {
    const auto publication = channel->peek();
    REQUIRE(publication.has_value());
    REQUIRE(channel->acknowledge(publication) !=
            g7::AcknowledgeResult::Mismatch);
  }

  const auto stale = baseline->begin_trace(10U, 20U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(99U, 99U),
                                         g3::SourceProvenance{1U}, stale),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_apply->disposition,
             core::ApplyDisposition::IgnoredStale);
  const auto duplicate = baseline->begin_trace(30U, 40U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, 101U),
                                         g3::SourceProvenance{1U}, duplicate),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_apply->disposition,
             core::ApplyDisposition::IgnoredDuplicate);
  const auto gap = baseline->begin_trace(50U, 60U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U),
                                         g3::SourceProvenance{1U}, gap),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_apply->disposition,
             core::ApplyDisposition::GapDetected);
  runtime.stop();

  REQUIRE_EQ(baseline->delivery_count(performance::DeliveryKind::OrderBook),
             0U);
  REQUIRE_EQ(baseline->trace(0U).t3_disposition,
             performance::T3Disposition::IgnoredStale);
  REQUIRE_EQ(baseline->trace(1U).t3_disposition,
             performance::T3Disposition::IgnoredDuplicate);
  REQUIRE_EQ(baseline->trace(2U).t3_disposition,
             performance::T3Disposition::GapDetected);

  StepClock absent_step;
  auto absent = std::make_shared<performance::ProductTraceBuffer>(
      performance::Product::Spot, absent_step.monotonic_clock(),
      performance::PerformanceBaselineLimits{4U, 4U});
  auto absent_clock = absent_step.runtime_clock();
  g3::MarketRuntime no_subscribers{runtime_limits(absent), absent_clock,
                                   numeric_spec()};
  bootstrap(no_subscribers);
  g9::EventPublication no_event_subscribers{
      "gw-performance", absent_clock, {}, absent};
  REQUIRE(no_event_subscribers.open_generation(1U));
  const auto trace = absent->begin_trace(70U, 80U);
  const auto event = make_event(102U);
  REQUIRE_EQ(no_event_subscribers.publish(event, 1U, trace),
             g9::EventPublishResult::Published);
  REQUIRE_EQ(
      no_subscribers.submit_depth_update(std::get<market::DepthUpdate>(*event),
                                         g3::SourceProvenance{1U}, trace),
      g3::AdmissionResult::Accepted);
  REQUIRE_EQ(no_subscribers.observe().last_update_id,
             std::optional<std::uint64_t>{102U});
  no_event_subscribers.shutdown();
  no_subscribers.stop();
  REQUIRE_EQ(absent->delivery_count(performance::DeliveryKind::OrderBook), 0U);
  REQUIRE_EQ(absent->delivery_count(performance::DeliveryKind::Event), 0U);
}

void failed_write_has_t4_without_t5() {
  StepClock step;
  auto baseline = std::make_shared<performance::ProductTraceBuffer>(
      performance::Product::Spot, step.monotonic_clock(),
      performance::PerformanceBaselineLimits{8U, 8U});
  auto clock = step.runtime_clock();
  g3::MarketRuntime runtime{runtime_limits(baseline), clock, numeric_spec()};
  bootstrap(runtime);
  g9::EventPublication events{"gw-performance", clock, {}, baseline};
  REQUIRE(events.open_generation(1U));

  std::mutex mutex;
  std::condition_variable condition;
  bool failed_depth_write = false;
  g7::GrpcServiceOptions options;
  options.idle_cancellation_check_interval = std::chrono::milliseconds{1};
  options.write_override = [&](auto &, const auto &item) {
    if (item.has_depth_update()) {
      {
        std::lock_guard lock{mutex};
        failed_depth_write = true;
      }
      condition.notify_all();
      return false;
    }
    return true;
  };
  g7::OrderBookGrpcServer server{runtime, events, "gw-performance",
                                 std::move(options)};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = make_stub(server.selected_port());
  grpc::ClientContext context;
  auto reader = stub->SubscribeOrderBook(&context, book_request());
  while (runtime.observe().resident_subscription_count == 0U) {
    std::this_thread::yield();
  }
  const auto trace = baseline->begin_trace(100U, 110U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, 102U),
                                         g3::SourceProvenance{1U}, trace),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  wait_until(condition, mutex, [&] { return failed_depth_write; });
  static_cast<void>(reader->Finish());
  server.shutdown();
  runtime.stop();

  REQUIRE_EQ(baseline->delivery_count(performance::DeliveryKind::OrderBook),
             1U);
  const auto &delivery =
      baseline->deliveries(performance::DeliveryKind::OrderBook).front();
  REQUIRE_EQ(delivery.trace_id, trace.trace_id);
  REQUIRE(delivery.t4_ns != 0U);
  REQUIRE(!delivery.t5_observed);
  REQUIRE_EQ(delivery.t5_ns, 0U);
}

void storage_exhaustion_and_queue_evidence_are_measurement_only() {
  StepClock step;
  auto baseline = std::make_shared<performance::ProductTraceBuffer>(
      performance::Product::Spot, step.monotonic_clock(),
      performance::PerformanceBaselineLimits{1U, 4U});
  auto clock = step.runtime_clock();
  g3::MarketRuntime runtime{runtime_limits(baseline), clock, numeric_spec()};
  bootstrap(runtime);
  const auto recorded = baseline->begin_trace(10U, 20U);
  const auto overflowed = baseline->begin_trace(30U, 40U);
  REQUIRE(recorded.valid());
  REQUIRE(!overflowed.valid());
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, 102U),
                                         g3::SourceProvenance{1U}, recorded),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, 103U),
                                         g3::SourceProvenance{1U}, overflowed),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{103U});
  runtime.stop();
  REQUIRE_EQ(baseline->trace_count(), 1U);
  REQUIRE_EQ(baseline->trace_storage_overflow(), 1U);
  REQUIRE(!baseline->evidence_valid());

  auto queues = std::make_shared<performance::ProductTraceBuffer>(
      performance::Product::Spot, step.monotonic_clock(),
      performance::PerformanceBaselineLimits{4U, 8U});
  g7::SubscriberChannel book{"ob-9", "gw-performance", 1U, 9U, queues.get()};
  const auto book_trace = queues->begin_trace(50U, 60U);
  const auto update =
      std::make_shared<const market::DepthUpdate>(make_update(104U, 104U));
  REQUIRE_EQ(book.admit_update(update, 1U, {}, book_trace),
             g7::OrdinaryAdmissionResult::Admitted);
  const auto book_full_trace = queues->begin_trace(70U, 80U);
  REQUIRE_EQ(book.admit_update(update, 1U, {}, book_full_trace),
             g7::OrdinaryAdmissionResult::Terminalized);

  g9::EventSubscriberChannel event{
      "ev-7", "gw-performance", common::STREAM_DIFF_DEPTH, 1U, 2U,
      7U,     queues.get()};
  wire::SubscriptionAccepted accepted;
  auto accepted_record = std::make_shared<const g9::EventPublicationRecord>(
      g9::EventPublicationRecord{1U, std::nullopt, {}, std::move(accepted)});
  REQUIRE(event.stage_accepted(std::move(accepted_record)));
  const auto event_trace = queues->begin_trace(90U, 100U);
  REQUIRE_EQ(event.admit_event(make_event(105U), {}, event_trace),
             g9::EventAdmissionResult::Admitted);
  const auto event_full_trace = queues->begin_trace(110U, 120U);
  REQUIRE_EQ(event.admit_event(make_event(106U), {}, event_full_trace),
             g9::EventAdmissionResult::Terminalized);

  const auto evidence = queues->queue_evidence();
  REQUIRE_EQ(evidence.order_book_max_occupancy, 1U);
  REQUIRE_EQ(evidence.order_book_full_terminalization_count, 1U);
  REQUIRE_EQ(evidence.event_max_occupancy, 2U);
  REQUIRE_EQ(evidence.event_full_terminalization_count, 1U);

  std::ostringstream artifact;
  baseline->write_json_lines(artifact);
  const auto text = artifact.str();
  REQUIRE(text.find("bmd-gateway-performance-baseline.v1") !=
          std::string::npos);
  REQUIRE(text.find("\"trace_storage_overflow\":1") != std::string::npos);
  REQUIRE(text.find("\"evidence_valid\":false") != std::string::npos);
  REQUIRE(text.find("\"product\":\"BINANCE/SPOT/BTCUSDT\"") !=
          std::string::npos);
}

void production_shutdown_precedes_bounded_export() {
  auto options = support::gateway_options();
  production::ProductionGateway gateway{
      support::numeric_spec(), support::numeric_spec(),
      support::fixed_clock(),  "gw-performance-production",
      "127.0.0.1:0",           std::move(options.gateway)};
  REQUIRE_EQ(gateway.start(), production::StartResult::Serving);
  gateway.stop();
  const auto final = gateway.observe();
  REQUIRE_EQ(final.state, production::GatewayState::Stopped);
  REQUIRE_EQ(final.tracked_contexts, 0U);
  REQUIRE(final.spot_runtime.owner_joined);
  REQUIRE(final.usdm_runtime.owner_joined);
  REQUIRE_EQ(final.spot_recovery.active_transport_count, 0U);
  REQUIRE_EQ(final.usdm_recovery.active_transport_count, 0U);

  std::ostringstream artifact;
  REQUIRE(gateway.write_performance_baseline(artifact));
  const auto text = artifact.str();
  REQUIRE(text.find("BINANCE/SPOT/BTCUSDT") != std::string::npos);
  REQUIRE(text.find("BINANCE/USD_M_PERPETUAL/BTCUSDT") != std::string::npos);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, void (*)()>> tests{
      {"CORRELATED_BRANCHES_AND_SUCCESSFUL_T5",
       correlated_branches_and_successful_t5},
      {"NON_APPLIED_AND_ABSENT_SUBSCRIBERS_HAVE_NO_FAKE_DELIVERY",
       non_applied_and_absent_subscribers_have_no_fake_delivery},
      {"FAILED_WRITE_HAS_T4_WITHOUT_T5", failed_write_has_t4_without_t5},
      {"STORAGE_EXHAUSTION_AND_QUEUE_EVIDENCE_ARE_MEASUREMENT_ONLY",
       storage_exhaustion_and_queue_evidence_are_measurement_only},
      {"PRODUCTION_SHUTDOWN_PRECEDES_BOUNDED_EXPORT",
       production_shutdown_precedes_bounded_export},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &failure) {
      std::cerr << name << "=FAIL " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
