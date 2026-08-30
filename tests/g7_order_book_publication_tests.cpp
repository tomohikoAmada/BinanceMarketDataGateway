#include "market_runtime.hpp"
#include "order_book_publication.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g7 = binance_market_data::gateway::g7;
namespace market = binance_market_data::market::v1;

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

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid numeric spec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock fixed_clock() {
  return
      [] { return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL}; };
}

[[nodiscard]] market::ExchangeDepthSnapshot
make_snapshot(std::uint64_t last_update_id = 100U) {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("g7-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("snapshot-1");
  snapshot.set_last_update_id(last_update_id);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("1.000");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("2.000");
  return snapshot;
}

[[nodiscard]] market::DepthUpdate make_update(std::uint64_t update_id,
                                              std::string quantity) {
  market::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common::VENUE_BINANCE);
  metadata->set_market(common::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("g7-test");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("test-source");
  metadata->set_stream(common::STREAM_DIFF_DEPTH);
  metadata->set_schema_version(g7::kUpdateSchema);
  update.set_first_update_id(update_id);
  update.set_final_update_id(update_id);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity(std::move(quantity));
  return update;
}

void bootstrap(g3::MarketRuntime &runtime, std::uint64_t generation) {
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot(),
                                     g3::SourceProvenance{generation}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, "2.000"),
                                         g3::SourceProvenance{generation}),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.current_projection_generation,
             std::optional<std::uint64_t>{generation});
}

[[nodiscard]] g7::ValidatedOrderBookSubscription
request(std::string request_id = "request-1") {
  return {std::move(request_id), "gw-test-fixed", std::nullopt};
}

[[nodiscard]] g3::AcceptedSubscription
accept(g3::MarketRuntime &runtime, std::string request_id = "request-1") {
  auto result =
      runtime.admit_order_book_subscription(request(std::move(request_id)));
  REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(result));
  return std::get<g3::AcceptedSubscription>(std::move(result));
}

[[nodiscard]] g7::PeekedPublication
peek_required(const std::shared_ptr<g7::SubscriberChannel> &channel) {
  auto publication = channel->peek();
  REQUIRE(publication.has_value());
  return publication;
}

void ack_required(const std::shared_ptr<g7::SubscriberChannel> &channel,
                  const g7::PeekedPublication &publication) {
  REQUIRE(channel->acknowledge(publication) != g7::AcknowledgeResult::Mismatch);
}

[[nodiscard]] std::vector<std::uint64_t>
drain_to_close(const std::shared_ptr<g7::SubscriberChannel> &channel) {
  std::vector<std::uint64_t> sequences;
  for (;;) {
    auto publication = channel->peek();
    if (!publication.has_value()) {
      break;
    }
    const auto sequence = publication.ordinary != nullptr
                              ? publication.ordinary->session_sequence
                              : publication.terminal->session_sequence;
    sequences.push_back(sequence);
    ack_required(channel, publication);
    if (publication.is_terminal()) {
      break;
    }
  }
  return sequences;
}

void initial_cut_and_provenance() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 7U);
  const auto accepted = accept(runtime);
  REQUIRE_EQ(accepted.admitted_on_thread, runtime.observe().owner_thread_id);

  auto item = peek_required(accepted.channel);
  REQUIRE_EQ(item.ordinary->session_sequence, 1U);
  REQUIRE_EQ(item.ordinary->kind(),
             g7::PublicationPayloadKind::SubscriptionAccepted);
  REQUIRE(!item.ordinary->connection_generation.has_value());
  ack_required(accepted.channel, item);

  item = peek_required(accepted.channel);
  REQUIRE_EQ(item.ordinary->session_sequence, 2U);
  REQUIRE_EQ(item.ordinary->kind(), g7::PublicationPayloadKind::Snapshot);
  REQUIRE_EQ(item.ordinary->connection_generation,
             std::optional<std::uint64_t>{7U});
  const auto &snapshot = std::get<g7::projection_wire::LocalOrderBookSnapshot>(
      item.ordinary->payload);
  REQUIRE_EQ(snapshot.last_update_id(), 101U);
  ack_required(accepted.channel, item);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{7U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{102U});
  item = peek_required(accepted.channel);
  REQUIRE_EQ(item.ordinary->session_sequence, 3U);
  REQUIRE_EQ(item.ordinary->kind(), g7::PublicationPayloadKind::DepthUpdate);
  REQUIRE_EQ(item.ordinary->connection_generation,
             std::optional<std::uint64_t>{7U});
  REQUIRE_EQ(std::get<std::shared_ptr<const market::DepthUpdate>>(
                 item.ordinary->payload)
                 ->final_update_id(),
             102U);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.last_publication_thread_id,
             observation.owner_thread_id);
  REQUIRE(observation.last_publication_thread_id != std::this_thread::get_id());
}

void buffered_generation_mismatch_faults_before_apply() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, "2.000"),
                                         g3::SourceProvenance{11U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(
      runtime.submit_snapshot(make_snapshot(), g3::SourceProvenance{10U}),
      g3::AdmissionResult::Accepted);

  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason,
             std::optional<g3::FaultReason>{g3::FaultReason::InternalError});
  REQUIRE_EQ(observation.current_projection_generation,
             std::optional<std::uint64_t>{10U});
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{100U});
  REQUIRE(!observation.last_apply.has_value());
  REQUIRE_EQ(observation.resident_subscription_count, 0U);
}

void live_generation_mismatch_faults_before_apply_and_publication() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 20U);
  const auto accepted = accept(runtime);
  for (int index = 0; index < 2; ++index) {
    ack_required(accepted.channel, peek_required(accepted.channel));
  }

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{21U}),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason,
             std::optional<g3::FaultReason>{g3::FaultReason::InternalError});
  REQUIRE_EQ(observation.current_projection_generation,
             std::optional<std::uint64_t>{20U});
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{101U});

  const auto terminal = peek_required(accepted.channel);
  REQUIRE(terminal.is_terminal());
  REQUIRE(terminal.ordinary == nullptr);
  REQUIRE_EQ(terminal.terminal->connection_generation,
             std::optional<std::uint64_t>{20U});
  REQUIRE_EQ(terminal.terminal->reason,
             common::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE);
}

void same_generation_buffered_and_live_updates_remain_normal() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, "2.000"),
                                         g3::SourceProvenance{30U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(
      runtime.submit_snapshot(make_snapshot(), g3::SourceProvenance{30U}),
      g3::AdmissionResult::Accepted);
  auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.current_projection_generation,
             std::optional<std::uint64_t>{30U});
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{101U});

  const auto accepted = accept(runtime);
  for (int index = 0; index < 2; ++index) {
    ack_required(accepted.channel, peek_required(accepted.channel));
  }
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{30U}),
             g3::AdmissionResult::Accepted);
  observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{102U});
  const auto publication = peek_required(accepted.channel);
  REQUIRE_EQ(publication.ordinary->kind(),
             g7::PublicationPayloadKind::DepthUpdate);
  REQUIRE_EQ(publication.ordinary->connection_generation,
             std::optional<std::uint64_t>{30U});
}

void absent_provenance_synthetic_flow_remains_supported() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, "2.000")),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot()),
             g3::AdmissionResult::Accepted);
  auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE(!observation.current_projection_generation.has_value());
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{101U});

  const auto accepted = accept(runtime);
  auto publication = peek_required(accepted.channel);
  ack_required(accepted.channel, publication);
  publication = peek_required(accepted.channel);
  REQUIRE(!publication.ordinary->connection_generation.has_value());
  ack_required(accepted.channel, publication);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000")),
             g3::AdmissionResult::Accepted);
  observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Live);
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{102U});
  publication = peek_required(accepted.channel);
  REQUIRE_EQ(publication.ordinary->kind(),
             g7::PublicationPayloadKind::DepthUpdate);
  REQUIRE(!publication.ordinary->connection_generation.has_value());
}

void mandatory_initial_capacity() {
  g3::MarketRuntime runtime{{8U, 8U, g7::PublicationLimits{8U, 1U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime, 1U);
  const auto result = runtime.admit_order_book_subscription(request());
  REQUIRE(std::holds_alternative<g3::SubscriptionAdmissionError>(result));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(result),
             g3::SubscriptionAdmissionError::ActiveLimit);
  REQUIRE_EQ(runtime.observe().resident_subscription_count, 0U);
}

void owner_target_ticket_subscription_cut() {
  std::mutex mutex;
  std::condition_variable condition;
  bool admission_enqueued = false;
  g3::RuntimeTestOptions test_options{false, [&] {
                                        std::lock_guard lock{mutex};
                                        admission_enqueued = true;
                                        condition.notify_all();
                                      }};
  g3::MarketRuntime runtime{
      {16U, 8U}, fixed_clock(), numeric_spec(), std::move(test_options)};
  bootstrap(runtime, 21U);
  runtime.pause_owner_for_testing();
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{21U}),
             g3::AdmissionResult::Accepted);
  auto pending = std::async(std::launch::async, [&] {
    return runtime.admit_order_book_subscription(request("cut"));
  });
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return admission_enqueued; });
  }
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, "4.000"),
                                         g3::SourceProvenance{21U}),
             g3::AdmissionResult::Accepted);
  runtime.release_owner_for_testing();

  auto result = pending.get();
  REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(result));
  const auto accepted = std::get<g3::AcceptedSubscription>(std::move(result));
  auto publication = peek_required(accepted.channel);
  REQUIRE_EQ(publication.ordinary->session_sequence, 1U);
  ack_required(accepted.channel, publication);
  publication = peek_required(accepted.channel);
  REQUIRE_EQ(publication.ordinary->session_sequence, 2U);
  REQUIRE_EQ(std::get<g7::projection_wire::LocalOrderBookSnapshot>(
                 publication.ordinary->payload)
                 .last_update_id(),
             102U);
  ack_required(accepted.channel, publication);
  static_cast<void>(runtime.observe());
  publication = peek_required(accepted.channel);
  REQUIRE_EQ(publication.ordinary->session_sequence, 3U);
  REQUIRE_EQ(std::get<std::shared_ptr<const market::DepthUpdate>>(
                 publication.ordinary->payload)
                 ->final_update_id(),
             103U);
}

void overflow_sequence_terminal_and_peek_ack_bound() {
  g3::MarketRuntime runtime{{8U, 8U, g7::PublicationLimits{8U, 3U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime, 4U);
  const auto accepted = accept(runtime);
  const auto in_flight = peek_required(accepted.channel);
  REQUIRE_EQ(accepted.channel->ordinary_size(), 2U);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{4U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE_EQ(accepted.channel->ordinary_size(), 3U);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, "4.000"),
                                         g3::SourceProvenance{4U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE_EQ(accepted.channel->ordinary_size(), 3U);
  REQUIRE_EQ(accepted.channel->state(), g7::SubscriberState::TerminalGap);
  REQUIRE(accepted.channel->terminal_reserved());
  REQUIRE_EQ(accepted.channel->next_session_sequence(), 5U);
  REQUIRE_EQ(in_flight.ordinary->session_sequence, 1U);

  REQUIRE_EQ(runtime.submit_depth_update(make_update(104U, "5.000"),
                                         g3::SourceProvenance{4U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE_EQ(accepted.channel->ordinary_size(), 3U);
  REQUIRE_EQ(accepted.channel->next_session_sequence(), 5U);

  const auto sequences = drain_to_close(accepted.channel);
  REQUIRE_EQ(sequences, (std::vector<std::uint64_t>{1U, 2U, 3U, 4U}));
  REQUIRE_EQ(accepted.channel->state(), g7::SubscriberState::Closed);
}

void overflow_descriptor_semantics_and_first_terminal_wins() {
  g3::MarketRuntime runtime{{8U, 8U, g7::PublicationLimits{8U, 2U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime, 3U);
  const auto accepted = accept(runtime);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{3U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE_EQ(accepted.channel->state(), g7::SubscriberState::TerminalGap);
  while (accepted.channel->ordinary_size() != 0U) {
    const auto publication = peek_required(accepted.channel);
    ack_required(accepted.channel, publication);
  }
  const auto terminal = peek_required(accepted.channel);
  REQUIRE(terminal.is_terminal());
  REQUIRE_EQ(terminal.terminal->session_sequence, 3U);
  REQUIRE_EQ(terminal.terminal->reason,
             common::CONSUMER_GAP_REASON_SLOW_CONSUMER);
  REQUIRE_EQ(terminal.terminal->recovery_action,
             common::RECOVERY_ACTION_RESUBSCRIBE);
  REQUIRE(!terminal.terminal->connection_generation.has_value());

  REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
             g3::PlannedRebootstrapResetResult::Reset);
  const auto unchanged = peek_required(accepted.channel);
  REQUIRE_EQ(unchanged.terminal->reason,
             common::CONSUMER_GAP_REASON_SLOW_CONSUMER);
}

void stale_and_duplicate_not_published() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 1U);
  const auto accepted = accept(runtime);
  for (int index = 0; index < 2; ++index) {
    const auto publication = peek_required(accepted.channel);
    ack_required(accepted.channel, publication);
  }
  REQUIRE_EQ(runtime.submit_depth_update(make_update(99U, "8.000"),
                                         g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  auto observation = runtime.observe();
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::IgnoredStale);
  REQUIRE(!accepted.channel->peek().has_value());

  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, "2.000"),
                                         g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  observation = runtime.observe();
  REQUIRE_EQ(observation.last_apply->disposition,
             core::ApplyDisposition::IgnoredDuplicate);
  REQUIRE(!accepted.channel->peek().has_value());
}

void active_bound_and_resident_terminal_count() {
  g3::MarketRuntime runtime{{8U, 8U, g7::PublicationLimits{2U, 2U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime, 1U);
  const auto first = accept(runtime, "request-1");
  const auto second = accept(runtime, "request-2");
  REQUIRE_EQ(first.channel->subscription_id(), "ob-1");
  REQUIRE_EQ(second.channel->subscription_id(), "ob-2");

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE_EQ(first.channel->state(), g7::SubscriberState::TerminalGap);
  auto rejected = runtime.admit_order_book_subscription(request("request-3"));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(rejected),
             g3::SubscriptionAdmissionError::ActiveLimit);

  first.channel->close_from_writer();
  runtime.notify_subscriber_closed();
  const auto third = accept(runtime, "request-3");
  REQUIRE_EQ(third.channel->subscription_id(), "ob-3");
}

void pending_admission_bound() {
  std::mutex mutex;
  std::condition_variable condition;
  std::size_t enqueued = 0U;
  g3::RuntimeTestOptions test_options{false, [&] {
                                        std::lock_guard lock{mutex};
                                        ++enqueued;
                                        condition.notify_all();
                                      }};
  g3::MarketRuntime runtime{{8U, 8U, g7::PublicationLimits{8U, 64U, 8U}},
                            fixed_clock(),
                            numeric_spec(),
                            std::move(test_options)};
  bootstrap(runtime, 1U);
  runtime.pause_owner_for_testing();

  std::vector<std::future<g3::SubscriptionAdmissionResult>> pending;
  for (std::size_t index = 0U; index < 8U; ++index) {
    pending.push_back(std::async(std::launch::async, [&, index] {
      return runtime.admit_order_book_subscription(
          request("pending-" + std::to_string(index + 1U)));
    }));
  }
  {
    std::unique_lock lock{mutex};
    condition.wait(lock, [&] { return enqueued == 8U; });
  }
  const auto rejected =
      runtime.admit_order_book_subscription(request("pending-9"));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(rejected),
             g3::SubscriptionAdmissionError::PendingLimit);
  runtime.release_owner_for_testing();
  for (auto &result : pending) {
    REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(result.get()));
  }
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.pending_admission_count, 0U);
  REQUIRE_EQ(observation.resident_subscription_count, 8U);
  const auto active_rejected =
      runtime.admit_order_book_subscription(request("active-9"));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(active_rejected),
             g3::SubscriptionAdmissionError::ActiveLimit);
}

void slow_consumers_are_isolated() {
  g3::MarketRuntime runtime{{16U, 8U, g7::PublicationLimits{3U, 3U, 8U}},
                            fixed_clock(),
                            numeric_spec()};
  bootstrap(runtime, 2U);
  const auto healthy = accept(runtime, "healthy");
  const auto slow_b = accept(runtime, "slow-b");
  const auto slow_c = accept(runtime, "slow-c");
  for (int index = 0; index < 2; ++index) {
    const auto publication = peek_required(healthy.channel);
    ack_required(healthy.channel, publication);
  }

  for (std::uint64_t update_id = 102U; update_id <= 103U; ++update_id) {
    REQUIRE_EQ(runtime.submit_depth_update(make_update(update_id, "3.000"),
                                           g3::SourceProvenance{2U}),
               g3::AdmissionResult::Accepted);
    static_cast<void>(runtime.observe());
    const auto publication = peek_required(healthy.channel);
    REQUIRE_EQ(publication.ordinary->session_sequence, update_id - 99U);
    ack_required(healthy.channel, publication);
  }
  REQUIRE_EQ(healthy.channel->state(), g7::SubscriberState::Active);
  REQUIRE_EQ(slow_b.channel->state(), g7::SubscriberState::TerminalGap);
  REQUIRE_EQ(slow_c.channel->state(), g7::SubscriberState::TerminalGap);
  REQUIRE_EQ(runtime.observe().last_update_id,
             std::optional<std::uint64_t>{103U});
}

void shared_update_payload_and_depth_limit() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 12U);
  auto limited_request = request("limited");
  limited_request.depth_limit = 1;
  auto limited_result =
      runtime.admit_order_book_subscription(std::move(limited_request));
  REQUIRE(std::holds_alternative<g3::AcceptedSubscription>(limited_result));
  const auto limited =
      std::get<g3::AcceptedSubscription>(std::move(limited_result));
  const auto other = accept(runtime, "other");

  ack_required(limited.channel, peek_required(limited.channel));
  auto limited_snapshot = peek_required(limited.channel);
  const auto &snapshot = std::get<g7::projection_wire::LocalOrderBookSnapshot>(
      limited_snapshot.ordinary->payload);
  REQUIRE(snapshot.has_depth_limit());
  REQUIRE_EQ(snapshot.depth_limit(), 1);
  REQUIRE(snapshot.bids_size() <= 1);
  REQUIRE(snapshot.asks_size() <= 1);
  ack_required(limited.channel, limited_snapshot);
  for (int index = 0; index < 2; ++index) {
    ack_required(other.channel, peek_required(other.channel));
  }

  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{12U}),
             g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  const auto first = peek_required(limited.channel);
  const auto second = peek_required(other.channel);
  const auto first_payload =
      std::get<std::shared_ptr<const market::DepthUpdate>>(
          first.ordinary->payload);
  const auto second_payload =
      std::get<std::shared_ptr<const market::DepthUpdate>>(
          second.ordinary->payload);
  REQUIRE(first_payload == second_payload);
}

void nonrecoverable_fault_terminates_session() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 13U);
  const auto accepted = accept(runtime);
  for (int index = 0; index < 2; ++index) {
    ack_required(accepted.channel, peek_required(accepted.channel));
  }
  auto malformed = make_update(102U, "3.000");
  malformed.mutable_metadata()->set_symbol("ETHUSDT");
  REQUIRE_EQ(runtime.submit_depth_update(std::move(malformed),
                                         g3::SourceProvenance{13U}),
             g3::AdmissionResult::Accepted);
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.state, g3::RuntimeState::Faulted);
  REQUIRE_EQ(observation.fault_reason,
             std::optional<g3::FaultReason>{g3::FaultReason::AdapterError});
  const auto terminal = peek_required(accepted.channel);
  REQUIRE(terminal.is_terminal());
  REQUIRE_EQ(terminal.terminal->reason,
             common::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE);
  REQUIRE_EQ(terminal.terminal->recovery_action,
             common::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT);
  REQUIRE_EQ(terminal.terminal->connection_generation,
             std::optional<std::uint64_t>{13U});
}

void failed_admission_does_not_consume_subscription_id() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 1U);
  auto invalid = request("invalid-depth");
  invalid.depth_limit = 0;
  const auto rejected =
      runtime.admit_order_book_subscription(std::move(invalid));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(rejected),
             g3::SubscriptionAdmissionError::InvalidDepthLimit);
  const auto accepted = accept(runtime, "valid-after-rejection");
  REQUIRE_EQ(accepted.channel->subscription_id(), "ob-1");
}

void disconnect_before_queued_fanout_is_owner_swept() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 1U);
  auto accepted = accept(runtime);
  for (int index = 0; index < 2; ++index) {
    ack_required(accepted.channel, peek_required(accepted.channel));
  }

  runtime.pause_owner_for_testing();
  REQUIRE_EQ(runtime.submit_depth_update(make_update(102U, "3.000"),
                                         g3::SourceProvenance{1U}),
             g3::AdmissionResult::Accepted);
  std::thread writer{[&] {
    accepted.channel->close_from_writer();
    runtime.notify_subscriber_closed();
  }};
  writer.join();
  runtime.release_owner_for_testing();
  const auto observation = runtime.observe();
  REQUIRE_EQ(observation.last_update_id, std::optional<std::uint64_t>{102U});
  REQUIRE_EQ(observation.resident_subscription_count, 0U);
  REQUIRE_EQ(accepted.channel->state(), g7::SubscriberState::Closed);
  REQUIRE(!accepted.channel->peek().has_value());
}

void continuity_terminals() {
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap(runtime, 9U);
    const auto accepted = accept(runtime);
    REQUIRE_EQ(runtime.submit_depth_update(make_update(103U, "4.000"),
                                           g3::SourceProvenance{9U}),
               g3::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::NeedsResync);
    while (accepted.channel->ordinary_size() != 0U) {
      ack_required(accepted.channel, peek_required(accepted.channel));
    }
    const auto terminal = peek_required(accepted.channel);
    REQUIRE_EQ(terminal.terminal->reason,
               common::CONSUMER_GAP_REASON_UPSTREAM_SEQUENCE_GAP);
    REQUIRE_EQ(terminal.terminal->recovery_action,
               common::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT);
    REQUIRE_EQ(terminal.terminal->connection_generation,
               std::optional<std::uint64_t>{9U});
  }
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap(runtime, 5U);
    const auto accepted = accept(runtime);
    REQUIRE_EQ(runtime.submit_transport_failure(),
               g3::AdmissionResult::Accepted);
    REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Faulted);
    while (accepted.channel->ordinary_size() != 0U) {
      ack_required(accepted.channel, peek_required(accepted.channel));
    }
    const auto terminal = peek_required(accepted.channel);
    REQUIRE_EQ(terminal.terminal->reason,
               common::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE);
    REQUIRE_EQ(terminal.terminal->recovery_action,
               common::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT);
  }
  {
    g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
    bootstrap(runtime, 6U);
    const auto accepted = accept(runtime);
    REQUIRE_EQ(runtime.reset_live_for_planned_rebootstrap(),
               g3::PlannedRebootstrapResetResult::Reset);
    while (accepted.channel->ordinary_size() != 0U) {
      ack_required(accepted.channel, peek_required(accepted.channel));
    }
    const auto terminal = peek_required(accepted.channel);
    REQUIRE_EQ(terminal.terminal->reason,
               common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED);
    REQUIRE_EQ(terminal.terminal->recovery_action,
               common::RECOVERY_ACTION_RESUBSCRIBE);
    REQUIRE_EQ(terminal.terminal->connection_generation,
               std::optional<std::uint64_t>{6U});
  }
}

void post_recovery_session_and_frozen_generation() {
  g3::MarketRuntime runtime{{16U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 1U);
  const auto old = accept(runtime, "old");
  REQUIRE_EQ(runtime.submit_transport_failure(), g3::AdmissionResult::Accepted);
  static_cast<void>(runtime.observe());
  REQUIRE_EQ(runtime.reset_for_rebootstrap(),
             g3::RebootstrapResetResult::Reset);
  REQUIRE_EQ(runtime.submit_snapshot(make_snapshot(), g3::SourceProvenance{2U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.submit_depth_update(make_update(101U, "2.000"),
                                         g3::SourceProvenance{2U}),
             g3::AdmissionResult::Accepted);
  REQUIRE_EQ(runtime.observe().state, g3::RuntimeState::Live);
  const auto fresh = accept(runtime, "fresh");
  REQUIRE_EQ(fresh.channel->subscription_id(), "ob-2");
  auto publication = peek_required(fresh.channel);
  REQUIRE_EQ(publication.ordinary->session_sequence, 1U);
  ack_required(fresh.channel, publication);
  publication = peek_required(fresh.channel);
  REQUIRE_EQ(publication.ordinary->connection_generation,
             std::optional<std::uint64_t>{2U});

  while (old.channel->ordinary_size() != 0U) {
    ack_required(old.channel, peek_required(old.channel));
  }
  const auto old_terminal = peek_required(old.channel);
  REQUIRE_EQ(old_terminal.terminal->connection_generation,
             std::optional<std::uint64_t>{1U});
}

void publication_shutdown_and_lifetime() {
  g3::MarketRuntime runtime{{8U, 8U}, fixed_clock(), numeric_spec()};
  bootstrap(runtime, 1U);
  auto accepted = accept(runtime);
  auto publication = peek_required(accepted.channel);
  std::weak_ptr<const g7::PublicationRecord> lifetime = publication.ordinary;
  runtime.close_publication_admission();
  REQUIRE_EQ(runtime.shutdown_publication(),
             g3::PublicationShutdownResult::ShutDown);
  REQUIRE_EQ(accepted.channel->state(), g7::SubscriberState::Closed);
  REQUIRE(publication.ordinary != nullptr);
  accepted.channel.reset();
  REQUIRE(!lifetime.expired());
  publication.ordinary.reset();
  REQUIRE(lifetime.expired());

  const auto rejected = runtime.admit_order_book_subscription(request("late"));
  REQUIRE_EQ(std::get<g3::SubscriptionAdmissionError>(rejected),
             g3::SubscriptionAdmissionError::ShuttingDown);
  REQUIRE(runtime.observe().publication_shutdown);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, void (*)()>> tests{
      {"initial_cut_and_provenance", initial_cut_and_provenance},
      {"buffered_generation_mismatch_faults_before_apply",
       buffered_generation_mismatch_faults_before_apply},
      {"live_generation_mismatch_faults_before_apply_and_publication",
       live_generation_mismatch_faults_before_apply_and_publication},
      {"same_generation_buffered_and_live_updates_remain_normal",
       same_generation_buffered_and_live_updates_remain_normal},
      {"absent_provenance_synthetic_flow_remains_supported",
       absent_provenance_synthetic_flow_remains_supported},
      {"mandatory_initial_capacity", mandatory_initial_capacity},
      {"owner_target_ticket_subscription_cut",
       owner_target_ticket_subscription_cut},
      {"overflow_sequence_terminal_and_peek_ack_bound",
       overflow_sequence_terminal_and_peek_ack_bound},
      {"overflow_descriptor_semantics_and_first_terminal_wins",
       overflow_descriptor_semantics_and_first_terminal_wins},
      {"stale_and_duplicate_not_published", stale_and_duplicate_not_published},
      {"active_bound_and_resident_terminal_count",
       active_bound_and_resident_terminal_count},
      {"pending_admission_bound", pending_admission_bound},
      {"slow_consumers_are_isolated", slow_consumers_are_isolated},
      {"shared_update_payload_and_depth_limit",
       shared_update_payload_and_depth_limit},
      {"nonrecoverable_fault_terminates_session",
       nonrecoverable_fault_terminates_session},
      {"failed_admission_does_not_consume_subscription_id",
       failed_admission_does_not_consume_subscription_id},
      {"disconnect_before_queued_fanout_is_owner_swept",
       disconnect_before_queued_fanout_is_owner_swept},
      {"continuity_terminals", continuity_terminals},
      {"post_recovery_session_and_frozen_generation",
       post_recovery_session_and_frozen_generation},
      {"publication_shutdown_and_lifetime", publication_shutdown_and_lifetime},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << "PASS " << name << '\n';
    } catch (const std::exception &failure) {
      std::cerr << "FAIL " << name << ": " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
