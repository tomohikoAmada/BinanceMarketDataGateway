#include "event_publication.hpp"

#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g9 = binance_market_data::gateway::g9;
namespace market = binance_market_data::market::v1;
namespace wire = binance_market_data::gateway::v1;

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

[[nodiscard]] g3::RuntimeClock advancing_clock() {
  auto next = std::make_shared<std::uint64_t>(1000U);
  return [next] {
    const auto value = (*next)++;
    return g3::ClockSample{value, value + 10000U};
  };
}

[[nodiscard]] std::shared_ptr<const g4::NormalizedSpotEvent>
make_event(common::Stream stream, std::uint64_t id) {
  if (stream == common::STREAM_DIFF_DEPTH) {
    market::DepthUpdate update;
    update.mutable_metadata()->set_stream(stream);
    update.mutable_metadata()->set_schema_version(g9::kDepthEventSchema);
    update.set_first_update_id(id);
    update.set_final_update_id(id);
    return std::make_shared<const g4::NormalizedSpotEvent>(std::move(update));
  }
  if (stream == common::STREAM_AGG_TRADE) {
    market::AggTrade trade;
    trade.mutable_metadata()->set_stream(stream);
    trade.mutable_metadata()->set_schema_version(g9::kAggTradeEventSchema);
    trade.set_aggregate_trade_id(id);
    trade.set_price("100.00");
    trade.set_quantity("1.000");
    trade.set_first_trade_id(id);
    trade.set_last_trade_id(id);
    trade.set_trade_time_ms(id);
    return std::make_shared<const g4::NormalizedSpotEvent>(std::move(trade));
  }
  market::BookTicker ticker;
  ticker.mutable_metadata()->set_stream(stream);
  ticker.mutable_metadata()->set_schema_version(g9::kBookTickerEventSchema);
  ticker.set_update_id(id);
  ticker.set_best_bid_price("100.00");
  ticker.set_best_bid_quantity("1.000");
  ticker.set_best_ask_price("101.00");
  ticker.set_best_ask_quantity("2.000");
  return std::make_shared<const g4::NormalizedSpotEvent>(std::move(ticker));
}

[[nodiscard]] g9::ValidatedEventSubscription
subscription(common::Stream stream, std::string request_id = "request-1") {
  const char *schema = nullptr;
  switch (stream) {
  case common::STREAM_DIFF_DEPTH:
    schema = g9::kDepthEventSchema;
    break;
  case common::STREAM_AGG_TRADE:
    schema = g9::kAggTradeEventSchema;
    break;
  case common::STREAM_BOOK_TICKER:
    schema = g9::kBookTickerEventSchema;
    break;
  default:
    throw TestFailure{"unsupported test stream"};
  }
  return {std::move(request_id), stream, schema};
}

[[nodiscard]] std::shared_ptr<g9::EventSubscriberChannel>
admit(g9::EventPublication &publication, common::Stream stream,
      std::string request_id = "request-1") {
  auto result = publication.admit(subscription(stream, std::move(request_id)));
  REQUIRE(std::holds_alternative<g9::AcceptedEventSubscription>(result));
  return std::get<g9::AcceptedEventSubscription>(std::move(result)).channel;
}

[[nodiscard]] g9::PeekedEventPublication
peek_sequence(const std::shared_ptr<g9::EventSubscriberChannel> &channel,
              std::uint64_t sequence) {
  auto record = channel->peek();
  REQUIRE(record.has_value());
  const auto actual = record.ordinary != nullptr
                          ? record.ordinary->session_sequence
                          : record.terminal->session_sequence;
  REQUIRE_EQ(actual, sequence);
  return record;
}

void acknowledge(const std::shared_ptr<g9::EventSubscriberChannel> &channel,
                 const g9::PeekedEventPublication &record,
                 g9::EventAcknowledgeResult expected =
                     g9::EventAcknowledgeResult::Acknowledged) {
  REQUIRE_EQ(channel->acknowledge(record), expected);
}

void exact_source_subscription_cut() {
  g9::EventPublication publication{"gw-g9", advancing_clock()};
  REQUIRE(publication.open_generation(1U));
  const auto event_a = make_event(common::STREAM_DIFF_DEPTH, 1U);
  REQUIRE_EQ(publication.publish(event_a, 1U),
             g9::EventPublishResult::Published);
  auto channel = admit(publication, common::STREAM_DIFF_DEPTH);
  const auto event_b = make_event(common::STREAM_DIFF_DEPTH, 2U);
  REQUIRE_EQ(publication.publish(event_b, 1U),
             g9::EventPublishResult::Published);

  auto record = peek_sequence(channel, 1U);
  REQUIRE(std::holds_alternative<wire::SubscriptionAccepted>(
      record.ordinary->payload));
  REQUIRE(!record.ordinary->connection_generation.has_value());
  acknowledge(channel, record);
  record = peek_sequence(channel, 2U);
  REQUIRE_EQ(std::get<std::shared_ptr<const g4::NormalizedSpotEvent>>(
                 record.ordinary->payload),
             event_b);
  REQUIRE_EQ(record.ordinary->connection_generation,
             std::optional<std::uint64_t>{1U});
  acknowledge(channel, record);
  REQUIRE(!channel->peek().has_value());
}

void normal_flow_each_stream() {
  for (const auto stream : {common::STREAM_DIFF_DEPTH, common::STREAM_AGG_TRADE,
                            common::STREAM_BOOK_TICKER}) {
    g9::EventPublication publication{"gw-g9", advancing_clock()};
    REQUIRE(publication.open_generation(7U));
    auto channel = admit(publication, stream);
    REQUIRE_EQ(publication.publish(make_event(stream, 1U), 7U),
               g9::EventPublishResult::Published);
    REQUIRE_EQ(publication.publish(make_event(stream, 2U), 7U),
               g9::EventPublishResult::Published);

    auto record = peek_sequence(channel, 1U);
    const auto &accepted =
        std::get<wire::SubscriptionAccepted>(record.ordinary->payload);
    REQUIRE_EQ(accepted.negotiated_payload_schema_versions_size(), 1);
    REQUIRE_EQ(accepted.negotiated_payload_schema_versions(0),
               subscription(stream).negotiated_payload_schema_version);
    acknowledge(channel, record);
    for (std::uint64_t sequence = 2U; sequence <= 3U; ++sequence) {
      record = peek_sequence(channel, sequence);
      REQUIRE_EQ(record.ordinary->connection_generation,
                 std::optional<std::uint64_t>{7U});
      const auto &event =
          std::get<std::shared_ptr<const g4::NormalizedSpotEvent>>(
              record.ordinary->payload);
      REQUIRE_EQ(g9::normalized_event_stream(*event), stream);
      acknowledge(channel, record);
    }
  }
}

void slow_consumer_isolated_and_failed_event_has_no_sequence() {
  g9::EventPublication publication{"gw-g9", advancing_clock(),
                                   g9::EventPublicationLimits{8U, 2U}};
  REQUIRE(publication.open_generation(1U));
  auto slow = admit(publication, common::STREAM_AGG_TRADE, "slow");
  auto draining = admit(publication, common::STREAM_AGG_TRADE, "draining");

  auto draining_record = peek_sequence(draining, 1U);
  acknowledge(draining, draining_record);
  REQUIRE_EQ(publication.publish(make_event(common::STREAM_AGG_TRADE, 1U), 1U),
             g9::EventPublishResult::Published);
  draining_record = peek_sequence(draining, 2U);
  acknowledge(draining, draining_record);
  REQUIRE_EQ(publication.publish(make_event(common::STREAM_AGG_TRADE, 2U), 1U),
             g9::EventPublishResult::Published);

  auto record = peek_sequence(slow, 1U);
  acknowledge(slow, record);
  record = peek_sequence(slow, 2U);
  acknowledge(slow, record);
  record = peek_sequence(slow, 3U);
  REQUIRE(record.is_terminal());
  REQUIRE_EQ(record.terminal->reason,
             common::CONSUMER_GAP_REASON_SLOW_CONSUMER);
  REQUIRE_EQ(record.terminal->recovery_action,
             common::RECOVERY_ACTION_RESUBSCRIBE);
  REQUIRE_EQ(record.terminal->connection_generation, 1U);
  acknowledge(slow, record, g9::EventAcknowledgeResult::Closed);

  draining_record = peek_sequence(draining, 3U);
  acknowledge(draining, draining_record);
  REQUIRE_EQ(publication.publish(make_event(common::STREAM_AGG_TRADE, 3U), 1U),
             g9::EventPublishResult::Published);
  draining_record = peek_sequence(draining, 4U);
  acknowledge(draining, draining_record);
}

void quiesce_replacement_and_fresh_generation() {
  g9::EventPublication publication{"gw-g9", advancing_clock()};
  REQUIRE(publication.open_generation(1U));
  auto old_channel = admit(publication, common::STREAM_BOOK_TICKER, "old");
  REQUIRE_EQ(
      publication.publish(make_event(common::STREAM_BOOK_TICKER, 1U), 1U),
      g9::EventPublishResult::Published);
  REQUIRE(publication.quiesce_generation(1U));
  const auto rejected = publication.admit(
      subscription(common::STREAM_BOOK_TICKER, "quiesce-race"));
  REQUIRE_EQ(std::get<g9::EventSubscriptionAdmissionError>(rejected),
             g9::EventSubscriptionAdmissionError::SourceUnavailable);
  REQUIRE(publication.close_generation_replaced(1U));
  REQUIRE(publication.open_generation(2U));
  auto fresh = admit(publication, common::STREAM_BOOK_TICKER, "fresh");
  REQUIRE_EQ(
      publication.publish(make_event(common::STREAM_BOOK_TICKER, 2U), 2U),
      g9::EventPublishResult::Published);

  auto record = peek_sequence(old_channel, 1U);
  acknowledge(old_channel, record);
  record = peek_sequence(old_channel, 2U);
  REQUIRE_EQ(record.ordinary->connection_generation,
             std::optional<std::uint64_t>{1U});
  acknowledge(old_channel, record);
  record = peek_sequence(old_channel, 3U);
  REQUIRE(record.is_terminal());
  REQUIRE_EQ(record.terminal->reason,
             common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED);
  REQUIRE_EQ(record.terminal->recovery_action,
             common::RECOVERY_ACTION_RESUBSCRIBE);

  record = peek_sequence(fresh, 1U);
  REQUIRE(!record.ordinary->connection_generation.has_value());
  acknowledge(fresh, record);
  record = peek_sequence(fresh, 2U);
  REQUIRE_EQ(record.ordinary->connection_generation,
             std::optional<std::uint64_t>{2U});
}

void permanent_close_drains_without_fake_gap() {
  g9::EventPublication publication{"gw-g9", advancing_clock()};
  REQUIRE(publication.open_generation(4U));
  auto channel = admit(publication, common::STREAM_AGG_TRADE);
  REQUIRE_EQ(publication.publish(make_event(common::STREAM_AGG_TRADE, 1U), 4U),
             g9::EventPublishResult::Published);
  REQUIRE(publication.quiesce_generation(4U));
  REQUIRE(publication.close_generation_permanently(4U));

  auto record = peek_sequence(channel, 1U);
  acknowledge(channel, record);
  record = peek_sequence(channel, 2U);
  acknowledge(channel, record);
  REQUIRE(!channel->peek().has_value());
  REQUIRE_EQ(channel->state(), g9::EventChannelState::TerminalUnavailable);
  REQUIRE(!channel->terminal_reserved());
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"EXACT_SOURCE_SUBSCRIPTION_CUT", exact_source_subscription_cut},
      {"NORMAL_FLOW_EACH_STREAM", normal_flow_each_stream},
      {"SLOW_CONSUMER_ISOLATED_AND_FAILED_EVENT_HAS_NO_SEQUENCE",
       slow_consumer_isolated_and_failed_event_has_no_sequence},
      {"QUIESCE_REPLACEMENT_AND_FRESH_GENERATION",
       quiesce_replacement_and_fresh_generation},
      {"PERMANENT_CLOSE_DRAINS_WITHOUT_FAKE_GAP",
       permanent_close_drains_without_fake_gap},
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
