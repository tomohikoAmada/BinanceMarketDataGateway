#include "synthetic_spot_host.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/market/v1/market_events.pb.h>

#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace adapter = binance_market_data::projection_adapter::v1;
namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g2 = binance_market_data::gateway::g2;
namespace market_wire = binance_market_data::market::v1;

constexpr std::uint64_t kSnapshotExchangeTimeMs = 1700000000001ULL;
constexpr std::uint64_t kSnapshotReceiveTimeNs = 1700000000001000000ULL;
constexpr std::uint64_t kSnapshotMonotonicNs = 9000000000001ULL;
constexpr std::uint64_t kUpdateExchangeTimeMs = 1700000000002ULL;
constexpr std::uint64_t kUpdateReceiveTimeNs = 1700000000002000000ULL;
constexpr std::uint64_t kUpdateMonotonicNs = 9000000000002ULL;

class TestFailure final : public std::exception {
public:
  explicit TestFailure(std::string message) : message_(std::move(message)) {}

  [[nodiscard]] const char *what() const noexcept override {
    return message_.c_str();
  }

private:
  std::string message_;
};

void require(bool condition, std::string_view expression) {
  if (!condition) {
    throw TestFailure{std::string(expression)};
  }
}

template <typename Actual, typename Expected>
void require_equal(const Actual &actual, const Expected &expected,
                   std::string_view expression) {
  if (!(actual == expected)) {
    throw TestFailure{std::string(expression)};
  }
}

#define REQUIRE(condition) require((condition), #condition)
#define REQUIRE_EQ(actual, expected)                                           \
  require_equal((actual), (expected), #actual " == " #expected)

[[nodiscard]] market_wire::ExchangeDepthSnapshot
make_snapshot(std::uint64_t last_update_id = 100,
              const std::string &bid_quantity = "2.500",
              const std::string &ask_quantity = "3.000") {
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g2-synthetic");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g2-snapshot-request-1");
  snapshot.set_last_update_id(last_update_id);
  snapshot.set_exchange_transaction_time_ms(kSnapshotExchangeTimeMs);
  snapshot.set_receive_time_utc_ns(kSnapshotReceiveTimeNs);
  snapshot.set_receive_monotonic_ns(kSnapshotMonotonicNs);

  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity(bid_quantity);
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity(ask_quantity);
  return snapshot;
}

[[nodiscard]] market_wire::DepthUpdate
make_bid_update(std::uint64_t first_update_id, std::uint64_t final_update_id,
                const std::string &bid_quantity) {
  market_wire::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("gateway-g2-synthetic");
  metadata->set_producer_version("1.0.0");
  metadata->set_connection_id("g2-connection-1");
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  metadata->set_exchange_event_time_ms(kUpdateExchangeTimeMs);
  metadata->set_receive_time_utc_ns(kUpdateReceiveTimeNs);
  metadata->set_receive_monotonic_ns(kUpdateMonotonicNs);
  update.set_first_update_id(first_update_id);
  update.set_final_update_id(final_update_id);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity(bid_quantity);
  return update;
}

[[nodiscard]] market_wire::DepthUpdate make_post_live_update() {
  auto update = make_bid_update(102, 103, "5.000");
  auto *removed_ask = update.add_asks();
  removed_ask->set_price("102.00");
  removed_ask->set_quantity("0.000");
  auto *new_ask = update.add_asks();
  new_ask->set_price("103.00");
  new_ask->set_quantity("2.000");
  return update;
}

void expect_installed(g2::SyntheticSpotHost &host,
                      const market_wire::ExchangeDepthSnapshot &snapshot) {
  const auto result = host.install_snapshot(snapshot);
  REQUIRE(std::holds_alternative<core::InstallResult>(result));
  REQUIRE_EQ(std::get<core::InstallResult>(result).disposition,
             core::InstallDisposition::Installed);
  REQUIRE_EQ(host.projection().status(),
             core::ProjectionStatus::AwaitingBridge);
  REQUIRE_EQ(host.projection().last_update_id(),
             core::UpdateId{snapshot.last_update_id()});
}

[[nodiscard]] std::string run_successful_scenario() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  expect_installed(host, make_snapshot());
  const auto replay = host.replay_buffered_updates();
  REQUIRE_EQ(replay.size(), 1U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(replay.front()));
  if (!std::holds_alternative<core::ApplyResult>(replay.front())) {
    return {};
  }
  REQUIRE_EQ(std::get<core::ApplyResult>(replay.front()).disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::Synchronized);
  const auto live = host.apply_live_update(make_post_live_update());
  REQUIRE(std::holds_alternative<core::ApplyResult>(live));
  if (!std::holds_alternative<core::ApplyResult>(live)) {
    return {};
  }
  REQUIRE_EQ(std::get<core::ApplyResult>(live).disposition,
             core::ApplyDisposition::Applied);
  const auto snapshot = host.make_snapshot();
  REQUIRE(std::holds_alternative<core::LocalOrderBookSnapshot>(snapshot));
  if (!std::holds_alternative<core::LocalOrderBookSnapshot>(snapshot)) {
    return {};
  }
  return std::get<core::LocalOrderBookSnapshot>(snapshot).SerializeAsString();
}

void normal_bootstrap() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  REQUIRE_EQ(host.buffered_update_count(), 1U);
  expect_installed(host, make_snapshot());

  const auto outcomes = host.replay_buffered_updates();
  REQUIRE_EQ(outcomes.size(), 1U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes.front()));
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes.front()).disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes.front()).status_after,
             core::ProjectionStatus::Synchronized);
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes.front()).last_update_id_after,
             core::UpdateId{101});
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::Synchronized);
}

void stale_prefix() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(90, 99, "9.000"));
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  expect_installed(host, make_snapshot());

  const auto outcomes = host.replay_buffered_updates();
  REQUIRE_EQ(outcomes.size(), 2U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes[0]));
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes[1]));
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[0]).disposition,
             core::ApplyDisposition::IgnoredStale);
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[0]).status_after,
             core::ProjectionStatus::AwaitingBridge);
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[1]).disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::Synchronized);
}

void duplicate_prefix() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(90, 100, "8.000"));
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  expect_installed(host, make_snapshot());

  const auto outcomes = host.replay_buffered_updates();
  REQUIRE_EQ(outcomes.size(), 2U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes[0]));
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes[1]));
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[0]).disposition,
             core::ApplyDisposition::IgnoredDuplicate);
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[1]).disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::Synchronized);
}

void valid_bridge() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(101, 103, "4.500"));
  expect_installed(host, make_snapshot());

  const auto outcomes = host.replay_buffered_updates();
  REQUIRE_EQ(outcomes.size(), 1U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes.front()));
  const auto &result = std::get<core::ApplyResult>(outcomes.front());
  REQUIRE_EQ(result.disposition, core::ApplyDisposition::Applied);
  REQUIRE_EQ(result.status_after, core::ProjectionStatus::Synchronized);
  REQUIRE_EQ(result.last_update_id_after, core::UpdateId{103});
}

void no_bridge() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(90, 99, "9.000"));
  host.receive_pre_snapshot(make_bid_update(90, 100, "8.000"));
  expect_installed(host, make_snapshot());

  const auto outcomes = host.replay_buffered_updates();
  REQUIRE_EQ(outcomes.size(), 2U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes[0]));
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes[1]));
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[0]).disposition,
             core::ApplyDisposition::IgnoredStale);
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes[1]).disposition,
             core::ApplyDisposition::IgnoredDuplicate);
  REQUIRE_EQ(host.projection().status(),
             core::ProjectionStatus::AwaitingBridge);
  REQUIRE_EQ(host.projection().last_update_id(), core::UpdateId{100});
}

void gap() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  expect_installed(host, make_snapshot());
  const auto replay = host.replay_buffered_updates();
  REQUIRE_EQ(replay.size(), 1U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(replay.front()));
  REQUIRE_EQ(std::get<core::ApplyResult>(replay.front()).disposition,
             core::ApplyDisposition::Applied);

  const auto gap = host.apply_live_update(make_bid_update(103, 103, "6.000"));
  REQUIRE(std::holds_alternative<core::ApplyResult>(gap));
  const auto &result = std::get<core::ApplyResult>(gap);
  REQUIRE_EQ(result.disposition, core::ApplyDisposition::GapDetected);
  REQUIRE_EQ(result.status_after, core::ProjectionStatus::NeedsResync);
  REQUIRE(result.gap.has_value());
  REQUIRE_EQ(result.gap->reason, core::GapReason::SpotLiveForwardGap);
  REQUIRE_EQ(result.gap->last_accepted_final, core::UpdateId{101});
  REQUIRE_EQ(result.gap->incoming_range.first(), core::UpdateId{103});
  REQUIRE_EQ(result.gap->incoming_range.final(), core::UpdateId{103});
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::NeedsResync);
}

void post_live_update() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  expect_installed(host, make_snapshot());
  const auto replay = host.replay_buffered_updates();
  REQUIRE_EQ(replay.size(), 1U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(replay.front()));
  REQUIRE_EQ(std::get<core::ApplyResult>(replay.front()).disposition,
             core::ApplyDisposition::Applied);

  const auto live = host.apply_live_update(make_post_live_update());
  REQUIRE(std::holds_alternative<core::ApplyResult>(live));
  REQUIRE_EQ(std::get<core::ApplyResult>(live).disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(std::get<core::ApplyResult>(live).last_update_id_after,
             core::UpdateId{103});

  const auto output = host.make_snapshot();
  REQUIRE(std::holds_alternative<core::LocalOrderBookSnapshot>(output));
  const auto &snapshot = std::get<core::LocalOrderBookSnapshot>(output);
  REQUIRE_EQ(snapshot.venue(), common_wire::VENUE_BINANCE);
  REQUIRE_EQ(snapshot.market(), common_wire::MARKET_SPOT);
  REQUIRE_EQ(snapshot.symbol(), "BTCUSDT");
  REQUIRE_EQ(snapshot.source(), common_wire::SNAPSHOT_SOURCE_GATEWAY_LIVE);
  REQUIRE_EQ(snapshot.last_update_id(), 103U);
  REQUIRE(snapshot.synchronized());
  REQUIRE_EQ(snapshot.bids_size(), 1);
  REQUIRE_EQ(snapshot.bids(0).price(), "100.00");
  REQUIRE_EQ(snapshot.bids(0).quantity(), "5.000");
  REQUIRE_EQ(snapshot.asks_size(), 2);
  REQUIRE_EQ(snapshot.asks(0).price(), "101.00");
  REQUIRE_EQ(snapshot.asks(0).quantity(), "3.000");
  REQUIRE_EQ(snapshot.asks(1).price(), "103.00");
  REQUIRE_EQ(snapshot.asks(1).quantity(), "2.000");
  REQUIRE_EQ(snapshot.generated_time_utc_ns(), 1700000000123456000ULL);
  REQUIRE_EQ(snapshot.generated_monotonic_ns(), 1700000000123456789ULL);
  REQUIRE_EQ(snapshot.producer(), "gateway-g2-synthetic");
  REQUIRE_EQ(snapshot.producer_version(), "1.0.0");
}

void deterministic_final_snapshot() {
  const auto first = run_successful_scenario();
  const auto second = run_successful_scenario();
  REQUIRE(!first.empty());
  REQUIRE(!second.empty());
  REQUIRE_EQ(first, second);
}

void reset_rebootstrap() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bid_update(101, 101, "4.500"));
  expect_installed(host, make_snapshot());
  REQUIRE_EQ(host.replay_buffered_updates().size(), 1U);
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::Synchronized);

  host.reset();
  REQUIRE_EQ(host.projection().status(),
             core::ProjectionStatus::AwaitingBaseline);
  REQUIRE(!host.projection().last_update_id().has_value());
  REQUIRE(!host.projection().last_gap().has_value());
  REQUIRE(host.projection().diagnostic_book().empty());
  REQUIRE_EQ(host.buffered_update_count(), 0U);

  host.receive_pre_snapshot(make_bid_update(201, 201, "7.000"));
  expect_installed(host, make_snapshot(200, "3.500", "4.000"));
  const auto outcomes = host.replay_buffered_updates();
  REQUIRE_EQ(outcomes.size(), 1U);
  REQUIRE(std::holds_alternative<core::ApplyResult>(outcomes.front()));
  REQUIRE_EQ(std::get<core::ApplyResult>(outcomes.front()).disposition,
             core::ApplyDisposition::Applied);
  REQUIRE_EQ(host.projection().status(), core::ProjectionStatus::Synchronized);
  REQUIRE_EQ(host.projection().last_update_id(), core::UpdateId{201});
}

void explicit_identity_and_numeric_spec() {
  g2::SyntheticSpotHost host;
  REQUIRE_EQ(host.expected_identity().symbol, "BTCUSDT");
  REQUIRE_EQ(host.expected_identity().policy, core::SequencePolicyKind::Spot);
  REQUIRE_EQ(host.numeric_spec().price_scale.value(), 2U);
  REQUIRE_EQ(host.numeric_spec().quantity_scale.value(), 3U);
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"NORMAL_BOOTSTRAP", normal_bootstrap},
      {"STALE_PREFIX", stale_prefix},
      {"DUPLICATE_PREFIX", duplicate_prefix},
      {"VALID_BRIDGE", valid_bridge},
      {"NO_BRIDGE", no_bridge},
      {"GAP", gap},
      {"POST_LIVE_UPDATE", post_live_update},
      {"DETERMINISTIC_FINAL_SNAPSHOT", deterministic_final_snapshot},
      {"RESET_REBOOTSTRAP", reset_rebootstrap},
      {"EXPLICIT_IDENTITY_AND_NUMERIC_SPEC",
       explicit_identity_and_numeric_spec},
  };

  for (const auto &[name, test] : tests) {
    try {
      test();
    } catch (const std::exception &error) {
      std::cerr << name << "=FAIL " << error.what() << '\n';
      return 1;
    }
  }

  std::cout << "NORMAL_BOOTSTRAP=PASS\n"
               "STALE_PREFIX=PASS\n"
               "DUPLICATE_PREFIX=PASS\n"
               "VALID_BRIDGE=PASS\n"
               "NO_BRIDGE=PASS\n"
               "GAP=PASS\n"
               "POST_LIVE_UPDATE=PASS\n"
               "DETERMINISTIC_FINAL_SNAPSHOT=PASS\n"
               "RESET_REBOOTSTRAP=PASS\n";
  return 0;
}
