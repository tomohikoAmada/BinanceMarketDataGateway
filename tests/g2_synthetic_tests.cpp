#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/synthetic_host.hpp>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <variant>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace gateway = binance_market_data::gateway::v1;
namespace market_wire = binance_market_data::market::v1;
namespace adapter = binance_market_data::projection_adapter::v1;

void expect(bool condition, const char *message, int &failures) {
  if (!condition) {
    std::cerr << "FAIL: " << message << '\n';
    ++failures;
  }
}

[[nodiscard]] market_wire::ExchangeDepthSnapshot snapshot_fixture() {
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("synthetic-rest");
  snapshot.set_producer_version("1");
  snapshot.set_request_id("synthetic-request");
  snapshot.set_last_update_id(100U);

  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("2.000");
  bid = snapshot.add_bids();
  bid->set_price("99.00");
  bid->set_quantity("1.000");

  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("1.500");
  ask = snapshot.add_asks();
  ask->set_price("102.00");
  ask->set_quantity("2.000");
  return snapshot;
}

[[nodiscard]] market_wire::DepthUpdate
update_fixture(std::uint64_t first, std::uint64_t final, std::string bid_price,
               std::string bid_quantity, std::string ask_price,
               std::string ask_quantity) {
  market_wire::DepthUpdate update;
  auto *metadata = update.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("synthetic-websocket");
  metadata->set_producer_version("1");
  metadata->set_connection_id("synthetic-connection");
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  update.set_first_update_id(first);
  update.set_final_update_id(final);

  auto *bid = update.add_bids();
  bid->set_price(std::move(bid_price));
  bid->set_quantity(std::move(bid_quantity));
  auto *ask = update.add_asks();
  ask->set_price(std::move(ask_price));
  ask->set_quantity(std::move(ask_quantity));
  return update;
}

[[nodiscard]] market_wire::DepthUpdate bridge_update() {
  return update_fixture(99U, 101U, "100.00", "3.000", "102.00", "0.500");
}

[[nodiscard]] market_wire::DepthUpdate contiguous_update() {
  return update_fixture(102U, 103U, "98.00", "1.000", "101.00", "0");
}

[[nodiscard]] market_wire::DepthUpdate live_update() {
  return update_fixture(104U, 105U, "100.00", "0", "100.50", "2.250");
}

[[nodiscard]] market_wire::DepthUpdate gap_update() {
  return update_fixture(107U, 107U, "97.00", "1.000", "103.00", "1.000");
}

[[nodiscard]] std::optional<core::LocalOrderBookSnapshot>
run_happy_path(int &failures) {
  gateway::SyntheticSpotBtcusdtHost host;
  expect(host.state() == gateway::SyntheticHostState::Buffering,
         "host starts BUFFERING", failures);
  expect(host.projection_status() == core::ProjectionStatus::AwaitingBaseline,
         "Projection starts AwaitingBaseline", failures);

  const auto first = bridge_update();
  const auto second = contiguous_update();
  expect(std::holds_alternative<gateway::SyntheticHostState>(
             host.receive_depth_update(first)),
         "first pre-snapshot diff is buffered", failures);
  expect(std::holds_alternative<gateway::SyntheticHostState>(
             host.receive_depth_update(second)),
         "second pre-snapshot diff is buffered", failures);
  expect(host.buffered_depth_update_count() == 2U,
         "two diffs remain in the pre-snapshot buffer", failures);
  expect(host.projection_status() == core::ProjectionStatus::AwaitingBaseline,
         "pre-snapshot diffs do not mutate Projection", failures);

  const auto before_baseline_snapshot = host.local_order_book_snapshot();
  expect(
      std::holds_alternative<adapter::AdapterError>(before_baseline_snapshot),
      "no local snapshot exists before a baseline", failures);

  expect(std::holds_alternative<gateway::SyntheticHostState>(
             host.install_snapshot(snapshot_fixture())),
         "synthetic REST snapshot installs", failures);
  expect(host.state() == gateway::SyntheticHostState::BaselineInstalled,
         "state becomes BASELINE_INSTALLED", failures);
  expect(host.projection_status() == core::ProjectionStatus::AwaitingBridge,
         "Projection reports AwaitingBridge after baseline", failures);
  expect(host.baseline_install_count() == 1U, "baseline is installed once",
         failures);

  const auto repeated_install = host.install_snapshot(snapshot_fixture());
  expect(std::holds_alternative<gateway::SyntheticHostError>(repeated_install),
         "second baseline install is rejected by host lifecycle", failures);
  expect(host.baseline_install_count() == 1U,
         "rejected second install does not reinstall baseline", failures);

  expect(
      std::holds_alternative<gateway::SyntheticHostState>(host.replay_buffer()),
      "buffer replay succeeds", failures);
  expect(host.state() == gateway::SyntheticHostState::Live,
         "state becomes LIVE after replay", failures);
  expect(host.buffered_depth_update_count() == 0U,
         "buffer is empty after replay", failures);
  expect(host.projection_status() == core::ProjectionStatus::Synchronized,
         "Projection reaches Synchronized", failures);
  expect(host.last_projection_apply().has_value() &&
             host.last_projection_apply()->disposition ==
                 core::ApplyDisposition::Applied,
         "replay branches on an Applied Projection result", failures);

  expect(std::holds_alternative<gateway::SyntheticHostState>(
             host.receive_depth_update(live_update())),
         "live diff applies through the same Projection path", failures);
  expect(host.state() == gateway::SyntheticHostState::Live,
         "host remains LIVE after live diff", failures);

  const auto output = host.local_order_book_snapshot();
  expect(std::holds_alternative<core::LocalOrderBookSnapshot>(output),
         "final LocalOrderBookSnapshot is produced", failures);
  if (!std::holds_alternative<core::LocalOrderBookSnapshot>(output)) {
    return std::nullopt;
  }

  const auto &book = std::get<core::LocalOrderBookSnapshot>(output);
  expect(book.synchronized(), "final snapshot is synchronized", failures);
  expect(book.last_update_id() == 105U, "final update id is 105", failures);
  expect(book.bids_size() == 2, "final snapshot has two bids", failures);
  expect(book.asks_size() == 2, "final snapshot has two asks", failures);
  expect(book.bids(0).price() == "99.00" && book.bids(0).quantity() == "1.000",
         "top bid changed to 99.00 @ 1.000", failures);
  expect(book.bids(1).price() == "98.00" && book.bids(1).quantity() == "1.000",
         "second bid is 98.00 @ 1.000", failures);
  expect(book.asks(0).price() == "100.50" && book.asks(0).quantity() == "2.250",
         "top ask changed to 100.50 @ 2.250", failures);
  expect(book.asks(1).price() == "102.00" && book.asks(1).quantity() == "0.500",
         "second ask is 102.00 @ 0.500", failures);
  expect(book.symbol() == "BTCUSDT" &&
             book.market() == common_wire::MARKET_SPOT &&
             book.venue() == common_wire::VENUE_BINANCE,
         "snapshot identity is Binance Spot BTCUSDT", failures);
  return book;
}

void test_sequence_result_ownership(int &failures) {
  gateway::SyntheticSpotBtcusdtHost host;
  static_cast<void>(host.receive_depth_update(bridge_update()));
  static_cast<void>(host.receive_depth_update(contiguous_update()));
  static_cast<void>(host.install_snapshot(snapshot_fixture()));
  static_cast<void>(host.replay_buffer());

  const auto result = host.receive_depth_update(gap_update());
  expect(std::holds_alternative<gateway::SyntheticHostError>(result),
         "host reports a Projection-reported gap as non-LIVE", failures);
  expect(host.state() == gateway::SyntheticHostState::Failed,
         "gap leaves the narrow G2 host in FAILED", failures);
  expect(host.projection_status() == core::ProjectionStatus::NeedsResync,
         "Projection owns the resulting NeedsResync status", failures);
  expect(host.last_projection_apply().has_value() &&
             host.last_projection_apply()->disposition ==
                 core::ApplyDisposition::GapDetected,
         "host observed Projection GapDetected without classifying ids",
         failures);
}

void test_determinism(int &failures) {
  const auto first = run_happy_path(failures);
  const auto second = run_happy_path(failures);
  expect(first.has_value() && second.has_value(),
         "both deterministic runs produce snapshots", failures);
  if (first.has_value() && second.has_value()) {
    expect(first->SerializeAsString() == second->SerializeAsString(),
           "repeated synthetic runs are byte-identical", failures);
  }
}

} // namespace

int main() {
  int failures = 0;
  static_cast<void>(run_happy_path(failures));
  test_sequence_result_ownership(failures);
  test_determinism(failures);

  if (failures != 0) {
    return EXIT_FAILURE;
  }
  std::cout << "G2 synthetic tests passed\n";
  return EXIT_SUCCESS;
}
