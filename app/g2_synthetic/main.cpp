#include "synthetic_spot_host.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <variant>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g2 = binance_market_data::gateway::g2;
namespace market_wire = binance_market_data::market::v1;

[[nodiscard]] market_wire::ExchangeDepthSnapshot make_snapshot() {
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("gateway-g2-synthetic");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g2-snapshot-request-1");
  snapshot.set_last_update_id(100);
  snapshot.set_exchange_transaction_time_ms(1700000000001ULL);
  snapshot.set_receive_time_utc_ns(1700000000001000000ULL);
  snapshot.set_receive_monotonic_ns(9000000000001ULL);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("2.500");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("3.000");
  return snapshot;
}

[[nodiscard]] market_wire::DepthUpdate make_bridge() {
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
  metadata->set_exchange_event_time_ms(1700000000002ULL);
  metadata->set_receive_time_utc_ns(1700000000002000000ULL);
  metadata->set_receive_monotonic_ns(9000000000002ULL);
  update.set_first_update_id(101);
  update.set_final_update_id(101);
  auto *bid = update.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("4.500");
  return update;
}

[[nodiscard]] market_wire::DepthUpdate make_live_update() {
  auto update = make_bridge();
  update.set_first_update_id(102);
  update.set_final_update_id(103);
  update.mutable_bids(0)->set_quantity("5.000");
  auto *removed_ask = update.add_asks();
  removed_ask->set_price("102.00");
  removed_ask->set_quantity("0.000");
  auto *new_ask = update.add_asks();
  new_ask->set_price("103.00");
  new_ask->set_quantity("2.000");
  return update;
}

} // namespace

int main() {
  g2::SyntheticSpotHost host;
  host.receive_pre_snapshot(make_bridge());

  const auto installed = host.install_snapshot(make_snapshot());
  if (!std::holds_alternative<core::InstallResult>(installed) ||
      std::get<core::InstallResult>(installed).disposition !=
          core::InstallDisposition::Installed) {
    return EXIT_FAILURE;
  }

  const auto replay = host.replay_buffered_updates();
  if (replay.size() != 1U ||
      !std::holds_alternative<core::ApplyResult>(replay.front()) ||
      std::get<core::ApplyResult>(replay.front()).disposition !=
          core::ApplyDisposition::Applied ||
      host.projection().status() != core::ProjectionStatus::Synchronized) {
    return EXIT_FAILURE;
  }

  const auto live = host.apply_live_update(make_live_update());
  if (!std::holds_alternative<core::ApplyResult>(live) ||
      std::get<core::ApplyResult>(live).disposition !=
          core::ApplyDisposition::Applied ||
      std::get<core::ApplyResult>(live).last_update_id_after !=
          core::UpdateId{103}) {
    return EXIT_FAILURE;
  }

  const auto output = host.make_snapshot();
  if (!std::holds_alternative<core::LocalOrderBookSnapshot>(output)) {
    return EXIT_FAILURE;
  }
  const auto &snapshot = std::get<core::LocalOrderBookSnapshot>(output);
  if (snapshot.last_update_id() != 103U || !snapshot.synchronized() ||
      snapshot.bids_size() != 1 || snapshot.asks_size() != 2 ||
      snapshot.bids(0).quantity() != "5.000" ||
      snapshot.asks(0).price() != "101.00" ||
      snapshot.asks(1).price() != "103.00") {
    return EXIT_FAILURE;
  }

  std::cout
      << "G2_SYNTHETIC_HOST=PASS venue=BINANCE market=SPOT symbol=BTCUSDT "
         "status=Synchronized last_update_id=103\n";
  return EXIT_SUCCESS;
}
