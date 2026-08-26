#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/synthetic_host.hpp>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>
#include <variant>

namespace {

namespace common_wire = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace gateway = binance_market_data::gateway::v1;
namespace market_wire = binance_market_data::market::v1;

[[nodiscard]] market_wire::ExchangeDepthSnapshot baseline() {
  market_wire::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common_wire::VENUE_BINANCE);
  snapshot.set_market(common_wire::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("synthetic-rest");
  snapshot.set_producer_version("1");
  snapshot.set_request_id("smoke-request");
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
update(std::uint64_t first, std::uint64_t final, std::string bid_price,
       std::string bid_quantity, std::string ask_price,
       std::string ask_quantity) {
  market_wire::DepthUpdate wire;
  auto *metadata = wire.mutable_metadata();
  metadata->set_venue(common_wire::VENUE_BINANCE);
  metadata->set_market(common_wire::MARKET_SPOT);
  metadata->set_symbol("BTCUSDT");
  metadata->set_producer("synthetic-websocket");
  metadata->set_producer_version("1");
  metadata->set_connection_id("synthetic-connection");
  metadata->set_stream(common_wire::STREAM_DIFF_DEPTH);
  metadata->set_schema_version("depth-update.v1");
  wire.set_first_update_id(first);
  wire.set_final_update_id(final);
  auto *bid = wire.add_bids();
  bid->set_price(std::move(bid_price));
  bid->set_quantity(std::move(bid_quantity));
  auto *ask = wire.add_asks();
  ask->set_price(std::move(ask_price));
  ask->set_quantity(std::move(ask_quantity));
  return wire;
}

} // namespace

int main() {
  gateway::SyntheticSpotBtcusdtHost host;
  if (!std::holds_alternative<gateway::SyntheticHostState>(
          host.receive_depth_update(
              update(99U, 101U, "100.00", "3.000", "102.00", "0.500"))) ||
      !std::holds_alternative<gateway::SyntheticHostState>(
          host.receive_depth_update(
              update(102U, 103U, "98.00", "1.000", "101.00", "0"))) ||
      !std::holds_alternative<gateway::SyntheticHostState>(
          host.install_snapshot(baseline())) ||
      !std::holds_alternative<gateway::SyntheticHostState>(
          host.replay_buffer()) ||
      !std::holds_alternative<gateway::SyntheticHostState>(
          host.receive_depth_update(
              update(104U, 105U, "100.00", "0", "100.50", "2.250"))) ||
      host.state() != gateway::SyntheticHostState::Live) {
    return EXIT_FAILURE;
  }

  const auto output = host.local_order_book_snapshot();
  if (!std::holds_alternative<core::LocalOrderBookSnapshot>(output)) {
    return EXIT_FAILURE;
  }
  const auto &book = std::get<core::LocalOrderBookSnapshot>(output);
  if (!book.synchronized() || book.last_update_id() != 105U ||
      book.bids_size() != 2 || book.asks_size() != 2 ||
      book.bids(0).price() != "99.00" || book.bids(0).quantity() != "1.000" ||
      book.bids(1).price() != "98.00" || book.asks(0).price() != "100.50" ||
      book.asks(0).quantity() != "2.250" || book.asks(1).price() != "102.00" ||
      book.asks(1).quantity() != "0.500") {
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
