#include "grpc_service.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace {

namespace common = binance_market_data::common::v1;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace wire = binance_market_data::gateway::v1;

[[nodiscard]] wire::OrderBookSubscriptionRequest
book_request(common::Market market, std::string request_id) {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(market);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] wire::EventSubscriptionRequest event_request() {
  wire::EventSubscriptionRequest request;
  request.set_request_id("production-real-usdm-event");
  request.set_schema_version(g9::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(common::MARKET_USD_M_PERPETUAL);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(common::STREAM_DIFF_DEPTH);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(g9::kDepthEventSchema);
  return request;
}

[[nodiscard]] bool
read_book_head(grpc::ClientReader<wire::OrderBookStreamItem> &reader,
               common::Market market) {
  wire::OrderBookStreamItem item;
  if (!reader.Read(&item) || !item.has_subscription_accepted()) {
    return false;
  }
  if (!reader.Read(&item) || !item.has_snapshot() ||
      item.snapshot().market() != market ||
      item.snapshot().symbol() != "BTCUSDT" ||
      !item.snapshot().synchronized() || item.snapshot().bids().empty() ||
      item.snapshot().asks().empty()) {
    return false;
  }
  while (reader.Read(&item)) {
    if (item.has_depth_update()) {
      return item.depth_update().metadata().market() == market &&
             item.depth_update().metadata().symbol() == "BTCUSDT";
    }
    if (item.has_consumer_gap()) {
      return false;
    }
  }
  return false;
}

[[nodiscard]] bool
read_event_head(grpc::ClientReader<wire::GatewayEventEnvelope> &reader) {
  wire::GatewayEventEnvelope item;
  if (!reader.Read(&item) || !item.has_subscription_accepted()) {
    return false;
  }
  while (reader.Read(&item)) {
    if (item.has_depth_update()) {
      return item.depth_update().metadata().market() ==
                 common::MARKET_USD_M_PERPETUAL &&
             item.depth_update().metadata().symbol() == "BTCUSDT";
    }
    if (item.has_consumer_gap()) {
      return false;
    }
  }
  return false;
}

template <typename Reader, typename Item> void drain(Reader &reader) {
  Item item;
  while (reader.Read(&item)) {
  }
}

} // namespace

int main(int argc, char **argv) {
  if (argc != 3 || std::string_view{argv[1]} != "--grpc-target") {
    std::cerr << "Usage: bmd-gateway-production-acceptance-client "
                 "--grpc-target HOST:PORT\n";
    return EXIT_FAILURE;
  }

  auto stub = wire::BinanceMarketDataGatewayService::NewStub(
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()));
  const auto deadline =
      std::chrono::system_clock::now() + std::chrono::seconds{180};
  grpc::ClientContext spot_context;
  grpc::ClientContext usdm_context;
  grpc::ClientContext event_context;
  spot_context.set_deadline(deadline);
  usdm_context.set_deadline(deadline);
  event_context.set_deadline(deadline);
  auto spot_reader = stub->SubscribeOrderBook(
      &spot_context,
      book_request(common::MARKET_SPOT, "production-real-spot-book"));
  auto usdm_reader = stub->SubscribeOrderBook(
      &usdm_context, book_request(common::MARKET_USD_M_PERPETUAL,
                                  "production-real-usdm-book"));
  auto event_reader = stub->SubscribeEvents(&event_context, event_request());

  const auto spot_book = read_book_head(*spot_reader, common::MARKET_SPOT);
  const auto usdm_book =
      read_book_head(*usdm_reader, common::MARKET_USD_M_PERPETUAL);
  const auto usdm_event = read_event_head(*event_reader);

  wire::GatewayStatusRequest status_request;
  status_request.set_request_id("production-real-status");
  status_request.set_schema_version(g10::kStatusRequestSchema);
  wire::GatewayStatusSnapshot status_response;
  grpc::ClientContext status_context;
  status_context.set_deadline(std::chrono::system_clock::now() +
                              std::chrono::seconds{10});
  const auto status =
      stub->GetGatewayStatus(&status_context, status_request, &status_response);
  const auto status_valid =
      status.ok() && status_response.markets_size() == 2 &&
      status_response.markets(0).market() == common::MARKET_SPOT &&
      status_response.markets(0).state() ==
          common::STREAM_LIFECYCLE_STATE_LIVE &&
      status_response.markets(1).market() == common::MARKET_USD_M_PERPETUAL &&
      status_response.markets(1).state() == common::STREAM_LIFECYCLE_STATE_LIVE;

  std::cout << "REAL_SPOT_ORDER_BOOK=" << (spot_book ? "PASS" : "FAIL") << '\n'
            << "REAL_USDM_ORDER_BOOK=" << (usdm_book ? "PASS" : "FAIL") << '\n'
            << "REAL_USDM_DIFF_DEPTH=" << (usdm_event ? "PASS" : "FAIL") << '\n'
            << "REAL_STATUS_TWO_MARKETS=" << (status_valid ? "PASS" : "FAIL")
            << '\n'
            << "CLIENT_READY_FOR_SIGTERM="
            << (spot_book && usdm_book && usdm_event && status_valid ? "YES"
                                                                     : "NO")
            << '\n'
            << std::flush;
  if (!spot_book || !usdm_book || !usdm_event || !status_valid) {
    spot_context.TryCancel();
    usdm_context.TryCancel();
    event_context.TryCancel();
    return EXIT_FAILURE;
  }

  drain<grpc::ClientReader<wire::OrderBookStreamItem>,
        wire::OrderBookStreamItem>(*spot_reader);
  drain<grpc::ClientReader<wire::OrderBookStreamItem>,
        wire::OrderBookStreamItem>(*usdm_reader);
  drain<grpc::ClientReader<wire::GatewayEventEnvelope>,
        wire::GatewayEventEnvelope>(*event_reader);
  static_cast<void>(spot_reader->Finish());
  static_cast<void>(usdm_reader->Finish());
  static_cast<void>(event_reader->Finish());
  std::cout << "CLIENT_STREAMS_TERMINATED=YES\n";
  return EXIT_SUCCESS;
}
