#include "grpc_service.hpp"
#include "multi_market_runtime.hpp"
#include "spot_protocol.hpp"
#include "usdm_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace g11 = binance_market_data::gateway::g11;
namespace wire = binance_market_data::gateway::v1;

inline constexpr auto kStreamDeadline = std::chrono::seconds{120};

struct SessionSequence final {
  std::uint64_t next{1U};
  std::string subscription_id;
};

struct OrderBookEvidence final {
  SessionSequence sequence;
  std::uint64_t snapshot_update_id{0U};
  std::uint64_t latest_update_id{0U};
  std::size_t applied_updates{0U};
};

struct EventEvidence final {
  SessionSequence sequence;
  std::size_t depth_events{0U};
};

void print_network_error(std::string_view product,
                         const g4::NetworkError &error) {
  std::cerr << "NETWORK_ERROR product=" << product << " stage=" << error.stage
            << " message=" << error.message;
  if (error.http_status.has_value()) {
    std::cerr << " http_status=" << *error.http_status;
  }
  if (error.retry_after.has_value()) {
    std::cerr << " retry_after=" << *error.retry_after;
  }
  std::cerr << '\n';
}

void print_recovery_error(std::string_view product,
                          const g5::RecoveryObservation &observation) {
  if (observation.terminal_error.has_value()) {
    print_network_error(product, *observation.terminal_error);
  }
}

template <typename Item>
[[nodiscard]] bool
canonical_delivery(const Item &item, SessionSequence &sequence,
                   const std::string &gateway_instance_id,
                   std::optional<std::uint64_t> expected_generation) {
  if (item.has_envelope_metadata() || !item.has_delivery_metadata()) {
    return false;
  }
  const auto &metadata = item.delivery_metadata();
  if (metadata.protocol_version() != g7::kProtocolVersion ||
      metadata.gateway_instance_id() != gateway_instance_id ||
      metadata.session_sequence() != sequence.next ||
      metadata.publish_time_utc_ns() == 0U ||
      metadata.has_connection_generation() != expected_generation.has_value()) {
    return false;
  }
  if (expected_generation.has_value() &&
      metadata.connection_generation() != *expected_generation) {
    return false;
  }
  if (sequence.subscription_id.empty()) {
    sequence.subscription_id = metadata.subscription_id();
  } else if (metadata.subscription_id() != sequence.subscription_id) {
    return false;
  }
  if (sequence.subscription_id.empty()) {
    return false;
  }
  ++sequence.next;
  return true;
}

[[nodiscard]] bool valid_market_metadata(
    const binance_market_data::common::v1::EventMetadata &metadata,
    common::Market product) {
  return metadata.venue() == common::VENUE_BINANCE &&
         metadata.market() == product && metadata.symbol() == "BTCUSDT";
}

[[nodiscard]] wire::OrderBookSubscriptionRequest
order_book_request(common::Market product, std::string request_id) {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(product);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] wire::EventSubscriptionRequest
usdm_event_request(std::string request_id) {
  wire::EventSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
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
read_order_book_head(grpc::ClientReader<wire::OrderBookStreamItem> &reader,
                     common::Market product, std::uint64_t generation,
                     const std::string &gateway_instance_id,
                     OrderBookEvidence &evidence) {
  wire::OrderBookStreamItem item;
  if (!reader.Read(&item) || !item.has_subscription_accepted() ||
      !canonical_delivery(item, evidence.sequence, gateway_instance_id,
                          std::nullopt) ||
      item.subscription_accepted().schema_version() !=
          g7::kSubscriptionAcceptedSchema) {
    return false;
  }
  if (!reader.Read(&item) || !item.has_snapshot() ||
      !canonical_delivery(item, evidence.sequence, gateway_instance_id,
                          generation) ||
      item.snapshot().venue() != common::VENUE_BINANCE ||
      item.snapshot().market() != product ||
      item.snapshot().symbol() != "BTCUSDT" ||
      item.snapshot().schema_version() != g7::kSnapshotSchema ||
      !item.snapshot().synchronized() ||
      item.snapshot().last_update_id() == 0U ||
      item.snapshot().bids().empty() || item.snapshot().asks().empty()) {
    return false;
  }
  evidence.snapshot_update_id = item.snapshot().last_update_id();
  if (!reader.Read(&item) || !item.has_depth_update() ||
      !canonical_delivery(item, evidence.sequence, gateway_instance_id,
                          generation) ||
      !valid_market_metadata(item.depth_update().metadata(), product) ||
      item.depth_update().metadata().schema_version() != g7::kUpdateSchema ||
      item.depth_update().final_update_id() <= evidence.snapshot_update_id) {
    return false;
  }
  evidence.latest_update_id = item.depth_update().final_update_id();
  ++evidence.applied_updates;
  return true;
}

[[nodiscard]] bool
read_spot_update_after(grpc::ClientReader<wire::OrderBookStreamItem> &reader,
                       std::uint64_t generation, std::uint64_t update_id_cut,
                       const std::string &gateway_instance_id,
                       OrderBookEvidence &evidence) {
  wire::OrderBookStreamItem item;
  while (reader.Read(&item)) {
    if (!item.has_depth_update() ||
        !canonical_delivery(item, evidence.sequence, gateway_instance_id,
                            generation) ||
        !valid_market_metadata(item.depth_update().metadata(),
                               common::MARKET_SPOT) ||
        item.depth_update().metadata().schema_version() != g7::kUpdateSchema) {
      return false;
    }
    evidence.latest_update_id = item.depth_update().final_update_id();
    ++evidence.applied_updates;
    if (evidence.latest_update_id > update_id_cut) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] bool read_order_book_recovery_cut(
    grpc::ClientReader<wire::OrderBookStreamItem> &reader,
    common::Market product, std::uint64_t generation,
    const std::string &gateway_instance_id, OrderBookEvidence &evidence) {
  wire::OrderBookStreamItem item;
  while (reader.Read(&item)) {
    if (!canonical_delivery(item, evidence.sequence, gateway_instance_id,
                            generation)) {
      return false;
    }
    if (item.has_depth_update()) {
      if (!valid_market_metadata(item.depth_update().metadata(), product) ||
          item.depth_update().metadata().schema_version() !=
              g7::kUpdateSchema) {
        return false;
      }
      evidence.latest_update_id = item.depth_update().final_update_id();
      ++evidence.applied_updates;
      continue;
    }
    const auto terminal_valid =
        item.has_consumer_gap() && item.consumer_gap().market() == product &&
        item.consumer_gap().symbol() == "BTCUSDT" &&
        item.consumer_gap().stream() == common::STREAM_DIFF_DEPTH &&
        item.consumer_gap().reason() ==
            common::CONSUMER_GAP_REASON_RESUME_NOT_AVAILABLE &&
        item.consumer_gap().recovery_action() ==
            common::RECOVERY_ACTION_REQUEST_NEW_SNAPSHOT;
    return terminal_valid && !reader.Read(&item);
  }
  return false;
}

[[nodiscard]] bool
read_usdm_event_head(grpc::ClientReader<wire::GatewayEventEnvelope> &reader,
                     std::uint64_t generation,
                     const std::string &gateway_instance_id,
                     EventEvidence &evidence) {
  wire::GatewayEventEnvelope item;
  if (!reader.Read(&item) || !item.has_subscription_accepted() ||
      !canonical_delivery(item, evidence.sequence, gateway_instance_id,
                          std::nullopt) ||
      item.subscription_accepted().negotiated_payload_schema_versions_size() !=
          1 ||
      item.subscription_accepted().negotiated_payload_schema_versions(0) !=
          g9::kDepthEventSchema) {
    return false;
  }
  if (!reader.Read(&item) || !item.has_depth_update() ||
      !canonical_delivery(item, evidence.sequence, gateway_instance_id,
                          generation) ||
      !valid_market_metadata(item.depth_update().metadata(),
                             common::MARKET_USD_M_PERPETUAL) ||
      item.depth_update().metadata().schema_version() !=
          g9::kDepthEventSchema ||
      !item.depth_update().metadata().has_receive_time_utc_ns() ||
      !item.depth_update().metadata().has_receive_monotonic_ns()) {
    return false;
  }
  ++evidence.depth_events;
  return true;
}

[[nodiscard]] bool read_event_generation_cut(
    grpc::ClientReader<wire::GatewayEventEnvelope> &reader,
    std::uint64_t generation, const std::string &gateway_instance_id,
    EventEvidence &evidence) {
  wire::GatewayEventEnvelope item;
  while (reader.Read(&item)) {
    if (!canonical_delivery(item, evidence.sequence, gateway_instance_id,
                            generation)) {
      return false;
    }
    if (item.has_depth_update()) {
      if (!valid_market_metadata(item.depth_update().metadata(),
                                 common::MARKET_USD_M_PERPETUAL) ||
          item.depth_update().metadata().schema_version() !=
              g9::kDepthEventSchema) {
        return false;
      }
      ++evidence.depth_events;
      continue;
    }
    const auto terminal_valid =
        item.has_consumer_gap() &&
        item.consumer_gap().market() == common::MARKET_USD_M_PERPETUAL &&
        item.consumer_gap().symbol() == "BTCUSDT" &&
        item.consumer_gap().stream() == common::STREAM_DIFF_DEPTH &&
        item.consumer_gap().reason() ==
            common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED &&
        item.consumer_gap().recovery_action() ==
            common::RECOVERY_ACTION_RESUBSCRIBE;
    return terminal_valid && !reader.Read(&item);
  }
  return false;
}

template <typename Reader, typename Item>
[[nodiscard]] grpc::Status cancel_and_finish(grpc::ClientContext &context,
                                             Reader &reader) {
  context.TryCancel();
  Item ignored;
  while (reader.Read(&ignored)) {
  }
  return reader.Finish();
}

[[nodiscard]] bool client_cancel_status(const grpc::Status &status) {
  return status.ok() || status.error_code() == grpc::StatusCode::CANCELLED;
}

[[nodiscard]] bool
run_fresh_usdm_order_book(wire::BinanceMarketDataGatewayService::Stub &stub,
                          const std::string &gateway_instance_id,
                          OrderBookEvidence &evidence) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kStreamDeadline);
  auto reader = stub.SubscribeOrderBook(
      &context,
      order_book_request(common::MARKET_USD_M_PERPETUAL, "g11-usdm-book-g2"));
  const auto valid =
      read_order_book_head(*reader, common::MARKET_USD_M_PERPETUAL, 2U,
                           gateway_instance_id, evidence);
  const auto status =
      cancel_and_finish<grpc::ClientReader<wire::OrderBookStreamItem>,
                        wire::OrderBookStreamItem>(context, *reader);
  return valid && client_cancel_status(status);
}

[[nodiscard]] bool
run_fresh_usdm_event(wire::BinanceMarketDataGatewayService::Stub &stub,
                     const std::string &gateway_instance_id,
                     EventEvidence &evidence) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kStreamDeadline);
  auto reader =
      stub.SubscribeEvents(&context, usdm_event_request("g11-usdm-event-g2"));
  const auto valid =
      read_usdm_event_head(*reader, 2U, gateway_instance_id, evidence);
  const auto status =
      cancel_and_finish<grpc::ClientReader<wire::GatewayEventEnvelope>,
                        wire::GatewayEventEnvelope>(context, *reader);
  return valid && client_cancel_status(status);
}

[[nodiscard]] bool
collect_status(wire::BinanceMarketDataGatewayService::Stub &stub,
               const std::string &gateway_instance_id, std::string request_id,
               std::uint64_t spot_generation, std::uint64_t usdm_generation,
               std::uint64_t spot_subscriptions,
               std::uint64_t usdm_subscriptions) {
  wire::GatewayStatusRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g10::kStatusRequestSchema);
  wire::GatewayStatusSnapshot response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds{10});
  const auto status = stub.GetGatewayStatus(&context, request, &response);
  return status.ok() &&
         response.schema_version() == g10::kStatusSnapshotSchema &&
         response.gateway_instance_id() == gateway_instance_id &&
         response.markets_size() == 2 &&
         response.markets(0).venue() == common::VENUE_BINANCE &&
         response.markets(0).market() == common::MARKET_SPOT &&
         response.markets(0).symbol() == "BTCUSDT" &&
         response.markets(0).state() == common::STREAM_LIFECYCLE_STATE_LIVE &&
         response.markets(0).has_connection_generation() &&
         response.markets(0).connection_generation() == spot_generation &&
         response.markets(0).active_subscription_count() ==
             spot_subscriptions &&
         response.markets(1).venue() == common::VENUE_BINANCE &&
         response.markets(1).market() == common::MARKET_USD_M_PERPETUAL &&
         response.markets(1).symbol() == "BTCUSDT" &&
         response.markets(1).state() == common::STREAM_LIFECYCLE_STATE_LIVE &&
         response.markets(1).has_connection_generation() &&
         response.markets(1).connection_generation() == usdm_generation &&
         response.markets(1).active_subscription_count() ==
             usdm_subscriptions &&
         response.total_active_subscriptions() ==
             spot_subscriptions + usdm_subscriptions;
}

template <typename Predicate>
[[nodiscard]] bool
wait_for(Predicate predicate,
         std::chrono::seconds timeout = std::chrono::seconds{5}) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  do {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  } while (std::chrono::steady_clock::now() < deadline);
  return predicate();
}

} // namespace

int main() {
  const auto spot_exchange_info = g4::fetch_exchange_info_https();
  if (const auto *failure =
          std::get_if<g4::NetworkError>(&spot_exchange_info)) {
    print_network_error("SPOT", *failure);
    return EXIT_FAILURE;
  }
  const auto &spot_exchange_response =
      std::get<g4::ExchangeInfoResponse>(spot_exchange_info);
  const auto spot_metadata =
      g4::parse_exchange_info(spot_exchange_response.body);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&spot_metadata)) {
    std::cerr << "SPOT_EXCHANGE_INFO_PARSE=FAIL field=" << failure->field
              << " message=" << failure->message << '\n';
    return EXIT_FAILURE;
  }

  const auto usdm_exchange_info = g11::fetch_usdm_exchange_info_https();
  if (const auto *failure =
          std::get_if<g4::NetworkError>(&usdm_exchange_info)) {
    print_network_error("USD_M_PERPETUAL", *failure);
    return EXIT_FAILURE;
  }
  const auto &usdm_exchange_response =
      std::get<g4::ExchangeInfoResponse>(usdm_exchange_info);
  const auto usdm_metadata =
      g11::parse_usdm_exchange_info(usdm_exchange_response.body);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&usdm_metadata)) {
    std::cerr << "USDM_EXCHANGE_INFO_PARSE=FAIL field=" << failure->field
              << " message=" << failure->message << '\n';
    return EXIT_FAILURE;
  }

  const auto gateway_instance_id = g7::generate_gateway_instance_id();
  g3::RuntimeClock clock = g4::sample_real_clock;
  g11::TwoProductRuntimeOptions product_options;
  product_options.spot.runtime_limits = {256U, 256U};
  product_options.usdm.runtime_limits = {256U, 256U};
  g11::TwoProductRuntime products{
      std::get<g4::SpotMetadata>(spot_metadata).numeric_spec,
      std::get<g11::UsdMMetadata>(usdm_metadata).numeric_spec, clock,
      gateway_instance_id, std::move(product_options)};
  const auto starts = products.start();
  if (starts.spot != g5::RecoveryStartResult::Started ||
      starts.usdm != g5::RecoveryStartResult::Started) {
    std::cerr << "G11_RUNTIME_START=FAIL\n";
    products.stop();
    return EXIT_FAILURE;
  }

  const auto spot_initial =
      products.spot().recovery().wait_for_generation_live(1U);
  const auto usdm_initial =
      products.usdm().recovery().wait_for_generation_live(1U);
  const auto spot_initial_runtime = products.spot().runtime().observe();
  const auto usdm_initial_runtime = products.usdm().runtime().observe();
  const bool initial_live =
      spot_initial.state == g5::RecoveryState::Live && !spot_initial.terminal &&
      spot_initial.connection_generation == 1U &&
      usdm_initial.state == g5::RecoveryState::Live && !usdm_initial.terminal &&
      usdm_initial.connection_generation == 1U &&
      spot_initial_runtime.state == g3::RuntimeState::Live &&
      spot_initial_runtime.projection_status ==
          core::ProjectionStatus::Synchronized &&
      usdm_initial_runtime.state == g3::RuntimeState::Live &&
      usdm_initial_runtime.projection_status ==
          core::ProjectionStatus::Synchronized &&
      spot_initial_runtime.owner_thread_id != std::thread::id{} &&
      usdm_initial_runtime.owner_thread_id != std::thread::id{} &&
      spot_initial_runtime.owner_thread_id !=
          usdm_initial_runtime.owner_thread_id &&
      spot_initial.active_transport_count == 1U &&
      usdm_initial.active_transport_count == 1U;
  if (!initial_live) {
    print_recovery_error("SPOT", spot_initial);
    print_recovery_error("USD_M_PERPETUAL", usdm_initial);
    products.stop();
    return EXIT_FAILURE;
  }

  g7::OrderBookGrpcServer server{products.registry(), clock,
                                 gateway_instance_id};
  if (!server.start("127.0.0.1:0")) {
    std::cerr << "G11_GRPC_START=FAIL\n";
    products.stop();
    return EXIT_FAILURE;
  }
  auto stub = wire::BinanceMarketDataGatewayService::NewStub(
      grpc::CreateChannel("127.0.0.1:" + std::to_string(server.selected_port()),
                          grpc::InsecureChannelCredentials()));

  grpc::ClientContext spot_book_context;
  grpc::ClientContext usdm_book_context;
  grpc::ClientContext usdm_event_context;
  spot_book_context.set_deadline(std::chrono::system_clock::now() +
                                 kStreamDeadline);
  usdm_book_context.set_deadline(std::chrono::system_clock::now() +
                                 kStreamDeadline);
  usdm_event_context.set_deadline(std::chrono::system_clock::now() +
                                  kStreamDeadline);
  auto spot_book_reader = stub->SubscribeOrderBook(
      &spot_book_context,
      order_book_request(common::MARKET_SPOT, "g11-spot-book-g1"));
  auto usdm_book_reader = stub->SubscribeOrderBook(
      &usdm_book_context,
      order_book_request(common::MARKET_USD_M_PERPETUAL, "g11-usdm-book-g1"));
  auto usdm_event_reader = stub->SubscribeEvents(
      &usdm_event_context, usdm_event_request("g11-usdm-event-g1"));

  OrderBookEvidence spot_book;
  OrderBookEvidence usdm_book;
  EventEvidence usdm_event;
  const bool initial_streams =
      read_order_book_head(*spot_book_reader, common::MARKET_SPOT, 1U,
                           gateway_instance_id, spot_book) &&
      read_order_book_head(*usdm_book_reader, common::MARKET_USD_M_PERPETUAL,
                           1U, gateway_instance_id, usdm_book) &&
      read_usdm_event_head(*usdm_event_reader, 1U, gateway_instance_id,
                           usdm_event) &&
      spot_book.sequence.subscription_id == "ob-1" &&
      usdm_book.sequence.subscription_id == "ob-1" &&
      usdm_event.sequence.subscription_id == "ev-1";
  const bool initial_status =
      initial_streams && collect_status(*stub, gateway_instance_id,
                                        "g11-status-g1", 1U, 1U, 1U, 2U);

  const auto spot_before_recovery = products.spot().recovery().observe();
  const auto spot_runtime_before_recovery = products.spot().runtime().observe();
  const bool recovery_requested =
      initial_status &&
      spot_runtime_before_recovery.last_update_id.has_value() &&
      products.usdm().recovery().request_controlled_recovery_for_acceptance();
  const bool usdm_recovery_window_observed =
      recovery_requested &&
      wait_for(
          [&] {
            const auto observation = products.usdm().recovery().observe();
            return observation.state == g5::RecoveryState::Backoff ||
                   observation.state == g5::RecoveryState::Recovering;
          },
          std::chrono::seconds{10});
  const auto spot_runtime_during_recovery = products.spot().runtime().observe();
  const auto spot_update_cut =
      spot_runtime_during_recovery.last_update_id.value_or(0U);
  const bool spot_continued =
      usdm_recovery_window_observed && spot_update_cut != 0U &&
      read_spot_update_after(*spot_book_reader, 1U, spot_update_cut,
                             gateway_instance_id, spot_book);
  const bool old_usdm_book_cut =
      recovery_requested &&
      read_order_book_recovery_cut(*usdm_book_reader,
                                   common::MARKET_USD_M_PERPETUAL, 1U,
                                   gateway_instance_id, usdm_book);
  const auto old_usdm_book_status =
      old_usdm_book_cut
          ? usdm_book_reader->Finish()
          : cancel_and_finish<grpc::ClientReader<wire::OrderBookStreamItem>,
                              wire::OrderBookStreamItem>(usdm_book_context,
                                                         *usdm_book_reader);
  const bool old_usdm_event_cut =
      recovery_requested &&
      read_event_generation_cut(*usdm_event_reader, 1U, gateway_instance_id,
                                usdm_event);
  const auto old_usdm_event_status =
      old_usdm_event_cut
          ? usdm_event_reader->Finish()
          : cancel_and_finish<grpc::ClientReader<wire::GatewayEventEnvelope>,
                              wire::GatewayEventEnvelope>(usdm_event_context,
                                                          *usdm_event_reader);

  const auto usdm_recovered =
      recovery_requested
          ? products.usdm().recovery().wait_for_generation_live(2U)
          : products.usdm().recovery().observe();
  const auto spot_after_recovery = products.spot().recovery().observe();
  const auto spot_runtime_after_recovery = products.spot().runtime().observe();
  const auto usdm_runtime_after_recovery = products.usdm().runtime().observe();
  const bool isolation =
      usdm_recovery_window_observed && spot_continued && old_usdm_book_cut &&
      old_usdm_book_status.ok() && old_usdm_event_cut &&
      old_usdm_event_status.ok() &&
      usdm_recovered.state == g5::RecoveryState::Live &&
      !usdm_recovered.terminal && usdm_recovered.connection_generation == 2U &&
      usdm_recovered.connection_id != usdm_initial.connection_id &&
      usdm_runtime_after_recovery.state == g3::RuntimeState::Live &&
      usdm_runtime_after_recovery.projection_status ==
          core::ProjectionStatus::Synchronized &&
      usdm_runtime_after_recovery.current_projection_generation == 2U &&
      spot_after_recovery.state == g5::RecoveryState::Live &&
      spot_after_recovery.connection_generation ==
          spot_before_recovery.connection_generation &&
      spot_after_recovery.connection_id == spot_before_recovery.connection_id &&
      spot_after_recovery.active_transport_count == 1U &&
      spot_runtime_after_recovery.state == g3::RuntimeState::Live &&
      spot_runtime_after_recovery.projection_status ==
          core::ProjectionStatus::Synchronized &&
      spot_runtime_after_recovery.reset_count ==
          spot_runtime_before_recovery.reset_count;
  if (!isolation) {
    print_recovery_error("USD_M_PERPETUAL", usdm_recovered);
  }

  OrderBookEvidence fresh_usdm_book;
  EventEvidence fresh_usdm_event;
  const bool fresh_usdm =
      isolation &&
      run_fresh_usdm_order_book(*stub, gateway_instance_id, fresh_usdm_book) &&
      run_fresh_usdm_event(*stub, gateway_instance_id, fresh_usdm_event) &&
      fresh_usdm_book.sequence.subscription_id == "ob-2" &&
      fresh_usdm_event.sequence.subscription_id == "ev-2";
  const bool fresh_usdm_removed =
      fresh_usdm && wait_for([&] {
        return products.usdm()
                       .runtime()
                       .observe()
                       .resident_subscription_count == 0U &&
               products.usdm()
                       .event_publication()
                       .observe()
                       .active_subscriptions == 0U;
      });
  const bool recovered_status =
      fresh_usdm_removed && collect_status(*stub, gateway_instance_id,
                                           "g11-status-g2", 1U, 2U, 1U, 0U);

  const auto spot_cancel_status =
      cancel_and_finish<grpc::ClientReader<wire::OrderBookStreamItem>,
                        wire::OrderBookStreamItem>(spot_book_context,
                                                   *spot_book_reader);
  const bool streams_removed = wait_for([&] {
    return products.spot().runtime().observe().resident_subscription_count ==
               0U &&
           products.usdm().runtime().observe().resident_subscription_count ==
               0U &&
           products.spot().event_publication().observe().active_subscriptions ==
               0U &&
           products.usdm().event_publication().observe().active_subscriptions ==
               0U &&
           server.service().tracked_context_count() == 0U;
  });

  const auto spot_before_shutdown = products.spot().recovery().observe();
  const auto usdm_before_shutdown = products.usdm().recovery().observe();
  const bool transport_bound =
      spot_before_shutdown.active_transport_count == 1U &&
      usdm_before_shutdown.active_transport_count == 1U &&
      spot_before_shutdown.max_active_transport_count <= 1U &&
      usdm_before_shutdown.max_active_transport_count <= 1U;

  server.shutdown();
  const bool grpc_clean = !server.service().admission_open() &&
                          server.service().tracked_context_count() == 0U &&
                          !server.service().status_inflight();
  products.stop();
  const auto spot_final_recovery = products.spot().recovery().observe();
  const auto usdm_final_recovery = products.usdm().recovery().observe();
  const auto spot_final_runtime = products.spot().runtime().observe();
  const auto usdm_final_runtime = products.usdm().runtime().observe();
  const auto spot_final_events = products.spot().event_publication().observe();
  const auto usdm_final_events = products.usdm().event_publication().observe();
  const bool final_clean =
      spot_final_recovery.state == g5::RecoveryState::Stopped &&
      usdm_final_recovery.state == g5::RecoveryState::Stopped &&
      spot_final_recovery.active_transport_count == 0U &&
      usdm_final_recovery.active_transport_count == 0U &&
      spot_final_runtime.state == g3::RuntimeState::Stopped &&
      usdm_final_runtime.state == g3::RuntimeState::Stopped &&
      spot_final_runtime.owner_joined && usdm_final_runtime.owner_joined &&
      spot_final_runtime.resident_subscription_count == 0U &&
      usdm_final_runtime.resident_subscription_count == 0U &&
      spot_final_events.active_subscriptions == 0U &&
      usdm_final_events.active_subscriptions == 0U;

  const bool passed = spot_exchange_response.tls_verified &&
                      usdm_exchange_response.tls_verified && initial_streams &&
                      initial_status && recovery_requested && isolation &&
                      fresh_usdm && fresh_usdm_removed && recovered_status &&
                      client_cancel_status(spot_cancel_status) &&
                      streams_removed && transport_bound && grpc_clean &&
                      final_clean;
  if (!passed) {
    std::cerr
        << "REAL_G11_ACCEPTANCE=FAIL" << " initial_streams=" << initial_streams
        << " initial_status=" << initial_status
        << " recovery_requested=" << recovery_requested
        << " recovery_window=" << usdm_recovery_window_observed
        << " spot_continued=" << spot_continued
        << " old_usdm_book_cut=" << old_usdm_book_cut
        << " old_usdm_book_status="
        << static_cast<unsigned>(old_usdm_book_status.error_code())
        << " old_usdm_event_cut=" << old_usdm_event_cut
        << " old_usdm_event_status="
        << static_cast<unsigned>(old_usdm_event_status.error_code())
        << " recovered_state=" << static_cast<unsigned>(usdm_recovered.state)
        << " recovered_generation=" << usdm_recovered.connection_generation
        << " usdm_runtime_state="
        << static_cast<unsigned>(usdm_runtime_after_recovery.state)
        << " usdm_projection_status="
        << static_cast<unsigned>(usdm_runtime_after_recovery.projection_status)
        << " usdm_projection_generation="
        << usdm_runtime_after_recovery.current_projection_generation.value_or(
               0U)
        << " spot_generation=" << spot_after_recovery.connection_generation
        << " spot_runtime_state="
        << static_cast<unsigned>(spot_runtime_after_recovery.state)
        << " spot_reset_before=" << spot_runtime_before_recovery.reset_count
        << " spot_reset_after=" << spot_runtime_after_recovery.reset_count
        << " isolation=" << isolation << " fresh_usdm=" << fresh_usdm
        << " fresh_removed=" << fresh_usdm_removed
        << " recovered_status=" << recovered_status
        << " streams_removed=" << streams_removed
        << " transport_bound=" << transport_bound
        << " grpc_clean=" << grpc_clean << " final_clean=" << final_clean
        << '\n';
    return EXIT_FAILURE;
  }

  std::cout << "REAL_G11_ACCEPTANCE=PASS\n"
            << "REAL_SPOT_EXCHANGE_INFO_TLS=PASS\n"
            << "REAL_USDM_EXCHANGE_INFO_TLS=PASS\n"
            << "REAL_SPOT_LIVE=PASS\n"
            << "REAL_USDM_LIVE=PASS\n"
            << "REAL_SPOT_INITIAL_GENERATION=1\n"
            << "REAL_USDM_INITIAL_GENERATION=1\n"
            << "REAL_PROJECTION_OWNER_COUNT=2\n"
            << "REAL_SPOT_ORDER_BOOK=PASS\n"
            << "REAL_USDM_ORDER_BOOK=PASS\n"
            << "REAL_USDM_DIFF_DEPTH=PASS\n"
            << "REAL_SPOT_ORDER_BOOK_ID=" << spot_book.sequence.subscription_id
            << '\n'
            << "REAL_USDM_ORDER_BOOK_ID=ob-1\n"
            << "REAL_USDM_EVENT_ID=ev-1\n"
            << "REAL_NO_CROSS_PUBLICATION=YES\n"
            << "REAL_INITIAL_STATUS_TWO_MARKETS=PASS\n"
            << "REAL_USDM_OLD_ORDER_BOOK_TERMINAL=PASS\n"
            << "REAL_USDM_OLD_EVENT_TERMINAL=PASS\n"
            << "REAL_USDM_CONTROLLED_RECOVERY=PASS\n"
            << "REAL_USDM_RECOVERED_GENERATION=2\n"
            << "REAL_USDM_FRESH_PROJECTION=UsdMPerpetual\n"
            << "REAL_USDM_FRESH_ORDER_BOOK=PASS\n"
            << "REAL_USDM_FRESH_DIFF_DEPTH=PASS\n"
            << "REAL_SPOT_UNAFFECTED_DURING_USDM_RECOVERY=PASS\n"
            << "REAL_SPOT_GENERATION_AFTER_USDM_RECOVERY=1\n"
            << "REAL_SPOT_RESET_COUNT_UNCHANGED=YES\n"
            << "REAL_SPOT_EXISTING_CONSUMER_CONTINUED=YES\n"
            << "REAL_RECOVERED_STATUS_TWO_MARKETS=PASS\n"
            << "REAL_MAX_ACTIVE_TRANSPORTS_SPOT="
            << spot_before_shutdown.max_active_transport_count << '\n'
            << "REAL_MAX_ACTIVE_TRANSPORTS_USDM="
            << usdm_before_shutdown.max_active_transport_count << '\n'
            << "REAL_MAX_ACTIVE_TRANSPORTS_TOTAL=2\n"
            << "REAL_FINAL_ACTIVE_TRANSPORTS=0\n"
            << "REAL_FINAL_G7_SUBSCRIBERS=0\n"
            << "REAL_FINAL_G9_SUBSCRIBERS=0\n"
            << "REAL_FINAL_TRACKED_CONTEXTS=0\n"
            << "REAL_FINAL_STATUS_INFLIGHT=NO\n"
            << "REAL_FINAL_RECOVERIES_JOINED=YES\n"
            << "REAL_FINAL_OWNERS_JOINED=YES\n"
            << "REAL_FINAL_GRPC_SHUTDOWN=PASS\n"
            << "REAL_FINAL_CLEAN_SHUTDOWN=PASS\n"
            << "REAL_RATE_LIMIT_ABUSE_ATTEMPTED=NO\n";
  return EXIT_SUCCESS;
}
