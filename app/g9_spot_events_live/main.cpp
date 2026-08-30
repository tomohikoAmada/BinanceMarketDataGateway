#include "event_publication.hpp"
#include "grpc_service.hpp"
#include "planned_rotation.hpp"
#include "spot_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <chrono>
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
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g6 = binance_market_data::gateway::g6;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace wire = binance_market_data::gateway::v1;

inline constexpr auto kRpcDeadline = std::chrono::seconds{60};

struct SessionEvidence final {
  bool valid{true};
  bool accepted{false};
  bool terminal{false};
  bool crossed_generation{false};
  std::size_t event_count{0U};
  std::uint64_t next_sequence{1U};
  std::string subscription_id;
  std::optional<std::uint64_t> terminal_generation;
  grpc::Status status;
};

void print_network_error(const g4::NetworkError &error) {
  std::cerr << "NETWORK_ERROR stage=" << error.stage
            << " message=" << error.message;
  if (error.http_status.has_value()) {
    std::cerr << " http_status=" << *error.http_status;
  }
  if (error.retry_after.has_value()) {
    std::cerr << " retry_after=" << *error.retry_after;
  }
  std::cerr << '\n';
}

[[nodiscard]] const char *schema_for(common::Stream stream) {
  switch (stream) {
  case common::STREAM_DIFF_DEPTH:
    return g9::kDepthEventSchema;
  case common::STREAM_AGG_TRADE:
    return g9::kAggTradeEventSchema;
  case common::STREAM_BOOK_TICKER:
    return g9::kBookTickerEventSchema;
  default:
    return "";
  }
}

[[nodiscard]] wire::EventSubscriptionRequest
make_request(common::Stream stream, std::string request_id) {
  wire::EventSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g9::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(common::MARKET_SPOT);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(stream);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(schema_for(stream));
  return request;
}

[[nodiscard]] bool payload_matches(const wire::GatewayEventEnvelope &item,
                                   common::Stream stream) {
  const auto metadata_valid = [&] {
    if (stream == common::STREAM_DIFF_DEPTH && item.has_depth_update()) {
      return item.depth_update().metadata().schema_version() ==
                 g9::kDepthEventSchema &&
             item.depth_update().metadata().has_receive_time_utc_ns() &&
             item.depth_update().metadata().has_receive_monotonic_ns();
    }
    if (stream == common::STREAM_AGG_TRADE && item.has_agg_trade()) {
      return item.agg_trade().metadata().schema_version() ==
                 g9::kAggTradeEventSchema &&
             item.agg_trade().metadata().has_receive_time_utc_ns() &&
             item.agg_trade().metadata().has_receive_monotonic_ns();
    }
    if (stream == common::STREAM_BOOK_TICKER && item.has_book_ticker()) {
      return item.book_ticker().metadata().schema_version() ==
                 g9::kBookTickerEventSchema &&
             item.book_ticker().metadata().has_receive_time_utc_ns() &&
             item.book_ticker().metadata().has_receive_monotonic_ns() &&
             !item.book_ticker().metadata().has_exchange_event_time_ms();
    }
    return false;
  }();
  return metadata_valid;
}

[[nodiscard]] bool accept_canonical(SessionEvidence &evidence,
                                    const wire::GatewayEventEnvelope &item,
                                    const std::string &gateway_instance_id,
                                    std::uint64_t expected_generation,
                                    common::Stream stream) {
  if (item.has_envelope_metadata() || !item.has_delivery_metadata()) {
    return false;
  }
  const auto &metadata = item.delivery_metadata();
  if (metadata.protocol_version() != g7::kProtocolVersion ||
      metadata.gateway_instance_id() != gateway_instance_id ||
      metadata.session_sequence() != evidence.next_sequence ||
      metadata.publish_time_utc_ns() == 0U) {
    return false;
  }
  ++evidence.next_sequence;
  if (evidence.subscription_id.empty()) {
    evidence.subscription_id = metadata.subscription_id();
  } else if (evidence.subscription_id != metadata.subscription_id()) {
    return false;
  }
  if (item.has_subscription_accepted()) {
    evidence.accepted =
        metadata.session_sequence() == 1U &&
        !metadata.has_connection_generation() &&
        item.subscription_accepted()
                .negotiated_payload_schema_versions_size() == 1 &&
        item.subscription_accepted().negotiated_payload_schema_versions(0) ==
            schema_for(stream);
    return evidence.accepted;
  }
  if (payload_matches(item, stream)) {
    if (!metadata.has_connection_generation()) {
      return false;
    }
    if (metadata.connection_generation() != expected_generation) {
      evidence.crossed_generation = true;
      return false;
    }
    ++evidence.event_count;
    return true;
  }
  if (item.has_consumer_gap()) {
    evidence.terminal =
        item.consumer_gap().reason() ==
            common::CONSUMER_GAP_REASON_CONNECTION_GENERATION_CHANGED &&
        item.consumer_gap().recovery_action() ==
            common::RECOVERY_ACTION_RESUBSCRIBE &&
        metadata.has_connection_generation();
    if (metadata.has_connection_generation()) {
      evidence.terminal_generation = metadata.connection_generation();
      if (metadata.connection_generation() != expected_generation) {
        evidence.crossed_generation = true;
      }
    }
    return evidence.terminal;
  }
  return false;
}

[[nodiscard]] SessionEvidence run_finite_session(
    wire::BinanceMarketDataGatewayService::Stub &stub, common::Stream stream,
    std::uint64_t generation, std::size_t required_events,
    const std::string &gateway_instance_id, std::string request_id) {
  SessionEvidence evidence;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kRpcDeadline);
  auto reader = stub.SubscribeEvents(
      &context, make_request(stream, std::move(request_id)));
  wire::GatewayEventEnvelope item;
  while (evidence.valid && evidence.event_count < required_events &&
         reader->Read(&item)) {
    evidence.valid = accept_canonical(evidence, item, gateway_instance_id,
                                      generation, stream);
  }
  context.TryCancel();
  while (reader->Read(&item)) {
  }
  evidence.status = reader->Finish();
  evidence.valid = evidence.valid && evidence.accepted &&
                   evidence.event_count >= required_events &&
                   !evidence.crossed_generation &&
                   (evidence.status.ok() || evidence.status.error_code() ==
                                                grpc::StatusCode::CANCELLED);
  return evidence;
}

[[nodiscard]] bool wait_for_event_subscribers(g9::EventPublication &publication,
                                              std::size_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  do {
    if (publication.observe().active_subscriptions == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  } while (std::chrono::steady_clock::now() < deadline);
  return publication.observe().active_subscriptions == expected;
}

} // namespace

int main() {
  const auto exchange_info = g4::fetch_exchange_info_https();
  if (const auto *failure = std::get_if<g4::NetworkError>(&exchange_info)) {
    print_network_error(*failure);
    return EXIT_FAILURE;
  }
  const auto &exchange_response =
      std::get<g4::ExchangeInfoResponse>(exchange_info);
  const auto metadata = g4::parse_exchange_info(exchange_response.body);
  if (const auto *failure = std::get_if<g4::ProtocolError>(&metadata)) {
    std::cerr << "EXCHANGE_INFO_PARSE=FAIL field=" << failure->field
              << " message=" << failure->message << '\n';
    return EXIT_FAILURE;
  }

  g3::RuntimeClock clock = g4::sample_real_clock;
  const auto gateway_instance_id = g7::generate_gateway_instance_id();
  g3::MarketRuntime runtime{
      {256U, 256U}, clock, std::get<g4::SpotMetadata>(metadata).numeric_spec};
  g9::EventPublication publication{gateway_instance_id, clock};
  g5::RecoveryOptions recovery_options;
  recovery_options.transport.profile =
      g4::SpotTransportProfile::G9CombinedEvents;
  recovery_options.transport.normalized_event_sink =
      [&publication](std::shared_ptr<const g4::NormalizedSpotEvent> event,
                     std::uint64_t generation) {
        return publication.publish(event, generation) ==
                       g9::EventPublishResult::InvariantFailure
                   ? g4::NormalizedEventSinkResult::InvariantFailure
                   : g4::NormalizedEventSinkResult::Continue;
      };
  recovery_options.source_generation_lifecycle.open =
      [&publication](std::uint64_t generation) {
        return publication.open_generation(generation);
      };
  recovery_options.source_generation_lifecycle.quiesce =
      [&publication](std::uint64_t generation) {
        if (publication.observe().source_state ==
            g9::EventSourceState::Shutdown) {
          return true;
        }
        return publication.quiesce_generation(generation);
      };
  recovery_options.source_generation_lifecycle.close =
      [&publication](std::uint64_t generation,
                     g5::SourceGenerationCloseOutcome outcome) {
        if (outcome == g5::SourceGenerationCloseOutcome::GlobalShutdown) {
          publication.shutdown();
          return true;
        }
        if (outcome == g5::SourceGenerationCloseOutcome::Replacement) {
          return publication.close_generation_replaced(generation);
        }
        return publication.close_generation_permanently(generation);
      };

  g5::SpotRecovery recovery{runtime, clock, g6::production_policy(),
                            std::move(recovery_options)};
  if (recovery.start() != g5::RecoveryStartResult::Started) {
    std::cerr << "G9_RECOVERY_START=FAIL\n";
    return EXIT_FAILURE;
  }
  const auto initial = recovery.wait_for_generation_live(1U);
  if (initial.state != g5::RecoveryState::Live || initial.terminal ||
      initial.connection_generation != 1U) {
    if (initial.terminal_error.has_value()) {
      print_network_error(*initial.terminal_error);
    }
    recovery.stop();
    return EXIT_FAILURE;
  }

  g7::OrderBookGrpcServer server{runtime, publication, gateway_instance_id};
  if (!server.start("127.0.0.1:0")) {
    std::cerr << "G9_GRPC_START=FAIL\n";
    recovery.stop();
    return EXIT_FAILURE;
  }
  auto stub = wire::BinanceMarketDataGatewayService::NewStub(
      grpc::CreateChannel("127.0.0.1:" + std::to_string(server.selected_port()),
                          grpc::InsecureChannelCredentials()));

  const auto depth = run_finite_session(*stub, common::STREAM_DIFF_DEPTH, 1U,
                                        2U, gateway_instance_id, "g9-depth");
  const auto trade = run_finite_session(*stub, common::STREAM_AGG_TRADE, 1U, 2U,
                                        gateway_instance_id, "g9-trade");
  const auto ticker = run_finite_session(*stub, common::STREAM_BOOK_TICKER, 1U,
                                         2U, gateway_instance_id, "g9-ticker");
  const bool initial_subscribers_removed =
      wait_for_event_subscribers(publication, 0U);

  SessionEvidence old;
  grpc::ClientContext old_context;
  old_context.set_deadline(std::chrono::system_clock::now() + kRpcDeadline);
  auto old_reader = stub->SubscribeEvents(
      &old_context,
      make_request(common::STREAM_AGG_TRADE, "g9-recovery-generation-1"));
  wire::GatewayEventEnvelope item;
  while (old.valid && old.event_count < 1U && old_reader->Read(&item)) {
    old.valid = accept_canonical(old, item, gateway_instance_id, 1U,
                                 common::STREAM_AGG_TRADE);
  }
  const bool controlled_requested =
      old.valid && old.event_count >= 1U &&
      recovery.request_controlled_recovery_for_acceptance();
  while (controlled_requested && old.valid && !old.terminal &&
         old_reader->Read(&item)) {
    old.valid = accept_canonical(old, item, gateway_instance_id, 1U,
                                 common::STREAM_AGG_TRADE);
  }
  while (old_reader->Read(&item)) {
  }
  old.status = old_reader->Finish();
  const bool old_valid = old.valid && old.accepted && old.terminal &&
                         old.terminal_generation == 1U &&
                         !old.crossed_generation && old.status.ok();

  const auto second = recovery.wait_for_generation_live(2U);
  const bool generation_two = second.state == g5::RecoveryState::Live &&
                              !second.terminal &&
                              second.connection_generation == 2U &&
                              second.connection_id != initial.connection_id;
  const auto fresh =
      generation_two
          ? run_finite_session(*stub, common::STREAM_BOOK_TICKER, 2U, 1U,
                               gateway_instance_id, "g9-generation-2")
          : SessionEvidence{};
  const bool final_session_removed =
      wait_for_event_subscribers(publication, 0U);
  const auto before_shutdown = recovery.observe();

  server.shutdown();
  const auto final_grpc_contexts = server.service().tracked_context_count();
  const auto final_event_subscribers =
      publication.observe().active_subscriptions;
  const auto final_order_book_subscribers =
      runtime.observe().resident_subscription_count;
  recovery.stop();
  const auto final_recovery = recovery.observe();
  const auto final_runtime = runtime.observe();

  const bool passed =
      exchange_response.tls_verified && depth.valid && trade.valid &&
      ticker.valid && initial_subscribers_removed && controlled_requested &&
      old_valid && generation_two && fresh.valid && final_session_removed &&
      before_shutdown.max_active_transport_count <= 1U &&
      final_recovery.active_transport_count == 0U &&
      final_grpc_contexts == 0U && final_event_subscribers == 0U &&
      final_order_book_subscribers == 0U &&
      final_runtime.state == g3::RuntimeState::Stopped;
  if (!passed) {
    std::cerr << "REAL_G9_ACCEPTANCE=FAIL"
              << " depth=" << depth.valid << " trade=" << trade.valid
              << " ticker=" << ticker.valid
              << " controlled=" << controlled_requested << " old=" << old_valid
              << " generation_two=" << generation_two
              << " second_state=" << static_cast<unsigned>(second.state)
              << " second_generation=" << second.connection_generation
              << " second_terminal=" << second.terminal
              << " connection_changed="
              << (second.connection_id != initial.connection_id)
              << " fresh=" << (generation_two && fresh.valid)
              << " event_subscribers=" << final_event_subscribers
              << " grpc_contexts=" << final_grpc_contexts << '\n';
    if (second.terminal_error.has_value()) {
      print_network_error(*second.terminal_error);
    }
    return EXIT_FAILURE;
  }

  std::cout << "REAL_DIFF_DEPTH=PASS\n"
            << "REAL_AGG_TRADE=PASS\n"
            << "REAL_BOOK_TICKER=PASS\n"
            << "REAL_CONTROLLED_RECOVERY=PASS\n"
            << "REAL_OLD_SESSION_CROSSED_GEN2=NO\n"
            << "MAX_ACTIVE_TRANSPORTS="
            << before_shutdown.max_active_transport_count << '\n'
            << "FINAL_TRANSPORT_ACTIVE=NO\n"
            << "FINAL_EVENT_SUBSCRIBERS=" << final_event_subscribers << '\n'
            << "FINAL_ORDER_BOOK_SUBSCRIBERS=" << final_order_book_subscribers
            << '\n'
            << "FINAL_GRPC_CONTEXTS=" << final_grpc_contexts << '\n'
            << "FINAL_RUNTIME_STATE=Stopped\n";
  return EXIT_SUCCESS;
}
