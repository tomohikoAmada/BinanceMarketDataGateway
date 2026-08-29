#include "grpc_service.hpp"
#include "planned_rotation.hpp"
#include "spot_protocol.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <variant>

namespace {

namespace common = binance_market_data::common::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g5 = binance_market_data::gateway::g5;
namespace g6 = binance_market_data::gateway::g6;
namespace g7 = binance_market_data::gateway::g7;
namespace wire = binance_market_data::gateway::v1;

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

[[nodiscard]] wire::OrderBookSubscriptionRequest make_request() {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id("g7-real-acceptance");
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(common::MARKET_SPOT);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] bool canonical_item(const wire::OrderBookStreamItem &item,
                                  const std::string &gateway_instance_id,
                                  const std::string &subscription_id,
                                  std::uint64_t expected_sequence) {
  return !item.has_envelope_metadata() && item.has_delivery_metadata() &&
         item.delivery_metadata().protocol_version() == g7::kProtocolVersion &&
         item.delivery_metadata().gateway_instance_id() ==
             gateway_instance_id &&
         item.delivery_metadata().subscription_id() == subscription_id &&
         item.delivery_metadata().session_sequence() == expected_sequence &&
         item.delivery_metadata().publish_time_utc_ns() != 0U;
}

[[nodiscard]] bool wait_for_subscriber_removal(g3::MarketRuntime &runtime) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  while (std::chrono::steady_clock::now() < deadline) {
    if (runtime.observe().resident_subscription_count == 0U) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  return runtime.observe().resident_subscription_count == 0U;
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
  g3::MarketRuntime runtime{
      {256U, 256U}, clock, std::get<g4::SpotMetadata>(metadata).numeric_spec};
  g5::SpotRecovery recovery{runtime, clock, g6::production_policy()};
  if (recovery.start() != g5::RecoveryStartResult::Started) {
    std::cerr << "G7_RECOVERY_START=FAIL\n";
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

  const auto gateway_instance_id = g7::generate_gateway_instance_id();
  g7::OrderBookGrpcServer server{runtime, gateway_instance_id};
  if (!server.start("127.0.0.1:0")) {
    std::cerr << "G7_GRPC_START=FAIL\n";
    recovery.stop();
    return EXIT_FAILURE;
  }

  auto stub = wire::BinanceMarketDataGatewayService::NewStub(
      grpc::CreateChannel("127.0.0.1:" + std::to_string(server.selected_port()),
                          grpc::InsecureChannelCredentials()));
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds{45});
  const auto request = make_request();
  auto reader = stub->SubscribeOrderBook(&context, request);

  wire::OrderBookStreamItem accepted;
  wire::OrderBookStreamItem snapshot;
  wire::OrderBookStreamItem update;
  bool stream_valid =
      reader->Read(&accepted) && accepted.has_subscription_accepted();
  const auto subscription_id =
      stream_valid ? accepted.delivery_metadata().subscription_id()
                   : std::string{};
  stream_valid =
      stream_valid &&
      canonical_item(accepted, gateway_instance_id, subscription_id, 1U) &&
      !accepted.delivery_metadata().has_connection_generation() &&
      accepted.subscription_accepted().schema_version() ==
          g7::kSubscriptionAcceptedSchema &&
      accepted.subscription_accepted().gateway_instance_id() ==
          gateway_instance_id &&
      accepted.subscription_accepted().subscription_id() == subscription_id &&
      accepted.subscription_accepted()
              .negotiated_payload_schema_versions_size() == 2;

  if (stream_valid) {
    stream_valid =
        reader->Read(&snapshot) && snapshot.has_snapshot() &&
        canonical_item(snapshot, gateway_instance_id, subscription_id, 2U) &&
        snapshot.delivery_metadata().has_connection_generation() &&
        snapshot.delivery_metadata().connection_generation() ==
            initial.connection_generation &&
        snapshot.snapshot().schema_version() == g7::kSnapshotSchema &&
        snapshot.snapshot().symbol() == "BTCUSDT" &&
        snapshot.snapshot().synchronized() &&
        snapshot.snapshot().last_update_id() != 0U &&
        !snapshot.snapshot().bids().empty() &&
        !snapshot.snapshot().asks().empty();
  }
  if (stream_valid) {
    stream_valid =
        reader->Read(&update) && update.has_depth_update() &&
        canonical_item(update, gateway_instance_id, subscription_id, 3U) &&
        update.delivery_metadata().has_connection_generation() &&
        update.delivery_metadata().connection_generation() ==
            initial.connection_generation &&
        update.depth_update().metadata().symbol() == "BTCUSDT" &&
        update.depth_update().metadata().schema_version() ==
            g7::kUpdateSchema &&
        update.depth_update().final_update_id() >
            snapshot.snapshot().last_update_id();
  }

  context.TryCancel();
  wire::OrderBookStreamItem ignored;
  while (reader->Read(&ignored)) {
  }
  const auto finish = reader->Finish();
  const bool client_disconnected =
      finish.ok() || finish.error_code() == grpc::StatusCode::CANCELLED;
  const bool subscriber_removed = wait_for_subscriber_removal(runtime);

  server.shutdown();
  const bool grpc_shutdown = !server.service().admission_open() &&
                             server.service().tracked_context_count() == 0U &&
                             runtime.observe().publication_shutdown;
  recovery.stop();
  const bool gateway_shutdown =
      runtime.observe().state == g3::RuntimeState::Stopped;

  if (!stream_valid || !client_disconnected || !subscriber_removed ||
      !grpc_shutdown || !gateway_shutdown) {
    std::cerr << "REAL_G7_ACCEPTANCE=FAIL"
              << " stream_valid=" << stream_valid
              << " client_disconnected=" << client_disconnected
              << " subscriber_removed=" << subscriber_removed
              << " grpc_shutdown=" << grpc_shutdown
              << " gateway_shutdown=" << gateway_shutdown << '\n';
    return EXIT_FAILURE;
  }

  std::cout << "EXCHANGE_INFO_FETCH=PASS\n"
            << "TLS_VERIFY="
            << (exchange_response.tls_verified ? "PASS" : "FAIL") << '\n'
            << "REAL_G7_ACCEPTANCE=PASS\n"
            << "REAL_INITIAL_GENERATION=" << initial.connection_generation
            << '\n'
            << "REAL_ACCEPTED_SEQUENCE=1\n"
            << "REAL_SNAPSHOT_SEQUENCE=2\n"
            << "REAL_FIRST_UPDATE_SEQUENCE=3\n"
            << "REAL_SESSION_SEQUENCE_CONTIGUOUS=YES\n"
            << "REAL_SNAPSHOT_GENERATION="
            << snapshot.delivery_metadata().connection_generation() << '\n'
            << "REAL_UPDATE_GENERATION="
            << update.delivery_metadata().connection_generation() << '\n'
            << "REAL_POST_SNAPSHOT_UPDATE=PASS\n"
            << "REAL_CLIENT_DISCONNECT=PASS\n"
            << "REAL_SUBSCRIBER_REMOVED=YES\n"
            << "REAL_GRPC_SHUTDOWN=PASS\n"
            << "REAL_GATEWAY_SHUTDOWN=PASS\n"
            << "REAL_SLOW_CLIENT_OVERFLOW_ATTEMPTED=NO\n"
            << "REAL_RATE_LIMIT_ABUSE_ATTEMPTED=NO\n";
  return EXIT_SUCCESS;
}
