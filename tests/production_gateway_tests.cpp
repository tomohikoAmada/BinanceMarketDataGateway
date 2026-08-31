#include "daemon_config.hpp"
#include "production_metadata.hpp"
#include "production_test_support.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace production = binance_market_data::gateway::production;
namespace support = production::test_support;
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

#define REQUIRE(condition) require((condition), #condition)

template <typename Predicate> void require_eventually(Predicate predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{2};
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw TestFailure{"bounded readiness deadline expired"};
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
}

[[nodiscard]] production::ProductionGateway
make_gateway(production::GatewayOptions options,
             std::string listen = "127.0.0.1:0") {
  return {support::numeric_spec(), support::numeric_spec(),
          support::fixed_clock(),  "gw-production-test",
          std::move(listen),       std::move(options)};
}

void require_fully_stopped(production::ProductionGateway &gateway) {
  const auto final = gateway.observe();
  REQUIRE(final.state == production::GatewayState::Stopped);
  REQUIRE(final.tracked_contexts == 0U);
  REQUIRE(final.spot_recovery.active_transport_count == 0U);
  REQUIRE(final.usdm_recovery.active_transport_count == 0U);
  REQUIRE(final.spot_recovery.state == g5::RecoveryState::Stopped);
  REQUIRE(final.usdm_recovery.state == g5::RecoveryState::Stopped);
  REQUIRE(final.spot_runtime.owner_joined);
  REQUIRE(final.usdm_runtime.owner_joined);
  REQUIRE(final.spot_runtime.resident_subscription_count == 0U);
  REQUIRE(final.usdm_runtime.resident_subscription_count == 0U);
  REQUIRE(final.spot_runtime.pending_admission_count == 0U);
  REQUIRE(final.usdm_runtime.pending_admission_count == 0U);
  REQUIRE(final.spot_events.active_subscriptions == 0U);
  REQUIRE(final.usdm_events.active_subscriptions == 0U);
}

[[nodiscard]] std::unique_ptr<wire::BinanceMarketDataGatewayService::Stub>
stub_for(const production::GatewayObservation &observation) {
  return wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(observation.selected_port),
      grpc::InsecureChannelCredentials()));
}

[[nodiscard]] wire::OrderBookSubscriptionRequest
book_request(common::Market market, std::string id) {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id(std::move(id));
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(market);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] wire::EventSubscriptionRequest
event_request(common::Market market, std::string id) {
  wire::EventSubscriptionRequest request;
  request.set_request_id(std::move(id));
  request.set_schema_version(g9::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(market);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(common::STREAM_DIFF_DEPTH);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(g9::kDepthEventSchema);
  return request;
}

[[nodiscard]] wire::GatewayStatusSnapshot
status(wire::BinanceMarketDataGatewayService::Stub &stub,
       std::string request_id) {
  wire::GatewayStatusRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g10::kStatusRequestSchema);
  wire::GatewayStatusSnapshot response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds{2});
  const auto result = stub.GetGatewayStatus(&context, request, &response);
  REQUIRE(result.ok());
  return response;
}

class OccupiedPort final {
public:
  OccupiedPort() {
    descriptor_ = socket(AF_INET, SOCK_STREAM, 0);
    if (descriptor_ < 0) {
      throw std::runtime_error{"socket failed"};
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(descriptor_, reinterpret_cast<sockaddr *>(&address),
             sizeof(address)) != 0 ||
        listen(descriptor_, 1) != 0) {
      close(descriptor_);
      throw std::runtime_error{"loopback bind failed"};
    }
    socklen_t size = sizeof(address);
    if (getsockname(descriptor_, reinterpret_cast<sockaddr *>(&address),
                    &size) != 0) {
      close(descriptor_);
      throw std::runtime_error{"getsockname failed"};
    }
    port_ = ntohs(address.sin_port);
  }

  ~OccupiedPort() { close(descriptor_); }
  [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

private:
  int descriptor_{-1};
  std::uint16_t port_{0U};
};

void normal_start_stop() {
  auto configured = support::gateway_options();
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::Serving);
  const auto serving = gateway.observe();
  REQUIRE(serving.state == production::GatewayState::Serving);
  REQUIRE(serving.selected_port > 0);
  REQUIRE(serving.spot_recovery.state == g5::RecoveryState::Live);
  REQUIRE(serving.usdm_recovery.state == g5::RecoveryState::Live);
  REQUIRE(serving.spot_runtime.state == g3::RuntimeState::Live);
  REQUIRE(serving.usdm_runtime.state == g3::RuntimeState::Live);
  REQUIRE(serving.spot_runtime.ingress_capacity == 64U);
  REQUIRE(serving.spot_runtime.bootstrap_capacity == 64U);
  REQUIRE(serving.usdm_runtime.ingress_capacity == 64U);
  REQUIRE(serving.usdm_runtime.bootstrap_capacity == 64U);
  REQUIRE(serving.spot_recovery.max_active_transport_count == 1U);
  REQUIRE(serving.usdm_recovery.max_active_transport_count == 1U);
  gateway.stop();
  require_fully_stopped(gateway);
}

void initial_spot_failure_rolls_back() {
  auto configured = support::gateway_options(support::AttemptMode::Terminal,
                                             support::AttemptMode::Live);
  auto spot_state = configured.spot;
  auto usdm_state = configured.usdm;
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::SpotInitialFailure);
  require_fully_stopped(gateway);
  REQUIRE(spot_state->active.load() == 0U);
  REQUIRE(usdm_state->active.load() == 0U);
}

void initial_usdm_failure_rolls_back() {
  auto configured = support::gateway_options(support::AttemptMode::Live,
                                             support::AttemptMode::Terminal);
  auto spot_state = configured.spot;
  auto usdm_state = configured.usdm;
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::UsdMInitialFailure);
  require_fully_stopped(gateway);
  REQUIRE(spot_state->active.load() == 0U);
  REQUIRE(usdm_state->active.load() == 0U);
}

void grpc_bind_failure_rolls_back() {
  OccupiedPort occupied;
  auto configured = support::gateway_options();
  configured.gateway.allow_ephemeral_listen_for_testing = false;
  auto gateway = make_gateway(std::move(configured.gateway),
                              "127.0.0.1:" + std::to_string(occupied.port()));
  REQUIRE(gateway.start() == production::StartResult::GrpcBindFailed);
  require_fully_stopped(gateway);
}

void post_start_failure_isolated() {
  auto configured = support::gateway_options();
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::Serving);
  auto stub = stub_for(gateway.observe());

  REQUIRE(
      gateway.products_for_testing().usdm().runtime().submit_depth_update(
          support::make_update(common::MARKET_USD_M_PERPETUAL, 1U, 102U, true),
          g3::SourceProvenance{1U}) == g3::AdmissionResult::Accepted);
  const auto terminal =
      gateway.products_for_testing().usdm().recovery().wait_until_terminal();
  REQUIRE(terminal.terminal);

  const auto serving = gateway.observe();
  REQUIRE(serving.state == production::GatewayState::Serving);
  REQUIRE(serving.spot_recovery.state == g5::RecoveryState::Live);
  REQUIRE(serving.usdm_recovery.terminal);
  const auto snapshot = status(*stub, "status-after-usdm-failure");
  REQUIRE(snapshot.markets_size() == 2);
  REQUIRE(snapshot.markets(0).state() == common::STREAM_LIFECYCLE_STATE_LIVE);
  REQUIRE(snapshot.markets(1).state() != common::STREAM_LIFECYCLE_STATE_LIVE);

  grpc::ClientContext spot_context;
  spot_context.set_deadline(std::chrono::system_clock::now() +
                            std::chrono::seconds{2});
  auto spot_reader = stub->SubscribeOrderBook(
      &spot_context,
      book_request(common::MARKET_SPOT, "spot-after-usdm-failure"));
  wire::OrderBookStreamItem spot_item;
  REQUIRE(spot_reader->Read(&spot_item));
  REQUIRE(spot_item.has_subscription_accepted());
  REQUIRE(spot_reader->Read(&spot_item));
  REQUIRE(spot_item.has_snapshot());
  spot_context.TryCancel();
  while (spot_reader->Read(&spot_item)) {
  }
  static_cast<void>(spot_reader->Finish());
  gateway.stop();
  require_fully_stopped(gateway);
}

void active_stream_shutdown() {
  auto configured = support::gateway_options();
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::Serving);
  auto stub = stub_for(gateway.observe());

  grpc::ClientContext spot_book_context;
  grpc::ClientContext usdm_book_context;
  grpc::ClientContext spot_event_context;
  grpc::ClientContext usdm_event_context;
  auto spot_book = stub->SubscribeOrderBook(
      &spot_book_context, book_request(common::MARKET_SPOT, "spot-book"));
  auto usdm_book = stub->SubscribeOrderBook(
      &usdm_book_context,
      book_request(common::MARKET_USD_M_PERPETUAL, "usdm-book"));
  auto spot_event = stub->SubscribeEvents(
      &spot_event_context, event_request(common::MARKET_SPOT, "spot-event"));
  auto usdm_event = stub->SubscribeEvents(
      &usdm_event_context,
      event_request(common::MARKET_USD_M_PERPETUAL, "usdm-event"));

  wire::OrderBookStreamItem book_item;
  wire::GatewayEventEnvelope event_item;
  REQUIRE(spot_book->Read(&book_item));
  REQUIRE(usdm_book->Read(&book_item));
  REQUIRE(spot_event->Read(&event_item));
  REQUIRE(usdm_event->Read(&event_item));
  require_eventually(
      [&gateway] { return gateway.observe().tracked_contexts == 4U; });
  const auto before = gateway.observe();
  REQUIRE(before.spot_runtime.resident_subscription_count == 1U);
  REQUIRE(before.usdm_runtime.resident_subscription_count == 1U);
  REQUIRE(before.spot_events.active_subscriptions == 1U);
  REQUIRE(before.usdm_events.active_subscriptions == 1U);
  REQUIRE(status(*stub, "status-near-shutdown").markets_size() == 2);

  gateway.stop();
  while (spot_book->Read(&book_item)) {
  }
  while (usdm_book->Read(&book_item)) {
  }
  while (spot_event->Read(&event_item)) {
  }
  while (usdm_event->Read(&event_item)) {
  }
  static_cast<void>(spot_book->Finish());
  static_cast<void>(usdm_book->Finish());
  static_cast<void>(spot_event->Finish());
  static_cast<void>(usdm_event->Finish());
  require_fully_stopped(gateway);
}

void repeated_stop() {
  auto configured = support::gateway_options();
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::Serving);
  gateway.request_stop();
  gateway.request_stop();
  gateway.stop();
  gateway.stop();
  require_fully_stopped(gateway);
}

void startup_stop_request_rolls_back() {
  auto configured = support::gateway_options(support::AttemptMode::NeverLive,
                                             support::AttemptMode::NeverLive);
  auto spot_state = configured.spot;
  auto usdm_state = configured.usdm;
  auto gateway = make_gateway(std::move(configured.gateway));
  auto start =
      std::async(std::launch::async, [&gateway] { return gateway.start(); });
  require_eventually([&] {
    return spot_state->starts.load() != 0U && usdm_state->starts.load() != 0U;
  });
  gateway.request_stop();
  REQUIRE(start.wait_for(std::chrono::seconds{2}) == std::future_status::ready);
  REQUIRE(start.get() == production::StartResult::StopRequested);
  require_fully_stopped(gateway);
}

void initial_startup_deadline_is_bounded() {
  auto configured = support::gateway_options(support::AttemptMode::NeverLive,
                                             support::AttemptMode::NeverLive);
  configured.gateway.initial_startup_timeout = std::chrono::milliseconds{25};
  auto gateway = make_gateway(std::move(configured.gateway));
  const auto started_at = std::chrono::steady_clock::now();
  REQUIRE(gateway.start() == production::StartResult::InitialStartupTimeout);
  REQUIRE(std::chrono::steady_clock::now() - started_at <
          std::chrono::seconds{2});
  require_fully_stopped(gateway);
}

void context_bound_preserved() {
  static_assert(g7::kMaximumGrpcTrackedContexts == 48U);
  auto configured = support::gateway_options();
  auto gateway = make_gateway(std::move(configured.gateway));
  REQUIRE(gateway.start() == production::StartResult::Serving);
  REQUIRE(gateway.observe().context_limit == 48U);
  gateway.stop();
}

void metadata_failures_are_ordered() {
  bool usdm_called = false;
  production::MetadataSources spot_failure;
  spot_failure.spot_fetch = [] {
    return binance_market_data::gateway::g4::ExchangeInfoResult{
        binance_market_data::gateway::g4::NetworkError{
            binance_market_data::gateway::g4::NetworkErrorCode::Dns, "spot",
            "failed", std::nullopt, std::nullopt}};
  };
  spot_failure.usdm_fetch = [&usdm_called] {
    usdm_called = true;
    return binance_market_data::gateway::g4::ExchangeInfoResult{
        binance_market_data::gateway::g4::ExchangeInfoResponse{}};
  };
  const auto spot =
      production::acquire_production_metadata(std::move(spot_failure));
  REQUIRE(std::get<production::MetadataError>(spot).stage ==
          production::MetadataStage::SpotFetch);
  REQUIRE(!usdm_called);

  production::MetadataSources usdm_failure;
  usdm_failure.spot_fetch = [] {
    return binance_market_data::gateway::g4::ExchangeInfoResult{
        binance_market_data::gateway::g4::ExchangeInfoResponse{
            R"json({"timezone":"UTC","symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAssetPrecision":1,"quotePrecision":17,"isSpotTradingAllowed":true,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]})json",
            true}};
  };
  usdm_failure.usdm_fetch = [] {
    return binance_market_data::gateway::g4::ExchangeInfoResult{
        binance_market_data::gateway::g4::NetworkError{
            binance_market_data::gateway::g4::NetworkErrorCode::Timeout, "usdm",
            "failed", std::nullopt, std::nullopt}};
  };
  const auto usdm =
      production::acquire_production_metadata(std::move(usdm_failure));
  REQUIRE(std::get<production::MetadataError>(usdm).stage ==
          production::MetadataStage::UsdMFetch);
}

void cli_is_fixed_and_fail_closed() {
  char program[] = "bmd-gatewayd";
  char listen[] = "--grpc-listen";
  char endpoint[] = "127.0.0.1:50051";
  char stale[] = "--symbol";
  char symbol[] = "BTCUSDT";
  char *valid_argv[]{program, listen, endpoint};
  char *invalid_argv[]{program, stale, symbol};
  REQUIRE(std::holds_alternative<production::DaemonConfig>(
      production::parse_daemon_config(3, valid_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::parse_daemon_config(3, invalid_argv)));
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"NORMAL_PRODUCTION_START_STOP", normal_start_stop},
      {"INITIAL_SPOT_FAILURE_ROLLBACK", initial_spot_failure_rolls_back},
      {"INITIAL_USDM_FAILURE_ROLLBACK", initial_usdm_failure_rolls_back},
      {"GRPC_BIND_FAILURE_ROLLBACK", grpc_bind_failure_rolls_back},
      {"POST_START_SINGLE_MARKET_FAILURE_ISOLATED",
       post_start_failure_isolated},
      {"ACTIVE_FOUR_STREAM_SHUTDOWN", active_stream_shutdown},
      {"REPEATED_STOP", repeated_stop},
      {"STARTUP_STOP_REQUEST_ROLLBACK", startup_stop_request_rolls_back},
      {"INITIAL_STARTUP_DEADLINE_BOUNDED", initial_startup_deadline_is_bounded},
      {"CONTEXT_48_PRESERVED", context_bound_preserved},
      {"METADATA_FAILURES_ORDERED", metadata_failures_are_ordered},
      {"FIXED_CLI_FAILS_CLOSED", cli_is_fixed_and_fail_closed},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &error) {
      std::cerr << name << "=FAIL " << error.what() << '\n';
      return 1;
    }
  }
  return 0;
}
