#include "grpc_service.hpp"

#include <binance_market_data/common/v1/enums.pb.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace {

namespace common = binance_market_data::common::v1;
namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g5 = binance_market_data::gateway::g5;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace market = binance_market_data::market::v1;
namespace wire = binance_market_data::gateway::v1;

#if !defined(BMD_GATEWAY_G11_ENABLED)
static_assert(g7::kMaximumGrpcTrackedContexts == 24U);
#endif

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

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(3U);
  if (!price.has_value() || !quantity.has_value()) {
    throw TestFailure{"invalid numeric spec"};
  }
  return {*price, *quantity};
}

[[nodiscard]] g3::RuntimeClock clock() {
  return [] { return g3::ClockSample{0U, 1'000'000'000U}; };
}

void submit_pending_snapshot(g3::MarketRuntime &runtime) {
  market::ExchangeDepthSnapshot snapshot;
  snapshot.set_venue(common::VENUE_BINANCE);
  snapshot.set_market(common::MARKET_SPOT);
  snapshot.set_symbol("BTCUSDT");
  snapshot.set_schema_version("exchange-depth-snapshot.v1");
  snapshot.set_producer("g10-grpc-test");
  snapshot.set_producer_version("1.0.0");
  snapshot.set_request_id("g10-grpc-snapshot");
  snapshot.set_last_update_id(100U);
  auto *bid = snapshot.add_bids();
  bid->set_price("100.00");
  bid->set_quantity("1.000");
  auto *ask = snapshot.add_asks();
  ask->set_price("101.00");
  ask->set_quantity("1.000");
  REQUIRE_EQ(runtime.submit_snapshot(std::move(snapshot)),
             g3::AdmissionResult::Accepted);
}

[[nodiscard]] wire::GatewayStatusRequest valid_request() {
  wire::GatewayStatusRequest request;
  request.set_request_id("status-1");
  request.set_schema_version(g10::kStatusRequestSchema);
  return request;
}

void unary_status_and_validation() {
  g3::MarketRuntime runtime{{8U, 8U}, clock(), numeric_spec()};
  g5::SpotRecovery recovery{runtime, clock()};
  g9::EventPublication events{"gw-grpc-status", clock()};
  g7::OrderBookGrpcServer server{runtime, recovery, events, clock(),
                                 "gw-grpc-status"};
  REQUIRE(server.start("127.0.0.1:0"));
  auto stub = wire::BinanceMarketDataGatewayService::NewStub(
      grpc::CreateChannel("127.0.0.1:" + std::to_string(server.selected_port()),
                          grpc::InsecureChannelCredentials()));

  wire::GatewayStatusSnapshot response;
  grpc::ClientContext context;
  const auto request = valid_request();
  const auto status = stub->GetGatewayStatus(&context, request, &response);
  REQUIRE(status.ok());
  REQUIRE_EQ(response.schema_version(), g10::kStatusSnapshotSchema);
  REQUIRE_EQ(response.gateway_instance_id(), "gw-grpc-status");
  REQUIRE_EQ(response.markets_size(), 1);
  REQUIRE_EQ(response.markets(0).state(),
             common::STREAM_LIFECYCLE_STATE_ACCEPTED);
  REQUIRE_EQ(response.markets(0).active_subscription_count(), 0U);
  REQUIRE_EQ(server.service().tracked_context_count(), 0U);

  wire::GatewayStatusRequest invalid;
  invalid.set_request_id("bad id");
  invalid.set_schema_version(g10::kStatusRequestSchema);
  grpc::ClientContext invalid_context;
  wire::GatewayStatusSnapshot invalid_response;
  const auto invalid_status =
      stub->GetGatewayStatus(&invalid_context, invalid, &invalid_response);
  REQUIRE_EQ(invalid_status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);

  server.shutdown();
  REQUIRE(!server.service().status_inflight());
  REQUIRE(!server.service().admission_open());
}

void legacy_service_is_unavailable_for_status() {
  g3::MarketRuntime runtime{{8U, 8U}, clock(), numeric_spec()};
  g7::OrderBookGrpcService service{runtime, "gw-legacy"};
  grpc::ServerContext context;
  const auto request = valid_request();
  wire::GatewayStatusSnapshot response;
  const auto status = service.GetGatewayStatus(&context, &request, &response);
  REQUIRE_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
}

void status_slot_is_nonblocking_and_single() {
  g3::MarketRuntime runtime{
      {8U, 8U}, clock(), numeric_spec(), g3::RuntimeTestOptions{true}};
  g5::SpotRecovery recovery{runtime, clock()};
  g9::EventPublication events{"gw-slot", clock()};
  g7::OrderBookGrpcService service{runtime, recovery, events, clock(),
                                   "gw-slot"};
  REQUIRE(service.prepare_status_start());
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  submit_pending_snapshot(runtime);

  const auto request = valid_request();
  wire::GatewayStatusSnapshot first_response;
  grpc::ServerContext first_context;
  grpc::Status first_status;
  std::thread first{[&] {
    first_status =
        service.GetGatewayStatus(&first_context, &request, &first_response);
  }};
  for (std::size_t tries = 0U; tries < 200U && !service.status_inflight();
       ++tries) {
    std::this_thread::yield();
  }
  REQUIRE(service.status_inflight());

  wire::GatewayStatusSnapshot second_response;
  grpc::ServerContext second_context;
  const auto started_at = std::chrono::steady_clock::now();
  const auto second_status =
      service.GetGatewayStatus(&second_context, &request, &second_response);
  const auto elapsed = std::chrono::steady_clock::now() - started_at;
  REQUIRE_EQ(second_status.error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);
  REQUIRE(elapsed < std::chrono::seconds{1});

  runtime.release_owner_for_testing();
  first.join();
  REQUIRE(first_status.ok());
  REQUIRE(!service.status_inflight());
  REQUIRE_EQ(service.tracked_context_count(), 0U);
  runtime.stop();
  events.shutdown();
}

void shutdown_closes_new_status_admission() {
  g3::MarketRuntime runtime{
      {8U, 8U}, clock(), numeric_spec(), g3::RuntimeTestOptions{true}};
  g5::SpotRecovery recovery{runtime, clock()};
  g9::EventPublication events{"gw-shutdown", clock()};
  g7::OrderBookGrpcService service{runtime, recovery, events, clock(),
                                   "gw-shutdown"};
  REQUIRE(service.prepare_status_start());
  REQUIRE_EQ(runtime.start(), g3::StartResult::Started);
  submit_pending_snapshot(runtime);

  const auto request = valid_request();
  wire::GatewayStatusSnapshot first_response;
  grpc::ServerContext first_context;
  grpc::Status first_status;
  std::thread first{[&] {
    first_status =
        service.GetGatewayStatus(&first_context, &request, &first_response);
  }};
  for (std::size_t tries = 0U; tries < 200U && !service.status_inflight();
       ++tries) {
    std::this_thread::yield();
  }
  REQUIRE(service.status_inflight());

  std::thread shutdown{[&service] { service.begin_shutdown(); }};
  for (std::size_t tries = 0U; tries < 200U && service.admission_open();
       ++tries) {
    std::this_thread::yield();
  }
  REQUIRE(!service.admission_open());

  wire::GatewayStatusSnapshot second_response;
  grpc::ServerContext second_context;
  const auto second_status =
      service.GetGatewayStatus(&second_context, &request, &second_response);
  REQUIRE_EQ(second_status.error_code(), grpc::StatusCode::UNAVAILABLE);

  runtime.release_owner_for_testing();
  first.join();
  shutdown.join();
  REQUIRE(first_status.ok());
  REQUIRE(!service.status_inflight());
  REQUIRE_EQ(service.tracked_context_count(), 0U);
  runtime.stop();
}

} // namespace

int main() {
  try {
    unary_status_and_validation();
    legacy_service_is_unavailable_for_status();
    status_slot_is_nonblocking_and_single();
    shutdown_closes_new_status_admission();
  } catch (const std::exception &) {
    return 1;
  }
  return 0;
}
