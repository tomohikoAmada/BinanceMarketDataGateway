#include "binance_market_data/gateway/v1/config.hpp"
#include "binance_market_data/gateway/v1/lifecycle.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace {

using namespace binance_market_data::gateway::v1;

static_assert(!std::is_constructible_v<Foundation, GatewayConfig>);
static_assert(!std::is_constructible_v<ValidatedGatewayConfig, GatewayConfig>);

void expect(bool condition, const char *expression, int &failures) {
  if (!condition) {
    std::cerr << "FAIL: " << expression << '\n';
    ++failures;
  }
}

GatewayConfig valid_config() {
  return GatewayConfig{Venue::Binance, Market::Spot, "BTCUSDT",
                       ListenEndpoint{"127.0.0.1", 50051U}, 1024U};
}

ValidatedGatewayConfig validated_config() {
  auto result = validate_config(valid_config());
  return std::get<ValidatedGatewayConfig>(std::move(result));
}

} // namespace

int main() {
  int failures = 0;

  const auto endpoint = parse_listen_endpoint("[::1]:50051");
  expect(std::holds_alternative<ListenEndpoint>(endpoint),
         "IPv6 endpoint parses", failures);
  if (std::holds_alternative<ListenEndpoint>(endpoint)) {
    const auto &parsed = std::get<ListenEndpoint>(endpoint);
    expect(parsed.host == "::1", "IPv6 host is unbracketed", failures);
    expect(parsed.port == 50051U, "IPv6 port is preserved", failures);
  }
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("127.0.0.1")),
      "endpoint requires a port", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("127.0.0.1:0")),
      "port zero is rejected", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("::1:50051")),
      "unbracketed IPv6 is rejected", failures);

  const auto validated = validate_config(valid_config());
  expect(std::holds_alternative<ValidatedGatewayConfig>(validated),
         "valid config is accepted", failures);

  auto non_ascii_symbol = valid_config();
  non_ascii_symbol.symbol = "\xE6\xB5\x8B\xE8\xAF\x95";
  const auto non_ascii_symbol_result =
      validate_config(std::move(non_ascii_symbol));
  expect(
      std::holds_alternative<ValidatedGatewayConfig>(non_ascii_symbol_result),
      "non-ASCII UTF-8 symbol is accepted as opaque identity", failures);

  auto invalid_symbol = valid_config();
  invalid_symbol.symbol.clear();
  const auto invalid_symbol_result = validate_config(std::move(invalid_symbol));
  expect(std::holds_alternative<ConfigError>(invalid_symbol_result),
         "empty symbol is rejected", failures);
  if (std::holds_alternative<ConfigError>(invalid_symbol_result)) {
    expect(std::get<ConfigError>(invalid_symbol_result).code ==
               ConfigErrorCode::InvalidSymbol,
           "symbol error has typed code", failures);
  }

  for (const std::string whitespace_symbol : {" ", "BTC USDT"}) {
    auto whitespace_config = valid_config();
    whitespace_config.symbol = whitespace_symbol;
    expect(std::holds_alternative<ConfigError>(
               validate_config(std::move(whitespace_config))),
           "ASCII whitespace in symbol is rejected", failures);
  }

  auto invalid_queue = valid_config();
  invalid_queue.queue_capacity = 0U;
  expect(std::holds_alternative<ConfigError>(
             validate_config(std::move(invalid_queue))),
         "zero queue capacity is rejected", failures);

  auto invalid_endpoint = valid_config();
  invalid_endpoint.grpc_listen.port = 0U;
  const auto invalid_endpoint_result =
      validate_config(std::move(invalid_endpoint));
  expect(std::holds_alternative<ConfigError>(invalid_endpoint_result),
         "invalid endpoint cannot produce a foundation config", failures);

  Foundation foundation{validated_config()};
  expect(foundation.state() == LifecycleState::Constructed,
         "foundation starts constructed", failures);
  expect(std::holds_alternative<LifecycleState>(foundation.start()),
         "foundation starts synchronously", failures);
  expect(foundation.state() == LifecycleState::Running, "state becomes running",
         failures);
  const auto repeated_start = foundation.start();
  expect(std::holds_alternative<LifecycleError>(repeated_start),
         "repeated start is rejected", failures);
  if (std::holds_alternative<LifecycleError>(repeated_start)) {
    expect(std::get<LifecycleError>(repeated_start).code ==
               LifecycleErrorCode::AlreadyRunning,
           "repeated start has typed error", failures);
  }
  expect(std::holds_alternative<LifecycleState>(foundation.stop()),
         "foundation stops synchronously", failures);
  expect(foundation.state() == LifecycleState::Stopped, "state becomes stopped",
         failures);
  const auto repeated_stop = foundation.stop();
  expect(std::holds_alternative<LifecycleError>(repeated_stop),
         "repeated stop is rejected", failures);

  if (failures != 0) {
    return EXIT_FAILURE;
  }
  std::cout << "foundation tests passed\n";
  return EXIT_SUCCESS;
}
