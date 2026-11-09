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

void expect_valid_symbol(const std::string &symbol, const char *description,
                         int &failures) {
  auto config = valid_config();
  config.symbol = symbol;
  const auto result = validate_config(std::move(config));
  expect(std::holds_alternative<ValidatedGatewayConfig>(result), description,
         failures);
  if (std::holds_alternative<ValidatedGatewayConfig>(result)) {
    expect(std::get<ValidatedGatewayConfig>(result).get().symbol == symbol,
           "valid symbol bytes are preserved exactly", failures);
  }
}

void expect_invalid_symbol(const std::string &symbol, const char *description,
                           int &failures) {
  auto config = valid_config();
  config.symbol = symbol;
  const auto result = validate_config(std::move(config));
  expect(std::holds_alternative<ConfigError>(result), description, failures);
  if (std::holds_alternative<ConfigError>(result)) {
    expect(std::get<ConfigError>(result).code == ConfigErrorCode::InvalidSymbol,
           "invalid symbol has typed error", failures);
  }
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
  expect(std::holds_alternative<ConfigError>(parse_listen_endpoint(":50051")),
         "endpoint rejects an empty host", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("127.0.0.1:")),
      "endpoint rejects an empty port", failures);
  expect(std::holds_alternative<ConfigError>(
             parse_listen_endpoint("127.0.0.1:not-a-port")),
         "endpoint rejects a non-numeric port", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("127.0.0.1:0")),
      "port zero is rejected", failures);
  expect(std::holds_alternative<ListenEndpoint>(
             parse_listen_endpoint("127.0.0.1:65535")),
         "maximum port is accepted", failures);
  expect(std::holds_alternative<ConfigError>(
             parse_listen_endpoint("127.0.0.1:65536")),
         "port above the maximum is rejected", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("::1:50051")),
      "unbracketed IPv6 is rejected", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("[::1:50051")),
      "unterminated bracketed host is rejected", failures);
  expect(
      std::holds_alternative<ConfigError>(parse_listen_endpoint("[::1]50051")),
      "bracketed host requires a colon before the port", failures);
  expect(std::holds_alternative<ConfigError>(
             parse_listen_endpoint("[::1]:50051:extra")),
         "bracketed host rejects an invalid port suffix", failures);
  expect(std::holds_alternative<ListenEndpoint>(
             parse_listen_endpoint(std::string(253U, 'a') + ":1")),
         "253-byte host token is accepted", failures);
  expect(std::holds_alternative<ConfigError>(
             parse_listen_endpoint(std::string(254U, 'a') + ":1")),
         "254-byte host token is rejected", failures);

  const auto validated = validate_config(valid_config());
  expect(std::holds_alternative<ValidatedGatewayConfig>(validated),
         "valid config is accepted", failures);

  expect_valid_symbol("A", "one-byte ASCII symbol is accepted", failures);
  expect_valid_symbol("btc-usdt", "lowercase opaque ASCII symbol is accepted",
                      failures);
  expect_valid_symbol(std::string(21U, 'X'),
                      "symbol longer than twenty bytes is accepted", failures);
  expect_valid_symbol("\xEF\xBC\x91", "fullwidth digit symbol is accepted",
                      failures);
  expect_valid_symbol("\xE6\xB5\x8B\xE8\xAF\x95",
                      "Chinese UTF-8 symbol is accepted", failures);
  expect_valid_symbol("\xC2\x80", "U+0080 is accepted", failures);
  expect_valid_symbol("\xC2\xA2", "U+00A2 is accepted", failures);
  expect_valid_symbol("\xC2\xA0", "NBSP is accepted", failures);
  expect_valid_symbol("\xE3\x80\x80", "U+3000 is accepted", failures);
  expect_valid_symbol("\xF4\x8F\xBF\xBF", "U+10FFFF is accepted", failures);

  expect_invalid_symbol("", "empty symbol is rejected", failures);
  for (unsigned int codepoint = 0U; codepoint <= 0x1FU; ++codepoint) {
    expect_invalid_symbol(std::string(1U, static_cast<char>(codepoint)),
                          "ASCII C0 control byte in symbol is rejected",
                          failures);
  }
  expect_invalid_symbol(" ", "ASCII space in symbol is rejected", failures);
  expect_invalid_symbol("\t", "ASCII tab in symbol is rejected", failures);
  expect_invalid_symbol("\n", "ASCII newline in symbol is rejected", failures);
  expect_invalid_symbol(std::string(1U, static_cast<char>(0x7FU)),
                        "ASCII DEL in symbol is rejected", failures);
  expect_invalid_symbol("\x80", "isolated continuation byte is rejected",
                        failures);
  expect_invalid_symbol("\xC2", "truncated two-byte sequence is rejected",
                        failures);
  expect_invalid_symbol("\xE2\x82", "truncated three-byte sequence is rejected",
                        failures);
  expect_invalid_symbol("\xF0\x90\x80",
                        "truncated four-byte sequence is rejected", failures);
  expect_invalid_symbol("\xC0\x80", "overlong two-byte sequence is rejected",
                        failures);
  expect_invalid_symbol("\xE0\x80\x80",
                        "overlong three-byte sequence is rejected", failures);
  expect_invalid_symbol("\xF0\x80\x80\x80",
                        "overlong four-byte sequence is rejected", failures);
  expect_invalid_symbol("\xE2\x28\xA1",
                        "bad second continuation byte is rejected", failures);
  expect_invalid_symbol("\xE2\x82\x28",
                        "bad trailing continuation byte is rejected", failures);
  expect_invalid_symbol("\xED\xA0\x80",
                        "UTF-16 surrogate code point is rejected", failures);
  expect_invalid_symbol("\xF4\x90\x80\x80",
                        "code point above U+10FFFF is rejected", failures);
  expect_invalid_symbol("\xF5\x80\x80\x80",
                        "invalid leading byte above U+F4 is rejected",
                        failures);

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
