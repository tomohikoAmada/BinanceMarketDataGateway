#include "binance_market_data/gateway/v1/config.hpp"
#include "binance_market_data/gateway/v1/lifecycle.hpp"

#include <charconv>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace {

using binance_market_data::gateway::v1::ConfigError;
using binance_market_data::gateway::v1::Foundation;
using binance_market_data::gateway::v1::GatewayConfig;
using binance_market_data::gateway::v1::Market;
using binance_market_data::gateway::v1::ValidatedGatewayConfig;
using binance_market_data::gateway::v1::Venue;

void print_usage(std::ostream &output) {
  output
      << "Usage: bmd-gatewayd --venue binance --market <spot|usd-m-perpetual> "
         "--symbol SYMBOL --grpc-listen HOST:PORT --queue-capacity COUNT\n";
}

void print_error(const ConfigError &error) {
  std::cerr << "configuration error [" << error.field << "]: " << error.message
            << '\n';
}

bool take_value(int &index, int argc, char **argv, std::string &value) {
  if (index + 1 >= argc) {
    return false;
  }
  value = argv[++index];
  return !value.empty();
}

int parse_queue_capacity(std::string_view value, std::uint32_t &result) {
  std::uint64_t parsed = 0U;
  const auto [end, parse_error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (parse_error != std::errc{} || end != value.data() + value.size() ||
      parsed > std::numeric_limits<std::uint32_t>::max()) {
    return 1;
  }
  result = static_cast<std::uint32_t>(parsed);
  return 0;
}

int fail_usage(std::string_view message) {
  std::cerr << message << '\n';
  print_usage(std::cerr);
  return 2;
}

} // namespace

int main(int argc, char **argv) {
  if (argc == 2 && std::string_view{argv[1]} == "--help") {
    print_usage(std::cout);
    return 0;
  }

  std::string venue;
  std::string market;
  std::string symbol;
  std::string listen;
  std::string queue_capacity_text;
  bool venue_seen = false;
  bool market_seen = false;
  bool symbol_seen = false;
  bool listen_seen = false;
  bool queue_seen = false;

  for (int index = 1; index < argc; ++index) {
    const std::string_view option{argv[index]};
    std::string *target = nullptr;
    bool *seen = nullptr;
    if (option == "--venue") {
      target = &venue;
      seen = &venue_seen;
    } else if (option == "--market") {
      target = &market;
      seen = &market_seen;
    } else if (option == "--symbol") {
      target = &symbol;
      seen = &symbol_seen;
    } else if (option == "--grpc-listen") {
      target = &listen;
      seen = &listen_seen;
    } else if (option == "--queue-capacity") {
      target = &queue_capacity_text;
      seen = &queue_seen;
    } else {
      return fail_usage("unknown option: " + std::string{option});
    }
    if (*seen) {
      return fail_usage("duplicate option: " + std::string{option});
    }
    if (!take_value(index, argc, argv, *target)) {
      return fail_usage("missing value for option: " + std::string{option});
    }
    *seen = true;
  }

  if (!venue_seen || !market_seen || !symbol_seen || !listen_seen ||
      !queue_seen) {
    return fail_usage("all five configuration options are required");
  }
  if (venue != "binance") {
    return fail_usage("venue must be binance");
  }

  Market parsed_market;
  if (market == "spot") {
    parsed_market = Market::Spot;
  } else if (market == "usd-m-perpetual") {
    parsed_market = Market::UsdMPerpetual;
  } else {
    return fail_usage("market must be spot or usd-m-perpetual");
  }

  std::uint32_t queue_capacity = 0U;
  if (parse_queue_capacity(queue_capacity_text, queue_capacity) != 0) {
    return fail_usage("queue-capacity must be an unsigned integer");
  }

  auto endpoint =
      binance_market_data::gateway::v1::parse_listen_endpoint(listen);
  if (!std::holds_alternative<binance_market_data::gateway::v1::ListenEndpoint>(
          endpoint)) {
    print_error(std::get<ConfigError>(endpoint));
    return 2;
  }

  GatewayConfig config{
      Venue::Binance, parsed_market, std::move(symbol),
      std::get<binance_market_data::gateway::v1::ListenEndpoint>(
          std::move(endpoint)),
      queue_capacity};
  auto validated =
      binance_market_data::gateway::v1::validate_config(std::move(config));
  if (!std::holds_alternative<ValidatedGatewayConfig>(validated)) {
    print_error(std::get<ConfigError>(validated));
    return 2;
  }

  Foundation foundation{std::get<ValidatedGatewayConfig>(std::move(validated))};
  const auto started = foundation.start();
  if (!std::holds_alternative<binance_market_data::gateway::v1::LifecycleState>(
          started)) {
    std::cerr << "failed to start foundation\n";
    return 1;
  }
  std::cout << "gateway foundation state=running symbol="
            << foundation.config().symbol << '\n';

  // Phase A has no transport loop. Stop immediately after proving the
  // synchronous seam.
  const auto stopped = foundation.stop();
  if (!std::holds_alternative<binance_market_data::gateway::v1::LifecycleState>(
          stopped)) {
    std::cerr << "failed to stop foundation\n";
    return 1;
  }
  std::cout << "gateway foundation state=stopped\n";
  return 0;
}
