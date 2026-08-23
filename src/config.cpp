#include "binance_market_data/gateway/v1/config.hpp"

#include <charconv>
#include <cstddef>
#include <limits>
#include <system_error>
#include <utility>

namespace binance_market_data::gateway::v1 {

namespace detail {

struct ConfigValidationAccess final {
  static ValidatedGatewayConfig create(GatewayConfig config) {
    return ValidatedGatewayConfig{std::move(config)};
  }
};

} // namespace detail

namespace {

constexpr std::size_t kMaxHostLength = 253U;

[[nodiscard]] ConfigError error(ConfigErrorCode code, std::string field,
                                std::string message) {
  return ConfigError{code, std::move(field), std::move(message)};
}

[[nodiscard]] bool valid_symbol(std::string_view value) noexcept {
  if (value.empty()) {
    return false;
  }
  for (const char character : value) {
    const auto byte = static_cast<unsigned char>(character);
    const bool ascii_whitespace = byte == ' ' || byte == '\t' || byte == '\n' ||
                                  byte == '\r' || byte == '\f' || byte == '\v';
    if (byte == 0U || byte < 0x20U || byte == 0x7FU || ascii_whitespace) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool valid_host(std::string_view value) noexcept {
  if (value.empty() || value.size() > kMaxHostLength) {
    return false;
  }
  for (const char character : value) {
    const bool hostname_character = (character >= 'a' && character <= 'z') ||
                                    (character >= 'A' && character <= 'Z') ||
                                    (character >= '0' && character <= '9') ||
                                    character == '.' || character == '-' ||
                                    character == ':';
    if (!hostname_character) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] EndpointResult parse_port(std::string_view host,
                                        std::string_view port) {
  if (!valid_host(host) || port.empty()) {
    return error(ConfigErrorCode::InvalidEndpoint, "grpc-listen",
                 "endpoint must contain a valid host and port");
  }

  std::uint32_t parsed_port = 0U;
  const auto [end, parse_error] =
      std::from_chars(port.data(), port.data() + port.size(), parsed_port);
  if (parse_error != std::errc{} || end != port.data() + port.size() ||
      parsed_port == 0U ||
      parsed_port > std::numeric_limits<std::uint16_t>::max()) {
    return error(ConfigErrorCode::InvalidEndpoint, "grpc-listen",
                 "port must be an integer in the range 1..65535");
  }
  return ListenEndpoint{std::string{host},
                        static_cast<std::uint16_t>(parsed_port)};
}

[[nodiscard]] ConfigResult validate_endpoint(const GatewayConfig &config) {
  const auto &endpoint = config.grpc_listen;
  if (!valid_host(endpoint.host) || endpoint.port == 0U) {
    return error(ConfigErrorCode::InvalidEndpoint, "grpc-listen",
                 "endpoint must contain a valid host and port");
  }
  return detail::ConfigValidationAccess::create(config);
}

} // namespace

EndpointResult parse_listen_endpoint(std::string_view value) {
  if (value.empty()) {
    return error(ConfigErrorCode::InvalidEndpoint, "grpc-listen",
                 "endpoint is required");
  }

  if (value.front() == '[') {
    const std::size_t close_bracket = value.find(']');
    if (close_bracket == std::string_view::npos ||
        close_bracket + 1U >= value.size() ||
        value[close_bracket + 1U] != ':') {
      return error(ConfigErrorCode::InvalidEndpoint, "grpc-listen",
                   "IPv6 endpoints must use [host]:port");
    }
    return parse_port(value.substr(1U, close_bracket - 1U),
                      value.substr(close_bracket + 2U));
  }

  const std::size_t colon = value.rfind(':');
  if (colon == std::string_view::npos || value.find(':') != colon) {
    return error(ConfigErrorCode::InvalidEndpoint, "grpc-listen",
                 "endpoint must use host:port or [ipv6]:port");
  }
  return parse_port(value.substr(0U, colon), value.substr(colon + 1U));
}

ConfigResult validate_config(GatewayConfig config) {
  if (config.venue != Venue::Binance) {
    return error(ConfigErrorCode::InvalidVenue, "venue",
                 "only binance is supported");
  }
  if (config.market != Market::Spot && config.market != Market::UsdMPerpetual) {
    return error(ConfigErrorCode::InvalidMarket, "market",
                 "market must be spot or usd-m-perpetual");
  }
  if (!valid_symbol(config.symbol)) {
    return error(
        ConfigErrorCode::InvalidSymbol, "symbol",
        "symbol must be non-empty and contain no NUL/control/ASCII whitespace");
  }
  if (config.queue_capacity == 0U) {
    return error(ConfigErrorCode::InvalidQueueCapacity, "queue-capacity",
                 "queue capacity must be greater than zero");
  }
  return validate_endpoint(config);
}

GatewayConfig ValidatedGatewayConfig::take() && noexcept {
  return std::move(value_);
}

} // namespace binance_market_data::gateway::v1
