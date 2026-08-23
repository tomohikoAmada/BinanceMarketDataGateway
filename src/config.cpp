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

  const auto is_continuation = [](unsigned char byte) noexcept {
    return byte >= 0x80U && byte <= 0xBFU;
  };

  std::size_t offset = 0U;
  while (offset < value.size()) {
    const auto lead = static_cast<unsigned char>(value[offset]);
    if (lead <= 0x7FU) {
      if (lead <= 0x20U || lead == 0x7FU) {
        return false;
      }
      ++offset;
      continue;
    }

    if (lead >= 0xC2U && lead <= 0xDFU) {
      if (offset + 1U >= value.size() ||
          !is_continuation(static_cast<unsigned char>(value[offset + 1U]))) {
        return false;
      }
      offset += 2U;
      continue;
    }

    if (lead >= 0xE0U && lead <= 0xEFU) {
      if (offset + 2U >= value.size()) {
        return false;
      }
      const auto second = static_cast<unsigned char>(value[offset + 1U]);
      const auto third = static_cast<unsigned char>(value[offset + 2U]);
      if (!is_continuation(third) || (lead == 0xE0U && second < 0xA0U) ||
          (lead == 0xEDU && second >= 0xA0U) || !is_continuation(second)) {
        return false;
      }
      offset += 3U;
      continue;
    }

    if (lead >= 0xF0U && lead <= 0xF4U) {
      if (offset + 3U >= value.size()) {
        return false;
      }
      const auto second = static_cast<unsigned char>(value[offset + 1U]);
      const auto third = static_cast<unsigned char>(value[offset + 2U]);
      const auto fourth = static_cast<unsigned char>(value[offset + 3U]);
      if (!is_continuation(third) || !is_continuation(fourth) ||
          (lead == 0xF0U && second < 0x90U) ||
          (lead == 0xF4U && second > 0x8FU) || !is_continuation(second)) {
        return false;
      }
      offset += 4U;
      continue;
    }

    // Invalid UTF-8 lead bytes C0/C1, isolated continuation bytes, and lead
    // bytes above F4 cannot encode Unicode scalar values.
    return false;
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
        "symbol must be a non-empty strict UTF-8 scalar identity without "
        "ASCII C0/whitespace or DEL bytes");
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
