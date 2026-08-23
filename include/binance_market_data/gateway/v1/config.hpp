#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace binance_market_data::gateway::v1 {

namespace detail {
struct ConfigValidationAccess;
}

enum class Venue : std::uint8_t {
  Binance,
};

enum class Market : std::uint8_t {
  Spot,
  UsdMPerpetual,
};

enum class ConfigErrorCode : std::uint8_t {
  InvalidVenue,
  InvalidMarket,
  InvalidSymbol,
  InvalidEndpoint,
  InvalidQueueCapacity,
};

struct ConfigError final {
  ConfigErrorCode code;
  std::string field;
  std::string message;

  friend bool operator==(const ConfigError &, const ConfigError &) = default;
};

struct ListenEndpoint final {
  std::string host;
  std::uint16_t port;

  friend bool operator==(const ListenEndpoint &,
                         const ListenEndpoint &) = default;
};

using EndpointResult = std::variant<ListenEndpoint, ConfigError>;

// Parses host:port without resolving or binding the endpoint. Bracketed IPv6 is
// accepted.
[[nodiscard]] EndpointResult parse_listen_endpoint(std::string_view value);

struct GatewayConfig final {
  Venue venue;
  Market market;
  std::string symbol;
  ListenEndpoint grpc_listen;
  std::uint32_t queue_capacity;

  friend bool operator==(const GatewayConfig &,
                         const GatewayConfig &) = default;
};

// A distinct result type prevents the lifecycle seam from being constructed
// from an unvalidated raw configuration by accident.
struct ValidatedGatewayConfig final {
  ValidatedGatewayConfig(const ValidatedGatewayConfig &) = default;
  ValidatedGatewayConfig(ValidatedGatewayConfig &&) noexcept = default;
  ValidatedGatewayConfig &operator=(const ValidatedGatewayConfig &) = default;
  ValidatedGatewayConfig &
  operator=(ValidatedGatewayConfig &&) noexcept = default;
  ~ValidatedGatewayConfig() = default;

  [[nodiscard]] const GatewayConfig &get() const & noexcept { return value_; }
  [[nodiscard]] GatewayConfig take() && noexcept;

  friend bool operator==(const ValidatedGatewayConfig &left,
                         const ValidatedGatewayConfig &right) {
    return left.value_ == right.value_;
  }

private:
  explicit ValidatedGatewayConfig(GatewayConfig config)
      : value_{std::move(config)} {}

  GatewayConfig value_;
  friend struct detail::ConfigValidationAccess;
};

using ConfigResult = std::variant<ValidatedGatewayConfig, ConfigError>;

// Validates the small Phase A configuration surface. Symbols remain opaque
// exact strict-UTF-8 scalar identities: validation rejects empty values,
// malformed UTF-8, Unicode surrogate code points, and ASCII C0/DEL/whitespace
// bytes without normalizing or case-folding the original string.
// This performs no I/O or runtime setup.
[[nodiscard]] ConfigResult validate_config(GatewayConfig config);

} // namespace binance_market_data::gateway::v1
