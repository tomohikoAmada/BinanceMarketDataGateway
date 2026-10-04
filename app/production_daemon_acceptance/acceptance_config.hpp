#pragma once

#include "daemon_config.hpp"

#include <unistd.h>

#include <charconv>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::production::acceptance {

namespace g11 = binance_market_data::gateway::g11;
namespace production = binance_market_data::gateway::production;

class AcceptanceFailure final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

inline void require(bool condition, std::string_view message) {
  if (!condition) {
    throw AcceptanceFailure{std::string{message}};
  }
}

template <typename Integer>
[[nodiscard]] std::optional<Integer> parse_unsigned(std::string_view text) {
  if (text.empty()) {
    return std::nullopt;
  }
  Integer value{};
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
    return std::nullopt;
  }
  return value;
}

struct Options final {
  std::string daemon_path;
  std::string grpc_target;
  std::string config_path;
  std::uint16_t grpc_port{};
};

[[nodiscard]] inline Options parse_options(int argc, char **argv) {
  std::optional<std::string> daemon_path;
  std::optional<std::string> grpc_target;
  std::optional<std::string> config_path;
  for (int index = 1; index < argc; index += 2) {
    const std::string_view option{argv[index]};
    if (option != "--daemon" && option != "--grpc-target" &&
        option != "--config") {
      throw AcceptanceFailure{"unknown option: " + std::string{option}};
    }
    if (index + 1 >= argc) {
      throw AcceptanceFailure{"missing value for " + std::string{option}};
    }
    const std::string_view value{argv[index + 1]};
    if (value.empty() || value.starts_with("--")) {
      throw AcceptanceFailure{"empty or missing value for " +
                              std::string{option}};
    }
    auto &destination = option == "--daemon"        ? daemon_path
                        : option == "--grpc-target" ? grpc_target
                                                    : config_path;
    if (destination.has_value()) {
      throw AcceptanceFailure{"duplicate option: " + std::string{option}};
    }
    destination = value;
  }
  require(daemon_path.has_value(), "missing --daemon");
  require(grpc_target.has_value(), "missing --grpc-target");
  require(config_path.has_value(), "missing --config");

  const auto colon = grpc_target->rfind(':');
  require(colon != std::string::npos && colon != 0U &&
              colon + 1U < grpc_target->size(),
          "--grpc-target must include a nonempty host and numeric port");
  const auto parsed_port = parse_unsigned<unsigned int>(
      std::string_view{*grpc_target}.substr(colon + 1U));
  require(parsed_port.has_value() && *parsed_port > 0U &&
              *parsed_port <= 65'535U,
          "--grpc-target port is invalid");

  std::error_code filesystem_error;
  const auto resolved_path =
      std::filesystem::canonical(*daemon_path, filesystem_error);
  require(!filesystem_error, "--daemon path cannot be resolved");
  filesystem_error.clear();
  const auto regular =
      std::filesystem::is_regular_file(resolved_path, filesystem_error);
  require(!filesystem_error && regular, "--daemon path is not a regular file");
  require(access(resolved_path.c_str(), X_OK) == 0,
          "--daemon path is not executable");

  const auto loaded = production::load_daemon_config(*config_path);
  if (const auto *failure =
          std::get_if<production::DaemonConfigError>(&loaded)) {
    throw AcceptanceFailure{"--config: " + failure->message};
  }
  require(std::holds_alternative<production::DaemonConfig>(loaded),
          "--config did not produce a daemon configuration");
  const auto &config = std::get<production::DaemonConfig>(loaded);
  require(config.market_keys ==
              std::vector<g11::MarketKey>{g11::spot_btcusdt_key(),
                                          g11::usdm_btcusdt_key()},
          "this acceptance client requires exactly Spot and USD-M BTCUSDT");
  const auto listen_port =
      parse_unsigned<unsigned int>(std::string_view{config.grpc_listen}.substr(
          config.grpc_listen.rfind(':') + 1U));
  require(listen_port.has_value() && *listen_port == *parsed_port,
          "--config listen port does not match --grpc-target");
  return {resolved_path.string(), *grpc_target, *config_path,
          static_cast<std::uint16_t>(*parsed_port)};
}

[[nodiscard]] inline std::optional<std::string_view>
field_value(std::string_view line, std::string_view key) {
  auto position = line.find(key);
  while (position != std::string_view::npos && position != 0U &&
         line[position - 1U] != ' ') {
    position = line.find(key, position + 1U);
  }
  if (position == std::string_view::npos) {
    return std::nullopt;
  }
  const auto value_start = position + key.size();
  const auto value_end = line.find(' ', value_start);
  const auto value = line.substr(value_start, value_end - value_start);
  if (value.empty()) {
    return std::nullopt;
  }
  return value;
}

struct ServingIdentity final {
  std::uint16_t grpc_port{};
  std::uint64_t spot_generation{};
  std::uint64_t usdm_generation{};
  std::string gateway_instance_id;
};

[[nodiscard]] inline ServingIdentity
parse_serving_line(std::string_view line, std::uint16_t expected_port) {
  const auto port_text = field_value(line, "grpc_port=");
  const auto products_text = field_value(line, "products=");
  const auto instance_text = field_value(line, "gateway_instance_id=");
  require(port_text.has_value() && products_text.has_value() &&
              instance_text.has_value(),
          "serving line is missing required identity fields");
  const auto port = parse_unsigned<unsigned int>(*port_text);
  const auto products = parse_unsigned<unsigned int>(*products_text);
  require(port.has_value() && *port == expected_port,
          "serving grpc_port does not match --grpc-target");
  require(products.has_value() && *products == 2U,
          "serving line must describe the two-product acceptance profile");
  return {static_cast<std::uint16_t>(*port), 0U, 0U,
          std::string{*instance_text}};
}

} // namespace binance_market_data::gateway::production::acceptance
