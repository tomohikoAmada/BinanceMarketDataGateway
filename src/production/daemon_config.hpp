#pragma once

#include "market_registry.hpp"

#include <cstddef>
#include <iosfwd>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::production {

inline constexpr std::size_t kMaximumDaemonConfigBytes = 64U * 1024U;

struct DaemonConfig final {
  std::string grpc_listen;
  std::vector<g11::MarketKey> market_keys;
};

struct HelpRequested final {};

struct DaemonConfigError final {
  std::string message;
};

using DaemonConfigResult =
    std::variant<DaemonConfig, HelpRequested, DaemonConfigError>;

[[nodiscard]] DaemonConfigResult parse_daemon_config(int argc, char **argv);
[[nodiscard]] DaemonConfigResult
load_daemon_config(std::string_view config_path);
void print_daemon_usage(std::ostream &output);

} // namespace binance_market_data::gateway::production
