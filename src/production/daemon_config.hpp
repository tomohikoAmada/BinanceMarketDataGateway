#pragma once

#include <iosfwd>
#include <string>
#include <variant>

namespace binance_market_data::gateway::production {

struct DaemonConfig final {
  std::string grpc_listen;
};

struct HelpRequested final {};

struct DaemonConfigError final {
  std::string message;
};

using DaemonConfigResult =
    std::variant<DaemonConfig, HelpRequested, DaemonConfigError>;

[[nodiscard]] DaemonConfigResult parse_daemon_config(int argc, char **argv);
void print_daemon_usage(std::ostream &output);

} // namespace binance_market_data::gateway::production
