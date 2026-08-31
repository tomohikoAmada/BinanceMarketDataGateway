#include "daemon_config.hpp"

#include <binance_market_data/gateway/v1/config.hpp>

#include <ostream>
#include <string_view>
#include <utility>

namespace binance_market_data::gateway::production {

DaemonConfigResult parse_daemon_config(int argc, char **argv) {
  if (argc == 2 && std::string_view{argv[1]} == "--help") {
    return HelpRequested{};
  }

  std::string listen;
  bool listen_seen = false;
  for (int index = 1; index < argc; ++index) {
    const std::string_view option{argv[index]};
    if (option != "--grpc-listen") {
      return DaemonConfigError{"unknown option: " + std::string{option}};
    }
    if (listen_seen) {
      return DaemonConfigError{"duplicate option: --grpc-listen"};
    }
    if (index + 1 >= argc || std::string_view{argv[index + 1]}.empty()) {
      return DaemonConfigError{"missing value for option: --grpc-listen"};
    }
    listen = argv[++index];
    listen_seen = true;
  }
  if (!listen_seen) {
    return DaemonConfigError{"--grpc-listen is required"};
  }

  const auto endpoint = v1::parse_listen_endpoint(listen);
  if (const auto *error = std::get_if<v1::ConfigError>(&endpoint)) {
    return DaemonConfigError{"invalid --grpc-listen [" + error->field +
                             "]: " + error->message};
  }
  return DaemonConfig{std::move(listen)};
}

void print_daemon_usage(std::ostream &output) {
  output << "Usage: bmd-gatewayd --grpc-listen HOST:PORT\n"
            "\n"
            "Serves the fixed production products:\n"
            "  BINANCE/SPOT/BTCUSDT\n"
            "  BINANCE/USD_M_PERPETUAL/BTCUSDT\n";
}

} // namespace binance_market_data::gateway::production
