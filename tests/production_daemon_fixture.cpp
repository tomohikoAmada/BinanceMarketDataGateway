#include "daemon_config.hpp"
#include "daemon_runtime.hpp"
#include "production_test_support.hpp"
#include "termination_signals.hpp"

#include <cstdlib>
#include <iostream>
#include <utility>
#include <variant>

namespace production = binance_market_data::gateway::production;
namespace support = production::test_support;

int main(int argc, char **argv) {
  const auto parsed = production::parse_daemon_config(argc, argv);
  if (std::holds_alternative<production::HelpRequested>(parsed)) {
    production::print_daemon_usage(std::cout);
    return EXIT_SUCCESS;
  }
  if (const auto *error = std::get_if<production::DaemonConfigError>(&parsed)) {
    std::cerr << "configuration_error=" << error->message << '\n';
    return 2;
  }

  production::TerminationSignals signals;
  auto configured = support::gateway_options();
  configured.gateway.allow_ephemeral_listen_for_testing = false;
  return production::run_production_service(
      std::get<production::DaemonConfig>(parsed), support::metadata(), signals,
      std::cout, std::cerr, std::move(configured.gateway));
}
