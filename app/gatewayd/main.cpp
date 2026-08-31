#include "daemon_config.hpp"
#include "daemon_runtime.hpp"
#include "production_metadata.hpp"
#include "termination_signals.hpp"

#include <cstdlib>
#include <exception>
#include <iostream>
#include <variant>

namespace {

namespace production = binance_market_data::gateway::production;

} // namespace

int main(int argc, char **argv) {
  const auto parsed = production::parse_daemon_config(argc, argv);
  if (std::holds_alternative<production::HelpRequested>(parsed)) {
    production::print_daemon_usage(std::cout);
    return EXIT_SUCCESS;
  }
  if (const auto *error = std::get_if<production::DaemonConfigError>(&parsed)) {
    std::cerr << "configuration_error=" << error->message << '\n';
    production::print_daemon_usage(std::cerr);
    return 2;
  }

  try {
    production::TerminationSignals signals;
    std::cout << "gateway_state=starting stage=metadata products=2\n"
              << std::flush;
    const auto metadata = production::acquire_production_metadata();
    if (const auto *error = std::get_if<production::MetadataError>(&metadata)) {
      std::cerr << "metadata=failed stage="
                << production::to_string(error->stage)
                << " message=" << error->message << '\n';
      return EXIT_FAILURE;
    }
    if (signals.requested()) {
      std::cout << "gateway_state=stopped startup_result=stop-requested "
                   "contexts=0 transports=0 subscriptions=0 "
                   "owners_joined=yes\n";
      return EXIT_SUCCESS;
    }
    return production::run_production_service(
        std::get<production::DaemonConfig>(parsed),
        std::get<production::ProductionMetadata>(metadata), signals, std::cout,
        std::cerr);
  } catch (const std::exception &error) {
    std::cerr << "gateway_fatal=" << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
