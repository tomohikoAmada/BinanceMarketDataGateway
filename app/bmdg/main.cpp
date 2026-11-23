#include "production/daemon_config.hpp"
#include "production/daemon_runtime.hpp"
#include "production/production_metadata.hpp"
#include "production/termination_signals.hpp"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <thread>
#include <variant>
namespace production = binance_market_data::gateway::production;

namespace {} // namespace

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
    const auto &config = std::get<production::DaemonConfig>(parsed);
    if (!production::performance_baseline_preflight(config, std::cerr)) {
      return EXIT_FAILURE;
    }
    production::TerminationSignals signals;
    std::cout << "gateway_state=starting stage=metadata products="
              << config.market_keys.size() << '\n'
              << std::flush;
    constexpr std::size_t kMaxMetadataAttempts = 3U;
    std::optional<production::ProductionMetadata> resolved_opt;
    for (std::size_t attempt = 0U; attempt < kMaxMetadataAttempts; ++attempt) {
      if (signals.requested()) {
        std::cout << "gateway_state=stopped startup_result=stop-requested "
                     "contexts=0 transports=0 subscriptions=0 "
                     "owners_joined=yes\n";
        return EXIT_SUCCESS;
      }
      const auto metadata =
          production::acquire_production_metadata(config.market_keys);
      if (const auto *error =
              std::get_if<production::MetadataError>(&metadata)) {
        const bool transient =
            error->stage == production::MetadataStage::SpotFetch ||
            error->stage == production::MetadataStage::UsdMFetch;
        if (transient && attempt + 1U < kMaxMetadataAttempts) {
          std::cerr << "metadata=retrying attempt=" << (attempt + 1U)
                    << " stage=" << production::to_string(error->stage)
                    << " message=" << error->message << '\n';
          static_cast<void>(
              signals.wait_for(std::chrono::seconds{attempt == 0U ? 2 : 5}));
          continue;
        }
        std::cerr << "metadata=failed stage="
                  << production::to_string(error->stage);
        if (error->product.has_value()) {
          production::write_product_identity(std::cerr, *error->product);
        }
        std::cerr << " message=" << error->message << '\n';
        return EXIT_FAILURE;
      }
      resolved_opt =
          std::get<production::ProductionMetadata>(std::move(metadata));
      break;
    }
    if (signals.requested()) {
      std::cout << "gateway_state=stopped startup_result=stop-requested "
                   "contexts=0 transports=0 subscriptions=0 "
                   "owners_joined=yes\n";
      return EXIT_SUCCESS;
    }
    auto resolved = std::move(*resolved_opt);
    production::GatewayOptions options;
    options.enable_usdm_reference = true;
    options.usdm_reference_seeds = resolved.usdm_references;
    return production::run_production_service(
        config,
        production::make_product_runtime_specs(std::move(resolved),
                                               &config.stream_switches),
        signals, std::cout, std::cerr, std::move(options));
  } catch (const std::exception &error) {
    std::cerr << "gateway_fatal=" << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
