#pragma once

#include "production/daemon_config.hpp"
#include "production/production_gateway.hpp"
#include "production/termination_signals.hpp"

#include <cstddef>
#include <iosfwd>
#include <vector>
namespace composition = binance_market_data::gateway::composition;

namespace binance_market_data::gateway::production {

inline constexpr std::size_t kRecoveryDiagnosticStringLimit = 256U;

void write_recovery_failure_diagnostics(std::ostream &output,
                                        const GatewayObservation &observation);
void write_product_identity(std::ostream &output,
                            const runtime::MarketKey &key);
[[nodiscard]] bool performance_baseline_preflight(const DaemonConfig &config,
                                                  std::ostream &errors);

[[nodiscard]] int run_production_service(
    const DaemonConfig &config,
    std::vector<composition::ProductRuntimeSpec> specifications,
    TerminationSignals &signals, std::ostream &output, std::ostream &errors,
    GatewayOptions options = {});

} // namespace binance_market_data::gateway::production
