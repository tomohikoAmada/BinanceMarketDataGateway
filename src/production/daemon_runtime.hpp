#pragma once

#include "daemon_config.hpp"
#include "production_gateway.hpp"
#include "termination_signals.hpp"

#include <cstddef>
#include <iosfwd>
#include <vector>

namespace binance_market_data::gateway::production {

inline constexpr std::size_t kRecoveryDiagnosticStringLimit = 256U;

void write_recovery_failure_diagnostics(std::ostream &output,
                                        const GatewayObservation &observation);
void write_product_identity(std::ostream &output, const g11::MarketKey &key);
[[nodiscard]] bool performance_baseline_preflight(const DaemonConfig &config,
                                                  std::ostream &errors);

[[nodiscard]] int
run_production_service(const DaemonConfig &config,
                       std::vector<g11::ProductRuntimeSpec> specifications,
                       TerminationSignals &signals, std::ostream &output,
                       std::ostream &errors, GatewayOptions options = {});

} // namespace binance_market_data::gateway::production
