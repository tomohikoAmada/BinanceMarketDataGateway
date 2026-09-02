#pragma once

#include "daemon_config.hpp"
#include "production_gateway.hpp"
#include "production_metadata.hpp"
#include "termination_signals.hpp"

#include <cstddef>
#include <iosfwd>

namespace binance_market_data::gateway::production {

inline constexpr std::size_t kRecoveryDiagnosticStringLimit = 256U;

void write_recovery_failure_diagnostics(std::ostream &output,
                                        const GatewayObservation &observation);

[[nodiscard]] int run_production_service(const DaemonConfig &config,
                                         const ProductionMetadata &metadata,
                                         TerminationSignals &signals,
                                         std::ostream &output,
                                         std::ostream &errors,
                                         GatewayOptions options = {});

} // namespace binance_market_data::gateway::production
