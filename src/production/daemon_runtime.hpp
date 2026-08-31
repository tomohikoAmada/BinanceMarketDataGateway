#pragma once

#include "daemon_config.hpp"
#include "production_gateway.hpp"
#include "production_metadata.hpp"
#include "termination_signals.hpp"

#include <iosfwd>

namespace binance_market_data::gateway::production {

[[nodiscard]] int run_production_service(const DaemonConfig &config,
                                         const ProductionMetadata &metadata,
                                         TerminationSignals &signals,
                                         std::ostream &output,
                                         std::ostream &errors,
                                         GatewayOptions options = {});

} // namespace binance_market_data::gateway::production
