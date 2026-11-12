#pragma once

#include "production/daemon_config.hpp"
#include "production/production_gateway.hpp"
#include "production/termination_signals.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <optional>
#include <vector>
namespace composition = binance_market_data::gateway::composition;

namespace binance_market_data::gateway::production {

namespace core = binance_market_data::projection::v1;

inline constexpr std::size_t kRecoveryDiagnosticStringLimit = 256U;
inline constexpr auto kRuntimeObservationInterval = std::chrono::seconds{30};

void write_recovery_failure_diagnostics(std::ostream &output,
                                        const GatewayObservation &observation);
void write_product_identity(std::ostream &output,
                            const runtime::MarketKey &key);
[[nodiscard]] bool performance_baseline_preflight(const DaemonConfig &config,
                                                  std::ostream &errors);

struct ProductDiagnosticState final {
  runtime::MarketKey key;
  bool initialized{false};
  std::uint64_t generation{0U};
  recovery::RecoveryState recovery_state{recovery::RecoveryState::Starting};
  runtime::RuntimeState runtime_state{runtime::RuntimeState::Constructed};
  core::ProjectionStatus projection_status{
      core::ProjectionStatus::AwaitingBaseline};
  std::uint64_t total_recovery_count{0U};
  bool terminal{false};
  bool exhausted{false};
  std::optional<recovery::RecoveryCause> last_cause;
  std::optional<runtime::FaultReason> fault_reason;
  std::uint64_t last_failure_generation{0U};
};

// Emits one summary per observation and product details only on a state,
// generation, recovery-count or terminal-cause change. Failure history is
// emitted once per new generation, never reprinted in full every cycle.
class RuntimeDiagnostics final {
public:
  void observe(std::ostream &output, std::ostream &errors,
               const GatewayObservation &observation);
  void write_pending_failures(std::ostream &errors,
                              const GatewayObservation &observation);

private:
  [[nodiscard]] bool prepare(const GatewayObservation &observation);
  void process_failures(std::ostream &errors,
                        const GatewayObservation &observation);

  std::vector<ProductDiagnosticState> products_;
  bool initialized_{false};
};

[[nodiscard]] int run_production_service(
    const DaemonConfig &config,
    std::vector<composition::ProductRuntimeSpec> specifications,
    TerminationSignals &signals, std::ostream &output, std::ostream &errors,
    GatewayOptions options = {});

} // namespace binance_market_data::gateway::production
