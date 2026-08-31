#include "daemon_runtime.hpp"

#include "spot_transport.hpp"

#include <csignal>
#include <cstdlib>
#include <ostream>
#include <utility>

namespace binance_market_data::gateway::production {

int run_production_service(const DaemonConfig &config,
                           const ProductionMetadata &metadata,
                           TerminationSignals &signals, std::ostream &output,
                           std::ostream &errors, GatewayOptions options) {
  const auto instance_id = g7::generate_gateway_instance_id();
  output << "gateway_state=starting products=2 grpc_listen="
         << config.grpc_listen << '\n'
         << std::flush;

  ProductionGateway gateway{
      metadata.spot_numeric_spec, metadata.usdm_numeric_spec,
      g4::sample_real_clock,      instance_id,
      config.grpc_listen,         std::move(options)};
  const auto started =
      gateway.start([&signals] { return signals.requested(); });
  if (started != StartResult::Serving) {
    const auto final = gateway.observe();
    if (started == StartResult::StopRequested) {
      output << "gateway_state=stopped startup_result=" << to_string(started)
             << " contexts=" << final.tracked_contexts << " transports="
             << final.spot_recovery.active_transport_count +
                    final.usdm_recovery.active_transport_count
             << " subscriptions="
             << final.spot_runtime.resident_subscription_count +
                    final.usdm_runtime.resident_subscription_count +
                    final.spot_events.active_subscriptions +
                    final.usdm_events.active_subscriptions
             << " owners_joined="
             << (final.spot_runtime.owner_joined &&
                         final.usdm_runtime.owner_joined
                     ? "yes"
                     : "no")
             << '\n';
      return EXIT_SUCCESS;
    }
    errors << "gateway_start=failed reason=" << to_string(started) << '\n';
    return EXIT_FAILURE;
  }

  const auto serving = gateway.observe();
  output << "gateway_state=serving products=2 grpc_port="
         << serving.selected_port
         << " spot_generation=" << serving.spot_recovery.connection_generation
         << " usdm_generation=" << serving.usdm_recovery.connection_generation
         << " context_limit=" << serving.context_limit
         << " gateway_instance_id=" << gateway.gateway_instance_id() << '\n'
         << std::flush;

  const auto received = signals.wait();
  output << "gateway_state=stopping signal="
         << (received == SIGINT    ? "SIGINT"
             : received == SIGTERM ? "SIGTERM"
                                   : "signal-wait-error")
         << '\n'
         << std::flush;
  gateway.request_stop();
  gateway.stop();

  const auto final = gateway.observe();
  output << "gateway_state=stopped contexts=" << final.tracked_contexts
         << " transports="
         << final.spot_recovery.active_transport_count +
                final.usdm_recovery.active_transport_count
         << " subscriptions="
         << final.spot_runtime.resident_subscription_count +
                final.usdm_runtime.resident_subscription_count +
                final.spot_events.active_subscriptions +
                final.usdm_events.active_subscriptions
         << " owners_joined="
         << (final.spot_runtime.owner_joined && final.usdm_runtime.owner_joined
                 ? "yes"
                 : "no")
         << '\n'
         << std::flush;
  return received == -1 ? EXIT_FAILURE : EXIT_SUCCESS;
}

} // namespace binance_market_data::gateway::production
