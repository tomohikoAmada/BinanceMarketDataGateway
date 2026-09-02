#include "daemon_runtime.hpp"

#include "spot_transport.hpp"

#include <algorithm>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

namespace binance_market_data::gateway::production {

namespace {

namespace adapter = projection_adapter::v1;
namespace core = projection::v1;

[[nodiscard]] std::string_view diagnostic_name(g5::RecoveryCause value) {
  switch (value) {
  case g5::RecoveryCause::NeedsResync:
    return "needs-resync";
  case g5::RecoveryCause::TransportFailure:
    return "transport-failure";
  case g5::RecoveryCause::SnapshotFailure:
    return "snapshot-failure";
  case g5::RecoveryCause::IngressOverflow:
    return "ingress-overflow";
  case g5::RecoveryCause::BootstrapBufferOverflow:
    return "bootstrap-buffer-overflow";
  case g5::RecoveryCause::Protocol:
    return "protocol";
  case g5::RecoveryCause::ServerShutdown:
    return "server-shutdown";
  case g5::RecoveryCause::Http429:
    return "http-429";
  case g5::RecoveryCause::Http418:
    return "http-418";
  case g5::RecoveryCause::Http5xx:
    return "http-5xx";
  case g5::RecoveryCause::TerminalHttp4xx:
    return "terminal-http-4xx";
  case g5::RecoveryCause::InternalFailure:
    return "internal-failure";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(g4::NetworkErrorCode value) {
  switch (value) {
  case g4::NetworkErrorCode::Timeout:
    return "timeout";
  case g4::NetworkErrorCode::Dns:
    return "dns";
  case g4::NetworkErrorCode::Tcp:
    return "tcp";
  case g4::NetworkErrorCode::Tls:
    return "tls";
  case g4::NetworkErrorCode::WebSocketHandshake:
    return "websocket-handshake";
  case g4::NetworkErrorCode::WebSocketRead:
    return "websocket-read";
  case g4::NetworkErrorCode::HttpWrite:
    return "http-write";
  case g4::NetworkErrorCode::HttpRead:
    return "http-read";
  case g4::NetworkErrorCode::HttpStatus:
    return "http-status";
  case g4::NetworkErrorCode::Protocol:
    return "protocol";
  case g4::NetworkErrorCode::RuntimeAdmission:
    return "runtime-admission";
  case g4::NetworkErrorCode::ServerShutdown:
    return "server-shutdown";
  case g4::NetworkErrorCode::Internal:
    return "internal";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(g3::RuntimeState value) {
  switch (value) {
  case g3::RuntimeState::Constructed:
    return "constructed";
  case g3::RuntimeState::Buffering:
    return "buffering";
  case g3::RuntimeState::AwaitingBridge:
    return "awaiting-bridge";
  case g3::RuntimeState::Live:
    return "live";
  case g3::RuntimeState::NeedsResync:
    return "needs-resync";
  case g3::RuntimeState::Faulted:
    return "faulted";
  case g3::RuntimeState::Stopping:
    return "stopping";
  case g3::RuntimeState::Stopped:
    return "stopped";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(g3::FaultReason value) {
  switch (value) {
  case g3::FaultReason::IngressOverflow:
    return "ingress-overflow";
  case g3::FaultReason::BootstrapBufferOverflow:
    return "bootstrap-buffer-overflow";
  case g3::FaultReason::AdapterError:
    return "adapter-error";
  case g3::FaultReason::TransportFailure:
    return "transport-failure";
  case g3::FaultReason::SnapshotFailure:
    return "snapshot-failure";
  case g3::FaultReason::ProjectionRejected:
    return "projection-rejected";
  case g3::FaultReason::ClockError:
    return "clock-error";
  case g3::FaultReason::InternalError:
    return "internal-error";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(core::ProjectionStatus value) {
  switch (value) {
  case core::ProjectionStatus::AwaitingBaseline:
    return "awaiting-baseline";
  case core::ProjectionStatus::AwaitingBridge:
    return "awaiting-bridge";
  case core::ProjectionStatus::Synchronized:
    return "synchronized";
  case core::ProjectionStatus::NeedsResync:
    return "needs-resync";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(core::GapReason value) {
  switch (value) {
  case core::GapReason::SpotBootstrapForwardGap:
    return "spot-bootstrap-forward-gap";
  case core::GapReason::SpotLiveForwardGap:
    return "spot-live-forward-gap";
  case core::GapReason::FuturesBootstrapRangeMiss:
    return "futures-bootstrap-range-miss";
  case core::GapReason::FuturesMissingPreviousFinal:
    return "futures-missing-previous-final";
  case core::GapReason::FuturesPreviousFinalMismatch:
    return "futures-previous-final-mismatch";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(core::SequencePolicyKind value) {
  switch (value) {
  case core::SequencePolicyKind::Spot:
    return "spot";
  case core::SequencePolicyKind::UsdMPerpetual:
    return "usdm-perpetual";
  }
  return "unknown";
}

[[nodiscard]] std::string_view
diagnostic_name(adapter::AdapterErrorCode value) {
  switch (value) {
  case adapter::AdapterErrorCode::UnsupportedVenue:
    return "unsupported-venue";
  case adapter::AdapterErrorCode::UnsupportedMarket:
    return "unsupported-market";
  case adapter::AdapterErrorCode::UnexpectedStream:
    return "unexpected-stream";
  case adapter::AdapterErrorCode::IdentityMismatch:
    return "identity-mismatch";
  case adapter::AdapterErrorCode::UnsupportedSchemaVersion:
    return "unsupported-schema-version";
  case adapter::AdapterErrorCode::UnspecifiedEnum:
    return "unspecified-enum";
  case adapter::AdapterErrorCode::UnknownEnumValue:
    return "unknown-enum-value";
  case adapter::AdapterErrorCode::InvalidUpdateRange:
    return "invalid-update-range";
  case adapter::AdapterErrorCode::MissingRequiredField:
    return "missing-required-field";
  case adapter::AdapterErrorCode::InvalidIdentifier:
    return "invalid-identifier";
  case adapter::AdapterErrorCode::InvalidDecimal:
    return "invalid-decimal";
  case adapter::AdapterErrorCode::NegativeQuantity:
    return "negative-quantity";
  case adapter::AdapterErrorCode::NonPositivePrice:
    return "non-positive-price";
  case adapter::AdapterErrorCode::ScaleMismatch:
    return "scale-mismatch";
  case adapter::AdapterErrorCode::NumericOverflow:
    return "numeric-overflow";
  case adapter::AdapterErrorCode::InvalidDepthLimit:
    return "invalid-depth-limit";
  case adapter::AdapterErrorCode::InvalidOrdering:
    return "invalid-ordering";
  case adapter::AdapterErrorCode::UnsupportedProjectionState:
    return "unsupported-projection-state";
  case adapter::AdapterErrorCode::MissingLastUpdateId:
    return "missing-last-update-id";
  case adapter::AdapterErrorCode::InvalidGapContext:
    return "invalid-gap-context";
  case adapter::AdapterErrorCode::InvalidHostQualityCombination:
    return "invalid-host-quality-combination";
  case adapter::AdapterErrorCode::ContractsVersionMismatch:
    return "contracts-version-mismatch";
  case adapter::AdapterErrorCode::ProjectionNumericSpecMismatch:
    return "projection-numeric-spec-mismatch";
  case adapter::AdapterErrorCode::ProjectionPolicyMismatch:
    return "projection-policy-mismatch";
  }
  return "unknown";
}

[[nodiscard]] std::string_view diagnostic_name(adapter::AdapterField value) {
  switch (value) {
  case adapter::AdapterField::None:
    return "none";
  case adapter::AdapterField::Venue:
    return "venue";
  case adapter::AdapterField::Market:
    return "market";
  case adapter::AdapterField::Stream:
    return "stream";
  case adapter::AdapterField::Symbol:
    return "symbol";
  case adapter::AdapterField::SchemaVersion:
    return "schema-version";
  case adapter::AdapterField::Producer:
    return "producer";
  case adapter::AdapterField::ProducerVersion:
    return "producer-version";
  case adapter::AdapterField::RequestId:
    return "request-id";
  case adapter::AdapterField::ConnectionId:
    return "connection-id";
  case adapter::AdapterField::FirstUpdateId:
    return "first-update-id";
  case adapter::AdapterField::FinalUpdateId:
    return "final-update-id";
  case adapter::AdapterField::PreviousFinalUpdateId:
    return "previous-final-update-id";
  case adapter::AdapterField::BidPrice:
    return "bid-price";
  case adapter::AdapterField::BidQuantity:
    return "bid-quantity";
  case adapter::AdapterField::AskPrice:
    return "ask-price";
  case adapter::AdapterField::AskQuantity:
    return "ask-quantity";
  case adapter::AdapterField::QualityFlag:
    return "quality-flag";
  case adapter::AdapterField::ProjectionPriceScale:
    return "projection-price-scale";
  case adapter::AdapterField::ProjectionQuantityScale:
    return "projection-quantity-scale";
  case adapter::AdapterField::ProjectionPolicy:
    return "projection-policy";
  case adapter::AdapterField::DepthLimit:
    return "depth-limit";
  case adapter::AdapterField::SnapshotSource:
    return "snapshot-source";
  case adapter::AdapterField::LastUpdateId:
    return "last-update-id";
  case adapter::AdapterField::CurrentGap:
    return "current-gap";
  case adapter::AdapterField::GapRecoveryState:
    return "gap-recovery-state";
  case adapter::AdapterField::HostQualityFact:
    return "host-quality-fact";
  }
  return "unknown";
}

void write_escaped(std::ostream &output, std::string_view value) {
  constexpr std::string_view hex{"0123456789ABCDEF"};
  const auto size = std::min(value.size(), kRecoveryDiagnosticStringLimit);
  output << '"';
  for (std::size_t index = 0U; index < size; ++index) {
    const auto byte = static_cast<unsigned char>(value[index]);
    switch (byte) {
    case '\\':
      output << "\\\\";
      break;
    case '"':
      output << "\\\"";
      break;
    case '\n':
      output << "\\n";
      break;
    case '\r':
      output << "\\r";
      break;
    case '\t':
      output << "\\t";
      break;
    default:
      if (byte >= 0x20U && byte <= 0x7EU) {
        output << static_cast<char>(byte);
      } else {
        output << "\\x" << hex[byte >> 4U] << hex[byte & 0x0FU];
      }
      break;
    }
  }
  output << '"';
}

void write_bounded_string(std::ostream &output, std::string_view name,
                          const std::string &value) {
  output << ' ' << name << '=';
  write_escaped(output, value);
  output << ' ' << name << "_truncated="
         << (value.size() > kRecoveryDiagnosticStringLimit ? "yes" : "no");
}

void write_absent_string(std::ostream &output, std::string_view name) {
  output << ' ' << name << "=none " << name << "_truncated=no";
}

void write_failure(std::ostream &output, std::string_view product,
                   std::size_t index,
                   const g5::RecoveryFailureDiagnostic &failure) {
  output << "gateway_recovery_failure product=" << product << " index=" << index
         << " generation=" << failure.connection_generation
         << " cause=" << diagnostic_name(failure.cause)
         << " runtime_state=" << diagnostic_name(failure.runtime_state)
         << " projection_status=" << diagnostic_name(failure.projection_status)
         << " fault_reason="
         << (failure.fault_reason.has_value()
                 ? diagnostic_name(*failure.fault_reason)
                 : std::string_view{"none"});

  if (failure.network_error.has_value()) {
    const auto &error = *failure.network_error;
    output << " network_error_code=" << diagnostic_name(error.code);
    write_bounded_string(output, "network_stage", error.stage);
    write_bounded_string(output, "network_message", error.message);
    output << " network_http_status=";
    if (error.http_status.has_value()) {
      output << *error.http_status;
    } else {
      output << "none";
    }
    if (error.retry_after.has_value()) {
      write_bounded_string(output, "network_retry_after", *error.retry_after);
    } else {
      write_absent_string(output, "network_retry_after");
    }
  } else {
    output << " network_error_code=none";
    write_absent_string(output, "network_stage");
    write_absent_string(output, "network_message");
    output << " network_http_status=none";
    write_absent_string(output, "network_retry_after");
  }

  if (failure.adapter_error.has_value()) {
    const auto &error = *failure.adapter_error;
    output << " adapter_error_code=" << diagnostic_name(error.code)
           << " adapter_error_field=" << diagnostic_name(error.field)
           << " adapter_decimal_error=";
    if (error.decimal_error.has_value()) {
      output << core::to_string(error.decimal_error->code)
             << " adapter_decimal_offset=" << error.decimal_error->offset;
    } else {
      output << "none adapter_decimal_offset=none";
    }
    output << " adapter_raw_enum=";
    if (error.raw_enum_value.has_value()) {
      output << *error.raw_enum_value;
    } else {
      output << "none";
    }
  } else {
    output << " adapter_error_code=none adapter_error_field=none"
              " adapter_decimal_error=none adapter_decimal_offset=none"
              " adapter_raw_enum=none";
  }

  if (failure.last_gap.has_value()) {
    const auto &gap = *failure.last_gap;
    output << " gap_reason=" << diagnostic_name(gap.reason)
           << " gap_policy=" << diagnostic_name(gap.policy)
           << " gap_last_accepted_final=" << gap.last_accepted_final.value()
           << " gap_incoming_first=" << gap.incoming_range.first().value()
           << " gap_incoming_final=" << gap.incoming_range.final().value()
           << " gap_incoming_previous_final=";
    if (gap.incoming_previous_final.has_value()) {
      output << gap.incoming_previous_final->value();
    } else {
      output << "none";
    }
  } else {
    output << " gap_reason=none gap_policy=none"
              " gap_last_accepted_final=none gap_incoming_first=none"
              " gap_incoming_final=none gap_incoming_previous_final=none";
  }
  output << '\n';
}

void write_product_failures(std::ostream &output, std::string_view product,
                            const g5::RecoveryObservation &recovery) {
  const auto size =
      std::min(recovery.failure_history_size, recovery.failure_history.size());
  for (std::size_t index = 0U; index < size; ++index) {
    write_failure(output, product, index, recovery.failure_history[index]);
  }
}

} // namespace

void write_recovery_failure_diagnostics(std::ostream &output,
                                        const GatewayObservation &observation) {
  write_product_failures(output, "spot", observation.spot_recovery);
  write_product_failures(output, "usdm", observation.usdm_recovery);
}

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
namespace {

[[nodiscard]] bool export_performance_baseline(ProductionGateway &gateway,
                                               std::ostream &errors) {
  const auto *path = std::getenv("BMD_GATEWAY_PERFORMANCE_BASELINE_OUTPUT");
  if (path == nullptr || *path == '\0') {
    errors << "performance_baseline_export=skipped reason=output-not-set\n";
    return true;
  }
  std::ofstream artifact{path, std::ios::out | std::ios::trunc};
  if (!artifact || !gateway.write_performance_baseline(artifact)) {
    errors << "performance_baseline_export=failed path=" << path << '\n';
    return false;
  }
  artifact.flush();
  if (!artifact) {
    errors << "performance_baseline_export=failed path=" << path << '\n';
    return false;
  }
  errors << "performance_baseline_export=complete path=" << path << '\n';
  return true;
}

} // namespace
#endif

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
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    const auto exported = export_performance_baseline(gateway, errors);
#endif
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
      return
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
          exported ? EXIT_SUCCESS : EXIT_FAILURE;
#else
          EXIT_SUCCESS;
#endif
    }
    errors << "gateway_start=failed reason=" << to_string(started) << '\n';
    write_recovery_failure_diagnostics(errors, final);
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

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  const auto exported = export_performance_baseline(gateway, errors);
#endif

  const auto final = gateway.observe();
  write_recovery_failure_diagnostics(errors, final);
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
  return received == -1
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                 || !exported
#endif
             ? EXIT_FAILURE
             : EXIT_SUCCESS;
}

} // namespace binance_market_data::gateway::production
