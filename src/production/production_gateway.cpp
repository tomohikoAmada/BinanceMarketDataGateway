#include "production_gateway.hpp"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

namespace binance_market_data::gateway::production {

namespace {

[[nodiscard]] std::vector<g11::ProductRuntimeSpec>
production_specs(projection::v1::NumericSpec spot_numeric_spec,
                 projection::v1::NumericSpec usdm_numeric_spec,
                 g11::TwoProductRuntimeOptions options) {
  std::vector<g11::ProductRuntimeSpec> specifications;
  specifications.reserve(2U);
  specifications.push_back(
      {g11::spot_btcusdt_key(), spot_numeric_spec, std::move(options.spot)});
  specifications.push_back(
      {g11::usdm_btcusdt_key(), usdm_numeric_spec, std::move(options.usdm)});
  return specifications;
}

[[nodiscard]] bool initial_live(g11::ProductRuntime &product) {
  const auto recovery = product.recovery().observe();
  const auto runtime = product.runtime().observe();
  return recovery.state == g5::RecoveryState::Live && !recovery.terminal &&
         recovery.connection_generation >= 1U &&
         recovery.active_transport_count == 1U &&
         runtime.state == g3::RuntimeState::Live &&
         runtime.projection_status ==
             projection::v1::ProjectionStatus::Synchronized &&
         runtime.owner_thread_id != std::thread::id{};
}

[[nodiscard]] bool initial_failure(g11::ProductRuntime &product) {
  const auto recovery = product.recovery().observe();
  const auto runtime = product.runtime().observe();
  return recovery.terminal || recovery.exhausted ||
         recovery.state == g5::RecoveryState::Stopped ||
         runtime.state == g3::RuntimeState::Faulted ||
         runtime.state == g3::RuntimeState::Stopped;
}

} // namespace

std::vector<ProductObservation>
observe_products(g11::ConfiguredProductRuntimeSet &products) {
  std::vector<ProductObservation> observations;
  observations.reserve(products.size());
  for (const auto &owner : products.products()) {
    observations.push_back({owner->key(), owner->recovery().observe(),
                            owner->runtime().observe(),
                            owner->event_publication().observe()});
  }
  return observations;
}

ProductionGateway::ProductionGateway(
    projection::v1::NumericSpec spot_numeric_spec,
    projection::v1::NumericSpec usdm_numeric_spec, g3::RuntimeClock clock,
    std::string gateway_instance_id, std::string grpc_listen_address,
    GatewayOptions options)
    : gateway_instance_id_{std::move(gateway_instance_id)},
      grpc_listen_address_{std::move(grpc_listen_address)},
      initial_startup_timeout_{options.initial_startup_timeout},
      allow_ephemeral_listen_for_testing_{
          options.allow_ephemeral_listen_for_testing},
      products_{production_specs(spot_numeric_spec, usdm_numeric_spec,
                                 std::move(options.products)),
                clock, gateway_instance_id_},
      server_{products_.registry(), std::move(clock), gateway_instance_id_,
              std::move(options.grpc)} {
  if (initial_startup_timeout_ <= std::chrono::steady_clock::duration::zero()) {
    throw std::invalid_argument{"initial startup timeout must be positive"};
  }
  if (grpc_listen_address_.empty()) {
    throw std::invalid_argument{"gRPC listen address must not be empty"};
  }
  if (!allow_ephemeral_listen_for_testing_ &&
      (grpc_listen_address_.ends_with(":0") ||
       grpc_listen_address_.ends_with("]:0"))) {
    throw std::invalid_argument{"production gRPC listen port must be nonzero"};
  }
}

ProductionGateway::~ProductionGateway() { stop(); }

StartResult
ProductionGateway::start(const std::function<bool()> &external_stop_requested) {
  std::lock_guard lifecycle_lock{lifecycle_mutex_};
  {
    std::lock_guard state_lock{state_mutex_};
    if (state_ != GatewayState::Constructed) {
      return StartResult::AlreadyStarted;
    }
    state_ = GatewayState::Starting;
  }

  if (stop_requested(external_stop_requested)) {
    rollback(StartResult::StopRequested);
    return StartResult::StopRequested;
  }

  const auto starts = products_.start();
  const auto start_for = [&starts](const g11::MarketKey &key) {
    const auto found =
        std::find_if(starts.begin(), starts.end(),
                     [&key](const auto &entry) { return entry.key == key; });
    return found == starts.end() ? g5::RecoveryStartResult::RuntimeStartFailed
                                 : found->result;
  };
  if (start_for(g11::spot_btcusdt_key()) != g5::RecoveryStartResult::Started) {
    rollback(StartResult::SpotStartFailed);
    return StartResult::SpotStartFailed;
  }
  if (start_for(g11::usdm_btcusdt_key()) != g5::RecoveryStartResult::Started) {
    rollback(StartResult::UsdMStartFailed);
    return StartResult::UsdMStartFailed;
  }

  const auto initial_result = wait_for_initial_live(external_stop_requested);
  if (initial_result != StartResult::Serving) {
    rollback(initial_result);
    return initial_result;
  }

  if (stop_requested(external_stop_requested)) {
    rollback(StartResult::StopRequested);
    return StartResult::StopRequested;
  }
  if (!server_.start(grpc_listen_address_)) {
    rollback(StartResult::GrpcBindFailed);
    return StartResult::GrpcBindFailed;
  }
  if (stop_requested(external_stop_requested)) {
    rollback(StartResult::StopRequested);
    return StartResult::StopRequested;
  }

  {
    std::lock_guard state_lock{state_mutex_};
    state_ = GatewayState::Serving;
  }
  state_condition_.notify_all();
  return StartResult::Serving;
}

void ProductionGateway::request_stop() noexcept {
  {
    std::lock_guard lock{state_mutex_};
    stop_requested_ = true;
  }
  state_condition_.notify_all();
}

void ProductionGateway::stop() noexcept {
  request_stop();
  std::lock_guard lifecycle_lock{lifecycle_mutex_};
  {
    std::lock_guard state_lock{state_mutex_};
    if (state_ == GatewayState::Stopped) {
      return;
    }
    state_ = GatewayState::Stopping;
  }
  shutdown_graph();
  {
    std::lock_guard state_lock{state_mutex_};
    state_ = GatewayState::Stopped;
  }
  state_condition_.notify_all();
}

GatewayObservation ProductionGateway::observe() {
  GatewayObservation observation;
  {
    std::lock_guard state_lock{state_mutex_};
    observation.state = state_;
  }
  observation.selected_port = server_.selected_port();
  observation.tracked_contexts = server_.service().tracked_context_count();
  observation.products = observe_products(products_);
  return observation;
}

const std::string &ProductionGateway::gateway_instance_id() const noexcept {
  return gateway_instance_id_;
}

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
bool ProductionGateway::write_performance_baseline(std::ostream &output) const {
  {
    std::lock_guard state_lock{state_mutex_};
    if (state_ != GatewayState::Stopped) {
      return false;
    }
  }
  if (products_.size() != 2U) {
    return false;
  }
  const auto *spot = products_.find(g11::spot_btcusdt_key());
  const auto *usdm = products_.find(g11::usdm_btcusdt_key());
  if (spot == nullptr || usdm == nullptr) {
    return false;
  }
  spot->performance_baseline().write_json_lines(output);
  usdm->performance_baseline().write_json_lines(output);
  return output.good();
}
#endif

g11::ConfiguredProductRuntimeSet &
ProductionGateway::products_for_testing() noexcept {
  return products_;
}

bool ProductionGateway::stop_requested(
    const std::function<bool()> &external_stop_requested) const {
  {
    std::lock_guard lock{state_mutex_};
    if (stop_requested_) {
      return true;
    }
  }
  return external_stop_requested && external_stop_requested();
}

StartResult ProductionGateway::wait_for_initial_live(
    const std::function<bool()> &external_stop_requested) {
  const auto deadline =
      std::chrono::steady_clock::now() + initial_startup_timeout_;
  for (;;) {
    if (stop_requested(external_stop_requested)) {
      return StartResult::StopRequested;
    }
    auto *spot = products_.find(g11::spot_btcusdt_key());
    auto *usdm = products_.find(g11::usdm_btcusdt_key());
    if (spot == nullptr || usdm == nullptr) {
      return StartResult::SpotInitialFailure;
    }
    if (initial_failure(*spot)) {
      return StartResult::SpotInitialFailure;
    }
    if (initial_failure(*usdm)) {
      return StartResult::UsdMInitialFailure;
    }
    if (initial_live(*spot) && initial_live(*usdm)) {
      return StartResult::Serving;
    }

    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return StartResult::InitialStartupTimeout;
    }
    std::unique_lock lock{state_mutex_};
    static_cast<void>(state_condition_.wait_until(
        lock, std::min(deadline, now + std::chrono::milliseconds{10}),
        [this] { return stop_requested_; }));
  }
}

void ProductionGateway::rollback(StartResult) noexcept {
  {
    std::lock_guard state_lock{state_mutex_};
    state_ = GatewayState::Stopping;
  }
  shutdown_graph();
  {
    std::lock_guard state_lock{state_mutex_};
    state_ = GatewayState::Stopped;
  }
  state_condition_.notify_all();
}

void ProductionGateway::shutdown_graph() noexcept {
  // OrderBookGrpcServer combines the accepted G11 admission cut, bounded
  // context cancellation, Server::Shutdown/Wait, and handler drain.
  server_.shutdown();
  products_.stop();
}

std::string_view to_string(GatewayState state) noexcept {
  switch (state) {
  case GatewayState::Constructed:
    return "constructed";
  case GatewayState::Starting:
    return "starting";
  case GatewayState::Serving:
    return "serving";
  case GatewayState::Stopping:
    return "stopping";
  case GatewayState::Stopped:
    return "stopped";
  }
  return "unknown";
}

std::string_view to_string(StartResult result) noexcept {
  switch (result) {
  case StartResult::Serving:
    return "serving";
  case StartResult::AlreadyStarted:
    return "already-started";
  case StartResult::StopRequested:
    return "stop-requested";
  case StartResult::SpotStartFailed:
    return "spot-start-failed";
  case StartResult::UsdMStartFailed:
    return "usdm-start-failed";
  case StartResult::SpotInitialFailure:
    return "spot-initial-failure";
  case StartResult::UsdMInitialFailure:
    return "usdm-initial-failure";
  case StartResult::InitialStartupTimeout:
    return "initial-startup-timeout";
  case StartResult::GrpcBindFailed:
    return "grpc-bind-failed";
  }
  return "unknown";
}

} // namespace binance_market_data::gateway::production
