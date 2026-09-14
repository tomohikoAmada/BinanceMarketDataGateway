#include "production_gateway.hpp"

#include <chrono>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

namespace binance_market_data::gateway::production {

namespace {

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
    std::vector<g11::ProductRuntimeSpec> specifications, g3::RuntimeClock clock,
    std::string gateway_instance_id, std::string grpc_listen_address,
    GatewayOptions options)
    : gateway_instance_id_{std::move(gateway_instance_id)},
      grpc_listen_address_{std::move(grpc_listen_address)},
      initial_startup_timeout_{options.initial_startup_timeout},
      allow_ephemeral_listen_for_testing_{
          options.allow_ephemeral_listen_for_testing},
      startup_now_{std::move(options.startup_now)},
      products_{std::move(specifications), clock, gateway_instance_id_},
      server_{products_.registry(), std::move(clock), gateway_instance_id_,
              std::move(options.grpc)} {
  if (initial_startup_timeout_ <= std::chrono::steady_clock::duration::zero()) {
    throw std::invalid_argument{"initial startup timeout must be positive"};
  }
  if (!startup_now_) {
    throw std::invalid_argument{"startup monotonic-now function is required"};
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
      return {StartCode::AlreadyStarted, std::nullopt};
    }
    state_ = GatewayState::Starting;
  }

  if (stop_requested(external_stop_requested)) {
    rollback();
    return {StartCode::StopRequested, std::nullopt};
  }

  const auto deadline = startup_now_() + initial_startup_timeout_;
  const auto starts = products_.start();
  for (const auto &started : starts) {
    if (started.result != g5::RecoveryStartResult::Started) {
      const StartResult result{StartCode::ProductStartFailed, started.key};
      rollback();
      return result;
    }
  }

  const auto initial_result =
      wait_for_initial_live(external_stop_requested, deadline);
  if (initial_result.code != StartCode::Serving) {
    rollback();
    return initial_result;
  }

  if (stop_requested(external_stop_requested)) {
    rollback();
    return {StartCode::StopRequested, std::nullopt};
  }
  if (!server_.start(grpc_listen_address_)) {
    rollback();
    return {StartCode::GrpcBindFailed, std::nullopt};
  }
  if (stop_requested(external_stop_requested)) {
    rollback();
    return {StartCode::StopRequested, std::nullopt};
  }

  {
    std::lock_guard state_lock{state_mutex_};
    state_ = GatewayState::Serving;
  }
  state_condition_.notify_all();
  return {StartCode::Serving, std::nullopt};
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
    const std::function<bool()> &external_stop_requested,
    std::chrono::steady_clock::time_point deadline) {
  for (;;) {
    if (stop_requested(external_stop_requested)) {
      return {StartCode::StopRequested, std::nullopt};
    }
    for (const auto &product : products_.products()) {
      if (initial_failure(*product)) {
        return {StartCode::ProductInitialFailure, product->key()};
      }
    }
    bool all_live = true;
    for (const auto &product : products_.products()) {
      all_live = all_live && initial_live(*product);
    }
    if (all_live) {
      return {StartCode::Serving, std::nullopt};
    }

    const auto now = startup_now_();
    if (now >= deadline) {
      return {StartCode::InitialStartupTimeout, std::nullopt};
    }
    std::unique_lock lock{state_mutex_};
    static_cast<void>(state_condition_.wait_for(
        lock,
        std::min(
            deadline - now,
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::milliseconds{10})),
        [this] { return stop_requested_; }));
  }
}

void ProductionGateway::rollback() noexcept {
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

std::string_view to_string(StartCode code) noexcept {
  switch (code) {
  case StartCode::Serving:
    return "serving";
  case StartCode::AlreadyStarted:
    return "already-started";
  case StartCode::StopRequested:
    return "stop-requested";
  case StartCode::ProductStartFailed:
    return "product-start-failed";
  case StartCode::ProductInitialFailure:
    return "product-initial-failure";
  case StartCode::InitialStartupTimeout:
    return "initial-startup-timeout";
  case StartCode::GrpcBindFailed:
    return "grpc-bind-failed";
  }
  return "unknown";
}

} // namespace binance_market_data::gateway::production
