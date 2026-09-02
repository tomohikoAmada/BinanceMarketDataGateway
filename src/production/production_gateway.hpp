#pragma once

#include "grpc_service.hpp"
#include "multi_market_runtime.hpp"

#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <mutex>
#include <string>
#include <string_view>

namespace binance_market_data::gateway::production {

inline constexpr auto kInitialStartupTimeout = std::chrono::seconds{60};

enum class GatewayState : std::uint8_t {
  Constructed,
  Starting,
  Serving,
  Stopping,
  Stopped,
};

enum class StartResult : std::uint8_t {
  Serving,
  AlreadyStarted,
  StopRequested,
  SpotStartFailed,
  UsdMStartFailed,
  SpotInitialFailure,
  UsdMInitialFailure,
  InitialStartupTimeout,
  GrpcBindFailed,
};

struct GatewayOptions final {
  std::chrono::steady_clock::duration initial_startup_timeout{
      kInitialStartupTimeout};
  // The production daemon intentionally retains the accepted bounded
  // RuntimeLimits defaults (64 ingress / 64 bootstrap) for both products.
  g11::TwoProductRuntimeOptions products;
  g7::GrpcServiceOptions grpc;
  // Port zero is unavailable to the production CLI. This seam only avoids a
  // loopback port reservation race in deterministic in-process tests.
  bool allow_ephemeral_listen_for_testing{false};
};

struct GatewayObservation final {
  GatewayState state{GatewayState::Constructed};
  int selected_port{0};
  std::size_t tracked_contexts{0U};
  std::size_t context_limit{g7::kMaximumGrpcTrackedContexts};
  g5::RecoveryObservation spot_recovery;
  g5::RecoveryObservation usdm_recovery;
  g3::RuntimeObservation spot_runtime;
  g3::RuntimeObservation usdm_runtime;
  g9::EventPublicationObservation spot_events;
  g9::EventPublicationObservation usdm_events;
};

// Concrete post-G11 process composition. It always owns exactly the two frozen
// BTCUSDT products and one synchronous Gateway server.
class ProductionGateway final {
public:
  ProductionGateway(projection::v1::NumericSpec spot_numeric_spec,
                    projection::v1::NumericSpec usdm_numeric_spec,
                    g3::RuntimeClock clock, std::string gateway_instance_id,
                    std::string grpc_listen_address,
                    GatewayOptions options = {});
  ~ProductionGateway();

  ProductionGateway(const ProductionGateway &) = delete;
  ProductionGateway &operator=(const ProductionGateway &) = delete;
  ProductionGateway(ProductionGateway &&) = delete;
  ProductionGateway &operator=(ProductionGateway &&) = delete;

  [[nodiscard]] StartResult
  start(const std::function<bool()> &external_stop_requested = {});
  void request_stop() noexcept;
  void stop() noexcept;

  [[nodiscard]] GatewayObservation observe();
  [[nodiscard]] const std::string &gateway_instance_id() const noexcept;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  [[nodiscard]] bool write_performance_baseline(std::ostream &output) const;
#endif

  // Focused deterministic tests inject data/failures through the already
  // accepted G11 owner boundaries. The production executable does not use
  // these accessors or any acceptance-only recovery hook.
  [[nodiscard]] g11::TwoProductRuntime &products_for_testing() noexcept;

private:
  [[nodiscard]] bool
  stop_requested(const std::function<bool()> &external_stop_requested) const;
  [[nodiscard]] StartResult
  wait_for_initial_live(const std::function<bool()> &external_stop_requested);
  void rollback(StartResult result) noexcept;
  void shutdown_graph() noexcept;

  const std::string gateway_instance_id_;
  const std::string grpc_listen_address_;
  const std::chrono::steady_clock::duration initial_startup_timeout_;
  const bool allow_ephemeral_listen_for_testing_;

  // Declaration order is the lifetime proof: server_ is destroyed before its
  // non-owning registry/status references in products_.
  g11::TwoProductRuntime products_;
  g7::OrderBookGrpcServer server_;

  mutable std::mutex state_mutex_;
  std::condition_variable state_condition_;
  GatewayState state_{GatewayState::Constructed};
  bool stop_requested_{false};
  std::mutex lifecycle_mutex_;
};

[[nodiscard]] std::string_view to_string(GatewayState state) noexcept;
[[nodiscard]] std::string_view to_string(StartResult result) noexcept;

} // namespace binance_market_data::gateway::production
