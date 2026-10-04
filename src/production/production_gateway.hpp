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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace binance_market_data::gateway::production {

inline constexpr auto kInitialStartupTimeout = std::chrono::seconds{60};

enum class GatewayState : std::uint8_t {
  Constructed,
  Starting,
  Serving,
  Stopping,
  Stopped,
};

enum class StartCode : std::uint8_t {
  Serving,
  AlreadyStarted,
  StopRequested,
  ProductStartFailed,
  ProductInitialFailure,
  InitialStartupTimeout,
  GrpcBindFailed,
};

struct StartResult final {
  StartCode code{StartCode::AlreadyStarted};
  std::optional<g11::MarketKey> product;

  friend bool operator==(const StartResult &, const StartResult &) = default;
};

struct GatewayOptions final {
  std::chrono::steady_clock::duration initial_startup_timeout{
      kInitialStartupTimeout};
  g7::GrpcServiceOptions grpc;
  // Port zero is unavailable to the production CLI. This seam only avoids a
  // loopback port reservation race in deterministic in-process tests.
  bool allow_ephemeral_listen_for_testing{false};
  // Narrow deterministic seam for the set-wide startup deadline only.
  std::function<std::chrono::steady_clock::time_point()> startup_now{
      std::chrono::steady_clock::now};
};

struct ProductObservation final {
  g11::MarketKey key;
  g5::RecoveryObservation recovery;
  g3::RuntimeObservation runtime;
  g9::EventPublicationObservation events;
};

struct GatewayObservation final {
  GatewayState state{GatewayState::Constructed};
  int selected_port{0};
  std::size_t tracked_contexts{0U};
  std::size_t context_limit{g7::kMaximumGrpcTrackedContexts};
  std::vector<ProductObservation> products;
};

[[nodiscard]] std::vector<ProductObservation>
observe_products(g11::ConfiguredProductRuntimeSet &products);

class ProductionGateway final {
public:
  ProductionGateway(std::vector<g11::ProductRuntimeSpec> specifications,
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
  [[nodiscard]] g11::ConfiguredProductRuntimeSet &
  products_for_testing() noexcept;

private:
  [[nodiscard]] bool
  stop_requested(const std::function<bool()> &external_stop_requested) const;
  [[nodiscard]] StartResult
  wait_for_initial_live(const std::function<bool()> &external_stop_requested,
                        std::chrono::steady_clock::time_point deadline);
  void rollback() noexcept;
  void shutdown_graph() noexcept;

  const std::string gateway_instance_id_;
  const std::string grpc_listen_address_;
  const std::chrono::steady_clock::duration initial_startup_timeout_;
  const bool allow_ephemeral_listen_for_testing_;
  const std::function<std::chrono::steady_clock::time_point()> startup_now_;

  // Declaration order is the lifetime proof: server_ is destroyed before its
  // non-owning registry/status references in products_.
  g11::ConfiguredProductRuntimeSet products_;
  g7::OrderBookGrpcServer server_;

  mutable std::mutex state_mutex_;
  std::condition_variable state_condition_;
  GatewayState state_{GatewayState::Constructed};
  bool stop_requested_{false};
  std::mutex lifecycle_mutex_;
};

[[nodiscard]] std::string_view to_string(GatewayState state) noexcept;
[[nodiscard]] std::string_view to_string(StartCode code) noexcept;

} // namespace binance_market_data::gateway::production
