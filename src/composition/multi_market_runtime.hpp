#pragma once

#include "composition/market_registry.hpp"
#include "publication/event_publication.hpp"
#include "recovery/planned_rotation.hpp"
#include "recovery/recovery_coordinator.hpp"
#include "runtime/market_runtime.hpp"
#include "transport/usdm_transport.hpp"

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

namespace binance_market_data::gateway::composition {

using runtime::MarketKey;
using runtime::MarketRuntimeRegistry;
using runtime::MarketServices;
using transport::UsdMTransport;
using transport::UsdMTransportOptions;
namespace core = protocol::core;
using protocol::usdm_stream_symbol;
using runtime::kMaximumConfiguredProducts;
using runtime::MarketKeyLess;
using runtime::spot_btcusdt_key;
using runtime::usdm_btcusdt_key;
namespace common_wire = ::binance_market_data::common::v1;

enum class ProductKind : std::uint8_t {
  Spot,
  UsdMPerpetual,
};

struct ProductRuntimeOptions final {
  runtime::RuntimeLimits runtime_limits{};
  runtime::RuntimeTestOptions runtime_test;
  publication::EventPublicationLimits event_limits{};
  recovery::PlannedRotationPolicy planned_rotation{
      recovery::production_policy()};
  recovery::detail::RecoveryTestOptions recovery_test;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  performance::PerformanceBaselineLimits performance_baseline_limits{};
#endif
};

// Already-resolved runtime input. Configuration parsing and metadata
// acquisition remain outside this G12-B type.
struct ProductRuntimeSpec final {
  MarketKey key;
  core::NumericSpec numeric_spec;
  ProductRuntimeOptions options;
};

// Owns exactly one single-product runtime graph.
class ProductRuntime final {
public:
  ProductRuntime(MarketKey key, core::NumericSpec numeric_spec,
                 runtime::RuntimeClock clock, std::string gateway_instance_id,
                 ProductRuntimeOptions options = {});
  // Historical single-product compatibility seam. New reusable construction
  // must pass an exact MarketKey explicitly.
  ProductRuntime(ProductKind kind, core::NumericSpec numeric_spec,
                 runtime::RuntimeClock clock, std::string gateway_instance_id,
                 ProductRuntimeOptions options = {});
  ~ProductRuntime();

  ProductRuntime(const ProductRuntime &) = delete;
  ProductRuntime &operator=(const ProductRuntime &) = delete;
  ProductRuntime(ProductRuntime &&) = delete;
  ProductRuntime &operator=(ProductRuntime &&) = delete;

  [[nodiscard]] recovery::RecoveryStartResult start();
  void shutdown_publications() noexcept;
  void stop() noexcept;

  [[nodiscard]] const MarketKey &key() const noexcept;
  [[nodiscard]] ProductKind kind() const noexcept;
  [[nodiscard]] runtime::MarketRuntime &runtime() noexcept;
  [[nodiscard]] recovery::RecoveryCoordinator &recovery() noexcept;
  [[nodiscard]] publication::EventPublication &event_publication() noexcept;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  [[nodiscard]] const performance::ProductTraceBuffer &
  performance_baseline() const noexcept;
#endif

private:
  const MarketKey key_;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  std::shared_ptr<performance::ProductTraceBuffer> performance_baseline_;
#endif
  runtime::MarketRuntime runtime_;
  publication::EventPublication event_publication_;
  recovery::RecoveryCoordinator recovery_;
};

struct TwoProductRuntimeOptions final {
  ProductRuntimeOptions spot;
  ProductRuntimeOptions usdm;
};

struct TwoProductStartResult final {
  recovery::RecoveryStartResult spot;
  recovery::RecoveryStartResult usdm;
};

struct ProductStartObservation final {
  MarketKey key;
  recovery::RecoveryStartResult result{
      recovery::RecoveryStartResult::AlreadyStarted};
};

// Finite immutable owner set. unique_ptr keeps every ProductRuntime and its
// registry-visible subobjects at a stable address for the set lifetime.
class ConfiguredProductRuntimeSet final {
public:
  ConfiguredProductRuntimeSet(std::vector<ProductRuntimeSpec> specifications,
                              runtime::RuntimeClock clock,
                              std::string gateway_instance_id);
  ~ConfiguredProductRuntimeSet();

  ConfiguredProductRuntimeSet(const ConfiguredProductRuntimeSet &) = delete;
  ConfiguredProductRuntimeSet &
  operator=(const ConfiguredProductRuntimeSet &) = delete;
  ConfiguredProductRuntimeSet(ConfiguredProductRuntimeSet &&) = delete;
  ConfiguredProductRuntimeSet &
  operator=(ConfiguredProductRuntimeSet &&) = delete;

  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] const MarketRuntimeRegistry &registry() const noexcept;
  [[nodiscard]] ProductRuntime *find(const MarketKey &key) noexcept;
  [[nodiscard]] const ProductRuntime *find(const MarketKey &key) const noexcept;
  [[nodiscard]] const std::vector<std::unique_ptr<ProductRuntime>> &
  products() const noexcept;
  [[nodiscard]] std::vector<ProductStartObservation> start();
  void shutdown_publications() noexcept;
  void stop() noexcept;

private:
  struct PreparedSpecifications final {
    std::vector<ProductRuntimeSpec> values;
  };

  ConfiguredProductRuntimeSet(PreparedSpecifications specifications,
                              runtime::RuntimeClock clock,
                              std::string gateway_instance_id);
  [[nodiscard]] static PreparedSpecifications
  prepare(std::vector<ProductRuntimeSpec> specifications);
  [[nodiscard]] static std::vector<std::unique_ptr<ProductRuntime>>
  construct_owners(PreparedSpecifications specifications,
                   const runtime::RuntimeClock &clock,
                   const std::string &gateway_instance_id);
  [[nodiscard]] static std::vector<MarketServices> make_registry_entries(
      const std::vector<std::unique_ptr<ProductRuntime>> &owners);

  // Declaration order is the lifetime proof: registry_ dies before owners_.
  std::vector<std::unique_ptr<ProductRuntime>> owners_;
  MarketRuntimeRegistry registry_;
};

// Thin fixed-G11 compatibility view over the generic configured owner set.
class TwoProductRuntime final {
public:
  TwoProductRuntime(core::NumericSpec spot_numeric_spec,
                    core::NumericSpec usdm_numeric_spec,
                    runtime::RuntimeClock clock,
                    std::string gateway_instance_id,
                    TwoProductRuntimeOptions options = {});
  ~TwoProductRuntime();

  TwoProductRuntime(const TwoProductRuntime &) = delete;
  TwoProductRuntime &operator=(const TwoProductRuntime &) = delete;
  TwoProductRuntime(TwoProductRuntime &&) = delete;
  TwoProductRuntime &operator=(TwoProductRuntime &&) = delete;

  [[nodiscard]] TwoProductStartResult start();
  void shutdown_publications() noexcept;
  void stop() noexcept;

  [[nodiscard]] ProductRuntime &spot() noexcept;
  [[nodiscard]] ProductRuntime &usdm() noexcept;
  [[nodiscard]] const MarketRuntimeRegistry &registry() const noexcept;
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  void write_performance_baseline(std::ostream &output) const;
#endif

private:
  ConfiguredProductRuntimeSet products_;
};

} // namespace binance_market_data::gateway::composition
