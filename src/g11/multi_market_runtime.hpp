#pragma once

#include "event_publication.hpp"
#include "market_runtime.hpp"
#include "planned_rotation.hpp"
#include "recovery_coordinator.hpp"
#include "usdm_transport.hpp"

#include <cstdint>
#include <string>

namespace binance_market_data::gateway::g11 {

enum class ProductKind : std::uint8_t {
  Spot,
  UsdMPerpetual,
};

struct ProductRuntimeOptions final {
  g3::RuntimeLimits runtime_limits{};
  g9::EventPublicationLimits event_limits{};
  g5::PlannedRotationPolicy planned_rotation{g6::production_policy()};
  g5::detail::RecoveryTestOptions recovery_test;
};

// Owns exactly one single-product runtime graph.
class ProductRuntime final {
public:
  ProductRuntime(ProductKind kind, core::NumericSpec numeric_spec,
                 g3::RuntimeClock clock, std::string gateway_instance_id,
                 ProductRuntimeOptions options = {});
  ~ProductRuntime();

  ProductRuntime(const ProductRuntime &) = delete;
  ProductRuntime &operator=(const ProductRuntime &) = delete;
  ProductRuntime(ProductRuntime &&) = delete;
  ProductRuntime &operator=(ProductRuntime &&) = delete;

  [[nodiscard]] g5::RecoveryStartResult start();
  void shutdown_publications() noexcept;
  void stop() noexcept;

  [[nodiscard]] ProductKind kind() const noexcept;
  [[nodiscard]] g3::MarketRuntime &runtime() noexcept;
  [[nodiscard]] g5::RecoveryCoordinator &recovery() noexcept;
  [[nodiscard]] g9::EventPublication &event_publication() noexcept;

private:
  const ProductKind kind_;
  g3::MarketRuntime runtime_;
  g9::EventPublication event_publication_;
  g5::RecoveryCoordinator recovery_;
};

struct TwoProductRuntimeOptions final {
  ProductRuntimeOptions spot;
  ProductRuntimeOptions usdm;
};

struct TwoProductStartResult final {
  g5::RecoveryStartResult spot;
  g5::RecoveryStartResult usdm;
};

// Fixed G11 owning aggregate: exactly Spot BTCUSDT and USD-M BTCUSDT.
class TwoProductRuntime final {
public:
  TwoProductRuntime(core::NumericSpec spot_numeric_spec,
                    core::NumericSpec usdm_numeric_spec, g3::RuntimeClock clock,
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

private:
  ProductRuntime spot_;
  ProductRuntime usdm_;
};

} // namespace binance_market_data::gateway::g11
