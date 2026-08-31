#include "multi_market_runtime.hpp"

#include "spot_transport.hpp"

#include <memory>
#include <stdexcept>
#include <utility>

namespace binance_market_data::gateway::g11 {

namespace {

class SpotProductAttempt final : public g5::detail::RecoveryAttempt {
public:
  SpotProductAttempt(g3::MarketRuntime &runtime, const g3::RuntimeClock &clock,
                     std::uint64_t generation,
                     g9::EventPublication &publication)
      : transport_{runtime, clock, generation, make_options(publication)} {}

  [[nodiscard]] g4::TransportStartResult start() override {
    return transport_.start();
  }

  void stop() noexcept override { transport_.stop(); }

  [[nodiscard]] g4::TransportObservation observe() const override {
    return transport_.observe();
  }

private:
  [[nodiscard]] static g4::SpotTransportOptions
  make_options(g9::EventPublication &publication) {
    g4::SpotTransportOptions options;
    options.profile = g4::SpotTransportProfile::G9CombinedEvents;
    options.normalized_event_sink =
        [&publication](std::shared_ptr<const g4::NormalizedMarketEvent> event,
                       std::uint64_t generation) {
          return publication.publish(event, generation) ==
                         g9::EventPublishResult::InvariantFailure
                     ? g4::NormalizedEventSinkResult::InvariantFailure
                     : g4::NormalizedEventSinkResult::Continue;
        };
    return options;
  }

  g4::SpotTransport transport_;
};

class UsdMProductAttempt final : public g5::detail::RecoveryAttempt {
public:
  UsdMProductAttempt(g3::MarketRuntime &runtime, const g3::RuntimeClock &clock,
                     std::uint64_t generation,
                     g9::EventPublication &publication)
      : transport_{runtime, clock, generation, make_options(publication)} {}

  [[nodiscard]] g4::TransportStartResult start() override {
    return transport_.start();
  }

  void stop() noexcept override { transport_.stop(); }

  [[nodiscard]] g4::TransportObservation observe() const override {
    return transport_.observe();
  }

private:
  [[nodiscard]] static UsdMTransportOptions
  make_options(g9::EventPublication &publication) {
    UsdMTransportOptions options;
    options.normalized_event_sink =
        [&publication](std::shared_ptr<const g4::NormalizedMarketEvent> event,
                       std::uint64_t generation) {
          return publication.publish(event, generation) ==
                         g9::EventPublishResult::InvariantFailure
                     ? g4::NormalizedEventSinkResult::InvariantFailure
                     : g4::NormalizedEventSinkResult::Continue;
        };
    return options;
  }

  UsdMTransport transport_;
};

[[nodiscard]] g3::adapter::ExpectedIdentity identity_for(ProductKind kind) {
  switch (kind) {
  case ProductKind::Spot:
    return {"BTCUSDT", core::SequencePolicyKind::Spot};
  case ProductKind::UsdMPerpetual:
    return {"BTCUSDT", core::SequencePolicyKind::UsdMPerpetual};
  }
  throw std::invalid_argument{"unsupported G11 product kind"};
}

[[nodiscard]] g5::RecoveryCoordinatorOptions
coordinator_options(ProductKind kind, g9::EventPublication &publication) {
  g5::RecoveryCoordinatorOptions options;
  options.attempt_factory = [kind, &publication](g3::MarketRuntime &runtime,
                                                 const g3::RuntimeClock &clock,
                                                 std::uint64_t generation)
      -> std::unique_ptr<g5::detail::RecoveryAttempt> {
    if (kind == ProductKind::Spot) {
      return std::make_unique<SpotProductAttempt>(runtime, clock, generation,
                                                  publication);
    }
    return std::make_unique<UsdMProductAttempt>(runtime, clock, generation,
                                                publication);
  };
  options.source_generation_lifecycle.open =
      [&publication](std::uint64_t generation) {
        return publication.open_generation(generation);
      };
  options.source_generation_lifecycle.quiesce = [&publication](
                                                    std::uint64_t generation) {
    if (publication.observe().source_state == g9::EventSourceState::Shutdown) {
      return true;
    }
    return publication.quiesce_generation(generation);
  };
  options.source_generation_lifecycle.close =
      [&publication](std::uint64_t generation,
                     g5::SourceGenerationCloseOutcome outcome) {
        if (outcome == g5::SourceGenerationCloseOutcome::GlobalShutdown) {
          publication.shutdown();
          return true;
        }
        if (outcome == g5::SourceGenerationCloseOutcome::Replacement) {
          return publication.close_generation_replaced(generation);
        }
        return publication.close_generation_permanently(generation);
      };
  return options;
}

} // namespace

ProductRuntime::ProductRuntime(ProductKind kind, core::NumericSpec numeric_spec,
                               g3::RuntimeClock clock,
                               std::string gateway_instance_id,
                               ProductRuntimeOptions options)
    : kind_{kind},
      runtime_{options.runtime_limits, clock, numeric_spec, identity_for(kind)},
      event_publication_{std::move(gateway_instance_id), clock,
                         options.event_limits},
      recovery_{runtime_, std::move(clock), options.planned_rotation,
                coordinator_options(kind, event_publication_),
                std::move(options.recovery_test)} {}

ProductRuntime::~ProductRuntime() { stop(); }

g5::RecoveryStartResult ProductRuntime::start() { return recovery_.start(); }

void ProductRuntime::shutdown_publications() noexcept {
  runtime_.close_publication_admission();
  static_cast<void>(runtime_.shutdown_publication());
  event_publication_.shutdown();
}

void ProductRuntime::stop() noexcept {
  shutdown_publications();
  recovery_.stop();
}

ProductKind ProductRuntime::kind() const noexcept { return kind_; }

g3::MarketRuntime &ProductRuntime::runtime() noexcept { return runtime_; }

g5::RecoveryCoordinator &ProductRuntime::recovery() noexcept {
  return recovery_;
}

g9::EventPublication &ProductRuntime::event_publication() noexcept {
  return event_publication_;
}

TwoProductRuntime::TwoProductRuntime(core::NumericSpec spot_numeric_spec,
                                     core::NumericSpec usdm_numeric_spec,
                                     g3::RuntimeClock clock,
                                     std::string gateway_instance_id,
                                     TwoProductRuntimeOptions options)
    : spot_{ProductKind::Spot, spot_numeric_spec, clock, gateway_instance_id,
            std::move(options.spot)},
      usdm_{ProductKind::UsdMPerpetual, usdm_numeric_spec, std::move(clock),
            std::move(gateway_instance_id), std::move(options.usdm)} {}

TwoProductRuntime::~TwoProductRuntime() { stop(); }

TwoProductStartResult TwoProductRuntime::start() {
  const auto spot_result = spot_.start();
  const auto usdm_result = usdm_.start();
  return {spot_result, usdm_result};
}

void TwoProductRuntime::shutdown_publications() noexcept {
  spot_.shutdown_publications();
  usdm_.shutdown_publications();
}

void TwoProductRuntime::stop() noexcept {
  shutdown_publications();
  spot_.recovery().stop();
  usdm_.recovery().stop();
}

ProductRuntime &TwoProductRuntime::spot() noexcept { return spot_; }

ProductRuntime &TwoProductRuntime::usdm() noexcept { return usdm_; }

} // namespace binance_market_data::gateway::g11
