#include "multi_market_runtime.hpp"

#include "spot_transport.hpp"

#include <memory>
#include <stdexcept>
#include <utility>

namespace binance_market_data::gateway::g11 {

namespace {

class SpotProductAttempt final : public g5::detail::RecoveryAttempt {
public:
  SpotProductAttempt(
      g3::MarketRuntime &runtime, const g3::RuntimeClock &clock,
      std::uint64_t generation, std::string exact_symbol,
      g9::EventPublication &publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      ,
      std::shared_ptr<performance::ProductTraceBuffer> performance_baseline
#endif
      )
      : transport_{runtime, clock, std::move(exact_symbol), generation,
                   make_options(publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                                ,
                                std::move(performance_baseline)
#endif
                                    )} {
  }

  [[nodiscard]] g4::TransportStartResult start() override {
    return transport_.start();
  }

  void stop() noexcept override { transport_.stop(); }

  [[nodiscard]] g4::TransportObservation observe() const override {
    return transport_.observe();
  }

private:
  [[nodiscard]] static g4::SpotTransportOptions make_options(
      g9::EventPublication &publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      ,
      std::shared_ptr<performance::ProductTraceBuffer> performance_baseline
#endif
  ) {
    g4::SpotTransportOptions options;
    options.profile = g4::SpotTransportProfile::G9CombinedEvents;
    options.normalized_event_sink =
        [&publication](std::shared_ptr<const g4::NormalizedMarketEvent> event,
                       std::uint64_t generation
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                       ,
                       performance::TraceToken trace
#endif
        ) {
          return publication.publish(event, generation
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                                     ,
                                     trace
#endif
                                     ) ==
                         g9::EventPublishResult::InvariantFailure
                     ? g4::NormalizedEventSinkResult::InvariantFailure
                     : g4::NormalizedEventSinkResult::Continue;
        };
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    options.performance_baseline = std::move(performance_baseline);
#endif
    return options;
  }

  g4::SpotTransport transport_;
};

class UsdMProductAttempt final : public g5::detail::RecoveryAttempt {
public:
  UsdMProductAttempt(
      g3::MarketRuntime &runtime, const g3::RuntimeClock &clock,
      std::uint64_t generation, std::string exact_symbol,
      g9::EventPublication &publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      ,
      std::shared_ptr<performance::ProductTraceBuffer> performance_baseline
#endif
      )
      : transport_{runtime, clock, std::move(exact_symbol), generation,
                   make_options(publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                                ,
                                std::move(performance_baseline)
#endif
                                    )} {
  }

  [[nodiscard]] g4::TransportStartResult start() override {
    return transport_.start();
  }

  void stop() noexcept override { transport_.stop(); }

  [[nodiscard]] g4::TransportObservation observe() const override {
    return transport_.observe();
  }

private:
  [[nodiscard]] static UsdMTransportOptions make_options(
      g9::EventPublication &publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
      ,
      std::shared_ptr<performance::ProductTraceBuffer> performance_baseline
#endif
  ) {
    UsdMTransportOptions options;
    options.normalized_event_sink =
        [&publication](std::shared_ptr<const g4::NormalizedMarketEvent> event,
                       std::uint64_t generation
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                       ,
                       performance::TraceToken trace
#endif
        ) {
          return publication.publish(event, generation
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                                     ,
                                     trace
#endif
                                     ) ==
                         g9::EventPublishResult::InvariantFailure
                     ? g4::NormalizedEventSinkResult::InvariantFailure
                     : g4::NormalizedEventSinkResult::Continue;
        };
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    options.performance_baseline = std::move(performance_baseline);
#endif
    return options;
  }

  UsdMTransport transport_;
};

[[nodiscard]] MarketKey validate_product_key(MarketKey key) {
  if (key.venue != common_wire::VENUE_BINANCE) {
    throw std::invalid_argument{"ProductRuntime requires Binance venue"};
  }
  if (key.symbol.empty()) {
    throw std::invalid_argument{"ProductRuntime symbol must be non-empty"};
  }

  switch (key.market) {
  case common_wire::MARKET_SPOT:
    if (!g4::spot_stream_symbol(key.symbol).has_value()) {
      throw std::invalid_argument{"ProductRuntime Spot symbol cannot be "
                                  "represented by a Binance route"};
    }
    break;
  case common_wire::MARKET_USD_M_PERPETUAL:
    if (!usdm_stream_symbol(key.symbol).has_value()) {
      throw std::invalid_argument{"ProductRuntime USD-M symbol cannot be "
                                  "represented by a Binance route"};
    }
    break;
  default:
    throw std::invalid_argument{
        "ProductRuntime requires Spot or USD-M perpetual market"};
  }
  return key;
}

[[nodiscard]] g5::RecoveryCoordinatorOptions coordinator_options(
    MarketKey key, g9::EventPublication &publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    ,
    std::shared_ptr<performance::ProductTraceBuffer> performance_baseline
#endif
) {
  g5::RecoveryCoordinatorOptions options;
  options.attempt_factory = [key = std::move(key), &publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                             ,
                             performance_baseline
#endif
  ](g3::MarketRuntime &runtime, const g3::RuntimeClock &clock,
                            std::uint64_t generation)
      -> std::unique_ptr<g5::detail::RecoveryAttempt> {
    if (key.market == common_wire::MARKET_SPOT) {
      return std::make_unique<SpotProductAttempt>(runtime, clock, generation,
                                                  key.symbol, publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                                                  ,
                                                  performance_baseline
#endif
      );
    }
    return std::make_unique<UsdMProductAttempt>(runtime, clock, generation,
                                                key.symbol, publication
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                                                ,
                                                performance_baseline
#endif
    );
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

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
[[nodiscard]] performance::Product measurement_product(const MarketKey &key) {
  return key.market == common_wire::MARKET_SPOT
             ? performance::Product::Spot
             : performance::Product::UsdMPerpetual;
}

[[nodiscard]] std::shared_ptr<performance::ProductTraceBuffer>
make_performance_baseline(const MarketKey &key, const g3::RuntimeClock &clock,
                          performance::PerformanceBaselineLimits limits) {
  return std::make_shared<performance::ProductTraceBuffer>(
      measurement_product(key), [clock] { return clock().monotonic_ns; },
      limits);
}

[[nodiscard]] g3::RuntimeLimits runtime_limits_with_baseline(
    g3::RuntimeLimits limits,
    const std::shared_ptr<performance::ProductTraceBuffer> &baseline) {
  limits.performance_baseline = baseline;
  return limits;
}
#endif

[[nodiscard]] ProductKind product_kind_for(const MarketKey &key) {
  switch (key.market) {
  case common_wire::MARKET_SPOT:
    return ProductKind::Spot;
  case common_wire::MARKET_USD_M_PERPETUAL:
    return ProductKind::UsdMPerpetual;
  default:
    throw std::logic_error{"ProductRuntime has an unsupported market"};
  }
}

[[nodiscard]] g3::adapter::ExpectedIdentity identity_for(const MarketKey &key) {
  switch (key.market) {
  case common_wire::MARKET_SPOT:
    return {key.symbol, core::SequencePolicyKind::Spot};
  case common_wire::MARKET_USD_M_PERPETUAL:
    return {key.symbol, core::SequencePolicyKind::UsdMPerpetual};
  default:
    throw std::logic_error{"ProductRuntime has an unsupported market"};
  }
}

[[nodiscard]] MarketKey legacy_key_for(ProductKind kind) {
  switch (kind) {
  case ProductKind::Spot:
    return spot_btcusdt_key();
  case ProductKind::UsdMPerpetual:
    return usdm_btcusdt_key();
  }
  throw std::invalid_argument{"unsupported legacy product kind"};
}

} // namespace

ProductRuntime::ProductRuntime(MarketKey key, core::NumericSpec numeric_spec,
                               g3::RuntimeClock clock,
                               std::string gateway_instance_id,
                               ProductRuntimeOptions options)
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    : key_{validate_product_key(std::move(key))},
      performance_baseline_{make_performance_baseline(
          key_, clock, options.performance_baseline_limits)},
      runtime_{runtime_limits_with_baseline(options.runtime_limits,
                                            performance_baseline_),
               clock, numeric_spec, identity_for(key_),
               std::move(options.runtime_test)},
      event_publication_{std::move(gateway_instance_id), clock,
                         options.event_limits, performance_baseline_},
      recovery_{
          runtime_, std::move(clock), options.planned_rotation,
          coordinator_options(key_, event_publication_, performance_baseline_),
          std::move(options.recovery_test)} {}
#else
    : key_{validate_product_key(std::move(key))},
      runtime_{options.runtime_limits, clock, numeric_spec, identity_for(key_),
               std::move(options.runtime_test)},
      event_publication_{std::move(gateway_instance_id), clock,
                         options.event_limits},
      recovery_{runtime_, std::move(clock), options.planned_rotation,
                coordinator_options(key_, event_publication_),
                std::move(options.recovery_test)} {
}
#endif

      ProductRuntime::ProductRuntime(
          ProductKind kind, core::NumericSpec numeric_spec,
          g3::RuntimeClock clock, std::string gateway_instance_id,
          ProductRuntimeOptions options)
    : ProductRuntime(legacy_key_for(kind), numeric_spec, std::move(clock),
                     std::move(gateway_instance_id), std::move(options)) {
}

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

const MarketKey &ProductRuntime::key() const noexcept { return key_; }

ProductKind ProductRuntime::kind() const noexcept {
  return product_kind_for(key_);
}

g3::MarketRuntime &ProductRuntime::runtime() noexcept { return runtime_; }

g5::RecoveryCoordinator &ProductRuntime::recovery() noexcept {
  return recovery_;
}

g9::EventPublication &ProductRuntime::event_publication() noexcept {
  return event_publication_;
}

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
const performance::ProductTraceBuffer &
ProductRuntime::performance_baseline() const noexcept {
  return *performance_baseline_;
}
#endif

TwoProductRuntime::TwoProductRuntime(core::NumericSpec spot_numeric_spec,
                                     core::NumericSpec usdm_numeric_spec,
                                     g3::RuntimeClock clock,
                                     std::string gateway_instance_id,
                                     TwoProductRuntimeOptions options)
    : spot_{spot_btcusdt_key(), spot_numeric_spec, clock, gateway_instance_id,
            std::move(options.spot)},
      usdm_{usdm_btcusdt_key(), usdm_numeric_spec, std::move(clock),
            std::move(gateway_instance_id), std::move(options.usdm)},
      registry_{{spot_.key(), &spot_.runtime(), &spot_.recovery(),
                 &spot_.event_publication()},
                {usdm_.key(), &usdm_.runtime(), &usdm_.recovery(),
                 &usdm_.event_publication()}} {}

TwoProductRuntime::~TwoProductRuntime() { stop(); }

TwoProductStartResult TwoProductRuntime::start() {
  const auto spot_result = spot_.start();
  const auto usdm_result = usdm_.start();
  return {spot_result, usdm_result};
}

void TwoProductRuntime::shutdown_publications() noexcept {
  spot_.runtime().close_publication_admission();
  usdm_.runtime().close_publication_admission();
  static_cast<void>(spot_.runtime().shutdown_publication());
  static_cast<void>(usdm_.runtime().shutdown_publication());
  spot_.event_publication().shutdown();
  usdm_.event_publication().shutdown();
}

void TwoProductRuntime::stop() noexcept {
  shutdown_publications();
  spot_.recovery().stop();
  usdm_.recovery().stop();
}

ProductRuntime &TwoProductRuntime::spot() noexcept { return spot_; }

ProductRuntime &TwoProductRuntime::usdm() noexcept { return usdm_; }

const MarketRuntimeRegistry &TwoProductRuntime::registry() const noexcept {
  return registry_;
}

#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
void TwoProductRuntime::write_performance_baseline(std::ostream &output) const {
  spot_.performance_baseline().write_json_lines(output);
  usdm_.performance_baseline().write_json_lines(output);
}
#endif

} // namespace binance_market_data::gateway::g11
