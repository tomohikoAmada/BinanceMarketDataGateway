#include "synthetic_spot_host.hpp"

#include <cstdlib>
#include <utility>

namespace binance_market_data::gateway::g2 {

core::NumericSpec SyntheticSpotHost::make_numeric_spec() {
  const auto price_scale = core::DecimalScale::create(2U);
  const auto quantity_scale = core::DecimalScale::create(3U);
  if (!price_scale.has_value() || !quantity_scale.has_value()) {
    std::abort();
  }
  return {*price_scale, *quantity_scale};
}

adapter::ExpectedIdentity SyntheticSpotHost::make_expected_identity() {
  return {"BTCUSDT", core::SequencePolicyKind::Spot};
}

adapter::SnapshotContext SyntheticSpotHost::make_snapshot_context(
    const adapter::ExpectedIdentity &expected_identity) {
  return {expected_identity,
          "gateway-g2-synthetic",
          "1.0.0",
          adapter::SnapshotOrigin::GatewayLive,
          1700000000123456000ULL,
          std::uint64_t{1700000000123456789ULL},
          std::nullopt};
}

SyntheticSpotHost::SyntheticSpotHost()
    : numeric_spec_{make_numeric_spec()},
      expected_identity_{make_expected_identity()},
      projection_{numeric_spec_, core::SequencePolicyKind::Spot},
      snapshot_context_{make_snapshot_context(expected_identity_)},
      snapshot_options_{} {}

void SyntheticSpotHost::receive_pre_snapshot(market::DepthUpdate update) {
  buffered_updates_.push_back(std::move(update));
}

InstallOutcome SyntheticSpotHost::install_snapshot(
    const market::ExchangeDepthSnapshot &snapshot) {
  auto adapted = adapter::adapt_exchange_depth_snapshot(snapshot, numeric_spec_,
                                                        expected_identity_);
  if (std::holds_alternative<adapter::AdapterError>(adapted)) {
    return std::get<adapter::AdapterError>(adapted);
  }
  return std::get<adapter::AdaptedBookBaseline>(adapted).install_into(
      projection_);
}

std::vector<ApplyOutcome> SyntheticSpotHost::replay_buffered_updates() {
  std::vector<ApplyOutcome> outcomes;
  outcomes.reserve(buffered_updates_.size());
  for (const auto &update : buffered_updates_) {
    outcomes.push_back(apply_live_update(update));
  }
  return outcomes;
}

ApplyOutcome
SyntheticSpotHost::apply_live_update(const market::DepthUpdate &update) {
  auto adapted =
      adapter::adapt_depth_update(update, numeric_spec_, expected_identity_);
  if (std::holds_alternative<adapter::AdapterError>(adapted)) {
    return std::get<adapter::AdapterError>(adapted);
  }
  return std::get<adapter::AdaptedDepthBatch>(adapted).apply_to(projection_);
}

SnapshotOutcome SyntheticSpotHost::make_snapshot() const {
  return adapter::make_local_order_book_snapshot(projection_, snapshot_context_,
                                                 snapshot_options_);
}

void SyntheticSpotHost::reset() noexcept {
  projection_.reset();
  buffered_updates_.clear();
}

const core::BookProjection &SyntheticSpotHost::projection() const noexcept {
  return projection_;
}

core::NumericSpec SyntheticSpotHost::numeric_spec() const noexcept {
  return numeric_spec_;
}

const adapter::ExpectedIdentity &
SyntheticSpotHost::expected_identity() const noexcept {
  return expected_identity_;
}

std::size_t SyntheticSpotHost::buffered_update_count() const noexcept {
  return buffered_updates_.size();
}

} // namespace binance_market_data::gateway::g2
