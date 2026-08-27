#include "binance_market_data/gateway/v1/synthetic_host.hpp"

#include <utility>

namespace binance_market_data::gateway::v1 {
namespace {

namespace adapter = projection_adapter::v1;
namespace core = projection::v1;
namespace market_wire = market::v1;

constexpr std::uint32_t kPriceScale = 2U;
constexpr std::uint32_t kQuantityScale = 3U;
constexpr std::uint64_t kSyntheticGeneratedTimeUtcNs =
    1'700'000'000'000'000'000ULL;
constexpr std::uint64_t kSyntheticGeneratedMonotonicNs = 42'000'000ULL;

[[nodiscard]] core::DecimalScale scale(std::uint32_t value) {
  return core::DecimalScale::create(value).value();
}

[[nodiscard]] bool
accepted_by_projection(const core::ApplyResult &result,
                       SyntheticHostState host_state) noexcept {
  switch (result.disposition) {
  case core::ApplyDisposition::Applied:
    return result.status_after == core::ProjectionStatus::Synchronized;
  case core::ApplyDisposition::IgnoredStale:
  case core::ApplyDisposition::IgnoredDuplicate:
    return result.status_after == core::ProjectionStatus::Synchronized ||
           (host_state == SyntheticHostState::ReplayingBuffer &&
            result.status_after == core::ProjectionStatus::AwaitingBridge);
  case core::ApplyDisposition::GapDetected:
  case core::ApplyDisposition::RejectedWrongState:
    return false;
  }
  return false;
}

} // namespace

SyntheticSpotBtcusdtHost::SyntheticSpotBtcusdtHost()
    : projection_{numeric_spec(), core::SequencePolicyKind::Spot} {}

core::ProjectionStatus
SyntheticSpotBtcusdtHost::projection_status() const noexcept {
  return projection_.status();
}

core::NumericSpec SyntheticSpotBtcusdtHost::numeric_spec() {
  return {scale(kPriceScale), scale(kQuantityScale)};
}

adapter::ExpectedIdentity SyntheticSpotBtcusdtHost::expected_identity() {
  return {"BTCUSDT", core::SequencePolicyKind::Spot};
}

adapter::SnapshotContext SyntheticSpotBtcusdtHost::snapshot_context() {
  return {expected_identity(),
          "gateway-g2-synthetic",
          "1",
          adapter::SnapshotOrigin::GatewayLive,
          kSyntheticGeneratedTimeUtcNs,
          kSyntheticGeneratedMonotonicNs,
          std::nullopt};
}

SyntheticHostResult SyntheticSpotBtcusdtHost::invalid_state() const noexcept {
  return SyntheticHostError{SyntheticHostErrorCode::InvalidState, state_};
}

SyntheticHostResult SyntheticSpotBtcusdtHost::processing_failure(
    SyntheticHostErrorCode code) noexcept {
  state_ = SyntheticHostState::Failed;
  return SyntheticHostError{code, state_};
}

SyntheticHostResult
SyntheticSpotBtcusdtHost::receive_depth_update(const DepthUpdate &update) {
  if (state_ == SyntheticHostState::Buffering) {
    pre_snapshot_buffer_.push_back(update);
    return state_;
  }
  if (state_ == SyntheticHostState::Live) {
    return apply_update(update);
  }
  return invalid_state();
}

SyntheticHostResult SyntheticSpotBtcusdtHost::install_snapshot(
    const market_wire::ExchangeDepthSnapshot &snapshot) {
  if (state_ != SyntheticHostState::Buffering) {
    return invalid_state();
  }

  auto adapted = adapter::adapt_exchange_depth_snapshot(
      snapshot, numeric_spec(), expected_identity());
  if (!std::holds_alternative<adapter::AdaptedBookBaseline>(adapted)) {
    return processing_failure(SyntheticHostErrorCode::AdapterRejected);
  }

  auto baseline = std::move(std::get<adapter::AdaptedBookBaseline>(adapted));
  const auto installed = baseline.install_into(projection_);
  if (!std::holds_alternative<core::InstallResult>(installed) ||
      std::get<core::InstallResult>(installed).disposition !=
          core::InstallDisposition::Installed ||
      std::get<core::InstallResult>(installed).status_after !=
          core::ProjectionStatus::AwaitingBridge) {
    return processing_failure(SyntheticHostErrorCode::BaselineRejected);
  }

  ++baseline_install_count_;
  state_ = SyntheticHostState::BaselineInstalled;
  return state_;
}

SyntheticHostResult SyntheticSpotBtcusdtHost::replay_buffer() {
  if (state_ != SyntheticHostState::BaselineInstalled) {
    return invalid_state();
  }

  state_ = SyntheticHostState::ReplayingBuffer;
  for (const auto &update : pre_snapshot_buffer_) {
    const auto result = apply_update(update);
    if (std::holds_alternative<SyntheticHostError>(result)) {
      return result;
    }
  }
  pre_snapshot_buffer_.clear();

  if (projection_.status() != core::ProjectionStatus::Synchronized) {
    return processing_failure(SyntheticHostErrorCode::BootstrapNotSynchronized);
  }

  state_ = SyntheticHostState::Live;
  return state_;
}

SyntheticHostResult
SyntheticSpotBtcusdtHost::apply_update(const DepthUpdate &update) {
  auto adapted =
      adapter::adapt_depth_update(update, numeric_spec(), expected_identity());
  if (!std::holds_alternative<adapter::AdaptedDepthBatch>(adapted)) {
    return processing_failure(SyntheticHostErrorCode::AdapterRejected);
  }

  auto batch = std::move(std::get<adapter::AdaptedDepthBatch>(adapted));
  const auto result = batch.apply_to(projection_);
  if (!std::holds_alternative<core::ApplyResult>(result)) {
    return processing_failure(SyntheticHostErrorCode::ProjectionRejected);
  }

  last_projection_apply_ = std::get<core::ApplyResult>(result);
  if (!accepted_by_projection(*last_projection_apply_, state_)) {
    return processing_failure(SyntheticHostErrorCode::ProjectionRejected);
  }
  return state_;
}

adapter::AdapterResult<core::LocalOrderBookSnapshot>
SyntheticSpotBtcusdtHost::local_order_book_snapshot() const {
  return adapter::make_local_order_book_snapshot(projection_,
                                                 snapshot_context(), {});
}

} // namespace binance_market_data::gateway::v1
