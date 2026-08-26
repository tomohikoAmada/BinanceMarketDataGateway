#pragma once

#include <binance_market_data/market/v1/market_events.pb.h>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>
#include <binance_market_data/projection_adapter/v1/proto_adapter.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::v1 {

enum class SyntheticHostState : std::uint8_t {
  Buffering,
  BaselineInstalled,
  ReplayingBuffer,
  Live,
  Failed,
};

enum class SyntheticHostErrorCode : std::uint8_t {
  InvalidState,
  AdapterRejected,
  BaselineRejected,
  ProjectionRejected,
  BootstrapNotSynchronized,
};

struct SyntheticHostError final {
  SyntheticHostErrorCode code;
  SyntheticHostState state;

  friend constexpr bool operator==(const SyntheticHostError &,
                                   const SyntheticHostError &) = default;
};

using SyntheticHostResult =
    std::variant<SyntheticHostState, SyntheticHostError>;

// A deterministic, single-symbol G2 host. It accepts only in-memory Contracts
// messages and delegates numeric, sequence, and order-book behavior to
// Projection.
class SyntheticSpotBtcusdtHost final {
public:
  SyntheticSpotBtcusdtHost();

  SyntheticSpotBtcusdtHost(SyntheticSpotBtcusdtHost &&) noexcept = default;
  SyntheticSpotBtcusdtHost &
  operator=(SyntheticSpotBtcusdtHost &&) noexcept = default;
  SyntheticSpotBtcusdtHost(const SyntheticSpotBtcusdtHost &) = delete;
  SyntheticSpotBtcusdtHost &
  operator=(const SyntheticSpotBtcusdtHost &) = delete;
  ~SyntheticSpotBtcusdtHost() = default;

  [[nodiscard]] SyntheticHostState state() const noexcept { return state_; }
  [[nodiscard]] projection::v1::ProjectionStatus
  projection_status() const noexcept;
  [[nodiscard]] std::size_t buffered_depth_update_count() const noexcept {
    return pre_snapshot_buffer_.size();
  }
  [[nodiscard]] std::size_t baseline_install_count() const noexcept {
    return baseline_install_count_;
  }
  [[nodiscard]] const std::optional<projection::v1::ApplyResult> &
  last_projection_apply() const noexcept {
    return last_projection_apply_;
  }

  [[nodiscard]] SyntheticHostResult
  receive_depth_update(const market::v1::DepthUpdate &update);
  [[nodiscard]] SyntheticHostResult
  install_snapshot(const market::v1::ExchangeDepthSnapshot &snapshot);
  [[nodiscard]] SyntheticHostResult replay_buffer();

  // Returns the real Contracts LocalOrderBookSnapshot produced by Projection's
  // adapter. Its context is fixed synthetic metadata so repeated runs have
  // byte-identical output.
  [[nodiscard]] projection_adapter::v1::AdapterResult<
      projection::v1::LocalOrderBookSnapshot>
  local_order_book_snapshot() const;

private:
  using CoreProjection = projection::v1::BookProjection;
  using DepthUpdate = market::v1::DepthUpdate;

  [[nodiscard]] static projection::v1::NumericSpec numeric_spec();
  [[nodiscard]] static projection_adapter::v1::ExpectedIdentity
  expected_identity();
  [[nodiscard]] static projection_adapter::v1::SnapshotContext
  snapshot_context();

  [[nodiscard]] SyntheticHostResult invalid_state() const noexcept;
  [[nodiscard]] SyntheticHostResult
  processing_failure(SyntheticHostErrorCode code) noexcept;
  [[nodiscard]] SyntheticHostResult apply_update(const DepthUpdate &update);

  SyntheticHostState state_{SyntheticHostState::Buffering};
  CoreProjection projection_;
  std::vector<DepthUpdate> pre_snapshot_buffer_;
  std::size_t baseline_install_count_{0U};
  std::optional<projection::v1::ApplyResult> last_projection_apply_;
};

} // namespace binance_market_data::gateway::v1
