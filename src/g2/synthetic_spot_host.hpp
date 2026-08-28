#pragma once

#include <binance_market_data/market/v1/market_events.pb.h>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>
#include <binance_market_data/projection_adapter/v1/proto_adapter.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace binance_market_data::gateway::g2 {

namespace adapter = projection_adapter::v1;
namespace core = projection::v1;
namespace market = ::binance_market_data::market::v1;

using InstallOutcome = adapter::AdapterResult<core::InstallResult>;
using ApplyOutcome = adapter::AdapterResult<core::ApplyResult>;
using SnapshotOutcome = adapter::AdapterResult<core::LocalOrderBookSnapshot>;

class SyntheticSpotHost final {
public:
  SyntheticSpotHost();

  void receive_pre_snapshot(market::DepthUpdate update);

  [[nodiscard]] InstallOutcome
  install_snapshot(const market::ExchangeDepthSnapshot &snapshot);

  [[nodiscard]] std::vector<ApplyOutcome> replay_buffered_updates();

  [[nodiscard]] ApplyOutcome
  apply_live_update(const market::DepthUpdate &update);

  [[nodiscard]] SnapshotOutcome make_snapshot() const;

  void reset() noexcept;

  [[nodiscard]] const core::BookProjection &projection() const noexcept;
  [[nodiscard]] core::NumericSpec numeric_spec() const noexcept;
  [[nodiscard]] const adapter::ExpectedIdentity &
  expected_identity() const noexcept;
  [[nodiscard]] std::size_t buffered_update_count() const noexcept;

private:
  [[nodiscard]] static core::NumericSpec make_numeric_spec();
  [[nodiscard]] static adapter::ExpectedIdentity make_expected_identity();
  [[nodiscard]] static adapter::SnapshotContext
  make_snapshot_context(const adapter::ExpectedIdentity &expected_identity);

  core::NumericSpec numeric_spec_;
  adapter::ExpectedIdentity expected_identity_;
  core::BookProjection projection_;
  std::vector<market::DepthUpdate> buffered_updates_;
  adapter::SnapshotContext snapshot_context_;
  adapter::SnapshotOptions snapshot_options_;
};

} // namespace binance_market_data::gateway::g2
