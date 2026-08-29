#pragma once

#include <cstdint>
#include <optional>

namespace binance_market_data::gateway::g3 {

// Gateway-internal source identity carried with an admitted source message.
// It is publication provenance only; it has no continuity semantics.
struct SourceProvenance final {
  std::optional<std::uint64_t> connection_generation;

  friend constexpr bool operator==(SourceProvenance,
                                   SourceProvenance) noexcept = default;
};

} // namespace binance_market_data::gateway::g3
