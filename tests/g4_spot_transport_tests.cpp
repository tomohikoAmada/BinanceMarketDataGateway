#include "spot_transport.hpp"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <variant>

namespace {

namespace core = binance_market_data::projection::v1;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;

[[nodiscard]] core::NumericSpec numeric_spec() {
  const auto price = core::DecimalScale::create(2U);
  const auto quantity = core::DecimalScale::create(5U);
  if (!price.has_value() || !quantity.has_value()) {
    std::abort();
  }
  return {*price, *quantity};
}

} // namespace

int main() {
  g3::RuntimeClock clock = [] {
    return g3::ClockSample{1700000000123456000ULL, 9000000000999ULL};
  };
  g3::MarketRuntime runtime{{4U, 4U}, clock, numeric_spec()};
  g4::SpotTransport transport{runtime, clock};

  const auto before = transport.observe();
  if (before.started || before.running || before.stopped ||
      before.connection_generation != 1U || before.connection_id.empty()) {
    return EXIT_FAILURE;
  }
  transport.stop();
  transport.stop();
  const auto after = transport.observe();
  if (!after.stopped || after.running || after.started ||
      after.connection_generation != 1U ||
      after.connection_id != before.connection_id ||
      after.terminal_error.has_value()) {
    return EXIT_FAILURE;
  }
  runtime.stop();
  std::cout << "CLEAN_TRANSPORT_STOP_CORE=PASS\n";
  return EXIT_SUCCESS;
}
