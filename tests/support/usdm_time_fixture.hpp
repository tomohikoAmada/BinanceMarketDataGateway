#pragma once

#include <nlohmann/json.hpp>

#include <fstream>
#include <stdexcept>

namespace binance_market_data::gateway::test_support {

// Byte-identical copy of Contracts C0's shared source oracle; see
// fixtures/README.md.
inline nlohmann::json usdm_time_fixture() {
  std::ifstream input{BMD_GATEWAY_TEST_FIXTURE_DIR "/usdm-depth-time.v1.json"};
  if (!input) {
    throw std::runtime_error{"missing Contracts C0 source-time fixture"};
  }
  return nlohmann::json::parse(input);
}

inline nlohmann::json source_case(const nlohmann::json &fixture,
                                  const nlohmann::json &test_case) {
  auto raw = fixture.at("raw");
  raw.update(test_case.at("source_patch"));
  for (const auto &field : test_case.at("remove_source_fields")) {
    raw.erase(field.get<std::string>());
  }
  return raw;
}

} // namespace binance_market_data::gateway::test_support
