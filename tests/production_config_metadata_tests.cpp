#include "daemon_config.hpp"
#include "production_metadata.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace common = binance_market_data::common::v1;
namespace g4 = binance_market_data::gateway::g4;
namespace g11 = binance_market_data::gateway::g11;
namespace production = binance_market_data::gateway::production;

class TestFailure final : public std::exception {
public:
  explicit TestFailure(std::string message) : message_{std::move(message)} {}
  [[nodiscard]] const char *what() const noexcept override {
    return message_.c_str();
  }

private:
  std::string message_;
};

void require(bool condition, std::string_view expression) {
  if (!condition) {
    throw TestFailure{std::string{expression}};
  }
}

#define REQUIRE(condition) require((condition), #condition)

class ConfigFile final {
public:
  explicit ConfigFile(std::string contents) {
    std::string pattern{"/tmp/bmd-gateway-config-unit-XXXXXX"};
    const auto descriptor = mkstemp(pattern.data());
    if (descriptor < 0) {
      throw std::runtime_error{"mkstemp failed"};
    }
    path_ = pattern;
    std::size_t offset = 0U;
    while (offset < contents.size()) {
      const auto count =
          write(descriptor, contents.data() + offset, contents.size() - offset);
      if (count < 0 && errno == EINTR) {
        continue;
      }
      if (count <= 0) {
        close(descriptor);
        unlink(path_.c_str());
        throw std::runtime_error{"config write failed"};
      }
      offset += static_cast<std::size_t>(count);
    }
    close(descriptor);
  }

  ~ConfigFile() { unlink(path_.c_str()); }
  [[nodiscard]] const std::string &path() const noexcept { return path_; }

private:
  std::string path_;
};

[[nodiscard]] production::DaemonConfigResult load(std::string contents) {
  ConfigFile file{std::move(contents)};
  return production::load_daemon_config(file.path());
}

[[nodiscard]] production::DaemonConfig
require_config(production::DaemonConfigResult result) {
  REQUIRE(std::holds_alternative<production::DaemonConfig>(result));
  return std::get<production::DaemonConfig>(std::move(result));
}

void cli_contract() {
  char program[] = "bmd-gatewayd";
  char help[] = "--help";
  char config[] = "--config";
  char missing[] = "/definitely/missing/bmd-gateway-config.json";
  char old[] = "--grpc-listen";
  char endpoint[] = "127.0.0.1:50051";
  char unknown[] = "--unknown";
  char *help_argv[]{program, help};
  char *missing_argv[]{program};
  char *missing_path_argv[]{program, config};
  char *duplicate_argv[]{program, config, missing, config, missing};
  char *old_argv[]{program, old, endpoint};
  char *unknown_argv[]{program, unknown};
  REQUIRE(std::holds_alternative<production::HelpRequested>(
      production::parse_daemon_config(2, help_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::parse_daemon_config(1, missing_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::parse_daemon_config(2, missing_path_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::parse_daemon_config(5, duplicate_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::parse_daemon_config(3, old_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::parse_daemon_config(2, unknown_argv)));
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(
      production::load_daemon_config(missing)));
}

void strict_json_rejections() {
  const std::vector<std::string> invalid{
      "{",
      "[]",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT"],"usdm_symbols":[],"extra":1})json",
      R"json({"spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[]})json",
      R"json({"grpc_listen":7,"spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":"BTCUSDT","usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[7],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"","spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:0","spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[""],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","grpc_listen":"127.0.0.1:50052","spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","grpc_\u006cisten":"127.0.0.1:50052","spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT","BTCUSDT"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[],"usdm_symbols":["BTCUSDT","BTCUSDT"]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["1","2","3","4","5","6","7","8","9"],"usdm_symbols":[]})json",
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT"],"usdm_symbols":[]} trailing)json"};
  for (const auto &contents : invalid) {
    REQUIRE(
        std::holds_alternative<production::DaemonConfigError>(load(contents)));
  }
}

void config_bound() {
  std::string exact =
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json";
  exact.append(production::kMaximumDaemonConfigBytes - exact.size(), ' ');
  REQUIRE(std::holds_alternative<production::DaemonConfig>(load(exact)));
  exact.push_back(' ');
  REQUIRE(std::holds_alternative<production::DaemonConfigError>(load(exact)));
}

void configuration_identity_and_cardinality() {
  const auto spot_only = require_config(load(
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT"],"usdm_symbols":[]})json"));
  REQUIRE(spot_only.market_keys.size() == 1U);
  REQUIRE(spot_only.market_keys[0] == g11::spot_btcusdt_key());

  const auto usdm_only = require_config(load(
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[],"usdm_symbols":["BTCUSDT"]})json"));
  REQUIRE(usdm_only.market_keys.size() == 1U);
  REQUIRE(usdm_only.market_keys[0] == g11::usdm_btcusdt_key());

  const auto mixed = require_config(load(
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["btcUSDT","BTCUSDT"],"usdm_symbols":["BTCUSDT"]})json"));
  REQUIRE(mixed.market_keys.size() == 3U);
  REQUIRE(mixed.market_keys[0].symbol == "BTCUSDT");
  REQUIRE(mixed.market_keys[1].symbol == "btcUSDT");
  REQUIRE(mixed.market_keys[2] == g11::usdm_btcusdt_key());

  const auto opaque = require_config(load(
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":[" BTCusdt "],"usdm_symbols":[]})json"));
  REQUIRE(opaque.market_keys[0].symbol == " BTCusdt ");

  const auto eight = require_config(load(
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["D","C","B","A"],"usdm_symbols":["d","c","b","a"]})json"));
  REQUIRE(eight.market_keys.size() == 8U);
  REQUIRE(std::is_sorted(eight.market_keys.begin(), eight.market_keys.end(),
                         g11::MarketKeyLess{}));
}

[[nodiscard]] std::string spot_body() {
  return R"json({"symbols":[{"symbol":"BTCUSDT","status":"TRADING","isSpotTradingAllowed":true,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},{"symbol":"ETHUSDT","status":"TRADING","isSpotTradingAllowed":true,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"LOT_SIZE","stepSize":"0.01"}]}]})json";
}

[[nodiscard]] std::string
usdm_body(std::string_view eth_contract = "PERPETUAL") {
  return std::string{
             R"json({"symbols":[{"symbol":"BTCUSDT","pair":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},{"symbol":"ETHUSDT","pair":"ETHUSD","contractType":")json"} +
         std::string{eth_contract} +
         R"json(","status":"TRADING","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.01"}]}]})json";
}

struct FetchCounts final {
  int spot{0};
  int usdm{0};
};

[[nodiscard]] production::MetadataSources
sources(FetchCounts &counts, std::string spot, std::string usdm) {
  production::MetadataSources result;
  result.spot_fetch = [&counts, body = std::move(spot)] {
    ++counts.spot;
    return g4::ExchangeInfoResult{g4::ExchangeInfoResponse{body, true}};
  };
  result.usdm_fetch = [&counts, body = std::move(usdm)] {
    ++counts.usdm;
    return g4::ExchangeInfoResult{g4::ExchangeInfoResponse{body, true}};
  };
  return result;
}

void metadata_fetch_counts_and_pairing() {
  const g11::MarketKey spot_eth{common::VENUE_BINANCE, common::MARKET_SPOT,
                                "ETHUSDT"};
  const g11::MarketKey usdm_eth{common::VENUE_BINANCE,
                                common::MARKET_USD_M_PERPETUAL, "ETHUSDT"};
  {
    FetchCounts counts;
    const auto result = production::acquire_production_metadata(
        {spot_eth, g11::spot_btcusdt_key()},
        sources(counts, spot_body(), usdm_body()));
    REQUIRE(std::holds_alternative<production::ProductionMetadata>(result));
    REQUIRE(counts.spot == 1);
    REQUIRE(counts.usdm == 0);
  }
  {
    FetchCounts counts;
    const auto result = production::acquire_production_metadata(
        {usdm_eth, g11::usdm_btcusdt_key()},
        sources(counts, spot_body(), usdm_body()));
    REQUIRE(std::holds_alternative<production::ProductionMetadata>(result));
    REQUIRE(counts.spot == 0);
    REQUIRE(counts.usdm == 1);
  }
  {
    FetchCounts counts;
    const auto result = production::acquire_production_metadata(
        {usdm_eth, g11::spot_btcusdt_key(), spot_eth, g11::usdm_btcusdt_key()},
        sources(counts, spot_body(), usdm_body()));
    REQUIRE(std::holds_alternative<production::ProductionMetadata>(result));
    REQUIRE(counts.spot == 1);
    REQUIRE(counts.usdm == 1);
    const auto &products =
        std::get<production::ProductionMetadata>(result).products;
    REQUIRE(products.size() == 4U);
    REQUIRE(products[0].key == g11::spot_btcusdt_key());
    REQUIRE(products[0].numeric_spec.price_scale.value() == 2U);
    REQUIRE(products[1].key == spot_eth);
    REQUIRE(products[1].numeric_spec.price_scale.value() == 1U);
    REQUIRE(products[2].key == g11::usdm_btcusdt_key());
    REQUIRE(products[2].numeric_spec.price_scale.value() == 1U);
    REQUIRE(products[3].key == usdm_eth);
    REQUIRE(products[3].numeric_spec.price_scale.value() == 2U);
    const auto specifications = production::make_product_runtime_specs(
        std::get<production::ProductionMetadata>(result));
    REQUIRE(specifications.size() == 4U);
    REQUIRE(specifications[0].key == g11::spot_btcusdt_key());
    REQUIRE(specifications[3].key == usdm_eth);
  }
}

void metadata_fails_all_or_nothing() {
  const g11::MarketKey spot_eth{common::VENUE_BINANCE, common::MARKET_SPOT,
                                "ETHUSDT"};
  const g11::MarketKey usdm_eth{common::VENUE_BINANCE,
                                common::MARKET_USD_M_PERPETUAL, "ETHUSDT"};
  {
    FetchCounts counts;
    const auto result = production::acquire_production_metadata(
        {g11::spot_btcusdt_key(),
         g11::MarketKey{common::VENUE_BINANCE, common::MARKET_SPOT, "MISSING"}},
        sources(counts, spot_body(), usdm_body()));
    REQUIRE(std::holds_alternative<production::MetadataError>(result));
    const auto &failure = std::get<production::MetadataError>(result);
    REQUIRE(failure.stage == production::MetadataStage::SpotParse);
    REQUIRE(failure.product.has_value());
    REQUIRE(failure.product->symbol == "MISSING");
  }
  {
    FetchCounts counts;
    auto malformed = spot_body();
    malformed.pop_back();
    const auto result = production::acquire_production_metadata(
        {spot_eth}, sources(counts, malformed, usdm_body()));
    REQUIRE(std::holds_alternative<production::MetadataError>(result));
  }
  {
    FetchCounts counts;
    auto ineligible = spot_body();
    const auto position = ineligible.find("TRADING");
    REQUIRE(position != std::string::npos);
    ineligible.replace(position, 7U, "BREAK");
    const auto result = production::acquire_production_metadata(
        {g11::spot_btcusdt_key()}, sources(counts, ineligible, usdm_body()));
    REQUIRE(std::holds_alternative<production::MetadataError>(result));
  }
  {
    FetchCounts counts;
    const auto result = production::acquire_production_metadata(
        {usdm_eth}, sources(counts, spot_body(), usdm_body("CURRENT_QUARTER")));
    REQUIRE(std::holds_alternative<production::MetadataError>(result));
    REQUIRE(std::get<production::MetadataError>(result).product == usdm_eth);
  }
  {
    FetchCounts counts;
    auto invalid = spot_body();
    const auto position = invalid.find("0.01");
    REQUIRE(position != std::string::npos);
    invalid.replace(position, 4U, "0.0000000000000000001");
    const auto result = production::acquire_production_metadata(
        {g11::spot_btcusdt_key()}, sources(counts, invalid, usdm_body()));
    REQUIRE(std::holds_alternative<production::MetadataError>(result));
  }
  {
    FetchCounts counts;
    const g11::MarketKey lower{common::VENUE_BINANCE, common::MARKET_SPOT,
                               "btcusdt"};
    const auto result = production::acquire_production_metadata(
        {lower}, sources(counts, spot_body(), usdm_body()));
    REQUIRE(std::holds_alternative<production::MetadataError>(result));
    REQUIRE(std::get<production::MetadataError>(result).product == lower);
  }
}

} // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"CONFIG_CLI_CONTRACT", cli_contract},
      {"CONFIG_STRICT_JSON_REJECTIONS", strict_json_rejections},
      {"CONFIG_64_KIB_BOUND", config_bound},
      {"CONFIG_IDENTITY_AND_CARDINALITY",
       configuration_identity_and_cardinality},
      {"METADATA_FETCH_COUNTS_AND_PAIRING", metadata_fetch_counts_and_pairing},
      {"METADATA_FAILS_ALL_OR_NOTHING", metadata_fails_all_or_nothing},
  };
  for (const auto &[name, test] : tests) {
    try {
      test();
      std::cout << name << "=PASS\n";
    } catch (const std::exception &failure) {
      std::cerr << name << "=FAIL " << failure.what() << '\n';
      return 1;
    }
  }
  return 0;
}
