#include "daemon_config.hpp"

#include <binance_market_data/gateway/v1/config.hpp>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <fstream>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <utility>

namespace binance_market_data::gateway::production {

namespace {

using Json = nlohmann::json;

[[nodiscard]] DaemonConfigError error(std::string message) {
  return {std::move(message)};
}

[[nodiscard]] std::variant<std::string, DaemonConfigError>
read_config_file(std::string_view path) {
  std::ifstream input{std::string{path}, std::ios::in | std::ios::binary};
  if (!input) {
    return error("unable to open config file");
  }
  std::array<char, kMaximumDaemonConfigBytes + 1U> buffer{};
  input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
  const auto count = static_cast<std::size_t>(input.gcount());
  if (input.bad()) {
    return error("unable to read config file");
  }
  if (count > kMaximumDaemonConfigBytes) {
    return error("config file exceeds 64 KiB limit");
  }
  return std::string{buffer.data(), count};
}

[[nodiscard]] std::variant<Json, DaemonConfigError>
parse_strict_json(const std::string &contents) {
  bool duplicate_top_level_key = false;
  std::set<std::string> top_level_keys;
  const auto callback = [&duplicate_top_level_key,
                         &top_level_keys](int depth, Json::parse_event_t event,
                                          Json &parsed) {
    if (depth == 1 && event == Json::parse_event_t::key) {
      const auto [unused, inserted] =
          top_level_keys.insert(parsed.get<std::string>());
      static_cast<void>(unused);
      duplicate_top_level_key = duplicate_top_level_key || !inserted;
    }
    return true;
  };
  Json parsed = Json::parse(contents, callback, false, false);
  if (parsed.is_discarded()) {
    return error("malformed JSON");
  }
  if (duplicate_top_level_key) {
    return error("duplicate top-level JSON key");
  }
  return parsed;
}

[[nodiscard]] std::optional<DaemonConfigError> validate_keys(const Json &root) {
  static const std::set<std::string> required{"grpc_listen", "spot_symbols",
                                              "usdm_symbols"};
  if (!root.is_object()) {
    return error("config root must be an object");
  }
  for (const auto &[key, unused] : root.items()) {
    static_cast<void>(unused);
    if (!required.contains(key)) {
      return error("unknown top-level key: " + key);
    }
  }
  for (const auto &key : required) {
    if (!root.contains(key)) {
      return error("missing required top-level key: " + key);
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::variant<std::vector<std::string>, DaemonConfigError>
symbols(const Json &root, std::string_view field) {
  const auto &value = root.at(std::string{field});
  if (!value.is_array()) {
    return error(std::string{field} + " must be an array of strings");
  }
  std::vector<std::string> result;
  result.reserve(value.size());
  for (const auto &entry : value) {
    if (!entry.is_string()) {
      return error(std::string{field} + " must be an array of strings");
    }
    auto symbol = entry.get<std::string>();
    if (symbol.empty()) {
      return error(std::string{field} + " contains an empty symbol");
    }
    result.push_back(std::move(symbol));
  }
  return result;
}

} // namespace

DaemonConfigResult parse_daemon_config(int argc, char **argv) {
  if (argc == 2 && std::string_view{argv[1]} == "--help") {
    return HelpRequested{};
  }

  std::string config_path;
  bool config_seen = false;
  for (int index = 1; index < argc; ++index) {
    const std::string_view option{argv[index]};
    if (option != "--config") {
      return DaemonConfigError{"unknown option: " + std::string{option}};
    }
    if (config_seen) {
      return DaemonConfigError{"duplicate option: --config"};
    }
    if (index + 1 >= argc || std::string_view{argv[index + 1]}.empty()) {
      return DaemonConfigError{"missing value for option: --config"};
    }
    config_path = argv[++index];
    config_seen = true;
  }
  if (!config_seen) {
    return DaemonConfigError{"--config is required"};
  }
  return load_daemon_config(config_path);
}

DaemonConfigResult load_daemon_config(std::string_view config_path) {
  const auto loaded = read_config_file(config_path);
  if (const auto *failure = std::get_if<DaemonConfigError>(&loaded)) {
    return *failure;
  }
  const auto parsed = parse_strict_json(std::get<std::string>(loaded));
  if (const auto *failure = std::get_if<DaemonConfigError>(&parsed)) {
    return *failure;
  }
  const auto &root = std::get<Json>(parsed);
  if (const auto failure = validate_keys(root); failure.has_value()) {
    return *failure;
  }
  if (!root.at("grpc_listen").is_string()) {
    return error("grpc_listen must be a string");
  }
  auto listen = root.at("grpc_listen").get<std::string>();
  if (listen.empty()) {
    return error("grpc_listen must not be empty");
  }
  const auto endpoint = v1::parse_listen_endpoint(listen);
  if (const auto *error = std::get_if<v1::ConfigError>(&endpoint)) {
    return DaemonConfigError{"invalid grpc_listen [" + error->field +
                             "]: " + error->message};
  }

  auto spot = symbols(root, "spot_symbols");
  if (const auto *failure = std::get_if<DaemonConfigError>(&spot)) {
    return *failure;
  }
  auto usdm = symbols(root, "usdm_symbols");
  if (const auto *failure = std::get_if<DaemonConfigError>(&usdm)) {
    return *failure;
  }

  std::vector<g11::MarketKey> keys;
  keys.reserve(std::get<std::vector<std::string>>(spot).size() +
               std::get<std::vector<std::string>>(usdm).size());
  for (auto &symbol : std::get<std::vector<std::string>>(spot)) {
    keys.push_back({g11::common_wire::VENUE_BINANCE,
                    g11::common_wire::MARKET_SPOT, std::move(symbol)});
  }
  for (auto &symbol : std::get<std::vector<std::string>>(usdm)) {
    keys.push_back({g11::common_wire::VENUE_BINANCE,
                    g11::common_wire::MARKET_USD_M_PERPETUAL,
                    std::move(symbol)});
  }
  if (keys.empty() || keys.size() > g11::kMaximumConfiguredProducts) {
    return error("configured products require between one and eight symbols");
  }
  std::sort(keys.begin(), keys.end(), g11::MarketKeyLess{});
  if (std::adjacent_find(keys.begin(), keys.end()) != keys.end()) {
    return error("configured products contain a duplicate exact MarketKey");
  }
  return DaemonConfig{std::move(listen), std::move(keys)};
}

void print_daemon_usage(std::ostream &output) {
  output << "Usage: bmd-gatewayd --config PATH\n"
            "\n"
            "Loads one strict startup-only JSON configuration file.\n";
}

} // namespace binance_market_data::gateway::production
