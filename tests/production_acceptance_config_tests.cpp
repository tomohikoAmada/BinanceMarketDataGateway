#include "acceptance_config.hpp"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace acceptance = binance_market_data::gateway::production::acceptance;

namespace {

class ConfigFile final {
public:
  explicit ConfigFile(std::string_view contents) {
    path_ = "/tmp/bmd-acceptance-config-XXXXXX";
    const auto descriptor = mkstemp(path_.data());
    acceptance::require(descriptor >= 0, "mkstemp failed");
    const auto written = write(descriptor, contents.data(), contents.size());
    close(descriptor);
    if (written != static_cast<ssize_t>(contents.size())) {
      unlink(path_.c_str());
      throw acceptance::AcceptanceFailure{"config write failed"};
    }
  }
  ~ConfigFile() { unlink(path_.c_str()); }
  [[nodiscard]] const std::string &path() const noexcept { return path_; }

private:
  std::string path_;
};

void rejects(const std::function<void()> &operation) {
  bool rejected = false;
  try {
    operation();
  } catch (const acceptance::AcceptanceFailure &) {
    rejected = true;
  }
  acceptance::require(rejected, "invalid acceptance input was accepted");
}

acceptance::Options options(std::vector<std::string> arguments) {
  std::vector<char *> pointers;
  for (auto &argument : arguments) {
    pointers.push_back(argument.data());
  }
  return acceptance::parse_options(static_cast<int>(pointers.size()),
                                   pointers.data());
}

void config_cli(const std::string &executable) {
  const ConfigFile two_products{
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT"],"usdm_symbols":["BTCUSDT"]})json"};
  const auto parsed =
      options({executable, "--daemon", executable, "--config",
               two_products.path(), "--grpc-target", "127.0.0.1:50051"});
  acceptance::require(parsed.config_path == two_products.path() &&
                          parsed.grpc_port == 50051U,
                      "config CLI lost its startup authority");
  rejects([&] {
    static_cast<void>(options({executable, "--daemon", executable,
                               "--grpc-target", "127.0.0.1:50051"}));
  });
  rejects([&] {
    static_cast<void>(
        options({executable, "--daemon", executable, "--config",
                 two_products.path(), "--grpc-target", "127.0.0.1:50052"}));
  });
  const ConfigFile four_products{
      R"json({"grpc_listen":"127.0.0.1:50051","spot_symbols":["BTCUSDT","ETHUSDT"],"usdm_symbols":["BTCUSDT","ETHUSDT"]})json"};
  rejects([&] {
    static_cast<void>(
        options({executable, "--daemon", executable, "--config",
                 four_products.path(), "--grpc-target", "127.0.0.1:50051"}));
  });
}

void current_serving_identity() {
  const auto identity = acceptance::parse_serving_line(
      "gateway_state=serving products=2 grpc_port=50051 context_limit=48 "
      "gateway_instance_id=gw-test",
      50051U);
  acceptance::require(identity.gateway_instance_id == "gw-test" &&
                          identity.spot_generation == 0U &&
                          identity.usdm_generation == 0U,
                      "generations must be obtained from initial status");
  rejects([] {
    static_cast<void>(acceptance::parse_serving_line(
        "gateway_state=serving products=4 grpc_port=50051 "
        "gateway_instance_id=gw-test",
        50051U));
  });
  rejects([] {
    static_cast<void>(acceptance::parse_serving_line(
        "gateway_state=serving products=2 grpc_port=50052 "
        "gateway_instance_id=gw-test",
        50051U));
  });
}

} // namespace

int main(int argc, char **argv) {
  try {
    acceptance::require(argc >= 1, "missing executable path");
    config_cli(argv[0]);
    current_serving_identity();
    std::cout << "ACCEPTANCE_CONFIG_AND_SERVING_IDENTITY=PASS\n";
    return EXIT_SUCCESS;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
