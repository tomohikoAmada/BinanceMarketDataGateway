#include "daemon_config.hpp"
#include "daemon_runtime.hpp"
#include "production_test_support.hpp"
#include "termination_signals.hpp"

#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <ostream>
#include <streambuf>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>

namespace production = binance_market_data::gateway::production;
namespace support = production::test_support;

namespace {

class ServingTriggerBuffer final : public std::streambuf {
public:
  ServingTriggerBuffer(std::streambuf *destination,
                       std::function<void()> on_serving)
      : destination_{destination}, on_serving_{std::move(on_serving)} {}

protected:
  int_type overflow(int_type character) override {
    if (!traits_type::eq_int_type(character, traits_type::eof())) {
      pending_.push_back(traits_type::to_char_type(character));
    }
    return character;
  }

  std::streamsize xsputn(const char *data, std::streamsize size) override {
    pending_.append(data, static_cast<std::size_t>(size));
    return size;
  }

  int sync() override {
    if (!triggered_ &&
        pending_.find("gateway_state=serving") != std::string::npos) {
      triggered_ = true;
      on_serving_();
    }
    const auto written = destination_->sputn(
        pending_.data(), static_cast<std::streamsize>(pending_.size()));
    const auto expected = static_cast<std::streamsize>(pending_.size());
    pending_.clear();
    return written == expected && destination_->pubsync() == 0 ? 0 : -1;
  }

private:
  std::streambuf *destination_;
  std::function<void()> on_serving_;
  std::string pending_;
  bool triggered_{false};
};

} // namespace

int main(int argc, char **argv) {
  const auto parsed = production::parse_daemon_config(argc, argv);
  if (std::holds_alternative<production::HelpRequested>(parsed)) {
    production::print_daemon_usage(std::cout);
    return EXIT_SUCCESS;
  }
  if (const auto *error = std::get_if<production::DaemonConfigError>(&parsed)) {
    std::cerr << "configuration_error=" << error->message << '\n';
    return 2;
  }

  production::TerminationSignals signals;
  auto spot_mode = support::AttemptMode::Live;
  bool recover_spot = false;
  if (const auto *scenario = std::getenv("BMD_GATEWAY_TEST_SCENARIO");
      scenario != nullptr) {
    if (std::string_view{scenario} == "startup-spot-failure") {
      spot_mode = support::AttemptMode::Terminal;
    } else if (std::string_view{scenario} == "spot-recovery") {
      recover_spot = true;
    }
  }
  auto configured = support::gateway_options(spot_mode);
  auto trigger_recovery = [state = configured.spot] {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{2};
    auto *attempt = state->active_attempt.load();
    if (attempt == nullptr || !attempt->inject_recoverable_failure()) {
      return;
    }
    while (state->starts.load() < 2U &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  };
  std::function<void()> on_serving = [] {};
  if (recover_spot) {
    on_serving = std::move(trigger_recovery);
  }
  ServingTriggerBuffer output_buffer{std::cout.rdbuf(), std::move(on_serving)};
  std::ostream output{&output_buffer};
  configured.gateway.allow_ephemeral_listen_for_testing = false;
  return production::run_production_service(
      std::get<production::DaemonConfig>(parsed), support::metadata(), signals,
      output, std::cerr, std::move(configured.gateway));
}
