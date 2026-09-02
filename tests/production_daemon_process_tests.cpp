#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

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

struct Child final {
  pid_t pid{-1};
  int output{-1};
};

[[nodiscard]] Child spawn(const std::string &path,
                          const std::vector<std::string> &arguments,
                          std::string_view scenario = {}) {
  int pipe_descriptors[2]{};
  if (pipe(pipe_descriptors) != 0) {
    throw std::runtime_error{"pipe failed"};
  }
  const auto pid = fork();
  if (pid < 0) {
    close(pipe_descriptors[0]);
    close(pipe_descriptors[1]);
    throw std::runtime_error{"fork failed"};
  }
  if (pid == 0) {
    close(pipe_descriptors[0]);
    static_cast<void>(dup2(pipe_descriptors[1], STDOUT_FILENO));
    static_cast<void>(dup2(pipe_descriptors[1], STDERR_FILENO));
    close(pipe_descriptors[1]);
    if (scenario.empty()) {
      static_cast<void>(unsetenv("BMD_GATEWAY_TEST_SCENARIO"));
    } else {
      const std::string scenario_value{scenario};
      static_cast<void>(
          setenv("BMD_GATEWAY_TEST_SCENARIO", scenario_value.c_str(), 1));
    }
    std::vector<char *> argv;
    argv.reserve(arguments.size() + 2U);
    argv.push_back(const_cast<char *>(path.c_str()));
    for (const auto &argument : arguments) {
      argv.push_back(const_cast<char *>(argument.c_str()));
    }
    argv.push_back(nullptr);
    execv(path.c_str(), argv.data());
    _exit(127);
  }
  close(pipe_descriptors[1]);
  const auto flags = fcntl(pipe_descriptors[0], F_GETFL, 0);
  static_cast<void>(fcntl(pipe_descriptors[0], F_SETFL, flags | O_NONBLOCK));
  return {pid, pipe_descriptors[0]};
}

[[nodiscard]] std::size_t count_occurrences(std::string_view value,
                                            std::string_view needle) {
  std::size_t count = 0U;
  std::size_t position = 0U;
  while ((position = value.find(needle, position)) != std::string_view::npos) {
    ++count;
    position += needle.size();
  }
  return count;
}

void collect_available(Child &child, std::string &output) {
  char buffer[4096];
  for (;;) {
    const auto count = read(child.output, buffer, sizeof(buffer));
    if (count > 0) {
      output.append(buffer, static_cast<std::size_t>(count));
      continue;
    }
    if (count == -1 && errno == EINTR) {
      continue;
    }
    return;
  }
}

[[nodiscard]] int wait_bounded(Child &child, std::string &output) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{15};
  int status = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    collect_available(child, output);
    const auto result = waitpid(child.pid, &status, WNOHANG);
    if (result == child.pid) {
      collect_available(child, output);
      close(child.output);
      child.output = -1;
      return status;
    }
    if (result < 0) {
      throw std::runtime_error{"waitpid failed"};
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
  static_cast<void>(kill(child.pid, SIGKILL));
  static_cast<void>(waitpid(child.pid, &status, 0));
  close(child.output);
  child.output = -1;
  throw TestFailure{"child exit deadline expired"};
}

[[nodiscard]] std::uint16_t available_port() {
  const auto descriptor = socket(AF_INET, SOCK_STREAM, 0);
  if (descriptor < 0) {
    throw std::runtime_error{"socket failed"};
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind(descriptor, reinterpret_cast<sockaddr *>(&address),
           sizeof(address)) != 0) {
    close(descriptor);
    throw std::runtime_error{"bind failed"};
  }
  socklen_t size = sizeof(address);
  if (getsockname(descriptor, reinterpret_cast<sockaddr *>(&address), &size) !=
      0) {
    close(descriptor);
    throw std::runtime_error{"getsockname failed"};
  }
  const auto port = ntohs(address.sin_port);
  close(descriptor);
  return port;
}

void cli_processes(const std::string &daemon) {
  {
    auto child = spawn(daemon, {"--help"});
    std::string output;
    const auto status = wait_bounded(child, output);
    REQUIRE(WIFEXITED(status));
    REQUIRE(WEXITSTATUS(status) == 0);
    REQUIRE(output.find("BINANCE/SPOT/BTCUSDT") != std::string::npos);
    REQUIRE(output.find("BINANCE/USD_M_PERPETUAL/BTCUSDT") !=
            std::string::npos);
    REQUIRE(output.find("--symbol") == std::string::npos);
  }
  {
    auto child = spawn(daemon, {"--symbol", "BTCUSDT"});
    std::string output;
    const auto status = wait_bounded(child, output);
    REQUIRE(WIFEXITED(status));
    REQUIRE(WEXITSTATUS(status) == 2);
    REQUIRE(output.find("configuration_error=") != std::string::npos);
    REQUIRE(output.find("gateway_state=starting") == std::string::npos);
  }
}

void signal_process(const std::string &fixture, int signal,
                    bool prove_still_running, std::string_view scenario = {}) {
  const auto endpoint = "127.0.0.1:" + std::to_string(available_port());
  auto child = spawn(fixture, {"--grpc-listen", endpoint}, scenario);
  std::string output;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{15};
  bool serving = false;
  while (std::chrono::steady_clock::now() < deadline) {
    collect_available(child, output);
    if (output.find("gateway_state=serving") != std::string::npos) {
      serving = true;
      break;
    }
    int status = 0;
    REQUIRE(waitpid(child.pid, &status, WNOHANG) == 0);
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
  REQUIRE(serving);
  if (prove_still_running) {
    int status = 0;
    REQUIRE(waitpid(child.pid, &status, WNOHANG) == 0);
    REQUIRE(kill(child.pid, 0) == 0);
  }
  REQUIRE(kill(child.pid, signal) == 0);
  const auto status = wait_bounded(child, output);
  REQUIRE(WIFEXITED(status));
  REQUIRE(WEXITSTATUS(status) == 0);
  REQUIRE(output.find(signal == SIGINT ? "signal=SIGINT" : "signal=SIGTERM") !=
          std::string::npos);
  REQUIRE(output.find("gateway_state=stopped contexts=0 transports=0 "
                      "subscriptions=0 owners_joined=yes") !=
          std::string::npos);
  if (scenario == "spot-recovery") {
    constexpr std::string_view prefix{
        "gateway_recovery_failure product=spot index=0 generation=1 "
        "cause=transport-failure"};
    REQUIRE(count_occurrences(output, "gateway_recovery_failure") == 1U);
    const auto start = output.find(prefix);
    REQUIRE(start != std::string::npos);
    const auto end = output.find('\n', start);
    REQUIRE(end != std::string::npos);
    const auto diagnostic = std::string_view{output}.substr(start, end - start);
    REQUIRE(diagnostic.find(
                "network_error_code=websocket-read network_stage=\"spot-test"
                "\\nstage\\\\source\"") != std::string_view::npos);
    REQUIRE(diagnostic.find("network_message_truncated=yes") !=
            std::string_view::npos);
    REQUIRE(diagnostic.find('\t') == std::string_view::npos);
  } else {
    REQUIRE(output.find("gateway_recovery_failure") == std::string::npos);
  }
}

void startup_failure_diagnostic(const std::string &fixture) {
  const auto endpoint = "127.0.0.1:" + std::to_string(available_port());
  auto child =
      spawn(fixture, {"--grpc-listen", endpoint}, "startup-spot-failure");
  std::string output;
  const auto status = wait_bounded(child, output);
  REQUIRE(WIFEXITED(status));
  REQUIRE(WEXITSTATUS(status) == 1);
  REQUIRE(output.find("gateway_start=failed reason=spot-initial-failure") !=
          std::string::npos);
  REQUIRE(output.find("gateway_recovery_failure product=spot index=0 "
                      "generation=1 cause=internal-failure") !=
          std::string::npos);
  REQUIRE(output.find("network_error_code=internal") != std::string::npos);
  REQUIRE(output.find("network_stage=\"spot-test-terminal\"") !=
          std::string::npos);
  REQUIRE(count_occurrences(output, "gateway_recovery_failure") == 1U);
}

} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "expected bmd-gatewayd and fixture paths\n";
    return EXIT_FAILURE;
  }
  try {
    cli_processes(argv[1]);
    std::cout << "DAEMON_HELP_AND_INVALID_CLI=PASS\n";
    signal_process(argv[2], SIGINT, true);
    std::cout << "DAEMON_SIGINT_LONG_RUNNING=PASS\n";
    signal_process(argv[2], SIGTERM, false);
    std::cout << "DAEMON_SIGTERM=PASS\n";
    startup_failure_diagnostic(argv[2]);
    std::cout << "DAEMON_STARTUP_FAILURE_DIAGNOSTIC=PASS\n";
    signal_process(argv[2], SIGTERM, false, "spot-recovery");
    std::cout << "DAEMON_POST_RECOVERY_SHUTDOWN_DIAGNOSTIC=PASS\n";
  } catch (const std::exception &error) {
    std::cerr << "PRODUCTION_DAEMON_PROCESS_TEST=FAIL " << error.what() << '\n';
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
