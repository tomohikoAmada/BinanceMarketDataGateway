#include "grpc_service.hpp"

#include <binance_market_data/common/v1/enums.pb.h>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

namespace {

namespace common = binance_market_data::common::v1;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g10 = binance_market_data::gateway::g10;
namespace wire = binance_market_data::gateway::v1;

inline constexpr auto kStartupTimeout = std::chrono::seconds{180};
inline constexpr auto kStreamTimeout = std::chrono::seconds{180};
inline constexpr auto kShutdownTimeout = std::chrono::seconds{30};
inline constexpr auto kFailureCleanupTimeout = std::chrono::seconds{15};
inline constexpr auto kPollInterval = std::chrono::milliseconds{10};

class AcceptanceFailure final : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

void require(bool condition, std::string_view message) {
  if (!condition) {
    throw AcceptanceFailure{std::string{message}};
  }
}

template <typename Integer>
[[nodiscard]] std::optional<Integer> parse_unsigned(std::string_view text) {
  if (text.empty()) {
    return std::nullopt;
  }
  Integer value{};
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
    return std::nullopt;
  }
  return value;
}

struct Options final {
  std::string daemon_path;
  std::string grpc_target;
  std::uint16_t grpc_port{};
};

[[nodiscard]] Options parse_options(int argc, char **argv) {
  std::optional<std::string> daemon_path;
  std::optional<std::string> grpc_target;
  for (int index = 1; index < argc; index += 2) {
    const std::string_view option{argv[index]};
    if (option != "--daemon" && option != "--grpc-target") {
      throw AcceptanceFailure{"unknown option: " + std::string{option}};
    }
    if (index + 1 >= argc) {
      throw AcceptanceFailure{"missing value for " + std::string{option}};
    }
    const std::string_view value{argv[index + 1]};
    if (value.empty() || value.starts_with("--")) {
      throw AcceptanceFailure{"empty or missing value for " +
                              std::string{option}};
    }
    auto &destination = option == "--daemon" ? daemon_path : grpc_target;
    if (destination.has_value()) {
      throw AcceptanceFailure{"duplicate option: " + std::string{option}};
    }
    destination = value;
  }
  require(daemon_path.has_value(), "missing --daemon");
  require(grpc_target.has_value(), "missing --grpc-target");

  const auto colon = grpc_target->rfind(':');
  require(colon != std::string::npos && colon != 0U &&
              colon + 1U < grpc_target->size(),
          "--grpc-target must include a nonempty host and numeric port");
  const auto parsed_port = parse_unsigned<unsigned int>(
      std::string_view{*grpc_target}.substr(colon + 1U));
  require(parsed_port.has_value() && *parsed_port > 0U &&
              *parsed_port <= 65'535U,
          "--grpc-target port is invalid");

  std::error_code filesystem_error;
  const auto resolved_path =
      std::filesystem::canonical(*daemon_path, filesystem_error);
  require(!filesystem_error, "--daemon path cannot be resolved");
  filesystem_error.clear();
  const auto regular =
      std::filesystem::is_regular_file(resolved_path, filesystem_error);
  require(!filesystem_error && regular, "--daemon path is not a regular file");
  require(access(resolved_path.c_str(), X_OK) == 0,
          "--daemon path is not executable");

  return {resolved_path.string(), *grpc_target,
          static_cast<std::uint16_t>(*parsed_port)};
}

class ChildProcess final {
public:
  [[nodiscard]] static ChildProcess spawn(const std::string &daemon_path,
                                          const std::string &grpc_target) {
    int pipe_descriptors[2]{};
    if (pipe(pipe_descriptors) != 0) {
      throw AcceptanceFailure{"daemon output pipe failed"};
    }

    const auto child_pid = fork();
    if (child_pid < 0) {
      close(pipe_descriptors[0]);
      close(pipe_descriptors[1]);
      throw AcceptanceFailure{"daemon fork failed"};
    }
    if (child_pid == 0) {
      close(pipe_descriptors[0]);
      if (dup2(pipe_descriptors[1], STDOUT_FILENO) < 0 ||
          dup2(pipe_descriptors[1], STDERR_FILENO) < 0) {
        _exit(126);
      }
      close(pipe_descriptors[1]);
      char listen_option[] = "--grpc-listen";
      char *child_argv[]{const_cast<char *>(daemon_path.c_str()), listen_option,
                         const_cast<char *>(grpc_target.c_str()), nullptr};
      execv(daemon_path.c_str(), child_argv);
      constexpr char message[] = "daemon_execv=failed\n";
      static_cast<void>(write(STDERR_FILENO, message, sizeof(message) - 1U));
      _exit(127);
    }

    close(pipe_descriptors[1]);
    const auto flags = fcntl(pipe_descriptors[0], F_GETFL, 0);
    if (flags < 0 ||
        fcntl(pipe_descriptors[0], F_SETFL, flags | O_NONBLOCK) < 0) {
      close(pipe_descriptors[0]);
      reap_spawn_failure(child_pid);
      throw AcceptanceFailure{"daemon output nonblocking setup failed"};
    }
    return ChildProcess{child_pid, pipe_descriptors[0]};
  }

  ChildProcess(const ChildProcess &) = delete;
  ChildProcess &operator=(const ChildProcess &) = delete;
  ChildProcess(ChildProcess &&) = delete;
  ChildProcess &operator=(ChildProcess &&) = delete;

  ~ChildProcess() { cleanup(); }

  [[nodiscard]] pid_t pid() const noexcept { return pid_; }
  [[nodiscard]] const std::string &output() const noexcept { return output_; }

  void collect_available() {
    if (output_descriptor_ < 0) {
      return;
    }
    char buffer[4096];
    for (;;) {
      const auto count = read(output_descriptor_, buffer, sizeof(buffer));
      if (count > 0) {
        output_.append(buffer, static_cast<std::size_t>(count));
        continue;
      }
      if (count == 0) {
        close_output();
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }
      throw AcceptanceFailure{"daemon output read failed"};
    }
  }

  [[nodiscard]] std::optional<int> poll_exit() {
    collect_available();
    if (reaped_) {
      return wait_status_;
    }
    int status = 0;
    pid_t result = -1;
    do {
      result = waitpid(pid_, &status, WNOHANG);
    } while (result < 0 && errno == EINTR);
    if (result == 0) {
      return std::nullopt;
    }
    if (result != pid_) {
      throw AcceptanceFailure{"waitpid failed while polling daemon"};
    }
    reaped_ = true;
    wait_status_ = status;
    collect_available();
    close_output();
    return wait_status_;
  }

  void require_running(std::string_view stage) {
    if (poll_exit().has_value()) {
      throw AcceptanceFailure{"daemon exited before intended SIGTERM at " +
                              std::string{stage}};
    }
  }

  void send_sigterm() {
    require(!reaped_, "daemon was already reaped before SIGTERM");
    int result = -1;
    do {
      result = kill(pid_, SIGTERM);
    } while (result != 0 && errno == EINTR);
    require(result == 0, "kill(child_pid, SIGTERM) failed");
    termination_requested_ = true;
  }

  [[nodiscard]] int wait_until(std::chrono::steady_clock::time_point deadline) {
    while (std::chrono::steady_clock::now() < deadline) {
      if (const auto status = poll_exit(); status.has_value()) {
        return *status;
      }
      std::this_thread::sleep_for(kPollInterval);
    }
    if (const auto status = poll_exit(); status.has_value()) {
      return *status;
    }
    throw AcceptanceFailure{"daemon shutdown deadline expired"};
  }

  void cleanup_after_failure() noexcept { cleanup(); }

private:
  ChildProcess(pid_t pid, int output_descriptor)
      : pid_{pid}, output_descriptor_{output_descriptor} {}

  static void reap_spawn_failure(pid_t pid) noexcept {
    static_cast<void>(kill(pid, SIGTERM));
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{2};
    int status = 0;
    while (std::chrono::steady_clock::now() < deadline) {
      const auto result = waitpid(pid, &status, WNOHANG);
      if (result == pid || (result < 0 && errno == ECHILD)) {
        return;
      }
      if (result < 0 && errno != EINTR) {
        break;
      }
      std::this_thread::sleep_for(kPollInterval);
    }
    static_cast<void>(kill(pid, SIGKILL));
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
  }

  void close_output() noexcept {
    if (output_descriptor_ >= 0) {
      close(output_descriptor_);
      output_descriptor_ = -1;
    }
  }

  void collect_available_noexcept() noexcept {
    try {
      collect_available();
    } catch (...) {
    }
  }

  void cleanup() noexcept {
    if (reaped_) {
      collect_available_noexcept();
      close_output();
      return;
    }
    collect_available_noexcept();
    if (!termination_requested_) {
      if (kill(pid_, SIGTERM) == 0) {
        termination_requested_ = true;
      }
    }

    const auto deadline =
        std::chrono::steady_clock::now() + kFailureCleanupTimeout;
    int status = 0;
    while (std::chrono::steady_clock::now() < deadline) {
      const auto result = waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        reaped_ = true;
        wait_status_ = status;
        collect_available_noexcept();
        close_output();
        return;
      }
      if (result < 0 && errno == ECHILD) {
        reaped_ = true;
        collect_available_noexcept();
        close_output();
        return;
      }
      if (result < 0 && errno != EINTR) {
        break;
      }
      collect_available_noexcept();
      std::this_thread::sleep_for(kPollInterval);
    }

    static_cast<void>(kill(pid_, SIGKILL));
    pid_t result = -1;
    do {
      result = waitpid(pid_, &status, 0);
    } while (result < 0 && errno == EINTR);
    if (result == pid_) {
      reaped_ = true;
      wait_status_ = status;
    }
    collect_available_noexcept();
    close_output();
  }

  pid_t pid_{-1};
  int output_descriptor_{-1};
  bool reaped_{false};
  bool termination_requested_{false};
  int wait_status_{};
  std::string output_;
};

struct LineMatch final {
  std::size_t position{};
  std::string_view line;
};

[[nodiscard]] std::optional<LineMatch>
find_complete_line(std::string_view output, std::string_view marker,
                   std::size_t from = 0U) {
  auto cursor = from;
  if (cursor > 0U && output[cursor - 1U] != '\n') {
    cursor = output.find('\n', cursor);
    if (cursor == std::string_view::npos) {
      return std::nullopt;
    }
    ++cursor;
  }
  while (cursor < output.size()) {
    const auto end = output.find('\n', cursor);
    if (end == std::string_view::npos) {
      return std::nullopt;
    }
    const auto line = output.substr(cursor, end - cursor);
    if (line.find(marker) != std::string_view::npos) {
      return LineMatch{cursor, line};
    }
    cursor = end + 1U;
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string_view>
field_value(std::string_view line, std::string_view key) {
  auto position = line.find(key);
  while (position != std::string_view::npos && position != 0U &&
         line[position - 1U] != ' ') {
    position = line.find(key, position + 1U);
  }
  if (position == std::string_view::npos) {
    return std::nullopt;
  }
  const auto value_start = position + key.size();
  const auto value_end = line.find(' ', value_start);
  const auto value = line.substr(value_start, value_end - value_start);
  if (value.empty()) {
    return std::nullopt;
  }
  return value;
}

struct ServingIdentity final {
  std::uint16_t grpc_port{};
  std::uint64_t spot_generation{};
  std::uint64_t usdm_generation{};
  std::string gateway_instance_id;
};

[[nodiscard]] ServingIdentity parse_serving_line(std::string_view line,
                                                 std::uint16_t expected_port) {
  const auto port_text = field_value(line, "grpc_port=");
  const auto spot_text = field_value(line, "spot_generation=");
  const auto usdm_text = field_value(line, "usdm_generation=");
  const auto instance_text = field_value(line, "gateway_instance_id=");
  require(port_text.has_value() && spot_text.has_value() &&
              usdm_text.has_value() && instance_text.has_value(),
          "serving line is missing required identity fields");
  const auto port = parse_unsigned<unsigned int>(*port_text);
  const auto spot_generation = parse_unsigned<std::uint64_t>(*spot_text);
  const auto usdm_generation = parse_unsigned<std::uint64_t>(*usdm_text);
  require(port.has_value() && *port == expected_port,
          "serving grpc_port does not match --grpc-target");
  require(spot_generation.has_value() && *spot_generation != 0U &&
              usdm_generation.has_value() && *usdm_generation != 0U,
          "serving line has invalid connection generation");
  return {static_cast<std::uint16_t>(*port), *spot_generation, *usdm_generation,
          std::string{*instance_text}};
}

[[nodiscard]] ServingIdentity wait_for_serving(ChildProcess &child,
                                               std::uint16_t expected_port) {
  const auto deadline = std::chrono::steady_clock::now() + kStartupTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    child.collect_available();
    child.require_running("startup serving barrier");
    const auto serving =
        find_complete_line(child.output(), "gateway_state=serving");
    if (serving.has_value()) {
      return parse_serving_line(serving->line, expected_port);
    }
    std::this_thread::sleep_for(kPollInterval);
  }
  child.require_running("startup serving deadline");
  throw AcceptanceFailure{"daemon serving deadline expired"};
}

struct SessionSequence final {
  std::uint64_t next{1U};
  std::string subscription_id;
};

template <typename Item>
[[nodiscard]] bool
canonical_delivery(const Item &item, SessionSequence &sequence,
                   const std::string &gateway_instance_id,
                   std::optional<std::uint64_t> expected_generation) {
  if (item.has_envelope_metadata() || !item.has_delivery_metadata()) {
    return false;
  }
  const auto &metadata = item.delivery_metadata();
  if (metadata.protocol_version() != g7::kProtocolVersion ||
      metadata.gateway_instance_id() != gateway_instance_id ||
      metadata.session_sequence() != sequence.next ||
      metadata.publish_time_utc_ns() == 0U ||
      metadata.has_connection_generation() != expected_generation.has_value()) {
    return false;
  }
  if (expected_generation.has_value() &&
      metadata.connection_generation() != *expected_generation) {
    return false;
  }
  if (sequence.subscription_id.empty()) {
    sequence.subscription_id = metadata.subscription_id();
  } else if (metadata.subscription_id() != sequence.subscription_id) {
    return false;
  }
  if (sequence.subscription_id.empty()) {
    return false;
  }
  ++sequence.next;
  return true;
}

[[nodiscard]] bool valid_market_metadata(
    const binance_market_data::common::v1::EventMetadata &metadata,
    common::Market market) {
  return metadata.venue() == common::VENUE_BINANCE &&
         metadata.market() == market && metadata.symbol() == "BTCUSDT";
}

[[nodiscard]] wire::OrderBookSubscriptionRequest
book_request(common::Market market, std::string request_id) {
  wire::OrderBookSubscriptionRequest request;
  request.set_request_id(std::move(request_id));
  request.set_schema_version(g7::kOrderBookRequestSchema);
  request.set_venue(common::VENUE_BINANCE);
  request.set_market(market);
  request.set_symbol("BTCUSDT");
  request.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
  request.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
  request.add_supported_update_schema_versions(g7::kUpdateSchema);
  return request;
}

[[nodiscard]] wire::EventSubscriptionRequest event_request() {
  wire::EventSubscriptionRequest request;
  request.set_request_id("production-real-usdm-event");
  request.set_schema_version(g9::kEventRequestSchema);
  auto *selector = request.add_selectors();
  selector->set_venue(common::VENUE_BINANCE);
  selector->set_market(common::MARKET_USD_M_PERPETUAL);
  selector->set_symbol("BTCUSDT");
  selector->set_stream(common::STREAM_DIFF_DEPTH);
  request.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
  request.add_supported_payload_schema_versions(g9::kDepthEventSchema);
  return request;
}

[[nodiscard]] bool
read_book_head(grpc::ClientReader<wire::OrderBookStreamItem> &reader,
               common::Market market, std::uint64_t generation,
               const std::string &gateway_instance_id) {
  SessionSequence sequence;
  wire::OrderBookStreamItem item;
  if (!reader.Read(&item) || !item.has_subscription_accepted() ||
      !canonical_delivery(item, sequence, gateway_instance_id, std::nullopt) ||
      item.subscription_accepted().schema_version() !=
          g7::kSubscriptionAcceptedSchema ||
      item.subscription_accepted().gateway_instance_id() !=
          gateway_instance_id ||
      item.subscription_accepted().subscription_id() !=
          sequence.subscription_id ||
      item.subscription_accepted().accepted_time_utc_ns() == 0U ||
      item.subscription_accepted().negotiated_payload_schema_versions_size() !=
          2) {
    return false;
  }
  if (!reader.Read(&item) || !item.has_snapshot() ||
      !canonical_delivery(item, sequence, gateway_instance_id, generation) ||
      item.snapshot().venue() != common::VENUE_BINANCE ||
      item.snapshot().market() != market ||
      item.snapshot().symbol() != "BTCUSDT" ||
      item.snapshot().schema_version() != g7::kSnapshotSchema ||
      !item.snapshot().synchronized() ||
      item.snapshot().last_update_id() == 0U ||
      item.snapshot().bids().empty() || item.snapshot().asks().empty()) {
    return false;
  }
  const auto snapshot_update_id = item.snapshot().last_update_id();
  if (!reader.Read(&item) || !item.has_depth_update() ||
      !canonical_delivery(item, sequence, gateway_instance_id, generation) ||
      !valid_market_metadata(item.depth_update().metadata(), market) ||
      item.depth_update().metadata().schema_version() != g7::kUpdateSchema ||
      item.depth_update().final_update_id() <= snapshot_update_id) {
    return false;
  }
  return true;
}

[[nodiscard]] bool
read_event_head(grpc::ClientReader<wire::GatewayEventEnvelope> &reader,
                std::uint64_t generation,
                const std::string &gateway_instance_id) {
  SessionSequence sequence;
  wire::GatewayEventEnvelope item;
  if (!reader.Read(&item) || !item.has_subscription_accepted() ||
      !canonical_delivery(item, sequence, gateway_instance_id, std::nullopt) ||
      item.subscription_accepted().schema_version() !=
          g9::kEventSubscriptionAcceptedSchema ||
      item.subscription_accepted().gateway_instance_id() !=
          gateway_instance_id ||
      item.subscription_accepted().subscription_id() !=
          sequence.subscription_id ||
      item.subscription_accepted().accepted_time_utc_ns() == 0U ||
      item.subscription_accepted().negotiated_payload_schema_versions_size() !=
          1 ||
      item.subscription_accepted().negotiated_payload_schema_versions(0) !=
          g9::kDepthEventSchema) {
    return false;
  }
  if (!reader.Read(&item) || !item.has_depth_update() ||
      !canonical_delivery(item, sequence, gateway_instance_id, generation) ||
      !valid_market_metadata(item.depth_update().metadata(),
                             common::MARKET_USD_M_PERPETUAL) ||
      item.depth_update().metadata().schema_version() !=
          g9::kDepthEventSchema ||
      !item.depth_update().metadata().has_receive_time_utc_ns() ||
      !item.depth_update().metadata().has_receive_monotonic_ns() ||
      item.depth_update().final_update_id() == 0U) {
    return false;
  }
  return true;
}

struct StatusEvidence final {
  bool schema_valid{false};
  bool instance_match{false};
  bool spot_live{false};
  bool usdm_live{false};
  bool subscription_counts{false};

  [[nodiscard]] bool two_markets_valid() const noexcept {
    return schema_valid && spot_live && usdm_live && subscription_counts;
  }
};

[[nodiscard]] StatusEvidence
validate_status(const grpc::Status &status,
                const wire::GatewayStatusSnapshot &response,
                const ServingIdentity &identity) {
  StatusEvidence evidence;
  evidence.schema_valid =
      status.ok() && response.schema_version() == g10::kStatusSnapshotSchema &&
      response.markets_size() == 2;
  evidence.instance_match =
      evidence.schema_valid &&
      response.gateway_instance_id() == identity.gateway_instance_id;
  if (!evidence.schema_valid) {
    return evidence;
  }
  const auto &spot = response.markets(0);
  const auto &usdm = response.markets(1);
  evidence.spot_live = spot.venue() == common::VENUE_BINANCE &&
                       spot.market() == common::MARKET_SPOT &&
                       spot.symbol() == "BTCUSDT" &&
                       spot.state() == common::STREAM_LIFECYCLE_STATE_LIVE &&
                       spot.has_connection_generation() &&
                       spot.connection_generation() == identity.spot_generation;
  evidence.usdm_live = usdm.venue() == common::VENUE_BINANCE &&
                       usdm.market() == common::MARKET_USD_M_PERPETUAL &&
                       usdm.symbol() == "BTCUSDT" &&
                       usdm.state() == common::STREAM_LIFECYCLE_STATE_LIVE &&
                       usdm.has_connection_generation() &&
                       usdm.connection_generation() == identity.usdm_generation;
  evidence.subscription_counts = spot.active_subscription_count() == 1U &&
                                 usdm.active_subscription_count() == 2U &&
                                 response.total_active_subscriptions() == 3U;
  return evidence;
}

class ClientCancellation final {
public:
  ClientCancellation(grpc::ClientContext &spot, grpc::ClientContext &usdm,
                     grpc::ClientContext &event)
      : spot_{spot}, usdm_{usdm}, event_{event} {}
  ClientCancellation(const ClientCancellation &) = delete;
  ClientCancellation &operator=(const ClientCancellation &) = delete;
  ~ClientCancellation() {
    if (active_) {
      spot_.TryCancel();
      usdm_.TryCancel();
      event_.TryCancel();
    }
  }
  void dismiss() noexcept { active_ = false; }

private:
  grpc::ClientContext &spot_;
  grpc::ClientContext &usdm_;
  grpc::ClientContext &event_;
  bool active_{true};
};

template <typename Reader, typename Item>
[[nodiscard]] grpc::Status finish_stream(Reader &reader) {
  Item item;
  while (reader.Read(&item)) {
  }
  return reader.Finish();
}

[[nodiscard]] bool valid_shutdown_finish(const grpc::Status &status) {
  return status.ok() || status.error_code() == grpc::StatusCode::CANCELLED ||
         status.error_code() == grpc::StatusCode::UNKNOWN ||
         status.error_code() == grpc::StatusCode::UNAVAILABLE;
}

[[nodiscard]] std::string one_line(std::string text) {
  for (auto &character : text) {
    if (character == '\n' || character == '\r') {
      character = ' ';
    }
  }
  return text;
}

void print_finish_status(std::string_view label, const grpc::Status &status) {
  std::cout << label << "_OK=" << (status.ok() ? "YES" : "NO") << '\n'
            << label << "_CODE=" << static_cast<int>(status.error_code())
            << '\n'
            << label << "_MESSAGE=" << one_line(status.error_message()) << '\n';
}

struct ShutdownEvidence final {
  bool stopping_sigterm{false};
  bool contexts_zero{false};
  bool transports_zero{false};
  bool subscriptions_zero{false};
  bool owners_joined{false};
};

[[nodiscard]] ShutdownEvidence
parse_shutdown_output(std::string_view output_after_signal) {
  ShutdownEvidence evidence;
  const auto stopping =
      find_complete_line(output_after_signal, "gateway_state=stopping");
  const auto stopped =
      find_complete_line(output_after_signal, "gateway_state=stopped");
  if (!stopping.has_value() || !stopped.has_value() ||
      stopped->position <= stopping->position) {
    return evidence;
  }
  const auto signal = field_value(stopping->line, "signal=");
  evidence.stopping_sigterm = signal.has_value() && *signal == "SIGTERM";
  const auto contexts = field_value(stopped->line, "contexts=");
  const auto transports = field_value(stopped->line, "transports=");
  const auto subscriptions = field_value(stopped->line, "subscriptions=");
  const auto owners = field_value(stopped->line, "owners_joined=");
  evidence.contexts_zero = contexts.has_value() && *contexts == "0";
  evidence.transports_zero = transports.has_value() && *transports == "0";
  evidence.subscriptions_zero =
      subscriptions.has_value() && *subscriptions == "0";
  evidence.owners_joined = owners.has_value() && *owners == "yes";
  return evidence;
}

void print_captured_output(std::ostream &stream, std::string_view output) {
  stream << "REAL_DAEMON_OUTPUT_BEGIN\n" << output;
  if (!output.empty() && output.back() != '\n') {
    stream << '\n';
  }
  stream << "REAL_DAEMON_OUTPUT_END\n";
}

int run_acceptance(const Options &options) {
  auto child = ChildProcess::spawn(options.daemon_path, options.grpc_target);
  std::cout << "REAL_DAEMON_PATH=" << options.daemon_path << '\n'
            << "REAL_DAEMON_PID=" << child.pid() << '\n'
            << std::flush;
  bool captured_output_printed = false;
  try {
    const auto identity = wait_for_serving(child, options.grpc_port);
    std::cout << "REAL_DAEMON_SERVING=YES\n"
              << "DAEMON_GATEWAY_INSTANCE_ID=" << identity.gateway_instance_id
              << '\n'
              << std::flush;

    auto stub =
        wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
            options.grpc_target, grpc::InsecureChannelCredentials()));
    const auto stream_deadline =
        std::chrono::system_clock::now() + kStreamTimeout;
    grpc::ClientContext spot_context;
    grpc::ClientContext usdm_context;
    grpc::ClientContext event_context;
    spot_context.set_deadline(stream_deadline);
    usdm_context.set_deadline(stream_deadline);
    event_context.set_deadline(stream_deadline);
    auto spot_reader = stub->SubscribeOrderBook(
        &spot_context,
        book_request(common::MARKET_SPOT, "production-real-spot-book"));
    auto usdm_reader = stub->SubscribeOrderBook(
        &usdm_context, book_request(common::MARKET_USD_M_PERPETUAL,
                                    "production-real-usdm-book"));
    auto event_reader = stub->SubscribeEvents(&event_context, event_request());
    ClientCancellation cancellation{spot_context, usdm_context, event_context};

    const auto spot_book =
        read_book_head(*spot_reader, common::MARKET_SPOT,
                       identity.spot_generation, identity.gateway_instance_id);
    child.require_running("Spot order-book evidence");
    const auto usdm_book =
        read_book_head(*usdm_reader, common::MARKET_USD_M_PERPETUAL,
                       identity.usdm_generation, identity.gateway_instance_id);
    child.require_running("USD-M order-book evidence");
    const auto usdm_event = read_event_head(
        *event_reader, identity.usdm_generation, identity.gateway_instance_id);
    child.require_running("USD-M event evidence");

    wire::GatewayStatusRequest status_request;
    status_request.set_request_id("production-real-status");
    status_request.set_schema_version(g10::kStatusRequestSchema);
    wire::GatewayStatusSnapshot status_response;
    grpc::ClientContext status_context;
    status_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::seconds{10});
    const auto status = stub->GetGatewayStatus(&status_context, status_request,
                                               &status_response);
    child.require_running("status authentication evidence");
    const auto status_evidence =
        validate_status(status, status_response, identity);

    std::cout << "REAL_SPOT_LIVE="
              << (status_evidence.spot_live ? "PASS" : "FAIL") << '\n'
              << "REAL_USDM_LIVE="
              << (status_evidence.usdm_live ? "PASS" : "FAIL") << '\n'
              << "REAL_SPOT_ORDER_BOOK=" << (spot_book ? "PASS" : "FAIL")
              << '\n'
              << "REAL_USDM_ORDER_BOOK=" << (usdm_book ? "PASS" : "FAIL")
              << '\n'
              << "REAL_USDM_DIFF_DEPTH=" << (usdm_event ? "PASS" : "FAIL")
              << '\n'
              << "REAL_STATUS_TWO_MARKETS="
              << (status_evidence.two_markets_valid() ? "PASS" : "FAIL") << '\n'
              << "REAL_STATUS_INSTANCE_MATCH="
              << (status_evidence.instance_match ? "PASS" : "FAIL") << '\n';
    require(spot_book, "Spot order-book evidence failed");
    require(usdm_book, "USD-M order-book evidence failed");
    require(usdm_event, "USD-M DIFF_DEPTH evidence failed");
    require(status_evidence.two_markets_valid(),
            "two-market status identity evidence failed");
    require(status_evidence.instance_match,
            "status gateway_instance_id does not match child log");
    std::cout << "CLIENT_READY_FOR_SIGTERM=YES\n" << std::flush;

    child.collect_available();
    child.require_running("immediate pre-SIGTERM liveness cut");
    const auto output_cut = child.output().size();
    std::cout << "REAL_CHILD_ALIVE_BEFORE_SIGTERM=YES\n" << std::flush;
    child.send_sigterm();
    std::cout << "REAL_SIGTERM_SENT=YES\n" << std::flush;

    const auto wait_status =
        child.wait_until(std::chrono::steady_clock::now() + kShutdownTimeout);
    const auto clean_exit =
        WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0;
    std::cout << "REAL_DAEMON_EXITED_CLEANLY=" << (clean_exit ? "YES" : "NO")
              << '\n';
    if (WIFEXITED(wait_status)) {
      std::cout << "REAL_DAEMON_EXIT_CODE=" << WEXITSTATUS(wait_status) << '\n';
    } else {
      std::cout << "REAL_DAEMON_EXIT_CODE=NOT_EXITED\n";
    }

    const auto shutdown_evidence = parse_shutdown_output(
        std::string_view{child.output()}.substr(output_cut));
    std::cout << "REAL_STOPPING_SIGTERM_EVIDENCE="
              << (shutdown_evidence.stopping_sigterm ? "YES" : "NO") << '\n'
              << "REAL_FINAL_CONTEXTS_ZERO="
              << (shutdown_evidence.contexts_zero ? "YES" : "NO") << '\n'
              << "REAL_FINAL_TRANSPORTS_ZERO="
              << (shutdown_evidence.transports_zero ? "YES" : "NO") << '\n'
              << "REAL_FINAL_SUBSCRIPTIONS_ZERO="
              << (shutdown_evidence.subscriptions_zero ? "YES" : "NO") << '\n'
              << "REAL_FINAL_OWNERS_JOINED="
              << (shutdown_evidence.owners_joined ? "YES" : "NO") << '\n';
    require(clean_exit, "daemon did not exit normally with code 0");
    require(shutdown_evidence.stopping_sigterm,
            "daemon did not emit stopping SIGTERM evidence");
    require(shutdown_evidence.contexts_zero,
            "daemon final contexts count is not zero");
    require(shutdown_evidence.transports_zero,
            "daemon final transports count is not zero");
    require(shutdown_evidence.subscriptions_zero,
            "daemon final subscriptions count is not zero");
    require(shutdown_evidence.owners_joined,
            "daemon final owners_joined is not yes");

    const auto spot_finish =
        finish_stream<grpc::ClientReader<wire::OrderBookStreamItem>,
                      wire::OrderBookStreamItem>(*spot_reader);
    const auto usdm_finish =
        finish_stream<grpc::ClientReader<wire::OrderBookStreamItem>,
                      wire::OrderBookStreamItem>(*usdm_reader);
    const auto event_finish =
        finish_stream<grpc::ClientReader<wire::GatewayEventEnvelope>,
                      wire::GatewayEventEnvelope>(*event_reader);
    cancellation.dismiss();
    print_finish_status("REAL_SPOT_STREAM_FINISH", spot_finish);
    print_finish_status("REAL_USDM_STREAM_FINISH", usdm_finish);
    print_finish_status("REAL_EVENT_STREAM_FINISH", event_finish);
    const auto streams_terminated = valid_shutdown_finish(spot_finish) &&
                                    valid_shutdown_finish(usdm_finish) &&
                                    valid_shutdown_finish(event_finish);
    std::cout << "REAL_STREAMS_TERMINATED="
              << (streams_terminated ? "YES" : "NO") << '\n';
    require(streams_terminated,
            "one or more stream Finish statuses are not shutdown-terminal");

    print_captured_output(std::cout, child.output());
    captured_output_printed = true;
    std::cout << "REAL_EXTERNAL_PROCESS_PROVEN=YES\n"
              << "REAL_ACCEPTANCE_FALSE_PASS_GUARD=PASS\n"
              << "REAL_PRODUCTION_DAEMON_ACCEPTANCE=PASS\n"
              << std::flush;
    return EXIT_SUCCESS;
  } catch (...) {
    child.cleanup_after_failure();
    if (!captured_output_printed) {
      print_captured_output(std::cerr, child.output());
    }
    throw;
  }
}

} // namespace

int main(int argc, char **argv) {
  try {
    const auto options = parse_options(argc, argv);
    return run_acceptance(options);
  } catch (const std::exception &error) {
    std::cerr << "REAL_PRODUCTION_DAEMON_ACCEPTANCE=FAIL reason="
              << one_line(error.what()) << '\n'
              << "Usage: bmd-gateway-production-acceptance-client "
                 "--daemon PATH --grpc-target HOST:PORT\n";
    return EXIT_FAILURE;
  }
}
