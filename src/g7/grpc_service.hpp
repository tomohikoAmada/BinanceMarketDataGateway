#pragma once

#include "market_runtime.hpp"
#include "order_book_publication.hpp"

#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::g7 {

enum class RequestValidationError : std::uint8_t {
  InvalidArgument,
  FailedPrecondition,
};

using RequestValidationResult =
    std::variant<ValidatedOrderBookSubscription, RequestValidationError>;

[[nodiscard]] RequestValidationResult validate_order_book_request(
    const gateway_wire::OrderBookSubscriptionRequest &request,
    const std::string &gateway_instance_id);

[[nodiscard]] gateway_wire::OrderBookStreamItem
materialize_stream_item(const SubscriberChannel &channel,
                        const PeekedPublication &publication);

struct GrpcServiceOptions final {
  GrpcServiceOptions(
      std::chrono::milliseconds idle_interval =
          kIdleClientCancellationCheckInterval,
      std::size_t tracked_context_limit = kMaximumActiveSubscriptions +
                                          kPendingAdmissionCapacity,
      std::function<
          bool(grpc::ServerWriter<gateway_wire::OrderBookStreamItem> &,
               const gateway_wire::OrderBookStreamItem &)>
          writer = {})
      : idle_cancellation_check_interval{idle_interval},
        maximum_tracked_contexts{tracked_context_limit},
        write_override{std::move(writer)} {}

  std::chrono::milliseconds idle_cancellation_check_interval{
      kIdleClientCancellationCheckInterval};
  std::size_t maximum_tracked_contexts{kMaximumActiveSubscriptions +
                                       kPendingAdmissionCapacity};
  // Empty in production. Focused tests may replace the blocking Write call
  // while retaining the real generated synchronous RPC handler.
  std::function<bool(grpc::ServerWriter<gateway_wire::OrderBookStreamItem> &,
                     const gateway_wire::OrderBookStreamItem &)>
      write_override;
  // Empty in production. Focused tests use these gates to deterministically
  // order handler finalization against the shutdown cancellation snapshot.
  std::function<void(grpc::StatusCode)> before_context_finalization;
  std::function<void(grpc::StatusCode)> after_context_finalization;
  std::function<void()> cancellation_snapshot_ready;
  std::function<void()> context_finalization_waiting;
};

// The generated synchronous service surface. A handler owns exactly one
// ServerWriter and no Write executes on MarketRuntime's Projection owner.
class OrderBookGrpcService final
    : public gateway_wire::BinanceMarketDataGatewayService::Service {
public:
  OrderBookGrpcService(g3::MarketRuntime &runtime,
                       std::string gateway_instance_id,
                       GrpcServiceOptions options = {});

  [[nodiscard]] grpc::Status SubscribeOrderBook(
      grpc::ServerContext *context,
      const gateway_wire::OrderBookSubscriptionRequest *request,
      grpc::ServerWriter<gateway_wire::OrderBookStreamItem> *writer) override;

  // Closes service/runtime admission, synchronously executes the reserved
  // owner shutdown control, snapshots contexts, then TryCancel()s outside the
  // service mutex. It is idempotent.
  void begin_shutdown() noexcept;

  [[nodiscard]] bool admission_open() const noexcept;
  [[nodiscard]] std::size_t tracked_context_count() const noexcept;
  [[nodiscard]] const std::string &gateway_instance_id() const noexcept;

private:
  enum class TrackResult : std::uint8_t {
    Tracked,
    Closed,
    Full,
  };

  struct TrackedContext final {
    grpc::ServerContext *context;
    bool selected_for_try_cancel{false};
  };

  [[nodiscard]] TrackResult track_context(grpc::ServerContext *context);
  [[nodiscard]] grpc::Status finalize_context(grpc::ServerContext *context,
                                              grpc::Status proposed_status);
  void untrack_context(grpc::ServerContext *context) noexcept;
  void
  close_channel(const std::shared_ptr<SubscriberChannel> &channel) noexcept;
  [[nodiscard]] grpc::Status
  map_admission_error(g3::SubscriptionAdmissionError error) const;

  g3::MarketRuntime &runtime_;
  const std::string gateway_instance_id_;
  const GrpcServiceOptions options_;

  mutable std::mutex mutex_;
  std::condition_variable contexts_condition_;
  std::vector<TrackedContext> contexts_;
  bool admission_open_{true};
  bool shutdown_started_{false};
  bool cancellation_snapshot_active_{false};
};

// Small synchronous server host used by loopback tests and the opt-in real G7
// acceptance executable.
class OrderBookGrpcServer final {
public:
  OrderBookGrpcServer(g3::MarketRuntime &runtime,
                      std::string gateway_instance_id,
                      GrpcServiceOptions options = {});
  ~OrderBookGrpcServer();

  OrderBookGrpcServer(const OrderBookGrpcServer &) = delete;
  OrderBookGrpcServer &operator=(const OrderBookGrpcServer &) = delete;
  OrderBookGrpcServer(OrderBookGrpcServer &&) = delete;
  OrderBookGrpcServer &operator=(OrderBookGrpcServer &&) = delete;

  [[nodiscard]] bool start(const std::string &listen_address);
  void shutdown() noexcept;

  [[nodiscard]] int selected_port() const noexcept;
  [[nodiscard]] OrderBookGrpcService &service() noexcept;

private:
  OrderBookGrpcService service_;
  std::unique_ptr<grpc::Server> server_;
  int selected_port_{0};
  bool shutdown_{false};
};

[[nodiscard]] std::string generate_gateway_instance_id();

} // namespace binance_market_data::gateway::g7
