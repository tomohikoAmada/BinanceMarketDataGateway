#pragma once

#include "market_runtime.hpp"
#include "order_book_publication.hpp"
#if defined(BMD_GATEWAY_G9_ENABLED)
#include "event_publication.hpp"
#endif
#if defined(BMD_GATEWAY_G10_ENABLED)
#include "gateway_status.hpp"
#endif
#if defined(BMD_GATEWAY_G11_ENABLED)
#include "market_registry.hpp"
#endif

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

#if defined(BMD_GATEWAY_G9_ENABLED)
using EventRequestValidationResult =
    std::variant<g9::ValidatedEventSubscription, RequestValidationError>;
#endif

#if defined(BMD_GATEWAY_G10_ENABLED)
[[nodiscard]] bool validate_gateway_status_request(
    const gateway_wire::GatewayStatusRequest &request) noexcept;
#endif

[[nodiscard]] RequestValidationResult validate_order_book_request(
    const gateway_wire::OrderBookSubscriptionRequest &request,
    const std::string &gateway_instance_id);
#if defined(BMD_GATEWAY_G11_ENABLED)
[[nodiscard]] RequestValidationResult validate_g11_order_book_request(
    const gateway_wire::OrderBookSubscriptionRequest &request,
    const std::string &gateway_instance_id);
#endif

[[nodiscard]] gateway_wire::OrderBookStreamItem
materialize_stream_item(const SubscriberChannel &channel,
                        const PeekedPublication &publication);
#if defined(BMD_GATEWAY_G11_ENABLED)
[[nodiscard]] gateway_wire::OrderBookStreamItem
materialize_stream_item(const SubscriberChannel &channel,
                        const PeekedPublication &publication,
                        const g11::MarketKey &market_key);
#endif

#if defined(BMD_GATEWAY_G9_ENABLED)
[[nodiscard]] EventRequestValidationResult
validate_event_request(const gateway_wire::EventSubscriptionRequest &request,
                       const std::string &gateway_instance_id);
#if defined(BMD_GATEWAY_G11_ENABLED)
[[nodiscard]] EventRequestValidationResult validate_g11_event_request(
    const gateway_wire::EventSubscriptionRequest &request,
    const std::string &gateway_instance_id);
#endif

[[nodiscard]] gateway_wire::GatewayEventEnvelope
materialize_event_envelope(const g9::EventSubscriberChannel &channel,
                           const g9::PeekedEventPublication &publication);
#if defined(BMD_GATEWAY_G11_ENABLED)
[[nodiscard]] gateway_wire::GatewayEventEnvelope
materialize_event_envelope(const g9::EventSubscriberChannel &channel,
                           const g9::PeekedEventPublication &publication,
                           const g11::MarketKey &market_key);
#endif
#endif

#if defined(BMD_GATEWAY_G11_ENABLED)
inline constexpr std::size_t kMaximumGrpcTrackedContexts =
    2U * (kMaximumActiveSubscriptions + kPendingAdmissionCapacity +
          g9::kMaximumActiveEventSubscriptions);
#elif defined(BMD_GATEWAY_G9_ENABLED)
inline constexpr std::size_t kMaximumGrpcTrackedContexts =
    kMaximumActiveSubscriptions + kPendingAdmissionCapacity +
    g9::kMaximumActiveEventSubscriptions;
#else
inline constexpr std::size_t kMaximumGrpcTrackedContexts =
    kMaximumActiveSubscriptions + kPendingAdmissionCapacity;
#endif

struct GrpcServiceOptions final {
  GrpcServiceOptions(
      std::chrono::milliseconds idle_interval =
          kIdleClientCancellationCheckInterval,
      std::size_t tracked_context_limit = kMaximumGrpcTrackedContexts,
      std::function<
          bool(grpc::ServerWriter<gateway_wire::OrderBookStreamItem> &,
               const gateway_wire::OrderBookStreamItem &)>
          writer = {}
#if defined(BMD_GATEWAY_G9_ENABLED)
      ,
      std::function<
          bool(grpc::ServerWriter<gateway_wire::GatewayEventEnvelope> &,
               const gateway_wire::GatewayEventEnvelope &)>
          event_writer = {}
#endif
      )
      : idle_cancellation_check_interval{idle_interval},
        maximum_tracked_contexts{tracked_context_limit},
        write_override{std::move(writer)}
#if defined(BMD_GATEWAY_G9_ENABLED)
        ,
        event_write_override{std::move(event_writer)}
#endif
  {
  }

  std::chrono::milliseconds idle_cancellation_check_interval{
      kIdleClientCancellationCheckInterval};
  std::size_t maximum_tracked_contexts{kMaximumGrpcTrackedContexts};
  // Empty in production. Focused tests may replace the blocking Write call
  // while retaining the real generated synchronous RPC handler.
  std::function<bool(grpc::ServerWriter<gateway_wire::OrderBookStreamItem> &,
                     const gateway_wire::OrderBookStreamItem &)>
      write_override;
#if defined(BMD_GATEWAY_G9_ENABLED)
  std::function<bool(grpc::ServerWriter<gateway_wire::GatewayEventEnvelope> &,
                     const gateway_wire::GatewayEventEnvelope &)>
      event_write_override;
#endif
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
#if defined(BMD_GATEWAY_G9_ENABLED)
  OrderBookGrpcService(g3::MarketRuntime &runtime,
                       g9::EventPublication &event_publication,
                       std::string gateway_instance_id,
                       GrpcServiceOptions options = {});

  [[nodiscard]] grpc::Status SubscribeEvents(
      grpc::ServerContext *context,
      const gateway_wire::EventSubscriptionRequest *request,
      grpc::ServerWriter<gateway_wire::GatewayEventEnvelope> *writer) override;
#endif

#if defined(BMD_GATEWAY_G10_ENABLED)
  OrderBookGrpcService(g3::MarketRuntime &runtime, g5::SpotRecovery &recovery,
                       g9::EventPublication &event_publication,
                       g3::RuntimeClock clock, std::string gateway_instance_id,
                       GrpcServiceOptions options = {});

  [[nodiscard]] grpc::Status
  GetGatewayStatus(grpc::ServerContext *context,
                   const gateway_wire::GatewayStatusRequest *request,
                   gateway_wire::GatewayStatusSnapshot *response) override;

  [[nodiscard]] bool prepare_status_start() noexcept;
  void clear_status_start() noexcept;
  [[nodiscard]] bool status_inflight() const noexcept;
#endif
#if defined(BMD_GATEWAY_G11_ENABLED)
  OrderBookGrpcService(const g11::MarketRuntimeRegistry &registry,
                       g3::RuntimeClock clock, std::string gateway_instance_id,
                       GrpcServiceOptions options = {});
#endif

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

#if defined(BMD_GATEWAY_G10_ENABLED)
  class StatusSlotGuard;
  void release_status_slot() noexcept;
#endif

  [[nodiscard]] TrackResult track_context(grpc::ServerContext *context);
  [[nodiscard]] grpc::Status finalize_context(grpc::ServerContext *context,
                                              grpc::Status proposed_status);
  void untrack_context(grpc::ServerContext *context) noexcept;
  [[nodiscard]] grpc::Status
  map_admission_error(g3::SubscriptionAdmissionError error) const;

  g3::MarketRuntime *runtime_{nullptr};
#if defined(BMD_GATEWAY_G9_ENABLED)
  g9::EventPublication *event_publication_{nullptr};
#endif
#if defined(BMD_GATEWAY_G11_ENABLED)
  const g11::MarketRuntimeRegistry *market_registry_{nullptr};
#endif
  const std::string gateway_instance_id_;
  const GrpcServiceOptions options_;
#if defined(BMD_GATEWAY_G10_ENABLED)
  std::unique_ptr<g10::GatewayStatusAssembler> status_assembler_;
  bool status_inflight_{false};
#endif

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
#if defined(BMD_GATEWAY_G9_ENABLED)
  OrderBookGrpcServer(g3::MarketRuntime &runtime,
                      g9::EventPublication &event_publication,
                      std::string gateway_instance_id,
                      GrpcServiceOptions options = {});
#endif
#if defined(BMD_GATEWAY_G10_ENABLED)
  OrderBookGrpcServer(g3::MarketRuntime &runtime, g5::SpotRecovery &recovery,
                      g9::EventPublication &event_publication,
                      g3::RuntimeClock clock, std::string gateway_instance_id,
                      GrpcServiceOptions options = {});
#endif
#if defined(BMD_GATEWAY_G11_ENABLED)
  OrderBookGrpcServer(const g11::MarketRuntimeRegistry &registry,
                      g3::RuntimeClock clock, std::string gateway_instance_id,
                      GrpcServiceOptions options = {});
#endif
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
