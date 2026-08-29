#include "grpc_service.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace binance_market_data::gateway::g7 {

namespace {

[[nodiscard]] bool identifier_safe(const std::string &value) noexcept {
  if (value.empty() || value.size() > 128U) {
    return false;
  }
  const auto allowed = [](unsigned char character) {
    return (character >= static_cast<unsigned char>('A') &&
            character <= static_cast<unsigned char>('Z')) ||
           (character >= static_cast<unsigned char>('a') &&
            character <= static_cast<unsigned char>('z')) ||
           (character >= static_cast<unsigned char>('0') &&
            character <= static_cast<unsigned char>('9')) ||
           character == static_cast<unsigned char>('.') ||
           character == static_cast<unsigned char>('_') ||
           character == static_cast<unsigned char>(':') ||
           character == static_cast<unsigned char>('/') ||
           character == static_cast<unsigned char>('-');
  };
  const auto first = static_cast<unsigned char>(value.front());
  if (!((first >= static_cast<unsigned char>('A') &&
         first <= static_cast<unsigned char>('Z')) ||
        (first >= static_cast<unsigned char>('a') &&
         first <= static_cast<unsigned char>('z')) ||
        (first >= static_cast<unsigned char>('0') &&
         first <= static_cast<unsigned char>('9')))) {
    return false;
  }
  return std::all_of(value.begin(), value.end(), [&](char character) {
    return allowed(static_cast<unsigned char>(character));
  });
}

[[nodiscard]] bool contains_version(
    const google::protobuf::RepeatedPtrField<std::string> &versions,
    const std::string_view expected) {
  return std::any_of(
      versions.begin(), versions.end(),
      [expected](const auto &version) { return version == expected; });
}

[[nodiscard]] bool valid_version_list(
    const google::protobuf::RepeatedPtrField<std::string> &versions) noexcept {
  for (int index = 0; index < versions.size(); ++index) {
    const auto &version = versions.Get(index);
    if (version.empty() ||
        std::isspace(static_cast<unsigned char>(version.front())) != 0 ||
        std::isspace(static_cast<unsigned char>(version.back())) != 0) {
      return false;
    }
    for (int earlier = 0; earlier < index; ++earlier) {
      if (versions.Get(earlier) == version) {
        return false;
      }
    }
  }
  return true;
}

void populate_delivery_metadata(
    gateway_wire::OrderBookStreamItem &item, const SubscriberChannel &channel,
    std::uint64_t session_sequence,
    std::optional<std::uint64_t> connection_generation,
    PublicationTime published_at) {
  auto *metadata = item.mutable_delivery_metadata();
  metadata->set_protocol_version(kProtocolVersion);
  metadata->set_gateway_instance_id(channel.gateway_instance_id());
  metadata->set_subscription_id(channel.subscription_id());
  if (connection_generation.has_value()) {
    metadata->set_connection_generation(*connection_generation);
  }
  metadata->set_session_sequence(session_sequence);
  metadata->set_publish_time_utc_ns(published_at.utc_ns);
  if (published_at.monotonic_ns.has_value()) {
    metadata->set_publish_monotonic_ns(*published_at.monotonic_ns);
  }
}

} // namespace

RequestValidationResult validate_order_book_request(
    const gateway_wire::OrderBookSubscriptionRequest &request,
    const std::string &gateway_instance_id) {
  if (!identifier_safe(gateway_instance_id)) {
    return RequestValidationError::InvalidArgument;
  }
  if (!identifier_safe(request.request_id()) ||
      request.schema_version() != kOrderBookRequestSchema ||
      request.venue() != common_wire::VENUE_BINANCE ||
      request.market() != common_wire::MARKET_SPOT ||
      request.symbol() != "BTCUSDT" ||
      request.initial_snapshot_mode() !=
          common_wire::INITIAL_SNAPSHOT_MODE_REQUIRED ||
      (request.has_depth_limit() && request.depth_limit() <= 0) ||
      !valid_version_list(request.supported_snapshot_schema_versions()) ||
      !valid_version_list(request.supported_update_schema_versions())) {
    return RequestValidationError::InvalidArgument;
  }
  if (!contains_version(request.supported_snapshot_schema_versions(),
                        kSnapshotSchema) ||
      !contains_version(request.supported_update_schema_versions(),
                        kUpdateSchema)) {
    return RequestValidationError::FailedPrecondition;
  }
  return ValidatedOrderBookSubscription{
      request.request_id(), gateway_instance_id,
      request.has_depth_limit()
          ? std::optional<std::int32_t>{request.depth_limit()}
          : std::nullopt};
}

gateway_wire::OrderBookStreamItem
materialize_stream_item(const SubscriberChannel &channel,
                        const PeekedPublication &publication) {
  if (!publication.has_value()) {
    throw std::invalid_argument{"cannot materialize an empty publication"};
  }
  gateway_wire::OrderBookStreamItem item;
  if (publication.ordinary != nullptr) {
    const auto &record = *publication.ordinary;
    populate_delivery_metadata(item, channel, record.session_sequence,
                               record.connection_generation,
                               record.published_at);
    if (const auto *accepted =
            std::get_if<gateway_wire::SubscriptionAccepted>(&record.payload)) {
      *item.mutable_subscription_accepted() = *accepted;
    } else if (const auto *snapshot =
                   std::get_if<projection_wire::LocalOrderBookSnapshot>(
                       &record.payload)) {
      *item.mutable_snapshot() = *snapshot;
    } else {
      const auto &update =
          std::get<std::shared_ptr<const market_wire::DepthUpdate>>(
              record.payload);
      if (update == nullptr) {
        throw std::logic_error{"publication update payload is null"};
      }
      *item.mutable_depth_update() = *update;
    }
    return item;
  }

  const auto &terminal = *publication.terminal;
  populate_delivery_metadata(item, channel, terminal.session_sequence,
                             terminal.connection_generation,
                             terminal.published_at);
  auto *gap = item.mutable_consumer_gap();
  gap->set_schema_version(kConsumerGapSchema);
  gap->set_subscription_id(channel.subscription_id());
  gap->set_detected_time_utc_ns(terminal.published_at.utc_ns);
  gap->set_reason(terminal.reason);
  gap->set_recovery_action(terminal.recovery_action);
  gap->set_market(common_wire::MARKET_SPOT);
  gap->set_symbol("BTCUSDT");
  gap->set_stream(common_wire::STREAM_DIFF_DEPTH);
  return item;
}

OrderBookGrpcService::OrderBookGrpcService(g3::MarketRuntime &runtime,
                                           std::string gateway_instance_id,
                                           GrpcServiceOptions options)
    : runtime_{runtime}, gateway_instance_id_{std::move(gateway_instance_id)},
      options_{options} {
  if (!identifier_safe(gateway_instance_id_)) {
    throw std::invalid_argument{"invalid G7 gateway_instance_id"};
  }
  if (options_.idle_cancellation_check_interval <=
          std::chrono::milliseconds::zero() ||
      options_.maximum_tracked_contexts == 0U) {
    throw std::invalid_argument{"invalid G7 service limits"};
  }
  if (options_.maximum_tracked_contexts >
      kMaximumActiveSubscriptions + kPendingAdmissionCapacity) {
    throw std::invalid_argument{"G7 handler tracking exceeds frozen bound"};
  }
  contexts_.reserve(options_.maximum_tracked_contexts);
}

grpc::Status OrderBookGrpcService::SubscribeOrderBook(
    grpc::ServerContext *context,
    const gateway_wire::OrderBookSubscriptionRequest *request,
    grpc::ServerWriter<gateway_wire::OrderBookStreamItem> *writer) {
  if (context == nullptr || request == nullptr || writer == nullptr) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "null RPC argument"};
  }
  const auto tracked = track_context(context);
  if (tracked == TrackResult::Closed) {
    return {grpc::StatusCode::UNAVAILABLE,
            "order-book publication is shutting down"};
  }
  if (tracked == TrackResult::Full) {
    return {grpc::StatusCode::RESOURCE_EXHAUSTED,
            "bounded G7 handler tracking is full"};
  }

  struct Untrack final {
    OrderBookGrpcService *service;
    grpc::ServerContext *context;
    ~Untrack() { service->untrack_context(context); }
  } untrack{this, context};

  const auto validated =
      validate_order_book_request(*request, gateway_instance_id_);
  if (const auto *failure = std::get_if<RequestValidationError>(&validated)) {
    return *failure == RequestValidationError::InvalidArgument
               ? grpc::Status{grpc::StatusCode::INVALID_ARGUMENT,
                              "unsupported or malformed order-book request"}
               : grpc::Status{
                     grpc::StatusCode::FAILED_PRECONDITION,
                     "required payload schema negotiation unavailable"};
  }

  auto admission = runtime_.admit_order_book_subscription(
      std::get<ValidatedOrderBookSubscription>(validated));
  if (const auto *failure =
          std::get_if<g3::SubscriptionAdmissionError>(&admission)) {
    return map_admission_error(*failure);
  }
  auto channel =
      std::get<g3::AcceptedSubscription>(std::move(admission)).channel;
  if (channel == nullptr) {
    return {grpc::StatusCode::INTERNAL, "accepted channel is missing"};
  }

  for (;;) {
    if (context->IsCancelled()) {
      close_channel(channel);
      return {grpc::StatusCode::CANCELLED, "client cancelled"};
    }
    auto publication = channel->peek();
    if (!publication.has_value()) {
      if (channel->state() == SubscriberState::Closed) {
        close_channel(channel);
        return grpc::Status::OK;
      }
      channel->wait_for_change(options_.idle_cancellation_check_interval);
      continue;
    }

    gateway_wire::OrderBookStreamItem item;
    try {
      item = materialize_stream_item(*channel, publication);
    } catch (...) {
      close_channel(channel);
      return {grpc::StatusCode::INTERNAL,
              "failed to materialize order-book stream item"};
    }
    bool write_succeeded = false;
    try {
      write_succeeded = options_.write_override
                            ? options_.write_override(*writer, item)
                            : writer->Write(item);
    } catch (...) {
      close_channel(channel);
      return {grpc::StatusCode::INTERNAL, "subscriber writer test seam failed"};
    }
    if (context->IsCancelled() || !write_succeeded) {
      close_channel(channel);
      return context->IsCancelled()
                 ? grpc::Status{grpc::StatusCode::CANCELLED, "client cancelled"}
                 : grpc::Status::OK;
    }
    const auto acknowledged = channel->acknowledge(publication);
    if (acknowledged == AcknowledgeResult::Mismatch) {
      close_channel(channel);
      return {grpc::StatusCode::INTERNAL,
              "subscriber peek/ack invariant failed"};
    }
    if (acknowledged == AcknowledgeResult::Closed) {
      close_channel(channel);
      return grpc::Status::OK;
    }
    if (publication.is_terminal()) {
      runtime_.notify_subscriber_closed();
      return grpc::Status::OK;
    }
  }
}

void OrderBookGrpcService::begin_shutdown() noexcept {
  {
    std::lock_guard lock{mutex_};
    if (shutdown_started_) {
      return;
    }
    shutdown_started_ = true;
    admission_open_ = false;
  }
  runtime_.close_publication_admission();
  static_cast<void>(runtime_.shutdown_publication());

  std::array<grpc::ServerContext *,
             kMaximumActiveSubscriptions + kPendingAdmissionCapacity>
      contexts{};
  std::size_t context_count = 0U;
  {
    std::lock_guard lock{mutex_};
    cancellation_snapshot_active_ = true;
    context_count = contexts_.size();
    std::copy(contexts_.begin(), contexts_.end(), contexts.begin());
  }
  for (std::size_t index = 0U; index < context_count; ++index) {
    auto *context = contexts[index];
    if (context != nullptr) {
      context->TryCancel();
    }
  }
  {
    std::lock_guard lock{mutex_};
    cancellation_snapshot_active_ = false;
  }
  contexts_condition_.notify_all();
}

bool OrderBookGrpcService::admission_open() const noexcept {
  std::lock_guard lock{mutex_};
  return admission_open_;
}

std::size_t OrderBookGrpcService::tracked_context_count() const noexcept {
  std::lock_guard lock{mutex_};
  return contexts_.size();
}

const std::string &OrderBookGrpcService::gateway_instance_id() const noexcept {
  return gateway_instance_id_;
}

OrderBookGrpcService::TrackResult
OrderBookGrpcService::track_context(grpc::ServerContext *context) {
  std::lock_guard lock{mutex_};
  if (!admission_open_) {
    return TrackResult::Closed;
  }
  if (contexts_.size() == options_.maximum_tracked_contexts) {
    return TrackResult::Full;
  }
  contexts_.push_back(context);
  return TrackResult::Tracked;
}

void OrderBookGrpcService::untrack_context(
    grpc::ServerContext *context) noexcept {
  std::unique_lock lock{mutex_};
  contexts_condition_.wait(lock,
                           [this] { return !cancellation_snapshot_active_; });
  const auto found = std::find(contexts_.begin(), contexts_.end(), context);
  if (found != contexts_.end()) {
    contexts_.erase(found);
  }
}

void OrderBookGrpcService::close_channel(
    const std::shared_ptr<SubscriberChannel> &channel) noexcept {
  channel->close_from_writer();
  runtime_.notify_subscriber_closed();
}

grpc::Status OrderBookGrpcService::map_admission_error(
    g3::SubscriptionAdmissionError error) const {
  switch (error) {
  case g3::SubscriptionAdmissionError::NotLive:
    return {grpc::StatusCode::UNAVAILABLE,
            "market runtime is not Live/Synchronized"};
  case g3::SubscriptionAdmissionError::PendingLimit:
  case g3::SubscriptionAdmissionError::ActiveLimit:
    return {grpc::StatusCode::RESOURCE_EXHAUSTED,
            "bounded order-book subscription capacity is full"};
  case g3::SubscriptionAdmissionError::InvalidDepthLimit:
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid depth_limit"};
  case g3::SubscriptionAdmissionError::ShuttingDown:
  case g3::SubscriptionAdmissionError::Stopped:
  case g3::SubscriptionAdmissionError::NotStarted:
    return {grpc::StatusCode::UNAVAILABLE,
            "order-book publication is unavailable"};
  case g3::SubscriptionAdmissionError::ClockError:
  case g3::SubscriptionAdmissionError::IdExhausted:
  case g3::SubscriptionAdmissionError::InternalError:
    return {grpc::StatusCode::INTERNAL,
            "order-book subscription admission failed"};
  }
  return {grpc::StatusCode::INTERNAL,
          "unknown order-book subscription admission failure"};
}

OrderBookGrpcServer::OrderBookGrpcServer(g3::MarketRuntime &runtime,
                                         std::string gateway_instance_id,
                                         GrpcServiceOptions options)
    : service_{runtime, std::move(gateway_instance_id), options} {}

OrderBookGrpcServer::~OrderBookGrpcServer() { shutdown(); }

bool OrderBookGrpcServer::start(const std::string &listen_address) {
  if (server_ != nullptr || shutdown_) {
    return false;
  }
  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials(),
                           &selected_port_);
  builder.RegisterService(&service_);
  server_ = builder.BuildAndStart();
  return server_ != nullptr && selected_port_ > 0;
}

void OrderBookGrpcServer::shutdown() noexcept {
  if (shutdown_) {
    return;
  }
  shutdown_ = true;
  service_.begin_shutdown();
  if (server_ != nullptr) {
    server_->Shutdown();
    server_->Wait();
    server_.reset();
  }
  assert(service_.tracked_context_count() == 0U);
}

int OrderBookGrpcServer::selected_port() const noexcept {
  return selected_port_;
}

OrderBookGrpcService &OrderBookGrpcServer::service() noexcept {
  return service_;
}

std::string generate_gateway_instance_id() {
  std::random_device entropy;
  const std::array<std::uint32_t, 4U> words{entropy(), entropy(), entropy(),
                                            entropy()};
  std::ostringstream output;
  output << "gw-" << std::hex << std::setfill('0');
  for (const auto word : words) {
    output << std::setw(8) << word;
  }
  return output.str();
}

} // namespace binance_market_data::gateway::g7
