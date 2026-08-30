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

#if defined(BMD_GATEWAY_G9_ENABLED)
void populate_event_delivery_metadata(
    gateway_wire::GatewayEventEnvelope &item,
    const g9::EventSubscriberChannel &channel, std::uint64_t session_sequence,
    std::optional<std::uint64_t> connection_generation,
    g9::EventPublicationTime published_at) {
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

[[nodiscard]] std::optional<std::string_view>
required_event_schema(common_wire::Stream stream) noexcept {
  switch (stream) {
  case common_wire::STREAM_DIFF_DEPTH:
    return g9::kDepthEventSchema;
  case common_wire::STREAM_AGG_TRADE:
    return g9::kAggTradeEventSchema;
  case common_wire::STREAM_BOOK_TICKER:
    return g9::kBookTickerEventSchema;
  case common_wire::STREAM_DEPTH_SNAPSHOT:
  case common_wire::STREAM_UNSPECIFIED:
    return std::nullopt;
  default:
    return std::nullopt;
  }
}
#endif

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

#if defined(BMD_GATEWAY_G9_ENABLED)
EventRequestValidationResult
validate_event_request(const gateway_wire::EventSubscriptionRequest &request,
                       const std::string &gateway_instance_id) {
  if (!identifier_safe(gateway_instance_id) ||
      !identifier_safe(request.request_id()) ||
      request.schema_version() != g9::kEventRequestSchema ||
      request.delivery_mode() != common_wire::DELIVERY_MODE_CONTIGUOUS_EVENTS ||
      request.selectors_size() != 1 ||
      request.supported_payload_schema_versions().empty() ||
      !valid_version_list(request.supported_payload_schema_versions())) {
    return RequestValidationError::InvalidArgument;
  }
  const auto &selector = request.selectors(0);
  const auto required_schema = required_event_schema(selector.stream());
  if (selector.venue() != common_wire::VENUE_BINANCE ||
      selector.market() != common_wire::MARKET_SPOT ||
      selector.symbol() != "BTCUSDT" || !required_schema.has_value()) {
    return RequestValidationError::InvalidArgument;
  }
  if (!contains_version(request.supported_payload_schema_versions(),
                        *required_schema)) {
    return RequestValidationError::FailedPrecondition;
  }
  return g9::ValidatedEventSubscription{request.request_id(), selector.stream(),
                                        std::string{*required_schema}};
}

gateway_wire::GatewayEventEnvelope
materialize_event_envelope(const g9::EventSubscriberChannel &channel,
                           const g9::PeekedEventPublication &publication) {
  if (!publication.has_value()) {
    throw std::invalid_argument{
        "cannot materialize an empty event publication"};
  }
  gateway_wire::GatewayEventEnvelope item;
  if (publication.ordinary != nullptr) {
    const auto &record = *publication.ordinary;
    populate_event_delivery_metadata(item, channel, record.session_sequence,
                                     record.connection_generation,
                                     record.published_at);
    if (const auto *accepted =
            std::get_if<gateway_wire::SubscriptionAccepted>(&record.payload)) {
      *item.mutable_subscription_accepted() = *accepted;
      return item;
    }
    const auto &event =
        std::get<std::shared_ptr<const g4::NormalizedSpotEvent>>(
            record.payload);
    if (event == nullptr ||
        g9::normalized_event_stream(*event) != channel.stream()) {
      throw std::logic_error{"event publication payload invariant failed"};
    }
    if (const auto *depth = std::get_if<g4::market::DepthUpdate>(event.get())) {
      *item.mutable_depth_update() = *depth;
    } else if (const auto *trade =
                   std::get_if<g4::market::AggTrade>(event.get())) {
      *item.mutable_agg_trade() = *trade;
    } else {
      *item.mutable_book_ticker() = std::get<g4::market::BookTicker>(*event);
    }
    return item;
  }

  const auto &terminal = *publication.terminal;
  populate_event_delivery_metadata(item, channel, terminal.session_sequence,
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
  gap->set_stream(channel.stream());
  return item;
}
#endif

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
  if (options_.maximum_tracked_contexts > kMaximumGrpcTrackedContexts) {
    throw std::invalid_argument{"gRPC handler tracking exceeds frozen bound"};
  }
  contexts_.reserve(options_.maximum_tracked_contexts);
}

#if defined(BMD_GATEWAY_G9_ENABLED)
OrderBookGrpcService::OrderBookGrpcService(
    g3::MarketRuntime &runtime, g9::EventPublication &event_publication,
    std::string gateway_instance_id, GrpcServiceOptions options)
    : OrderBookGrpcService(runtime, std::move(gateway_instance_id),
                           std::move(options)) {
  if (event_publication.gateway_instance_id() != gateway_instance_id_) {
    throw std::invalid_argument{
        "G7/G9 gateway_instance_id values must be identical"};
  }
  event_publication_ = &event_publication;
}
#endif

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

  bool finalization_committed = false;
  struct Untrack final {
    OrderBookGrpcService *service;
    grpc::ServerContext *context;
    bool *finalization_committed;
    ~Untrack() {
      if (!*finalization_committed) {
        service->untrack_context(context);
      }
    }
  } untrack{this, context, &finalization_committed};
  const auto finalize = [&](grpc::Status proposed_status) {
    auto final_status = finalize_context(context, std::move(proposed_status));
    finalization_committed = true;
    return final_status;
  };

  const auto validated =
      validate_order_book_request(*request, gateway_instance_id_);
  if (const auto *failure = std::get_if<RequestValidationError>(&validated)) {
    return finalize(
        *failure == RequestValidationError::InvalidArgument
            ? grpc::Status{grpc::StatusCode::INVALID_ARGUMENT,
                           "unsupported or malformed order-book request"}
            : grpc::Status{grpc::StatusCode::FAILED_PRECONDITION,
                           "required payload schema negotiation unavailable"});
  }

  auto admission = runtime_.admit_order_book_subscription(
      std::get<ValidatedOrderBookSubscription>(validated));
  if (const auto *failure =
          std::get_if<g3::SubscriptionAdmissionError>(&admission)) {
    return finalize(map_admission_error(*failure));
  }
  auto channel =
      std::get<g3::AcceptedSubscription>(std::move(admission)).channel;
  if (channel == nullptr) {
    return finalize(
        {grpc::StatusCode::INTERNAL, "accepted channel is missing"});
  }

  for (;;) {
    if (context->IsCancelled()) {
      close_channel(channel);
      return finalize({grpc::StatusCode::CANCELLED, "client cancelled"});
    }
    auto publication = channel->peek();
    if (!publication.has_value()) {
      if (channel->state() == SubscriberState::Closed) {
        close_channel(channel);
        return finalize(grpc::Status::OK);
      }
      channel->wait_for_change(options_.idle_cancellation_check_interval);
      continue;
    }

    gateway_wire::OrderBookStreamItem item;
    try {
      item = materialize_stream_item(*channel, publication);
    } catch (...) {
      close_channel(channel);
      return finalize({grpc::StatusCode::INTERNAL,
                       "failed to materialize order-book stream item"});
    }
    bool write_succeeded = false;
    try {
      write_succeeded = options_.write_override
                            ? options_.write_override(*writer, item)
                            : writer->Write(item);
    } catch (...) {
      close_channel(channel);
      return finalize(
          {grpc::StatusCode::INTERNAL, "subscriber writer test seam failed"});
    }
    if (context->IsCancelled() || !write_succeeded) {
      close_channel(channel);
      return finalize(
          context->IsCancelled()
              ? grpc::Status{grpc::StatusCode::CANCELLED, "client cancelled"}
              : grpc::Status::OK);
    }
    const auto acknowledged = channel->acknowledge(publication);
    if (acknowledged == AcknowledgeResult::Mismatch) {
      close_channel(channel);
      return finalize(
          {grpc::StatusCode::INTERNAL, "subscriber peek/ack invariant failed"});
    }
    if (acknowledged == AcknowledgeResult::Closed) {
      close_channel(channel);
      return finalize(grpc::Status::OK);
    }
    if (publication.is_terminal()) {
      runtime_.notify_subscriber_closed();
      return finalize(grpc::Status::OK);
    }
  }
}

#if defined(BMD_GATEWAY_G9_ENABLED)
grpc::Status OrderBookGrpcService::SubscribeEvents(
    grpc::ServerContext *context,
    const gateway_wire::EventSubscriptionRequest *request,
    grpc::ServerWriter<gateway_wire::GatewayEventEnvelope> *writer) {
  if (context == nullptr || request == nullptr || writer == nullptr) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "null RPC argument"};
  }
  const auto tracked = track_context(context);
  if (tracked == TrackResult::Closed) {
    return {grpc::StatusCode::UNAVAILABLE,
            "event publication is shutting down"};
  }
  if (tracked == TrackResult::Full) {
    return {grpc::StatusCode::RESOURCE_EXHAUSTED,
            "bounded gRPC handler tracking is full"};
  }

  bool finalization_committed = false;
  struct Untrack final {
    OrderBookGrpcService *service;
    grpc::ServerContext *context;
    bool *finalization_committed;
    ~Untrack() {
      if (!*finalization_committed) {
        service->untrack_context(context);
      }
    }
  } untrack{this, context, &finalization_committed};
  const auto finalize = [&](grpc::Status proposed_status) {
    auto final_status = finalize_context(context, std::move(proposed_status));
    finalization_committed = true;
    return final_status;
  };

  if (event_publication_ == nullptr) {
    return finalize({grpc::StatusCode::UNAVAILABLE,
                     "G9 event publication is not configured"});
  }
  const auto validated = validate_event_request(*request, gateway_instance_id_);
  if (const auto *failure = std::get_if<RequestValidationError>(&validated)) {
    return finalize(
        *failure == RequestValidationError::InvalidArgument
            ? grpc::Status{grpc::StatusCode::INVALID_ARGUMENT,
                           "unsupported or malformed event request"}
            : grpc::Status{grpc::StatusCode::FAILED_PRECONDITION,
                           "required event payload schema unavailable"});
  }

  auto admission = event_publication_->admit(
      std::get<g9::ValidatedEventSubscription>(validated));
  if (const auto *failure =
          std::get_if<g9::EventSubscriptionAdmissionError>(&admission)) {
    switch (*failure) {
    case g9::EventSubscriptionAdmissionError::SourceUnavailable:
    case g9::EventSubscriptionAdmissionError::ShuttingDown:
      return finalize(
          {grpc::StatusCode::UNAVAILABLE, "event source is unavailable"});
    case g9::EventSubscriptionAdmissionError::ActiveLimit:
      return finalize({grpc::StatusCode::RESOURCE_EXHAUSTED,
                       "bounded event subscription capacity is full"});
    case g9::EventSubscriptionAdmissionError::ClockError:
    case g9::EventSubscriptionAdmissionError::IdExhausted:
    case g9::EventSubscriptionAdmissionError::InternalError:
      return finalize(
          {grpc::StatusCode::INTERNAL, "event subscription admission failed"});
    }
  }
  auto channel =
      std::get<g9::AcceptedEventSubscription>(std::move(admission)).channel;
  if (channel == nullptr) {
    return finalize(
        {grpc::StatusCode::INTERNAL, "accepted event channel is missing"});
  }

  for (;;) {
    if (context->IsCancelled()) {
      close_event_channel(channel);
      return finalize({grpc::StatusCode::CANCELLED, "client cancelled"});
    }
    auto publication = channel->peek();
    if (!publication.has_value()) {
      const auto state = channel->state();
      if (state == g9::EventChannelState::TerminalUnavailable) {
        close_event_channel(channel);
        return finalize({grpc::StatusCode::UNAVAILABLE,
                         "event source terminated permanently"});
      }
      if (state == g9::EventChannelState::Closed) {
        close_event_channel(channel);
        return finalize(grpc::Status::OK);
      }
      channel->wait_for_change(options_.idle_cancellation_check_interval);
      continue;
    }

    gateway_wire::GatewayEventEnvelope item;
    try {
      item = materialize_event_envelope(*channel, publication);
    } catch (...) {
      close_event_channel(channel);
      return finalize({grpc::StatusCode::INTERNAL,
                       "failed to materialize event stream item"});
    }
    bool write_succeeded = false;
    try {
      write_succeeded = options_.event_write_override
                            ? options_.event_write_override(*writer, item)
                            : writer->Write(item);
    } catch (...) {
      close_event_channel(channel);
      return finalize({grpc::StatusCode::INTERNAL,
                       "event subscriber writer test seam failed"});
    }
    if (context->IsCancelled() || !write_succeeded) {
      close_event_channel(channel);
      return finalize(
          context->IsCancelled()
              ? grpc::Status{grpc::StatusCode::CANCELLED, "client cancelled"}
              : grpc::Status::OK);
    }
    const auto acknowledged = channel->acknowledge(publication);
    if (acknowledged == g9::EventAcknowledgeResult::Mismatch) {
      close_event_channel(channel);
      return finalize({grpc::StatusCode::INTERNAL,
                       "event subscriber peek/ack invariant failed"});
    }
    if (acknowledged == g9::EventAcknowledgeResult::Closed) {
      event_publication_->remove(channel);
      return finalize(grpc::Status::OK);
    }
  }
}
#endif

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
#if defined(BMD_GATEWAY_G9_ENABLED)
  if (event_publication_ != nullptr) {
    event_publication_->shutdown();
  }
#endif

  std::array<grpc::ServerContext *, kMaximumGrpcTrackedContexts> contexts{};
  std::size_t context_count = 0U;
  {
    std::lock_guard lock{mutex_};
    cancellation_snapshot_active_ = true;
    context_count = contexts_.size();
    for (std::size_t index = 0U; index < context_count; ++index) {
      contexts_[index].selected_for_try_cancel = true;
      contexts[index] = contexts_[index].context;
    }
  }
  if (options_.cancellation_snapshot_ready) {
    options_.cancellation_snapshot_ready();
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
  contexts_.push_back(TrackedContext{context});
  return TrackResult::Tracked;
}

grpc::Status
OrderBookGrpcService::finalize_context(grpc::ServerContext *context,
                                       grpc::Status proposed_status) {
  if (options_.before_context_finalization) {
    options_.before_context_finalization(proposed_status.error_code());
  }
  std::unique_lock lock{mutex_};
  if (cancellation_snapshot_active_ && options_.context_finalization_waiting) {
    options_.context_finalization_waiting();
  }
  contexts_condition_.wait(lock,
                           [this] { return !cancellation_snapshot_active_; });
  const auto found = std::find_if(
      contexts_.begin(), contexts_.end(),
      [context](const auto &entry) { return entry.context == context; });
  const auto selected_for_try_cancel =
      found != contexts_.end() && found->selected_for_try_cancel;
  if (found != contexts_.end()) {
    contexts_.erase(found);
  }
  lock.unlock();
  if (proposed_status.ok() && selected_for_try_cancel) {
    proposed_status =
        grpc::Status{grpc::StatusCode::CANCELLED, "server shutdown"};
  }
  if (options_.after_context_finalization) {
    options_.after_context_finalization(proposed_status.error_code());
  }
  return proposed_status;
}

void OrderBookGrpcService::untrack_context(
    grpc::ServerContext *context) noexcept {
  std::unique_lock lock{mutex_};
  contexts_condition_.wait(lock,
                           [this] { return !cancellation_snapshot_active_; });
  const auto found = std::find_if(
      contexts_.begin(), contexts_.end(),
      [context](const auto &entry) { return entry.context == context; });
  if (found != contexts_.end()) {
    contexts_.erase(found);
  }
}

void OrderBookGrpcService::close_channel(
    const std::shared_ptr<SubscriberChannel> &channel) noexcept {
  channel->close_from_writer();
  runtime_.notify_subscriber_closed();
}

#if defined(BMD_GATEWAY_G9_ENABLED)
void OrderBookGrpcService::close_event_channel(
    const std::shared_ptr<g9::EventSubscriberChannel> &channel) noexcept {
  channel->close_from_writer();
  if (event_publication_ != nullptr) {
    event_publication_->remove(channel);
  }
}
#endif

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

#if defined(BMD_GATEWAY_G9_ENABLED)
OrderBookGrpcServer::OrderBookGrpcServer(
    g3::MarketRuntime &runtime, g9::EventPublication &event_publication,
    std::string gateway_instance_id, GrpcServiceOptions options)
    : service_{runtime, event_publication, std::move(gateway_instance_id),
               std::move(options)} {}
#endif

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
