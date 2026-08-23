#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>
#include <binance_market_data/projection/v1/numeric/decimal_scale.hpp>
#include <binance_market_data/projection/v1/numeric/numeric_spec.hpp>
#include <binance_market_data/projection/v1/projection_state/book_projection.hpp>
#include <binance_market_data/projection_adapter/v1/proto_adapter.hpp>

#include <cstdlib>
#include <memory>
#include <variant>

int main() {
  using namespace binance_market_data;
  using projection::v1::BookProjection;
  using projection::v1::NumericSpec;
  using projection::v1::SequencePolicyKind;

  gateway::v1::EventSubscriptionRequest message;
  gateway::v1::BinanceMarketDataGatewayService::Service service;
  const auto price_scale = projection::v1::DecimalScale::create(2U);
  const auto quantity_scale = projection::v1::DecimalScale::create(3U);
  if (!price_scale.has_value() || !quantity_scale.has_value()) {
    return EXIT_FAILURE;
  }
  BookProjection book_projection{NumericSpec{*price_scale, *quantity_scale},
                                 SequencePolicyKind::Spot};
  const auto depth_limit = projection_adapter::v1::DepthLimit::create(1);

  // The executable deliberately has no Gateway business logic. Its one final
  // link consumes the Contracts message target, Contracts gRPC service target,
  // Projection Core, and adapter.
  return message.GetDescriptor() != nullptr &&
                 static_cast<grpc::Service *>(&service) != nullptr &&
                 book_projection.status() ==
                     projection::v1::ProjectionStatus::AwaitingBaseline &&
                 std::holds_alternative<projection_adapter::v1::DepthLimit>(
                     depth_limit)
             ? EXIT_SUCCESS
             : EXIT_FAILURE;
}
