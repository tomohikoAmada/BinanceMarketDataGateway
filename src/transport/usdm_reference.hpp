#pragma once

#include "publication/event_publication.hpp"
#include "runtime/clock.hpp"
#include "runtime/market_key.hpp"
#include "transport/spot_transport.hpp"

#include <binance_market_data/market/v1/reference_data.pb.h>
#include <binance_market_data/projection/v1/numeric/numeric_spec.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

namespace binance_market_data::gateway::transport {

namespace market = ::binance_market_data::market::v1;
namespace common_wire = ::binance_market_data::common::v1;

enum class UsdMReferenceKind : std::uint8_t {
  OpenInterest,
  TopLongShortAccountRatio,
  TopLongShortPositionRatio,
  GlobalLongShortAccountRatio,
  TakerBuySellVolume,
  Instrument,
};

struct UsdMReferenceParseError final {
  std::string field;
  std::string message;
};

using UsdMReferenceParseResult =
    std::variant<market::ReferenceObservation, UsdMReferenceParseError>;

struct UsdMInstrumentBatchRequest final {
  std::string_view symbol;
  std::string_view request_id;
};

struct UsdMInstrumentBatch final {
  std::vector<UsdMReferenceParseResult> symbols;
};

using UsdMInstrumentBatchResult =
    std::variant<UsdMInstrumentBatch, UsdMReferenceParseError>;

[[nodiscard]] common_wire::ReferenceSourceKind
usdm_reference_source_kind(UsdMReferenceKind kind) noexcept;

[[nodiscard]] std::string_view
usdm_reference_kind_name(UsdMReferenceKind kind) noexcept;

[[nodiscard]] std::string usdm_reference_target(UsdMReferenceKind kind,
                                                std::string_view symbol);

[[nodiscard]] UsdMReferenceParseResult
parse_usdm_reference_payload(UsdMReferenceKind kind, std::string_view payload,
                             runtime::ClockSample received_at,
                             std::string_view request_id,
                             std::string_view symbol);

[[nodiscard]] UsdMInstrumentBatchResult parse_usdm_instrument_definitions_batch(
    std::string_view payload, runtime::ClockSample received_at,
    std::span<const UsdMInstrumentBatchRequest> requests);

[[nodiscard]] UsdMReferenceParseResult parse_usdm_instrument_definition(
    std::string_view payload, runtime::ClockSample received_at,
    std::string_view request_id, std::string_view symbol);

struct UsdMReferenceSeed final {
  runtime::MarketKey key;
  std::string exchange_info_body;
  runtime::ClockSample received_at{};
  std::optional<market::ReferenceObservation> instrument{};
  std::shared_ptr<const std::string> shared_exchange_info_body{};
};

struct UsdMReferenceProduct final {
  runtime::MarketKey key;
  publication::EventPublication *publication{nullptr};
  std::optional<projection::v1::NumericSpec> baseline_numeric_spec{};
  std::function<void(std::string_view reason)> on_incompatible_definition{};
};

struct UsdMReferenceOptions final {
  std::chrono::milliseconds periodic_period{std::chrono::seconds{300}};
  std::chrono::milliseconds exchange_info_period{std::chrono::hours{24}};
  std::chrono::milliseconds initial_spacing{std::chrono::milliseconds{500}};
  std::string user_agent{"bmdg-usdm-reference/1.0.0"};
};

struct UsdMReferenceSources final {
  std::function<HttpsJsonResult(std::string_view target)> fetch;
  std::function<ExchangeInfoResult()> exchange_info_fetch;
};

struct UsdMReferenceObservation final {
  std::uint64_t requests{0U};
  std::uint64_t failures{0U};
  std::uint64_t published{0U};
  bool running{false};
};

// One low-frequency REST scheduler for all configured USD-M products. A
// single thread owns at most one in-flight request; observations are published
// through each product's EventPublication and therefore reuse its bounded
// queue and per-subscription session_sequence.
class UsdMReferenceService final {
public:
  UsdMReferenceService(std::vector<UsdMReferenceProduct> products,
                       std::vector<UsdMReferenceSeed> seeds,
                       runtime::RuntimeClock clock,
                       UsdMReferenceOptions options = {},
                       UsdMReferenceSources sources = {});
  ~UsdMReferenceService();

  UsdMReferenceService(const UsdMReferenceService &) = delete;
  UsdMReferenceService &operator=(const UsdMReferenceService &) = delete;
  UsdMReferenceService(UsdMReferenceService &&) = delete;
  UsdMReferenceService &operator=(UsdMReferenceService &&) = delete;

  [[nodiscard]] bool start();
  void stop() noexcept;

  [[nodiscard]] UsdMReferenceObservation observe() const noexcept;

  // Deterministic seam: performs one cycle of all five periodic sources plus
  // an exchangeInfo refresh without starting the scheduler thread.
  void poll_once_for_testing();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace binance_market_data::gateway::transport
