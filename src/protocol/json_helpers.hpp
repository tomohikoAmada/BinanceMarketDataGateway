#pragma once

#include "protocol/spot_protocol.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace binance_market_data::gateway::protocol::detail {

using Json = nlohmann::json;

[[nodiscard]] inline ProtocolError
error(ProtocolErrorCode code, std::string field, std::string message) {
  return {code, std::move(field), std::move(message)};
}

[[nodiscard]] inline std::variant<Json, ProtocolError>
parse_json(std::string_view payload) {
  try {
    auto parsed = Json::parse(payload.begin(), payload.end(), nullptr, false);
    if (parsed.is_discarded()) {
      return error(ProtocolErrorCode::InvalidJson, "payload",
                   "payload is not valid JSON");
    }
    return parsed;
  } catch (const std::exception &) {
    return error(ProtocolErrorCode::InvalidJson, "payload",
                 "payload could not be decoded");
  }
}

[[nodiscard]] inline bool
has_required_keys(const Json &value,
                  std::initializer_list<std::string_view> required) {
  if (!value.is_object()) {
    return false;
  }
  return std::all_of(required.begin(), required.end(), [&value](auto key) {
    return value.contains(std::string{key});
  });
}

// Strict unsigned integer extraction for WebSocket wire messages.
[[nodiscard]] inline std::optional<std::uint64_t>
unsigned_field(const Json &object, std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end() || !iterator->is_number_unsigned()) {
    return std::nullopt;
  }
  return iterator->get<std::uint64_t>();
}

// Reference REST payloads may serialize zero or positive counts as signed
// JSON integers (for example, negative numbers rejected here while 0 is valid).
[[nodiscard]] inline std::optional<std::uint64_t>
non_negative_integer_field(const Json &object, std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end()) {
    return std::nullopt;
  }
  if (iterator->is_number_unsigned()) {
    return iterator->get<std::uint64_t>();
  }
  if (iterator->is_number_integer()) {
    const auto value = iterator->get<std::int64_t>();
    if (value >= 0) {
      return static_cast<std::uint64_t>(value);
    }
  }
  return std::nullopt;
}

[[nodiscard]] inline std::optional<std::string_view>
string_field(const Json &object, std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end() || !iterator->is_string()) {
    return std::nullopt;
  }
  return iterator->get_ref<const std::string &>();
}

[[nodiscard]] inline std::optional<bool> boolean_field(const Json &object,
                                                       std::string_view name) {
  const auto iterator = object.find(std::string{name});
  if (iterator == object.end() || !iterator->is_boolean()) {
    return std::nullopt;
  }
  return iterator->get<bool>();
}

[[nodiscard]] inline bool plain_decimal(std::string_view value,
                                        bool allow_zero) noexcept {
  if (value.empty() || value.front() == '+' || value.front() == '-') {
    return false;
  }
  const auto decimal = value.find('.');
  if (decimal != std::string_view::npos &&
      value.find('.', decimal + 1U) != std::string_view::npos) {
    return false;
  }
  const auto whole = value.substr(0U, decimal);
  const auto fraction = decimal == std::string_view::npos
                            ? std::string_view{}
                            : value.substr(decimal + 1U);
  const auto digits = [](std::string_view text) {
    return std::all_of(text.begin(), text.end(),
                       [](char digit) { return digit >= '0' && digit <= '9'; });
  };
  if (whole.empty() || !digits(whole) ||
      (decimal != std::string_view::npos && fraction.empty()) ||
      !digits(fraction)) {
    return false;
  }
  // The decimal point is syntax, not a significant digit; "0.0" is zero.
  const auto nonzero = [](char digit) { return digit >= '1' && digit <= '9'; };
  return allow_zero || std::any_of(value.begin(), value.end(), nonzero);
}

template <typename RepeatedLevels>
[[nodiscard]] std::optional<ProtocolError>
append_levels(const Json &levels, RepeatedLevels *output,
              std::string_view field_name) {
  if (!levels.is_array()) {
    return error(ProtocolErrorCode::InvalidField, std::string{field_name},
                 "depth side must be an array");
  }
  for (const auto &level : levels) {
    if (!level.is_array() || level.size() != 2U || !level.at(0).is_string() ||
        !level.at(1).is_string()) {
      return error(ProtocolErrorCode::InvalidField, std::string{field_name},
                   "each depth level must be exactly two strings");
    }
    auto *wire_level = output->Add();
    wire_level->set_price(level.at(0).get_ref<const std::string &>());
    wire_level->set_quantity(level.at(1).get_ref<const std::string &>());
  }
  return std::nullopt;
}

} // namespace binance_market_data::gateway::protocol::detail
