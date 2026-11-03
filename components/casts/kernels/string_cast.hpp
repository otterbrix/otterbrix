#pragma once

#include <components/casts/cast_function.hpp>
#include <components/casts/kernels/decimal_cast.hpp>

#include <core/date/date_parse.hpp>
#include <core/date/date_to_string.hpp>

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <charconv>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

namespace components::casts::kernels {

    namespace detail {

        [[nodiscard]] inline bool is_ascii_space(char character) noexcept {
            return character == ' ' || character == '\t' || character == '\n' || character == '\r' ||
                   character == '\v' || character == '\f';
        }

        [[nodiscard]] inline std::string_view trim(std::string_view text) noexcept {
            size_t begin = 0;
            size_t end = text.size();
            while (begin < end && is_ascii_space(text[begin])) {
                ++begin;
            }
            while (end > begin && is_ascii_space(text[end - 1])) {
                --end;
            }
            return text.substr(begin, end - begin);
        }

        [[nodiscard]] inline bool equals_ignore_case(std::string_view text, std::string_view lower) noexcept {
            if (text.size() != lower.size()) {
                return false;
            }
            for (size_t index = 0; index < text.size(); ++index) {
                char character = text[index];
                if (character >= 'A' && character <= 'Z') {
                    character = static_cast<char>(character - 'A' + 'a');
                }
                if (character != lower[index]) {
                    return false;
                }
            }
            return true;
        }

        template<typename T>
        inline constexpr bool is_128_bit_integer =
            std::is_same_v<T, types::int128_t> || std::is_same_v<T, types::uint128_t>;

        // PostgreSQL tells a malformed literal (22P02) from a well-formed one that does not fit (22003).
        enum class parse_outcome
        {
            parsed,
            malformed,
            out_of_range
        };

        [[nodiscard]] inline bool is_integer_syntax(std::string_view text) noexcept {
            if (!text.empty() && text.front() == '-') {
                text.remove_prefix(1);
            }
            if (text.empty()) {
                return false;
            }
            for (const char character : text) {
                if (character < '0' || character > '9') {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] inline core::error_t parse_refusal(parse_outcome outcome, std::pmr::memory_resource* resource) {
            return core::error_t{outcome == parse_outcome::out_of_range
                                     ? core::error_code_t::numeric_value_out_of_range
                                     : core::error_code_t::invalid_text_representation,
                                 std::pmr::string{"parse error", resource}};
        }

        template<typename Target>
        [[nodiscard]] inline parse_outcome parse_number(std::string_view text, Target& out) noexcept {
            size_t begin = 0;
            size_t end = text.size();
            while (begin < end && is_ascii_space(text[begin])) {
                ++begin;
            }
            while (end > begin && is_ascii_space(text[end - 1])) {
                --end;
            }
            const char* first = text.data() + begin;
            const char* last = text.data() + end;
            if (first != last && *first == '+') {
                ++first;
            }
            if (first == last) {
                return parse_outcome::malformed;
            }
            if constexpr (is_128_bit_integer<Target>) {
                const std::string_view digits{first, static_cast<size_t>(last - first)};
                if (absl::SimpleAtoi(digits, &out)) {
                    return parse_outcome::parsed;
                }
                return is_integer_syntax(digits) ? parse_outcome::out_of_range : parse_outcome::malformed;
            } else {
                std::from_chars_result parsed;
                if constexpr (std::is_floating_point_v<Target>) {
                    parsed = std::from_chars(first, last, out, std::chars_format::general);
                } else {
                    parsed = std::from_chars(first, last, out); // base 10
                }
                if (parsed.ptr != last) {
                    return parse_outcome::malformed;
                }
                if (parsed.ec == std::errc::result_out_of_range) {
                    return parse_outcome::out_of_range;
                }
                return parsed.ec == std::errc() ? parse_outcome::parsed : parse_outcome::malformed;
            }
        }

        enum class decimal_token
        {
            none,
            positive_infinity,
            negative_infinity,
            not_a_number
        };

        [[nodiscard]] inline decimal_token match_decimal_special(std::string_view trimmed) noexcept {
            if (equals_ignore_case(trimmed, "nan")) {
                return decimal_token::not_a_number;
            }
            bool negative = false;
            std::string_view rest = trimmed;
            if (!rest.empty() && (rest.front() == '+' || rest.front() == '-')) {
                negative = rest.front() == '-';
                rest.remove_prefix(1);
            }
            if (equals_ignore_case(rest, "inf") || equals_ignore_case(rest, "infinity")) {
                return negative ? decimal_token::negative_infinity : decimal_token::positive_infinity;
            }
            return decimal_token::none;
        }

        [[nodiscard]] inline parse_outcome
        parse_decimal(std::string_view text, uint8_t width, uint8_t scale, types::int128_t* out) noexcept {
            size_t index = 0;
            size_t size = text.size();
            bool negative = false;
            if (index < size && (text[index] == '+' || text[index] == '-')) {
                negative = text[index] == '-';
                ++index;
            }
            bool seen_digit = false;
            // Too many integer digits is decided after the whole text is read: a malformed text is 22P02 first.
            bool overflow = false;
            types::int128_t integer_value = 0;
            types::int128_t integer_capacity = types::POWERS_OF_TEN[width - scale];
            while (index < size && text[index] >= '0' && text[index] <= '9') {
                seen_digit = true;
                ++index;
                if (overflow) {
                    continue;
                }
                // Keep integer_value below 10^37 before the * 10 to prevent overflow
                if (integer_value >= types::POWERS_OF_TEN[37]) {
                    overflow = true;
                    continue;
                }
                integer_value = integer_value * 10 + (text[index - 1] - '0');
                if (integer_value >= integer_capacity) {
                    overflow = true; // more integer digits than the target can hold
                }
            }
            types::int128_t raw = overflow ? types::int128_t{0} : integer_value * types::POWERS_OF_TEN[scale];
            if (index < size && text[index] == '.') {
                ++index;
                for (uint8_t position = 0; position < scale; ++position) {
                    int digit = 0;
                    if (index < size && text[index] >= '0' && text[index] <= '9') {
                        digit = text[index] - '0';
                        seen_digit = true;
                        ++index;
                    }
                    raw += static_cast<types::int128_t>(digit) * types::POWERS_OF_TEN[scale - 1 - position];
                }
                int round_digit = 0;
                if (index < size && text[index] >= '0' && text[index] <= '9') {
                    round_digit = text[index] - '0';
                    seen_digit = true;
                    ++index;
                }
                while (index < size && text[index] >= '0' && text[index] <= '9') {
                    ++index;
                }
                if (round_digit >= 5) {
                    raw += 1;
                }
            }
            if (index != size || !seen_digit) {
                return parse_outcome::malformed;
            }
            if (overflow) {
                return parse_outcome::out_of_range;
            }
            if (negative) {
                raw = -raw;
            }
            // A round-up can push the magnitude to exactly 10^width (e.g. 9.99 -> 10.0),
            // which no longer fits.
            if (raw >= types::POWERS_OF_TEN[width] || raw <= -types::POWERS_OF_TEN[width]) {
                return parse_outcome::out_of_range;
            }
            *out = raw;
            return parse_outcome::parsed;
        }

        template<typename To>
        [[nodiscard]] std::optional<To> parse_datetime(std::string_view text) noexcept {
            if constexpr (std::is_same_v<To, core::date::date_t>) {
                return core::date::parse_date(text);
            } else if constexpr (std::is_same_v<To, core::date::time_t>) {
                return core::date::parse_time(text);
            } else if constexpr (std::is_same_v<To, core::date::timetz_t>) {
                return core::date::parse_timetz(text);
            } else if constexpr (std::is_same_v<To, core::date::timestamp_t>) {
                return core::date::parse_timestamp(text);
            } else if constexpr (std::is_same_v<To, core::date::timestamptz_t>) {
                return core::date::parse_timestamptz(text);
            } else {
                static_assert(std::is_same_v<To, core::date::interval_t>, "unsupported string -> date/time target");
                return core::date::parse_interval(text);
            }
        }

        // Trims surrounding whitespace and matches PostgreSQL's boolean spellings case-insensitively.
        [[nodiscard]] inline std::optional<bool> parse_bool(std::string_view text) noexcept {
            std::string_view token = trim(text);
            for (const std::string_view truthy : {"true", "t", "yes", "y", "1", "on"}) {
                if (equals_ignore_case(token, truthy)) {
                    return true;
                }
            }
            for (const std::string_view falsy : {"false", "f", "no", "n", "0", "off"}) {
                if (equals_ignore_case(token, falsy)) {
                    return false;
                }
            }
            return std::nullopt;
        }

    } // namespace detail

    template<typename Source>
    core::error_t number_to_string_cast(const vector::vector_t& source,
                                        vector::vector_t* result,
                                        const graph_execution_context&,
                                        uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const Source value = source.get_value<Source>(row);
            if constexpr (detail::is_128_bit_integer<Source>) {
                const std::string text = absl::StrCat(value);
                result->set_value(row, std::string_view{text});
            } else {
                char buffer[64];
                const auto [end, error_code] = std::to_chars(buffer, buffer + sizeof(buffer), value);
                // cannot fail: the buffer is large enough
                (void) error_code;
                result->set_value(row, std::string_view{buffer, static_cast<size_t>(end - buffer)});
            }
        }
        return core::error_t::no_error();
    }

    template<typename Target>
    core::error_t string_to_number_cast(const vector::vector_t& source,
                                        vector::vector_t* result,
                                        const graph_execution_context&,
                                        uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Target value;
            if (const auto outcome = detail::parse_number<Target>(source.get_value<std::string_view>(row), value);
                outcome != detail::parse_outcome::parsed) {
                return detail::parse_refusal(outcome, result->resource());
            }
            result->set_value(row, value);
        }
        return core::error_t::no_error();
    }

    template<typename Target>
    void string_to_number_try_cast(const vector::vector_t& source,
                                   vector::vector_t* result,
                                   const graph_execution_context&,
                                   uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Target value;
            if (detail::parse_number<Target>(source.get_value<std::string_view>(row), value) !=
                detail::parse_outcome::parsed) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, value);
        }
    }

    inline core::error_t decimal_to_string_cast(const vector::vector_t& source,
                                                vector::vector_t* result,
                                                const graph_execution_context&,
                                                uint64_t count) noexcept {
        types::physical_type physical = source.type().to_physical_type();
        const auto* extension = source.type().extension_as<types::decimal_logical_type_extension>();
        uint8_t width = extension->width();
        uint8_t scale = extension->scale();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(physical, raw)) {
                const char* text = raw >= types::decimal_special::positive_infinity(physical)   ? "Infinity"
                                   : raw == types::decimal_special::negative_infinity(physical) ? "-Infinity"
                                                                                                : "NaN";
                result->set_value(row, std::string_view{text});
                continue;
            }
            std::pmr::string text = types::format_decimal(result->resource(), raw, width, scale);
            result->set_value(row, std::string_view{text});
        }
        return core::error_t::no_error();
    }

    inline core::error_t string_to_decimal_cast(const vector::vector_t& source,
                                                vector::vector_t* result,
                                                const graph_execution_context&,
                                                uint64_t count) noexcept {
        types::physical_type physical = result->type().to_physical_type();
        const auto* extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t width = extension->width();
        uint8_t scale = extension->scale();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            std::string_view trimmed = detail::trim(source.get_value<std::string_view>(row));
            switch (detail::match_decimal_special(trimmed)) {
                case detail::decimal_token::positive_infinity:
                    detail::write_decimal_raw(result, row, types::decimal_special::positive_infinity(physical));
                    continue;
                case detail::decimal_token::negative_infinity:
                    detail::write_decimal_raw(result, row, types::decimal_special::negative_infinity(physical));
                    continue;
                case detail::decimal_token::not_a_number:
                    detail::write_decimal_raw(result, row, types::decimal_special::not_a_number(physical));
                    continue;
                case detail::decimal_token::none:
                    break;
            }
            types::int128_t raw;
            if (const auto outcome = detail::parse_decimal(trimmed, width, scale, &raw);
                outcome != detail::parse_outcome::parsed) {
                return detail::parse_refusal(outcome, result->resource());
            }
            detail::write_decimal_raw(result, row, raw);
        }
        return core::error_t::no_error();
    }

    inline void string_to_decimal_try_cast(const vector::vector_t& source,
                                           vector::vector_t* result,
                                           const graph_execution_context&,
                                           uint64_t count) noexcept {
        types::physical_type physical = result->type().to_physical_type();
        const auto* extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t width = extension->width();
        uint8_t scale = extension->scale();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            std::string_view trimmed = detail::trim(source.get_value<std::string_view>(row));
            switch (detail::match_decimal_special(trimmed)) {
                case detail::decimal_token::positive_infinity:
                    detail::write_decimal_raw(result, row, types::decimal_special::positive_infinity(physical));
                    continue;
                case detail::decimal_token::negative_infinity:
                    detail::write_decimal_raw(result, row, types::decimal_special::negative_infinity(physical));
                    continue;
                case detail::decimal_token::not_a_number:
                    detail::write_decimal_raw(result, row, types::decimal_special::not_a_number(physical));
                    continue;
                case detail::decimal_token::none:
                    break;
            }
            types::int128_t raw;
            if (detail::parse_decimal(trimmed, width, scale, &raw) != detail::parse_outcome::parsed) {
                result->set_null(row, true);
                continue;
            }
            detail::write_decimal_raw(result, row, raw);
        }
    }

    template<typename From>
    core::error_t datetime_to_string_cast(const vector::vector_t& source,
                                          vector::vector_t* result,
                                          const graph_execution_context&,
                                          uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const std::string text = core::date::to_string(source.get_value<From>(row));
            result->set_value(row, std::string_view{text});
        }
        return core::error_t::no_error();
    }

    template<typename To>
    core::error_t string_to_datetime_cast(const vector::vector_t& source,
                                          vector::vector_t* result,
                                          const graph_execution_context&,
                                          uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const std::optional<To> parsed = detail::parse_datetime<To>(source.get_value<std::string_view>(row));
            if (!parsed.has_value()) {
                return core::error_t{core::error_code_t::invalid_datetime_format,
                                     std::pmr::string{"parse error", result->resource()}};
            }
            result->set_value(row, *parsed);
        }
        return core::error_t::no_error();
    }

    template<typename To>
    void string_to_datetime_try_cast(const vector::vector_t& source,
                                     vector::vector_t* result,
                                     const graph_execution_context&,
                                     uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const std::optional<To> parsed = detail::parse_datetime<To>(source.get_value<std::string_view>(row));
            if (!parsed.has_value()) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, *parsed);
        }
    }

    inline core::error_t bool_to_string_cast(const vector::vector_t& source,
                                             vector::vector_t* result,
                                             const graph_execution_context&,
                                             uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, std::string_view{source.get_value<bool>(row) ? "true" : "false"});
        }
        return core::error_t::no_error();
    }

    inline core::error_t string_to_bool_cast(const vector::vector_t& source,
                                             vector::vector_t* result,
                                             const graph_execution_context&,
                                             uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            std::optional<bool> parsed = detail::parse_bool(source.get_value<std::string_view>(row));
            if (!parsed.has_value()) {
                return core::error_t{core::error_code_t::invalid_text_representation,
                                     std::pmr::string{"parse error", result->resource()}};
            }
            result->set_value(row, *parsed);
        }
        return core::error_t::no_error();
    }

    inline void string_to_bool_try_cast(const vector::vector_t& source,
                                        vector::vector_t* result,
                                        const graph_execution_context&,
                                        uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            std::optional<bool> parsed = detail::parse_bool(source.get_value<std::string_view>(row));
            if (!parsed.has_value()) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, *parsed);
        }
    }

} // namespace components::casts::kernels
