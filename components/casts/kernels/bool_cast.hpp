#pragma once

#include <components/casts/cast_function.hpp>

#include <optional>
#include <string_view>

namespace components::casts::kernels {

    // BOOLEAN <-> numeric needs no dedicated kernel: bool is arithmetic, so
    // numeric_cast (a per-row static_cast) already yields 0/1 for bool->int and
    // (x != 0) for numeric->bool. Only the text forms need their own kernels below.

    // BOOLEAN -> STRING_LITERAL: "true"/"false" (the ::text form). Infallible, so only
    // the mandatory `cast` body is needed. (set_value copies the literal into the
    // vector's string buffer.)
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

    namespace detail {

        // Parses a boolean from text: the PostgreSQL spellings, case-insensitive and
        // whitespace-trimmed. nullopt when the text names no boolean.
        [[nodiscard]] inline std::optional<bool> parse_bool(std::string_view text) noexcept {
            const auto is_space = [](char character) noexcept {
                return character == ' ' || character == '\t' || character == '\n' || character == '\r' ||
                       character == '\v' || character == '\f';
            };
            size_t begin = 0;
            size_t end = text.size();
            while (begin < end && is_space(text[begin])) {
                ++begin;
            }
            while (end > begin && is_space(text[end - 1])) {
                --end;
            }
            const std::string_view token = text.substr(begin, end - begin);
            const auto equals_ignore_case = [](std::string_view value, std::string_view lower) noexcept {
                if (value.size() != lower.size()) {
                    return false;
                }
                for (size_t index = 0; index < value.size(); ++index) {
                    char character = value[index];
                    if (character >= 'A' && character <= 'Z') {
                        character = static_cast<char>(character - 'A' + 'a');
                    }
                    if (character != lower[index]) {
                        return false;
                    }
                }
                return true;
            };
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

    // STRING_LITERAL -> BOOLEAN. Fallible: text that names no boolean has no value.
    // This `cast` body errors on the first bad row; its `try_cast` companion NULLs it.
    inline core::error_t string_to_bool_cast(const vector::vector_t& source,
                                             vector::vector_t* result,
                                             const graph_execution_context&,
                                             uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const std::optional<bool> parsed = detail::parse_bool(source.get_value<std::string_view>(row));
            if (!parsed.has_value()) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"parse error", result->resource()}};
            }
            result->set_value(row, *parsed);
        }
        return core::error_t::no_error();
    }

    // TRY_CAST companion for STRING_LITERAL -> BOOLEAN: a bad row becomes NULL.
    inline void string_to_bool_try_cast(const vector::vector_t& source,
                                        vector::vector_t* result,
                                        const graph_execution_context&,
                                        uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const std::optional<bool> parsed = detail::parse_bool(source.get_value<std::string_view>(row));
            if (!parsed.has_value()) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, *parsed);
        }
    }

} // namespace components::casts::kernels
