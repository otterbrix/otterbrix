#pragma once

#include <components/casts/cast_function.hpp>
#include <components/casts/kernels/numeric_convert.hpp>
#include <components/types/operations_helper.hpp>

#include <cmath>
#include <limits>
#include <optional>
#include <string_view>

namespace components::casts::kernels {

    namespace detail {

        [[nodiscard]] inline types::int128_t decimal_raw(const vector::vector_t& vec, uint64_t row) noexcept {
            switch (vec.type().to_physical_type()) {
                case types::physical_type::INT16:
                    return static_cast<types::int128_t>(vec.get_value<int16_t>(row));
                case types::physical_type::INT32:
                    return static_cast<types::int128_t>(vec.get_value<int32_t>(row));
                case types::physical_type::INT64:
                    return static_cast<types::int128_t>(vec.get_value<int64_t>(row));
                default:
                    return vec.get_value<types::int128_t>(row);
            }
        }

        inline void write_decimal_raw(vector::vector_t* vec, uint64_t row, types::int128_t raw) noexcept {
            switch (vec->type().to_physical_type()) {
                case types::physical_type::INT16:
                    vec->set_value(row, static_cast<int16_t>(raw));
                    break;
                case types::physical_type::INT32:
                    vec->set_value(row, static_cast<int32_t>(raw));
                    break;
                case types::physical_type::INT64:
                    vec->set_value(row, static_cast<int64_t>(raw));
                    break;
                default:
                    vec->set_value(row, raw);
                    break;
            }
        }

        [[nodiscard]] inline types::int128_t map_decimal_special(types::physical_type source,
                                                                 types::physical_type destination,
                                                                 types::int128_t raw) noexcept {
            if (raw >= types::decimal_special::positive_infinity(source)) {
                return types::decimal_special::positive_infinity(destination);
            }
            if (raw == types::decimal_special::negative_infinity(source)) {
                return types::decimal_special::negative_infinity(destination);
            }
            return types::decimal_special::not_a_number(destination);
        }

        [[nodiscard]] inline bool rescale_decimal(types::int128_t raw,
                                                  uint8_t src_scale,
                                                  uint8_t dst_scale,
                                                  uint8_t dst_width,
                                                  types::int128_t* out) noexcept {
            if (dst_scale >= src_scale) {
                uint8_t delta = dst_scale - src_scale;
                // |raw| < 10^(dst_width - delta) <=> |raw| * 10^delta < 10^dst_width;
                // this also bounds the product below 10^38 < 2^127, so it fits int128.
                types::int128_t max_magnitude = types::POWERS_OF_TEN[dst_width - delta];
                if (raw >= max_magnitude || raw <= -max_magnitude) {
                    return false;
                }
                *out = raw * types::POWERS_OF_TEN[delta];
                return true;
            }
            uint8_t down = src_scale - dst_scale;
            types::int128_t power = types::POWERS_OF_TEN[down];
            types::int128_t rounding = (raw < 0 ? -power : power) / 2;
            types::int128_t scaled = (raw + rounding) / power; // half away from zero
            types::int128_t max_magnitude = types::POWERS_OF_TEN[dst_width];
            if (scaled >= max_magnitude || scaled <= -max_magnitude) {
                return false;
            }
            *out = scaled;
            return true;
        }

        template<typename Source>
        [[nodiscard]] inline bool integer_within_decimal_magnitude(Source value,
                                                                   types::int128_t magnitude_exclusive) noexcept {
            if constexpr (std::numeric_limits<Source>::is_signed) {
                types::int128_t widened = static_cast<types::int128_t>(value);
                return widened > -magnitude_exclusive && widened < magnitude_exclusive;
            } else {
                return detail::numeric_convert<types::uint128_t>(value) <
                       detail::numeric_convert<types::uint128_t>(magnitude_exclusive);
            }
        }

    } // namespace detail

    template<typename Target>
    core::error_t decimal_to_floating_cast(const vector::vector_t& source,
                                           vector::vector_t* result,
                                           const cast_context&,
                                           uint64_t count) noexcept {
        types::physical_type physical = source.type().to_physical_type();
        uint8_t scale = source.type().extension_as<types::decimal_logical_type_extension>()->scale();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            Target out;
            if (types::decimal_special::is_special(physical, raw)) {
                if (raw >= types::decimal_special::positive_infinity(physical)) {
                    out = std::numeric_limits<Target>::infinity();
                } else if (raw == types::decimal_special::negative_infinity(physical)) {
                    out = -std::numeric_limits<Target>::infinity();
                } else {
                    out = std::numeric_limits<Target>::quiet_NaN();
                }
            } else {
                out = static_cast<Target>(static_cast<double>(raw) / types::DOUBLE_POWERS_OF_TEN[scale]);
            }
            result->set_value(row, out);
        }
        return core::error_t::no_error();
    }

    template<typename Source>
    core::error_t floating_to_decimal_cast(const vector::vector_t& source,
                                           vector::vector_t* result,
                                           const cast_context&,
                                           uint64_t count) noexcept {
        types::physical_type physical = result->type().to_physical_type();
        const auto* extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t scale = extension->scale();
        uint8_t width = extension->width();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Source value = source.get_value<Source>(row);
            if (std::isnan(value)) {
                detail::write_decimal_raw(result, row, types::decimal_special::not_a_number(physical));
                continue;
            }
            if (std::isinf(value)) {
                detail::write_decimal_raw(result,
                                          row,
                                          std::signbit(value) ? types::decimal_special::negative_infinity(physical)
                                                              : types::decimal_special::positive_infinity(physical));
                continue;
            }
            double scaled = std::round(static_cast<double>(value) * types::DOUBLE_POWERS_OF_TEN[scale]);
            if (scaled >= types::DOUBLE_POWERS_OF_TEN[width] || scaled <= -types::DOUBLE_POWERS_OF_TEN[width]) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            detail::write_decimal_raw(result, row, static_cast<types::int128_t>(scaled));
        }
        return core::error_t::no_error();
    }

    template<typename Source>
    void floating_to_decimal_try_cast(const vector::vector_t& source,
                                      vector::vector_t* result,
                                      const cast_context&,
                                      uint64_t count) noexcept {
        types::physical_type physical = result->type().to_physical_type();
        const auto* extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t scale = extension->scale();
        uint8_t width = extension->width();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Source value = source.get_value<Source>(row);
            if (std::isnan(value)) {
                detail::write_decimal_raw(result, row, types::decimal_special::not_a_number(physical));
                continue;
            }
            if (std::isinf(value)) {
                detail::write_decimal_raw(result,
                                          row,
                                          std::signbit(value) ? types::decimal_special::negative_infinity(physical)
                                                              : types::decimal_special::positive_infinity(physical));
                continue;
            }
            double scaled = std::round(static_cast<double>(value) * types::DOUBLE_POWERS_OF_TEN[scale]);
            if (scaled >= types::DOUBLE_POWERS_OF_TEN[width] || scaled <= -types::DOUBLE_POWERS_OF_TEN[width]) {
                result->set_null(row, true);
                continue;
            }
            detail::write_decimal_raw(result, row, static_cast<types::int128_t>(scaled));
        }
    }

    template<typename Target>
    core::error_t decimal_to_integer_cast(const vector::vector_t& source,
                                          vector::vector_t* result,
                                          const cast_context&,
                                          uint64_t count) noexcept {
        types::physical_type physical = source.type().to_physical_type();
        uint8_t scale = source.type().extension_as<types::decimal_logical_type_extension>()->scale();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(physical, raw)) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            std::optional<Target> value = types::decimal_to_numeric<types::int128_t, Target>(raw, scale);
            if (!value.has_value()) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            result->set_value(row, *value);
        }
        return core::error_t::no_error();
    }

    template<typename Target>
    void decimal_to_integer_try_cast(const vector::vector_t& source,
                                     vector::vector_t* result,
                                     const cast_context&,
                                     uint64_t count) noexcept {
        types::physical_type physical = source.type().to_physical_type();
        uint8_t scale = source.type().extension_as<types::decimal_logical_type_extension>()->scale();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(physical, raw)) {
                result->set_null(row, true);
                continue;
            }
            std::optional<Target> value = types::decimal_to_numeric<types::int128_t, Target>(raw, scale);
            if (!value.has_value()) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, *value);
        }
    }

    // Scale-independent: a decimal is zero iff its raw value is, so nothing is divided out.
    inline core::error_t decimal_to_bool_cast(const vector::vector_t& source,
                                              vector::vector_t* result,
                                              const cast_context&,
                                              uint64_t count) noexcept {
        types::physical_type physical = source.type().to_physical_type();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(physical, raw)) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            result->set_value(row, raw != 0);
        }
        return core::error_t::no_error();
    }

    inline void decimal_to_bool_try_cast(const vector::vector_t& source,
                                         vector::vector_t* result,
                                         const cast_context&,
                                         uint64_t count) noexcept {
        types::physical_type physical = source.type().to_physical_type();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(physical, raw)) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, raw != 0);
        }
    }

    template<typename Source>
    core::error_t integer_to_decimal_cast(const vector::vector_t& source,
                                          vector::vector_t* result,
                                          const cast_context&,
                                          uint64_t count) noexcept {
        const auto* extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t scale = extension->scale();
        uint8_t width = extension->width();
        types::int128_t magnitude_exclusive = types::POWERS_OF_TEN[width - scale];
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Source value = source.get_value<Source>(row);
            if (!detail::integer_within_decimal_magnitude(value, magnitude_exclusive)) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            detail::write_decimal_raw(result,
                                      row,
                                      detail::numeric_convert<types::int128_t>(value) * types::POWERS_OF_TEN[scale]);
        }
        return core::error_t::no_error();
    }

    template<typename Source>
    void integer_to_decimal_try_cast(const vector::vector_t& source,
                                     vector::vector_t* result,
                                     const cast_context&,
                                     uint64_t count) noexcept {
        const auto* extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t scale = extension->scale();
        uint8_t width = extension->width();
        types::int128_t magnitude_exclusive = types::POWERS_OF_TEN[width - scale];
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Source value = source.get_value<Source>(row);
            if (!detail::integer_within_decimal_magnitude(value, magnitude_exclusive)) {
                result->set_null(row, true);
                continue;
            }
            detail::write_decimal_raw(result,
                                      row,
                                      detail::numeric_convert<types::int128_t>(value) * types::POWERS_OF_TEN[scale]);
        }
    }

    inline core::error_t decimal_to_decimal_cast(const vector::vector_t& source,
                                                 vector::vector_t* result,
                                                 const cast_context&,
                                                 uint64_t count) noexcept {
        types::physical_type source_physical = source.type().to_physical_type();
        types::physical_type result_physical = result->type().to_physical_type();
        uint8_t source_scale = source.type().extension_as<types::decimal_logical_type_extension>()->scale();
        const auto* result_extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t result_scale = result_extension->scale();
        uint8_t result_width = result_extension->width();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(source_physical, raw)) {
                detail::write_decimal_raw(result,
                                          row,
                                          detail::map_decimal_special(source_physical, result_physical, raw));
                continue;
            }
            types::int128_t out;
            if (!detail::rescale_decimal(raw, source_scale, result_scale, result_width, &out)) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            detail::write_decimal_raw(result, row, out);
        }
        return core::error_t::no_error();
    }

    inline void decimal_to_decimal_try_cast(const vector::vector_t& source,
                                            vector::vector_t* result,
                                            const cast_context&,
                                            uint64_t count) noexcept {
        types::physical_type source_physical = source.type().to_physical_type();
        types::physical_type result_physical = result->type().to_physical_type();
        uint8_t source_scale = source.type().extension_as<types::decimal_logical_type_extension>()->scale();
        const auto* result_extension = result->type().extension_as<types::decimal_logical_type_extension>();
        uint8_t result_scale = result_extension->scale();
        uint8_t result_width = result_extension->width();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            types::int128_t raw = detail::decimal_raw(source, row);
            if (types::decimal_special::is_special(source_physical, raw)) {
                detail::write_decimal_raw(result,
                                          row,
                                          detail::map_decimal_special(source_physical, result_physical, raw));
                continue;
            }
            types::int128_t out;
            if (!detail::rescale_decimal(raw, source_scale, result_scale, result_width, &out)) {
                result->set_null(row, true);
                continue;
            }
            detail::write_decimal_raw(result, row, out);
        }
    }

} // namespace components::casts::kernels
