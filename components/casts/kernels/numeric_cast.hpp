#pragma once

#include <components/casts/cast_function.hpp>
#include <components/casts/kernels/numeric_convert.hpp>

#include <cmath>
#include <limits>

namespace components::casts::kernels {

    template<typename Source, typename Target>
    core::error_t numeric_cast(const vector::vector_t& source,
                               vector::vector_t* result,
                               const cast_context&,
                               uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
            } else {
                result->set_value(row, detail::numeric_convert<Target>(source.get_value<Source>(row)));
            }
        }
        return core::error_t::no_error();
    }

    namespace detail {

        template<typename Floating, typename Target>
        [[nodiscard]] inline Floating integer_lower_bound() noexcept {
            if constexpr (std::numeric_limits<Target>::is_signed) {
                return -std::ldexp(Floating(1), sizeof(Target) * 8 - 1);
            } else {
                return Floating(0);
            }
        }

        template<typename Floating, typename Target>
        [[nodiscard]] inline Floating integer_upper_bound_exclusive() noexcept {
            constexpr int bits = sizeof(Target) * 8;
            return std::ldexp(Floating(1), std::numeric_limits<Target>::is_signed ? bits - 1 : bits);
        }

        template<typename Floating, typename Target>
        [[nodiscard]] inline bool
        round_to_integer(Floating value, Floating lower, Floating upper_exclusive, Target* out) noexcept {
            if (!std::isfinite(value)) {
                return false;
            }
            const Floating rounded = std::nearbyint(value);
            if (rounded < lower || rounded >= upper_exclusive) {
                return false;
            }
            *out = static_cast<Target>(rounded);
            return true;
        }

    } // namespace detail

    template<typename Source, typename Target>
    core::error_t floating_to_integer_cast(const vector::vector_t& source,
                                           vector::vector_t* result,
                                           const cast_context&,
                                           uint64_t count) noexcept {
        const Source lower = detail::integer_lower_bound<Source, Target>();
        const Source upper = detail::integer_upper_bound_exclusive<Source, Target>();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Target value;
            if (!detail::round_to_integer<Source, Target>(source.get_value<Source>(row), lower, upper, &value)) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            result->set_value(row, value);
        }
        return core::error_t::no_error();
    }

    template<typename Source, typename Target>
    void floating_to_integer_try_cast(const vector::vector_t& source,
                                      vector::vector_t* result,
                                      const cast_context&,
                                      uint64_t count) noexcept {
        const Source lower = detail::integer_lower_bound<Source, Target>();
        const Source upper = detail::integer_upper_bound_exclusive<Source, Target>();
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            Target value;
            if (!detail::round_to_integer<Source, Target>(source.get_value<Source>(row), lower, upper, &value)) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, value);
        }
    }

    namespace detail {

        template<typename Source, typename Target>
        [[nodiscard]] inline bool integer_fits(Source value) noexcept {
            constexpr bool source_signed = std::numeric_limits<Source>::is_signed;
            constexpr bool target_signed = std::numeric_limits<Target>::is_signed;
            if constexpr (source_signed && target_signed) {
                return value >= std::numeric_limits<Target>::min() && value <= std::numeric_limits<Target>::max();
            } else if constexpr (!source_signed && !target_signed) {
                return numeric_convert<types::uint128_t>(value) <=
                       numeric_convert<types::uint128_t>(std::numeric_limits<Target>::max());
            } else if constexpr (source_signed) {
                return value >= 0 && numeric_convert<types::uint128_t>(value) <=
                                         numeric_convert<types::uint128_t>(std::numeric_limits<Target>::max());
            } else {
                return numeric_convert<types::uint128_t>(value) <=
                       numeric_convert<types::uint128_t>(std::numeric_limits<Target>::max());
            }
        }
    } // namespace detail

    template<typename Source, typename Target>
    [[nodiscard]] constexpr bool is_lossless_integer_conversion() noexcept {
        constexpr bool source_signed = std::numeric_limits<Source>::is_signed;
        constexpr bool target_signed = std::numeric_limits<Target>::is_signed;
        if constexpr (source_signed == target_signed) {
            return sizeof(Source) <= sizeof(Target);
        } else if constexpr (!source_signed && target_signed) {
            return sizeof(Source) < sizeof(Target);
        } else {
            return false;
        }
    }

    template<typename Source, typename Target>
    core::error_t integer_narrow_cast(const vector::vector_t& source,
                                      vector::vector_t* result,
                                      const cast_context&,
                                      uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const Source value = source.get_value<Source>(row);
            if (!detail::integer_fits<Source, Target>(value)) {
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"out of range", result->resource()}};
            }
            result->set_value(row, static_cast<Target>(value));
        }
        return core::error_t::no_error();
    }

    template<typename Source, typename Target>
    void integer_narrow_try_cast(const vector::vector_t& source,
                                 vector::vector_t* result,
                                 const cast_context&,
                                 uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const Source value = source.get_value<Source>(row);
            if (!detail::integer_fits<Source, Target>(value)) {
                result->set_null(row, true);
                continue;
            }
            result->set_value(row, static_cast<Target>(value));
        }
    }

} // namespace components::casts::kernels