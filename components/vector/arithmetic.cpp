#include "arithmetic.hpp"

#include <cmath>
#include <core/date/date_types.hpp>
#include <functional>
#include <limits>

namespace components::vector {

    namespace {

        // Detect problematic int128 <-> float/double combinations
        template<typename L, typename R>
        constexpr bool is_int128_float_mix_v =
            ((std::is_same_v<std::decay_t<L>, types::int128_t> ||
              std::is_same_v<std::decay_t<L>, types::uint128_t>) &&std::is_floating_point_v<R>) ||
            (std::is_floating_point_v<L> &&
             (std::is_same_v<std::decay_t<R>, types::int128_t> || std::is_same_v<std::decay_t<R>, types::uint128_t>) );

        // Binary vector-vector
        template<template<typename...> class Op>
        struct binary_op_wrapper {
            template<typename...>
            struct callback {
                template<typename L, typename R>
                void operator()(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<L>&& types::is_numeric_type_v<R>) {
                    auto* lhs = left.data<L>();
                    auto* rhs = right.data<R>();
                    if constexpr (is_int128_float_mix_v<L, R> || std::is_floating_point_v<L> ||
                                  std::is_floating_point_v<R>) {
                        // All floating-point arithmetic uses double for precision
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(static_cast<double>(lhs[i]), static_cast<double>(rhs[i]));
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<L>(), std::declval<R>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(lhs[i], rhs[i]);
                        }
                    }
                }
                template<typename L, typename R>
                void operator()(const vector_t&, const vector_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>) ) {
                    throw std::logic_error("Arithmetic not supported for non-numeric types");
                }
            };
        };

        // Binary vector-vector with zero-check (for divide/mod): sets NULL on division by zero
        template<template<typename...> class Op>
        struct binary_div_wrapper {
            template<typename...>
            struct callback {
                template<typename L, typename R>
                void operator()(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<L>&& types::is_numeric_type_v<R>) {
                    auto* lhs = left.data<L>();
                    auto* rhs = right.data<R>();
                    if constexpr (is_int128_float_mix_v<L, R> || std::is_floating_point_v<L> ||
                                  std::is_floating_point_v<R>) {
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(rhs[i])) {
                                output.validity().set_invalid(i);
                                out[i] = std::numeric_limits<double>::quiet_NaN();
                            } else {
                                out[i] = op(static_cast<double>(lhs[i]), static_cast<double>(rhs[i]));
                            }
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<L>(), std::declval<R>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(rhs[i])) {
                                output.validity().set_invalid(i);
                            } else {
                                out[i] = op(lhs[i], rhs[i]);
                            }
                        }
                    }
                }
                template<typename L, typename R>
                void operator()(const vector_t&, const vector_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>) ) {
                    throw std::logic_error("Arithmetic not supported for non-numeric types");
                }
            };
        };

        // Vector-scalar
        template<template<typename...> class Op>
        struct vec_scalar_op_wrapper {
            template<typename...>
            struct callback {
                template<typename VecT, typename ScalarT>
                void operator()(const vector_t& vec,
                                const types::logical_value_t& scalar_val,
                                vector_t& output,
                                uint64_t count) const
                    requires(types::is_numeric_type_v<VecT>&& types::is_numeric_type_v<ScalarT>) {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<VecT, ScalarT> || std::is_floating_point_v<VecT> ||
                                  std::is_floating_point_v<ScalarT>) {
                        auto dcval = static_cast<double>(cval);
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(static_cast<double>(src[i]), dcval);
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<VecT>(), std::declval<ScalarT>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(src[i], cval);
                        }
                    }
                }
                template<typename VecT, typename ScalarT>
                void operator()(const vector_t&, const types::logical_value_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>) ) {
                    throw std::logic_error("Arithmetic not supported for non-numeric types");
                }
            };
        };

        // Vector-scalar with zero-check: sets NULL for all rows when scalar divisor is zero
        template<template<typename...> class Op>
        struct vec_scalar_div_wrapper {
            template<typename...>
            struct callback {
                template<typename VecT, typename ScalarT>
                void operator()(const vector_t& vec,
                                const types::logical_value_t& scalar_val,
                                vector_t& output,
                                uint64_t count) const
                    requires(types::is_numeric_type_v<VecT>&& types::is_numeric_type_v<ScalarT>) {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    if (detail::is_zero(cval)) {
                        if constexpr (is_int128_float_mix_v<VecT, ScalarT> || std::is_floating_point_v<VecT> ||
                                      std::is_floating_point_v<ScalarT>) {
                            auto* out = output.data<double>();
                            for (uint64_t i = 0; i < count; i++) {
                                output.validity().set_invalid(i);
                                out[i] = std::numeric_limits<double>::quiet_NaN();
                            }
                        } else {
                            for (uint64_t i = 0; i < count; i++) {
                                output.validity().set_invalid(i);
                            }
                        }
                        return;
                    }
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<VecT, ScalarT> || std::is_floating_point_v<VecT> ||
                                  std::is_floating_point_v<ScalarT>) {
                        auto dcval = static_cast<double>(cval);
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(static_cast<double>(src[i]), dcval);
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<VecT>(), std::declval<ScalarT>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(src[i], cval);
                        }
                    }
                }
                template<typename VecT, typename ScalarT>
                void operator()(const vector_t&, const types::logical_value_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>) ) {
                    throw std::logic_error("Arithmetic not supported for non-numeric types");
                }
            };
        };

        // Scalar-vector
        template<template<typename...> class Op>
        struct scalar_vec_op_wrapper {
            template<typename...>
            struct callback {
                template<typename ScalarT, typename VecT>
                void operator()(const types::logical_value_t& scalar_val,
                                const vector_t& vec,
                                vector_t& output,
                                uint64_t count) const
                    requires(types::is_numeric_type_v<ScalarT>&& types::is_numeric_type_v<VecT>) {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<ScalarT, VecT> || std::is_floating_point_v<ScalarT> ||
                                  std::is_floating_point_v<VecT>) {
                        auto dcval = static_cast<double>(cval);
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(dcval, static_cast<double>(src[i]));
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<ScalarT>(), std::declval<VecT>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(cval, src[i]);
                        }
                    }
                }
                template<typename ScalarT, typename VecT>
                void operator()(const types::logical_value_t&, const vector_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>) ) {
                    throw std::logic_error("Arithmetic not supported for non-numeric types");
                }
            };
        };

        // Scalar-vector with zero-check: per-element zero check on vector divisor
        template<template<typename...> class Op>
        struct scalar_vec_div_wrapper {
            template<typename...>
            struct callback {
                template<typename ScalarT, typename VecT>
                void operator()(const types::logical_value_t& scalar_val,
                                const vector_t& vec,
                                vector_t& output,
                                uint64_t count) const
                    requires(types::is_numeric_type_v<ScalarT>&& types::is_numeric_type_v<VecT>) {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<ScalarT, VecT> || std::is_floating_point_v<ScalarT> ||
                                  std::is_floating_point_v<VecT>) {
                        auto dcval = static_cast<double>(cval);
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(src[i])) {
                                output.validity().set_invalid(i);
                                out[i] = std::numeric_limits<double>::quiet_NaN();
                            } else {
                                out[i] = op(dcval, static_cast<double>(src[i]));
                            }
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<ScalarT>(), std::declval<VecT>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(src[i])) {
                                output.validity().set_invalid(i);
                            } else {
                                out[i] = op(cval, src[i]);
                            }
                        }
                    }
                }
                template<typename ScalarT, typename VecT>
                void operator()(const types::logical_value_t&, const vector_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>) ) {
                    throw std::logic_error("Arithmetic not supported for non-numeric types");
                }
            };
        };

        // Unary negation
        struct unary_neg_wrapper {
            template<typename...>
            struct callback {
                template<typename T>
                void operator()(const vector_t& vec, vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<T>) {
                    auto* src = vec.data<T>();
                    auto* out = output.data<T>();
                    for (uint64_t i = 0; i < count; i++) {
                        out[i] = -src[i];
                    }
                }
                template<typename T>
                void operator()(const vector_t&, vector_t&, uint64_t) const requires(!types::is_numeric_type_v<T>) {
                    throw std::logic_error("Negation not supported for non-numeric types");
                }
            };
        };

        template<template<typename...> class Op>
        void dispatch_binary(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<binary_op_wrapper<Op>::template callback>(
                left.type().to_physical_type(),
                right.type().to_physical_type(),
                left,
                right,
                output,
                count);
        }

        template<template<typename...> class Op>
        void dispatch_binary_div(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<binary_div_wrapper<Op>::template callback>(
                left.type().to_physical_type(),
                right.type().to_physical_type(),
                left,
                right,
                output,
                count);
        }

        template<template<typename...> class Op>
        void dispatch_vec_scalar(const vector_t& vec,
                                 const types::logical_value_t& scalar,
                                 vector_t& output,
                                 uint64_t count) {
            types::double_simple_physical_type_switch<vec_scalar_op_wrapper<Op>::template callback>(
                vec.type().to_physical_type(),
                scalar.type().to_physical_type(),
                vec,
                scalar,
                output,
                count);
        }

        template<template<typename...> class Op>
        void dispatch_vec_scalar_div(const vector_t& vec,
                                     const types::logical_value_t& scalar,
                                     vector_t& output,
                                     uint64_t count) {
            types::double_simple_physical_type_switch<vec_scalar_div_wrapper<Op>::template callback>(
                vec.type().to_physical_type(),
                scalar.type().to_physical_type(),
                vec,
                scalar,
                output,
                count);
        }

        template<template<typename...> class Op>
        void dispatch_scalar_vec(const types::logical_value_t& scalar,
                                 const vector_t& vec,
                                 vector_t& output,
                                 uint64_t count) {
            types::double_simple_physical_type_switch<scalar_vec_op_wrapper<Op>::template callback>(
                scalar.type().to_physical_type(),
                vec.type().to_physical_type(),
                scalar,
                vec,
                output,
                count);
        }

        template<template<typename...> class Op>
        void dispatch_scalar_vec_div(const types::logical_value_t& scalar,
                                     const vector_t& vec,
                                     vector_t& output,
                                     uint64_t count) {
            types::double_simple_physical_type_switch<scalar_vec_div_wrapper<Op>::template callback>(
                scalar.type().to_physical_type(),
                vec.type().to_physical_type(),
                scalar,
                vec,
                output,
                count);
        }

        // ---- Temporal arithmetic ----

        static constexpr core::date::microseconds ONE_DAY_US =
            std::chrono::duration_cast<core::date::microseconds>(core::date::days{1});

        inline core::date::days apply_interval_to_date(core::date::days date,
                                                       core::date::days ivl_days,
                                                       core::date::months ivl_months,
                                                       int sign) noexcept {
            auto sd = core::date::pg_epoch + std::chrono::days{date.count()};
            if (ivl_months.count()) {
                sd = core::date::apply_months(sd, (sign * ivl_months).count());
            }
            sd += std::chrono::days{(sign * ivl_days).count()};
            return core::date::days{static_cast<core::date::days::rep>((sd - core::date::pg_epoch).count())};
        }

        inline core::date::microseconds apply_interval_to_ts(core::date::microseconds ts,
                                                             core::date::microseconds ivl_time,
                                                             core::date::days ivl_days,
                                                             core::date::months ivl_months,
                                                             int sign) noexcept {
            auto [d, tod] = core::date::split_timestamp(ts);
            auto sd = core::date::pg_epoch + std::chrono::days{d.count()};
            if (ivl_months.count()) {
                sd = core::date::apply_months(sd, (sign * ivl_months).count());
            }
            sd += std::chrono::days{(sign * ivl_days).count()};
            return core::date::from_sys_days_us(sd, tod + sign * ivl_time);
        }

        struct ivl_ro {
            const core::date::microseconds* time;
            const core::date::days* days;
            const core::date::months* months;
            static ivl_ro from(const vector_t& v) noexcept {
                return {v.entries()[0]->data<core::date::microseconds>(),
                        v.entries()[1]->data<core::date::days>(),
                        v.entries()[2]->data<core::date::months>()};
            }
        };

        struct ivl_rw {
            core::date::microseconds* time;
            core::date::days* days;
            core::date::months* months;
            static ivl_rw from(vector_t& v) noexcept {
                return {v.entries()[0]->data<core::date::microseconds>(),
                        v.entries()[1]->data<core::date::days>(),
                        v.entries()[2]->data<core::date::months>()};
            }
        };

        inline double scalar_as_double(const types::logical_value_t& v) noexcept {
            using lt = types::logical_type;
            switch (v.type().type()) {
                case lt::TINYINT:
                    return static_cast<double>(v.value<int8_t>());
                case lt::UTINYINT:
                    return static_cast<double>(v.value<uint8_t>());
                case lt::SMALLINT:
                    return static_cast<double>(v.value<int16_t>());
                case lt::USMALLINT:
                    return static_cast<double>(v.value<uint16_t>());
                case lt::INTEGER:
                    return static_cast<double>(v.value<int32_t>());
                case lt::UINTEGER:
                    return static_cast<double>(v.value<uint32_t>());
                case lt::BIGINT:
                    return static_cast<double>(v.value<int64_t>());
                case lt::UBIGINT:
                    return static_cast<double>(v.value<uint64_t>());
                case lt::FLOAT:
                    return static_cast<double>(v.value<float>());
                case lt::DOUBLE:
                    return v.value<double>();
                default:
                    return 0.0;
            }
        }

        inline core::date::interval_t scale_interval(core::date::interval_t iv, double f) noexcept {
            return {core::date::microseconds{std::llround(static_cast<double>(iv.time.count()) * f)},
                    core::date::days{static_cast<int32_t>(std::llround(static_cast<double>(iv.day.count()) * f))},
                    core::date::months{static_cast<int32_t>(std::llround(static_cast<double>(iv.month.count()) * f))}};
        }

        template<typename T>
        void scale_ivl_vec(const ivl_ro& ivl, ivl_rw& out, const T* factors, bool divide, uint64_t count) noexcept {
            for (uint64_t i = 0; i < count; i++) {
                const double f = divide ? (1.0 / static_cast<double>(factors[i])) : static_cast<double>(factors[i]);
                out.time[i] = core::date::microseconds{std::llround(static_cast<double>(ivl.time[i].count()) * f)};
                out.days[i] =
                    core::date::days{static_cast<int32_t>(std::llround(static_cast<double>(ivl.days[i].count()) * f))};
                out.months[i] = core::date::months{
                    static_cast<int32_t>(std::llround(static_cast<double>(ivl.months[i].count()) * f))};
            }
        }

        void dispatch_ivl_vec_scale(const ivl_ro& ivl,
                                    ivl_rw& out,
                                    const vector_t& factors_vec,
                                    bool divide,
                                    uint64_t count) noexcept {
            using lt = types::logical_type;
            switch (factors_vec.type().type()) {
                case lt::TINYINT:
                    scale_ivl_vec(ivl, out, factors_vec.data<int8_t>(), divide, count);
                    break;
                case lt::UTINYINT:
                    scale_ivl_vec(ivl, out, factors_vec.data<uint8_t>(), divide, count);
                    break;
                case lt::SMALLINT:
                    scale_ivl_vec(ivl, out, factors_vec.data<int16_t>(), divide, count);
                    break;
                case lt::USMALLINT:
                    scale_ivl_vec(ivl, out, factors_vec.data<uint16_t>(), divide, count);
                    break;
                case lt::INTEGER:
                    scale_ivl_vec(ivl, out, factors_vec.data<int32_t>(), divide, count);
                    break;
                case lt::UINTEGER:
                    scale_ivl_vec(ivl, out, factors_vec.data<uint32_t>(), divide, count);
                    break;
                case lt::BIGINT:
                    scale_ivl_vec(ivl, out, factors_vec.data<int64_t>(), divide, count);
                    break;
                case lt::UBIGINT:
                    scale_ivl_vec(ivl, out, factors_vec.data<uint64_t>(), divide, count);
                    break;
                case lt::FLOAT:
                    scale_ivl_vec(ivl, out, factors_vec.data<float>(), divide, count);
                    break;
                case lt::DOUBLE:
                    scale_ivl_vec(ivl, out, factors_vec.data<double>(), divide, count);
                    break;
                default:
                    break;
            }
        }

        vector_t compute_temporal_binary(std::pmr::memory_resource* resource,
                                         arithmetic_op op,
                                         const vector_t& left,
                                         const vector_t& right,
                                         uint64_t count) {
            using lt = types::logical_type;
            const auto lhs = left.type().type();
            const auto rhs = right.type().type();
            vector_t output(resource, types::complex_logical_type(types::arithmetic_result_type(lhs, rhs, op)), count);
            const bool is_add = (op == arithmetic_op::add);
            const int sign = is_add ? 1 : -1;

            // DATE ± INTERVAL  →  DATE
            if (lhs == lt::DATE && rhs == lt::INTERVAL) {
                const auto* dates = left.data<core::date::days>();
                const auto ivl = ivl_ro::from(right);
                auto* outs = output.data<core::date::days>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_date(dates[i], ivl.days[i], ivl.months[i], sign);
                }
                return output;
            }
            // INTERVAL + DATE  →  DATE  (commutative)
            if (is_add && lhs == lt::INTERVAL && rhs == lt::DATE) {
                const auto ivl = ivl_ro::from(left);
                const auto* dates = right.data<core::date::days>();
                auto* outs = output.data<core::date::days>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_date(dates[i], ivl.days[i], ivl.months[i], 1);
                }
                return output;
            }
            // TIMESTAMP/TZ ± INTERVAL  →  TIMESTAMP/TZ
            if ((lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ) && rhs == lt::INTERVAL) {
                const auto* ts = left.data<core::date::microseconds>();
                const auto ivl = ivl_ro::from(right);
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_ts(ts[i], ivl.time[i], ivl.days[i], ivl.months[i], sign);
                }
                return output;
            }
            // INTERVAL + TIMESTAMP/TZ  →  TIMESTAMP/TZ  (commutative)
            if (is_add && lhs == lt::INTERVAL && (rhs == lt::TIMESTAMP || rhs == lt::TIMESTAMP_TZ)) {
                const auto ivl = ivl_ro::from(left);
                const auto* ts = right.data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_ts(ts[i], ivl.time[i], ivl.days[i], ivl.months[i], 1);
                }
                return output;
            }
            // TIME ± INTERVAL  →  TIME
            if (lhs == lt::TIME && rhs == lt::INTERVAL) {
                const auto* times = left.data<core::date::microseconds>();
                const auto* ivl_us = right.entries()[0]->data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (times[i] + sign * ivl_us[i]) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    outs[i] = result;
                }
                return output;
            }
            // INTERVAL + TIME  →  TIME  (commutative)
            if (is_add && lhs == lt::INTERVAL && rhs == lt::TIME) {
                const auto* ivl_us = left.entries()[0]->data<core::date::microseconds>();
                const auto* times = right.data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (times[i] + ivl_us[i]) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    outs[i] = result;
                }
                return output;
            }
            // TIME_TZ ± INTERVAL  →  TIME_TZ
            if (lhs == lt::TIME_TZ && rhs == lt::INTERVAL) {
                const auto* tz_us = left.entries()[0]->data<core::date::microseconds>();
                const auto* tz_zone = left.entries()[1]->data<core::date::timezone_offset_t>();
                const auto* ivl_us = right.entries()[0]->data<core::date::microseconds>();
                auto* out_us = output.entries()[0]->data<core::date::microseconds>();
                auto* out_zone = output.entries()[1]->data<core::date::timezone_offset_t>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (tz_us[i] + sign * ivl_us[i]) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    out_us[i] = result;
                    out_zone[i] = tz_zone[i];
                }
                return output;
            }
            // INTERVAL + TIME_TZ  →  TIME_TZ  (commutative)
            if (is_add && lhs == lt::INTERVAL && rhs == lt::TIME_TZ) {
                const auto* ivl_us = left.entries()[0]->data<core::date::microseconds>();
                const auto* tz_us = right.entries()[0]->data<core::date::microseconds>();
                const auto* tz_zone = right.entries()[1]->data<core::date::timezone_offset_t>();
                auto* out_us = output.entries()[0]->data<core::date::microseconds>();
                auto* out_zone = output.entries()[1]->data<core::date::timezone_offset_t>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (tz_us[i] + ivl_us[i]) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    out_us[i] = result;
                    out_zone[i] = tz_zone[i];
                }
                return output;
            }
            // INTERVAL ± INTERVAL  →  INTERVAL
            if (lhs == lt::INTERVAL && rhs == lt::INTERVAL) {
                const auto livl = ivl_ro::from(left);
                const auto rivl = ivl_ro::from(right);
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = livl.time[i] + sign * rivl.time[i];
                    out.days[i] = livl.days[i] + sign * rivl.days[i];
                    out.months[i] = livl.months[i] + sign * rivl.months[i];
                }
                return output;
            }
            // INTERVAL * numeric_vec → INTERVAL
            if (op == arithmetic_op::multiply && lhs == lt::INTERVAL && types::is_numeric(rhs)) {
                const auto livl = ivl_ro::from(left);
                auto out = ivl_rw::from(output);
                dispatch_ivl_vec_scale(livl, out, right, false, count);
                return output;
            }
            // numeric_vec * INTERVAL → INTERVAL (commutative)
            if (op == arithmetic_op::multiply && types::is_numeric(lhs) && rhs == lt::INTERVAL) {
                return compute_temporal_binary(resource, op, right, left, count);
            }
            // INTERVAL / numeric_vec → INTERVAL
            if (op == arithmetic_op::divide && lhs == lt::INTERVAL && types::is_numeric(rhs)) {
                const auto livl = ivl_ro::from(left);
                auto out = ivl_rw::from(output);
                dispatch_ivl_vec_scale(livl, out, right, true, count);
                return output;
            }
            // DATE - DATE  →  INTERVAL (days component)
            if (!is_add && lhs == lt::DATE && rhs == lt::DATE) {
                const auto* ldates = left.data<core::date::days>();
                const auto* rdates = right.data<core::date::days>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = core::date::microseconds{0};
                    out.days[i] = ldates[i] - rdates[i];
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            // TIMESTAMP/TZ - TIMESTAMP/TZ  →  INTERVAL (µs component)
            if (!is_add && (lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ) &&
                (rhs == lt::TIMESTAMP || rhs == lt::TIMESTAMP_TZ)) {
                const auto* lts = left.data<core::date::microseconds>();
                const auto* rts = right.data<core::date::microseconds>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = lts[i] - rts[i];
                    out.days[i] = core::date::days{0};
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            // TIME - TIME  →  INTERVAL (µs component)
            if (!is_add && lhs == lt::TIME && rhs == lt::TIME) {
                const auto* lt_us = left.data<core::date::microseconds>();
                const auto* rt_us = right.data<core::date::microseconds>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = lt_us[i] - rt_us[i];
                    out.days[i] = core::date::days{0};
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            return output;
        }

        vector_t compute_temporal_vec_scalar(std::pmr::memory_resource* resource,
                                             arithmetic_op op,
                                             const vector_t& vec,
                                             const types::logical_value_t& scalar,
                                             uint64_t count) {
            using lt = types::logical_type;
            const auto lhs = vec.type().type();
            const auto rhs = scalar.type().type();
            vector_t output(resource, types::complex_logical_type(types::arithmetic_result_type(lhs, rhs, op)), count);
            const bool is_add = (op == arithmetic_op::add);
            const int sign = is_add ? 1 : -1;

            // DATE ± INTERVAL_scalar  →  DATE
            if (lhs == lt::DATE && rhs == lt::INTERVAL) {
                const auto ivl = scalar.value<core::date::interval_t>();
                const auto id = ivl.day;
                const auto im = ivl.month;
                const auto* dates = vec.data<core::date::days>();
                auto* outs = output.data<core::date::days>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_date(dates[i], id, im, sign);
                }
                return output;
            }
            // TIMESTAMP/TZ ± INTERVAL_scalar  →  TIMESTAMP/TZ
            if ((lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ) && rhs == lt::INTERVAL) {
                const auto ivl = scalar.value<core::date::interval_t>();
                const auto it = ivl.time;
                const auto id = ivl.day;
                const auto im = ivl.month;
                const auto* ts = vec.data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_ts(ts[i], it, id, im, sign);
                }
                return output;
            }
            // TIME ± INTERVAL_scalar  →  TIME
            if (lhs == lt::TIME && rhs == lt::INTERVAL) {
                const auto it = scalar.value<core::date::interval_t>().time;
                const auto* times = vec.data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (times[i] + sign * it) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    outs[i] = result;
                }
                return output;
            }
            // TIME_TZ ± INTERVAL_scalar  →  TIME_TZ
            if (lhs == lt::TIME_TZ && rhs == lt::INTERVAL) {
                const auto it = scalar.value<core::date::interval_t>().time;
                const auto* tz_us = vec.entries()[0]->data<core::date::microseconds>();
                const auto* tz_zone = vec.entries()[1]->data<core::date::timezone_offset_t>();
                auto* out_us = output.entries()[0]->data<core::date::microseconds>();
                auto* out_zone = output.entries()[1]->data<core::date::timezone_offset_t>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (tz_us[i] + sign * it) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    out_us[i] = result;
                    out_zone[i] = tz_zone[i];
                }
                return output;
            }
            // INTERVAL ± INTERVAL_scalar  →  INTERVAL
            if (lhs == lt::INTERVAL && rhs == lt::INTERVAL) {
                const auto ivl = scalar.value<core::date::interval_t>();
                const auto livl = ivl_ro::from(vec);
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = livl.time[i] + sign * ivl.time;
                    out.days[i] = livl.days[i] + sign * ivl.day;
                    out.months[i] = livl.months[i] + sign * ivl.month;
                }
                return output;
            }
            // INTERVAL_vec * numeric_scalar → INTERVAL
            if (op == arithmetic_op::multiply && lhs == lt::INTERVAL && types::is_numeric(rhs)) {
                const double f = scalar_as_double(scalar);
                const auto livl = ivl_ro::from(vec);
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = core::date::microseconds{std::llround(static_cast<double>(livl.time[i].count()) * f)};
                    out.days[i] = core::date::days{
                        static_cast<int32_t>(std::llround(static_cast<double>(livl.days[i].count()) * f))};
                    out.months[i] = core::date::months{
                        static_cast<int32_t>(std::llround(static_cast<double>(livl.months[i].count()) * f))};
                }
                return output;
            }
            // INTERVAL_vec / numeric_scalar → INTERVAL
            if (op == arithmetic_op::divide && lhs == lt::INTERVAL && types::is_numeric(rhs)) {
                const double f = scalar_as_double(scalar);
                const auto livl = ivl_ro::from(vec);
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = core::date::microseconds{std::llround(static_cast<double>(livl.time[i].count()) / f)};
                    out.days[i] = core::date::days{
                        static_cast<int32_t>(std::llround(static_cast<double>(livl.days[i].count()) / f))};
                    out.months[i] = core::date::months{
                        static_cast<int32_t>(std::llround(static_cast<double>(livl.months[i].count()) / f))};
                }
                return output;
            }
            // numeric_vec * INTERVAL_scalar → INTERVAL
            if (op == arithmetic_op::multiply && types::is_numeric(lhs) && rhs == lt::INTERVAL) {
                const auto ivl = scalar.value<core::date::interval_t>();
                auto out = ivl_rw::from(output);
                auto do_scale = [&](auto* factors) {
                    for (uint64_t i = 0; i < count; i++) {
                        const double f = static_cast<double>(factors[i]);
                        out.time[i] = core::date::microseconds{std::llround(static_cast<double>(ivl.time.count()) * f)};
                        out.days[i] = core::date::days{
                            static_cast<int32_t>(std::llround(static_cast<double>(ivl.day.count()) * f))};
                        out.months[i] = core::date::months{
                            static_cast<int32_t>(std::llround(static_cast<double>(ivl.month.count()) * f))};
                    }
                };
                switch (lhs) {
                    case lt::TINYINT:
                        do_scale(vec.data<int8_t>());
                        break;
                    case lt::UTINYINT:
                        do_scale(vec.data<uint8_t>());
                        break;
                    case lt::SMALLINT:
                        do_scale(vec.data<int16_t>());
                        break;
                    case lt::USMALLINT:
                        do_scale(vec.data<uint16_t>());
                        break;
                    case lt::INTEGER:
                        do_scale(vec.data<int32_t>());
                        break;
                    case lt::UINTEGER:
                        do_scale(vec.data<uint32_t>());
                        break;
                    case lt::BIGINT:
                        do_scale(vec.data<int64_t>());
                        break;
                    case lt::UBIGINT:
                        do_scale(vec.data<uint64_t>());
                        break;
                    case lt::FLOAT:
                        do_scale(vec.data<float>());
                        break;
                    case lt::DOUBLE:
                        do_scale(vec.data<double>());
                        break;
                    default:
                        break;
                }
                return output;
            }
            // DATE - DATE_scalar  →  INTERVAL
            if (!is_add && lhs == lt::DATE && rhs == lt::DATE) {
                const auto scalar_days = scalar.value<core::date::date_t>().value;
                const auto* dates = vec.data<core::date::days>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = core::date::microseconds{0};
                    out.days[i] = dates[i] - scalar_days;
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            // TIMESTAMP/TZ - TIMESTAMP/TZ_scalar  →  INTERVAL
            if (!is_add && (lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ) &&
                (rhs == lt::TIMESTAMP || rhs == lt::TIMESTAMP_TZ)) {
                const auto scalar_ts = (rhs == lt::TIMESTAMP) ? scalar.value<core::date::timestamp_t>().value
                                                              : scalar.value<core::date::timestamptz_t>().value;
                const auto* ts = vec.data<core::date::microseconds>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = ts[i] - scalar_ts;
                    out.days[i] = core::date::days{0};
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            // TIME - TIME_scalar  →  INTERVAL
            if (!is_add && lhs == lt::TIME && rhs == lt::TIME) {
                const auto scalar_t = scalar.value<core::date::time_t>().value;
                const auto* t_us = vec.data<core::date::microseconds>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = t_us[i] - scalar_t;
                    out.days[i] = core::date::days{0};
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            return output;
        }

        vector_t compute_temporal_scalar_vec(std::pmr::memory_resource* resource,
                                             arithmetic_op op,
                                             const types::logical_value_t& scalar,
                                             const vector_t& vec,
                                             uint64_t count) {
            using lt = types::logical_type;
            const auto lhs = scalar.type().type();
            const auto rhs = vec.type().type();
            vector_t output(resource, types::complex_logical_type(types::arithmetic_result_type(lhs, rhs, op)), count);
            const bool is_add = (op == arithmetic_op::add);
            const int sign = is_add ? 1 : -1;

            // DATE_scalar ± INTERVAL_vec  →  DATE
            if (lhs == lt::DATE && rhs == lt::INTERVAL) {
                const auto scalar_days = scalar.value<core::date::date_t>().value;
                const auto ivl = ivl_ro::from(vec);
                auto* outs = output.data<core::date::days>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_date(scalar_days, ivl.days[i], ivl.months[i], sign);
                }
                return output;
            }
            // INTERVAL_scalar + DATE_vec  →  DATE  (commutative)
            if (is_add && lhs == lt::INTERVAL && rhs == lt::DATE) {
                const auto ivl = scalar.value<core::date::interval_t>();
                const auto* dates = vec.data<core::date::days>();
                auto* outs = output.data<core::date::days>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_date(dates[i], ivl.day, ivl.month, 1);
                }
                return output;
            }
            // TIMESTAMP/TZ_scalar ± INTERVAL_vec  →  TIMESTAMP/TZ
            if ((lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ) && rhs == lt::INTERVAL) {
                const auto scalar_ts = (lhs == lt::TIMESTAMP) ? scalar.value<core::date::timestamp_t>().value
                                                              : scalar.value<core::date::timestamptz_t>().value;
                const auto ivl = ivl_ro::from(vec);
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_ts(scalar_ts, ivl.time[i], ivl.days[i], ivl.months[i], sign);
                }
                return output;
            }
            // INTERVAL_scalar + TIMESTAMP/TZ_vec  →  TIMESTAMP/TZ  (commutative)
            if (is_add && lhs == lt::INTERVAL && (rhs == lt::TIMESTAMP || rhs == lt::TIMESTAMP_TZ)) {
                const auto ivl = scalar.value<core::date::interval_t>();
                const auto* ts = vec.data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    outs[i] = apply_interval_to_ts(ts[i], ivl.time, ivl.day, ivl.month, 1);
                }
                return output;
            }
            // TIME_scalar ± INTERVAL_vec  →  TIME
            if (lhs == lt::TIME && rhs == lt::INTERVAL) {
                const auto scalar_time = scalar.value<core::date::time_t>().value;
                const auto* ivl_us = vec.entries()[0]->data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (scalar_time + sign * ivl_us[i]) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    outs[i] = result;
                }
                return output;
            }
            // INTERVAL_scalar + TIME_vec  →  TIME  (commutative)
            if (is_add && lhs == lt::INTERVAL && rhs == lt::TIME) {
                const auto it = scalar.value<core::date::interval_t>().time;
                const auto* times = vec.data<core::date::microseconds>();
                auto* outs = output.data<core::date::microseconds>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (times[i] + it) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    outs[i] = result;
                }
                return output;
            }
            // TIME_TZ_scalar ± INTERVAL_vec  →  TIME_TZ
            if (lhs == lt::TIME_TZ && rhs == lt::INTERVAL) {
                const auto timetz = scalar.value<core::date::timetz_t>();
                const auto* ivl_us = vec.entries()[0]->data<core::date::microseconds>();
                auto* out_us = output.entries()[0]->data<core::date::microseconds>();
                auto* out_zone = output.entries()[1]->data<core::date::timezone_offset_t>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (timetz.time + sign * ivl_us[i]) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    out_us[i] = result;
                    out_zone[i] = timetz.zone;
                }
                return output;
            }
            // INTERVAL_scalar + TIME_TZ_vec  →  TIME_TZ  (commutative)
            if (is_add && lhs == lt::INTERVAL && rhs == lt::TIME_TZ) {
                const auto it = scalar.value<core::date::interval_t>().time;
                const auto* tz_us = vec.entries()[0]->data<core::date::microseconds>();
                const auto* tz_zone = vec.entries()[1]->data<core::date::timezone_offset_t>();
                auto* out_us = output.entries()[0]->data<core::date::microseconds>();
                auto* out_zone = output.entries()[1]->data<core::date::timezone_offset_t>();
                for (uint64_t i = 0; i < count; i++) {
                    auto result = (tz_us[i] + it) % ONE_DAY_US;
                    if (result.count() < 0)
                        result += ONE_DAY_US;
                    out_us[i] = result;
                    out_zone[i] = tz_zone[i];
                }
                return output;
            }
            // INTERVAL_scalar ± INTERVAL_vec  →  INTERVAL
            if (lhs == lt::INTERVAL && rhs == lt::INTERVAL) {
                const auto ivl = scalar.value<core::date::interval_t>();
                const auto rivl = ivl_ro::from(vec);
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = ivl.time + sign * rivl.time[i];
                    out.days[i] = ivl.day + sign * rivl.days[i];
                    out.months[i] = ivl.month + sign * rivl.months[i];
                }
                return output;
            }
            // INTERVAL_scalar * numeric_vec → INTERVAL
            if (op == arithmetic_op::multiply && lhs == lt::INTERVAL && types::is_numeric(rhs)) {
                const auto ivl = scalar.value<core::date::interval_t>();
                auto out = ivl_rw::from(output);
                auto do_scale = [&](auto* factors) {
                    for (uint64_t i = 0; i < count; i++) {
                        const double f = static_cast<double>(factors[i]);
                        out.time[i] = core::date::microseconds{std::llround(static_cast<double>(ivl.time.count()) * f)};
                        out.days[i] = core::date::days{
                            static_cast<int32_t>(std::llround(static_cast<double>(ivl.day.count()) * f))};
                        out.months[i] = core::date::months{
                            static_cast<int32_t>(std::llround(static_cast<double>(ivl.month.count()) * f))};
                    }
                };
                switch (rhs) {
                    case lt::TINYINT:
                        do_scale(vec.data<int8_t>());
                        break;
                    case lt::UTINYINT:
                        do_scale(vec.data<uint8_t>());
                        break;
                    case lt::SMALLINT:
                        do_scale(vec.data<int16_t>());
                        break;
                    case lt::USMALLINT:
                        do_scale(vec.data<uint16_t>());
                        break;
                    case lt::INTEGER:
                        do_scale(vec.data<int32_t>());
                        break;
                    case lt::UINTEGER:
                        do_scale(vec.data<uint32_t>());
                        break;
                    case lt::BIGINT:
                        do_scale(vec.data<int64_t>());
                        break;
                    case lt::UBIGINT:
                        do_scale(vec.data<uint64_t>());
                        break;
                    case lt::FLOAT:
                        do_scale(vec.data<float>());
                        break;
                    case lt::DOUBLE:
                        do_scale(vec.data<double>());
                        break;
                    default:
                        break;
                }
                return output;
            }
            // numeric_scalar * INTERVAL_vec → INTERVAL (commutative with scalar_as_double)
            if (op == arithmetic_op::multiply && types::is_numeric(lhs) && rhs == lt::INTERVAL) {
                const double f = scalar_as_double(scalar);
                const auto rivl = ivl_ro::from(vec);
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = core::date::microseconds{std::llround(static_cast<double>(rivl.time[i].count()) * f)};
                    out.days[i] = core::date::days{
                        static_cast<int32_t>(std::llround(static_cast<double>(rivl.days[i].count()) * f))};
                    out.months[i] = core::date::months{
                        static_cast<int32_t>(std::llround(static_cast<double>(rivl.months[i].count()) * f))};
                }
                return output;
            }
            // DATE_scalar - DATE_vec  →  INTERVAL
            if (!is_add && lhs == lt::DATE && rhs == lt::DATE) {
                const auto scalar_days = scalar.value<core::date::date_t>().value;
                const auto* dates = vec.data<core::date::days>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = core::date::microseconds{0};
                    out.days[i] = scalar_days - dates[i];
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            // TIMESTAMP/TZ_scalar - TIMESTAMP/TZ_vec  →  INTERVAL
            if (!is_add && (lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ) &&
                (rhs == lt::TIMESTAMP || rhs == lt::TIMESTAMP_TZ)) {
                const auto scalar_ts = (lhs == lt::TIMESTAMP) ? scalar.value<core::date::timestamp_t>().value
                                                              : scalar.value<core::date::timestamptz_t>().value;
                const auto* ts = vec.data<core::date::microseconds>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = scalar_ts - ts[i];
                    out.days[i] = core::date::days{0};
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            // TIME_scalar - TIME_vec  →  INTERVAL
            if (!is_add && lhs == lt::TIME && rhs == lt::TIME) {
                const auto scalar_t = scalar.value<core::date::time_t>().value;
                const auto* t_us = vec.data<core::date::microseconds>();
                auto out = ivl_rw::from(output);
                for (uint64_t i = 0; i < count; i++) {
                    out.time[i] = scalar_t - t_us[i];
                    out.days[i] = core::date::days{0};
                    out.months[i] = core::date::months{0};
                }
                return output;
            }
            return output;
        }

    } // anonymous namespace

    vector_t compute_binary_arithmetic(std::pmr::memory_resource* resource,
                                       arithmetic_op op,
                                       const vector_t& left,
                                       const vector_t& right,
                                       uint64_t count) {
        if (types::is_duration(left.type().type()) || types::is_duration(right.type().type())) {
            return compute_temporal_binary(resource, op, left, right, count);
        }
        auto result_logical = types::arithmetic_result_type(left.type().type(), right.type().type(), op);
        if (result_logical == types::logical_type::FLOAT) {
            result_logical = types::logical_type::DOUBLE;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);
        if (result_type.type() == types::logical_type::NA) {
            return output;
        }

        switch (op) {
            case arithmetic_op::add:
                dispatch_binary<std::plus>(left, right, output, count);
                break;
            case arithmetic_op::subtract:
                dispatch_binary<std::minus>(left, right, output, count);
                break;
            case arithmetic_op::multiply:
                dispatch_binary<std::multiplies>(left, right, output, count);
                break;
            case arithmetic_op::divide:
                dispatch_binary_div<checked_divides>(left, right, output, count);
                break;
            case arithmetic_op::mod:
                dispatch_binary_div<checked_modulus>(left, right, output, count);
                break;
        }
        return output;
    }

    vector_t compute_vector_scalar_arithmetic(std::pmr::memory_resource* resource,
                                              arithmetic_op op,
                                              const vector_t& vec,
                                              const types::logical_value_t& scalar,
                                              uint64_t count) {
        if (types::is_duration(vec.type().type()) || types::is_duration(scalar.type().type())) {
            return compute_temporal_vec_scalar(resource, op, vec, scalar, count);
        }
        auto result_logical = types::arithmetic_result_type(vec.type().type(), scalar.type().type(), op);
        if (result_logical == types::logical_type::FLOAT) {
            result_logical = types::logical_type::DOUBLE;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);
        if (result_type.type() == types::logical_type::NA) {
            return output;
        }

        switch (op) {
            case arithmetic_op::add:
                dispatch_vec_scalar<std::plus>(vec, scalar, output, count);
                break;
            case arithmetic_op::subtract:
                dispatch_vec_scalar<std::minus>(vec, scalar, output, count);
                break;
            case arithmetic_op::multiply:
                dispatch_vec_scalar<std::multiplies>(vec, scalar, output, count);
                break;
            case arithmetic_op::divide:
                dispatch_vec_scalar_div<checked_divides>(vec, scalar, output, count);
                break;
            case arithmetic_op::mod:
                dispatch_vec_scalar_div<checked_modulus>(vec, scalar, output, count);
                break;
        }
        return output;
    }

    vector_t compute_scalar_vector_arithmetic(std::pmr::memory_resource* resource,
                                              arithmetic_op op,
                                              const types::logical_value_t& scalar,
                                              const vector_t& vec,
                                              uint64_t count) {
        if (types::is_duration(scalar.type().type()) || types::is_duration(vec.type().type())) {
            return compute_temporal_scalar_vec(resource, op, scalar, vec, count);
        }
        auto result_logical = types::arithmetic_result_type(scalar.type().type(), vec.type().type(), op);
        if (result_logical == types::logical_type::FLOAT) {
            result_logical = types::logical_type::DOUBLE;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);
        if (result_type.type() == types::logical_type::NA) {
            return output;
        }

        switch (op) {
            case arithmetic_op::add:
                dispatch_scalar_vec<std::plus>(scalar, vec, output, count);
                break;
            case arithmetic_op::subtract:
                dispatch_scalar_vec<std::minus>(scalar, vec, output, count);
                break;
            case arithmetic_op::multiply:
                dispatch_scalar_vec<std::multiplies>(scalar, vec, output, count);
                break;
            case arithmetic_op::divide:
                dispatch_scalar_vec_div<checked_divides>(scalar, vec, output, count);
                break;
            case arithmetic_op::mod:
                dispatch_scalar_vec_div<checked_modulus>(scalar, vec, output, count);
                break;
        }
        return output;
    }

    vector_t compute_unary_neg(std::pmr::memory_resource* resource, const vector_t& vec, uint64_t count) {
        vector_t output(resource, vec.type(), count);
        types::simple_physical_type_switch<unary_neg_wrapper::callback>(vec.type().to_physical_type(),
                                                                        vec,
                                                                        output,
                                                                        count);
        return output;
    }

} // namespace components::vector