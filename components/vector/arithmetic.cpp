#include "arithmetic.hpp"

#include <cmath>
#include <core/date/date_types.hpp>
#include <functional>
#include <limits>

namespace components::vector {

    namespace {

        core::error_t non_numeric_operands(std::pmr::memory_resource* resource) {
            return core::error_t(core::error_code_t::arithmetics_failure,
                                 std::pmr::string{"arithmetic is not supported for non-numeric types", resource});
        }

        // The result of an arithmetic op is stored through the OUTPUT vector's physical type,
        // which types::arithmetic_result_type already fixed -- never through the C++ promotion
        // type of op(L, R). Those two differ, and storing through the wrong one overruns the
        // output buffer: int8 + int8 promotes to `int` (4 bytes into a 1-byte TINYINT slot), and
        // any float mix computes in double (8 bytes into a 4-byte FLOAT slot). This second
        // dispatch picks the store type from the output and hands it to the loop.
        template<typename Fn>
        core::error_t with_numeric_store(const vector_t& output, Fn&& loop) {
            switch (output.type().to_physical_type()) {
                case types::physical_type::BOOL:
                    return loop.template operator()<bool>();
                case types::physical_type::INT8:
                    return loop.template operator()<int8_t>();
                case types::physical_type::INT16:
                    return loop.template operator()<int16_t>();
                case types::physical_type::INT32:
                    return loop.template operator()<int32_t>();
                case types::physical_type::INT64:
                    return loop.template operator()<int64_t>();
                case types::physical_type::INT128:
                    return loop.template operator()<types::int128_t>();
                case types::physical_type::UINT8:
                    return loop.template operator()<uint8_t>();
                case types::physical_type::UINT16:
                    return loop.template operator()<uint16_t>();
                case types::physical_type::UINT32:
                    return loop.template operator()<uint32_t>();
                case types::physical_type::UINT64:
                    return loop.template operator()<uint64_t>();
                case types::physical_type::UINT128:
                    return loop.template operator()<types::uint128_t>();
                case types::physical_type::FLOAT:
                    return loop.template operator()<float>();
                case types::physical_type::DOUBLE:
                    return loop.template operator()<double>();
                default:
                    return core::error_t(
                        core::error_code_t::arithmetics_failure,
                        std::pmr::string{"arithmetic result type is not a numeric type", output.resource()});
            }
        }

        // Binary vector-vector
        template<template<typename...> class Op>
        struct binary_op_wrapper {
            template<typename...>
            struct callback {
                template<typename L, typename R>
                core::error_t
                operator()(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<L>&& types::is_numeric_type_v<R>) {
                    const auto* lhs = left.data<L>();
                    const auto* rhs = right.data<R>();
                    return with_numeric_store(output, [&]<typename OutT>() -> core::error_t {
                        auto* out = output.data<OutT>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = detail::to_result<OutT>(
                                op(detail::to_result<OutT>(lhs[i]), detail::to_result<OutT>(rhs[i])));
                        }
                        return core::error_t::no_error();
                    });
                }
                template<typename L, typename R>
                core::error_t operator()(const vector_t&, const vector_t&, vector_t& output, uint64_t) const
                    requires(!(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>) ) {
                    return non_numeric_operands(output.resource());
                }
            };
        };

        // Binary vector-vector with zero-check (for divide/mod): sets NULL on division by zero
        template<template<typename...> class Op>
        struct binary_div_wrapper {
            template<typename...>
            struct callback {
                template<typename L, typename R>
                core::error_t
                operator()(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<L>&& types::is_numeric_type_v<R>) {
                    const auto* lhs = left.data<L>();
                    const auto* rhs = right.data<R>();
                    return with_numeric_store(output, [&]<typename OutT>() -> core::error_t {
                        auto* out = output.data<OutT>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            // The zero test reads the untouched divisor, as it did before the
                            // store type was decoupled from the operand types.
                            if (detail::is_zero(rhs[i])) {
                                output.validity().set_invalid(i);
                                if constexpr (std::is_floating_point_v<OutT>) {
                                    out[i] = std::numeric_limits<OutT>::quiet_NaN();
                                }
                            } else {
                                out[i] = detail::to_result<OutT>(
                                    op(detail::to_result<OutT>(lhs[i]), detail::to_result<OutT>(rhs[i])));
                            }
                        }
                        return core::error_t::no_error();
                    });
                }
                template<typename L, typename R>
                core::error_t operator()(const vector_t&, const vector_t&, vector_t& output, uint64_t) const
                    requires(!(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>) ) {
                    return non_numeric_operands(output.resource());
                }
            };
        };

        // Vector-scalar
        template<template<typename...> class Op>
        struct vec_scalar_op_wrapper {
            template<typename...>
            struct callback {
                template<typename VecT, typename ScalarT>
                core::error_t operator()(const vector_t& vec,
                                         const types::logical_value_t& scalar_val,
                                         vector_t& output,
                                         uint64_t count) const
                    requires(types::is_numeric_type_v<VecT>&& types::is_numeric_type_v<ScalarT>) {
                    const ScalarT cval = scalar_val.value<ScalarT>();
                    const auto* src = vec.data<VecT>();
                    return with_numeric_store(output, [&]<typename OutT>() -> core::error_t {
                        const auto scalar_out = detail::to_result<OutT>(cval);
                        auto* out = output.data<OutT>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = detail::to_result<OutT>(op(detail::to_result<OutT>(src[i]), scalar_out));
                        }
                        return core::error_t::no_error();
                    });
                }
                template<typename VecT, typename ScalarT>
                core::error_t
                operator()(const vector_t&, const types::logical_value_t&, vector_t& output, uint64_t) const
                    requires(!(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>) ) {
                    return non_numeric_operands(output.resource());
                }
            };
        };

        // Vector-scalar with zero-check: sets NULL for all rows when scalar divisor is zero
        template<template<typename...> class Op>
        struct vec_scalar_div_wrapper {
            template<typename...>
            struct callback {
                template<typename VecT, typename ScalarT>
                core::error_t operator()(const vector_t& vec,
                                         const types::logical_value_t& scalar_val,
                                         vector_t& output,
                                         uint64_t count) const
                    requires(types::is_numeric_type_v<VecT>&& types::is_numeric_type_v<ScalarT>) {
                    const ScalarT cval = scalar_val.value<ScalarT>();
                    const auto* src = vec.data<VecT>();
                    return with_numeric_store(output, [&]<typename OutT>() -> core::error_t {
                        auto* out = output.data<OutT>();
                        if (detail::is_zero(cval)) {
                            for (uint64_t i = 0; i < count; i++) {
                                output.validity().set_invalid(i);
                                if constexpr (std::is_floating_point_v<OutT>) {
                                    out[i] = std::numeric_limits<OutT>::quiet_NaN();
                                }
                            }
                            return core::error_t::no_error();
                        }
                        const auto scalar_out = detail::to_result<OutT>(cval);
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = detail::to_result<OutT>(op(detail::to_result<OutT>(src[i]), scalar_out));
                        }
                        return core::error_t::no_error();
                    });
                }
                template<typename VecT, typename ScalarT>
                core::error_t
                operator()(const vector_t&, const types::logical_value_t&, vector_t& output, uint64_t) const
                    requires(!(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>) ) {
                    return non_numeric_operands(output.resource());
                }
            };
        };

        // Scalar-vector
        template<template<typename...> class Op>
        struct scalar_vec_op_wrapper {
            template<typename...>
            struct callback {
                template<typename ScalarT, typename VecT>
                core::error_t operator()(const types::logical_value_t& scalar_val,
                                         const vector_t& vec,
                                         vector_t& output,
                                         uint64_t count) const
                    requires(types::is_numeric_type_v<ScalarT>&& types::is_numeric_type_v<VecT>) {
                    const ScalarT cval = scalar_val.value<ScalarT>();
                    const auto* src = vec.data<VecT>();
                    return with_numeric_store(output, [&]<typename OutT>() -> core::error_t {
                        const auto scalar_out = detail::to_result<OutT>(cval);
                        auto* out = output.data<OutT>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = detail::to_result<OutT>(op(scalar_out, detail::to_result<OutT>(src[i])));
                        }
                        return core::error_t::no_error();
                    });
                }
                template<typename ScalarT, typename VecT>
                core::error_t
                operator()(const types::logical_value_t&, const vector_t&, vector_t& output, uint64_t) const
                    requires(!(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>) ) {
                    return non_numeric_operands(output.resource());
                }
            };
        };

        // Scalar-vector with zero-check: per-element zero check on vector divisor
        template<template<typename...> class Op>
        struct scalar_vec_div_wrapper {
            template<typename...>
            struct callback {
                template<typename ScalarT, typename VecT>
                core::error_t operator()(const types::logical_value_t& scalar_val,
                                         const vector_t& vec,
                                         vector_t& output,
                                         uint64_t count) const
                    requires(types::is_numeric_type_v<ScalarT>&& types::is_numeric_type_v<VecT>) {
                    const ScalarT cval = scalar_val.value<ScalarT>();
                    const auto* src = vec.data<VecT>();
                    return with_numeric_store(output, [&]<typename OutT>() -> core::error_t {
                        const auto scalar_out = detail::to_result<OutT>(cval);
                        auto* out = output.data<OutT>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(src[i])) {
                                output.validity().set_invalid(i);
                                if constexpr (std::is_floating_point_v<OutT>) {
                                    out[i] = std::numeric_limits<OutT>::quiet_NaN();
                                }
                            } else {
                                out[i] = detail::to_result<OutT>(op(scalar_out, detail::to_result<OutT>(src[i])));
                            }
                        }
                        return core::error_t::no_error();
                    });
                }
                template<typename ScalarT, typename VecT>
                core::error_t
                operator()(const types::logical_value_t&, const vector_t&, vector_t& output, uint64_t) const
                    requires(!(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>) ) {
                    return non_numeric_operands(output.resource());
                }
            };
        };

        // Unary negation
        struct unary_neg_wrapper {
            template<typename...>
            struct callback {
                template<typename T>
                core::error_t operator()(const vector_t& vec, vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<T>) {
                    const auto* src = vec.data<T>();
                    auto* out = output.data<T>();
                    for (uint64_t i = 0; i < count; i++) {
                        out[i] = detail::to_result<T>(-detail::widen_type(src[i]));
                    }
                    return core::error_t::no_error();
                }
                template<typename T>
                core::error_t operator()(const vector_t&, vector_t& output, uint64_t) const
                    requires(!types::is_numeric_type_v<T>) {
                    return core::error_t(
                        core::error_code_t::arithmetics_failure,
                        std::pmr::string{"negation is not supported for non-numeric types", output.resource()});
                }
            };
        };

        template<template<typename...> class Op>
        core::error_t dispatch_binary(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) {
            return types::double_simple_physical_type_switch<binary_op_wrapper<Op>::template callback>(
                left.type().to_physical_type(),
                right.type().to_physical_type(),
                left,
                right,
                output,
                count);
        }

        template<template<typename...> class Op>
        core::error_t
        dispatch_binary_div(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) {
            return types::double_simple_physical_type_switch<binary_div_wrapper<Op>::template callback>(
                left.type().to_physical_type(),
                right.type().to_physical_type(),
                left,
                right,
                output,
                count);
        }

        template<template<typename...> class Op>
        core::error_t dispatch_vec_scalar(const vector_t& vec,
                                          const types::logical_value_t& scalar,
                                          vector_t& output,
                                          uint64_t count) {
            return types::double_simple_physical_type_switch<vec_scalar_op_wrapper<Op>::template callback>(
                vec.type().to_physical_type(),
                scalar.type().to_physical_type(),
                vec,
                scalar,
                output,
                count);
        }

        template<template<typename...> class Op>
        core::error_t dispatch_vec_scalar_div(const vector_t& vec,
                                              const types::logical_value_t& scalar,
                                              vector_t& output,
                                              uint64_t count) {
            return types::double_simple_physical_type_switch<vec_scalar_div_wrapper<Op>::template callback>(
                vec.type().to_physical_type(),
                scalar.type().to_physical_type(),
                vec,
                scalar,
                output,
                count);
        }

        template<template<typename...> class Op>
        core::error_t dispatch_scalar_vec(const types::logical_value_t& scalar,
                                          const vector_t& vec,
                                          vector_t& output,
                                          uint64_t count) {
            return types::double_simple_physical_type_switch<scalar_vec_op_wrapper<Op>::template callback>(
                scalar.type().to_physical_type(),
                vec.type().to_physical_type(),
                scalar,
                vec,
                output,
                count);
        }

        template<template<typename...> class Op>
        core::error_t dispatch_scalar_vec_div(const types::logical_value_t& scalar,
                                              const vector_t& vec,
                                              vector_t& output,
                                              uint64_t count) {
            return types::double_simple_physical_type_switch<scalar_vec_div_wrapper<Op>::template callback>(
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

    core::result_wrapper_t<vector_t> compute_binary_arithmetic(std::pmr::memory_resource* resource,
                                                               arithmetic_op op,
                                                               const vector_t& left,
                                                               const vector_t& right,
                                                               uint64_t count) {
        // The result type of an empty input is the operand-derived one, exactly as for a
        // non-empty input: a 0-row chunk is real input, not a drain sentinel, and the column
        // it produces is still typed. (Reading the operand types here is safe because
        // data_chunk_t::at answers nullptr for a column past the chunk's width, so an
        // unresolvable operand is rejected before the kernel is ever called.) The dispatch is
        // still skipped, so no operand ROW is touched.
        const auto result_logical = types::arithmetic_result_type(left.type().type(), right.type().type(), op);
        if (count == 0) {
            return vector_t(resource, types::complex_logical_type(result_logical), 0);
        }
        // Compute the result values first (numeric or temporal), then apply NULL propagation as a
        // single tail below. Capturing the temporal branch in this local instead of returning it
        // directly keeps it on the same validity path as the numeric branch.
        if (types::is_duration(left.type().type()) || types::is_duration(right.type().type())) {
            vector_t temporal = compute_temporal_binary(resource, op, left, right, count);
            temporal.validity().combine(left.validity(), count);
            temporal.validity().combine(right.validity(), count);
            return temporal;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);
        if (result_type.type() != types::logical_type::NA) {
            core::error_t error = core::error_t::no_error();
            switch (op) {
                case arithmetic_op::add:
                    error = dispatch_binary<std::plus>(left, right, output, count);
                    break;
                case arithmetic_op::subtract:
                    error = dispatch_binary<std::minus>(left, right, output, count);
                    break;
                case arithmetic_op::multiply:
                    error = dispatch_binary<std::multiplies>(left, right, output, count);
                    break;
                case arithmetic_op::divide:
                    error = dispatch_binary_div<checked_divides>(left, right, output, count);
                    break;
                case arithmetic_op::mod:
                    error = dispatch_binary_div<checked_modulus>(left, right, output, count);
                    break;
            }
            if (error.contains_error()) {
                return error;
            }
        }
        // SQL three-valued logic: a NULL operand makes the result NULL. combine is a word-wise
        // AND that is a no-op when the operand mask is all-valid, so NULL-free columns pay nothing.
        // Applied after the dispatch so it composes with the divide/mod zero-invalidation.
        output.validity().combine(left.validity(), count);
        output.validity().combine(right.validity(), count);
        return output;
    }

    core::result_wrapper_t<vector_t> compute_vector_scalar_arithmetic(std::pmr::memory_resource* resource,
                                                                      arithmetic_op op,
                                                                      const vector_t& vec,
                                                                      const types::logical_value_t& scalar,
                                                                      uint64_t count) {
        // Empty input keeps the operand-derived type: see compute_binary_arithmetic.
        const auto result_logical = types::arithmetic_result_type(vec.type().type(), scalar.type().type(), op);
        if (count == 0) {
            return vector_t(resource, types::complex_logical_type(result_logical), 0);
        }
        // A NULL scalar makes every result NULL; otherwise propagate the vector operand's nulls.
        auto propagate_nulls = [&](vector_t& out) {
            if (scalar.is_null()) {
                out.validity().set_all_invalid(count);
            } else {
                out.validity().combine(vec.validity(), count);
            }
        };
        if (types::is_duration(vec.type().type()) || types::is_duration(scalar.type().type())) {
            vector_t temporal = compute_temporal_vec_scalar(resource, op, vec, scalar, count);
            propagate_nulls(temporal);
            return temporal;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);
        if (result_type.type() != types::logical_type::NA) {
            core::error_t error = core::error_t::no_error();
            switch (op) {
                case arithmetic_op::add:
                    error = dispatch_vec_scalar<std::plus>(vec, scalar, output, count);
                    break;
                case arithmetic_op::subtract:
                    error = dispatch_vec_scalar<std::minus>(vec, scalar, output, count);
                    break;
                case arithmetic_op::multiply:
                    error = dispatch_vec_scalar<std::multiplies>(vec, scalar, output, count);
                    break;
                case arithmetic_op::divide:
                    error = dispatch_vec_scalar_div<checked_divides>(vec, scalar, output, count);
                    break;
                case arithmetic_op::mod:
                    error = dispatch_vec_scalar_div<checked_modulus>(vec, scalar, output, count);
                    break;
            }
            if (error.contains_error()) {
                return error;
            }
        }
        propagate_nulls(output);
        return output;
    }

    core::result_wrapper_t<vector_t> compute_scalar_vector_arithmetic(std::pmr::memory_resource* resource,
                                                                      arithmetic_op op,
                                                                      const types::logical_value_t& scalar,
                                                                      const vector_t& vec,
                                                                      uint64_t count) {
        // Empty input keeps the operand-derived type: see compute_binary_arithmetic.
        const auto result_logical = types::arithmetic_result_type(scalar.type().type(), vec.type().type(), op);
        if (count == 0) {
            return vector_t(resource, types::complex_logical_type(result_logical), 0);
        }
        // A NULL scalar makes every result NULL; otherwise propagate the vector operand's nulls.
        auto propagate_nulls = [&](vector_t& out) {
            if (scalar.is_null()) {
                out.validity().set_all_invalid(count);
            } else {
                out.validity().combine(vec.validity(), count);
            }
        };
        if (types::is_duration(scalar.type().type()) || types::is_duration(vec.type().type())) {
            vector_t temporal = compute_temporal_scalar_vec(resource, op, scalar, vec, count);
            propagate_nulls(temporal);
            return temporal;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);
        if (result_type.type() != types::logical_type::NA) {
            core::error_t error = core::error_t::no_error();
            switch (op) {
                case arithmetic_op::add:
                    error = dispatch_scalar_vec<std::plus>(scalar, vec, output, count);
                    break;
                case arithmetic_op::subtract:
                    error = dispatch_scalar_vec<std::minus>(scalar, vec, output, count);
                    break;
                case arithmetic_op::multiply:
                    error = dispatch_scalar_vec<std::multiplies>(scalar, vec, output, count);
                    break;
                case arithmetic_op::divide:
                    error = dispatch_scalar_vec_div<checked_divides>(scalar, vec, output, count);
                    break;
                case arithmetic_op::mod:
                    error = dispatch_scalar_vec_div<checked_modulus>(scalar, vec, output, count);
                    break;
            }
            if (error.contains_error()) {
                return error;
            }
        }
        propagate_nulls(output);
        return output;
    }

    core::result_wrapper_t<vector_t>
    compute_unary_neg(std::pmr::memory_resource* resource, const vector_t& vec, uint64_t count) {
        // Empty input keeps the operand's type: negation never changes it.
        vector_t output(resource, vec.type(), count);
        if (count == 0) {
            return output;
        }
        auto error = types::simple_physical_type_switch<unary_neg_wrapper::callback>(vec.type().to_physical_type(),
                                                                                      vec,
                                                                                      output,
                                                                                      count);
        if (error.contains_error()) {
            return error;
        }
        // -NULL is NULL: carry the operand's nulls into the negated result.
        output.validity().combine(vec.validity(), count);
        return output;
    }

} // namespace components::vector