#include "arithmetic.hpp"

#include <functional>

namespace components::vector {

    namespace {

        // Detect problematic int128 <-> float/double combinations
        template<typename L, typename R>
        constexpr bool is_int128_float_mix_v =
            ((std::is_same_v<std::remove_cv_t<L>, types::int128_t> ||
              std::is_same_v<std::remove_cv_t<L>, types::uint128_t>) &&
             std::is_floating_point_v<R>) ||
            (std::is_floating_point_v<L> &&
             (std::is_same_v<std::remove_cv_t<R>, types::int128_t> ||
              std::is_same_v<std::remove_cv_t<R>, types::uint128_t>));

        // Binary vector-vector
        template<template<typename...> class Op>
        struct binary_op_wrapper {
            template<typename...>
            struct callback {
                template<typename L, typename R>
                void operator()(const vector_t& left, const vector_t& right,
                                vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>)
                {
                    auto* lhs = left.data<L>();
                    auto* rhs = right.data<R>();
                    if constexpr (is_int128_float_mix_v<L, R> ||
                                  std::is_floating_point_v<L> || std::is_floating_point_v<R>) {
                        // All floating-point arithmetic uses double for precision
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            out[i] = op(static_cast<double>(lhs[i]),
                                        static_cast<double>(rhs[i]));
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
                    requires(!(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>))
                {
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
                void operator()(const vector_t& left, const vector_t& right,
                                vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>)
                {
                    auto* lhs = left.data<L>();
                    auto* rhs = right.data<R>();
                    if constexpr (is_int128_float_mix_v<L, R> ||
                                  std::is_floating_point_v<L> || std::is_floating_point_v<R>) {
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(rhs[i])) {
                                output.validity().set_invalid(i);
                                out[i] = double{};
                            } else {
                                out[i] = op(static_cast<double>(lhs[i]),
                                            static_cast<double>(rhs[i]));
                            }
                        }
                    } else {
                        Op<void> op{};
                        using result_t = std::decay_t<decltype(op(std::declval<L>(), std::declval<R>()))>;
                        auto* out = output.data<result_t>();
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(rhs[i])) {
                                output.validity().set_invalid(i);
                                out[i] = result_t{};
                            } else {
                                out[i] = op(lhs[i], rhs[i]);
                            }
                        }
                    }
                }
                template<typename L, typename R>
                void operator()(const vector_t&, const vector_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<L> && types::is_numeric_type_v<R>))
                {
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
                void operator()(const vector_t& vec, const types::logical_value_t& scalar_val,
                                vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>)
                {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<VecT, ScalarT> ||
                                  std::is_floating_point_v<VecT> || std::is_floating_point_v<ScalarT>) {
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
                    requires(!(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>))
                {
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
                void operator()(const vector_t& vec, const types::logical_value_t& scalar_val,
                                vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>)
                {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    if (detail::is_zero(cval)) {
                        // All results are NULL
                        for (uint64_t i = 0; i < count; i++) {
                            output.validity().set_invalid(i);
                        }
                        return;
                    }
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<VecT, ScalarT> ||
                                  std::is_floating_point_v<VecT> || std::is_floating_point_v<ScalarT>) {
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
                    requires(!(types::is_numeric_type_v<VecT> && types::is_numeric_type_v<ScalarT>))
                {
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
                void operator()(const types::logical_value_t& scalar_val, const vector_t& vec,
                                vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>)
                {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<ScalarT, VecT> ||
                                  std::is_floating_point_v<ScalarT> || std::is_floating_point_v<VecT>) {
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
                    requires(!(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>))
                {
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
                void operator()(const types::logical_value_t& scalar_val, const vector_t& vec,
                                vector_t& output, uint64_t count) const
                    requires(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>)
                {
                    ScalarT cval = scalar_val.value<ScalarT>();
                    auto* src = vec.data<VecT>();
                    if constexpr (is_int128_float_mix_v<ScalarT, VecT> ||
                                  std::is_floating_point_v<ScalarT> || std::is_floating_point_v<VecT>) {
                        auto dcval = static_cast<double>(cval);
                        auto* out = output.data<double>();
                        Op<void> op{};
                        for (uint64_t i = 0; i < count; i++) {
                            if (detail::is_zero(src[i])) {
                                output.validity().set_invalid(i);
                                out[i] = double{};
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
                                out[i] = result_t{};
                            } else {
                                out[i] = op(cval, src[i]);
                            }
                        }
                    }
                }
                template<typename ScalarT, typename VecT>
                void operator()(const types::logical_value_t&, const vector_t&, vector_t&, uint64_t) const
                    requires(!(types::is_numeric_type_v<ScalarT> && types::is_numeric_type_v<VecT>))
                {
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
                    requires(types::is_numeric_type_v<T>)
                {
                    auto* src = vec.data<T>();
                    auto* out = output.data<T>();
                    for (uint64_t i = 0; i < count; i++) {
                        out[i] = -src[i];
                    }
                }
                template<typename T>
                void operator()(const vector_t&, vector_t&, uint64_t) const
                    requires(!types::is_numeric_type_v<T>)
                {
                    throw std::logic_error("Negation not supported for non-numeric types");
                }
            };
        };

        template<template<typename...> class Op>
        void dispatch_binary(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<binary_op_wrapper<Op>::template callback>(
                left.type().to_physical_type(), right.type().to_physical_type(),
                left, right, output, count);
        }

        template<template<typename...> class Op>
        void dispatch_binary_div(const vector_t& left, const vector_t& right, vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<binary_div_wrapper<Op>::template callback>(
                left.type().to_physical_type(), right.type().to_physical_type(),
                left, right, output, count);
        }

        template<template<typename...> class Op>
        void dispatch_vec_scalar(const vector_t& vec, const types::logical_value_t& scalar,
                                 vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<vec_scalar_op_wrapper<Op>::template callback>(
                vec.type().to_physical_type(), scalar.type().to_physical_type(),
                vec, scalar, output, count);
        }

        template<template<typename...> class Op>
        void dispatch_vec_scalar_div(const vector_t& vec, const types::logical_value_t& scalar,
                                     vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<vec_scalar_div_wrapper<Op>::template callback>(
                vec.type().to_physical_type(), scalar.type().to_physical_type(),
                vec, scalar, output, count);
        }

        template<template<typename...> class Op>
        void dispatch_scalar_vec(const types::logical_value_t& scalar, const vector_t& vec,
                                 vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<scalar_vec_op_wrapper<Op>::template callback>(
                scalar.type().to_physical_type(), vec.type().to_physical_type(),
                scalar, vec, output, count);
        }

        template<template<typename...> class Op>
        void dispatch_scalar_vec_div(const types::logical_value_t& scalar, const vector_t& vec,
                                     vector_t& output, uint64_t count) {
            types::double_simple_physical_type_switch<scalar_vec_div_wrapper<Op>::template callback>(
                scalar.type().to_physical_type(), vec.type().to_physical_type(),
                scalar, vec, output, count);
        }

    } // anonymous namespace

    vector_t compute_binary_arithmetic(std::pmr::memory_resource* resource,
                                       arithmetic_op op,
                                       const vector_t& left,
                                       const vector_t& right,
                                       uint64_t count) {
        auto result_logical = types::promote_type(left.type().type(), right.type().type());
        // Promote FLOAT to DOUBLE for precision (matches PostgreSQL behavior)
        if (result_logical == types::logical_type::FLOAT) {
            result_logical = types::logical_type::DOUBLE;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);

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
        auto result_logical = types::promote_type(vec.type().type(), scalar.type().type());
        if (result_logical == types::logical_type::FLOAT) {
            result_logical = types::logical_type::DOUBLE;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);

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
        auto result_logical = types::promote_type(scalar.type().type(), vec.type().type());
        if (result_logical == types::logical_type::FLOAT) {
            result_logical = types::logical_type::DOUBLE;
        }
        auto result_type = types::complex_logical_type(result_logical);
        vector_t output(resource, result_type, count);

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

    vector_t compute_unary_neg(std::pmr::memory_resource* resource,
                               const vector_t& vec,
                               uint64_t count) {
        vector_t output(resource, vec.type(), count);
        types::simple_physical_type_switch<unary_neg_wrapper::callback>(
            vec.type().to_physical_type(), vec, output, count);
        return output;
    }

} // namespace components::vector