#include "../function.hpp"
#include <components/types/logical_value.hpp>
#include <components/vector/operations/apply_operator.hpp>
#include <components/vector/vector_operations.hpp>

#include <cassert>
#include <compare>
#include <string_view>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {
    void register_kernel(std::pmr::memory_resource* resource, auto& fn, auto kernel) {
        // Bound, not std::ignore'd (<tuple> is banned): add_kernel only refuses on a full slot
        // table or an arity mismatch, both compile-time constants here, so the assert states a
        // file invariant rather than screening runtime input.
        [[maybe_unused]] const auto added = fn->add_kernel(resource, std::move(kernel));
        assert(!added.contains_error() && "aggregate kernel must fit the declared slots and arity");
    }

    template<typename T>
    concept addable = requires(T& a, const T& b) {
        a += b;
    };
    template<typename T>
    concept comparable = requires(const T& a, const T& b) {
        a < b;
    };
    template<typename T>
    concept dividable = requires(const T& a, const T& b) {
        a / b;
    };

    // An aggregate accumulates into one state per group, addressed by group id

    template<typename T>
    struct numeric_state_t {
        T value{};
        bool has_value{false};
    };

    template<typename T>
    struct avg_state_t {
        T value{};
        uint64_t count{0};
    };

    // avg over signed integers and decimals sums exactly in 128 bits, over real/double in double.
    struct avg_wide_state_t {
        int128_t exact{0};
        double inexact{0};
        uint64_t count{0};
    };

    struct count_state_t {
        uint64_t value{0};
    };

    // How sum/avg accumulate an argument type (Trino semantics). Unsigned integers and HUGEINT have no Trino
    // counterpart and keep accumulating in their own type.
    enum class accumulation
    {
        widened_integer,
        decimal,
        floating,
        input_type
    };

    accumulation accumulation_of(const complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT:
                return accumulation::widened_integer;
            case logical_type::DECIMAL:
                return accumulation::decimal;
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
                return accumulation::floating;
            default:
                return accumulation::input_type;
        }
    }

    core::error_t one_argument_expected(std::pmr::memory_resource* resource) {
        return core::error_t(core::error_code_t::incorrect_function_argument,
                             std::pmr::string{"the aggregate takes exactly one argument", resource});
    }

    core::result_wrapper_t<complex_logical_type> sum_result_type(std::pmr::memory_resource* resource,
                                                                 const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1) {
            return one_argument_expected(resource);
        }
        const auto& input = inputs.front();
        switch (accumulation_of(input)) {
            case accumulation::widened_integer:
                return complex_logical_type{logical_type::BIGINT};
            case accumulation::decimal:
                return complex_logical_type::create_decimal(
                    resource,
                    DECIMAL_MAX_WIDTH,
                    input.extension_as<decimal_logical_type_extension>()->scale());
            case accumulation::floating:
            case accumulation::input_type:
                return input;
        }
        return input;
    }

    core::result_wrapper_t<complex_logical_type> avg_result_type(std::pmr::memory_resource* resource,
                                                                 const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1) {
            return one_argument_expected(resource);
        }
        if (accumulation_of(inputs.front()) == accumulation::widened_integer) {
            return complex_logical_type{logical_type::DOUBLE};
        }
        return inputs.front();
    }

    // Adds without ever wrapping: false means the total left the accumulator's range.
    bool checked_add(int64_t& total, int64_t value) { return !__builtin_add_overflow(total, value, &total); }

    // The largest unscaled DECIMAL(38, s) payload.
    const int128_t& max_decimal_payload() {
        static const int128_t limit = [] {
            int128_t value{1};
            for (uint8_t digit = 0; digit < DECIMAL_MAX_WIDTH; digit++) {
                value *= 10;
            }
            return value - 1;
        }();
        return limit;
    }

    bool checked_add(int128_t& total, int128_t value) {
        const auto& limit = max_decimal_payload();
        if ((value > 0 && total > limit - value) || (value < 0 && total < -limit - value)) {
            return false;
        }
        total += value;
        return true;
    }

    // Folds every non-null row into its group's accumulator; stops at the first refused addition.
    template<typename in_t, typename state_t, typename add_t>
    bool fold(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states, add_t add) {
        const auto* data = input.data<in_t>();
        const bool all_valid = input.validity().all_valid();
        for (uint64_t row = 0; row < groups.size(); row++) {
            if (!all_valid && input.is_null(row)) {
                continue;
            }
            if (!add(states.at<state_t>(groups[row]), data[row])) {
                return false;
            }
        }
        return true;
    }

    core::error_t overflow(kernel_context& ctx, const char* name) {
        std::pmr::string message{name, ctx.exec_context().resource()};
        message += " overflow: the total leaves the range of its result type";
        return core::error_t(core::error_code_t::arithmetics_failure, std::move(message));
    }

    template<template<typename> class op_t, typename fallback_t, typename... args_t>
    auto arithmetic_dispatch(const complex_logical_type& type, fallback_t fallback, args_t&&... args)
        -> decltype(fallback()) {
        switch (type.type()) {
            case logical_type::TINYINT:
                return op_t<int8_t>{}(std::forward<args_t>(args)...);
            case logical_type::SMALLINT:
                return op_t<int16_t>{}(std::forward<args_t>(args)...);
            case logical_type::INTEGER:
                return op_t<int32_t>{}(std::forward<args_t>(args)...);
            case logical_type::BIGINT:
                return op_t<int64_t>{}(std::forward<args_t>(args)...);
            case logical_type::HUGEINT:
                return op_t<int128_t>{}(std::forward<args_t>(args)...);
            case logical_type::UTINYINT:
                return op_t<uint8_t>{}(std::forward<args_t>(args)...);
            case logical_type::USMALLINT:
                return op_t<uint16_t>{}(std::forward<args_t>(args)...);
            case logical_type::UINTEGER:
                return op_t<uint32_t>{}(std::forward<args_t>(args)...);
            case logical_type::UBIGINT:
                return op_t<uint64_t>{}(std::forward<args_t>(args)...);
            case logical_type::UHUGEINT:
                return op_t<uint128_t>{}(std::forward<args_t>(args)...);
            case logical_type::FLOAT:
                return op_t<float>{}(std::forward<args_t>(args)...);
            case logical_type::DOUBLE:
                return op_t<double>{}(std::forward<args_t>(args)...);
            case logical_type::DECIMAL:
                switch (type.to_physical_type()) {
                    case physical_type::INT16:
                        return op_t<int16_t>{}(std::forward<args_t>(args)...);
                    case physical_type::INT32:
                        return op_t<int32_t>{}(std::forward<args_t>(args)...);
                    case physical_type::INT64:
                        return op_t<int64_t>{}(std::forward<args_t>(args)...);
                    case physical_type::INT128:
                        return op_t<int128_t>{}(std::forward<args_t>(args)...);
                    default:
                        return fallback();
                }
            default:
                return fallback();
        }
    }

    template<template<typename> class op_t, typename fallback_t, typename... args_t>
    auto ordered_dispatch(const complex_logical_type& type, fallback_t fallback, args_t&&... args)
        -> decltype(fallback()) {
        switch (type.to_physical_type()) {
            case physical_type::BOOL:
            case physical_type::INT8:
                return op_t<int8_t>{}(std::forward<args_t>(args)...);
            case physical_type::INT16:
                return op_t<int16_t>{}(std::forward<args_t>(args)...);
            case physical_type::INT32:
                return op_t<int32_t>{}(std::forward<args_t>(args)...);
            case physical_type::INT64:
                return op_t<int64_t>{}(std::forward<args_t>(args)...);
            case physical_type::INT128:
                return op_t<int128_t>{}(std::forward<args_t>(args)...);
            case physical_type::UINT8:
                return op_t<uint8_t>{}(std::forward<args_t>(args)...);
            case physical_type::UINT16:
                return op_t<uint16_t>{}(std::forward<args_t>(args)...);
            case physical_type::UINT32:
                return op_t<uint32_t>{}(std::forward<args_t>(args)...);
            case physical_type::UINT64:
                return op_t<uint64_t>{}(std::forward<args_t>(args)...);
            case physical_type::UINT128:
                return op_t<uint128_t>{}(std::forward<args_t>(args)...);
            case physical_type::FLOAT:
                return op_t<float>{}(std::forward<args_t>(args)...);
            case physical_type::DOUBLE:
                return op_t<double>{}(std::forward<args_t>(args)...);
            default:
                return fallback();
        }
    }

    core::error_t unsupported_argument(kernel_context& ctx, const char* name) {
        std::pmr::string message{name, ctx.exec_context().resource()};
        message += " does not accumulate the type it was given";
        return core::error_t(core::error_code_t::kernel_error, std::move(message));
    }

    // ---- layouts ---------------------------------------------------------------------------

    template<typename T>
    struct numeric_layout_t {
        aggregate_state_layout_t operator()() const { return aggregate_state_of<numeric_state_t<T>>(); }
    };

    template<typename T>
    struct avg_layout_t {
        aggregate_state_layout_t operator()() const { return aggregate_state_of<avg_state_t<T>>(); }
    };

    aggregate_state_layout_t no_layout() { return {}; }

    aggregate_state_layout_t sum_layout(const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1) {
            return {};
        }
        switch (accumulation_of(inputs.front())) {
            case accumulation::widened_integer:
                return aggregate_state_of<numeric_state_t<int64_t>>();
            case accumulation::decimal:
                return aggregate_state_of<numeric_state_t<int128_t>>();
            case accumulation::floating:
            case accumulation::input_type:
                break;
        }
        return arithmetic_dispatch<numeric_layout_t>(inputs.front(), no_layout);
    }

    // MIN/MAX does not accumulate a running total: it keeps the winning ROW
    struct min_max_state_t {
        bool has_value{false};
    };

    template<typename>
    struct orderable_t {
        bool operator()() const { return true; }
    };

    [[nodiscard]] bool is_orderable(const complex_logical_type& type) {
        if (type.type() == logical_type::LIST || type.type() == logical_type::ARRAY) {
            return is_orderable(type.child_type());
        }
        if (type.to_physical_type() == physical_type::STRING) {
            return true;
        }
        return ordered_dispatch<orderable_t>(type, [] { return false; });
    }

    aggregate_state_layout_t min_max_layout(const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1 || !is_orderable(inputs.front())) {
            return {};
        }
        auto layout = aggregate_state_of<min_max_state_t>();
        layout.argument_type = inputs.front();
        return layout;
    }

    aggregate_state_layout_t avg_layout(const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1) {
            return {};
        }
        if (accumulation_of(inputs.front()) != accumulation::input_type) {
            return aggregate_state_of<avg_wide_state_t>();
        }
        return arithmetic_dispatch<avg_layout_t>(inputs.front(), no_layout);
    }

    aggregate_state_layout_t count_layout(const std::pmr::vector<complex_logical_type>&) {
        return aggregate_state_of<count_state_t>();
    }

    // ---- updates ---------------------------------------------------------------------------

    template<typename T>
    struct sum_update_t {
        core::error_t
        operator()(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states) const {
            const auto* data = input.data<T>();
            const bool all_valid = input.validity().all_valid();
            for (uint64_t row = 0; row < groups.size(); row++) {
                if (!all_valid && input.is_null(row)) {
                    continue;
                }
                auto& accumulator = states.at<numeric_state_t<T>>(groups[row]);
                accumulator.value = static_cast<T>(accumulator.value + data[row]);
                accumulator.has_value = true;
            }
            return core::error_t::no_error();
        }
    };

    template<typename T>
    struct avg_update_t {
        core::error_t
        operator()(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states) const {
            const auto* data = input.data<T>();
            const bool all_valid = input.validity().all_valid();
            for (uint64_t row = 0; row < groups.size(); row++) {
                if (!all_valid && input.is_null(row)) {
                    continue;
                }
                auto& accumulator = states.at<avg_state_t<T>>(groups[row]);
                accumulator.value = static_cast<T>(accumulator.value + data[row]);
                accumulator.count++;
            }
            return core::error_t::no_error();
        }
    };

    core::error_t sum_update(kernel_context& ctx,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        const auto& column = input.data.front();
        using sum64_t = numeric_state_t<int64_t>;
        using sum128_t = numeric_state_t<int128_t>;
        auto add64 = [](sum64_t& total, auto value) {
            total.has_value = true;
            return checked_add(total.value, static_cast<int64_t>(value));
        };
        auto add128 = [](sum128_t& total, auto value) {
            total.has_value = true;
            return checked_add(total.value, static_cast<int128_t>(value));
        };
        bool fits = true;
        switch (column.type().type()) {
            case logical_type::TINYINT:
                fits = fold<int8_t, sum64_t>(column, groups, states, add64);
                break;
            case logical_type::SMALLINT:
                fits = fold<int16_t, sum64_t>(column, groups, states, add64);
                break;
            case logical_type::INTEGER:
                fits = fold<int32_t, sum64_t>(column, groups, states, add64);
                break;
            case logical_type::BIGINT:
                fits = fold<int64_t, sum64_t>(column, groups, states, add64);
                break;
            case logical_type::DECIMAL:
                switch (column.type().to_physical_type()) {
                    case physical_type::INT16:
                        fits = fold<int16_t, sum128_t>(column, groups, states, add128);
                        break;
                    case physical_type::INT32:
                        fits = fold<int32_t, sum128_t>(column, groups, states, add128);
                        break;
                    case physical_type::INT64:
                        fits = fold<int64_t, sum128_t>(column, groups, states, add128);
                        break;
                    case physical_type::INT128:
                        fits = fold<int128_t, sum128_t>(column, groups, states, add128);
                        break;
                    default:
                        return unsupported_argument(ctx, "sum");
                }
                break;
            default:
                return arithmetic_dispatch<sum_update_t>(
                    column.type(),
                    [&ctx] { return unsupported_argument(ctx, "sum"); },
                    column,
                    groups,
                    states);
        }
        return fits ? core::error_t::no_error() : overflow(ctx, "sum");
    }

    // One update for every type MIN/MAX accepts: order the incoming row against the group's
    // current winner, and when it wins, copy it in. `wants_greater` picks max.
    template<bool wants_greater>
    core::error_t min_max_update(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states) {
        const bool all_valid = input.validity().all_valid();
        for (uint64_t row = 0; row < groups.size(); row++) {
            if (!all_valid && input.is_null(row)) {
                continue;
            }
            auto& accumulator = states.at<min_max_state_t>(groups[row]);
            vector_t& winners = states.values(groups[row]);
            const uint64_t slot = states.slot_of(groups[row]);
            if (accumulator.has_value) {
                const std::partial_ordering ordering = operations::compare_cells(input, row, winners, slot);
                const bool wins = wants_greater ? ordering == std::partial_ordering::greater
                                                : ordering == std::partial_ordering::less;
                if (!wins) {
                    continue;
                }
            }
            vector_ops::copy(input, winners, row + 1, row, slot);
            accumulator.has_value = true;
        }
        return core::error_t::no_error();
    }

    core::error_t min_update(kernel_context&,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        return min_max_update</*wants_greater*/ false>(input.data.front(), groups, states);
    }

    core::error_t max_update(kernel_context&,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        return min_max_update</*wants_greater*/ true>(input.data.front(), groups, states);
    }

    core::error_t avg_update(kernel_context& ctx,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        const auto& column = input.data.front();
        auto add_integer = [](avg_wide_state_t& total, auto value) {
            total.exact += static_cast<int128_t>(value);
            total.count++;
            return true;
        };
        auto add_decimal = [](avg_wide_state_t& total, auto value) {
            total.count++;
            return checked_add(total.exact, static_cast<int128_t>(value));
        };
        auto add_floating = [](avg_wide_state_t& total, auto value) {
            total.inexact += static_cast<double>(value);
            total.count++;
            return true;
        };
        bool fits = true;
        switch (column.type().type()) {
            case logical_type::TINYINT:
                fits = fold<int8_t, avg_wide_state_t>(column, groups, states, add_integer);
                break;
            case logical_type::SMALLINT:
                fits = fold<int16_t, avg_wide_state_t>(column, groups, states, add_integer);
                break;
            case logical_type::INTEGER:
                fits = fold<int32_t, avg_wide_state_t>(column, groups, states, add_integer);
                break;
            case logical_type::BIGINT:
                fits = fold<int64_t, avg_wide_state_t>(column, groups, states, add_integer);
                break;
            case logical_type::FLOAT:
                fits = fold<float, avg_wide_state_t>(column, groups, states, add_floating);
                break;
            case logical_type::DOUBLE:
                fits = fold<double, avg_wide_state_t>(column, groups, states, add_floating);
                break;
            case logical_type::DECIMAL:
                switch (column.type().to_physical_type()) {
                    case physical_type::INT16:
                        fits = fold<int16_t, avg_wide_state_t>(column, groups, states, add_decimal);
                        break;
                    case physical_type::INT32:
                        fits = fold<int32_t, avg_wide_state_t>(column, groups, states, add_decimal);
                        break;
                    case physical_type::INT64:
                        fits = fold<int64_t, avg_wide_state_t>(column, groups, states, add_decimal);
                        break;
                    case physical_type::INT128:
                        fits = fold<int128_t, avg_wide_state_t>(column, groups, states, add_decimal);
                        break;
                    default:
                        return unsupported_argument(ctx, "avg");
                }
                break;
            default:
                return arithmetic_dispatch<avg_update_t>(
                    column.type(),
                    [&ctx] { return unsupported_argument(ctx, "avg"); },
                    column,
                    groups,
                    states);
        }
        return fits ? core::error_t::no_error() : overflow(ctx, "avg");
    }

    // COUNT(x) counts the rows where x is not null.
    core::error_t count_update(kernel_context&,
                               const data_chunk_t& input,
                               core::span<const uint32_t> groups,
                               aggregate_states_t states) {
        const auto& column = input.data.front();
        const bool all_valid = column.validity().all_valid();
        for (uint64_t row = 0; row < groups.size(); row++) {
            if (!all_valid && column.is_null(row)) {
                continue;
            }
            states.at<count_state_t>(groups[row]).value++;
        }
        return core::error_t::no_error();
    }

    // COUNT(*) takes no argument column: every row counts.
    core::error_t count_star_update(kernel_context&,
                                    const data_chunk_t&,
                                    core::span<const uint32_t> groups,
                                    aggregate_states_t states) {
        for (uint64_t row = 0; row < groups.size(); row++) {
            states.at<count_state_t>(groups[row]).value++;
        }
        return core::error_t::no_error();
    }

    // ---- finalizes -------------------------------------------------------------------------

    template<typename T>
    struct numeric_finalize_t {
        core::error_t operator()(aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) const {
            auto* data = output.data<T>();
            for (uint64_t row = 0; row < count; row++) {
                const auto& accumulator = states.at<numeric_state_t<T>>(first + row);
                output.set_null(row, !accumulator.has_value);
                data[row] = accumulator.has_value ? accumulator.value : T{};
            }
            return core::error_t::no_error();
        }
    };

    template<typename T>
    struct avg_finalize_t {
        core::error_t operator()(aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) const {
            auto* data = output.data<T>();
            for (uint64_t row = 0; row < count; row++) {
                const auto& accumulator = states.at<avg_state_t<T>>(first + row);
                output.set_null(row, accumulator.count == 0);
                data[row] = accumulator.count == 0
                                ? T{}
                                : static_cast<T>(accumulator.value / static_cast<T>(accumulator.count));
            }
            return core::error_t::no_error();
        }
    };

    core::error_t
    sum_finalize(kernel_context& ctx, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) {
        return arithmetic_dispatch<numeric_finalize_t>(
            output.type(),
            [&ctx] { return unsupported_argument(ctx, "sum"); },
            states,
            first,
            count,
            output);
    }

    core::error_t
    min_max_finalize(kernel_context&, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) {
        // The winners already sit one row per group in a vector of this very type, so emitting
        // them is a copy — whatever the type, and with no accumulator to unpack.
        for (uint64_t row = 0; row < count; row++) {
            const uint64_t group = first + row;
            if (!states.at<min_max_state_t>(group).has_value) {
                output.set_null(row, true);
                continue;
            }
            const uint64_t slot = states.slot_of(group);
            vector_ops::copy(states.values(group), output, slot + 1, slot, row);
        }
        return core::error_t::no_error();
    }

    template<typename out_t, typename value_of_t>
    core::error_t
    emit_averages(aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output, value_of_t value_of) {
        auto* data = output.data<out_t>();
        for (uint64_t row = 0; row < count; row++) {
            const auto& accumulator = states.at<avg_wide_state_t>(first + row);
            output.set_null(row, accumulator.count == 0);
            data[row] = accumulator.count == 0 ? out_t{} : value_of(accumulator);
        }
        return core::error_t::no_error();
    }

    // The unscaled DECIMAL mean, rounded half away from zero.
    int128_t decimal_average(const avg_wide_state_t& accumulator) {
        const int128_t count{accumulator.count};
        int128_t quotient = accumulator.exact / count;
        const int128_t remainder = accumulator.exact % count;
        if ((remainder < 0 ? -remainder : remainder) * 2 >= count) {
            quotient += accumulator.exact < 0 ? -1 : 1;
        }
        return quotient;
    }

    template<typename out_t>
    out_t decimal_average_as(const avg_wide_state_t& accumulator) {
        return static_cast<out_t>(decimal_average(accumulator));
    }

    core::error_t
    avg_finalize(kernel_context& ctx, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) {
        switch (output.type().type()) {
            case logical_type::DOUBLE:
                return emit_averages<double>(states, first, count, output, [](const avg_wide_state_t& a) {
                    return (static_cast<double>(a.exact) + a.inexact) / static_cast<double>(a.count);
                });
            case logical_type::FLOAT:
                return emit_averages<float>(states, first, count, output, [](const avg_wide_state_t& a) {
                    return static_cast<float>(a.inexact / static_cast<double>(a.count));
                });
            case logical_type::DECIMAL:
                switch (output.type().to_physical_type()) {
                    case physical_type::INT16:
                        return emit_averages<int16_t>(states, first, count, output, decimal_average_as<int16_t>);
                    case physical_type::INT32:
                        return emit_averages<int32_t>(states, first, count, output, decimal_average_as<int32_t>);
                    case physical_type::INT64:
                        return emit_averages<int64_t>(states, first, count, output, decimal_average_as<int64_t>);
                    case physical_type::INT128:
                        return emit_averages<int128_t>(states, first, count, output, decimal_average);
                    default:
                        return unsupported_argument(ctx, "avg");
                }
            default:
                break;
        }
        return arithmetic_dispatch<avg_finalize_t>(
            output.type(),
            [&ctx] { return unsupported_argument(ctx, "avg"); },
            states,
            first,
            count,
            output);
    }

    core::error_t
    count_finalize(kernel_context&, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) {
        auto* data = output.data<uint64_t>();
        for (uint64_t row = 0; row < count; row++) {
            output.set_null(row, false);
            data[row] = states.at<count_state_t>(first + row).value;
        }
        return core::error_t::no_error();
    }

    std::pmr::vector<complex_logical_type> numeric_parameters(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types(resource);
        for (auto type : {logical_type::TINYINT,
                          logical_type::SMALLINT,
                          logical_type::INTEGER,
                          logical_type::BIGINT,
                          logical_type::HUGEINT,
                          logical_type::UTINYINT,
                          logical_type::USMALLINT,
                          logical_type::UINTEGER,
                          logical_type::UBIGINT,
                          logical_type::UHUGEINT,
                          logical_type::FLOAT,
                          logical_type::DOUBLE,
                          // Any (width, scale):
                          logical_type::DECIMAL}) {
            types.emplace_back(type);
        }
        return types;
    }

    std::unique_ptr<aggregate_function> make_sum_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn =
            std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots, /*mergeable=*/true);

        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::variable(0, numeric_parameters(resource))},
                               {output_type::computed(&sum_result_type)});
        aggregate_kernel k{std::move(sig), sum_layout, sum_update, sum_finalize};

        register_kernel(resource, fn, std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_min_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn =
            std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots, /*mergeable=*/true);

        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::variable(0)},
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), min_max_layout, min_update, min_max_finalize};

        register_kernel(resource, fn, std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_max_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn =
            std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots, /*mergeable=*/true);

        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::variable(0)},
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), min_max_layout, max_update, min_max_finalize};

        register_kernel(resource, fn, std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_count_func(std::pmr::memory_resource* resource,
                                                        const std::string& name,
                                                        const std::string& short_doc,
                                                        const std::string& full_doc,
                                                        size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name,
                                                       arity::var_args(0),
                                                       doc,
                                                       available_kernel_slots + 1,
                                                       /*mergeable=*/true);

        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::variable(0)},
                               {output_type::fixed(logical_type::UBIGINT)});
        aggregate_kernel k{std::move(sig), count_layout, count_update, count_finalize};
        register_kernel(resource, fn, std::move(k));

        // COUNT(*) — zero-argument kernel
        kernel_signature_t sig_star(function_type_t::aggregate, {}, {output_type::fixed(logical_type::UBIGINT)});
        aggregate_kernel k_star{std::move(sig_star), count_layout, count_star_update, count_finalize};
        register_kernel(resource, fn, std::move(k_star));

        return fn;
    }

    std::unique_ptr<aggregate_function> make_avg_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn =
            std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots, /*mergeable=*/true);

        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::variable(0, numeric_parameters(resource))},
                               {output_type::computed(&avg_result_type)});
        aggregate_kernel k{std::move(sig), avg_layout, avg_update, avg_finalize};

        register_kernel(resource, fn, std::move(k));
        return fn;
    }
} // namespace

namespace components::compute {
    // WARNING: array size, names order and uid has to be the same as in DEFAULT_FUNCTIONS
    void register_default_functions(function_registry_t& r) {
        r.add_builtin(make_sum_func(r.resource(),
                                    "sum",
                                    "Add all numeric values",
                                    "BIGINT over integers, DECIMAL(38, s) over DECIMAL(p, s), else the input type"));
        r.add_builtin(make_min_func(r.resource(),
                                    "min",
                                    "Selects minimal value",
                                    "Results in a single number of the same type as input"));
        r.add_builtin(make_max_func(r.resource(),
                                    "max",
                                    "Selects maximum value",
                                    "Results in a single number of the same type as input"));
        r.add_builtin(
            make_count_func(r.resource(), "count", "Return data size", "Results in a single number of uint64"));
        r.add_builtin(make_avg_func(r.resource(),
                                    "avg",
                                    "Average of all numeric values",
                                    "DOUBLE over integers, else the input type"));
        register_string_functions(r);
        register_expand_functions(r);
        register_math_functions(r);
    }
} // namespace components::compute
