#include "../function.hpp"
#include <components/types/logical_value.hpp>

#include <cassert>
#include <string_view>
#include <tuple>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {
    void register_kernel(std::pmr::memory_resource* resource, auto& fn, auto kernel) {
        auto added = fn->add_kernel(resource, std::move(kernel));
        assert(!added.contains_error() && "aggregate kernel must fit the declared slots and arity");
        std::ignore = added;
    }

    template<typename T>
    concept addable = requires(T& a, const T& b) { a += b; };
    template<typename T>
    concept comparable = requires(const T& a, const T& b) { a < b; };
    template<typename T>
    concept dividable = requires(const T& a, const T& b) { a / b; };

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

    struct count_state_t {
        uint64_t value{0};
    };

    template<template<typename> class op_t, typename fallback_t, typename... args_t>
    auto arithmetic_dispatch(const complex_logical_type& type,
                             fallback_t fallback,
                             args_t&&... args) -> decltype(fallback()) {
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
    auto
    ordered_dispatch(const complex_logical_type& type, fallback_t fallback, args_t&&... args) -> decltype(fallback()) {
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
        return arithmetic_dispatch<numeric_layout_t>(inputs.front(), no_layout);
    }

    // MIN/MAX over text. A vector stores its strings as string_view into an auxiliary buffer that
    // belongs to the input chunk, so the accumulator must own its copy: the winning row's chunk is
    // long gone by the time finalize runs.
    struct string_state_t {
        explicit string_state_t(std::pmr::memory_resource* resource)
            : value(resource) {}
        std::pmr::string value;
        bool has_value{false};
    };

    aggregate_state_layout_t min_max_layout(const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1) {
            return {};
        }
        if (inputs.front().to_physical_type() == physical_type::STRING) {
            return aggregate_state_of<string_state_t>();
        }
        return ordered_dispatch<numeric_layout_t>(inputs.front(), no_layout);
    }

    aggregate_state_layout_t avg_layout(const std::pmr::vector<complex_logical_type>& inputs) {
        if (inputs.size() != 1) {
            return {};
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
    struct min_update_t {
        core::error_t
        operator()(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states) const {
            const auto* data = input.data<T>();
            const bool all_valid = input.validity().all_valid();
            for (uint64_t row = 0; row < groups.size(); row++) {
                if (!all_valid && input.is_null(row)) {
                    continue;
                }
                auto& accumulator = states.at<numeric_state_t<T>>(groups[row]);
                if (!accumulator.has_value || data[row] < accumulator.value) {
                    accumulator.value = data[row];
                    accumulator.has_value = true;
                }
            }
            return core::error_t::no_error();
        }
    };

    template<typename T>
    struct max_update_t {
        core::error_t
        operator()(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states) const {
            const auto* data = input.data<T>();
            const bool all_valid = input.validity().all_valid();
            for (uint64_t row = 0; row < groups.size(); row++) {
                if (!all_valid && input.is_null(row)) {
                    continue;
                }
                auto& accumulator = states.at<numeric_state_t<T>>(groups[row]);
                if (!accumulator.has_value || accumulator.value < data[row]) {
                    accumulator.value = data[row];
                    accumulator.has_value = true;
                }
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
        return arithmetic_dispatch<sum_update_t>(
            column.type(),
            [&ctx] { return unsupported_argument(ctx, "sum"); },
            column,
            groups,
            states);
    }

    // Shared by min and max: `keep` decides whether the incoming string replaces the accumulator.
    template<typename keep_t>
    core::error_t
    string_min_max_update(const vector_t& input, core::span<const uint32_t> groups, aggregate_states_t states) {
        const auto* data = input.data<std::string_view>();
        const bool all_valid = input.validity().all_valid();
        const keep_t keep{};
        for (uint64_t row = 0; row < groups.size(); row++) {
            if (!all_valid && input.is_null(row)) {
                continue;
            }
            auto& accumulator = states.at<string_state_t>(groups[row]);
            const std::string_view candidate = data[row];
            if (!accumulator.has_value || keep(candidate, std::string_view{accumulator.value})) {
                accumulator.value.assign(candidate);
                accumulator.has_value = true;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t min_update(kernel_context& ctx,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        const auto& column = input.data.front();
        if (column.type().to_physical_type() == physical_type::STRING) {
            return string_min_max_update<std::less<std::string_view>>(column, groups, states);
        }
        return ordered_dispatch<min_update_t>(
            column.type(),
            [&ctx] { return unsupported_argument(ctx, "min"); },
            column,
            groups,
            states);
    }

    core::error_t max_update(kernel_context& ctx,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        const auto& column = input.data.front();
        if (column.type().to_physical_type() == physical_type::STRING) {
            return string_min_max_update<std::greater<std::string_view>>(column, groups, states);
        }
        return ordered_dispatch<max_update_t>(
            column.type(),
            [&ctx] { return unsupported_argument(ctx, "max"); },
            column,
            groups,
            states);
    }

    core::error_t avg_update(kernel_context& ctx,
                             const data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) {
        const auto& column = input.data.front();
        return arithmetic_dispatch<avg_update_t>(
            column.type(),
            [&ctx] { return unsupported_argument(ctx, "avg"); },
            column,
            groups,
            states);
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
    min_max_finalize(kernel_context& ctx, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) {
        if (output.type().to_physical_type() == physical_type::STRING) {
            // set_value copies the bytes into the output vector's own string buffer, so the
            // accumulator's storage is free to die with the arena.
            for (uint64_t row = 0; row < count; row++) {
                const auto& accumulator = states.at<string_state_t>(first + row);
                if (!accumulator.has_value) {
                    output.set_null(row, true);
                    continue;
                }
                output.set_value(row, std::string_view{accumulator.value});
            }
            return core::error_t::no_error();
        }
        return ordered_dispatch<numeric_finalize_t>(
            output.type(),
            [&ctx] { return unsupported_argument(ctx, "min/max"); },
            states,
            first,
            count,
            output);
    }

    core::error_t
    avg_finalize(kernel_context& ctx, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) {
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
                               {output_type::computed(same_type_resolver(0))});
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
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), avg_layout, avg_update, avg_finalize};

        register_kernel(resource, fn, std::move(k));
        return fn;
    }
} // namespace

namespace components::compute {
    // WARNING: array size, names order and uid has to be the same as in DEFAULT_FUNCTIONS
    void register_default_functions(function_registry_t& r) {
        (void) r.add_function(make_sum_func(r.resource(),
                                            "sum",
                                            "Add all numeric values",
                                            "Results in a single number of the same type as input"));
        (void) r.add_function(make_min_func(r.resource(),
                                            "min",
                                            "Selects minimal value",
                                            "Results in a single number of the same type as input"));
        (void) r.add_function(make_max_func(r.resource(),
                                            "max",
                                            "Selects maximum value",
                                            "Results in a single number of the same type as input"));
        (void) r.add_function(
            make_count_func(r.resource(), "count", "Return data size", "Results in a single number of uint64"));
        (void) r.add_function(make_avg_func(r.resource(),
                                            "avg",
                                            "Return data size",
                                            "Results in a single number of the same type as input"));
        register_string_functions(r);
        register_expand_functions(r);
        register_math_functions(r);
    }
} // namespace components::compute
