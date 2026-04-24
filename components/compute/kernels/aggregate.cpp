#include "../function.hpp"
#include <components/types/logical_value.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    template<typename T = void>
    struct sum_operator_t;
    template<typename T = void>
    struct divide_operator_t;
    template<typename T = void>
    struct min_operator_t;
    template<typename T = void>
    struct max_operator_t;

    template<>
    struct sum_operator_t<void> {
        template<typename T>
        auto operator()(const vector_t& v, size_t count) const {
            auto raw_sum = T();
            for (size_t i = 0; i < count; i++) {
                raw_sum += v.data<T>()[i];
            }
            return logical_value_t{v.resource(), raw_sum};
        }
        template<typename T, typename U>
        auto operator()(const vector_t& v, size_t count) const {
            auto raw_sum = T();
            for (size_t i = 0; i < count; i++) {
                raw_sum += T(v.data<U>()[i]);
            }
            return logical_value_t{v.resource(), raw_sum};
        }
    };

    template<>
    struct divide_operator_t<void> {
        template<typename T>
        auto operator()(const logical_value_t& v, size_t count) const {
            return logical_value_t{v.resource(), v.value<T>() / static_cast<T>(count)};
        }
        template<typename T, typename U>
        auto operator()(const logical_value_t& v, size_t count) const {
            return logical_value_t{v.resource(), T(v.value<U>() / static_cast<U>(count))};
        }
    };

    template<>
    struct min_operator_t<void> {
        template<typename T>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{v.resource(), *std::min_element(v.data<T>(), v.data<T>() + count)};
        }
        template<typename T, typename U>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{v.resource(), T(*std::min_element(v.data<U>(), v.data<U>() + count))};
        }
        template<typename T>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{v1.resource(), std::min(v1.value<T>(), v2.value<T>())};
        }
        template<typename T, typename U>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{v1.resource(), T(std::min(v1.value<U>(), v2.value<U>()))};
        }
    };

    template<>
    struct max_operator_t<void> {
        template<typename T>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{v.resource(), *std::max_element(v.data<T>(), v.data<T>() + count)};
        }
        template<typename T, typename U>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{v.resource(), T(*std::max_element(v.data<U>(), v.data<U>() + count))};
        }
        template<typename T>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{v1.resource(), std::max(v1.value<T>(), v2.value<T>())};
        }
        template<typename T, typename U>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{v1.resource(), T(std::max(v1.value<U>(), v2.value<U>()))};
        }
    };

    template<template<typename...> class OP>
    logical_value_t operator_switch(const vector_t& v, size_t count) {
        OP op{};
        switch (v.type().type()) {
            case logical_type::BOOLEAN:
                return op.template operator()<bool>(v, count);
            case logical_type::TINYINT:
                return op.template operator()<int8_t>(v, count);
            case logical_type::SMALLINT:
                return op.template operator()<int16_t>(v, count);
            case logical_type::INTEGER:
                return op.template operator()<int32_t>(v, count);
            case logical_type::BIGINT:
                return op.template operator()<int64_t>(v, count);
            case logical_type::HUGEINT:
                return op.template operator()<int128_t>(v, count);
            case logical_type::UTINYINT:
                return op.template operator()<uint8_t>(v, count);
            case logical_type::USMALLINT:
                return op.template operator()<uint16_t>(v, count);
            case logical_type::UINTEGER:
                return op.template operator()<uint32_t>(v, count);
            case logical_type::UBIGINT:
                return op.template operator()<uint64_t>(v, count);
            case logical_type::UHUGEINT:
                return op.template operator()<uint128_t>(v, count);
            case logical_type::DECIMAL: {
                // stored as int???_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                switch (v.type().to_physical_type()) {
                    case physical_type::INT16: {
                        auto int_sum = op.template operator()<int16_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int16_t>());
                    }
                    case physical_type::INT32: {
                        auto int_sum = op.template operator()<int32_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int32_t>());
                    }
                    case physical_type::INT64: {
                        auto int_sum = op.template operator()<int64_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int64_t>());
                    }
                    case physical_type::INT128: {
                        auto int_sum = op.template operator()<int128_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int128_t>());
                    }
                    default:
                        throw std::runtime_error("operators::aggregate::sum encountered incorrect decimal type");
                }
            }
            case logical_type::FLOAT:
                return op.template operator()<float>(v, count);
            case logical_type::DOUBLE:
                return op.template operator()<double>(v, count);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return logical_value_t(std::pmr::null_memory_resource(), logical_type::NA);
    }

    template<template<typename...> class OP>
    logical_value_t operator_switch(const logical_value_t& v1, const logical_value_t& v2) {
        OP op{};
        switch (v1.type().type()) {
            case logical_type::BOOLEAN:
                return op.template operator()<bool>(v1, v2);
            case logical_type::TINYINT:
                return op.template operator()<int8_t>(v1, v2);
            case logical_type::SMALLINT:
                return op.template operator()<int16_t>(v1, v2);
            case logical_type::INTEGER:
                return op.template operator()<int32_t>(v1, v2);
            case logical_type::BIGINT:
                return op.template operator()<int64_t>(v1, v2);
            case logical_type::HUGEINT:
                return op.template operator()<int128_t>(v1, v2);
            case logical_type::UTINYINT:
                return op.template operator()<uint8_t>(v1, v2);
            case logical_type::USMALLINT:
                return op.template operator()<uint16_t>(v1, v2);
            case logical_type::UINTEGER:
                return op.template operator()<uint32_t>(v1, v2);
            case logical_type::UBIGINT:
                return op.template operator()<uint64_t>(v1, v2);
            case logical_type::UHUGEINT:
                return op.template operator()<uint128_t>(v1, v2);
            case logical_type::DECIMAL: {
                // stored as int???_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                switch (v1.type().to_physical_type()) {
                    case physical_type::INT16: {
                        auto int_sum = op.template operator()<int16_t>(v1, v2);
                        return logical_value_t::create_decimal(v1.resource(),
                                                               v1.type(),
                                                               int_sum.template value<int16_t>());
                    }
                    case physical_type::INT32: {
                        auto int_sum = op.template operator()<int32_t>(v1, v2);
                        return logical_value_t::create_decimal(v1.resource(),
                                                               v1.type(),
                                                               int_sum.template value<int32_t>());
                    }
                    case physical_type::INT64: {
                        auto int_sum = op.template operator()<int64_t>(v1, v2);
                        return logical_value_t::create_decimal(v1.resource(),
                                                               v1.type(),
                                                               int_sum.template value<int64_t>());
                    }
                    case physical_type::INT128: {
                        auto int_sum = op.template operator()<int128_t>(v1, v2);
                        return logical_value_t::create_decimal(v1.resource(),
                                                               v1.type(),
                                                               int_sum.template value<int128_t>());
                    }
                    default:
                        throw std::runtime_error("operators::aggregate::sum encountered incorrect decimal type");
                }
            }
            case logical_type::FLOAT:
                return op.template operator()<float>(v1, v2);
            case logical_type::DOUBLE:
                return op.template operator()<double>(v1, v2);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return logical_value_t(std::pmr::null_memory_resource(), logical_type::NA);
    }

    template<template<typename...> class OP>
    logical_value_t operator_switch(const logical_value_t& v, size_t count) {
        OP op{};
        switch (v.type().type()) {
            case logical_type::BOOLEAN:
                return op.template operator()<bool>(v, count);
            case logical_type::TINYINT:
                return op.template operator()<int8_t>(v, count);
            case logical_type::SMALLINT:
                return op.template operator()<int16_t>(v, count);
            case logical_type::INTEGER:
                return op.template operator()<int32_t>(v, count);
            case logical_type::BIGINT:
                return op.template operator()<int64_t>(v, count);
            case logical_type::HUGEINT:
                return op.template operator()<int128_t>(v, count);
            case logical_type::UTINYINT:
                return op.template operator()<uint8_t>(v, count);
            case logical_type::USMALLINT:
                return op.template operator()<uint16_t>(v, count);
            case logical_type::UINTEGER:
                return op.template operator()<uint32_t>(v, count);
            case logical_type::UBIGINT:
                return op.template operator()<uint64_t>(v, count);
            case logical_type::UHUGEINT:
                return op.template operator()<uint128_t>(v, count);
            case logical_type::DECIMAL: {
                // stored as int???_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                switch (v.type().to_physical_type()) {
                    case physical_type::INT16: {
                        auto int_sum = op.template operator()<int16_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int16_t>());
                    }
                    case physical_type::INT32: {
                        auto int_sum = op.template operator()<int32_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int32_t>());
                    }
                    case physical_type::INT64: {
                        auto int_sum = op.template operator()<int64_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int64_t>());
                    }
                    case physical_type::INT128: {
                        auto int_sum = op.template operator()<int128_t>(v, count);
                        return logical_value_t::create_decimal(v.resource(),
                                                               v.type(),
                                                               int_sum.template value<int128_t>());
                    }
                    default:
                        throw std::runtime_error("operators::aggregate::sum encountered incorrect decimal type");
                }
            }
            case logical_type::FLOAT:
                return op.template operator()<float>(v, count);
            case logical_type::DOUBLE:
                return op.template operator()<double>(v, count);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return logical_value_t(std::pmr::null_memory_resource(), logical_type::NA);
    }

    logical_value_t sum(const vector_t& v, size_t count) { return operator_switch<sum_operator_t>(v, count); }

    logical_value_t min(const vector_t& v, size_t count) { return operator_switch<min_operator_t>(v, count); }

    logical_value_t max(const vector_t& v, size_t count) { return operator_switch<max_operator_t>(v, count); }

    struct sum_kernel_state : kernel_state {
        logical_value_t value{std::pmr::null_memory_resource(), logical_type::NA};
    };

    static core::result_wrapper_t<kernel_state_ptr> sum_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<sum_kernel_state>();
        c->value = logical_value_t{std::pmr::null_memory_resource(), logical_type::NA};
        return c;
    }

    static core::error_t sum_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<sum_kernel_state*>(ctx.state());
        acc->value = sum(in.data[0], in.size());
        return core::error_t::no_error();
    }

    static core::error_t sum_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
        ctx.batch_results.push_back(static_cast<sum_kernel_state&>(from).value);
        return core::error_t::no_error();
    }

    static core::error_t sum_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    struct min_kernel_state : kernel_state {
        logical_value_t value{std::pmr::null_memory_resource(), logical_type::NA};
    };

    static core::result_wrapper_t<kernel_state_ptr> min_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<min_kernel_state>();
        c->value = logical_value_t{std::pmr::null_memory_resource(), logical_type::NA};
        return c;
    }

    static core::error_t min_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<min_kernel_state*>(ctx.state());
        acc->value = min(in.data[0], in.size());
        return core::error_t::no_error();
    }

    static core::error_t min_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
        ctx.batch_results.push_back(static_cast<min_kernel_state&>(from).value);
        return core::error_t::no_error();
    }

    static core::error_t min_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    struct max_kernel_state : kernel_state {
        logical_value_t value{std::pmr::null_memory_resource(), logical_type::NA};
    };

    static core::result_wrapper_t<kernel_state_ptr> max_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<max_kernel_state>();
        c->value = logical_value_t{std::pmr::null_memory_resource(), logical_type::NA};
        return c;
    }

    static core::error_t max_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<max_kernel_state*>(ctx.state());
        acc->value = max(in.data[0], in.size());
        return core::error_t::no_error();
    }

    static core::error_t max_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
        ctx.batch_results.push_back(static_cast<max_kernel_state&>(from).value);
        return core::error_t::no_error();
    }

    static core::error_t max_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    struct count_kernel_state : kernel_state {
        size_t value;
    };

    static core::result_wrapper_t<kernel_state_ptr> count_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<count_kernel_state>();
        c->value = size_t{0};
        return c;
    }

    static core::error_t count_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<count_kernel_state*>(ctx.state());
        acc->value = 0;
        for (size_t i = 0; i < in.size(); i++) {
            acc->value += !in.data[0].is_null(i);
        }
        return core::error_t::no_error();
    }

    static core::error_t count_consume_empty(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<count_kernel_state*>(ctx.state());
        acc->value = in.size();
        return core::error_t::no_error();
    }

    static core::error_t count_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
        ctx.batch_results.emplace_back(ctx.batch_results.get_allocator().resource(),
                                       static_cast<count_kernel_state&>(from).value);
        return core::error_t::no_error();
    }

    static core::error_t count_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    struct avg_kernel_state : kernel_state {
        size_t count;
        logical_value_t value{std::pmr::null_memory_resource(), logical_type::NA};
    };

    static core::result_wrapper_t<kernel_state_ptr> avg_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<avg_kernel_state>();
        c->count = size_t{0};
        c->value = logical_value_t{std::pmr::null_memory_resource(), logical_type::NA};
        return c;
    }

    static core::error_t avg_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<avg_kernel_state*>(ctx.state());
        acc->count = in.size();
        acc->value = sum(in.data[0], in.size());
        return core::error_t::no_error();
    }

    static core::error_t avg_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
        auto& s = static_cast<avg_kernel_state&>(from);
        ctx.batch_results.push_back(operator_switch<divide_operator_t>(s.value, s.count));
        return core::error_t::no_error();
    }

    static core::error_t avg_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    std::unique_ptr<aggregate_function> make_sum_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig(function_type_t::aggregate,
                               {numeric_types_matcher()},
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), sum_init, sum_consume, sum_merge, sum_finalize};

        fn->add_kernel(resource, std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_min_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig(function_type_t::aggregate,
                               {always_true_type_matcher()},
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), min_init, min_consume, min_merge, min_finalize};

        fn->add_kernel(resource, std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_max_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig(function_type_t::aggregate,
                               {always_true_type_matcher()},
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), max_init, max_consume, max_merge, max_finalize};

        fn->add_kernel(resource, std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_count_func(std::pmr::memory_resource* resource,
                                                        const std::string& name,
                                                        const std::string& short_doc,
                                                        const std::string& full_doc,
                                                        size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::var_args(0), doc, available_kernel_slots + 1);

        kernel_signature_t sig(function_type_t::aggregate,
                               {always_true_type_matcher()},
                               {output_type::fixed(logical_type::UBIGINT)});
        aggregate_kernel k{std::move(sig), count_init, count_consume, count_merge, count_finalize};
        fn->add_kernel(resource, std::move(k));

        // COUNT(*) — zero-argument kernel
        kernel_signature_t sig_star(function_type_t::aggregate, {}, {output_type::fixed(logical_type::UBIGINT)});
        aggregate_kernel k_star{std::move(sig_star), count_init, count_consume_empty, count_merge, count_finalize};
        fn->add_kernel(resource, std::move(k_star));

        return fn;
    }

    std::unique_ptr<aggregate_function> make_avg_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig(function_type_t::aggregate,
                               {numeric_types_matcher()},
                               {output_type::computed(same_type_resolver(0))});
        aggregate_kernel k{std::move(sig), avg_init, avg_consume, avg_merge, avg_finalize};

        fn->add_kernel(resource, std::move(k));
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
    }

} // namespace components::compute
