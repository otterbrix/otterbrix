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
            return logical_value_t{raw_sum};
        }
        template<typename T, typename U>
        auto operator()(const vector_t& v, size_t count) const {
            auto raw_sum = T();
            for (size_t i = 0; i < count; i++) {
                raw_sum += T(v.data<U>()[i]);
            }
            return logical_value_t{raw_sum};
        }
    };

    template<>
    struct divide_operator_t<void> {
        template<typename T>
        auto operator()(const logical_value_t& v, size_t count) const {
            return logical_value_t{v.value<T>() / static_cast<T>(count)};
        }
        template<typename T, typename U>
        auto operator()(const logical_value_t& v, size_t count) const {
            return logical_value_t{T(v.value<U>() / static_cast<U>(count))};
        }
    };

    template<>
    struct min_operator_t<void> {
        template<typename T>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{*std::min_element(v.data<T>(), v.data<T>() + count)};
        }
        template<typename T, typename U>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{T(*std::min_element(v.data<U>(), v.data<U>() + count))};
        }
        template<typename T>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{std::min(v1.value<T>(), v2.value<T>())};
        }
        template<typename T, typename U>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{T(std::min(v1.value<U>(), v2.value<U>()))};
        }
    };

    template<>
    struct max_operator_t<void> {
        template<typename T>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{*std::max_element(v.data<T>(), v.data<T>() + count)};
        }
        template<typename T, typename U>
        auto operator()(const vector_t& v, size_t count) const {
            return logical_value_t{T(*std::max_element(v.data<U>(), v.data<U>() + count))};
        }
        template<typename T>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{std::max(v1.value<T>(), v2.value<T>())};
        }
        template<typename T, typename U>
        auto operator()(const logical_value_t& v1, const logical_value_t& v2) const {
            if (v2.type().type() == logical_type::NA) {
                return v1;
            }
            return logical_value_t{T(std::max(v1.value<U>(), v2.value<U>()))};
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
            case logical_type::TIMESTAMP_SEC:
                return op.template operator()<std::chrono::seconds, int64_t>(v, count);
            case logical_type::TIMESTAMP_MS:
                return op.template operator()<std::chrono::milliseconds, int64_t>(v, count);
            case logical_type::TIMESTAMP_US:
                return op.template operator()<std::chrono::microseconds, int64_t>(v, count);
            case logical_type::TIMESTAMP_NS:
                return op.template operator()<std::chrono::nanoseconds, int64_t>(v, count);
            case logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                auto int_sum = op.template operator()<int64_t>(v, count);
                int_sum = logical_value_t::create_decimal(
                    int_sum.template value<int64_t>(),
                    static_cast<decimal_logical_type_extension*>(v.type().extension())->width(),
                    static_cast<decimal_logical_type_extension*>(v.type().extension())->scale());
                return int_sum;
            }
            case logical_type::FLOAT:
                return op.template operator()<float>(v, count);
            case logical_type::DOUBLE:
                return op.template operator()<double>(v, count);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return logical_value_t(nullptr);
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
            case logical_type::TIMESTAMP_SEC:
                return op.template operator()<std::chrono::seconds, int64_t>(v1, v2);
            case logical_type::TIMESTAMP_MS:
                return op.template operator()<std::chrono::milliseconds, int64_t>(v1, v2);
            case logical_type::TIMESTAMP_US:
                return op.template operator()<std::chrono::microseconds, int64_t>(v1, v2);
            case logical_type::TIMESTAMP_NS:
                return op.template operator()<std::chrono::nanoseconds, int64_t>(v1, v2);
            case logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                auto int_sum = op.template operator()<int64_t>(v1, v2);
                int_sum = logical_value_t::create_decimal(
                    int_sum.template value<int64_t>(),
                    static_cast<decimal_logical_type_extension*>(v1.type().extension())->width(),
                    static_cast<decimal_logical_type_extension*>(v1.type().extension())->scale());
                return int_sum;
            }
            case logical_type::FLOAT:
                return op.template operator()<float>(v1, v2);
            case logical_type::DOUBLE:
                return op.template operator()<double>(v1, v2);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return logical_value_t(nullptr);
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
            case logical_type::TIMESTAMP_SEC:
                return op.template operator()<std::chrono::seconds, int64_t>(v, count);
            case logical_type::TIMESTAMP_MS:
                return op.template operator()<std::chrono::milliseconds, int64_t>(v, count);
            case logical_type::TIMESTAMP_US:
                return op.template operator()<std::chrono::microseconds, int64_t>(v, count);
            case logical_type::TIMESTAMP_NS:
                return op.template operator()<std::chrono::nanoseconds, int64_t>(v, count);
            case logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                auto int_sum = op.template operator()<int64_t>(v, count);
                int_sum = logical_value_t::create_decimal(
                    int_sum.template value<int64_t>(),
                    static_cast<decimal_logical_type_extension*>(v.type().extension())->width(),
                    static_cast<decimal_logical_type_extension*>(v.type().extension())->scale());
                return int_sum;
            }
            case logical_type::FLOAT:
                return op.template operator()<float>(v, count);
            case logical_type::DOUBLE:
                return op.template operator()<double>(v, count);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return logical_value_t(nullptr);
    }

    logical_value_t sum(const vector_t& v, size_t count) { return operator_switch<sum_operator_t>(v, count); }

    logical_value_t min(const vector_t& v, size_t count) { return operator_switch<min_operator_t>(v, count); }

    logical_value_t max(const vector_t& v, size_t count) { return operator_switch<max_operator_t>(v, count); }

    struct sum_kernel_state : kernel_state {
        logical_value_t value;
    };

    static compute_result<kernel_state_ptr> sum_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<sum_kernel_state>();
        c->value = logical_value_t{};
        return compute_result<kernel_state_ptr>(std::move(c));
    }

    static compute_status sum_consume(kernel_context& ctx, const data_chunk_t& in, size_t exec_length) {
        auto* acc = static_cast<sum_kernel_state*>(ctx.state());
        acc->value = sum(in.data[0], exec_length);
        return compute_status::ok();
    }

    static compute_status sum_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
        static_cast<sum_kernel_state&>(into).value = logical_value_t::sum(static_cast<sum_kernel_state&>(from).value,
                                                                          static_cast<sum_kernel_state&>(into).value);
        return compute_status::ok();
    }

    static compute_status sum_finalize(kernel_context& ctx, datum_t& out) {
        vector_t vec(ctx.exec_context().resource(), static_cast<sum_kernel_state*>(ctx.state())->value.type(), 1);
        vec.set_value(0, static_cast<sum_kernel_state*>(ctx.state())->value);
        out.data.emplace_back(std::move(vec));
        out.set_cardinality(out.size() + 1);
        return compute_status::ok();
    }

    struct min_kernel_state : kernel_state {
        logical_value_t value;
    };

    static compute_result<kernel_state_ptr> min_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<min_kernel_state>();
        c->value = logical_value_t{};
        return compute_result<kernel_state_ptr>(std::move(c));
    }

    static compute_status min_consume(kernel_context& ctx, const data_chunk_t& in, size_t exec_length) {
        auto* acc = static_cast<min_kernel_state*>(ctx.state());
        acc->value = min(in.data[0], exec_length);
        return compute_status::ok();
    }

    static compute_status min_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
        static_cast<min_kernel_state&>(into).value =
            operator_switch<min_operator_t>(static_cast<min_kernel_state&>(from).value,
                                            static_cast<min_kernel_state&>(into).value);
        return compute_status::ok();
    }

    static compute_status min_finalize(kernel_context& ctx, datum_t& out) {
        vector_t vec(ctx.exec_context().resource(), static_cast<min_kernel_state*>(ctx.state())->value.type(), 1);
        vec.set_value(0, static_cast<min_kernel_state*>(ctx.state())->value);
        out.data.emplace_back(std::move(vec));
        out.set_cardinality(out.size() + 1);
        return compute_status::ok();
    }

    struct max_kernel_state : kernel_state {
        logical_value_t value;
    };

    static compute_result<kernel_state_ptr> max_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<max_kernel_state>();
        c->value = logical_value_t{};
        return compute_result<kernel_state_ptr>(std::move(c));
    }

    static compute_status max_consume(kernel_context& ctx, const data_chunk_t& in, size_t exec_length) {
        auto* acc = static_cast<max_kernel_state*>(ctx.state());
        acc->value = max(in.data[0], exec_length);
        return compute_status::ok();
    }

    static compute_status max_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
        static_cast<max_kernel_state&>(into).value =
            operator_switch<max_operator_t>(static_cast<max_kernel_state&>(from).value,
                                            static_cast<max_kernel_state&>(into).value);
        return compute_status::ok();
    }

    static compute_status max_finalize(kernel_context& ctx, datum_t& out) {
        vector_t vec(ctx.exec_context().resource(), static_cast<max_kernel_state*>(ctx.state())->value.type(), 1);
        vec.set_value(0, static_cast<max_kernel_state*>(ctx.state())->value);
        out.data.emplace_back(std::move(vec));
        out.set_cardinality(out.size() + 1);
        return compute_status::ok();
    }

    struct count_kernel_state : kernel_state {
        size_t value;
    };

    static compute_result<kernel_state_ptr> count_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<count_kernel_state>();
        c->value = size_t{0};
        return compute_result<kernel_state_ptr>(std::move(c));
    }

    static compute_status count_consume(kernel_context& ctx, const data_chunk_t& in, size_t) {
        auto* acc = static_cast<count_kernel_state*>(ctx.state());
        acc->value = in.size();
        return compute_status::ok();
    }

    static compute_status count_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
        static_cast<count_kernel_state&>(into).value += static_cast<count_kernel_state&>(from).value;
        return compute_status::ok();
    }

    static compute_status count_finalize(kernel_context& ctx, datum_t& out) {
        vector_t vec(ctx.exec_context().resource(), logical_type::UBIGINT, 1);
        vec.set_value(0, logical_value_t{static_cast<count_kernel_state*>(ctx.state())->value});
        out.data.emplace_back(std::move(vec));
        out.set_cardinality(out.size() + 1);
        return compute_status::ok();
    }

    struct avg_kernel_state : kernel_state {
        size_t count;
        logical_value_t value;
    };

    static compute_result<kernel_state_ptr> avg_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<avg_kernel_state>();
        c->count = size_t{0};
        c->value = logical_value_t{};
        return compute_result<kernel_state_ptr>(std::move(c));
    }

    static compute_status avg_consume(kernel_context& ctx, const data_chunk_t& in, size_t exec_length) {
        auto* acc = static_cast<avg_kernel_state*>(ctx.state());
        acc->count = in.size();
        acc->value = sum(in.data[0], exec_length);
        return compute_status::ok();
    }

    static compute_status avg_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
        static_cast<avg_kernel_state&>(into).count += static_cast<avg_kernel_state&>(from).count;
        static_cast<avg_kernel_state&>(into).value = logical_value_t::sum(static_cast<avg_kernel_state&>(from).value,
                                                                          static_cast<avg_kernel_state&>(into).value);
        return compute_status::ok();
    }

    static compute_status avg_finalize(kernel_context& ctx, datum_t& out) {
        vector_t vec(ctx.exec_context().resource(), static_cast<avg_kernel_state*>(ctx.state())->value.type(), 1);
        vec.set_value(0,
                      operator_switch<divide_operator_t>(static_cast<avg_kernel_state*>(ctx.state())->value,
                                                         static_cast<avg_kernel_state*>(ctx.state())->count));
        out.data.emplace_back(std::move(vec));
        out.set_cardinality(out.size() + 1);
        return compute_status::ok();
    }

    std::unique_ptr<aggregate_function> make_sum_func(const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig({numeric_types_matcher()}, output_type::computed(same_type_resolver()));
        aggregate_kernel k{std::move(sig), sum_init, sum_consume, sum_merge, sum_finalize};

        fn->add_kernel(std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_min_func(const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig({always_true_type_matcher()}, output_type::computed(same_type_resolver()));
        aggregate_kernel k{std::move(sig), min_init, min_consume, min_merge, min_finalize};

        fn->add_kernel(std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_max_func(const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig({always_true_type_matcher()}, output_type::computed(same_type_resolver()));
        aggregate_kernel k{std::move(sig), max_init, max_consume, max_merge, max_finalize};

        fn->add_kernel(std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_count_func(const std::string& name,
                                                        const std::string& short_doc,
                                                        const std::string& full_doc,
                                                        size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig({always_true_type_matcher()}, output_type::fixed(logical_type::UBIGINT));
        aggregate_kernel k{std::move(sig), count_init, count_consume, count_merge, count_finalize};

        fn->add_kernel(std::move(k));
        return fn;
    }

    std::unique_ptr<aggregate_function> make_avg_func(const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc,
                                                      size_t available_kernel_slots = 1) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<aggregate_function>(name, arity::unary(), doc, available_kernel_slots);

        kernel_signature_t sig({numeric_types_matcher()}, output_type::computed(same_type_resolver()));
        aggregate_kernel k{std::move(sig), avg_init, avg_consume, avg_merge, avg_finalize};

        fn->add_kernel(std::move(k));
        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: array size, names order and uid has to be the same as in DEFAULT_FUNCTIONS
    void register_default_functions(function_registry_t& r) {
        r.add_function(
            make_sum_func("sum", "Add all numeric values", "Results in a single number of the same type as input"));
        r.add_function(
            make_min_func("min", "Selects minimal value", "Results in a single number of the same type as input"));
        r.add_function(
            make_max_func("max", "Selects maximum value", "Results in a single number of the same type as input"));
        r.add_function(make_count_func("count", "Return data size", "Results in a single number of uint64"));
        r.add_function(
            make_avg_func("avg", "Return data size", "Results in a single number of the same type as input"));
    }

} // namespace components::compute
