#include <catch2/catch.hpp>
#include <components/compute/function.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

constexpr int MAGIC_MULTIPLIER = 1234;
static const auto TEST_ERROR = compute_status::execution_error("error!");

struct test_options : function_options {
    int multiplier;
};

struct counters : kernel_state {
    int multiplier = 0;
    int exec_called = 0;
};

static compute_result<kernel_state_ptr> vector_init(kernel_context&, kernel_init_args args) {
    auto* opts = static_cast<const test_options*>(args.options);
    REQUIRE(opts != nullptr);

    auto c = std::make_unique<counters>();
    c->multiplier = opts->multiplier;
    return compute_result<kernel_state_ptr>(std::move(c));
}

static compute_status vector_exec(kernel_context& ctx, const data_chunk_t& in, size_t, vector_t& out) {
    auto* c = static_cast<counters*>(ctx.state());
    c->exec_called++;

    for (size_t i = 0; i < in.data.size(); ++i) {
        auto cnt = logical_value_t(in.data[0].data<int>()[i] * c->multiplier);
        out.set_value(i, cnt);
    }
    return compute_status::ok();
}

static compute_status vector_finalize(kernel_context& ctx, size_t, datum_t&) {
    auto* c = static_cast<counters*>(ctx.state());
    REQUIRE(c->exec_called);                    // at least one call
    REQUIRE(c->multiplier == MAGIC_MULTIPLIER); // init was called with function_options
    return compute_status::ok();
}

struct agg_counter : kernel_state {
    int value;
};

static compute_result<kernel_state_ptr> agg_init(kernel_context&, kernel_init_args) {
    auto c = std::make_unique<agg_counter>();
    c->value = 10;
    return compute_result<kernel_state_ptr>(std::move(c));
}

static compute_status agg_consume(kernel_context& ctx, const data_chunk_t& in, size_t exec_length) {
    auto* acc = static_cast<agg_counter*>(ctx.state());
    for (size_t i = 0; i < exec_length; ++i) {
        acc->value += in.data[0].data<int>()[i];
    }
    return compute_status::ok();
}

static compute_status agg_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
    static_cast<agg_counter&>(into).value += static_cast<agg_counter&>(from).value;
    return compute_status::ok();
}

static compute_status agg_finalize(kernel_context& ctx, datum_t& out) {
    vector_t vec(ctx.exec_context().resource(), logical_type::INTEGER, 2);
    vec.set_value(0, logical_value_t{static_cast<agg_counter*>(ctx.state())->value});
    out.data.emplace_back(std::move(vec));
    return compute_status::ok();
}

static compute_status vector_exec_fail(kernel_context&, const data_chunk_t&, size_t, vector_t&) { return TEST_ERROR; }

static compute_status agg_consume_fail(kernel_context&, const data_chunk_t&, size_t) { return TEST_ERROR; }

inline function_doc function_doc_with_options() { return function_doc{"", "", {}, true}; }

TEST_CASE("components::compute::vector::single") {
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_test", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE(fn->add_kernel(std::move(k)));

    data_chunk_t chunk(std::pmr::get_default_resource(), {logical_type::INTEGER});
    chunk.set_value(0, 0, logical_value_t(10));

    auto res = fn->execute(chunk, 1, &opts);
    REQUIRE(res);
    REQUIRE(res.value().data[0].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::vector::batch") {
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_batch", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE(fn->add_kernel(std::move(k)));

    data_chunk_t c1(std::pmr::get_default_resource(), {logical_type::INTEGER});
    c1.set_value(0, 0, logical_value_t(1));

    data_chunk_t c2(std::pmr::get_default_resource(), {logical_type::INTEGER});
    c2.set_value(0, 0, logical_value_t(10));

    std::vector<data_chunk_t> batch;
    batch.emplace_back(std::move(c1));
    batch.emplace_back(std::move(c2));

    auto res = fn->execute(batch, 1, &opts);
    REQUIRE(res);
    REQUIRE(res.value().data.size() == 2);
    REQUIRE(res.value().data[0].data<int>()[0] == MAGIC_MULTIPLIER);
    REQUIRE(res.value().data[1].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::aggregate::single") {
    auto fn = std::make_unique<aggregate_function>("agg_single", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
    aggregate_kernel k(std::move(sig), agg_init, agg_consume, agg_merge, agg_finalize);
    REQUIRE(fn->add_kernel(std::move(k)));

    data_chunk_t chunk(std::pmr::get_default_resource(), {logical_type::INTEGER}, 2);
    chunk.set_value(0, 0, logical_value_t(2));
    chunk.set_value(0, 1, logical_value_t(3));

    auto res = fn->execute(chunk, 2);
    REQUIRE(res);
    REQUIRE(res.value().data[0].data<int>()[0] == 25); // 10 (init) + 5 (agg) + 10 (init + merge)
}

TEST_CASE("components::compute::aggregate::batch") {
    auto fn = std::make_unique<aggregate_function>("agg_batch", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
    aggregate_kernel k(std::move(sig), agg_init, agg_consume, agg_merge, agg_finalize);
    REQUIRE(fn->add_kernel(std::move(k)));

    data_chunk_t c1(std::pmr::get_default_resource(), {logical_type::INTEGER}, 2);
    c1.set_value(0, 0, logical_value_t(1));
    c1.set_value(0, 1, logical_value_t(2));

    data_chunk_t c2(std::pmr::get_default_resource(), {logical_type::INTEGER}, 2);
    c2.set_value(0, 0, logical_value_t(3));
    c2.set_value(0, 1, logical_value_t(4));

    std::vector<data_chunk_t> batch;
    batch.emplace_back(std::move(c1));
    batch.emplace_back(std::move(c2));

    auto res = fn->execute(batch, 2);
    REQUIRE(res);
    REQUIRE(res.value().data[0].data<int>()[0] == 40); // 3 init (1 initial + 2 for each batch, 10 from aggregate)
}

TEST_CASE("components::compute::options_required") {
    auto fn = std::make_unique<vector_function>("opts", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE(fn->add_kernel(std::move(k)));

    data_chunk_t chunk(std::pmr::get_default_resource(), {logical_type::INTEGER});
    chunk.set_value(0, 0, logical_value_t(1));

    auto res = fn->execute(chunk, 1);
    REQUIRE_FALSE(res);
    REQUIRE(res.status().code() == compute_status_code_t::INVALID);
}

TEST_CASE("components::compute::errors") {
    data_chunk_t chunk(std::pmr::get_default_resource(), {logical_type::INTEGER});

    SECTION("arity mismatch") {
        auto fn = std::make_unique<vector_function>("vec", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER), exact_type_matcher(logical_type::NA)},
                               output_type::fixed(logical_type::INTEGER));
        vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
        REQUIRE(fn->add_kernel(std::move(k)).code() == compute_status_code_t::INVALID);
    }

    SECTION("type mismatch") {
        auto fn = std::make_unique<vector_function>("bad_types", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
        vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
        REQUIRE(fn->add_kernel(std::move(k)));

        data_chunk_t chunk(std::pmr::get_default_resource(), {logical_type::STRING_LITERAL});
        chunk.set_value(0, 0, logical_value_t("oops"));

        auto res = fn->execute(chunk, 1);
        REQUIRE_FALSE(res);
        REQUIRE(res.status().code() == compute_status_code_t::EXECUTION_ERROR);
    }

    SECTION("faulty vector exec") {
        test_options opts;
        auto fn = std::make_unique<vector_function>("vec", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
        vector_kernel k(std::move(sig), vector_exec_fail, vector_init, vector_finalize);
        REQUIRE(fn->add_kernel(std::move(k)));

        auto status = fn->execute(chunk, 0, &opts).status();
        REQUIRE(status == TEST_ERROR);
    }

    SECTION("faulty consume") {
        auto fn = std::make_unique<aggregate_function>("agg", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig({exact_type_matcher(logical_type::INTEGER)}, output_type::fixed(logical_type::INTEGER));
        aggregate_kernel k(std::move(sig), agg_init, agg_consume_fail, agg_merge, agg_finalize);
        REQUIRE(fn->add_kernel(std::move(k)));

        auto status = fn->execute(chunk, 0).status();
        REQUIRE(status == TEST_ERROR);
    }
}
