#include <catch2/catch.hpp>
#include <components/compute/function.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

constexpr int MAGIC_MULTIPLIER = 1234;
static const auto TEST_ERROR =
    core::error_t(core::error_code_t::kernel_error, core::error_tag_t::compute_kernel, std::pmr::string{"error!"});

struct test_options : function_options {
    int multiplier;
};

struct counters : kernel_state {
    int multiplier = 0;
    int exec_called = 0;
};

static core::result_wrapper_t<kernel_state_ptr> vector_init(kernel_context&, kernel_init_args args) {
    auto* opts = static_cast<const test_options*>(args.options);
    REQUIRE(opts != nullptr);

    auto c = std::make_unique<counters>();
    c->multiplier = opts->multiplier;
    return c;
}

static core::error_t vector_exec(kernel_context& ctx, const data_chunk_t& in, vector_t& out) {
    auto* c = static_cast<counters*>(ctx.state());
    c->exec_called++;

    for (size_t i = 0; i < in.data.size(); ++i) {
        auto cnt = logical_value_t(in.resource(), in.data[0].data<int>()[i] * c->multiplier);
        out.set_value(i, cnt);
    }
    return core::error_t::no_error();
}

static core::error_t vector_finalize(kernel_context& ctx, data_chunk_t&) {
    auto* c = static_cast<counters*>(ctx.state());
    REQUIRE(c->exec_called);                    // at least one call
    REQUIRE(c->multiplier == MAGIC_MULTIPLIER); // init was called with function_options
    return core::error_t::no_error();
}

struct agg_counter : kernel_state {
    int value;
};

static core::result_wrapper_t<kernel_state_ptr> agg_init(kernel_context&, kernel_init_args) {
    auto c = std::make_unique<agg_counter>();
    c->value = 10;
    return c;
}

static core::error_t agg_consume(kernel_context& ctx, const data_chunk_t& in) {
    auto* acc = static_cast<agg_counter*>(ctx.state());
    for (size_t i = 0; i < in.size(); ++i) {
        acc->value += in.data[0].data<int>()[i];
    }
    return core::error_t::no_error();
}

static core::error_t agg_merge(aggregate_kernel_context&, kernel_state&& from, kernel_state& into) {
    static_cast<agg_counter&>(into).value += static_cast<agg_counter&>(from).value;
    return core::error_t::no_error();
}

static core::error_t agg_finalize(aggregate_kernel_context& ctx) {
    ctx.batch_results.emplace_back(ctx.exec_context().resource(), static_cast<agg_counter*>(ctx.state())->value);
    return core::error_t::no_error();
}

static core::error_t vector_exec_fail(kernel_context&, const data_chunk_t&, vector_t&) { return TEST_ERROR; }

static core::error_t agg_consume_fail(kernel_context&, const data_chunk_t&) { return TEST_ERROR; }

static core::error_t agg_push_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
    ctx.batch_results.emplace_back(ctx.batch_results.get_allocator().resource(), static_cast<agg_counter&>(from).value);
    return core::error_t::no_error();
}

static core::error_t agg_push_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

static core::error_t row_double(kernel_context&,
                                const std::pmr::vector<logical_value_t>& inputs,
                                std::pmr::vector<logical_value_t>& output) {
    output.emplace_back(inputs[0].resource(), inputs[0].value<int>() * 2);
    return core::error_t::no_error();
}

static core::error_t
row_exec_fail(kernel_context&, const std::pmr::vector<logical_value_t>&, std::pmr::vector<logical_value_t>&) {
    return TEST_ERROR;
}

inline function_doc function_doc_with_options() { return function_doc{"", "", {}, true}; }

TEST_CASE("components::compute::vector::single") {
    std::pmr::synchronized_pool_resource resource;
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_test", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, logical_value_t(chunk.resource(), 10));
    chunk.set_cardinality(1);

    auto res = fn->execute(chunk, &opts);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<data_chunk_t>(res.value()).data[0].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::vector::batch") {
    std::pmr::synchronized_pool_resource resource;
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_batch", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t c1(&resource, {logical_type::INTEGER});
    c1.set_value(0, 0, logical_value_t(c1.resource(), 1));
    c1.set_cardinality(1);

    data_chunk_t c2(&resource, {logical_type::INTEGER});
    c2.set_value(0, 0, logical_value_t(c2.resource(), 10));
    c2.set_cardinality(1);

    std::vector<data_chunk_t> batch;
    batch.emplace_back(std::move(c1));
    batch.emplace_back(std::move(c2));

    auto res = fn->execute(batch, &opts);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<data_chunk_t>(res.value()).data.size() == 2);
    REQUIRE(std::get<data_chunk_t>(res.value()).data[0].data<int>()[0] == MAGIC_MULTIPLIER);
    REQUIRE(std::get<data_chunk_t>(res.value()).data[1].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::aggregate::single") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<aggregate_function>("agg_single", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::aggregate,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    aggregate_kernel k(std::move(sig), agg_init, agg_consume, agg_merge, agg_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER}, 2);
    chunk.set_value(0, 0, logical_value_t(chunk.resource(), 2));
    chunk.set_value(0, 1, logical_value_t(chunk.resource(), 3));
    chunk.set_cardinality(2);

    auto res = fn->execute(chunk);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<std::pmr::vector<logical_value_t>>(res.value())[0].value<int>() ==
            25); // 10 (init) + 5 (agg) + 10 (init + merge)
}

TEST_CASE("components::compute::aggregate::batch") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<aggregate_function>("agg_batch", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::aggregate,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    aggregate_kernel k(std::move(sig), agg_init, agg_consume, agg_merge, agg_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t c1(&resource, {logical_type::INTEGER}, 2);
    c1.set_value(0, 0, logical_value_t(c1.resource(), 1));
    c1.set_value(0, 1, logical_value_t(c1.resource(), 2));
    c1.set_cardinality(2);

    data_chunk_t c2(&resource, {logical_type::INTEGER}, 2);
    c2.set_value(0, 0, logical_value_t(c2.resource(), 3));
    c2.set_value(0, 1, logical_value_t(c2.resource(), 4));
    c2.set_cardinality(2);

    std::vector<data_chunk_t> batch;
    batch.emplace_back(std::move(c1));
    batch.emplace_back(std::move(c2));

    auto res = fn->execute(batch);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<std::pmr::vector<logical_value_t>>(res.value())[0].value<int>() ==
            40); // 3 init (1 initial + 2 for each batch, 10 from aggregate)
}

TEST_CASE("components::compute::aggregate::batch_per_group") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<aggregate_function>("agg_per_group", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::aggregate,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    aggregate_kernel k(std::move(sig), agg_init, agg_consume, agg_push_merge, agg_push_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t c1(&resource, {logical_type::INTEGER}, 2);
    c1.set_value(0, 0, logical_value_t(c1.resource(), 1));
    c1.set_value(0, 1, logical_value_t(c1.resource(), 2));
    c1.set_cardinality(2);

    data_chunk_t c2(&resource, {logical_type::INTEGER}, 2);
    c2.set_value(0, 0, logical_value_t(c2.resource(), 3));
    c2.set_value(0, 1, logical_value_t(c2.resource(), 4));
    c2.set_cardinality(2);

    std::vector<data_chunk_t> batch;
    batch.emplace_back(std::move(c1));
    batch.emplace_back(std::move(c2));

    auto res = fn->execute(batch);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 2);
    REQUIRE(vals[0].value<int>() == 13); // 10 (init) + 1 + 2
    REQUIRE(vals[1].value<int>() == 17); // 10 (init) + 3 + 4
}

TEST_CASE("components::compute::row::single") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<row_function>("row_single", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), row_double);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER}, 3);
    chunk.set_value(0, 0, logical_value_t(chunk.resource(), 1));
    chunk.set_value(0, 1, logical_value_t(chunk.resource(), 2));
    chunk.set_value(0, 2, logical_value_t(chunk.resource(), 3));
    chunk.set_cardinality(3);

    auto res = fn->execute(chunk);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 3);
    REQUIRE(vals[0].value<int>() == 2);
    REQUIRE(vals[1].value<int>() == 4);
    REQUIRE(vals[2].value<int>() == 6);
}

TEST_CASE("components::compute::row::batch") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<row_function>("row_batch", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), row_double);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t c1(&resource, {logical_type::INTEGER}, 2);
    c1.set_value(0, 0, logical_value_t(c1.resource(), 5));
    c1.set_value(0, 1, logical_value_t(c1.resource(), 7));
    c1.set_cardinality(2);

    data_chunk_t c2(&resource, {logical_type::INTEGER}, 1);
    c2.set_value(0, 0, logical_value_t(c2.resource(), 10));
    c2.set_cardinality(1);

    std::vector<data_chunk_t> batch;
    batch.emplace_back(std::move(c1));
    batch.emplace_back(std::move(c2));

    auto res = fn->execute(batch);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 3);
    REQUIRE(vals[0].value<int>() == 10);
    REQUIRE(vals[1].value<int>() == 14);
    REQUIRE(vals[2].value<int>() == 20);
}

TEST_CASE("components::compute::row::values") {
    std::pmr::synchronized_pool_resource resource;
    // Direct pmr::vector path — single scalar call
    auto fn = std::make_unique<row_function>("row_vals", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), row_double);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    std::pmr::vector<logical_value_t> inputs(&resource);
    inputs.emplace_back(&resource, 21);

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<int>() == 42);
}

TEST_CASE("components::compute::options_required") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<vector_function>("opts", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {exact_type_matcher(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, logical_value_t(chunk.resource(), 1));
    chunk.set_cardinality(1);

    auto res = fn->execute(chunk);
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
}

TEST_CASE("components::compute::errors") {
    std::pmr::synchronized_pool_resource resource;
    data_chunk_t chunk(&resource, {logical_type::INTEGER});

    SECTION("arity mismatch") {
        auto fn = std::make_unique<vector_function>("vec", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::vector,
                               {exact_type_matcher(logical_type::INTEGER), exact_type_matcher(logical_type::NA)},
                               {output_type::fixed(logical_type::INTEGER)});
        vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
        REQUIRE(fn->add_kernel(&resource, std::move(k)).type == core::error_code_t::kernel_error);
    }

    SECTION("type mismatch") {
        auto fn = std::make_unique<vector_function>("bad_types", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::vector,
                               {exact_type_matcher(logical_type::INTEGER)},
                               {output_type::fixed(logical_type::INTEGER)});
        vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
        REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

        data_chunk_t try_chunk(&resource, {logical_type::STRING_LITERAL});
        try_chunk.set_value(0, 0, logical_value_t(chunk.resource(), "oops"));
        try_chunk.set_cardinality(1);

        auto res = fn->execute(try_chunk);
        REQUIRE(res.has_error());
        REQUIRE(res.error().type == core::error_code_t::kernel_error);
    }

    SECTION("faulty vector exec") {
        test_options opts;
        auto fn = std::make_unique<vector_function>("vec", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::vector,
                               {exact_type_matcher(logical_type::INTEGER)},
                               {output_type::fixed(logical_type::INTEGER)});
        vector_kernel k(std::move(sig), vector_exec_fail, vector_init, vector_finalize);
        REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

        auto status = fn->execute(chunk, &opts).error().type;
        REQUIRE(status == core::error_code_t::kernel_error);
    }

    SECTION("faulty consume") {
        auto fn = std::make_unique<aggregate_function>("agg", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::aggregate,
                               {exact_type_matcher(logical_type::INTEGER)},
                               {output_type::fixed(logical_type::INTEGER)});
        aggregate_kernel k(std::move(sig), agg_init, agg_consume_fail, agg_merge, agg_finalize);
        REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

        auto status = fn->execute(chunk).error().type;
        REQUIRE(status == core::error_code_t::kernel_error);
    }

    SECTION("faulty row exec") {
        auto fn = std::make_unique<row_function>("row", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::row,
                               {exact_type_matcher(logical_type::INTEGER)},
                               {output_type::fixed(logical_type::INTEGER)});
        row_kernel k(std::move(sig), row_exec_fail);
        REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

        std::pmr::vector<logical_value_t> inputs(&resource);
        inputs.emplace_back(&resource, 1);

        auto status = fn->execute(inputs).error().type;
        REQUIRE(status == core::error_code_t::kernel_error);
    }
}
