#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>
#include <core/pmr.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

constexpr int MAGIC_MULTIPLIER = 1234;
static const auto TEST_ERROR = core::error_t(core::error_code_t::kernel_error, std::pmr::string{"error!"});

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
        auto cnt = in.data[0].data<int>()[i] * c->multiplier;
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

struct agg_counter {
    int value{10};
};

static aggregate_state_layout_t agg_layout(const std::pmr::vector<complex_logical_type>&) {
    return aggregate_state_of<agg_counter>();
}

static core::error_t
agg_update(kernel_context&, const data_chunk_t& in, core::span<const uint32_t> groups, aggregate_states_t states) {
    for (size_t i = 0; i < in.size(); ++i) {
        states.at<agg_counter>(groups[i]).value += in.data[0].data<int>()[i];
    }
    return core::error_t::no_error();
}

static core::error_t
agg_finalize(kernel_context&, aggregate_states_t states, uint64_t first, uint64_t count, vector_t& out) {
    for (uint64_t row = 0; row < count; ++row) {
        out.data<int>()[row] = states.at<agg_counter>(first + row).value;
    }
    return core::error_t::no_error();
}

static core::error_t vector_exec_fail(kernel_context&, const data_chunk_t&, vector_t&) { return TEST_ERROR; }

static core::error_t
agg_update_fail(kernel_context&, const data_chunk_t&, core::span<const uint32_t>, aggregate_states_t) {
    return TEST_ERROR;
}

// Drives an aggregate the way the engine does: reserve the accumulators, fold the chunks, then
// emit one value per group.
static core::result_wrapper_t<std::pmr::vector<int>> run_aggregate(std::pmr::memory_resource* resource,
                                                                   const aggregate_function& fn,
                                                                   const std::vector<data_chunk_t>& chunks,
                                                                   const std::vector<std::vector<uint32_t>>& groups,
                                                                   uint64_t group_count) {
    std::pmr::vector<complex_logical_type> in_types(resource);
    in_types.emplace_back(logical_type::INTEGER);
    auto executor = fn.make_executor(resource, in_types);
    if (executor.has_error()) {
        return executor.error();
    }
    aggregate_state_arena_t arena(resource, executor.value()->state_layout());
    arena.reserve(group_count);
    for (size_t chunk = 0; chunk < chunks.size(); chunk++) {
        if (auto error =
                executor.value()->update(chunks[chunk], {groups[chunk].data(), groups[chunk].size()}, arena.states());
            error.contains_error()) {
            return error;
        }
    }
    vector_t out(resource, logical_type::INTEGER, group_count);
    if (auto error = executor.value()->finalize(arena.states(), 0, group_count, out); error.contains_error()) {
        return error;
    }
    std::pmr::vector<int> values(resource);
    for (uint64_t group = 0; group < group_count; group++) {
        values.push_back(out.data<int>()[group]);
    }
    return values;
}

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
    core::pmr::otterbrix_resource resource;
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_test", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, 10);
    chunk.set_cardinality(1);

    auto res = fn->execute(chunk, &opts);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<data_chunk_t>(res.value()).data[0].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::vector::batch") {
    core::pmr::otterbrix_resource resource;
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_batch", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t c1(&resource, {logical_type::INTEGER});
    c1.set_value(0, 0, 1);
    c1.set_cardinality(1);

    data_chunk_t c2(&resource, {logical_type::INTEGER});
    c2.set_value(0, 0, 10);
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

TEST_CASE("components::compute::vector::output_resolver_refusal_reaches_the_caller") {
    core::pmr::otterbrix_resource resource;
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = std::make_unique<vector_function>("vec_bad_output", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(
        function_type_t::vector,
        {parameter_type::exact(logical_type::INTEGER)},
        {output_type::computed(
            [](std::pmr::memory_resource* r, const std::pmr::vector<fixed_t>&) -> core::result_wrapper_t<fixed_t> {
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"output type cannot be resolved", r});
            })});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    CHECK(fn->make_executor(&resource, {logical_type::INTEGER}, &opts).has_error());
}

static data_chunk_t two_ints(std::pmr::memory_resource* resource, int first, int second) {
    data_chunk_t chunk(resource, {logical_type::INTEGER}, 2);
    chunk.set_value(0, 0, first);
    chunk.set_value(0, 1, second);
    chunk.set_cardinality(2);
    return chunk;
}

static std::unique_ptr<aggregate_function> counting_aggregate(std::pmr::memory_resource* resource,
                                                              const std::string& name,
                                                              aggregate_update_fn update = agg_update) {
    auto fn = std::make_unique<aggregate_function>(name, arity::unary(), function_doc{}, 1);
    kernel_signature_t sig(function_type_t::aggregate,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    aggregate_kernel k(std::move(sig), agg_layout, update, agg_finalize);
    REQUIRE_FALSE(fn->add_kernel(resource, std::move(k)).contains_error());
    return fn;
}

TEST_CASE("components::compute::aggregate::single") {
    core::pmr::otterbrix_resource resource;
    auto fn = counting_aggregate(&resource, "agg_single");

    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(two_ints(&resource, 2, 3));

    auto res = run_aggregate(&resource, *fn, chunks, {{0, 0}}, 1);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value()[0] == 15); // 10 (init) + 2 + 3
}

TEST_CASE("components::compute::aggregate::batch") {
    core::pmr::otterbrix_resource resource;
    auto fn = counting_aggregate(&resource, "agg_batch");

    // One group fed by two chunks: the accumulator survives between them.
    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(two_ints(&resource, 1, 2));
    chunks.emplace_back(two_ints(&resource, 3, 4));

    auto res = run_aggregate(&resource, *fn, chunks, {{0, 0}, {0, 0}}, 1);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value()[0] == 20); // 10 (init) + 1 + 2 + 3 + 4
}

TEST_CASE("components::compute::aggregate::per_group") {
    core::pmr::otterbrix_resource resource;
    auto fn = counting_aggregate(&resource, "agg_per_group");

    // Rows of one chunk scatter into different accumulators, and a group discovered in the
    // second chunk keeps accumulating into its own.
    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(two_ints(&resource, 1, 2));
    chunks.emplace_back(two_ints(&resource, 3, 4));

    auto res = run_aggregate(&resource, *fn, chunks, {{0, 1}, {1, 0}}, 2);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value().size() == 2);
    REQUIRE(res.value()[0] == 15); // 10 (init) + 1 + 4
    REQUIRE(res.value()[1] == 15); // 10 (init) + 2 + 3
}

TEST_CASE("components::compute::row::single") {
    core::pmr::otterbrix_resource resource;
    auto fn = std::make_unique<row_function>("row_single", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), row_double);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER}, 3);
    chunk.set_value(0, 0, 1);
    chunk.set_value(0, 1, 2);
    chunk.set_value(0, 2, 3);
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
    core::pmr::otterbrix_resource resource;
    auto fn = std::make_unique<row_function>("row_batch", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), row_double);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t c1(&resource, {logical_type::INTEGER}, 2);
    c1.set_value(0, 0, 5);
    c1.set_value(0, 1, 7);
    c1.set_cardinality(2);

    data_chunk_t c2(&resource, {logical_type::INTEGER}, 1);
    c2.set_value(0, 0, 10);
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
    core::pmr::otterbrix_resource resource;
    // Direct pmr::vector path — single scalar call
    auto fn = std::make_unique<row_function>("row_vals", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {parameter_type::exact(logical_type::INTEGER)},
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

TEST_CASE("components::compute::expand::generate_series") {
    core::pmr::otterbrix_resource resource;

    auto* reg = function_registry_t::get_default();
    function_uid uid = invalid_function_uid;
    for (const auto& [n, u] : reg->get_functions()) {
        if (n == "generate_series") {
            uid = u;
        }
    }
    REQUIRE(uid != invalid_function_uid);
    auto* fn = reg->get_function(uid);
    REQUIRE(fn != nullptr);

    // Argument columns for one input row: start=1, stop=5.
    std::pmr::vector<complex_logical_type> arg_types(&resource);
    arg_types.emplace_back(logical_type::BIGINT);
    arg_types.emplace_back(logical_type::BIGINT);
    data_chunk_t args(&resource, arg_types, 1);
    args.set_value(0, 0, logical_value_t(&resource, static_cast<int64_t>(1)));
    args.set_value(1, 0, logical_value_t(&resource, static_cast<int64_t>(5)));
    args.set_cardinality(1);

    auto kres = fn->dispatch_exact(&resource, arg_types);
    REQUIRE_FALSE(kres.has_error());
    const auto* expand = dynamic_cast<const expand_kernel*>(&kres.value().get());
    REQUIRE(expand != nullptr);

    exec_context_t exec_ctx(&resource, reg);
    kernel_context kctx(exec_ctx, *expand);
    std::pmr::vector<data_chunk_t> outputs(&resource);
    REQUIRE_FALSE(expand->execute(kctx, args, outputs).contains_error());

    size_t total = 0;
    for (const auto& chunk : outputs) {
        total += chunk.size();
    }
    REQUIRE(total == 5);
    // Values 1..5 in order across the produced chunks.
    int64_t expected = 1;
    for (const auto& chunk : outputs) {
        for (uint64_t i = 0; i < chunk.size(); ++i) {
            REQUIRE(chunk.value(0, i).value<int64_t>() == expected);
            ++expected;
        }
    }
}

TEST_CASE("components::compute::options_required") {
    core::pmr::otterbrix_resource resource;
    auto fn = std::make_unique<vector_function>("opts", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, 1);
    chunk.set_cardinality(1);

    auto res = fn->execute(chunk);
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
}

TEST_CASE("components::compute::errors") {
    core::pmr::otterbrix_resource resource;
    data_chunk_t chunk(&resource, {logical_type::INTEGER});

    SECTION("arity mismatch") {
        auto fn = std::make_unique<vector_function>("vec", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::INTEGER), parameter_type::exact(logical_type::NA)},
                               {output_type::fixed(logical_type::INTEGER)});
        vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
        REQUIRE(fn->add_kernel(&resource, std::move(k)).type == core::error_code_t::kernel_error);
    }

    SECTION("type mismatch") {
        auto fn = std::make_unique<vector_function>("bad_types", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::INTEGER)},
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
                               {parameter_type::exact(logical_type::INTEGER)},
                               {output_type::fixed(logical_type::INTEGER)});
        vector_kernel k(std::move(sig), vector_exec_fail, vector_init, vector_finalize);
        REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

        auto status = fn->execute(chunk, &opts).error().type;
        REQUIRE(status == core::error_code_t::kernel_error);
    }

    SECTION("faulty update") {
        auto fn = counting_aggregate(&resource, "agg", agg_update_fail);

        std::vector<data_chunk_t> chunks;
        chunks.emplace_back(two_ints(&resource, 1, 2));

        auto status = run_aggregate(&resource, *fn, chunks, {{0, 0}}, 1).error().type;
        REQUIRE(status == core::error_code_t::kernel_error);
    }

    SECTION("faulty row exec") {
        auto fn = std::make_unique<row_function>("row", arity::unary(), function_doc{}, 1);

        kernel_signature_t sig(function_type_t::row,
                               {parameter_type::exact(logical_type::INTEGER)},
                               {output_type::fixed(logical_type::INTEGER)});
        row_kernel k(std::move(sig), row_exec_fail);
        REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

        std::pmr::vector<logical_value_t> inputs(&resource);
        inputs.emplace_back(&resource, 1);

        auto status = fn->execute(inputs).error().type;
        REQUIRE(status == core::error_code_t::kernel_error);
    }
}

// ---------------------------------------------------------------------------
// SUBSTRING / LENGTH / REGEXP_REPLACE — string row-kernel execution tests
// ---------------------------------------------------------------------------

namespace {
    struct string_registry_fixture {
        core::pmr::otterbrix_resource resource;
        function_registry_t registry{&resource};

        string_registry_fixture() { register_string_functions(registry); }

        function* get(const std::string& name) const {
            for (const auto& [n, uid] : registry.get_functions()) {
                if (n == name) {
                    return registry.get_function(uid);
                }
            }
            return nullptr;
        }
    };
} // namespace

TEST_CASE("components::compute::string::substring_basic") {
    string_registry_fixture fx;
    auto* fn = fx.get("substring");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string("hello world"));
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(7));
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(5));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<std::string_view>() == "world");
}

TEST_CASE("components::compute::string::substring_omit_len") {
    string_registry_fixture fx;
    auto* fn = fx.get("substring");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string("abcdefgh"));
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(3));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<std::string_view>() == "cdefgh");
}

TEST_CASE("components::compute::string::substring_out_of_range") {
    string_registry_fixture fx;
    auto* fn = fx.get("substring");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string("abc"));
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(99));
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(5));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<std::string_view>().empty());
}

TEST_CASE("components::compute::string::substring_null") {
    string_registry_fixture fx;
    auto* fn = fx.get("substring");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, logical_type::NA);
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(1));
    inputs.emplace_back(&fx.resource, static_cast<int64_t>(2));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].type().type() == logical_type::NA);
}

TEST_CASE("components::compute::string::length_basic") {
    string_registry_fixture fx;
    auto* fn = fx.get("length");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string("hello"));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<int64_t>() == 5);
}

TEST_CASE("components::compute::string::length_empty") {
    string_registry_fixture fx;
    auto* fn = fx.get("length");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string(""));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<int64_t>() == 0);
}

TEST_CASE("components::compute::string::length_null") {
    string_registry_fixture fx;
    auto* fn = fx.get("length");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, logical_type::NA);

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].type().type() == logical_type::NA);
}

TEST_CASE("components::compute::string::regexp_replace_basic") {
    string_registry_fixture fx;
    auto* fn = fx.get("regexp_replace");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string("hello 123 world 456"));
    inputs.emplace_back(&fx.resource, std::string("[0-9]+"));
    inputs.emplace_back(&fx.resource, std::string("#"));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<std::string_view>() == "hello # world #");
}

TEST_CASE("components::compute::string::regexp_replace_no_match") {
    string_registry_fixture fx;
    auto* fn = fx.get("regexp_replace");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, std::string("abcdef"));
    inputs.emplace_back(&fx.resource, std::string("[0-9]+"));
    inputs.emplace_back(&fx.resource, std::string("#"));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<std::string_view>() == "abcdef");
}

TEST_CASE("components::compute::string::regexp_replace_null") {
    string_registry_fixture fx;
    auto* fn = fx.get("regexp_replace");
    REQUIRE(fn != nullptr);

    std::pmr::vector<logical_value_t> inputs(&fx.resource);
    inputs.emplace_back(&fx.resource, logical_type::NA);
    inputs.emplace_back(&fx.resource, std::string("x"));
    inputs.emplace_back(&fx.resource, std::string("y"));

    auto res = fn->execute(inputs);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].type().type() == logical_type::NA);
}
