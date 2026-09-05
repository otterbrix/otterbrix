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
    exec_context_t ctx(resource);
    auto executor = fn.make_executor(resource, in_types, nullptr, ctx);
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
    exec_context_t ctx(&resource);
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

    auto res = fn->execute(chunk, &opts, ctx);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<data_chunk_t>(res.value()).data[0].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::vector::batch") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
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

    auto res = fn->execute(batch, &opts, ctx);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<data_chunk_t>(res.value()).data.size() == 2);
    REQUIRE(std::get<data_chunk_t>(res.value()).data[0].data<int>()[0] == MAGIC_MULTIPLIER);
    REQUIRE(std::get<data_chunk_t>(res.value()).data[1].data<int>()[0] == MAGIC_MULTIPLIER * 10);
}

TEST_CASE("components::compute::vector::output_resolver_refusal_reaches_the_caller") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
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

    CHECK(fn->make_executor(&resource, {logical_type::INTEGER}, &opts, ctx).has_error());
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
    exec_context_t ctx(&resource);
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

    auto res = fn->execute(chunk, nullptr, ctx);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 3);
    REQUIRE(vals[0].value<int>() == 2);
    REQUIRE(vals[1].value<int>() == 4);
    REQUIRE(vals[2].value<int>() == 6);
}

TEST_CASE("components::compute::row::batch") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
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

    auto res = fn->execute(batch, nullptr, ctx);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 3);
    REQUIRE(vals[0].value<int>() == 10);
    REQUIRE(vals[1].value<int>() == 14);
    REQUIRE(vals[2].value<int>() == 20);
}

TEST_CASE("components::compute::row::values") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    // Direct pmr::vector path — single scalar call
    auto fn = std::make_unique<row_function>("row_vals", arity::unary(), function_doc{}, 1);

    kernel_signature_t sig(function_type_t::row,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), row_double);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    std::pmr::vector<logical_value_t> inputs(&resource);
    inputs.emplace_back(&resource, 21);

    auto res = fn->execute(inputs, nullptr, ctx);
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
    exec_context_t ctx(&resource);
    auto fn = std::make_unique<vector_function>("opts", arity::unary(), function_doc_with_options(), 1);

    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, 1);
    chunk.set_cardinality(1);

    auto res = fn->execute(chunk, nullptr, ctx);
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
}

TEST_CASE("components::compute::errors") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
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

        auto res = fn->execute(try_chunk, nullptr, ctx);
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

        auto status = fn->execute(chunk, &opts, ctx).error().type;
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

        auto status = fn->execute(inputs, nullptr, ctx).error().type;
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
        // Explicit, because function::execute has no defaulted context any more: the calls
        // below used to run on the process-global default resource.
        exec_context_t ctx{&resource, &registry};

        // The full builtin set, not register_string_functions alone: the
        // helpers are ORDERED STAGES of register_default_functions ("substring"
        // must land on uid 5), and a standalone stage now poisons the registry
        // for shifting the uid table. Lookups here are by name, so the extra
        // builtins are invisible to these cases.
        string_registry_fixture() { register_default_functions(registry); }

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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
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

    auto res = fn->execute(inputs, nullptr, fx.ctx);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].type().type() == logical_type::NA);
}

// Builds the one-slot INTEGER->INTEGER vector function the cases below drive.
static std::unique_ptr<vector_function> multiplying_vector_function(std::pmr::memory_resource* resource,
                                                                    const std::string& name) {
    auto fn = std::make_unique<vector_function>(name, arity::unary(), function_doc_with_options(), 1);
    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), vector_exec, vector_init, vector_finalize);
    REQUIRE_FALSE(fn->add_kernel(resource, std::move(k)).contains_error());
    return fn;
}

static data_chunk_t one_int(std::pmr::memory_resource* resource, int value) {
    data_chunk_t chunk(resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, value);
    chunk.set_cardinality(1);
    return chunk;
}

// The batch overload fuses the per-chunk outputs into one chunk but never stamped a row count,
// so the fused chunk carried the values and reported zero rows to everyone who asked size().
TEST_CASE("components::compute::vector::batch_reports_the_rows_it_carries") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = multiplying_vector_function(&resource, "vec_batch_rows");

    std::vector<data_chunk_t> batch;
    batch.emplace_back(one_int(&resource, 1));
    batch.emplace_back(one_int(&resource, 10));

    auto res = fn->execute(batch, &opts, ctx);
    REQUIRE_FALSE(res.has_error());
    const auto& out = std::get<data_chunk_t>(res.value());
    REQUIRE(out.data.size() == 2);
    REQUIRE(out.size() == 1);
}

// Fusing per-chunk outputs side by side only describes a chunk when every input chunk is the
// same height. Unequal inputs have no honest row count, so the call must refuse rather than
// pick one and mislabel the rest.
TEST_CASE("components::compute::vector::batch_refuses_chunks_of_unequal_height") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = multiplying_vector_function(&resource, "vec_batch_ragged");

    std::vector<data_chunk_t> batch;
    batch.emplace_back(one_int(&resource, 1));
    batch.emplace_back(two_ints(&resource, 2, 3));

    auto res = fn->execute(batch, &opts, ctx);
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
}

// The vector executor collected its per-chunk outputs in a member it never cleared, and each
// call read results_.front(). execution_dag keeps ONE executor per function node and pushes
// every chunk through it, so the second chunk was answered with the moved-from remains of the
// first. Both results are held alive on purpose: that keeps the stale read pointing at live
// memory, so the case fails on the wrong VALUE instead of on freed bytes.
TEST_CASE("components::compute::vector::a_reused_executor_answers_the_current_chunk") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    test_options opts;
    opts.multiplier = MAGIC_MULTIPLIER;

    auto fn = multiplying_vector_function(&resource, "vec_reused");

    std::pmr::vector<complex_logical_type> in_types(&resource);
    in_types.emplace_back(logical_type::INTEGER);
    auto executor = fn->make_executor(&resource, in_types, &opts, ctx);
    REQUIRE_FALSE(executor.has_error());

    auto first_chunk = one_int(&resource, 3);
    auto first = executor.value()->execute(first_chunk);
    REQUIRE_FALSE(first.has_error());
    REQUIRE(std::get<data_chunk_t>(first.value()).data[0].data<int>()[0] == MAGIC_MULTIPLIER * 3);

    auto second_chunk = one_int(&resource, 7);
    auto second = executor.value()->execute(second_chunk);
    REQUIRE_FALSE(second.has_error());
    const auto& out = std::get<data_chunk_t>(second.value());
    REQUIRE(out.size() == 1);
    REQUIRE(out.data[0].data<int>()[0] == MAGIC_MULTIPLIER * 7);
}

// UDFs in this project are row_function objects carrying a user-supplied row_exec_fn (see
// integration/cpp/test/test_udfs.cpp), so foreign code decides what lands in `output`. A kernel
// that reports success and writes nothing is ordinary foreign-code behaviour, not a fiction.
static core::error_t row_double_silent_on_two(kernel_context&,
                                              const std::pmr::vector<logical_value_t>& inputs,
                                              std::pmr::vector<logical_value_t>& output) {
    if (inputs[0].value<int>() == 2) {
        return core::error_t::no_error();
    }
    output.emplace_back(inputs[0].resource(), inputs[0].value<int>() * 2);
    return core::error_t::no_error();
}

// The other side of the same contract: a kernel that writes more than one value for one row.
static core::error_t row_double_and_triple(kernel_context&,
                                           const std::pmr::vector<logical_value_t>& inputs,
                                           std::pmr::vector<logical_value_t>& output) {
    output.emplace_back(inputs[0].resource(), inputs[0].value<int>() * 2);
    output.emplace_back(inputs[0].resource(), inputs[0].value<int>() * 3);
    return core::error_t::no_error();
}

static std::unique_ptr<row_function>
unary_row_function(std::pmr::memory_resource* resource, const std::string& name, row_exec_fn exec) {
    auto fn = std::make_unique<row_function>(name, arity::unary(), function_doc{}, 1);
    kernel_signature_t sig(function_type_t::row,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    row_kernel k(std::move(sig), exec);
    REQUIRE_FALSE(fn->add_kernel(resource, std::move(k)).contains_error());
    return fn;
}

static data_chunk_t three_ints(std::pmr::memory_resource* resource, int first, int second, int third) {
    data_chunk_t chunk(resource, {logical_type::INTEGER}, 3);
    chunk.set_value(0, 0, first);
    chunk.set_value(0, 1, second);
    chunk.set_value(0, 2, third);
    chunk.set_cardinality(3);
    return chunk;
}

// The row executor declared "one scalar output per call" in a comment and then, on seeing the
// contract broken, silently skipped the row: `results` came back SHORTER than the chunk.
// components/execution_dag/execution_dag.cpp only catches that with an assert() and then walks
// `values` by row index, so under -DNDEBUG the rows the kernel never answered keep whatever the
// output vector held before. The claim is therefore about what the caller receives, not merely
// about the return code, so the length is checked first and the refusal second.
TEST_CASE("components::compute::row::a_row_the_kernel_left_empty_is_refused_not_skipped") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    auto fn = unary_row_function(&resource, "row_silent", row_double_silent_on_two);

    auto chunk = three_ints(&resource, 1, 2, 3);
    auto res = fn->execute(chunk, nullptr, ctx);

    if (!res.has_error()) {
        const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
        INFO("execute() succeeded with " << vals.size() << " values for " << chunk.size() << " rows");
        REQUIRE(vals.size() == chunk.size());
    }
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
    // The refusal has to say WHICH row broke the contract and what it produced instead.
    const std::string refusal(res.error().what.data(), res.error().what.size());
    INFO("refusal reads: " << refusal);
    REQUIRE(res.error().what.find("row 1") != std::pmr::string::npos);
}

// The same skip discarded surplus values: everything past front() was dropped and the call
// still reported success, so a kernel returning two values per row looked exactly like a
// well-behaved one.
TEST_CASE("components::compute::row::a_row_with_several_outputs_is_refused") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    auto fn = unary_row_function(&resource, "row_two_outputs", row_double_and_triple);

    auto chunk = three_ints(&resource, 1, 2, 3);
    auto res = fn->execute(chunk, nullptr, ctx);

    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
    const std::string refusal(res.error().what.data(), res.error().what.size());
    INFO("refusal reads: " << refusal);
    REQUIRE(res.error().what.find("row 0") != std::pmr::string::npos);
}

// The multi-chunk overload walks the same per-row loop, so a contract break in a later chunk
// must reach the caller too rather than shortening the concatenated result.
TEST_CASE("components::compute::row::a_broken_contract_in_a_later_chunk_still_refuses") {
    core::pmr::otterbrix_resource resource;
    exec_context_t ctx(&resource);
    auto fn = unary_row_function(&resource, "row_silent_batch", row_double_silent_on_two);

    std::vector<data_chunk_t> batch;
    batch.emplace_back(three_ints(&resource, 1, 3, 5));
    batch.emplace_back(three_ints(&resource, 7, 2, 9));

    auto res = fn->execute(batch, nullptr, ctx);

    if (!res.has_error()) {
        const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
        INFO("execute() succeeded with " << vals.size() << " values for 6 rows");
        REQUIRE(vals.size() == 6);
    }
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
}
