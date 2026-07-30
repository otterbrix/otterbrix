#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>
#include <components/physical_plan/operators/operator_group.hpp>
#include <components/physical_plan/operators/operator_raw_data.hpp>

#include <memory_resource>
#include <string>
#include <vector>

using namespace components;

// Error-contract tests for operator_group_t, built around direct operator
// construction (no dispatcher): malformed key metadata and aggregator
// failures must surface as clean operator errors (set_error / has_error),
// never as asserts/UB inside the operator.

namespace {

    operators::operator_ptr make_child(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return operators::operator_ptr(
            new operators::operator_empty_t(resource, operators::make_operator_data(resource, std::move(chunk))));
    }

    // Drive a group operator through its public streaming sink entries (push every
    // child output chunk, then finalize) — the same path execute_pipeline uses for a
    // sink — and publish the finalized chunks as the operator's output_. This is what
    // the executor's streaming drive does; the legacy on_execute() materialized entry
    // (now deleted) folded the same accumulate()/materialize_groups()/
    // empty_aggregate_result() cores finalize() reaches, so the error contracts below
    // are unchanged.
    void
    drive_group(operators::operator_group_t* group, std::pmr::memory_resource* resource, pipeline::context_t* ctx) {
        group->prepare();
        operators::chunks_vector_t out(resource);
        if (auto child = group->left(); child && child->output()) {
            for (const auto& c : child->output()->chunks()) {
                auto err = group->push(ctx, c.partial_copy(resource, 0, c.size()), out);
                if (err.contains_error()) {
                    group->set_error(err);
                    return;
                }
            }
        }
        auto fin_err = group->finalize(ctx, out);
        if (fin_err.contains_error()) {
            group->set_error(fin_err);
            return;
        }
        group->set_output(operators::make_operator_data(resource, std::move(out)));
    }

    compute::function* find_function(std::string_view name) {
        auto* registry = compute::function_registry_t::get_default();
        if (!registry) {
            return nullptr;
        }
        for (const auto& [fn_name, uid] : registry->get_functions()) {
            if (fn_name == name) {
                return registry->get_function(uid);
            }
        }
        return nullptr;
    }

    // compute::datum_t is a variant owned by the compute layer. A hand-written visitor names
    // none of its alternatives and stops compiling if a third one is ever added, so no new
    // site here spells std::holds_alternative/std::get (rule 14).
    struct first_value_t {
        std::pmr::memory_resource* resource;

        types::logical_value_t operator()(const std::pmr::vector<types::logical_value_t>& values) const {
            return values.empty() ? types::logical_value_t(resource, types::logical_type::NA) : values.front();
        }
        types::logical_value_t operator()(const vector::data_chunk_t& chunk) const {
            return (chunk.data.empty() || chunk.size() == 0)
                       ? types::logical_value_t(resource, types::logical_type::NA)
                       : chunk.value(0, 0);
        }
    };

    // Drive ONE aggregator over ONE input chunk through its public entry (compute() over an
    // already-materialized operator_batch_t) and answer the single value it produced. This is
    // the shape operator_group_t uses per group.
    types::logical_value_t aggregate_over(std::pmr::memory_resource* resource,
                                          pipeline::context_t& ctx,
                                          compute::function* fn,
                                          std::pmr::vector<expressions::param_storage>&& args,
                                          operators::chunks_vector_t&& chunks,
                                          bool distinct = false) {
        auto batch = operators::make_operator_batch(resource, std::move(chunks));
        boost::intrusive_ptr<operators::aggregate::operator_func_t> agg(
            new operators::aggregate::operator_func_t(resource, log_t{}, fn, std::move(args), distinct));
        agg->compute(&ctx, batch);
        INFO("aggregate error: " << (agg->has_error() ? agg->get_error().what : std::pmr::string{resource}));
        REQUIRE_FALSE(agg->has_error());
        auto datum = agg->take_batch_values();
        return std::visit(first_value_t{resource}, datum);
    }

    // One BIGINT column named `name`, one row per value.
    vector::data_chunk_t
    bigint_chunk(std::pmr::memory_resource* resource, std::string_view name, std::initializer_list<int64_t> values) {
        vector::data_chunk_t chunk(resource,
                                   std::pmr::vector<types::complex_logical_type>{
                                       {types::complex_logical_type{types::logical_type::BIGINT}}, resource},
                                   values.size());
        chunk.set_column_name(0, name);
        uint64_t row = 0;
        for (int64_t v : values) {
            chunk.set_value(0, row++, v);
        }
        chunk.set_cardinality(values.size());
        return chunk;
    }

    expressions::key_t column_key(std::pmr::memory_resource* resource, std::string_view name, size_t column_index) {
        expressions::key_t key{resource, name};
        std::pmr::vector<size_t> path(resource);
        path.push_back(column_index);
        key.set_path(std::move(path));
        return key;
    }

} // namespace

TEST_CASE("group operator contracts: unresolved column key surfaces operator error", "[group_contracts]") {
    // Pre-fix this aborted in Debug via assert(!key.full_path.empty()) in
    // extract_key_value (operator_group.cpp); in Release the assert is compiled
    // out and chunk.value(empty_path, row) is UB.
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_column_name(0, "k");
    chunk.set_value(0, 0, int64_t(1));
    chunk.set_value(0, 1, int64_t(2));
    chunk.set_cardinality(2);

    boost::intrusive_ptr<operators::operator_group_t> group(new operators::operator_group_t(&resource, log_t{}));
    operators::group_key_t key(&resource);
    key.name = std::pmr::string("k", &resource);
    key.type = operators::group_key_t::kind::column;
    // full_path deliberately left empty: an unresolved key must become an
    // operator error, not an assert/UB.
    group->add_key(std::move(key));
    group->set_children(make_child(&resource, std::move(chunk)));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    drive_group(group.get(), &resource, &ctx);

    REQUIRE(group->has_error());
    REQUIRE(group->get_error().type == core::error_code_t::schema_error);
    REQUIRE(group->get_error().what.find("k") != std::pmr::string::npos);
}

TEST_CASE("group operator contracts: struct-field key type comes from input schema, not first group value",
          "[group_contracts]") {
    // Key with a multi-part path ({struct column, field index}) where the FIRST
    // group's key value is NULL (extracted as an NA-typed value). Pre-fix
    // build_result_chunk fell back to group_keys_[0][k].type() == NA for any
    // path with size != 1, so writing the later non-NULL keys aborted in Debug
    // via assert("value has to be casted to vector's type before set_value")
    // in vector_t::set_value.
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> fields(&resource);
    fields.emplace_back(types::logical_type::BIGINT);
    fields.back().set_field_name("f");
    auto struct_type = types::complex_logical_type::create_struct("s", fields);

    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.push_back(struct_type);
    vector::data_chunk_t chunk(&resource, cols, 4);
    chunk.set_column_name(0, "s");
    auto field_null = types::logical_value_t(&resource, types::complex_logical_type{types::logical_type::NA});
    auto make_row = [&](types::logical_value_t field_val) {
        return types::logical_value_t::create_struct(&resource, struct_type, {std::move(field_val)});
    };
    // Groups form in row order: NULL first, then 10, then 20.
    chunk.set_value(0, 0, make_row(field_null));
    chunk.set_value(0, 1, make_row(types::logical_value_t(&resource, int64_t(10))));
    chunk.set_value(0, 2, make_row(types::logical_value_t(&resource, int64_t(10))));
    chunk.set_value(0, 3, make_row(types::logical_value_t(&resource, int64_t(20))));
    chunk.set_cardinality(4);

    boost::intrusive_ptr<operators::operator_group_t> group(new operators::operator_group_t(&resource, log_t{}));
    operators::group_key_t key(&resource);
    key.name = std::pmr::string("kf", &resource);
    key.type = operators::group_key_t::kind::column;
    key.full_path.push_back(0); // struct column
    key.full_path.push_back(0); // field "f"
    group->add_key(std::move(key));
    group->set_children(make_child(&resource, std::move(chunk)));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    drive_group(group.get(), &resource, &ctx);

    REQUIRE_FALSE(group->has_error());
    REQUIRE(group->output());
    auto& out = group->output()->chunks().front();
    REQUIRE(out.size() == 3);
    REQUIRE(out.column_count() == 1);
    // The key column type must be the field's type walked through the input
    // schema, not the NA type of the first (NULL) group key.
    REQUIRE(out.data[0].type().type() == types::logical_type::BIGINT);
    REQUIRE(out.data[0].is_null(0));
    REQUIRE(out.get_value<int64_t>(0, 1) == 10);
    REQUIRE(out.get_value<int64_t>(0, 2) == 20);
    // The key column's NAME survives materialization. Names live on the vector, not the
    // type (M3-B5), so the per-group key chunk has to carry them explicitly — a nested
    // key names itself after the group key (struct children are name-less by contract).
    REQUIRE(out.data[0].name() == "kf");
}

TEST_CASE("group operator contracts: flat key column keeps its name through materialization",
          "[group_contracts]") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_cardinality(2);
    chunk.set_column_name(0, "k");
    REQUIRE_FALSE(chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1))).contains_error());
    REQUIRE_FALSE(chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(2))).contains_error());

    boost::intrusive_ptr<operators::operator_group_t> group(new operators::operator_group_t(&resource, log_t{}));
    operators::group_key_t key(&resource);
    key.name = std::pmr::string("k", &resource);
    key.type = operators::group_key_t::kind::column;
    key.full_path.push_back(0);
    group->add_key(std::move(key));
    group->set_children(make_child(&resource, std::move(chunk)));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    drive_group(group.get(), &resource, &ctx);

    REQUIRE_FALSE(group->has_error());
    REQUIRE(group->output());
    auto& out = group->output()->chunks().front();
    REQUIRE(out.size() == 2);
    REQUIRE(out.data[0].name() == "k");
}

TEST_CASE("group operator contracts: a post-aggregate operand with an unbound parameter is an error, not a throw",
          "[group_contracts]") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, cols, 1);
    chunk.set_cardinality(1);
    chunk.set_column_name(0, "k");
    REQUIRE_FALSE(chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1))).contains_error());

    boost::intrusive_ptr<operators::operator_group_t> group(new operators::operator_group_t(&resource, log_t{}));
    operators::group_key_t key(&resource);
    key.name = std::pmr::string("k", &resource);
    key.type = operators::group_key_t::kind::column;
    key.full_path.push_back(0);
    group->add_key(std::move(key));

    // SUM($77) + $77 with nothing bound: the post-aggregate resolve used to read the
    // parameter map with .at(), an exception escaping an actor coroutine (a swallowed
    // future under NDEBUG, an abort with assertions on). It must leave via the error
    // channel like every other operator failure.
    operators::post_aggregate_column_t post{};
    post.alias = std::pmr::string("p", &resource);
    post.op = expressions::scalar_type::add;
    post.operands = std::pmr::vector<expressions::param_storage>(&resource);
    post.operands.emplace_back(core::parameter_id_t(77));
    post.operands.emplace_back(core::parameter_id_t(77));
    group->add_post_aggregate(std::move(post));

    group->set_children(make_child(&resource, std::move(chunk)));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    drive_group(group.get(), &resource, &ctx);

    REQUIRE(group->has_error());
    CHECK(group->get_error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("group operator contracts: aggregator error on empty-input global aggregate is propagated",
          "[group_contracts]") {
    // The empty-input global-aggregate branch (no left output, keys empty,
    // values present) runs each aggregator and pre-fix never checked it for
    // an error afterwards: AVG over a string argument fails kernel dispatch
    // inside the aggregator, but the operator reported success and emitted a
    // NULL row instead.
    auto resource = core::pmr::otterbrix_resource();

    auto* registry = compute::function_registry_t::get_default();
    REQUIRE(registry != nullptr);
    compute::function* avg_fn = nullptr;
    for (const auto& [name, uid] : registry->get_functions()) {
        if (name == "avg") {
            avg_fn = registry->get_function(uid);
            break;
        }
    }
    REQUIRE(avg_fn != nullptr);

    boost::intrusive_ptr<operators::operator_group_t> group(new operators::operator_group_t(&resource, log_t{}));
    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(core::parameter_id_t(1)); // AVG($1), $1 bound to a string below
    group->add_value(std::pmr::string("a", &resource),
                     operators::aggregate::operator_aggregate_ptr(
                         new operators::aggregate::operator_func_t(&resource, log_t{}, avg_fn, std::move(args))));
    // No children on purpose: left output is absent.

    logical_plan::storage_parameters params{&resource};
    logical_plan::add_parameter(params, core::parameter_id_t(1), std::string("not_a_number"));
    pipeline::context_t ctx(std::move(params));
    drive_group(group.get(), &resource, &ctx);

    REQUIRE(group->has_error());
}

// -------------------------------------------------------------- operator_func_t arguments
//
// Characterization pins for operator_func_t's ARGUMENT RESOLUTION -- the step that turns each
// expressions::param_storage argument into the column the compute kernel reads. Three argument
// SHAPES go in (a column key, a plan parameter, a nested scalar expression) and two RESOLVED
// shapes come out (a column of the input chunk, referenced not copied; a scalar value broadcast
// over the chunk's rows). Every one of them, plus DISTINCT and the empty-input placeholder
// branch, gets a pin so the resolution is fixed by behaviour rather than by reading.

TEST_CASE("aggregate func args: a column key argument sums the referenced column", "[aggregate_args]") {
    auto resource = core::pmr::otterbrix_resource();
    auto* sum_fn = find_function("sum");
    REQUIRE(sum_fn != nullptr);

    operators::chunks_vector_t chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, "v", {1, 2, 3}));

    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(column_key(&resource, "v", 0));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    ctx.function_registry = compute::function_registry_t::get_default();

    auto value = aggregate_over(&resource, ctx, sum_fn, std::move(args), std::move(chunks));
    REQUIRE(value.type().type() == types::logical_type::BIGINT);
    REQUIRE(value.value<int64_t>() == 6);
}

TEST_CASE("aggregate func args: a parameter argument is broadcast over every row", "[aggregate_args]") {
    // The scalar alternative: the argument is not a column at all but one plan-parameter VALUE,
    // referenced as a constant vector and flattened to the chunk's row count. SUM($1) over a
    // 3-row chunk is therefore 3*$1, not $1.
    auto resource = core::pmr::otterbrix_resource();
    auto* sum_fn = find_function("sum");
    REQUIRE(sum_fn != nullptr);

    operators::chunks_vector_t chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, "v", {1, 2, 3}));

    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(core::parameter_id_t(1));

    logical_plan::storage_parameters params{&resource};
    logical_plan::add_parameter(params, core::parameter_id_t(1), int64_t(5));
    pipeline::context_t ctx(std::move(params));
    ctx.function_registry = compute::function_registry_t::get_default();

    auto value = aggregate_over(&resource, ctx, sum_fn, std::move(args), std::move(chunks));
    REQUIRE(value.type().type() == types::logical_type::BIGINT);
    REQUIRE(value.value<int64_t>() == 15);
}

TEST_CASE("aggregate func args: a nested scalar expression is computed then aggregated", "[aggregate_args]") {
    // SUM(v + $1): the expression argument is evaluated into a column that is APPENDED to the
    // input chunk, referenced as the aggregate's argument, and then removed again so the chunk a
    // sibling aggregator sees is unchanged.
    auto resource = core::pmr::otterbrix_resource();
    auto* sum_fn = find_function("sum");
    REQUIRE(sum_fn != nullptr);

    operators::chunks_vector_t chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, "v", {1, 2, 3}));

    auto add_expr =
        expressions::make_scalar_expression(&resource, expressions::scalar_type::add, expressions::key_t{&resource});
    add_expr->append_param(column_key(&resource, "v", 0));
    add_expr->append_param(core::parameter_id_t(1));

    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(expressions::expression_ptr(add_expr));

    logical_plan::storage_parameters params{&resource};
    logical_plan::add_parameter(params, core::parameter_id_t(1), int64_t(1));
    pipeline::context_t ctx(std::move(params));
    ctx.function_registry = compute::function_registry_t::get_default();

    auto value = aggregate_over(&resource, ctx, sum_fn, std::move(args), std::move(chunks));
    REQUIRE(value.type().type() == types::logical_type::BIGINT);
    REQUIRE(value.value<int64_t>() == 9);
}

TEST_CASE("aggregate func args: DISTINCT deduplicates the resolved argument column", "[aggregate_args]") {
    auto resource = core::pmr::otterbrix_resource();
    auto* sum_fn = find_function("sum");
    REQUIRE(sum_fn != nullptr);

    operators::chunks_vector_t chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, "v", {1, 2, 2, 3, 3, 3}));

    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(column_key(&resource, "v", 0));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    ctx.function_registry = compute::function_registry_t::get_default();

    auto value = aggregate_over(&resource, ctx, sum_fn, std::move(args), std::move(chunks), /*distinct=*/true);
    REQUIRE(value.type().type() == types::logical_type::BIGINT);
    REQUIRE(value.value<int64_t>() == 6);
}

TEST_CASE("aggregate func args: a column key over the empty-input placeholder counts zero rows", "[aggregate_args]") {
    // The global-aggregate-over-empty branch: operator_batch_t hands one 0-COLUMN, 0-row chunk,
    // so a column key indexes past the (zero) columns. The resolution appends a 0-row numeric
    // placeholder and references that -- over zero rows the answer is type-independent, so
    // COUNT(v) is 0 rather than an assert.
    auto resource = core::pmr::otterbrix_resource();
    auto* count_fn = find_function("count");
    REQUIRE(count_fn != nullptr);

    // Deliberately empty: operator_batch_t materializes the single 0-column, 0-row chunk.
    operators::chunks_vector_t chunks(&resource);

    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(column_key(&resource, "v", 0));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    ctx.function_registry = compute::function_registry_t::get_default();

    auto value = aggregate_over(&resource, ctx, count_fn, std::move(args), std::move(chunks));
    REQUIRE(value.value<uint64_t>() == 0);
}

// ------------------------------------------------------------------- operator_raw_data_t
//
// The multi-chunk literal carrier: pins that the batch is copied across one-for-one and that the
// operator reports the resource its output actually lives on.

TEST_CASE("raw data operator: multi-chunk literal input is copied across one-for-one", "[raw_data]") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<vector::data_chunk_t> src(&resource);
    src.emplace_back(bigint_chunk(&resource, "v", {1, 2}));
    src.emplace_back(bigint_chunk(&resource, "v", {3}));

    boost::intrusive_ptr<operators::operator_raw_data_t> op(new operators::operator_raw_data_t(&resource, src));

    REQUIRE(op->resource() == &resource);
    REQUIRE(op->output());
    const auto& out = op->output()->chunks();
    REQUIRE(out.size() == 2);
    REQUIRE(out[0].size() == 2);
    REQUIRE(out[1].size() == 1);
    REQUIRE(out[0].value(0, 0).value<int64_t>() == 1);
    REQUIRE(out[0].value(0, 1).value<int64_t>() == 2);
    REQUIRE(out[1].value(0, 0).value<int64_t>() == 3);
}

TEST_CASE("raw data operator: an empty chunk list keeps the caller's resource", "[raw_data]") {
    // Rule 6/14: the multi-chunk ctor used to read its resource out of its FIRST INPUT CHUNK and
    // fall back to std::pmr::get_default_resource() when there was none, so a caller working on an
    // arena silently got an operator allocating on global new. The resource is an argument now, so
    // there is one path and no forbidden call.
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<vector::data_chunk_t> src(&resource);
    boost::intrusive_ptr<operators::operator_raw_data_t> op(new operators::operator_raw_data_t(&resource, src));

    REQUIRE(op->resource() == &resource);
    REQUIRE(op->resource() != std::pmr::get_default_resource());
}

TEST_CASE("aggregate func args: a column key survives an expression argument appended after it",
          "[aggregate_args]") {
    // An aggregate whose argument list mixes a COLUMN KEY with a nested EXPRESSION, in that order
    // (`count(v, v + 1)`).
    //
    // Resolving the expression argument APPENDS its computed column to the input chunk, and
    // chunk.data is a plain std::vector that reallocates when it grows. The column key resolved
    // BEFORE it must still address the same column afterwards — addressing it through an
    // iterator taken before the append points into the freed block and aborts the process.
    //
    // The contract pinned here is that the operator ANSWERS -- no kernel accepts this argument
    // pair, so it answers a clean core::error_t (rules 2/9), which is a query error the caller can
    // report. An abort is not an answer. The SQL surface never gets this far: function validation
    // rejects the arity first (test_correctness_bugs.cpp's multi_arg_aggregate_rejected_by_arity).
    auto resource = core::pmr::otterbrix_resource();
    auto* count_fn = find_function("count");
    REQUIRE(count_fn != nullptr);

    operators::chunks_vector_t chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, "v", {1, 2, 3}));

    auto add_expr =
        expressions::make_scalar_expression(&resource, expressions::scalar_type::add, expressions::key_t{&resource});
    add_expr->append_param(column_key(&resource, "v", 0));
    add_expr->append_param(core::parameter_id_t(1));

    std::pmr::vector<expressions::param_storage> args(&resource);
    args.emplace_back(column_key(&resource, "v", 0));
    args.emplace_back(expressions::expression_ptr(add_expr));

    logical_plan::storage_parameters params{&resource};
    logical_plan::add_parameter(params, core::parameter_id_t(1), int64_t(1));
    pipeline::context_t ctx(std::move(params));
    ctx.function_registry = compute::function_registry_t::get_default();

    auto batch = operators::make_operator_batch(&resource, std::move(chunks));
    boost::intrusive_ptr<operators::aggregate::operator_func_t> agg(
        new operators::aggregate::operator_func_t(&resource, log_t{}, count_fn, std::move(args), false));
    agg->compute(&ctx, batch);

    REQUIRE(agg->has_error());
    REQUIRE_FALSE(agg->get_error().what.empty());
}

namespace {
    // The result's column names, in output order, as plain std::string so a mismatch
    // prints both sides byte for byte.
    std::vector<std::string> result_column_names(const cursor::cursor_t& cur) {
        std::vector<std::string> names;
        names.reserve(cur.columns().size());
        for (const auto& column : cur.columns()) {
            names.emplace_back(column.name.data(), column.name.size());
        }
        return names;
    }
} // namespace

// operator_group_t builds its output column names from two carriers at once: the
// plan-stamped output schema (positions the plan owns) and the operator's own
// per-aggregate `values_[a].name` (positions past the plan's end -- internal
// aggregates). This pins what a user actually sees, byte for byte, on every shape
// that reaches the group operator: a plain GROUP BY key, an aliased aggregate, an
// unaliased aggregate (the name the SQL layer synthesizes), a post-aggregate
// arithmetic column, and the empty-group / global-aggregate-over-zero-rows path,
// which is a SEPARATE emit site (empty_aggregate_result) with its own copy of the
// naming.
TEST_CASE("group operator contracts: output column names", "[group_contracts]") {
    auto config = test_create_config(test_temp_path("test_group_operator_contracts/names"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE gnamedb;")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE gnamedb.t (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE gnamedb.empty_t (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(
        dispatcher->execute_sql(session, "INSERT INTO gnamedb.t (k, v) VALUES (1, 10), (1, 20), (2, 30);")
            ->is_success());

    SECTION("plain GROUP BY: key name and synthesized aggregate name") {
        auto cur = dispatcher->execute_sql(session, "SELECT k, COUNT(*) FROM gnamedb.t GROUP BY k;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{"k", "count"});
    }

    SECTION("aggregate alias") {
        auto cur = dispatcher->execute_sql(session, "SELECT k, SUM(v) AS total FROM gnamedb.t GROUP BY k;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{"k", "total"});
    }

    SECTION("post-aggregate arithmetic column") {
        auto cur = dispatcher->execute_sql(session, "SELECT k, SUM(v) + 1 AS bumped FROM gnamedb.t GROUP BY k;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{"k", "bumped"});
    }

    SECTION("internal aggregate: HAVING over an aggregate absent from the select list") {
        // SUM(v) becomes an INTERNAL aggregate: it sits past the plan's output
        // columns, is named from values_[a].name, and is erased before output. The
        // pin is that it does NOT appear in the result and does not shift the names
        // of the columns that do.
        auto cur = dispatcher->execute_sql(session, "SELECT k FROM gnamedb.t GROUP BY k HAVING SUM(v) > 15;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{"k"});
    }

    SECTION("global aggregate over rows") {
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(*), SUM(v) AS s FROM gnamedb.t;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{"count", "s"});
    }

    SECTION("empty group: global aggregate over zero rows") {
        // empty_aggregate_result(): a separate emit site with its own copy of the
        // naming, reached only when no input row ever arrives.
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(*), SUM(v) AS s FROM gnamedb.empty_t;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{"count", "s"});
    }

    SECTION("empty group: GROUP BY over zero rows") {
        // Today a keyed GROUP BY over zero rows answers with NO columns at all --
        // materialize_groups() never runs, so nothing names anything. That is the
        // known empty-result-schema hole, pinned here as current behaviour so that a
        // change to it has to say so rather than happen in passing.
        auto cur = dispatcher->execute_sql(session, "SELECT k, COUNT(*) AS n FROM gnamedb.empty_t GROUP BY k;");
        REQUIRE(cur->is_success());
        CHECK(result_column_names(*cur) == std::vector<std::string>{});
    }
}
