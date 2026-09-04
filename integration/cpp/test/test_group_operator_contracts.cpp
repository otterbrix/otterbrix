#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>
#include <components/physical_plan/operators/operator_hash_group.hpp>

#include <memory_resource>

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
    void drive_group(operators::operator_hash_group_t* group,
                     std::pmr::memory_resource* resource,
                     pipeline::context_t* ctx) {
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

} // namespace

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
    fields.back().set_alias("f");
    auto struct_type = types::complex_logical_type::create_struct("s", fields, "s");

    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.push_back(struct_type);
    vector::data_chunk_t chunk(&resource, cols, 4);
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

    boost::intrusive_ptr<operators::operator_hash_group_t> group(
        new operators::operator_hash_group_t(&resource, log_t{}));
    expressions::key_t key{&resource, std::string("kf")};
    std::pmr::vector<size_t> key_path{&resource};
    key_path.push_back(0); // struct column
    key_path.push_back(0); // field "f"
    key.set_path(std::move(key_path));
    group->add_key(operators::projected_column_t{&resource, "kf", expressions::param_storage{key}});
    // A grouping key is not an output on its own — a group emits its TARGET LIST — so the column
    // under test has to be named, exactly as create_plan_group does for `SELECT s.f ... GROUP BY s.f`.
    expressions::key_t output_key{&resource, std::string("kf")};
    std::pmr::vector<size_t> output_path{&resource};
    output_path.push_back(0); // struct column
    output_path.push_back(0); // field "f"
    output_key.set_path(std::move(output_path));
    group->add_output(expressions::make_scalar_expression(&resource, expressions::scalar_type::get_field, output_key));
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
    compute::function_uid avg_uid = compute::invalid_function_uid;
    for (const auto& [name, uid] : registry->get_functions()) {
        if (name == "avg") {
            avg_uid = uid;
            break;
        }
    }
    REQUIRE(avg_uid != compute::invalid_function_uid);

    boost::intrusive_ptr<operators::operator_hash_group_t> group(
        new operators::operator_hash_group_t(&resource, log_t{}));
    // The reduce IS an aggregate node in the group's graph, so the OUTPUT carries the aggregate
    // expression; add_value only records that a reduction exists. Both stamps the builder demands
    // of validation (uid + result type) are applied here by hand.
    const components::types::complex_logical_type double_type{components::types::logical_type::DOUBLE};
    expressions::key_t agg_key{&resource, "a"};
    auto aggregate = expressions::make_aggregate_expression(&resource, "avg", agg_key);
    aggregate->add_function_uid(avg_uid);
    aggregate->set_result_type(double_type);
    aggregate->append_param(core::parameter_id_t(1)); // AVG($1), $1 bound to a string below
    group->add_value(std::pmr::string("a", &resource), double_type);
    group->add_output(expressions::expression_ptr(aggregate));
    // No children on purpose: left output is absent.

    logical_plan::storage_parameters params{&resource};
    logical_plan::add_parameter(params, core::parameter_id_t(1), std::string("not_a_number"));
    pipeline::context_t ctx(std::move(params));
    drive_group(group.get(), &resource, &ctx);

    REQUIRE(group->has_error());
}
