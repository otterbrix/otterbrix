#include <catch2/catch.hpp>

#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>
#include <components/physical_plan/operators/operator_group.hpp>

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

} // namespace

TEST_CASE("group operator contracts: unresolved column key surfaces operator error", "[group_contracts]") {
    // Pre-fix this aborted in Debug via assert(!key.full_path.empty()) in
    // extract_key_value (operator_group.cpp); in Release the assert is compiled
    // out and chunk.value(empty_path, row) is UB.
    auto resource = std::pmr::synchronized_pool_resource();

    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("k");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(2)));
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
    group->on_execute(&ctx);

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
    auto resource = std::pmr::synchronized_pool_resource();

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

    boost::intrusive_ptr<operators::operator_group_t> group(new operators::operator_group_t(&resource, log_t{}));
    operators::group_key_t key(&resource);
    key.name = std::pmr::string("kf", &resource);
    key.type = operators::group_key_t::kind::column;
    key.full_path.push_back(0); // struct column
    key.full_path.push_back(0); // field "f"
    group->add_key(std::move(key));
    group->set_children(make_child(&resource, std::move(chunk)));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});
    group->on_execute(&ctx);

    REQUIRE_FALSE(group->has_error());
    REQUIRE(group->output());
    auto& out = group->output()->data_chunk();
    REQUIRE(out.size() == 3);
    REQUIRE(out.column_count() == 1);
    // The key column type must be the field's type walked through the input
    // schema, not the NA type of the first (NULL) group key.
    REQUIRE(out.data[0].type().type() == types::logical_type::BIGINT);
    REQUIRE(out.data[0].is_null(0));
    REQUIRE(out.value(0, 1).value<int64_t>() == 10);
    REQUIRE(out.value(0, 2).value<int64_t>() == 20);
}

TEST_CASE("group operator contracts: aggregator error on empty-input global aggregate is propagated",
          "[group_contracts]") {
    // The empty-input global-aggregate branch (no left output, keys empty,
    // values present) runs each aggregator and pre-fix never checked it for
    // an error afterwards: AVG over a string argument fails kernel dispatch
    // inside the aggregator, but the operator reported success and emitted a
    // NULL row instead.
    auto resource = std::pmr::synchronized_pool_resource();

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
    group->on_execute(&ctx);

    REQUIRE(group->has_error());
}
