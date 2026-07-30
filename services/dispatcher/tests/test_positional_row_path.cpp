#include <catch2/catch_test_macros.hpp>

#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <memory_resource>
#include <vector>

// ============================================================================
// A STRUCT field's name is part of the type's SHAPE, so the STRUCT descent
// inside find_types matches the next path element against each child type's
// name. A POSITIONAL ROW(10, 20) has no field names at all: the parser builds
// it with logical_value_t::create_struct(resource, "", fields), whose child
// types are the field literals' own types — bare, extension-less
// complex_logical_types. Asking such a type for its alias() asserts in Debug
// and dereferences null in Release, so the descent has to ask TOTALLY
// (name_carried_by_type) and answer "this field has no name" instead.
//
// This is the shape SQL cannot currently reach end to end — a ROW(...) is
// accepted only inside INSERT ... VALUES, a computing table refuses STRUCT
// columns outright ("complex types are not yet supported on relkind='g'"), and
// a declared STRUCT column takes its field names from its declared type — so
// the walk is pinned here, at the altitude where it actually happens, against
// the exact value the parser produces.
// ============================================================================

using namespace components;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;

TEST_CASE("services::dispatcher::positional_row::named_field_of_nameless_row_is_not_found") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    // Exactly what the transformer builds for ROW(10, 20) (utils.cpp, T_RowExpr).
    std::vector<logical_value_t> fields;
    fields.emplace_back(res, int64_t{10});
    fields.emplace_back(res, int64_t{20});
    auto row = logical_value_t::create_struct(res, "", fields);
    REQUIRE(row.type().type() == logical_type::STRUCT);
    REQUIRE(row.type().child_types().size() == 2);
    // The premise of the whole test: the fields carry no name to match against.
    REQUIRE(row.type().child_types()[0].field_name().empty());
    REQUIRE(row.type().child_types()[1].field_name().empty());

    // A one-column scan whose column "s" holds that positional ROW.
    std::pmr::vector<complex_logical_type> types(res);
    types.emplace_back(row.type());
    types.back().set_field_name("s");
    vector::data_chunk_t chunk(res, types, 1);
    chunk.set_cardinality(1);
    REQUIRE_FALSE(chunk.set_value(uint64_t{0}, uint64_t{0}, row).contains_error());

    // SELECT s.x — the descent visits both nameless fields looking for "x".
    std::pmr::vector<std::pmr::string> path(res);
    path.emplace_back("s");
    path.emplace_back("x");
    expressions::key_t field_key{std::move(path)};

    auto select = logical_plan::make_node_select(res, core::dbname_t{}, core::relname_t{});
    select->append_expression(
        expressions::make_scalar_expression(res, expressions::scalar_type::get_field, std::move(field_key)));

    auto agg = logical_plan::make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(logical_plan::make_node_raw_data(res, std::move(chunk)));
    agg->append_child(select);

    auto params = logical_plan::make_parameter_node(res);
    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params->parameters());

    // Total, not fatal: the field has no name, so the path simply does not resolve.
    REQUIRE(validated.has_error());
    INFO("error: " << validated.error().what);
    CHECK(validated.error().type == core::error_code_t::field_not_exists);
}
