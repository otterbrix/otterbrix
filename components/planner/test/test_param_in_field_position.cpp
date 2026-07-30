// The physical-plan half of the `get_field`-carrying-a-parameter contract.
//
// The operand of a field reference names a COLUMN; a bound parameter names a value. They
// are different alternatives of the same slot, and the readers below used to take the key
// alternative unconditionally — a throw today, a read of the wrong union member once the
// slot stops being a std::variant. SQL never builds this shape (a projected `$n` is a
// constant), so these pin the DEFENSIVE contract for a hand-built or extension-built plan:
// a clean refusal, never a crash.
//
// create_plan's refusal channel is a null operator_ptr, which services/collection/executor
// turns into an error cursor; transform_predicate's is core::result_wrapper_t.

#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/date_types.hpp>

#include "pushdown_plan_builders.hpp"

#include <memory_resource>
#include <vector>

using namespace components::logical_plan;
using namespace components::expressions;
namespace types = components::types;
using ekey = components::expressions::key_t;

namespace {

    using planner_test::dbn;
    using planner_test::reln;

    // `get_field` carrying a bound parameter where a column key belongs.
    expression_ptr poisoned_field(std::pmr::memory_resource* r, parameter_node_t* params, scalar_type type) {
        auto scalar = make_scalar_expression(r, type, ekey{r, "a"});
        auto id = params->add_parameter(types::logical_value_t{r, int64_t{1}});
        scalar->append_param(id);
        return expression_ptr{scalar};
    }

    components::vector::schema_t one_bigint_column(std::pmr::memory_resource* r) {
        components::vector::schema_t schema(r);
        components::vector::column_schema_t column(r);
        column.name = std::pmr::string{"a", r};
        column.type = types::complex_logical_type{types::logical_type::BIGINT};
        schema.push_back(std::move(column));
        return schema;
    }

    // One BIGINT column named "a", one row — a raw-data source that lowers cleanly.
    node_ptr one_column_source(std::pmr::memory_resource* r) {
        std::pmr::vector<types::complex_logical_type> cols(r);
        cols.emplace_back(types::logical_type::BIGINT);
        components::vector::data_chunk_t chunk(r, cols, 1);
        chunk.set_cardinality(1);
        chunk.set_column_name(0, "a");
        REQUIRE_FALSE(chunk.set_value(uint64_t{0}, uint64_t{0}, types::logical_value_t{r, int64_t{1}}).contains_error());
        return make_node_raw_data(r, std::move(chunk));
    }

} // namespace

TEST_CASE("create_plan: a select projection whose field operand is a parameter is refused") {
    std::pmr::synchronized_pool_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto params = make_parameter_node(&resource);
    context.parameters = &params->parameters();

    auto select = make_node_select(&resource, dbn(), reln());
    select->append_expression(poisoned_field(&resource, params.get(), scalar_type::get_field));

    auto plan = services::planner::create_plan(context, registry, select, limit_t::unlimit(), &params->parameters());

    REQUIRE(plan == nullptr);
}

TEST_CASE("create_plan: a group key whose field operand is a parameter is refused") {
    std::pmr::synchronized_pool_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto params = make_parameter_node(&resource);
    context.parameters = &params->parameters();

    std::vector<expression_ptr> group_exprs{poisoned_field(&resource, params.get(), scalar_type::get_field)};
    auto group = make_node_group(&resource, dbn(), reln(), group_exprs);

    auto plan = services::planner::create_plan(context, registry, group, limit_t::unlimit(), &params->parameters());

    REQUIRE(plan == nullptr);
}

TEST_CASE("transform_predicate: an ANY/ALL whose operands are the wrong alternatives is an error") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    auto params = make_parameter_node(res);
    const auto id = params->add_parameter(types::logical_value_t{res, int64_t{1}});
    const auto schema = one_bigint_column(res);

    // ANY expects (column key, bound parameter). Hand it the mirror image of each operand.
    SECTION("left operand is a parameter, not a column") {
        auto expr = make_compare_expression(res, compare_type::any, id, id);
        expr->set_inner_op(compare_type::eq);
        auto filter = components::operators::transform_predicate(res,
                                                                 expr,
                                                                 schema,
                                                                 &params->parameters(),
                                                                 core::date::timezone_offset_t{});
        REQUIRE(filter.has_error());
        CHECK_FALSE(filter.error().what.empty());
    }

    SECTION("right operand is a column, not a parameter") {
        auto expr = make_compare_expression(res, compare_type::all, ekey{res, "a"}, ekey{res, "a"});
        expr->set_inner_op(compare_type::eq);
        auto filter = components::operators::transform_predicate(res,
                                                                 expr,
                                                                 schema,
                                                                 &params->parameters(),
                                                                 core::date::timezone_offset_t{});
        REQUIRE(filter.has_error());
        CHECK_FALSE(filter.error().what.empty());
    }
}

// ----------------------------------------------------------------------------
// The aggregate wrapper must propagate its children's refusals. Every real
// SELECT lowers through node_aggregate, so a refusal that only the direct
// select/group path honors is a refusal the engine never actually delivers:
// the projection (or grouping, or ordering) would silently vanish from the
// plan and the query would run without it.
// ----------------------------------------------------------------------------

TEST_CASE("create_plan: an aggregate wrapping a refused select is itself refused") {
    std::pmr::synchronized_pool_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto params = make_parameter_node(&resource);
    context.parameters = &params->parameters();

    auto select = make_node_select(&resource, dbn(), reln());
    select->append_expression(poisoned_field(&resource, params.get(), scalar_type::get_field));

    auto agg = make_node_aggregate(&resource, dbn(), reln());
    agg->append_child(one_column_source(&resource));
    agg->append_child(select);

    auto plan = services::planner::create_plan(context, registry, agg, limit_t::unlimit(), &params->parameters());

    REQUIRE(plan == nullptr);
}

TEST_CASE("create_plan: an aggregate wrapping a refused group is itself refused") {
    std::pmr::synchronized_pool_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto params = make_parameter_node(&resource);
    context.parameters = &params->parameters();

    std::vector<expression_ptr> group_exprs{poisoned_field(&resource, params.get(), scalar_type::get_field)};
    auto group = make_node_group(&resource, dbn(), reln(), group_exprs);

    auto agg = make_node_aggregate(&resource, dbn(), reln());
    agg->append_child(one_column_source(&resource));
    agg->append_child(group);

    auto plan = services::planner::create_plan(context, registry, agg, limit_t::unlimit(), &params->parameters());

    REQUIRE(plan == nullptr);
}

TEST_CASE("create_plan: an unlowerable earlier source child invalidates the whole plan") {
    std::pmr::synchronized_pool_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto params = make_parameter_node(&resource);
    context.parameters = &params->parameters();

    // create_database_t has no lowering (create_plan's default arm answers nullptr).
    // With two non-role children the role classifier keeps only the LAST as the source;
    // an aggregate must still refuse when ANY of them fails to lower, not silently
    // execute over the one that happened to come last.
    auto agg = make_node_aggregate(&resource, dbn(), reln());
    agg->append_child(make_node_create_database(&resource, core::dbname_t{std::string{"db"}}));
    agg->append_child(one_column_source(&resource));

    auto plan = services::planner::create_plan(context, registry, agg, limit_t::unlimit(), &params->parameters());

    REQUIRE(plan == nullptr);
}

TEST_CASE("create_plan: a constant projection whose parameter id is unbound is refused") {
    std::pmr::synchronized_pool_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto params = make_parameter_node(&resource);
    context.parameters = &params->parameters();

    // A constant column referencing an id the parameter node never bound. The SQL
    // transformer always registers a slot, so this is the hand-built/extension shape;
    // it used to throw std::out_of_range out of the operator-build path.
    auto scalar = make_scalar_expression(&resource, scalar_type::constant, ekey{&resource});
    scalar->append_param(core::parameter_id_t{77});

    auto select = make_node_select(&resource, dbn(), reln());
    select->append_expression(expression_ptr{scalar});

    auto plan = services::planner::create_plan(context, registry, select, limit_t::unlimit(), &params->parameters());

    REQUIRE(plan == nullptr);
}
