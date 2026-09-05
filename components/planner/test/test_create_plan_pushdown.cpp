// create_plan aggregate-pushdown lowering.
//
// An aggregate_t node whose group_t child carries pushdown()==true lowers to an
// operator_group_merge (the coordinator-side aggregate terminal) over a pushed_reduce_scan
// (the source shipping the POD spec on the dedicated storage_reduce leg): the owning agent
// reduces its own slice, the merge passes the final rows through and owns the empty-input
// scalar row. The SAME node with the flag cleared lowers to the normal aggregate chain (the
// group operator, which tags itself operator_type::aggregate) — routing is gated purely on
// the flag.

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/scan/pushed_reduce_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/date/date_types.hpp>

#include "pushdown_plan_builders.hpp"

#include <memory_resource>

using namespace components::logical_plan;
using namespace components::expressions;
namespace op = components::operators;

namespace {

    using planner_test::dbn;
    using planner_test::reln;

    // Build a minimal pushdown-shaped plan:
    //   aggregate_t (table_oid 123)
    //     └─ group_t (pushdown = `pushdown`)  [one GROUP BY key, no aggregate func]
    // Deliberately no aggregate function expression so the non-pushdown chain builds
    // through create_plan_group without needing a registered function. The aggregate-node
    // wrapping is the shared planner_test::make_agg.
    node_ptr build_agg(std::pmr::memory_resource* r, bool pushdown) {
        auto grp_field =
            make_scalar_expression(r, scalar_type::group_field, components::expressions::key_t(r, "g", side_t::left));
        std::vector<expression_ptr> group_exprs{expression_ptr(grp_field)};
        auto group = make_node_group(r, dbn(), reln(), group_exprs);
        group->set_pushdown(pushdown);
        group->set_table_oid(components::catalog::oid_t{123});

        return planner_test::make_agg(r, group, components::catalog::oid_t{123});
    }

} // namespace

TEST_CASE("create_plan: aggregate with pushdown group child lowers to merge over pushed_reduce_scan") {
    core::pmr::otterbrix_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto node = build_agg(&resource, /*pushdown=*/true);
    auto plan =
        services::planner::create_plan(context, registry, node, components::logical_plan::limit_t::unlimit(), nullptr);

    REQUIRE(plan != nullptr);
    // The plan keeps a truthful aggregate-shaped terminal (group_merge) whose child is
    // the pushed_reduce_scan source that ships the spec on storage_reduce.
    REQUIRE(plan->type() == op::operator_type::group_merge);
    REQUIRE(plan->left() != nullptr);
    REQUIRE(plan->left()->type() == op::operator_type::pushed_reduce_scan);
}

TEST_CASE("create_plan: aggregate WITHOUT pushdown lowers to the normal aggregate chain") {
    core::pmr::otterbrix_resource resource;
    services::context_storage_t context(&resource, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(&resource);

    auto node = build_agg(&resource, /*pushdown=*/false);
    auto plan =
        services::planner::create_plan(context, registry, node, components::logical_plan::limit_t::unlimit(), nullptr);

    REQUIRE(plan != nullptr);
    // The normal chain never produces the pushed pair; the group operator tags itself
    // operator_type::aggregate.
    REQUIRE(plan->type() != op::operator_type::group_merge);
    REQUIRE(plan->type() == op::operator_type::aggregate);
}
