#pragma once

// Shared plan builders for the planner aggregate-pushdown tests. All three suites
// (test_optimizer, test_pushed_spec_build, test_create_plan_pushdown) construct the
// same aggregate_t -> group_t pushdown shape over the ("database", "collection")
// table, so the aggregate-node wrapper (make_agg) and the canonical scalar+count
// group builder (make_agg_group) live here in ONE copy. Test-only header.

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace planner_test {

    inline core::dbname_t dbn() { return core::dbname_t{std::string{"database"}}; }
    inline core::relname_t reln() { return core::relname_t{std::string{"collection"}}; }

    // Wrap `group` in an aggregate_t at `table_oid` (no output types, not distinct).
    // When `having != nullptr`, attach a node_having_t as an aggregate CHILD (HAVING is
    // first-class, not carried inside node_group) so pushdown gates that scan the
    // aggregate's children detect it.
    inline components::logical_plan::node_aggregate_ptr
    make_agg(std::pmr::memory_resource* r,
             const components::logical_plan::node_group_ptr& group,
             components::catalog::oid_t table_oid,
             components::expressions::expression_ptr having = nullptr) {
        auto agg = components::logical_plan::make_node_aggregate(r, dbn(), reln());
        agg->set_table_oid(table_oid);
        agg->append_child(group);
        if (having != nullptr) {
            agg->append_child(components::logical_plan::make_node_having(r, dbn(), reln(), having));
        }
        return agg;
    }

    // Extended form: additionally stamp the aggregate DISTINCT flag and (when non-empty)
    // the plan-resolved output types — the extra state test_pushed_spec_build asserts on.
    inline components::logical_plan::node_aggregate_ptr
    make_agg(std::pmr::memory_resource* r,
             const components::logical_plan::node_group_ptr& group,
             components::catalog::oid_t table_oid,
             std::pmr::vector<components::types::complex_logical_type> out_types,
             bool agg_distinct) {
        auto agg = components::logical_plan::make_node_aggregate(r, dbn(), reln());
        agg->set_table_oid(table_oid);
        agg->set_distinct(agg_distinct);
        if (!out_types.empty()) {
            agg->set_output_types(std::move(out_types));
        }
        agg->append_child(group);
        return agg;
    }

    // The canonical scalar+count aggregate group used by the optimizer pushdown tests.
    // `with_group_key` adds a scalar group_field (grouped shape) vs none (scalar shape).
    inline components::logical_plan::node_group_ptr
    make_agg_group(std::pmr::memory_resource* r, bool with_group_key, bool distinct_agg) {
        using namespace components::expressions;
        using key = components::expressions::key_t;
        std::vector<expression_ptr> exprs;
        if (with_group_key) {
            exprs.push_back(make_scalar_expression(r, scalar_type::group_field, key(r, "g")));
        }
        auto sum = make_aggregate_expression(r, "sum", key(r, "s"));
        sum->append_param(key(r, "v"));
        // Stamp the resolved fragment-merge capability the validator would set for a
        // builtin SUM/COUNT (the optimizer now reads is_mergeable() rather than a
        // hardcoded name list). Tests build the plan directly, bypassing validate,
        // so mirror the stamp here.
        sum->set_mergeable(true);
        exprs.push_back(expression_ptr(sum));
        auto cnt = make_aggregate_expression(r, "count", key(r, "c"));
        cnt->set_distinct(distinct_agg);
        cnt->set_mergeable(true);
        cnt->append_param(key(r, "v"));
        exprs.push_back(expression_ptr(cnt));
        return components::logical_plan::make_node_group(r, dbn(), reln(), exprs);
    }

} // namespace planner_test
