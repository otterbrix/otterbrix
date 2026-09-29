#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <cstdint>
#include <memory_resource>

// What an embedding host plugs in at spawn_engine. Plain function pointers: the engine copies them into every
// executor, so they carry no host state.
namespace components::planner {

    // Seams of optimize(), in pipeline order; a rule runs after the built-in rules named here.
    enum class optimizer_stage : std::uint8_t
    {
        after_simplify,           // fold_constants, drop_redundant_distinct, promote_cross_joins
        after_filters_and_joins,  // pushdown_cte_filter, pushdown_filter, rewrite_hash_joins
        after_limit,              // eager_aggregation, pushdown_limit
        after_aggregate_pushdown, // pushdown_aggregate
        last                      // prune_columns
    };

    struct optimizer_rule_context_t {
        const logical_plan::catalog_resolves_t* resolves;
        const logical_plan::parameter_node_t* parameters;
        bool can_push_to_agent;
    };

    using optimizer_rule_fn = logical_plan::node_ptr (*)(std::pmr::memory_resource*,
                                                         logical_plan::node_ptr,
                                                         const optimizer_rule_context_t&);

    // Within one stage the rules run in registration order.
    struct optimizer_rule_t {
        optimizer_stage stage;
        optimizer_rule_fn apply;
    };

} // namespace components::planner
