#pragma once

#include <components/logical_plan/node.hpp>
#include <components/planner/connector.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>

// What an embedding host plugs in at open_engine: optimizer rules (plain function pointers, copied into every
// executor, so they carry no host state) and its connector factory.
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

    // A rule recognizes the tables it serves by the owner tag of their storage (a read node's table_metadata()), and
    // reads a parameter's value at run time from the operator's context, so it gets the tree only.
    using optimizer_rule_fn = logical_plan::node_ptr (*)(std::pmr::memory_resource*, logical_plan::node_ptr);

    // Within one stage the rules run in registration order.
    struct optimizer_rule_t {
        optimizer_stage stage;
        optimizer_rule_fn apply;
    };

    // Read once by open_engine: every executor copies the rules and makes its own connector, so the array need not
    // outlive the call; the factory and what it shares live longer than the engine. No factory: a host without
    // connections (a database with a connection is then refused when a statement reaches its table).
    struct primitives_t final {
        std::span<const optimizer_rule_t> optimizer_rules{};
        const connector_factory_t* connector{nullptr};
    };

} // namespace components::planner
