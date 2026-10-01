#pragma once

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>
#include <string_view>

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

    // A table name of the statement that the catalog did not resolve.
    struct unresolved_table_t {
        std::string_view dbname;
        std::string_view schema;
        std::string_view relname;
    };

    // Name resolution, phase "need": the host reads the tree and the unresolved names and answers the reads it
    // needs, as logical plans over its own tables. The engine runs them in the statement's snapshot.
    using name_resolution_need_fn = core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>> (*)(
        std::pmr::memory_resource*,
        const logical_plan::node_ptr& tree,
        std::span<const unresolved_table_t> unresolved);

    // Phase "decide": the rows of every read, in request order; answers the tree to validate.
    using name_resolution_decide_fn = core::result_wrapper_t<logical_plan::node_ptr> (*)(
        std::pmr::memory_resource*,
        logical_plan::node_ptr tree,
        std::span<const unresolved_table_t> unresolved,
        std::span<const std::pmr::vector<vector::data_chunk_t>> read_results);

    inline core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>>
    no_name_reads(std::pmr::memory_resource* resource,
                  const logical_plan::node_ptr&,
                  std::span<const unresolved_table_t>) {
        return std::pmr::vector<logical_plan::execution_plan_t>{resource};
    }

    inline core::result_wrapper_t<logical_plan::node_ptr>
    keep_tree(std::pmr::memory_resource*,
              logical_plan::node_ptr tree,
              std::span<const unresolved_table_t>,
              std::span<const std::pmr::vector<vector::data_chunk_t>>) {
        return tree;
    }

    struct name_resolution_hook_t {
        name_resolution_need_fn need = &no_name_reads;
        name_resolution_decide_fn decide = &keep_tree;
    };

} // namespace components::planner
