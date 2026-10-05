#pragma once

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/logical_plan/table_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>

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

    // A rule recognizes the tables it serves by the owner tag of their storage (a read node's table_metadata()), and
    // reads a parameter's value at run time from the operator's context, so it gets the tree only.
    using optimizer_rule_fn = logical_plan::node_ptr (*)(std::pmr::memory_resource*, logical_plan::node_ptr);

    // Within one stage the rules run in registration order.
    struct optimizer_rule_t {
        optimizer_stage stage;
        optimizer_rule_fn apply;
    };

    // Name resolution, phase "need": the host reads the tree and the unresolved names and answers the reads it
    // needs, as logical plans over its own tables. The engine runs them in the statement's snapshot.
    using name_resolution_need_fn = core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>> (*)(
        std::pmr::memory_resource*,
        const logical_plan::node_ptr& tree,
        std::span<const qualified_name_t> unresolved);

    // decide's answer for one unresolved name: the declared columns (named by their type alias, lower case as an
    // unquoted reference is) and the storage that reads and writes them in this statement. A null storage is
    // "not mine": the name stays unresolved and is refused as "does not exist".
    struct table_storage_answer_t {
        std::pmr::vector<types::complex_logical_type> columns;
        logical_plan::table_storage_ptr storage{nullptr, core::pmr::polymorphic_deleter_t{nullptr, 0, 0}};
    };

    // Phase "decide": the rows of every read, in request order; one answer per unresolved name, in its order. The
    // tree is not the host's to change. `explicit_transaction`: the statement runs inside BEGIN ... COMMIT, where a
    // ROLLBACK does not undo what the storage wrote (#663); the storage decides what it allows there.
    using name_resolution_decide_fn = core::result_wrapper_t<std::pmr::vector<table_storage_answer_t>> (*)(
        std::pmr::memory_resource*,
        std::span<const qualified_name_t> unresolved,
        std::span<const std::pmr::vector<vector::data_chunk_t>> read_results,
        bool explicit_transaction);

    inline core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>>
    no_name_reads(std::pmr::memory_resource* resource,
                  const logical_plan::node_ptr&,
                  std::span<const qualified_name_t>) {
        return std::pmr::vector<logical_plan::execution_plan_t>{resource};
    }

    inline core::result_wrapper_t<std::pmr::vector<table_storage_answer_t>>
    no_storages(std::pmr::memory_resource* resource,
                std::span<const qualified_name_t> unresolved,
                std::span<const std::pmr::vector<vector::data_chunk_t>>,
                bool) {
        std::pmr::vector<table_storage_answer_t> answers{resource};
        answers.reserve(unresolved.size());
        for (std::size_t i = 0; i < unresolved.size(); ++i) {
            answers.push_back(table_storage_answer_t{std::pmr::vector<types::complex_logical_type>{resource}});
        }
        return answers;
    }

    struct name_resolution_hook_t {
        name_resolution_need_fn need = &no_name_reads;
        name_resolution_decide_fn decide = &no_storages;
    };

    // Read once by spawn_engine: every executor copies the rules, so the host's array need not outlive the call.
    struct primitives_t final {
        std::span<const optimizer_rule_t> optimizer_rules{};
        name_resolution_hook_t name_resolution{};
    };

} // namespace components::planner
