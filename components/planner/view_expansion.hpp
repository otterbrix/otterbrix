#pragma once

// SELECT-time view expansion splices the body in place (rather than swapping in the outer plan)
// so everything built above the view (WHERE, projection, aggregate, join) survives.

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>

namespace components::planner {

    struct view_reference_t {
        logical_plan::node_aggregate_t* node{nullptr};
        const logical_plan::resolve_entry_t* entry{nullptr};
    };

    // Only aggregate_t is a splice site: match_t/sort_t/group_t/limit_t also carry a relname, but
    // splicing there would hang the body under a clause instead of the consumer.
    std::pmr::vector<view_reference_t> collect_view_references(std::pmr::memory_resource* resource,
                                                               const logical_plan::catalog_resolves_t& resolves,
                                                               logical_plan::node_t* root);

    struct view_body_t {
        logical_plan::node_ptr plan;
        logical_plan::parameter_node_ptr params;
        // The body's own catalog lookups; the caller must merge these and run another resolve round.
        std::optional<logical_plan::catalog_resolves_t> resolves;
        // Set when re-parse / re-transform failed; `plan` is then null.
        core::error_t error{core::error_t::no_error()};
    };

    // Each reference gets its own body -- filter pushdown appends a match child into it, so two
    // references cannot share a subtree (same policy as CTE inlining in optimizer.cpp).
    view_body_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql);

    // Spliced at position 0 (appending would silently disable filter pushdown, which reads
    // children()[0] as the source). Refuses a correlated (LATERAL) `body`: node_join_t::correlations()
    // is const-only, so its parameter ids can't be renumbered and would collide with the outer plan's.
    core::error_t splice_view_body(logical_plan::node_aggregate_t* ref, logical_plan::node_ptr body);

    // Without this refusal, INSERT/UPDATE/DELETE against a view would report success and write nothing.
    core::error_t reject_view_dml_target(const logical_plan::catalog_resolves_t& resolves,
                                         const logical_plan::node_t* root);

    // Unrenumbered `#0`s collide: `WHERE col_b > 18` over a body `WHERE col_b > 10` would run the body against 18.
    void renumber_body_parameters(std::pmr::memory_resource* resource,
                                  logical_plan::node_t* body,
                                  const logical_plan::parameter_node_ptr& body_params,
                                  const logical_plan::parameter_node_ptr& out_params);

    // A true cycle should be impossible, but this is the loud stop instead of an endless resolve loop.
    inline constexpr std::size_t max_view_expansion_depth = 16;

} // namespace components::planner
