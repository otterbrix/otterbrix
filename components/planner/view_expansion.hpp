#pragma once

// SELECT-time view expansion: after catalog resolve stamps view_sql on a relkind='v' entry, the
// reference is spliced in place (body becomes its child) rather than swapped for the outer plan —
// a swap would silently drop everything built above the view (WHERE, projection, aggregate, join).
// Matches the CTE-inlining shape (transform_select.cpp::transform_from_element) so the rest of the
// pipeline needs no changes. Pure functions; the executor drives the async resolve round.

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>

namespace components::planner {

    // A place in the plan where a view is read, together with the resolved catalog
    // entry that says which view it is.
    struct view_reference_t {
        logical_plan::node_aggregate_t* node{nullptr};
        const logical_plan::resolve_entry_t* entry{nullptr};
    };

    // Every aggregate_t in `root` naming a resolved relkind='v' entry with non-empty view_sql.
    // ONLY aggregate_t is a splice site: match_t/sort_t/group_t/limit_t also carry a relname but
    // splicing there would hang the body under a clause instead of the consumer (the transformer
    // always turns a FROM item into an aggregate_t — transform_from_element). relkind='m'
    // (materialized view) is a real heap read like a table, so it's excluded.
    std::pmr::vector<view_reference_t> collect_view_references(std::pmr::memory_resource* resource,
                                                               const logical_plan::catalog_resolves_t& resolves,
                                                               logical_plan::node_t* root);

    // A fresh logical plan parsed and transformed from a view's body SQL, ready to be
    // spliced in place of the reference.
    struct view_body_t {
        logical_plan::node_ptr plan;
        logical_plan::parameter_node_ptr params;
        // The body's own catalog lookups (its FROM tables). The caller merges these
        // into the outer plan's and runs another resolve round for whatever is new.
        std::optional<logical_plan::catalog_resolves_t> resolves;
        // Set when re-parse / re-transform failed; `plan` is then null.
        core::error_t error{core::error_t::no_error()};
    };

    // Parses and transforms `view_sql` into a fresh plan per call — each reference gets its own body,
    // since filter pushdown appends a match child into it and two references cannot share a subtree
    // (same policy as CTE inlining in optimizer.cpp).
    view_body_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql);

    // Put `body` under `ref` at position 0 and stop `ref` being a source. Position matters: filter
    // pushdown reads children()[0] as the source and scans clauses from index 1 — appending at the
    // end would silently disable pushdown into the body.
    //
    // Refuses when `body` has a correlated (LATERAL) join: node_join_t::correlations() is const-only,
    // so those parameter ids can't be renumbered and would collide with the outer plan's.
    core::error_t splice_view_body(logical_plan::node_aggregate_t* ref, logical_plan::node_ptr body);

    // Nothing rewrites view-targeted DML onto the base table — without this refusal, INSERT/UPDATE/
    // DELETE against a view would report success and write nothing.
    core::error_t reject_view_dml_target(const logical_plan::catalog_resolves_t& resolves,
                                         const logical_plan::node_t* root);

    // Re-register every parameter of `body_params` in `out_params` under a fresh id and rewrite the body's
    // expressions to use the new ids.
    //
    // Both plans number constants from 0 (parameter_node_t::counter_ is per node), so without this
    // the outer `#0` and the body's `#0` collide: `SELECT * FROM v WHERE col_b > 18` over a body
    // `WHERE col_b > 10` would run the body against 18 and report success.
    void renumber_body_parameters(std::pmr::memory_resource* resource,
                                  logical_plan::node_t* body,
                                  const logical_plan::parameter_node_ptr& body_params,
                                  const logical_plan::parameter_node_ptr& out_params);

    // Depth cap for nested views. A true cycle should be impossible (a view can't be dropped while
    // another depends on it) but this is the loud stop instead of an endless resolve loop.
    inline constexpr std::size_t max_view_expansion_depth = 16;

} // namespace components::planner
