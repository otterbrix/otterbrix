#pragma once

// SELECT-time view expansion.
//
// After the catalog resolve pass has stamped `view_sql` (pg_rewrite.ev_action) on
// the resolve entry of a relkind='v' target, every reference to that view in the
// plan tree has to be replaced by the view's body.
//
// The replacement is a SPLICE, not a plan swap. The reference node stays where it
// is and the body becomes its child — the same shape an inlined CTE reference
// already has (components/sql/transformer/impl/transform_select.cpp,
// transform_from_element: `make_node_aggregate(resource, {}, {})` +
// `append_child(body)` + `children().back()->set_result_alias(visible)`). Because
// the shape is identical, everything downstream — the validator's recursive schema
// derivation, create_plan_aggregate's `child_op -> match -> group -> having -> sort
// -> select` chain, filter pushdown into the body, column pruning, limit pushdown,
// hash-join rewriting — already handles it, with no change of their own.
//
// What it replaces: the executor used to do
//     plan.sub_queries.back() = std::move(expanded_plan);
// i.e. throw the WHOLE outer plan away and answer with the view body. Anything
// built above the view — an outer WHERE, a narrowed projection, an aggregate, a
// join — was silently dropped and the user got the unfiltered body back as a
// successful answer.
//
// These are pure functions over (root, catalog_resolves_t): no actor, no I/O, no
// member state. The executor supplies the async resolve round between passes.

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

    // Every aggregate_t in `root` that names a resolved relkind='v' entry carrying a
    // non-empty view_sql.
    //
    // ONLY aggregate_t is a splice site. A view name also shows up on match_t /
    // sort_t / group_t / limit_t (they carry the relname of what they filter or
    // order), and splicing there would hang the body under a clause instead of under
    // the consumer. The transformer's own invariant is that a FROM item becomes an
    // aggregate_t with a non-empty relname (transform_from_element), so that is the
    // one shape a view reference can have.
    //
    // relkind='m' is deliberately NOT collected: a materialized view is a real heap
    // and is read like a table.
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

    // Parse `view_sql` and transform it into a fresh logical plan. The transformer is
    // instantiated per call (its mutable state lives on the instance), so each
    // reference gets its OWN body: the nodes carry per-node state (table_oid,
    // table_metadata, output_types, projected_cols, read_cap) and filter pushdown
    // APPENDS a match child into the body, so two references cannot share a subtree.
    // The same policy is already written down for CTEs in optimizer.cpp.
    view_body_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql);

    // Put `body` under `ref` and stop `ref` being a source.
    //
    // Order matters: the body goes in FIRST (position 0), because the filter-pushdown
    // rule reads the aggregate's source as `children()[0]` and starts its clause scan
    // at index 1 — appending at the end compiles, answers correctly, and silently
    // turns pushdown into the body off.
    //
    // Refuses loudly when the body carries a correlated (LATERAL) join:
    // node_join_t::correlations() is const-only, so those parameter ids cannot be
    // renumbered and would collide with the outer plan's.
    core::error_t splice_view_body(logical_plan::node_aggregate_t* ref, logical_plan::node_ptr body);

    // INSERT / UPDATE / DELETE whose target resolves to a view. Nothing rewrites a
    // view-targeted DML into a DML on its base table, so today the statement reports
    // success and writes nothing. Rule 6: refuse it.
    core::error_t reject_view_dml_target(const logical_plan::catalog_resolves_t& resolves,
                                         const logical_plan::node_t* root);

    // Re-register every parameter of `body_params` in `out_params` under a fresh id and
    // rewrite the body's expressions to use the new ids.
    //
    // Both plans number their constants from 0 (parameter_node_t::counter_ is per
    // node), so without this the outer query's `#0` and the view body's `#0` are the
    // same slot: `SELECT * FROM v WHERE col_b > 18` over a body `WHERE col_b > 10`
    // would run the body against 18 and report success.
    void renumber_body_parameters(std::pmr::memory_resource* resource,
                                  logical_plan::node_t* body,
                                  const logical_plan::parameter_node_ptr& body_params,
                                  const logical_plan::parameter_node_ptr& out_params);

    // Depth cap for nested views (a view over a view over ...). A view cannot be
    // dropped while another depends on it, so a true cycle should be impossible; this
    // is the loud stop if one ever exists, instead of an endless resolve loop.
    inline constexpr std::size_t max_view_expansion_depth = 16;

} // namespace components::planner
