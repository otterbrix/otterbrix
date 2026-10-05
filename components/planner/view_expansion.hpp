#pragma once

// SELECT-time view expansion splices the body in place (rather than swapping in the outer plan)
// so everything built above the view (WHERE, projection, aggregate, join) survives.

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

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

    // One SQL statement through the parser and the transformer, as a fresh plan; `what` names it in a refusal.
    core::result_wrapper_t<logical_plan::execution_plan_t>
    parse_statement(std::pmr::memory_resource* resource, const std::string& sql, std::string_view what);

    // Each reference gets its own body -- filter pushdown appends a match child into it, so two
    // references cannot share a subtree (same policy as CTE inlining in optimizer.cpp).
    view_body_t expand_view_body(std::pmr::memory_resource* resource, const core::body_sql_t& view_sql);

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

    // "view \"v\" is stale: <why>": what the view was bound to at CREATE VIEW is not what the read finds.
    core::error_t view_stale_error(std::pmr::memory_resource* resource, std::string_view view, std::string_view why);

    // Every table name of a view body gets what CREATE VIEW bound it to (pg_rewrite_ref): a relation is read by its
    // oid, a host name goes to the host only. A body name without a binding is refused.
    core::error_t pin_view_body_names(std::pmr::memory_resource* resource,
                                      logical_plan::catalog_resolves_t& body_resolves,
                                      const logical_plan::resolved_table_metadata_t& view);

    // Merges a pinned body's lookups into the statement's; the same name bound two different ways is refused.
    core::error_t merge_view_body_resolves(std::pmr::memory_resource* resource,
                                           logical_plan::catalog_resolves_t& dest,
                                           const logical_plan::catalog_resolves_t& body_resolves);

    // A body with a star reads the columns its tables have now; the view keeps the columns it was created with
    // (PostgreSQL 18 expands the star at CREATE VIEW).
    logical_plan::node_ptr project_view_body(std::pmr::memory_resource* resource,
                                             logical_plan::node_ptr body,
                                             const logical_plan::resolved_table_metadata_t& view);

    // REFRESH MATERIALIZED VIEW (PostgreSQL 18 matview.c runs the stored query; Trino 483 analyzes an INSERT into the
    // storage table with the parsed body as its source): INSERT INTO dbname.matview <stored body>, the body's names
    // pinned to what CREATE bound and the body spliced into a reference of its own, listed in stored_bodies for the
    // read's check. The matview is the write target only; nothing reads it.
    core::result_wrapper_t<logical_plan::execution_plan_t>
    refresh_matview_plan(std::pmr::memory_resource* resource,
                         const logical_plan::resolved_table_metadata_t& matview,
                         const core::dbname_t& dbname);

    // A true cycle should be impossible, but this is the loud stop instead of an endless resolve loop.
    inline constexpr std::size_t max_view_expansion_depth = 16;

} // namespace components::planner
