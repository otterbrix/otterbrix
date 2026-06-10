#pragma once

// Dispatcher-side catalog enrichment pass.
// Called after validate_schema and before planner_t::create_plan. Fills
// logical plan node fields (outgoing_fks, not_null_cols, etc.) from the
// plan-tree resolve idx populated by operator_resolve_*_t. The planner then
// does pure structural rewrite reading those fields — no external context
// parameter needed.

#include <actor-zeta.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/context/execution_context.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <memory_resource>
#include <services/collection/context_storage.hpp>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace services::dispatcher {

    // name->OID lookup table built from catalog_resolve_*_t logical-plan leaves
    // AFTER they were stamped by operator_resolve_*_t. enrich_plan walks this
    // map directly, so no async catalog actor messages from validate / enrich /
    // planner. Empty when the plan has no resolve wrap (DDL paths, disk-less
    // harnesses). Also stores pointers to full table metadata
    // (resolved_table_metadata_t living on the resolve node) so enrich can
    // read columns / not-null / default specs through the plan-tree idx.
    struct enrich_resolve_idx_t {
        // "dbname|relname" → table_oid.
        std::unordered_map<std::string, components::catalog::oid_t> tbl_oid_by_qname;
        // "dbname|relname" → const resolved_table_metadata_t*. Points into the
        // resolve node's `resolved_metadata()` optional. Pointer stays valid
        // for the lifetime of the plan tree (intrusive_ptr keeps nodes alive
        // through the executor's coroutine).
        std::unordered_map<std::string, const components::logical_plan::resolved_table_metadata_t*> tbl_md_by_qname;
        // table_oid → const resolved_table_metadata_t*. Mirror for oid-keyed
        // table metadata probes.
        std::unordered_map<components::catalog::oid_t, const components::logical_plan::resolved_table_metadata_t*>
            tbl_md_by_oid;
        // Constraint snapshots keyed by parent table_oid, stamped by
        // operator_resolve_constraint_t.
        std::unordered_map<components::catalog::oid_t, std::vector<components::catalog::fk_info_t>> outgoing_fks_by_oid;
        std::unordered_map<components::catalog::oid_t, std::vector<components::catalog::fk_info_t>>
            referencing_fks_by_oid;
        std::unordered_map<components::catalog::oid_t, std::vector<std::pair<std::string, std::string>>>
            check_exprs_by_oid;
    };

    // Walks the plan tree and fills catalog metadata fields into DML nodes
    // (node_insert_t, node_update_t, node_delete_t).  DDL nodes are left
    // untouched — they go through execute_ddl_inline unchanged.
    //
    // Precondition: validate_schema has already co_awaited get_table() for every
    // table referenced in the plan, so try_get_table() hits the cache synchronously.
    //
    // `idx`: when non-null, supplies the plan-tree resolve map used to stamp
    // table_oid / namespace_oid without async catalog probes. When null,
    // enrich gathers a local index from `root` itself (recursive calls then
    // thread the gathered pointer through children).
    actor_zeta::unique_future<void>
    enrich_plan(std::pmr::memory_resource* resource,
                components::logical_plan::node_ptr root,
                actor_zeta::address_t disk_address,
                components::execution_context_t ctx,
                actor_zeta::address_t index_address = actor_zeta::address_t::empty_address(),
                services::context_storage_t* collections_ctx = nullptr,
                const enrich_resolve_idx_t* idx = nullptr);

} // namespace services::dispatcher

// catalog-resolve helpers shared by the dispatcher and executor pipelines.
// Pure functions over plan trees / lookup paths — no member-state access.
namespace services::catalog_resolve {

    // Propagate OIDs from sibling catalog_resolve_* nodes onto their consumer
    // nodes (drop/create/DML/alter) inside each sequence_t. Idempotent.
    // Dispatcher invokes this AFTER resolve and BEFORE validate so check_node
    // and tbl_md_for_oid see stamped OIDs on consumer nodes whose name
    // fields are no longer present.
    void stamp_oids_from_resolves(components::logical_plan::node_t* root);

    // SELECT-time view expansion. Result of expand_view_body():
    // a fresh logical plan parsed/transformed from the view's body SQL,
    // ready to be spliced into the outer plan in place of the view
    // reference. `error` is set (non-null) when re-parse / re-transform
    // failed; in that case `expanded_plan` is null and the caller must
    // surface `error` to the user.
    struct view_expansion_result_t {
        bool had_expansion{false};
        components::logical_plan::node_ptr expanded_plan;
        components::logical_plan::parameter_node_ptr expanded_params;
        components::cursor::cursor_t_ptr error;
    };

    // Find the FIRST catalog_resolve_table_t with relkind='v' (and non-empty
    // view_sql) in `root`'s direct children. Returns nullptr if none.
    components::logical_plan::node_catalog_resolve_table_t*
    find_first_view_resolve(components::logical_plan::node_t* root);

    // Parse view body SQL and transform it into a fresh logical plan. The
    // transformer is instantiated per-call (its mutable state lives on the
    // instance — re-entrant when allocated fresh per call). All allocations
    // use `resource`.
    view_expansion_result_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql);

    // Collect catalog_resolve_*_t nodes whose oid hasn't been stamped yet
    // (i.e. need a fresh resolve round). Operates on direct children of
    // sequence_t roots; sub-plan splicing places them at the front so a
    // shallow scan suffices.
    std::vector<components::logical_plan::node_ptr> extract_unresolved_resolves(components::logical_plan::node_t* root);

    // When the SQL transformer wraps a DML/DDL plan in
    //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t, <real_root>)
    // callers needs to route based on <real_root> (insert_t, select_t, ...)
    // not the wrapping sequence_t. This helper descends a sequence_t root and
    // returns the LAST non-catalog_resolve_* child (the "consumer" node, after
    // all resolution-only prefix children). For non-sequence_t roots it returns
    // the node itself unchanged. Returns nullptr only when the input is null or
    // a sequence_t with no non-resolve children.
    const components::logical_plan::node_t* effective_root_node(const components::logical_plan::node_t* n);
    // Mutable-pointer overload, for call sites that mutate the consumer node.
    components::logical_plan::node_t* effective_root_node(components::logical_plan::node_t* n);

    // drop_* nodes no longer carry user-typed dbname/relname; their sibling
    // resolve_namespace / resolve_table nodes inside the wrapping sequence_t
    // do. Extract (db, rel) from the resolve siblings so routing code that
    // still needs names (qualified_name_t for table_id, catalog-resolve
    // lookups, etc.) keeps working.
    std::pair<std::string, std::string>
    drop_target_names_from_resolves(const components::logical_plan::node_t* plan_root);

    // Probe `name` in the plan-tree idx across the dbname search path.
    // The transformer emits resolve_type for every (dbname, name) tuple
    // we expect to find here (CREATE TABLE column UDT, CREATE TYPE
    // collision check, DROP TYPE existence check). search_dbnames carries
    // dbname strings ordered by precedence.
    struct plan_resolve_index_t;
    const components::logical_plan::resolved_type_metadata_t*
    probe_type_in_path(const plan_resolve_index_t* idx,
                       std::string_view name,
                       std::span<const std::string> search_dbnames);

    // String-based search path for plan-tree idx lookup. Deduplicates
    // entries (when target_dbname is already "public" / "pg_catalog").
    std::vector<std::string> build_type_search_path_str(std::string_view target_dbname);

} // namespace services::catalog_resolve

// Lets dispatcher code keep calling stamp_oids_from_resolves() unqualified.
namespace services::dispatcher {
    using catalog_resolve::stamp_oids_from_resolves;
} // namespace services::dispatcher
