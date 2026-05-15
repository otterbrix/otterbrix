#pragma once

// Dispatcher-side catalog enrichment pass.
// Called after validate_schema and before planner_t::create_plan. Fills
// logical plan node fields (outgoing_fks, not_null_cols, etc.) from the
// plan-tree resolve idx populated by Pass 1's operator_resolve_*_t. The
// planner then does pure structural rewrite reading those fields — no
// external context parameter needed.

#include <actor-zeta.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/context/execution_context.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace services::dispatcher {

    // Phase 13 Step 3 — name→OID lookup table built from catalog_resolve_*_t
    // logical-plan leaves AFTER they were stamped by operator_resolve_*_t (Pass 1).
    // enrich_plan walks this map directly, so no async catalog actor messages
    // from validate/enrich/planner. Empty when the plan has no resolve wrap
    // (DDL paths, disk-less harnesses).
    //
    // Phase 13 M4.C: also stores pointers to full table metadata
    // (resolved_table_metadata_t living on the resolve node) so enrich can
    // read columns / not-null / default specs through the plan-tree idx.
    struct enrich_resolve_idx_t {
        // dbname → namespace_oid.
        std::unordered_map<std::string, components::catalog::oid_t> ns_by_dbname;
        // "dbname|relname" → table_oid.
        std::unordered_map<std::string, components::catalog::oid_t> tbl_oid_by_qname;
        // "dbname|relname" → const resolved_table_metadata_t*. Points into the
        // resolve node's `resolved_metadata()` optional. Pointer stays valid
        // for the lifetime of the plan tree (intrusive_ptr keeps nodes alive
        // through the dispatcher's coroutine).
        std::unordered_map<std::string, const components::logical_plan::resolved_table_metadata_t*>
            tbl_md_by_qname;
        // table_oid → const resolved_table_metadata_t*. Mirror for oid-keyed
        // table metadata probes.
        std::unordered_map<components::catalog::oid_t,
                           const components::logical_plan::resolved_table_metadata_t*>
            tbl_md_by_oid;
        // Constraint snapshots keyed by parent table_oid, stamped by
        // operator_resolve_constraint_t at Pass 1 time.
        std::unordered_map<components::catalog::oid_t,
                           std::vector<components::catalog::fk_info_t>> outgoing_fks_by_oid;
        std::unordered_map<components::catalog::oid_t,
                           std::vector<components::catalog::fk_info_t>> referencing_fks_by_oid;
        std::unordered_map<components::catalog::oid_t,
                           std::vector<std::pair<std::string, std::string>>> check_exprs_by_oid;
        // M4.F: "dbname|typename" -> const resolved_type_metadata_t*. Mirrors
        // plan_resolve_index_t::type_md_by_qname for enrich-only consumers
        // (drop_type_t, create_collection_t column-type resolution).
        std::unordered_map<std::string,
                           const components::logical_plan::resolved_type_metadata_t*>
            type_md_by_qname;
    };

    // Walks the plan tree and fills catalog metadata fields into DML nodes
    // (node_insert_t, node_update_t, node_delete_t).  DDL nodes are left
    // untouched — they go through execute_ddl_inline unchanged.
    //
    // Precondition: validate_schema has already co_awaited get_table() for every
    // table referenced in the plan, so try_get_table() hits the cache synchronously.
    //
    // `idx` (Phase 13 Step 3): when non-null, supplies the plan-tree resolve
    // map used to stamp table_oid / namespace_oid without async catalog probes.
    // When null, enrich gathers a local index from `root` itself (recursive
    // calls then thread the gathered pointer through children).
    actor_zeta::unique_future<void>
    enrich_plan(components::logical_plan::node_ptr root,
                actor_zeta::address_t disk_address,
                components::execution_context_t ctx,
                std::pmr::memory_resource* resource,
                const enrich_resolve_idx_t* idx = nullptr);

    // Propagate OIDs from sibling catalog_resolve_* nodes onto their consumer
    // nodes (drop/create/DML/alter) inside each sequence_t. Idempotent.
    // Dispatcher invokes this AFTER Pass 1 and BEFORE validate so check_node
    // and tbl_md_for_oid see stamped OIDs on consumer nodes whose name
    // fields were removed in tasks 3, 6, 7, 8.
    void stamp_oids_from_resolves(components::logical_plan::node_t* root);

} // namespace services::dispatcher
