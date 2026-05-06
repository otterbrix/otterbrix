#pragma once

// Pure catalog-layer helper: builds pg_catalog rows for a CREATE TABLE statement
// and returns them as node_primitive_write_t nodes (no I/O, no disk, no actors).
//
// The caller (planner_t::create_plan DDL overload) passes a pre-allocated OID
// batch and the namespace OID resolved by enrich_plan.  The returned nodes are
// in insertion order: pg_class first, then pg_attribute (one per column), then
// pg_depend rows (per-column type dependency + table→namespace dependency).

#include <catalog/catalog_oids.hpp>
#include <catalog/oid_batch.hpp>

#include <logical_plan/node.hpp>
#include <logical_plan/node_create_collection.hpp>

#include <memory_resource>
#include <vector>

namespace components::catalog {

    // Build pg_catalog write nodes for a CREATE TABLE statement.
    //
    // Preconditions:
    //   - Each column in `node.column_definitions()` must have `atttypid` filled
    //     (via `set_atttypid`) by the enrich phase.  Columns with atttypid == 0
    //     (INVALID_OID) still get a pg_attribute row, but their pg_depend row is
    //     omitted (same behaviour as create_relation_impl in manager_disk_ddl.cpp).
    //   - `oid_batch` must hold at least 1 + N + N + 1 OIDs where N = column count:
    //       1   — table OID (pg_class)
    //       N   — one attoid per column (pg_attribute)
    //       N   — one pg_depend row per column with valid atttypid  (may be < N)
    //       1   — pg_depend table→namespace row
    //     The function only calls `oid_batch.allocate()` for the table OID and
    //     per-column attoids.  Depend rows reuse the already-allocated attoids.
    //
    // Returns a vector of node_primitive_write_t nodes ordered as described above.
    std::vector<logical_plan::node_ptr>
    build_create_table_writes(
        std::pmr::memory_resource*                   resource,
        const logical_plan::node_create_collection_t& node,
        oid_t                                         namespace_oid,
        oid_batch_t&                                  oid_batch);

} // namespace components::catalog