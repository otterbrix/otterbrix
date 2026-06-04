#pragma once

#include "catalog_codes.hpp"
#include "catalog_oids.hpp"
#include "catalog_write.hpp"
#include "oid_batch.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::catalog {

    // Build pg_catalog write rows for a CREATE TABLE statement.
    // Returns plain catalog_write_t values (no logical_plan dependency).
    // The caller (planner) wraps each into node_primitive_write_t.
    //
    // Preconditions:
    //   - Each column must have atttypid set. Columns with atttypid == INVALID_OID
    //     still get a pg_attribute row but their pg_depend row is omitted.
    //   - oid_batch must hold at least 1 + N OIDs (table OID + one attoid per column).
    std::vector<catalog_write_t> build_create_table_writes(std::pmr::memory_resource* resource,
                                                           const std::string& dbname,
                                                           const std::string& relname,
                                                           const std::vector<table::column_definition_t>& columns,
                                                           bool is_disk_storage,
                                                           oid_t namespace_oid,
                                                           oid_batch_t& oid_batch,
                                                           char relkind = relkind::regular);

    // Writes 1 row → pg_namespace (oid, nspname).
    // oid_batch must hold at least 1 OID.
    std::vector<catalog_write_t>
    build_create_namespace_writes(std::pmr::memory_resource* resource, const std::string& name, oid_t namespace_oid);

    // Writes pg_class (relkind='S') + pg_sequence + pg_depend (seq→ns 'n').
    // oid_batch must hold at least 1 OID (seq_oid).
    std::vector<catalog_write_t> build_create_sequence_writes(std::pmr::memory_resource* resource,
                                                              const std::string& name,
                                                              oid_t namespace_oid,
                                                              oid_t seq_oid,
                                                              std::int64_t start,
                                                              std::int64_t increment,
                                                              std::int64_t min_value,
                                                              std::int64_t max_value,
                                                              bool cycle);

    // Writes pg_class (relkind='v') + pg_rewrite + pg_depend (view→ns 'n').
    // oid_batch must hold at least 2 OIDs (view_oid + rule_oid).
    std::vector<catalog_write_t> build_create_view_writes(std::pmr::memory_resource* resource,
                                                          const std::string& name,
                                                          oid_t namespace_oid,
                                                          oid_t view_oid,
                                                          oid_t rule_oid,
                                                          const std::string& body_sql);

    // Writes pg_class (relkind='F') + pg_rewrite + pg_depend (macro→ns 'n').
    // (Macro reljkind moved 'm' → 'F' in M0 to free 'm' for materialized_view.)
    // oid_batch must hold at least 2 OIDs (macro_oid + rule_oid).
    std::vector<catalog_write_t> build_create_macro_writes(std::pmr::memory_resource* resource,
                                                           const std::string& name,
                                                           oid_t namespace_oid,
                                                           oid_t macro_oid,
                                                           oid_t rule_oid,
                                                           const std::string& body_sql);

    // Writes pg_rewrite (ev_class=mv_oid, ev_type='m', ev_action=body_sql) +
    // pg_depend (mv→source 'n'). The matview's pg_class + pg_attribute rows
    // are written separately via build_create_table_writes(... relkind::materialized_view).
    std::vector<catalog_write_t> build_matview_rewrite_writes(std::pmr::memory_resource* resource,
                                                              oid_t mv_oid,
                                                              oid_t rule_oid,
                                                              const std::string& mv_name,
                                                              const std::string& body_sql,
                                                              oid_t source_table_oid);

    // Writes pg_class (relkind='i') + pg_index (indisvalid=false) +
    //   pg_depend (index→table 'a') + N×pg_depend (index→column 'i').
    // column_attoids[i] is the pg_attribute.attoid for the i-th indexed column;
    // routing identity (no name lookup needed).
    // oid_batch must hold at least 1 OID (index_oid).
    std::vector<catalog_write_t> build_create_index_writes(std::pmr::memory_resource* resource,
                                                           const std::string& index_name,
                                                           oid_t namespace_oid,
                                                           oid_t table_oid,
                                                           oid_t index_oid,
                                                           const std::vector<oid_t>& column_attoids);

    // Writes pg_type + pg_depend (type→ns 'n').
    // type_spec may be empty for built-in types.
    std::vector<catalog_write_t> build_create_type_writes(std::pmr::memory_resource* resource,
                                                          const std::string& type_name,
                                                          oid_t namespace_oid,
                                                          oid_t type_oid,
                                                          const std::string& type_spec);

    // Writes pg_proc + pg_depend (fn→ns 'n').
    std::vector<catalog_write_t> build_create_function_writes(std::pmr::memory_resource* resource,
                                                              const std::string& function_name,
                                                              oid_t namespace_oid,
                                                              oid_t fn_oid,
                                                              std::int32_t pronargs,
                                                              std::int64_t prouid,
                                                              const std::string& proargmatchers,
                                                              const std::string& prorettype);

    // Writes pg_constraint + pg_depend(→table 'i') +
    //   N×pg_depend(→fk_col 'i') + if FK: pg_depend(→ref_table 'n').
    // ref_table_oid == INVALID_OID for non-FK constraints.
    std::vector<catalog_write_t> build_create_constraint_writes(std::pmr::memory_resource* resource,
                                                                const std::string& constraint_name,
                                                                oid_t table_oid,
                                                                oid_t constraint_oid,
                                                                char contype,
                                                                oid_t ref_table_oid,
                                                                const std::vector<oid_t>& fk_column_attoids,
                                                                const std::vector<oid_t>& ref_column_attoids,
                                                                char fk_matchtype,
                                                                char fk_del_action,
                                                                char fk_upd_action,
                                                                const std::string& check_expr);

    // Row-builder helpers for update-operations (rename_column, drop_column tombstone,
    // index_set_valid). Return a single data_chunk_t, not a catalog_write_t vector.

    // commit_id contract: CREATE TABLE passes added_at_commit_id=0 (always visible);
    // ALTER ADD COLUMN passes the ALTER's commit_id; ALTER DROP COLUMN writes a
    // tombstone row (same attoid, is_dropped=true, dropped_at_commit_id = DROP's commit_id).
    vector::data_chunk_t build_pg_attribute_row(std::pmr::memory_resource* resource,
                                                oid_t attoid,
                                                oid_t table_oid,
                                                const std::string& name,
                                                oid_t atttypid,
                                                std::int32_t attnum,
                                                bool not_null,
                                                bool has_default,
                                                bool is_dropped,
                                                const std::string& typspec,
                                                const std::string& defspec,
                                                std::int64_t added_at_commit_id = 0,
                                                std::int64_t dropped_at_commit_id = 0);

    vector::data_chunk_t build_pg_index_row(std::pmr::memory_resource* resource,
                                            oid_t index_oid,
                                            oid_t indrelid,
                                            const std::string& indkey,
                                            bool indisvalid);

    // pg_computed_column row builder for tests / primitive-write callers.
    // Schema: [relid, attoid, attname, atttypid, atttypspec, attversion,
    // attrefcount]. atttypspec defaults to "" — only complex types
    // (ARRAY/STRUCT/UNION/DECIMAL/...) need it; builtin scalars are
    // reconstructed from atttypid alone via oid_to_builtin_type.
    vector::data_chunk_t build_pg_computed_column_row(std::pmr::memory_resource* resource,
                                                      oid_t table_oid,
                                                      oid_t attoid,
                                                      const std::string& attname,
                                                      oid_t atttypid,
                                                      std::int64_t attversion,
                                                      std::int64_t attrefcount,
                                                      const std::string& atttypspec = std::string{});

    // pg_depend single-row builder for primitive-write callers (e.g. dynamic
    // computed-column register, or any operator that needs to emit one
    // dependency row outside build_create_table_writes / build_create_*_writes).
    // Schema: [classid, objid, refclassid, refobjid, deptype].
    // deptype is a single character ('n', 'a', 'i', 'p').
    vector::data_chunk_t build_pg_depend_row(std::pmr::memory_resource* resource,
                                             oid_t classid,
                                             oid_t objid,
                                             oid_t refclassid,
                                             oid_t refobjid,
                                             char deptype);

} // namespace components::catalog