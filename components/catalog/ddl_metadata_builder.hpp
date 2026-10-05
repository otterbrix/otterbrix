#pragma once

#include "catalog_codes.hpp"
#include "catalog_oids.hpp"
#include "catalog_write.hpp"
#include "oid_batch.hpp"
#include "view_binding.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>

#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>
#include <string>
#include <vector>

namespace components::catalog {

    // oid_batch needs >= 1+N OIDs (table + one attoid per column); `columns` is mutated in place
    // to mint each new attoid, which physical storage and .otbx serialization rely on downstream.
    std::vector<catalog_write_t> build_create_table_writes(std::pmr::memory_resource* resource,
                                                           const std::string& dbname,
                                                           const std::string& relname,
                                                           std::vector<table::column_definition_t>& columns,
                                                           oid_t namespace_oid,
                                                           oid_batch_t& oid_batch,
                                                           char relkind = relkind::regular);

    std::vector<catalog_write_t>
    build_create_namespace_writes(std::pmr::memory_resource* resource, const std::string& name, oid_t namespace_oid);

    std::vector<catalog_write_t> build_create_sequence_writes(std::pmr::memory_resource* resource,
                                                              const std::string& name,
                                                              oid_t namespace_oid,
                                                              oid_t seq_oid,
                                                              std::int64_t start,
                                                              std::int64_t increment,
                                                              std::int64_t min_value,
                                                              std::int64_t max_value,
                                                              bool cycle);

    // The view's pg_class row. OR REPLACE keeps the old one, so what depends on the view stays. `relkind` is
    // relkind::view or relkind::materialized_view.
    catalog_write_t build_view_class_row(std::pmr::memory_resource* resource,
                                         oid_t view_oid,
                                         const std::string& name,
                                         oid_t namespace_oid,
                                         char relkind);

    // Everything of a view but its pg_class row: one pg_attribute row per output column (attoids minted from
    // `oid_batch` and stamped back), pg_depend: view -> namespace and one 'n' edge per dependency, pg_rewrite (the
    // text, ev_type = `relkind`), one pg_rewrite_ref row per binding.
    std::vector<catalog_write_t> build_view_body_writes(std::pmr::memory_resource* resource,
                                                        oid_t view_oid,
                                                        std::span<table::column_definition_t> columns,
                                                        std::span<const view_binding_t> bindings,
                                                        std::span<const view_dependency_t> dependencies,
                                                        oid_t namespace_oid,
                                                        oid_t rule_oid,
                                                        const std::string& name,
                                                        const std::string& body_sql,
                                                        char relkind,
                                                        oid_batch_t& oid_batch);

    // pg_class relkind='F' (not 'm', which is reserved for materialized_view) + pg_depend(macro->ns 'n').
    std::vector<catalog_write_t> build_create_macro_writes(std::pmr::memory_resource* resource,
                                                           const std::string& name,
                                                           oid_t namespace_oid,
                                                           oid_t macro_oid,
                                                           oid_t rule_oid,
                                                           const std::string& body_sql);

    // indtype (catalog_codes.hpp) must never be 0; refuses rather than silently hiding a dropped
    // pg_depend edge behind an INVALID_OID in column_attoids.
    core::result_wrapper_t<std::vector<catalog_write_t>>
    build_create_index_writes(std::pmr::memory_resource* resource,
                              const std::string& index_name,
                              oid_t namespace_oid,
                              oid_t table_oid,
                              oid_t index_oid,
                              const std::vector<oid_t>& column_attoids,
                              char indtype);

    std::vector<catalog_write_t> build_create_type_writes(std::pmr::memory_resource* resource,
                                                          const std::string& type_name,
                                                          oid_t namespace_oid,
                                                          oid_t type_oid,
                                                          const std::string& type_spec);

    std::vector<catalog_write_t> build_create_function_writes(std::pmr::memory_resource* resource,
                                                              const std::string& function_name,
                                                              oid_t namespace_oid,
                                                              oid_t fn_oid,
                                                              std::int32_t pronargs,
                                                              std::int64_t prouid,
                                                              const std::string& proargmatchers,
                                                              const std::string& prorettype);

    // pg_depend 'n' edges anchor the cast to castsource/casttarget so a DROP TYPE cascades to it.
    std::vector<catalog_write_t> build_create_cast_writes(std::pmr::memory_resource* resource,
                                                          oid_t cast_oid,
                                                          oid_t source_type_oid,
                                                          oid_t target_type_oid);

    // deptype separates conkey ('i', dies with the column) from confkey ('n', lives in another table).
    // Refuses an INVALID_OID in fk/ref_column_attoids; EMPTY stays legal (test_declared_key_conkey_loss.cpp).
    [[nodiscard]] core::result_wrapper_t<std::vector<catalog_write_t>>
    build_create_constraint_writes(std::pmr::memory_resource* resource,
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

    // added_at_commit_id=0 means always-visible; ALTER DROP COLUMN tombstones the row
    // (is_dropped=true) instead of deleting it.
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
                                            bool indisvalid,
                                            char indtype);

    // Schema: [relid, attoid, attname, atttypid, atttypspec, attversion, attrefcount]; atttypspec
    // defaults to "".
    vector::data_chunk_t build_pg_computed_column_row(std::pmr::memory_resource* resource,
                                                      oid_t table_oid,
                                                      oid_t attoid,
                                                      const std::string& attname,
                                                      oid_t atttypid,
                                                      std::int64_t attversion,
                                                      std::int64_t attrefcount,
                                                      const std::string& atttypspec = std::string{});

    // Schema: [classid, objid, refclassid, refobjid, deptype ('n'/'a'/'i'/'p')].
    vector::data_chunk_t build_pg_depend_row(std::pmr::memory_resource* resource,
                                             oid_t classid,
                                             oid_t objid,
                                             oid_t refclassid,
                                             oid_t refobjid,
                                             char deptype);

} // namespace components::catalog