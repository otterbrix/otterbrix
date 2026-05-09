#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    void manager_disk_t::bootstrap_system_tables_sync() {
        // In-memory deployment (no disk path): create system tables purely in memory so DDL
        // and resolve_* still have storages to scan against. Disk-backed deployment goes
        // through create_storage_disk_sync below + a final per-table checkpoint() so the
        // .otbx files are loadable on restart.
        const bool disk_backed = !config_.path.empty();
        std::filesystem::path sys_dir;
        if (disk_backed) {
            sys_dir = config_.path / "pg_catalog" / "main";
            std::filesystem::create_directories(sys_dir);
        }

        bool any_created = false;
        for (const auto& def : components::catalog::all_system_tables()) {
            collection_full_name_t name{"pg_catalog", "main", std::string(def.name)};
            if (storages_.find(name) != storages_.end()) {
                // Already initialized in this process (e.g. duplicate bootstrap call).
                continue;
            }
            if (disk_backed) {
                auto coll_dir = sys_dir / std::string(def.name);
                std::filesystem::create_directories(coll_dir);
                auto otbx = coll_dir / "table.otbx";
                if (std::filesystem::exists(otbx)) {
                    // .otbx exists — load path takes care of it; bootstrap should not overwrite.
                    continue;
                }
                trace(log_, "manager_disk_t::bootstrap_system_tables_sync creating disk : {}",
                      std::string(def.name));
                create_storage_disk_sync(name, def.columns, otbx);
            } else {
                trace(log_, "manager_disk_t::bootstrap_system_tables_sync creating in-memory : {}",
                      std::string(def.name));
                auto cols = def.columns;
                create_storage_with_columns_sync(name, std::move(cols));
            }
            any_created = true;
        }

        // Only seed bootstrap rows on a true fresh install (something was created in this call).
        // On a partial-bootstrap edge case (some .otbx exist, some don't) skip seeding — we'd
        // duplicate well-known rows in the survivors otherwise. This is rare and harmless: a
        // future call will see all tables present and become a no-op.
        if (!any_created) {
            return;
        }
        trace(log_, "manager_disk_t::bootstrap_system_tables_sync : seeding well-known rows");

        const auto pg_catalog_oid = catalog::well_known_oid::pg_catalog_namespace;

        // pg_database: single default "main" row.
        if (auto* def = catalog::find_system_table("pg_database")) {
            const auto db = catalog::builtin_database_row();
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, db.oid));
                chunk.set_value(1, 0, lv_str(res, std::string(db.name)));
            });
            direct_append_sync(pg_database_name, row);
        }

        // pg_namespace: pg_catalog, public, information_schema.
        if (auto* def = catalog::find_system_table("pg_namespace")) {
            for (const auto& nrow : catalog::builtin_namespace_rows()) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, nrow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(nrow.name)));
                });
                direct_append_sync(pg_namespace_name, row);
            }
        }

        // pg_type: all builtin scalar types and aliases.
        if (auto* def = catalog::find_system_table("pg_type")) {
            for (const auto& trow : catalog::builtin_type_rows()) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, trow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(trow.name)));
                    chunk.set_value(2, 0, lv_oid(res, pg_catalog_oid));
                });
                direct_append_sync(pg_type_name, row);
            }
        }

        // pg_proc: count, sum, avg, min, max.
        if (auto* def = catalog::find_system_table("pg_proc")) {
            for (const auto& frow : catalog::builtin_proc_rows()) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, frow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(frow.name)));
                    chunk.set_value(2, 0, lv_oid(res, pg_catalog_oid));
                });
                direct_append_sync(pg_proc_name, row);
            }
        }

        // Persist metadata + bootstrap rows to disk so a subsequent process restart can call
        // load_system_tables_sync without "metadata_reader_t: attempted to read past end of
        // chain". create_new_database() (in create_storage_disk_sync) only writes the file
        // header — it does not flush a meta block. The first checkpoint() does. checkpoint()
        // is a no-op for in-memory storage so we can call it unconditionally.
        if (disk_backed) {
            for (const auto& def : components::catalog::all_system_tables()) {
                collection_full_name_t name{"pg_catalog", "main", std::string(def.name)};
                auto it = storages_.find(name);
                if (it != storages_.end()) {
                    it->second->table_storage.checkpoint();
                }
            }
        }
    }

    // Load existing system catalog tables from disk on subsequent starts.
    // Idempotent: skip tables already present in storages_, skip tables without .otbx
    // (treated as fresh install — bootstrap_system_tables_sync should be called first).
    void manager_disk_t::load_system_tables_sync() {
        if (config_.path.empty()) {
            return;
        }
        auto sys_dir = config_.path / "pg_catalog" / "main";
        if (!std::filesystem::exists(sys_dir)) {
            return;
        }
        for (const auto& def : components::catalog::all_system_tables()) {
            collection_full_name_t name{"pg_catalog", "main", std::string(def.name)};
            if (storages_.find(name) != storages_.end()) {
                continue;
            }
            auto otbx = sys_dir / std::string(def.name) / "table.otbx";
            if (!std::filesystem::exists(otbx)) {
                continue;
            }
            trace(log_, "manager_disk_t::load_system_tables_sync loading : {}", std::string(def.name));
            // load_storage_disk_sync provides W-TORN .prev fallback transparently.
            load_storage_disk_sync(name, otbx);
        }
    }

    // Scan pg_class/pg_attribute/pg_type/pg_proc/pg_constraint
    // (and pg_namespace/pg_index/pg_computed_column for completeness), collect max(oid), seed
    // oid_gen_ to max+1 so future allocate() never collides with on-disk OIDs.
    //
    // Implementation: each system table has the OID-as-uint32 in column index 0
    // (pg_namespace, pg_class, pg_type, pg_proc, pg_constraint, pg_index — all use "oid" or
    // "indexrelid" first; pg_attribute uses "attoid" first). pg_depend has no scalar OID per
    // row (it's a join table) and pg_computed_column's column-0 is "relid" (an existing pg_class
    // oid). We scan all those that carry a fresh OID source.
    void manager_disk_t::restore_oid_generator_sync() {
        if (storages_.empty()) {
            trace(log_, "manager_disk_t::restore_oid_generator_sync : no storages, skipping");
            return;
        }

        components::catalog::oid_t high_water = components::catalog::FIRST_USER_OID - 1;
        std::pmr::synchronized_pool_resource scan_resource;

        for (const auto& tbl : catalog::all_system_tables()) {
            const collection_full_name_t name{"pg_catalog", "main", std::string(tbl.name)};
            auto it = storages_.find(name);
            if (it == storages_.end()) {
                continue;
            }
            auto& table = it->second->table_storage.table();
            if (table.column_count() == 0 || table.calculate_size() == 0) {
                continue;
            }
            // OID lives in column 0 for every scanned table.
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0));
            components::table::table_scan_state scan_state(&scan_resource);
            table.initialize_scan(scan_state, col_indices);

            std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
            types.push_back(table.columns()[0].type());

            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource, types,
                                                        components::vector::DEFAULT_VECTOR_CAPACITY);
                table.scan(chunk, scan_state);
                if (chunk.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    auto val = chunk.value(0, i);
                    if (val.is_null()) {
                        continue;
                    }
                    // OIDs persisted as uint32_t (UINTEGER).
                    const auto seen = static_cast<components::catalog::oid_t>(val.value<std::uint32_t>());
                    if (seen > high_water) {
                        high_water = seen;
                    }
                }
            }
        }

        oid_gen_.seed(high_water);
        trace(log_, "manager_disk_t::restore_oid_generator_sync : seeded high_water={}", high_water);
    }

    // ========================================================================
    // Catalog DDL (async coroutines, public API).
    // ------------------------------------------------------------------------
    // Each method takes execution_context_t. Every system-table mutation routes through
    // direct_append_sync(name, row, ctx.txn) — when ctx.txn carries a non-zero transaction_id,
    // the storage layer's append/delete propagate the txn through finalize_append /
    // delete_rows(txn_id), making rollback work via MVCC. ctx.txn={0,0} keeps the existing
    // committed-at-txn-0 semantics used by bootstrap and tests.
    //
    // append_pg_catalog_row also writes a WAL physical_insert record in the same call when
    // a WAL actor is wired, so a DDL appears as a sequence of physical_inserts on
    // pg_catalog.* in WAL.
    // ========================================================================

} // namespace services::disk
