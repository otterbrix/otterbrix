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

        namespace ns = components::catalog::well_known_oid;
        const auto pg_catalog_oid = ns::pg_catalog_namespace;

        // pg_database: single default "main" row. Additional databases get OIDs from
        // oid_generator (>= FIRST_USER_OID) via ddl_create_database.
        if (auto* def = components::catalog::find_system_table("pg_database")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, ns::main_database));
                chunk.set_value(1, 0, lv_str(res, std::string("main")));
            });
            direct_append_sync(pg_database_name, row);
        }

        // pg_namespace: 3 standard schemas.
        if (auto* def = components::catalog::find_system_table("pg_namespace")) {
            struct ns_row {
                components::catalog::oid_t oid;
                const char* name;
            };
            const ns_row rows[] = {{ns::pg_catalog_namespace, "pg_catalog"},
                                   {ns::public_namespace, "public"},
                                   {ns::information_schema_namespace, "information_schema"}};
            for (const auto& nrow : rows) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, nrow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(nrow.name)));
                });
                direct_append_sync(pg_namespace_name, row);
            }
        }

        // pg_type: 14 builtin scalar types, all in pg_catalog namespace.
        if (auto* def = components::catalog::find_system_table("pg_type")) {
            struct t_row {
                components::catalog::oid_t oid;
                const char* name;
            };
            const t_row rows[] = {
                // Canonical otterbrix names
                {ns::boolean_type,   "bool"},
                {ns::int8_type,      "int1"},      // 1-byte signed; no standard PG equivalent
                {ns::int16_type,     "int16"},
                {ns::int32_type,     "int32"},
                {ns::int64_type,     "int64"},
                {ns::float32_type,   "float32"},
                {ns::float64_type,   "float64"},
                {ns::string_type,    "string"},
                {ns::timestamp_type, "timestamp"},
                {ns::date_type,      "date"},
                {ns::time_type,      "time"},
                {ns::blob_type,      "blob"},
                {ns::numeric_type,   "numeric"},
                {ns::uuid_type,      "uuid"},
                // PostgreSQL internal typnames (produced by the SQL parser for SQL keywords)
                {ns::int16_type,     "int2"},       // SMALLINT
                {ns::int32_type,     "int4"},       // INTEGER
                {ns::int64_type,     "int8"},       // BIGINT (PG: int8 = 8-byte int)
                {ns::int64_type,     "int8_t"},     // legacy alias
                {ns::float32_type,   "float4"},
                {ns::float64_type,   "float8"},
                {ns::string_type,    "text"},
                {ns::string_type,    "varchar"},
                {ns::string_type,    "bpchar"},     // char(n)
                {ns::string_type,    "name"},       // pg internal name type
                {ns::blob_type,      "bytea"},
                // SQL-facing user aliases
                {ns::boolean_type,   "boolean"},
                {ns::int8_type,      "tinyint"},
                {ns::int16_type,     "smallint"},
                {ns::int32_type,     "integer"},
                {ns::int32_type,     "int"},
                {ns::int64_type,     "bigint"},
                {ns::float64_type,   "double"},
                {ns::float64_type,   "double precision"},
                {ns::numeric_type,   "decimal"},
                // Timestamp variants
                {ns::timestamp_type, "timestamp_sec"},
                {ns::timestamp_type, "timestamp_ms"},
                {ns::timestamp_type, "timestamp_us"},
                {ns::timestamp_type, "timestamp_ns"},
            };
            for (const auto& trow : rows) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, trow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(trow.name)));
                    chunk.set_value(2, 0, lv_oid(res, pg_catalog_oid));
                });
                direct_append_sync(pg_type_name, row);
            }
        }

        // pg_proc: 5 builtin aggregates.
        if (auto* def = components::catalog::find_system_table("pg_proc")) {
            struct fn_row {
                components::catalog::oid_t oid;
                const char* name;
            };
            const fn_row rows[] = {{ns::fn_count, "count"}, {ns::fn_sum, "sum"},
                                   {ns::fn_avg, "avg"},     {ns::fn_min, "min"},
                                   {ns::fn_max, "max"}};
            for (const auto& frow : rows) {
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
        // Populate O(1) lookup indexes from the freshly seeded pg_namespace/pg_class rows.
        rebuild_lookup_indexes();
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
        rebuild_lookup_indexes();
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

        const collection_full_name_t scanned[] = {
            pg_database_name,  pg_namespace_name, pg_class_name,     pg_attribute_name,
            pg_type_name,      pg_proc_name,      pg_index_name,
            pg_constraint_name, pg_computed_column_name,
            pg_sequence_name,  pg_rewrite_name,
        };
        components::catalog::oid_t high_water = components::catalog::FIRST_USER_OID - 1;
        std::pmr::synchronized_pool_resource scan_resource;

        for (const auto& name : scanned) {
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
        // Rebuild O(1) lookup indexes after WAL replay may have added new pg_namespace/pg_class rows.
        rebuild_lookup_indexes();
    }

    void manager_disk_t::rebuild_lookup_indexes() {
        ns_name_to_oid_.clear();
        ns_oid_to_name_.clear();
        table_to_oid_.clear();
        table_oid_to_key_.clear();

        std::pmr::synchronized_pool_resource scan_resource;

        // Scan pg_namespace: col 0 = oid (uint32), col 1 = nspname (string).
        if (auto it = storages_.find(pg_namespace_name); it != storages_.end()) {
            inline_scan(it->second->table_storage.table(), {0, 1}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto oid_v = chunk.value(0, i);
                            auto name_v = chunk.value(1, i);
                            if (oid_v.is_null() || name_v.is_null())
                                return true;
                            auto oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            std::string name{name_v.value<std::string_view>()};
                            ns_name_to_oid_.emplace(name, oid);
                            ns_oid_to_name_.emplace(oid, std::move(name));
                            return true;
                        });
        }

        // Scan pg_class: col 0 = oid, col 1 = relname, col 2 = relnamespace, col 3 = relkind.
        if (auto it = storages_.find(pg_class_name); it != storages_.end()) {
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto oid_v = chunk.value(0, i);
                            auto name_v = chunk.value(1, i);
                            auto ns_v = chunk.value(2, i);
                            if (oid_v.is_null() || name_v.is_null() || ns_v.is_null())
                                return true;
                            auto toid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            auto ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                            std::string tname{name_v.value<std::string_view>()};
                            char relkind = catalog::relkind::regular;
                            auto kind_v = chunk.value(3, i);
                            if (!kind_v.is_null()) {
                                auto ks = kind_v.value<std::string_view>();
                                if (!ks.empty())
                                    relkind = ks.front();
                            }
                            ns_table_key_t key{ns_oid, tname};
                            table_to_oid_.emplace(key, table_index_entry_t{toid, relkind});
                            table_oid_to_key_.emplace(toid, std::move(key));
                            return true;
                        });
        }
        trace(log_, "rebuild_lookup_indexes: {} namespaces, {} tables",
              ns_name_to_oid_.size(), table_to_oid_.size());
    }

    // restore_user_storages_sync is defined after the inline_scan helper namespace.

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


    // Restart helper: scan pg_class for user relations and reattach each collection's
    // storage. Disk-backed tables are loaded from .otbx; in-memory tables are
    // reconstructed from pg_attribute rows so WAL replay can populate them.
    void manager_disk_t::restore_user_storages_sync() {
        if (config_.path.empty()) {
            return;
        }
        auto pg_class_it = storages_.find(pg_class_name);
        auto pg_namespace_it = storages_.find(pg_namespace_name);
        if (pg_class_it == storages_.end() || pg_namespace_it == storages_.end()) {
            return;
        }

        std::pmr::synchronized_pool_resource scan_resource;

        std::unordered_map<components::catalog::oid_t, std::string> ns_oid_to_name;
        inline_scan(pg_namespace_it->second->table_storage.table(), {0, 1}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        auto name_v = chunk.value(1, i);
                        if (oid_v.is_null() || name_v.is_null())
                            return true;
                        const auto oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                        ns_oid_to_name.emplace(oid, std::string(name_v.value<std::string_view>()));
                        return true;
                    });

        struct rel_t {
            components::catalog::oid_t oid{0};
            std::string ns_name;
            std::string name;
        };
        std::vector<rel_t> rels;
        inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        auto name_v = chunk.value(1, i);
                        auto ns_v = chunk.value(2, i);
                        auto kind_v = chunk.value(3, i);
                        if (oid_v.is_null() || name_v.is_null() || ns_v.is_null())
                            return true;
                        char relkind = catalog::relkind::regular;
                        if (!kind_v.is_null()) {
                            auto ks = kind_v.value<std::string_view>();
                            if (!ks.empty()) {
                                relkind = ks.front();
                            }
                        }
                        if (relkind != catalog::relkind::regular) {
                            return true;
                        }
                        const auto ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                        auto ns_it = ns_oid_to_name.find(ns_oid);
                        if (ns_it == ns_oid_to_name.end() || ns_it->second == "pg_catalog") {
                            return true;
                        }
                        rel_t r;
                        r.oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                        r.ns_name = ns_it->second;
                        r.name = std::string(name_v.value<std::string_view>());
                        rels.push_back(std::move(r));
                        return true;
                    });

        if (rels.empty()) {
            return;
        }

        auto pg_attribute_it = storages_.find(pg_attribute_name);
        for (const auto& r : rels) {
            collection_full_name_t key{r.ns_name, r.name};
            if (storages_.find(key) != storages_.end()) {
                continue;
            }
            auto otbx = config_.path / r.ns_name / "main" / r.name / "table.otbx";
            if (std::filesystem::exists(otbx)) {
                try {
                    load_storage_disk_sync(key, otbx);
                } catch (const std::exception& e) {
                    warn(log_, "restore_user_storages_sync: failed to load {}: {}",
                         otbx.string(), e.what());
                }
                continue;
            }
            // (b) IN-MEMORY storage rehydration. With atttypspec round-tripping the full
            // complex_logical_type, we can rebuild the column list from pg_attribute alone
            // — including DECIMAL precision/scale and ARRAY element types.
            if (pg_attribute_it == storages_.end()) {
                continue;
            }
            struct rebuild_attr_t {
                std::string name;
                components::catalog::oid_t typid{0};
                std::int32_t attnum{0};
                bool not_null{false};
                std::string typspec;
                std::string defspec;
            };
            std::vector<rebuild_attr_t> attrs;
            inline_scan(pg_attribute_it->second->table_storage.table(),
                         {1, 2, 3, 4, 5, 7, 8, 9}, &scan_resource,
                         [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                             auto attrelid_v = chunk.value(0, i);
                             auto attname_v = chunk.value(1, i);
                             auto atttypid_v = chunk.value(2, i);
                             auto attnum_v = chunk.value(3, i);
                             auto notnull_v = chunk.value(4, i);
                             auto dropped_v = chunk.value(5, i);
                             auto typspec_v = chunk.value(6, i);
                             auto defspec_v = chunk.value(7, i);
                             if (attrelid_v.is_null() || attname_v.is_null())
                                 return true;
                             if (static_cast<components::catalog::oid_t>(
                                     attrelid_v.value<std::uint32_t>()) != r.oid)
                                 return true;
                             if (!dropped_v.is_null() && dropped_v.value<bool>())
                                 return true;
                             rebuild_attr_t a;
                             a.name = std::string(attname_v.value<std::string_view>());
                             a.typid = atttypid_v.is_null()
                                           ? components::catalog::INVALID_OID
                                           : static_cast<components::catalog::oid_t>(
                                                 atttypid_v.value<std::uint32_t>());
                             a.attnum = attnum_v.is_null() ? 0 : attnum_v.value<std::int32_t>();
                             a.not_null = notnull_v.is_null() ? false : notnull_v.value<bool>();
                             if (!typspec_v.is_null())
                                 a.typspec = std::string(typspec_v.value<std::string_view>());
                             if (!defspec_v.is_null())
                                 a.defspec = std::string(defspec_v.value<std::string_view>());
                             attrs.push_back(std::move(a));
                             return true;
                         });
            std::sort(attrs.begin(), attrs.end(),
                      [](const rebuild_attr_t& a, const rebuild_attr_t& b) { return a.attnum < b.attnum; });
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(attrs.size());
            for (const auto& a : attrs) {
                components::types::complex_logical_type ct = a.typspec.empty()
                    ? components::types::complex_logical_type{oid_to_builtin_type(a.typid)}
                    : decode_type_spec(resource(), a.typspec);
                ct.set_alias(a.name);
                components::table::column_definition_t cd(a.name, ct, a.not_null);
                if (!a.defspec.empty()) {
                    try {
                        std::pmr::string buf(a.defspec, resource());
                        components::serializer::msgpack_deserializer_t des(buf);
                        // Deserializer already positioned at root; no advance_array needed.
                        auto dv = components::types::logical_value_t::deserialize(resource(), &des);
                        cd.set_default_value(std::move(dv));
                    } catch (...) {
                    }
                }
                columns.push_back(std::move(cd));
            }
            if (columns.empty()) {
                storages_.emplace(key, std::make_unique<collection_storage_entry_t>(resource()));
            } else {
                storages_.emplace(key,
                                   std::make_unique<collection_storage_entry_t>(resource(),
                                                                                  std::move(columns)));
            }
        }
    }

    // Push the result's first invalidation event into the ring buffer so M5's plan
    // cache catches it on its next recent_invalidations_since pull, then return the result
    // unchanged. Idempotent w.r.t. the ring (no duplicate push if events is empty).
    ddl_result_t manager_disk_t::finalize_ddl(ddl_result_t r) noexcept {
        if (!r.events.empty()) {
            invalidations_.push(r.events.front());
        }
        return r;
    }

} // namespace services::disk
