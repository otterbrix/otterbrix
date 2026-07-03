#include "manager_disk_impl.hpp"

#include <charconv>

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    namespace {
        namespace wk = components::catalog::well_known_oid;

        // ----------------------------------------------------------------------
        // Builtin seed rows for pg_catalog bootstrap.
        // Previously a standalone catalog builtin-seed unit; inlined here
        // since manager_disk_bootstrap.cpp is the only consumer.
        // ----------------------------------------------------------------------

        struct ns_seed_row_t {
            components::catalog::oid_t oid;
            std::string_view name;
        };
        struct type_seed_row_t {
            components::catalog::oid_t oid;
            std::string_view name;
        };
        struct proc_seed_row_t {
            components::catalog::oid_t oid;
            std::string_view name;
        };

        ns_seed_row_t builtin_database_row() { return {wk::main_database, "main"}; }

        std::vector<ns_seed_row_t> builtin_namespace_rows() {
            return {
                {wk::pg_catalog_namespace, "pg_catalog"},
                {wk::public_namespace, "public"},
                {wk::information_schema_namespace, "information_schema"},
            };
        }

        std::vector<type_seed_row_t> builtin_type_rows() {
            return {
                // Canonical otterbrix names. Note: int8_type is the 1-byte (8-bit) signed
                // integer in otterbrix's vocabulary, so its canonical name is "int1" — NOT
                // PostgreSQL's "int8" which means 8 bytes (64-bit). PG's "int8" alias is
                // listed below under "PostgreSQL internal typnames" and maps to int64_type.
                {wk::boolean_type, "bool"},
                {wk::int8_type, "int1"},
                {wk::int16_type, "int16"},
                {wk::int32_type, "int32"},
                {wk::int64_type, "int64"},
                {wk::float32_type, "float32"},
                {wk::float64_type, "float64"},
                {wk::string_type, "string"},
                {wk::timestamp_type, "timestamp"},
                {wk::date_type, "date"},
                {wk::time_type, "time"},
                {wk::blob_type, "blob"},
                {wk::numeric_type, "numeric"},
                {wk::uuid_type, "uuid"},
                // PostgreSQL internal typnames
                {wk::int16_type, "int2"},
                {wk::int32_type, "int4"},
                {wk::int64_type, "int8"},
                {wk::int64_type, "int8_t"},
                {wk::float32_type, "float4"},
                {wk::float64_type, "float8"},
                {wk::string_type, "text"},
                {wk::string_type, "varchar"},
                {wk::string_type, "bpchar"},
                {wk::string_type, "name"},
                {wk::blob_type, "bytea"},
                // SQL-facing user aliases
                {wk::boolean_type, "boolean"},
                {wk::int8_type, "tinyint"},
                {wk::int16_type, "smallint"},
                {wk::int32_type, "integer"},
                {wk::int32_type, "int"},
                {wk::int64_type, "bigint"},
                {wk::float64_type, "double"},
                {wk::float64_type, "double precision"},
                {wk::numeric_type, "decimal"},
                // Timestamp variants
                {wk::timestamp_type, "timestamp"},
                {wk::timestamp_tz_type, "timestamp with time zone"},
                {wk::date_type, "date"},
                {wk::time_type, "time"},
                {wk::time_tz_type, "time with time zone"},
                {wk::interval_type, "interval"},
            };
        }

        std::vector<proc_seed_row_t> builtin_proc_rows() {
            return {
                {wk::fn_count, "count"},
                {wk::fn_sum, "sum"},
                {wk::fn_avg, "avg"},
                {wk::fn_min, "min"},
                {wk::fn_max, "max"},
            };
        }

        // Map system table name (def->name) to its well-known OID.
        // Mirrors the constants in catalog_oids.hpp::well_known_oid::pg_*_table.
        components::catalog::oid_t well_known_oid_for_system_table(std::string_view name) {
            if (name == "pg_namespace")
                return components::catalog::well_known_oid::pg_namespace_table;
            if (name == "pg_class")
                return components::catalog::well_known_oid::pg_class_table;
            if (name == "pg_attribute")
                return components::catalog::well_known_oid::pg_attribute_table;
            if (name == "pg_type")
                return components::catalog::well_known_oid::pg_type_table;
            if (name == "pg_proc")
                return components::catalog::well_known_oid::pg_proc_table;
            if (name == "pg_depend")
                return components::catalog::well_known_oid::pg_depend_table;
            if (name == "pg_constraint")
                return components::catalog::well_known_oid::pg_constraint_table;
            if (name == "pg_index")
                return components::catalog::well_known_oid::pg_index_table;
            if (name == "pg_computed_column")
                return components::catalog::well_known_oid::pg_computed_column_table;
            if (name == "pg_database")
                return components::catalog::well_known_oid::pg_database_table;
            if (name == "pg_sequence")
                return components::catalog::well_known_oid::pg_sequence_table;
            if (name == "pg_rewrite")
                return components::catalog::well_known_oid::pg_rewrite_table;
            if (name == "pg_settings")
                return components::catalog::well_known_oid::pg_settings_table;
            return components::catalog::INVALID_OID;
        }
    } // namespace

    void manager_disk_t::bootstrap_system_tables_sync() {
        const bool disk_backed = !config_.path.empty();
        const auto sys_db_oid = catalog::well_known_oid::main_database;
        std::filesystem::path sys_dir;
        if (disk_backed) {
            sys_dir = config_.path / std::to_string(static_cast<unsigned>(sys_db_oid));
            std::filesystem::create_directories(sys_dir);
        }

        // Helper: load or create a single system table. Returns true if freshly created.
        auto bootstrap_one = [&](const components::catalog::system_table_def_t& def) -> bool {
            const auto tbl_oid = well_known_oid_for_system_table(def.name);
            if (tbl_oid == catalog::INVALID_OID)
                return false;
            // agents_[0] (CATALOG agent) is the sole source of truth for
            // pg_* system tables.
            if (!agents_.empty() && agents_[0] != nullptr) {
                if (agents_[0]->has_storage_sync(tbl_oid))
                    return false;
            }
            if (disk_backed) {
                auto coll_dir = sys_dir / std::to_string(static_cast<unsigned>(tbl_oid));
                std::filesystem::create_directories(coll_dir);
                auto otbx = coll_dir / "table.otbx";
                if (std::filesystem::exists(otbx)) {
                    trace(log_,
                          "manager_disk_t::bootstrap_system_tables_sync loading : {} oid={}",
                          std::string(def.name),
                          static_cast<unsigned>(tbl_oid));
                    // load_storage_disk_sync returns an error instead of throwing on the terminal
                    // corrupt/no-.prev case: this open/bootstrap chain must not throw (base_spaces
                    // drives it pre-scheduler-start). The error is logged; the table is left
                    // unloaded and a later resolve / WAL replay surfaces the absence cleanly.
                    if (auto err = load_storage_disk_sync(tbl_oid, sys_db_oid, otbx); err.contains_error()) {
                        warn(log_,
                             "bootstrap_system_tables_sync: failed to load system table {} oid={} : {}",
                             std::string(def.name),
                             static_cast<unsigned>(tbl_oid),
                             err.what.c_str());
                    }
                    return false; // loaded (or failed-and-logged), not freshly created
                }
                trace(log_,
                      "manager_disk_t::bootstrap_system_tables_sync creating disk : {} oid={}",
                      std::string(def.name),
                      static_cast<unsigned>(tbl_oid));
                create_storage_disk_sync(tbl_oid, sys_db_oid, def.columns, otbx);
            } else {
                trace(log_,
                      "manager_disk_t::bootstrap_system_tables_sync creating in-memory : {} oid={}",
                      std::string(def.name),
                      static_cast<unsigned>(tbl_oid));
                auto cols = def.columns;
                create_storage_with_columns_sync(tbl_oid, sys_db_oid, std::move(cols));
            }
            return true; // freshly created
        };

        std::unordered_set<catalog::oid_t> freshly_created;

        // Bootstrap pg_settings FIRST so stored_catalog_ is populated before any
        // other table's seeding calls direct_append_sync (which takes the timezone).
        if (const auto* settings_def = catalog::find_system_table("pg_settings")) {
            if (bootstrap_one(*settings_def)) {
                freshly_created.insert(catalog::well_known_oid::pg_settings_table);
                auto row = make_row(resource(), settings_def->columns, [&](data_chunk_t& chunk, auto*) {
                    chunk.set_value(0, 0, std::string_view("TimeZone"));
                    chunk.set_value(1, 0, std::string_view("UTC"));
                });
                direct_append_sync(catalog::well_known_oid::pg_settings_table, row, {});
            }
            auto tz_name = read_setting_sync("TimeZone");
            if (!tz_name.empty()) {
                stored_catalog_.set_timezone(resource(), tz_name);
            }
        }

        // Remaining tables — pg_settings is already in storages_ so bootstrap_one skips it.
        for (const auto& def : components::catalog::all_system_tables()) {
            if (bootstrap_one(def)) {
                freshly_created.insert(well_known_oid_for_system_table(def.name));
            }
        }

        if (freshly_created.empty() ||
            freshly_created == std::unordered_set<catalog::oid_t>{catalog::well_known_oid::pg_settings_table}) {
            // Only pg_settings was freshly created — checkpoint it if disk-backed.
            // storage_entry_sync returns nullptr for record-only markers, so we
            // checkpoint against whichever holds the SFBM; table_storage_t::checkpoint
            // is a no-op for IN_MEMORY, so the agent branch is harmless on a twin.
            if (disk_backed && freshly_created.count(catalog::well_known_oid::pg_settings_table)) {
                constexpr auto settings_oid = catalog::well_known_oid::pg_settings_table;
                const collection_storage_entry_t* entry = nullptr;
                if (!agents_.empty() && agents_[0] != nullptr) {
                    entry = agents_[0]->storage_entry_sync(settings_oid);
                }
                if (entry != nullptr) {
                    // const_cast: checkpoint mutates the SFBM/free-list but
                    // storage_entry_sync hands back a const pointer. Safe because the
                    // agent thread is idle at this bootstrap-time call. The wrapper carries
                    // out_of_memory; bind it and warn (bootstrap has no error channel — a
                    // system-table checkpoint OOM here is a hard environment fault).
                    auto cp_r = const_cast<collection_storage_entry_t*>(entry)->table_storage.checkpoint();
                    if (cp_r.has_error()) {
                        warn(log_, "manager_disk bootstrap: pg_settings checkpoint failed (rules 2/9)");
                    }
                }
            }
            if (freshly_created.size() <= 1)
                return;
        }

        trace(log_,
              "manager_disk_t::bootstrap_system_tables_sync : seeding well-known rows for {} fresh tables",
              freshly_created.size());

        const auto pg_catalog_ns_oid = catalog::well_known_oid::pg_catalog_namespace;
        const auto tz = stored_catalog_.timezone_offset;

        if (freshly_created.count(pg_database_oid)) {
            if (auto* def = catalog::find_system_table("pg_database")) {
                const auto db = builtin_database_row();
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                    chunk.set_value(0, 0, db.oid);
                    chunk.set_value(1, 0, db.name);
                });
                direct_append_sync(pg_database_oid, row, tz);
            }
        }

        if (freshly_created.count(pg_namespace_oid_tbl)) {
            if (auto* def = catalog::find_system_table("pg_namespace")) {
                for (const auto& nrow : builtin_namespace_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                        chunk.set_value(0, 0, nrow.oid);
                        chunk.set_value(1, 0, nrow.name);
                    });
                    direct_append_sync(pg_namespace_oid_tbl, row, tz);
                }
            }
        }

        if (freshly_created.count(pg_type_oid)) {
            if (auto* def = catalog::find_system_table("pg_type")) {
                for (const auto& trow : builtin_type_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                        chunk.set_value(0, 0, trow.oid);
                        chunk.set_value(1, 0, trow.name);
                        chunk.set_value(2, 0, pg_catalog_ns_oid);
                    });
                    direct_append_sync(pg_type_oid, row, tz);
                }
            }
        }

        if (freshly_created.count(pg_proc_oid)) {
            if (auto* def = catalog::find_system_table("pg_proc")) {
                for (const auto& frow : builtin_proc_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                        chunk.set_value(0, 0, frow.oid);
                        chunk.set_value(1, 0, frow.name);
                        chunk.set_value(2, 0, pg_catalog_ns_oid);
                    });
                    direct_append_sync(pg_proc_oid, row, tz);
                }
            }
        }

        if (disk_backed) {
            for (auto tbl_oid : freshly_created) {
                // Checkpoint each fresh catalog table (same probe as the pg_settings
                // branch above; checkpoint no-ops on IN_MEMORY twins).
                const collection_storage_entry_t* entry = nullptr;
                if (!agents_.empty() && agents_[0] != nullptr) {
                    entry = agents_[0]->storage_entry_sync(tbl_oid);
                }
                if (entry != nullptr) {
                    // The wrapper carries out_of_memory; bind it and warn
                    // (bootstrap has no error channel).
                    auto cp_r = const_cast<collection_storage_entry_t*>(entry)->table_storage.checkpoint();
                    if (cp_r.has_error()) {
                        warn(log_,
                             "manager_disk bootstrap: catalog table oid={} checkpoint failed (rules 2/9)",
                             static_cast<unsigned>(tbl_oid));
                    }
                }
            }
        }
    }

    void manager_disk_t::restore_oid_generator_sync() {
        // agents_[0] (catalog agent) owns all catalog SFBM entries.
        // Pre-scheduler-start, single-threaded.
        if (agents_.empty() || agents_[0] == nullptr) {
            trace(log_, "manager_disk_t::restore_oid_generator_sync : no catalog agent, skipping");
            return;
        }

        components::catalog::oid_t high_water = components::catalog::FIRST_USER_OID - 1;
        std::pmr::synchronized_pool_resource scan_resource;

        for (const auto& tbl : catalog::all_system_tables()) {
            const auto tbl_oid = well_known_oid_for_system_table(tbl.name);
            if (tbl_oid == catalog::INVALID_OID)
                continue;
            const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(tbl_oid);
            if (entry == nullptr) {
                continue;
            }
            auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
            if (table.column_count() == 0 || table.calculate_size() == 0) {
                continue;
            }
            // Column 0 is the identity OID only for oid-keyed system tables, where
            // it is a UINTEGER (oid_col()). Some system tables (e.g. pg_settings)
            // key on a STRING column 0 (`name`); reading that as a uint32 OID
            // yields garbage that would poison oid_gen_ with a huge, non-
            // deterministic high_water — every fresh CREATE TABLE then mints a
            // wild OID, and on reopen the persisted (garbage) catalog OID no longer
            // matches the storage the agent loaded, so user-table appends silently
            // no-op. Only scan column 0 when it is an OID column.
            if (table.columns()[0].type().type() != components::types::logical_type::UINTEGER) {
                continue;
            }
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0));
            components::table::table_scan_state scan_state(&scan_resource);
            table.initialize_scan(scan_state, col_indices);

            std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
            types.push_back(table.columns()[0].type());

            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       types,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                table.scan(chunk, scan_state);
                if (chunk.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    if (chunk.is_null(0, i)) {
                        continue;
                    }
                    const auto seen = static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                    if (seen > high_water) {
                        high_water = seen;
                    }
                }
            }
        }

        oid_gen_.seed(high_water);
        trace(log_, "manager_disk_t::restore_oid_generator_sync : seeded high_water={}", high_water);
    }

    std::uint64_t manager_disk_t::max_persisted_commit_id_sync() const {
        // agents_[0] (catalog agent) owns all catalog SFBM entries.
        // Pre-scheduler-start, single-threaded (same window as restore_oid_generator_sync).
        if (agents_.empty() || agents_[0] == nullptr) {
            return 0;
        }

        // pg_attribute is the ONLY system table carrying commit-id columns
        // (added_at_commit_id @ index 10, dropped_at_commit_id @ index 11 — both
        // i64). pg_class et al. carry none, so scanning pg_attribute is sufficient
        // and authoritative.
        constexpr std::size_t kAddedAtCol = 10;
        constexpr std::size_t kDroppedAtCol = 11;

        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_attribute_oid);
        if (entry == nullptr || entry->storage == nullptr) {
            return 0;
        }
        auto& storage = *const_cast<collection_storage_entry_t*>(entry)->storage;
        const auto total = storage.total_rows();
        if (total == 0) {
            return 0;
        }
        auto types = storage.types();
        if (types.size() <= kDroppedAtCol) {
            return 0;
        }

        std::pmr::synchronized_pool_resource scan_resource;
        // Read via storage->scan with a default ("see all committed") transaction_data
        // (snapshot_horizon = UINT64_MAX). This is the SAME read path resolve_table
        // takes, so it folds the MVCC UPDATE that the schema-growth backfill applies
        // to added_at_commit_id (the stamp is written as an UPDATE producing a newer
        // row version; a raw table.scan/create_index_scan would surface the original
        // added_at = NULL/0 version instead and miss the real commit-id).
        components::vector::data_chunk_t chunk(&scan_resource, types, total);
        storage.scan(chunk, /*filter=*/nullptr, /*limit=*/-1, components::table::transaction_data{});

        std::uint64_t max_commit_id = 0;
        for (uint64_t i = 0; i < chunk.size(); ++i) {
            if (!chunk.is_null(kAddedAtCol, i)) {
                const auto v = chunk.get_value<std::int64_t>(kAddedAtCol, i);
                if (v > 0 && static_cast<std::uint64_t>(v) > max_commit_id) {
                    max_commit_id = static_cast<std::uint64_t>(v);
                }
            }
            if (!chunk.is_null(kDroppedAtCol, i)) {
                const auto v = chunk.get_value<std::int64_t>(kDroppedAtCol, i);
                if (v > 0 && static_cast<std::uint64_t>(v) > max_commit_id) {
                    max_commit_id = static_cast<std::uint64_t>(v);
                }
            }
        }

        return max_commit_id;
    }

    void manager_disk_t::load_user_table_storages_sync() {
        // Walks ${config_.path}/${db_oid}/${tbl_oid} for every user-table
        // directory and calls load_storage_disk_sync, which records the
        // OID on the routed agent slice. Precondition: agents_ non-empty
        // (base_spaces pre-scheduler-start ordering).
        if (config_.path.empty()) {
            return;
        }
        if (!std::filesystem::exists(config_.path)) {
            return;
        }
        // Layout: ${config_.path}/${database_oid}/${table_oid}/table.otbx. System
        // tables (db_oid = main_database) are already loaded; here we walk the rest.
        for (const auto& db_entry : std::filesystem::directory_iterator(config_.path)) {
            if (!db_entry.is_directory())
                continue;
            const auto db_name = db_entry.path().filename().string();
            std::uint64_t db_oid_raw = 0;
            {
                auto [ptr, ec] = std::from_chars(db_name.data(), db_name.data() + db_name.size(), db_oid_raw);
                if (ec != std::errc{})
                    continue; // non-numeric (e.g. wal segment dirs at the same level)
            }
            const auto db_oid = static_cast<catalog::oid_t>(db_oid_raw);
            for (const auto& tbl_entry : std::filesystem::directory_iterator(db_entry.path())) {
                if (!tbl_entry.is_directory())
                    continue;
                const auto tbl_name = tbl_entry.path().filename().string();
                std::uint64_t tbl_oid_raw = 0;
                {
                    auto [ptr, ec] = std::from_chars(tbl_name.data(), tbl_name.data() + tbl_name.size(), tbl_oid_raw);
                    if (ec != std::errc{})
                        continue;
                }
                const auto tbl_oid = static_cast<catalog::oid_t>(tbl_oid_raw);
                if (tbl_oid < catalog::FIRST_USER_OID)
                    continue;
                // User-OID SFBM ownership lives on the routed agent slice.
                if (has_storage(tbl_oid))
                    continue;
                auto otbx = tbl_entry.path() / "table.otbx";
                if (!std::filesystem::exists(otbx))
                    continue;
                trace(log_,
                      "manager_disk_t::load_user_table_storages_sync : oid={} db_oid={}",
                      static_cast<unsigned>(tbl_oid),
                      static_cast<unsigned>(db_oid));
                if (auto err = load_storage_disk_sync(tbl_oid, db_oid, otbx); err.contains_error()) {
                    warn(log_,
                         "load_user_table_storages_sync: failed for oid={} : {}",
                         static_cast<unsigned>(tbl_oid),
                         err.what.c_str());
                }
            }
        }
    }

    void manager_disk_t::rehydrate_in_memory_user_storages_sync() {
        // pg_class persists unconditionally (system tables are always written to
        // disk), but IN_MEMORY user-table row data is not. After
        // load_user_table_storages_sync has loaded every on-disk .otbx, any alive
        // user table still missing a storage is an IN_MEMORY table whose shell was
        // lost on restart. Reconstruct an empty in-memory storage from its
        // pg_attribute columns so the catalog (which says the table exists) and the
        // storage layer agree: otherwise CREATE TABLE IF NOT EXISTS skips creation,
        // resolve_table returns a schema, but storage_append no-ops (returns 0,0)
        // and scans see nothing. Pre-scheduler-start, single-threaded.
        if (agents_.empty() || agents_[0] == nullptr) {
            return;
        }
        constexpr components::catalog::oid_t main_db_oid = catalog::well_known_oid::main_database;

        // Pass 1: scan pg_class for alive user tables that have row storage
        // (relkind 'r' regular or 'm' materialized view) and are not yet loaded.
        // pg_class layout: [0=oid, 1=relname, 2=relnamespace, 3=relkind, 4=relstoragemode].
        std::vector<catalog::oid_t> need_oids;
        {
            const collection_storage_entry_t* cls_entry = agents_[0]->storage_entry_sync(pg_class_oid);
            if (cls_entry == nullptr) {
                return;
            }
            auto& cls_table = const_cast<collection_storage_entry_t*>(cls_entry)->table_storage.table();
            if (cls_table.column_count() < 4 || cls_table.calculate_size() == 0) {
                return;
            }
            std::pmr::synchronized_pool_resource scan_resource;
            // Sparse scan of the non-adjacent [oid (0), relkind (3)] columns via
            // the projected_cols data_chunk ctor: pass the FULL pg_class type list
            // plus the projected absolute indices, and read by ABSOLUTE index
            // (chunk.value(0,..) / chunk.value(3,..)). A compacted 2-type chunk
            // mis-decodes the dictionary-encoded relkind string column (segfault).
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0)); // oid
            col_indices.emplace_back(static_cast<int64_t>(3)); // relkind
            components::table::table_scan_state scan_state(&scan_resource);
            cls_table.initialize_scan(scan_state, col_indices);
            const auto& all_cols = cls_table.columns();
            std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
            all_types.reserve(all_cols.size());
            for (const auto& c : all_cols) {
                all_types.push_back(c.type());
            }
            const std::vector<std::size_t> projected{static_cast<std::size_t>(0), static_cast<std::size_t>(3)};
            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       all_types,
                                                       projected,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                cls_table.scan(chunk, scan_state);
                if (chunk.size() == 0)
                    break;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.is_null(0, i))
                        continue;
                    const auto oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                    if (oid < catalog::FIRST_USER_OID)
                        continue;
                    if (has_storage(oid))
                        continue;
                    const bool rk_null = chunk.is_null(3, i);
                    const auto rk_v = rk_null ? std::string_view{} : chunk.get_value<std::string_view>(3, i);
                    const char relkind = (rk_null || rk_v.empty()) ? catalog::relkind::regular : rk_v.front();
                    // Only relkinds with physical row storage. Views, computed/
                    // virtual tables, sequences etc. have no append-target storage.
                    if (relkind != catalog::relkind::regular && relkind != catalog::relkind::materialized_view) {
                        continue;
                    }
                    need_oids.push_back(oid);
                }
            }
        }
        if (need_oids.empty()) {
            return;
        }

        // Pass 2: read pg_attribute once, grouping live (non-dropped) columns by
        // attrelid. pg_attribute layout: [0=attoid, 1=attrelid, 2=attname,
        // 3=atttypid, 4=attnum, 5=attnotnull, 6=atthasdefault, 7=attisdropped,
        // 8=atttypspec, ...]. Each column's (attnum, name, type) reconstructs the
        // storage schema in ordinal order — the same order CREATE TABLE registered.
        struct rehydrate_col_t {
            std::int32_t attnum{0};
            std::string name;
            components::types::complex_logical_type type;
        };
        std::unordered_map<catalog::oid_t, std::vector<rehydrate_col_t>> cols_by_relid;
        {
            const collection_storage_entry_t* attr_entry = agents_[0]->storage_entry_sync(pg_attribute_oid);
            if (attr_entry == nullptr) {
                return;
            }
            auto& attr_table = const_cast<collection_storage_entry_t*>(attr_entry)->table_storage.table();
            if (attr_table.column_count() < 9 || attr_table.calculate_size() == 0) {
                return;
            }
            std::unordered_set<catalog::oid_t> wanted(need_oids.begin(), need_oids.end());
            std::pmr::synchronized_pool_resource scan_resource;
            const auto& all_cols = attr_table.columns();
            std::vector<components::table::storage_index_t> col_indices;
            for (std::size_t c = 0; c < all_cols.size(); ++c) {
                col_indices.emplace_back(static_cast<int64_t>(c));
            }
            components::table::table_scan_state scan_state(&scan_resource);
            attr_table.initialize_scan(scan_state, col_indices);
            std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
            all_types.reserve(all_cols.size());
            for (const auto& c : all_cols) {
                all_types.push_back(c.type());
            }
            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       all_types,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                attr_table.scan(chunk, scan_state);
                if (chunk.size() == 0)
                    break;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.is_null(1, i))
                        continue;
                    const auto relid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                    if (wanted.find(relid) == wanted.end())
                        continue;
                    if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i))
                        continue; // tombstoned column
                    rehydrate_col_t rc;
                    if (!chunk.is_null(2, i)) {
                        auto attname_v = chunk.get_value<std::string_view>(2, i);
                        rc.name.assign(attname_v.data(), attname_v.size());
                    }
                    rc.attnum = chunk.is_null(4, i) ? 0 : chunk.get_value<std::int32_t>(4, i);
                    std::string typspec;
                    if (!chunk.is_null(8, i)) {
                        auto typspec_v = chunk.get_value<std::string_view>(8, i);
                        typspec.assign(typspec_v.data(), typspec_v.size());
                    }
                    if (!typspec.empty()) {
                        rc.type = catalog::decode_type_spec(resource_, typspec);
                    } else {
                        const auto atttypid = chunk.is_null(3, i)
                                                  ? catalog::INVALID_OID
                                                  : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                        rc.type = components::types::complex_logical_type(catalog::oid_to_builtin_type(atttypid));
                    }
                    if (!rc.name.empty() && !rc.type.has_alias()) {
                        rc.type.set_alias(rc.name);
                    }
                    cols_by_relid[relid].push_back(std::move(rc));
                }
            }
        }

        // Pass 3: create the in-memory storage shell for each missing table, with
        // columns in attnum (ordinal) order.
        for (auto oid : need_oids) {
            auto it = cols_by_relid.find(oid);
            if (it == cols_by_relid.end() || it->second.empty()) {
                continue; // no columns resolved — skip rather than create a 0-col storage
            }
            auto& cols = it->second;
            std::sort(cols.begin(), cols.end(), [](const rehydrate_col_t& a, const rehydrate_col_t& b) {
                return a.attnum < b.attnum;
            });
            std::vector<components::table::column_definition_t> defs;
            defs.reserve(cols.size());
            for (auto& c : cols) {
                defs.emplace_back(c.name, c.type);
            }
            trace(log_,
                  "manager_disk_t::rehydrate_in_memory_user_storages_sync : oid={} cols={}",
                  static_cast<unsigned>(oid),
                  defs.size());
            create_storage_with_columns_sync(oid, main_db_oid, std::move(defs));
        }
    }

    std::pmr::vector<components::vector::data_chunk_t>
    manager_disk_t::scan_storage_for_rebuild_sync(components::catalog::oid_t table_oid,
                                                  std::pmr::memory_resource* resource) const {
        std::pmr::vector<components::vector::data_chunk_t> batches{resource};
        if (agents_.empty())
            return batches;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        if (idx >= agents_.size() || agents_[idx] == nullptr)
            return batches;
        const collection_storage_entry_t* entry = agents_[idx]->storage_entry_sync(table_oid);
        if (entry == nullptr || entry->storage == nullptr)
            return batches;
        const auto total = entry->storage->total_rows();
        if (total == 0)
            return batches;
        // REGULAR scan (default transaction_data) so the visibility filter drops
        // committed-deleted tombstones. scan_segment (COMMITTED_ROWS, no filter)
        // would seed the index with deleted rows whose column data is still present,
        // and index_scan + fetch + WHERE would then return them. scan_batched emits the
        // table as ≤DEFAULT_VECTOR_CAPACITY chunks, so no oversized chunk is built.
        entry->storage->scan_batched(batches,
                                     /*filter=*/nullptr,
                                     /*limit=*/-1,
                                     /*projected_cols=*/nullptr,
                                     components::table::transaction_data{});
        return batches;
    }

    std::pmr::vector<components::catalog::oid_t> manager_disk_t::scan_live_table_oids_sync() const {
        // See header. Pre-scheduler-start, single-threaded scan of pg_class on agents_[0].
        std::pmr::vector<components::catalog::oid_t> live{resource_};
        if (agents_.empty() || agents_[0] == nullptr) {
            return live;
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_class_oid);
        if (entry == nullptr) {
            return live;
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() < 4 || table.calculate_size() == 0) {
            return live;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class: 0=oid, 3=relkind. templated_scan writes into
        // result.data[column.primary_index()] (by storage column index, not position
        // in col_indices), so the chunk must have a slot at every storage index the
        // scan touches. projected_cols ctor allocates buffers only for [0, 3].
        std::vector<components::table::storage_index_t> col_indices;
        col_indices.emplace_back(static_cast<int64_t>(0));
        col_indices.emplace_back(static_cast<int64_t>(3));
        components::table::table_scan_state scan_state(&scan_resource);
        table.initialize_scan(scan_state, col_indices);
        const auto& all_cols = table.columns();
        std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
        all_types.reserve(all_cols.size());
        for (const auto& c : all_cols) {
            all_types.push_back(c.type());
        }
        const std::vector<std::size_t> projected{0, 3};
        while (true) {
            components::vector::data_chunk_t chunk(&scan_resource,
                                                   all_types,
                                                   projected,
                                                   components::vector::DEFAULT_VECTOR_CAPACITY);
            table.scan(chunk, scan_state);
            if (chunk.size() == 0)
                break;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(3, i))
                    continue;
                const auto seen = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (seen < catalog::FIRST_USER_OID)
                    continue;
                const auto kind = chunk.get_value<std::string_view>(3, i);
                if (kind.size() != 1)
                    continue;
                const char k = kind.front();
                if (k != catalog::relkind::regular && k != catalog::relkind::materialized_view)
                    continue;
                live.push_back(seen);
            }
        }
        return live;
    }

    std::pmr::vector<pg_index_row_t> manager_disk_t::scan_alive_pg_index_sync() const {
        // Three single-pass catalog sweeps on agents_[0] (pg_index, then pg_class for
        // names, then pg_attribute for indkey) instead of O(N_indexes × C) per-index
        // rescans. Pre-scheduler-start, single-threaded.
        std::pmr::vector<pg_index_row_t> result{resource_};
        if (agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        const collection_storage_entry_t* idx_entry = agents_[0]->storage_entry_sync(pg_index_oid);
        if (idx_entry == nullptr) {
            return result;
        }
        auto& idx_table = const_cast<collection_storage_entry_t*>(idx_entry)->table_storage.table();
        if (idx_table.column_count() < 4 || idx_table.calculate_size() == 0) {
            return result;
        }

        // Pass 1: scan pg_index. The raw indkey attoid CSV is stashed per-row and
        // resolved against pg_attribute in pass 3.
        std::pmr::vector<std::pmr::string> raw_indkeys{resource_};
        {
            std::pmr::synchronized_pool_resource scan_resource;
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0)); // indexrelid
            col_indices.emplace_back(static_cast<int64_t>(1)); // indrelid
            col_indices.emplace_back(static_cast<int64_t>(2)); // indkey
            col_indices.emplace_back(static_cast<int64_t>(3)); // indisvalid
            components::table::table_scan_state scan_state(&scan_resource);
            idx_table.initialize_scan(scan_state, col_indices);
            std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
            for (std::size_t idx : {0u, 1u, 2u, 3u}) {
                types.push_back(idx_table.columns()[idx].type());
            }
            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       types,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                idx_table.scan(chunk, scan_state);
                if (chunk.size() == 0)
                    break;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.is_null(0, i) || chunk.is_null(1, i))
                        continue;
                    pg_index_row_t row{resource_};
                    row.oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                    row.table_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                    // indisvalid → ready_since sentinel (1 = alive, 0 = skip; see pg_index_row_t).
                    const bool valid = chunk.is_null(3, i) ? false : chunk.get_value<bool>(3, i);
                    row.ready_since = valid ? std::uint64_t{1} : std::uint64_t{0};
                    std::pmr::string raw_indkey{resource_};
                    if (!chunk.is_null(2, i)) {
                        auto indkey_v = chunk.get_value<std::string_view>(2, i);
                        raw_indkey.assign(indkey_v.data(), indkey_v.size());
                    }
                    raw_indkeys.push_back(std::move(raw_indkey));
                    result.push_back(std::move(row));
                }
            }
        }
        if (result.empty()) {
            return result;
        }

        // Pass 2: index names from pg_class.relname, keyed by indexrelid.
        std::pmr::unordered_map<catalog::oid_t, std::pmr::string> class_names{resource_};
        if (const collection_storage_entry_t* cls_entry = agents_[0]->storage_entry_sync(pg_class_oid)) {
            auto& cls_table = const_cast<collection_storage_entry_t*>(cls_entry)->table_storage.table();
            if (cls_table.column_count() >= 2 && cls_table.calculate_size() > 0) {
                std::pmr::synchronized_pool_resource scan_resource;
                std::vector<components::table::storage_index_t> col_indices;
                col_indices.emplace_back(static_cast<int64_t>(0)); // oid
                col_indices.emplace_back(static_cast<int64_t>(1)); // relname
                components::table::table_scan_state scan_state(&scan_resource);
                cls_table.initialize_scan(scan_state, col_indices);
                std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
                types.push_back(cls_table.columns()[0].type());
                types.push_back(cls_table.columns()[1].type());
                while (true) {
                    components::vector::data_chunk_t chunk(&scan_resource,
                                                           types,
                                                           components::vector::DEFAULT_VECTOR_CAPACITY);
                    cls_table.scan(chunk, scan_state);
                    if (chunk.size() == 0)
                        break;
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        if (chunk.is_null(0, i) || chunk.is_null(1, i))
                            continue;
                        const auto cls_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                        auto sv = chunk.get_value<std::string_view>(1, i);
                        class_names.emplace(cls_oid, std::pmr::string{sv.data(), sv.size(), resource_});
                    }
                }
            }
        }
        for (auto& row : result) {
            auto it = class_names.find(row.oid);
            if (it != class_names.end()) {
                row.name = it->second;
            }
        }

        // Pass 3: resolve the stashed indkey CSVs to attnames via one pg_attribute
        // scan (build attoid → attname, then walk each row's CSV).
        std::pmr::unordered_map<catalog::oid_t, std::pmr::string> attoid_to_name{resource_};
        if (const collection_storage_entry_t* attr_entry = agents_[0]->storage_entry_sync(pg_attribute_oid)) {
            auto& attr_table = const_cast<collection_storage_entry_t*>(attr_entry)->table_storage.table();
            if (attr_table.column_count() >= 3 && attr_table.calculate_size() > 0) {
                std::pmr::synchronized_pool_resource scan_resource;
                // Sparse scan [attoid, attname] via the projected_cols ctor — same
                // chunk-slot convention as scan_live_table_oids_sync above.
                std::vector<components::table::storage_index_t> col_indices;
                col_indices.emplace_back(static_cast<int64_t>(catalog::pg_attribute_col::attoid));
                col_indices.emplace_back(static_cast<int64_t>(catalog::pg_attribute_col::attname));
                components::table::table_scan_state scan_state(&scan_resource);
                attr_table.initialize_scan(scan_state, col_indices);
                const auto& all_cols = attr_table.columns();
                std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
                all_types.reserve(all_cols.size());
                for (const auto& c : all_cols) {
                    all_types.push_back(c.type());
                }
                const std::vector<std::size_t> projected{static_cast<std::size_t>(catalog::pg_attribute_col::attoid),
                                                         static_cast<std::size_t>(catalog::pg_attribute_col::attname)};
                while (true) {
                    components::vector::data_chunk_t chunk(&scan_resource,
                                                           all_types,
                                                           projected,
                                                           components::vector::DEFAULT_VECTOR_CAPACITY);
                    attr_table.scan(chunk, scan_state);
                    if (chunk.size() == 0)
                        break;
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        if (chunk.is_null(catalog::pg_attribute_col::attoid, i) ||
                            chunk.is_null(catalog::pg_attribute_col::attname, i))
                            continue;
                        const auto att_oid = static_cast<catalog::oid_t>(
                            chunk.get_value<std::uint32_t>(catalog::pg_attribute_col::attoid, i));
                        auto sv = chunk.get_value<std::string_view>(catalog::pg_attribute_col::attname, i);
                        attoid_to_name.emplace(att_oid, std::pmr::string{sv.data(), sv.size(), resource_});
                    }
                }
            }
        }
        for (std::size_t i = 0; i < result.size(); ++i) {
            const auto& csv = raw_indkeys[i];
            if (csv.empty())
                continue;
            // Parse CSV of attoids inline (avoid std::string allocation roundtrip).
            std::size_t pos = 0;
            while (pos < csv.size()) {
                std::size_t end = csv.find(',', pos);
                if (end == std::pmr::string::npos)
                    end = csv.size();
                std::uint64_t token = 0;
                auto [ptr, ec] = std::from_chars(csv.data() + pos, csv.data() + end, token);
                pos = end + 1;
                if (ec != std::errc{})
                    continue;
                const auto att_oid = static_cast<catalog::oid_t>(token);
                auto it = attoid_to_name.find(att_oid);
                if (it == attoid_to_name.end())
                    continue;
                result[i].keys.emplace_back(resource_, std::string_view{it->second.data(), it->second.size()});
            }
        }

        return result;
    }

    std::unordered_set<components::catalog::oid_t> manager_disk_t::alive_user_oids_sync() const {
        // agents_[0] (catalog agent) owns pg_class. Pre-scheduler-start,
        // single-threaded (see header comment on storage_entry_sync).
        std::unordered_set<components::catalog::oid_t> alive;
        if (agents_.empty() || agents_[0] == nullptr) {
            return alive;
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_class_oid);
        if (entry == nullptr) {
            return alive;
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() == 0 || table.calculate_size() == 0) {
            return alive;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class column 0 = oid.
        std::vector<components::table::storage_index_t> col_indices;
        col_indices.emplace_back(static_cast<int64_t>(0));
        components::table::table_scan_state scan_state(&scan_resource);
        table.initialize_scan(scan_state, col_indices);
        std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
        types.push_back(table.columns()[0].type());
        while (true) {
            components::vector::data_chunk_t chunk(&scan_resource, types, components::vector::DEFAULT_VECTOR_CAPACITY);
            table.scan(chunk, scan_state);
            if (chunk.size() == 0)
                break;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i))
                    continue;
                const auto seen = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (seen >= catalog::FIRST_USER_OID) {
                    alive.insert(seen);
                }
            }
        }
        return alive;
    }

    std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>> manager_disk_t::scan_dropped_oids_sync() {
        // See header. Strategy: scan pg_class with COMMITTED_ROWS (includes
        // tombstones) for every user OID ever recorded, then set-difference against
        // alive_user_oids_sync (which omits permanently-deleted) to isolate the
        // "DROP committed, GC pending" OIDs.
        std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>> result{resource_};
        if (agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_class_oid);
        if (entry == nullptr) {
            return result;
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() == 0 || table.calculate_size() == 0) {
            return result;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        std::vector<components::table::storage_index_t> col_indices;
        col_indices.emplace_back(static_cast<int64_t>(0)); // pg_class.oid

        // create_index_scan exposes table_scan_type, so it can request COMMITTED_ROWS
        // (incl. tombstones); the plain scan_committed/scan APIs are hard-wired to
        // COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED.
        std::unordered_set<components::catalog::oid_t> all_user_oids;
        {
            components::table::table_scan_state scan_state(&scan_resource);
            table.initialize_scan(scan_state, col_indices);
            std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
            types.push_back(table.columns()[0].type());
            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       types,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                const bool produced =
                    table.create_index_scan(scan_state, chunk, components::table::table_scan_type::COMMITTED_ROWS);
                if (!produced) {
                    break;
                }
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.is_null(0, i))
                        continue;
                    const auto seen = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                    if (seen >= catalog::FIRST_USER_OID) {
                        all_user_oids.insert(seen);
                    }
                }
            }
        }

        // dropped = all - alive. Sentinel delete_id = 1 — see header comment.
        const auto alive = alive_user_oids_sync();
        for (auto oid : all_user_oids) {
            if (alive.count(oid) == 0) {
                result.emplace_back(oid, static_cast<std::uint64_t>(1));
            }
        }
        return result;
    }

    std::string manager_disk_t::read_setting_sync(std::string_view name) {
        // agents_[0] (catalog agent) owns pg_settings. Pre-scheduler-start,
        // single-threaded.
        const auto settings_oid = catalog::well_known_oid::pg_settings_table;
        if (agents_.empty() || agents_[0] == nullptr) {
            return {};
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(settings_oid);
        if (entry == nullptr) {
            return {};
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() < 2 || table.calculate_size() == 0) {
            return {};
        }
        std::pmr::synchronized_pool_resource scan_resource;
        std::vector<components::table::storage_index_t> col_indices;
        col_indices.emplace_back(static_cast<int64_t>(0)); // name column
        col_indices.emplace_back(static_cast<int64_t>(1)); // setting column
        components::table::table_scan_state scan_state(&scan_resource);
        table.initialize_scan(scan_state, col_indices);
        std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
        types.push_back(table.columns()[0].type());
        types.push_back(table.columns()[1].type());
        // pg_settings is append-only: return the LAST row with the matching name
        // so that a SET TIMEZONE append supersedes the seeded default.
        std::string last_value;
        while (true) {
            components::vector::data_chunk_t chunk(&scan_resource, types, components::vector::DEFAULT_VECTOR_CAPACITY);
            table.scan(chunk, scan_state);
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                if (chunk.is_null(0, i)) {
                    continue;
                }
                if (chunk.get_value<std::string_view>(0, i) == name) {
                    if (!chunk.is_null(1, i)) {
                        last_value = std::string{chunk.get_value<std::string_view>(1, i)};
                    }
                }
            }
        }
        return last_value;
    }

} // namespace services::disk
