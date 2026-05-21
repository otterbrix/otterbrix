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
        // Previously in components/catalog/builtin_seed.{cpp,hpp}; inlined here
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
                {wk::timestamp_type, "timestamp_sec"},
                {wk::timestamp_type, "timestamp_ms"},
                {wk::timestamp_type, "timestamp_us"},
                {wk::timestamp_type, "timestamp_ns"},
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
            return components::catalog::INVALID_OID;
        }
    } // namespace

    void manager_disk_t::bootstrap_system_tables_sync() {
        const bool disk_backed = !config_.path.empty();
        const auto sys_db_oid = catalog::well_known_oid::main_database;
        std::filesystem::path sys_dir;
        if (disk_backed) {
            // pg_catalog system tables under ${configpath}/${main_database_oid}/
            sys_dir = config_.path / std::to_string(static_cast<unsigned>(sys_db_oid));
            std::filesystem::create_directories(sys_dir);
        }

        // Idempotent per-table: load if .otbx already exists, else create fresh and
        // seed builtin rows. `freshly_created` collects only the tables that were
        // initialised from scratch — those need their builtin rows inserted and an
        // initial checkpoint flush. Existing .otbx files already carry their data
        // from a previous run.
        std::unordered_set<catalog::oid_t> freshly_created;
        for (const auto& def : components::catalog::all_system_tables()) {
            const auto tbl_oid = well_known_oid_for_system_table(def.name);
            if (tbl_oid == catalog::INVALID_OID)
                continue;
            if (storages_.find(tbl_oid) != storages_.end()) {
                continue;
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
                    load_storage_disk_sync(tbl_oid, sys_db_oid, otbx);
                    continue;
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
            freshly_created.insert(tbl_oid);
        }

        if (freshly_created.empty()) {
            return;
        }
        trace(log_,
              "manager_disk_t::bootstrap_system_tables_sync : seeding well-known rows for {} fresh tables",
              freshly_created.size());

        const auto pg_catalog_ns_oid = catalog::well_known_oid::pg_catalog_namespace;

        // Builtin rows are seeded only into freshly-created tables. Tables loaded
        // from existing .otbx already carry their data from the previous run.
        if (freshly_created.count(pg_database_oid)) {
            if (auto* def = catalog::find_system_table("pg_database")) {
                const auto db = builtin_database_row();
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, db.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(db.name)));
                });
                direct_append_sync(pg_database_oid, row);
            }
        }

        if (freshly_created.count(pg_namespace_oid_tbl)) {
            if (auto* def = catalog::find_system_table("pg_namespace")) {
                for (const auto& nrow : builtin_namespace_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                        chunk.set_value(0, 0, lv_oid(res, nrow.oid));
                        chunk.set_value(1, 0, lv_str(res, std::string(nrow.name)));
                    });
                    direct_append_sync(pg_namespace_oid_tbl, row);
                }
            }
        }

        if (freshly_created.count(pg_type_oid)) {
            if (auto* def = catalog::find_system_table("pg_type")) {
                for (const auto& trow : builtin_type_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                        chunk.set_value(0, 0, lv_oid(res, trow.oid));
                        chunk.set_value(1, 0, lv_str(res, std::string(trow.name)));
                        chunk.set_value(2, 0, lv_oid(res, pg_catalog_ns_oid));
                    });
                    direct_append_sync(pg_type_oid, row);
                }
            }
        }

        if (freshly_created.count(pg_proc_oid)) {
            if (auto* def = catalog::find_system_table("pg_proc")) {
                for (const auto& frow : builtin_proc_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                        chunk.set_value(0, 0, lv_oid(res, frow.oid));
                        chunk.set_value(1, 0, lv_str(res, std::string(frow.name)));
                        chunk.set_value(2, 0, lv_oid(res, pg_catalog_ns_oid));
                    });
                    direct_append_sync(pg_proc_oid, row);
                }
            }
        }

        // Initial checkpoint flushes the seeded rows into freshly-created .otbx
        // files so a subsequent load picks them up. Existing tables already have
        // their data on disk; skip them.
        if (disk_backed) {
            for (auto tbl_oid : freshly_created) {
                auto it = storages_.find(tbl_oid);
                if (it != storages_.end()) {
                    it->second->table_storage.checkpoint();
                }
            }
        }
    }

    void manager_disk_t::load_system_tables_sync() {
        if (config_.path.empty()) {
            return;
        }
        const auto sys_db_oid = catalog::well_known_oid::main_database;
        auto sys_dir = config_.path / std::to_string(static_cast<unsigned>(sys_db_oid));
        if (!std::filesystem::exists(sys_dir)) {
            return;
        }
        for (const auto& def : components::catalog::all_system_tables()) {
            const auto tbl_oid = well_known_oid_for_system_table(def.name);
            if (tbl_oid == catalog::INVALID_OID)
                continue;
            if (storages_.find(tbl_oid) != storages_.end()) {
                continue;
            }
            auto otbx = sys_dir / std::to_string(static_cast<unsigned>(tbl_oid)) / "table.otbx";
            if (!std::filesystem::exists(otbx)) {
                continue;
            }
            trace(log_,
                  "manager_disk_t::load_system_tables_sync loading : {} oid={}",
                  std::string(def.name),
                  static_cast<unsigned>(tbl_oid));
            load_storage_disk_sync(tbl_oid, sys_db_oid, otbx);
        }
    }

    void manager_disk_t::restore_oid_generator_sync() {
        if (storages_.empty()) {
            trace(log_, "manager_disk_t::restore_oid_generator_sync : no storages, skipping");
            return;
        }

        components::catalog::oid_t high_water = components::catalog::FIRST_USER_OID - 1;
        std::pmr::synchronized_pool_resource scan_resource;

        for (const auto& tbl : catalog::all_system_tables()) {
            const auto tbl_oid = well_known_oid_for_system_table(tbl.name);
            if (tbl_oid == catalog::INVALID_OID)
                continue;
            auto it = storages_.find(tbl_oid);
            if (it == storages_.end()) {
                continue;
            }
            auto& table = it->second->table_storage.table();
            if (table.column_count() == 0 || table.calculate_size() == 0) {
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
                    auto val = chunk.value(0, i);
                    if (val.is_null()) {
                        continue;
                    }
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

    void manager_disk_t::load_user_table_storages_sync() {
        if (config_.path.empty()) {
            return;
        }
        if (!std::filesystem::exists(config_.path)) {
            return;
        }
        // Layout: ${config_.path}/${database_oid}/${table_oid}/table.otbx.
        // System tables live under db_oid = main_database (4); user tables under
        // their own db_oid (>= FIRST_USER_OID). bootstrap_system_tables_sync
        // already loaded the system ones — here we walk the rest.
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
                if (storages_.find(tbl_oid) != storages_.end())
                    continue;
                auto otbx = tbl_entry.path() / "table.otbx";
                if (!std::filesystem::exists(otbx))
                    continue;
                trace(log_,
                      "manager_disk_t::load_user_table_storages_sync : oid={} db_oid={}",
                      static_cast<unsigned>(tbl_oid),
                      static_cast<unsigned>(db_oid));
                try {
                    load_storage_disk_sync(tbl_oid, db_oid, otbx);
                } catch (const std::exception& e) {
                    warn(log_,
                         "load_user_table_storages_sync: failed for oid={} : {}",
                         static_cast<unsigned>(tbl_oid),
                         e.what());
                }
            }
        }
    }

    std::unordered_set<components::catalog::oid_t> manager_disk_t::alive_user_oids_sync() const {
        std::unordered_set<components::catalog::oid_t> alive;
        auto it = storages_.find(pg_class_oid);
        if (it == storages_.end()) {
            return alive;
        }
        auto& table = it->second->table_storage.table();
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
                auto val = chunk.value(0, i);
                if (val.is_null())
                    continue;
                const auto seen = static_cast<catalog::oid_t>(val.value<std::uint32_t>());
                if (seen >= catalog::FIRST_USER_OID) {
                    alive.insert(seen);
                }
            }
        }
        return alive;
    }

} // namespace services::disk
