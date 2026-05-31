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

    // ----------------------------------------------------------------------
    // Version B* Step 8.8 — Path A (decided in docs/version-b-step8-roadmap.md
    // §8.8). All bootstrap helpers below remain manager-side and continue to
    // mutate `manager_disk_t::storages_` directly. Routing into the agent
    // slice happens IMPLICITLY via Step 8.2 (creation) and Step 4
    // (direct_append) dual-write fanouts that were wired earlier:
    //
    //   - create_storage_with_columns_sync  (manager_disk_io.cpp:288-320)
    //     fans the newly created entry to agents_[pool_idx_for_oid] via
    //     agent_disk_t::bootstrap_inner_sync (IN_MEMORY twin).
    //   - create_storage_disk_sync          (manager_disk_io.cpp:322-332)
    //     manager remains the canonical owner of the DISK entry; agent
    //     receives the record-only marker via load_storage_disk_sync /
    //     bootstrap_record_oid_sync (DISK twinning deferred to Step 8.1.B).
    //   - direct_append_sync                (manager_disk_storage.cpp:25-28)
    //     fans every catalog seed row to the agent's IN_MEMORY twin.
    //   - load_storage_disk_sync            (manager_disk_io.cpp:334-409)
    //     dispatches `bootstrap_record_oid_sync` on the routed agent for
    //     every DISK entry it materialises.
    //
    // Net effect: a successful bootstrap leaves BOTH manager.storages_ AND
    // the corresponding agent slice populated, even though the bootstrap
    // helpers themselves only touch manager.storages_.
    //
    // Path B (future, post-Step 8.11) — move these bodies onto agent 0
    // (catalog agent) and delete the manager-side map. Tracked under
    // Step 8.1.C / Step 8.11; intentionally NOT attempted here to keep
    // Step 8 incremental.
    // ----------------------------------------------------------------------

    void manager_disk_t::bootstrap_system_tables_sync() {
        // Step 8.8 Path A contract: this helper still emplaces into
        // manager.storages_ via {create,load}_storage_*_sync. Those helpers
        // do the agent-side fanout themselves (see Step 8.2 dual-write
        // comment in manager_disk_io.cpp:292-305), so the agent slice ends
        // up populated implicitly. Seeded rows go through
        // direct_append_sync which ALSO fans to the routed agent (see
        // manager_disk_storage.cpp:19-28). No agent-side wiring needed
        // inside this function.
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
            // Step 8.11 wrap (2026-05-31): manager.storages_ deleted.
            // agents_[0] (CATALOG agent) is the sole source of truth — its
            // has_storage_sync probe reports true for both IN_MEMORY twins
            // (from bootstrap_inner_sync) and DISK record-only markers (from
            // bootstrap_record_oid_sync).
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
                    load_storage_disk_sync(tbl_oid, sys_db_oid, otbx);
                    return false; // loaded, not freshly created
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
                auto row = make_row(resource(), settings_def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_str(res, "TimeZone"));
                    chunk.set_value(1, 0, lv_str(res, "UTC"));
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
            // Only pg_settings was freshly created — still need to checkpoint it if disk-backed.
            //
            // Step 8.4.A unblocker — agent-first entry probe (mirrors the
            // bootstrap_one skip-if-present migration). pg_settings DISK
            // storage is currently a record-only marker on agent 0 (the
            // SFBM still lives on manager.storages_ until 8.1.B/8.1.C
            // execute the transfer). storage_entry_sync returns nullptr
            // for record-only markers, so on the DISK catalog path we
            // fall through to the manager map and checkpoint the manager
            // SFBM. Post-8.1.B (agent owns the SFBM) the agent entry
            // becomes non-null and the checkpoint runs against the
            // agent-owned table_storage_t — table_storage_t::checkpoint
            // is a no-op for IN_MEMORY (mode_ != DISK) so the agent
            // branch is harmless when invoked against an IN_MEMORY twin.
            if (disk_backed && freshly_created.count(catalog::well_known_oid::pg_settings_table)) {
                constexpr auto settings_oid = catalog::well_known_oid::pg_settings_table;
                const collection_storage_entry_t* entry = nullptr;
                if (!agents_.empty() && agents_[0] != nullptr) {
                    entry = agents_[0]->storage_entry_sync(settings_oid);
                }
                if (entry != nullptr) {
                    // const_cast — table_storage_t::checkpoint mutates
                    // the SFBM/free-list. storage_entry_sync returns a
                    // const pointer (the slice's unique_ptr ownership is
                    // immutable across the actor boundary), but the
                    // checkpoint operation is a routine maintenance
                    // mutation on the entry's interior state. Same
                    // Constraint #11 carve-out as the sync probe — the
                    // agent thread is idle vs. this bootstrap-time call.
                    const_cast<collection_storage_entry_t*>(entry)->table_storage.checkpoint();
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
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, db.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(db.name)));
                });
                direct_append_sync(pg_database_oid, row, tz);
            }
        }

        if (freshly_created.count(pg_namespace_oid_tbl)) {
            if (auto* def = catalog::find_system_table("pg_namespace")) {
                for (const auto& nrow : builtin_namespace_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                        chunk.set_value(0, 0, lv_oid(res, nrow.oid));
                        chunk.set_value(1, 0, lv_str(res, std::string(nrow.name)));
                    });
                    direct_append_sync(pg_namespace_oid_tbl, row, tz);
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
                    direct_append_sync(pg_type_oid, row, tz);
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
                    direct_append_sync(pg_proc_oid, row, tz);
                }
            }
        }

        if (disk_backed) {
            for (auto tbl_oid : freshly_created) {
                // Step 8.4.A unblocker — agent-first checkpoint target
                // probe (same rationale as the pg_settings early-checkpoint
                // branch above). Catalog OIDs all live on agent 0; DISK
                // record-only markers (current state for catalog DISK
                // entries until 8.1.B) make storage_entry_sync return
                // nullptr, so we fall back to the manager SFBM. The
                // table_storage_t::checkpoint method is a no-op when
                // mode_ != DISK, which makes the IN_MEMORY-twin branch
                // (post-8.1.B, when the agent owns the entry) safe even
                // for the catalog OIDs whose agent twin happens to be
                // IN_MEMORY today.
                const collection_storage_entry_t* entry = nullptr;
                if (!agents_.empty() && agents_[0] != nullptr) {
                    entry = agents_[0]->storage_entry_sync(tbl_oid);
                }
                if (entry != nullptr) {
                    const_cast<collection_storage_entry_t*>(entry)->table_storage.checkpoint();
                }
            }
        }
    }

    void manager_disk_t::load_system_tables_sync() {
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. Walks the
        // on-disk sys_dir and re-hydrates every pg_* catalog table via
        // load_storage_disk_sync, which constructs the SFBM directly on
        // agents_[0] (catalog agent). Skip-if-present probe uses
        // has_storage_sync against the agent slice — it reports true for
        // both IN_MEMORY twins and record-only markers, preventing double-load.
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
            if (!agents_.empty() && agents_[0] != nullptr) {
                if (agents_[0]->has_storage_sync(tbl_oid))
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; agents_[0]
        // (catalog agent) is the sole owner of catalog SFBM entries.
        // Pre-scheduler-start, single-threaded — Constraint #11 carve-out.
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
        // Step 8.8 Path A contract: walks ${config_.path}/${db_oid}/${tbl_oid}
        // for every user table directory and calls load_storage_disk_sync,
        // which (per Step 8.2 + Step 8.1.A) emplaces into manager.storages_
        // AND records the OID on the routed agent slice
        // (agents_[pool_idx_for_oid] via bootstrap_record_oid_sync).
        // User tables therefore become routable AS SOON AS THE AGENTS ARE
        // SPAWNED (precondition: agents_ non-empty when this runs — see
        // base_spaces.cpp pre-scheduler-start ordering).
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
                // §8.1.C — user-OID SFBM ownership now lives on the routed
                // agent slice. Use has_storage(table_oid) which probes the
                // agent first and falls back to manager.storages_ only for
                // the no-agents test fixture (see manager_disk.hpp).
                if (has_storage(tbl_oid))
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; agents_[0]
        // (catalog agent) is the sole owner of pg_class. Pre-scheduler-start,
        // single-threaded — Constraint #11 carve-out (see header comment on
        // agent_disk_t::storage_entry_sync).
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

    std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>>
    manager_disk_t::scan_dropped_oids_sync() {
        // Step 8.8 Path A contract: pure READ of manager.storages_[pg_class]
        // with COMMITTED_ROWS scan to surface tombstoned user OIDs.
        // Pre-scheduler-start, single-threaded; agent-side migration is
        // covered by Step 8.9 risk register entry #3.
        //
        // dec 37 V1 catalog scan rebuild. Walk pg_class twice — once with
        // COMMITTED_ROWS (sees every committed row, including tombstoned
        // ones) to collect every user OID ever recorded, then compute the
        // set-difference against alive_user_oids_sync (which uses
        // COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) to isolate the
        // tombstoned rows. The difference is the set of "DROP TABLE
        // committed, GC pending" user oids that need to be re-registered
        // into dropped_storages_ so a post-restart horizon advance can
        // finish removing their .otbx files.
        std::pmr::vector<std::pair<components::catalog::oid_t, std::uint64_t>> result{resource_};
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; agents_[0]
        // (catalog agent) is the sole owner of pg_class. Pre-scheduler-start,
        // single-threaded — Constraint #11 carve-out.
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

        // COMMITTED_ROWS scan — includes tombstoned rows. Use create_index_scan
        // which exposes the table_scan_type parameter (the plain
        // scan_committed/scan APIs are hard-wired to
        // COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED). Returns false when the scan
        // is fully drained.
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
                const bool produced = table.create_index_scan(scan_state,
                                                              chunk,
                                                              components::table::table_scan_type::COMMITTED_ROWS);
                if (!produced) {
                    break;
                }
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto val = chunk.value(0, i);
                    if (val.is_null())
                        continue;
                    const auto seen = static_cast<catalog::oid_t>(val.value<std::uint32_t>());
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
        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted; agents_[0]
        // (catalog agent) is the sole owner of pg_settings. Pre-scheduler-
        // start, single-threaded — Constraint #11 carve-out.
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
                auto key_val = chunk.value(0, i);
                if (key_val.is_null()) {
                    continue;
                }
                if (key_val.value<std::string_view>() == name) {
                    auto setting_val = chunk.value(1, i);
                    if (!setting_val.is_null()) {
                        last_value = std::string{setting_val.value<std::string_view>()};
                    }
                }
            }
        }
        return last_value;
    }

} // namespace services::disk
