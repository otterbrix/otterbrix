#include "manager_disk_impl.hpp"

#include <charconv>
#include <stdexcept>

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
    } // namespace

    void manager_disk_t::bootstrap_system_tables_sync() {
        // An empty path names no directory a `.otbx` could live in, so there is nothing this
        // call could honestly do. Refuse loudly rather than manufacture a relative-path database
        // under the process CWD (rule 6). No production configuration reaches this: every
        // binding fills `config_disk::path` from `<base>/…`, and the C++ constructor cannot
        // produce an empty one.
        if (config_.path.empty()) {
            error(log_,
                  "manager_disk_t::bootstrap_system_tables_sync: config_disk::path is empty — there is no "
                  "directory to bootstrap pg_catalog into; refusing");
            return;
        }
        const auto sys_db_oid = catalog::well_known_oid::main_database;
        const std::filesystem::path sys_dir = config_.path / std::to_string(static_cast<unsigned>(sys_db_oid));
        std::filesystem::create_directories(sys_dir);

        // The system tables this bootstrap writes builtin rows into — the only ones for which
        // "loaded, and empty" is a LOSS rather than a legitimate steady state. Kept next to the
        // seeding branches below, which are exactly these five.
        auto has_builtin_seed_rows = [](catalog::oid_t tbl_oid) {
            return tbl_oid == catalog::well_known_oid::pg_settings_table || tbl_oid == pg_database_oid ||
                   tbl_oid == pg_namespace_oid_tbl || tbl_oid == pg_type_oid || tbl_oid == pg_proc_oid;
        };

        // How many rows a system table holds right now, on agents_[0]. Used for the
        // came-up / was-seeded post-conditions below.
        auto rows_in_sync = [&](catalog::oid_t tbl_oid) -> std::uint64_t {
            if (agents_.empty() || agents_[0] == nullptr) {
                return 0;
            }
            const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(tbl_oid);
            if (entry == nullptr) {
                return 0;
            }
            return const_cast<collection_storage_entry_t*>(entry)->table_storage.table().calculate_size();
        };

        // Helper: load or create a single system table. Returns true if the table has to be
        // SEEDED — freshly created, or loaded healthy but empty (see the young-file note below).
        auto bootstrap_one = [&](const components::catalog::system_table_def_t& def) -> bool {
            // The schema array carries the well-known OID for every system table
            // (catalog_oids.hpp), so there is no name lookup and no "unknown table" case.
            const auto tbl_oid = def.relation_oid;
            // agents_[0] (CATALOG agent) is the sole source of truth for
            // pg_* system tables.
            if (!agents_.empty() && agents_[0] != nullptr) {
                if (agents_[0]->has_storage_sync(tbl_oid))
                    return false;
            }
            bool needs_seeding = false;
            bool took_create_leg = false;
            const auto otbx = sys_dir / std::to_string(static_cast<unsigned>(tbl_oid)) / "table.otbx";
            {
                std::filesystem::create_directories(otbx.parent_path());
                if (std::filesystem::exists(otbx)) {
                    trace(log_,
                          "manager_disk_t::bootstrap_system_tables_sync loading : {} oid={}",
                          std::string(def.name),
                          static_cast<unsigned>(tbl_oid));
                    // WHY THIS THROWS while load_storage_disk_sync only reports: that function's
                    // load ctor runs on the agent thread inside bootstrap_create_disk_inner_sync,
                    // which is noexcept (manager_disk.hpp) — a throw from THERE terminates. This
                    // throw is from the lambda's own frame, on the bootstrap thread, after that
                    // call returned and long before scheduler_*->start(); it is the same
                    // std::runtime_error the base_spaces startup refusals use, catchable by the
                    // embedder.
                    //
                    // AND WHY IT REFUSES AT ALL. Leaving the table unloaded and answering "not
                    // freshly created" would also skip the seeding branch, so the engine would
                    // come up with an EMPTY pg_catalog over live storage and the next DDL would
                    // mint fresh oids on top of it (restore_oid_generator_sync skips a table it
                    // cannot see, so the frontier drops below oids that exist). The refusal writes
                    // and deletes nothing, and a repeat start with the cause removed succeeds.
                    //
                    // The builtin schema is the catalog for a system table: what create would have
                    // used, and the overlay a never-checkpointed .otbx opens empty with.
                    if (auto err = load_storage_disk_sync(tbl_oid, sys_db_oid, otbx, def.columns);
                        err.contains_error()) {
                        error(log_,
                              "bootstrap REFUSED , system table {} oid={} could not be opened: {}",
                              std::string(def.name),
                              static_cast<unsigned>(tbl_oid),
                              err.what.c_str());
                        throw std::runtime_error(
                            "a pg_catalog system table could not be opened, refusing to start: " +
                            std::string(err.what.c_str()));
                    }
                    // THE SILENT TWIN. A crash between "the .otbx was created" and "its first
                    // checkpoint committed" leaves a proven-young file: it opens HEALTHY and EMPTY
                    // (the builtin schema is overlaid). Reporting that as "not freshly created"
                    // would skip every seeding branch and leave the catalog empty with no error
                    // anywhere; refusing would repeat on every start over a file that is fine. So
                    // it counts as freshly created and the seeding runs.
                    //
                    // ONLY FOR THE TABLES THAT OWN BUILTIN ROWS. For every other pg_* table an
                    // empty load is the normal steady state of a database where nothing of that
                    // kind was created, and calling it "fresh" would rewrite it on EVERY start —
                    // which breaks bootstrap idempotence (the .otbx grows a checkpoint per
                    // restart). NAMED SIDE EFFECT, ACCEPTED: if a user deleted the builtin pg_proc
                    // rows (operator_unregister_udf can), a restart puts them back.
                    needs_seeding = has_builtin_seed_rows(tbl_oid) && rows_in_sync(tbl_oid) == 0;
                } else {
                    trace(log_,
                          "manager_disk_t::bootstrap_system_tables_sync creating disk : {} oid={}",
                          std::string(def.name),
                          static_cast<unsigned>(tbl_oid));
                    // System tables are never computed (relkind='g' is user-table-only). The
                    // create reports, but the shared post-condition below is what decides the
                    // refusal (it also covers the load leg and the transfer); this only adds the
                    // CAUSE to the log line.
                    if (auto create_err =
                            create_storage_disk_sync(tbl_oid, sys_db_oid, def.columns, otbx, /*is_computed=*/false);
                        create_err.contains_error()) {
                        error(log_,
                              "bootstrap , system table {} oid={} could not be created: {}",
                              std::string(def.name),
                              static_cast<unsigned>(tbl_oid),
                              create_err.what.c_str());
                    }
                    took_create_leg = true;
                    needs_seeding = true;
                }
            }
            // ONE shared post-condition over BOTH legs. create_storage_disk_sync returns void
            // and swallows construction_failed() (agent_disk.cpp, reachable with a device that
            // refuses the very first write); bootstrap_disk_inner_sync collapses three outcomes
            // into one bool. The load leg refuses on transfer_to_agent's result directly, and
            // this post-condition stands behind it as the one check that also covers the create
            // leg. Every failure ends the same way — no storage for this oid — and this is the
            // one place that can see it.
            if (agents_.empty() || agents_[0] == nullptr || !agents_[0]->has_storage_sync(tbl_oid)) {
                // A REFUSAL MUST BE RETRYABLE, so it may not leave behind the one thing that
                // would block the retry: the zero-byte file a create leaves when its very first
                // write is refused. That cleanup lives inside create_storage_disk_sync, which is
                // where every caller reaches it — rehydrate and replay synthesis run the same
                // create. `took_create_leg` tells this post-condition which leg the table came
                // through, so the load leg is never reported as a failed create.
                error(log_,
                      "bootstrap REFUSED , system table {} oid={} did not come up on the {} leg (path {})",
                      std::string(def.name),
                      static_cast<unsigned>(tbl_oid),
                      took_create_leg ? "create" : "load",
                      otbx.string());
                throw std::runtime_error("a pg_catalog system table did not come up, refusing to start: " +
                                         std::string(def.name));
            }
            return needs_seeding;
        };

        // THE BOUNDARY, AND IT IS HONOURED BELOW. Refuse only where a repeat start with the
        // cause removed SUCCEEDS. So: no throw on a failed CHECKPOINT of a freshly created
        // system table (that would leave a never-checkpointed .otbx which loads empty forever
        // and would then refuse forever), and no throw on a successful EMPTY load (seeded
        // above instead).

        // direct_append_sync answers with the appended row's START ROW, not a count
        // (table_storage_adapter_t::append; manager_disk_storage.cpp), so its VALUE alone cannot
        // tell a seeded row from one that never landed. Its wrapper can, and each seeding append
        // binds it, so a refused row is named here with its cause rather than inferred from the
        // row count require_seeded checks after. The post-condition still stands on the TABLE: a
        // seed can go missing without any single append refusing, and a system table this
        // bootstrap seeded must hold exactly the rows it wrote.
        auto seed_row = [&](catalog::oid_t tbl_oid, std::string_view tbl_name, components::vector::data_chunk_t& row) {
            if (auto seeded = direct_append_sync(tbl_oid, row); seeded.has_error()) {
                error(log_,
                      "bootstrap , builtin row for system table {} oid={} was not written: {}",
                      std::string(tbl_name),
                      static_cast<unsigned>(tbl_oid),
                      seeded.error().what.c_str());
            }
        };

        auto require_seeded = [&](catalog::oid_t tbl_oid, std::string_view tbl_name, std::uint64_t expected) {
            const auto seeded = rows_in_sync(tbl_oid);
            if (seeded != expected) {
                error(log_,
                      "bootstrap REFUSED , system table {} oid={} kept {} of the {} builtin rows it was seeded with",
                      std::string(tbl_name),
                      static_cast<unsigned>(tbl_oid),
                      seeded,
                      expected);
                throw std::runtime_error("a pg_catalog system table could not be seeded, refusing to start: " +
                                         std::string(tbl_name));
            }
        };

        std::unordered_set<catalog::oid_t> freshly_created;

        // Bootstrap pg_settings FIRST so stored_catalog_ is populated before any
        // other table's seeding calls direct_append_sync (which takes the timezone).
        if (const auto* settings_def = catalog::find_system_table(pg_settings_oid)) {
            if (bootstrap_one(*settings_def)) {
                freshly_created.insert(catalog::well_known_oid::pg_settings_table);
                auto row = make_row(resource(), settings_def->columns, [&](data_chunk_t& chunk, auto*) {
                    chunk.set_value(0, 0, std::string_view("TimeZone"));
                    // Lowercase, deliberately: it is what SET TIMEZONE would store (the SQL
                    // transformer lowercases the name before it reaches the plan), and the ONLY
                    // form core::date::timezone_to_offset recognizes -- its contract is
                    // lowercase input. Seeding "UTC" here makes every start write a default and
                    // then WARN about the stored catalog refusing it.
                    chunk.set_value(1, 0, std::string_view("utc"));
                });
                seed_row(catalog::well_known_oid::pg_settings_table, settings_def->name, row);
                require_seeded(catalog::well_known_oid::pg_settings_table, settings_def->name, 1);
            }
            auto tz_name = read_setting_sync("TimeZone");
            if (!tz_name.empty()) {
                if (auto err = stored_catalog_.set_timezone(resource(), tz_name); err.contains_error()) {
                    warn(log_, "bootstrap: stored catalog refused timezone '{}': {}", tz_name, err.what);
                }
            }
        }

        // Remaining tables — pg_settings is already in storages_ so bootstrap_one skips it.
        for (const auto& def : components::catalog::all_system_tables()) {
            if (bootstrap_one(def)) {
                freshly_created.insert(def.relation_oid);
            }
        }

        if (freshly_created.empty() ||
            freshly_created == std::unordered_set<catalog::oid_t>{catalog::well_known_oid::pg_settings_table}) {
            // Only pg_settings was freshly created — checkpoint it. storage_entry_sync
            // returns nullptr for a record-only marker, so the checkpoint runs against
            // whichever entry holds the SFBM.
            if (freshly_created.count(catalog::well_known_oid::pg_settings_table)) {
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

        if (freshly_created.count(pg_database_oid)) {
            if (auto* def = catalog::find_system_table(pg_database_oid)) {
                const auto db = builtin_database_row();
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                    chunk.set_value(0, 0, db.oid);
                    chunk.set_value(1, 0, db.name);
                });
                seed_row(pg_database_oid, def->name, row);
                require_seeded(pg_database_oid, def->name, 1);
            }
        }

        if (freshly_created.count(pg_namespace_oid_tbl)) {
            if (auto* def = catalog::find_system_table(pg_namespace_oid_tbl)) {
                std::uint64_t written = 0;
                for (const auto& nrow : builtin_namespace_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                        chunk.set_value(0, 0, nrow.oid);
                        chunk.set_value(1, 0, nrow.name);
                    });
                    seed_row(pg_namespace_oid_tbl, def->name, row);
                    ++written;
                }
                require_seeded(pg_namespace_oid_tbl, def->name, written);
            }
        }

        if (freshly_created.count(pg_type_oid)) {
            if (auto* def = catalog::find_system_table(pg_type_oid)) {
                std::uint64_t written = 0;
                for (const auto& trow : builtin_type_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                        chunk.set_value(0, 0, trow.oid);
                        chunk.set_value(1, 0, trow.name);
                        chunk.set_value(2, 0, pg_catalog_ns_oid);
                    });
                    seed_row(pg_type_oid, def->name, row);
                    ++written;
                }
                require_seeded(pg_type_oid, def->name, written);
            }
        }

        if (freshly_created.count(pg_proc_oid)) {
            if (auto* def = catalog::find_system_table(pg_proc_oid)) {
                std::uint64_t written = 0;
                for (const auto& frow : builtin_proc_rows()) {
                    auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto*) {
                        chunk.set_value(0, 0, frow.oid);
                        chunk.set_value(1, 0, frow.name);
                        chunk.set_value(2, 0, pg_catalog_ns_oid);
                    });
                    seed_row(pg_proc_oid, def->name, row);
                    ++written;
                }
                require_seeded(pg_proc_oid, def->name, written);
            }
        }

        for (auto tbl_oid : freshly_created) {
            // Checkpoint each fresh catalog table (same probe as the pg_settings branch above).
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

    void manager_disk_t::restore_oid_generator_sync() {
        // agents_[0] (catalog agent) owns all catalog SFBM entries.
        // Pre-scheduler-start, single-threaded.
        if (agents_.empty() || agents_[0] == nullptr) {
            trace(log_, "manager_disk_t::restore_oid_generator_sync : no catalog agent, skipping");
            return;
        }

        components::catalog::oid_t high_water = components::catalog::FIRST_USER_OID - 1;
        core::pmr::otterbrix_resource scan_resource;

        for (const auto& tbl : catalog::all_system_tables()) {
            const auto tbl_oid = tbl.relation_oid;
            const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(tbl_oid);
            if (entry == nullptr) {
                continue;
            }
            auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
            if (table.column_count() == 0 || table.calculate_size() == 0) {
                continue;
            }
            // Column 0 is the identity OID only for oid-keyed system tables, where it
            // is a UINTEGER (oid_col()). Some system tables (e.g. pg_settings) key on a
            // STRING column 0 (`name`); reading that as a uint32 OID yields garbage that
            // poisons oid_gen_ with a huge, non-deterministic high_water — every fresh
            // CREATE TABLE then mints a wild OID, and on reopen the persisted (garbage)
            // catalog OID no longer matches the storage the agent loaded, so user-table
            // appends silently no-op.
            //
            // pg_computed_column's ALLOCATED oid is `attoid` (column 1); column 0 is the
            // parent relid, which never raises the frontier past pg_class. Skipping the
            // attoids lets a reopened engine re-mint an attoid already taken, and the
            // duplicate breaks the attoid sort in resolve_table, so the catalog column
            // order diverges from the storage order and every pushed-down filter on the
            // table matches zero rows.
            const std::uint64_t id_col = (tbl_oid == catalog::well_known_oid::pg_computed_column_table)
                                             ? catalog::pg_computed_column_col::attoid
                                             : 0;
            if (table.columns()[id_col].type().type() != components::types::logical_type::UINTEGER) {
                continue;
            }
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(id_col));
            components::table::table_scan_state scan_state(&scan_resource);
            table.initialize_scan(scan_state, col_indices);

            // The scan writes into chunk.data[storage column index] (same sparse
            // chunk-slot convention as scan_live_table_oids_sync), so the chunk
            // needs a slot per storage column up to id_col; only id_col gets a
            // buffer.
            const auto& all_cols = table.columns();
            std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
            all_types.reserve(all_cols.size());
            for (const auto& c : all_cols) {
                all_types.push_back(c.type());
            }
            const std::vector<std::size_t> projected{static_cast<std::size_t>(id_col)};

            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       all_types,
                                                       projected,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                table.scan(chunk, scan_state);
                if (chunk.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    if (chunk.is_null(id_col, i)) {
                        continue;
                    }
                    const auto seen =
                        static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(id_col, i));
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

        core::pmr::otterbrix_resource scan_resource;
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
                // No overlay passed — load_storage_disk_sync resolves the columns from
                // pg_attribute. On the PRE-replay walk a never-checkpointed .otbx whose
                // catalog rows still sit in the WAL is deferred (traced, not an error) and
                // picked up by the post-replay walk in base_spaces.
                if (auto err = load_storage_disk_sync(tbl_oid, db_oid, otbx, {}); err.contains_error()) {
                    warn(log_,
                         "load_user_table_storages_sync: failed for oid={} : {}",
                         static_cast<unsigned>(tbl_oid),
                         err.what.c_str());
                }
            }
        }
    }

    core::result_wrapper_t<std::size_t> manager_disk_t::rehydrate_missing_user_storages_sync() {
        // Every user table is disk-backed, so after load_user_table_storages_sync has loaded every
        // on-disk .otbx, any alive user table still missing a storage lost its file. The header
        // states the window and what a missing storage costs; this body only has to be honest
        // about not running.
        //
        // THE WALK DID NOT RUN IS NOT THE WALK FOUND NOTHING. Answering 0 on the four returns
        // below — the same value a start where every alive table has its storage gives — would
        // leave the one caller that reads the count unable to tell a healthy database from one
        // where this walk never looked at a single table.
        if (agents_.empty() || agents_[0] == nullptr) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"rehydrate_missing_user_storages_sync: there is no catalog agent to "
                                                  "read pg_class from; no alive table could be examined",
                                                  resource()});
        }
        // An empty path names no directory to recreate a file in. Refuse up front rather than
        // build relative paths under the process CWD (rule 6).
        if (config_.path.empty()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"rehydrate_missing_user_storages_sync: config_disk::path is empty — "
                                                  "no directory to recreate a lost .otbx in; refusing",
                                                  resource()});
        }
        // Every alive user table this walk could NOT give a storage back to. The walk exists
        // to close the catalog/storage divergence; the count is what it leaves open.
        std::size_t unclosed = 0;

        // Pass 1: scan pg_class for alive user tables that have row storage (relkind 'r' regular
        // or 'm' materialized view) and are not yet loaded. pg_class layout: [0=oid, 1=relname,
        // 2=relnamespace, 3=relkind, 4=relstoragemode].
        //
        // `relnamespace` is read here too: the recreated `.otbx` has to land where
        // create_storage_disk put the original — `${db_root}/${relnamespace}/${oid}/`. A hardwired
        // well_known_oid::main_database (4) is a value no user table carries (CREATE DATABASE
        // allocates its namespace from FIRST_USER_OID upward), and a rehydrated file under oid 4
        // is one the next restart's directory walk does find (it accepts any numeric directory)
        // but the table's own resolve never looks for — leaving the catalog and the storage
        // exactly as far apart as before.
        std::vector<std::pair<catalog::oid_t, catalog::oid_t>> need_oids; // (table oid, namespace oid)
        {
            const collection_storage_entry_t* cls_entry = agents_[0]->storage_entry_sync(pg_class_oid);
            if (cls_entry == nullptr) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"rehydrate_missing_user_storages_sync: pg_class is not loaded — "
                                                      "no alive table can be named, so no lost .otbx can be recreated",
                                                      resource()});
            }
            auto& cls_table = const_cast<collection_storage_entry_t*>(cls_entry)->table_storage.table();
            if (cls_table.column_count() < 4) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{"rehydrate_missing_user_storages_sync: pg_class carries only " +
                                         std::to_string(cls_table.column_count()) +
                                         " columns and cannot be scanned for alive tables",
                                     resource()});
            }
            if (cls_table.calculate_size() == 0) {
                // An empty pg_class names no table: nothing to rehydrate, honestly.
                return std::size_t{0};
            }
            core::pmr::otterbrix_resource scan_resource;
            // Sparse scan of the non-adjacent [oid (0), relkind (3)] columns via
            // the projected_cols data_chunk ctor: pass the FULL pg_class type list
            // plus the projected absolute indices, and read by ABSOLUTE index
            // (chunk.value(0,..) / chunk.value(3,..)). A compacted 2-type chunk
            // mis-decodes the dictionary-encoded relkind string column (segfault).
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0)); // oid
            col_indices.emplace_back(static_cast<int64_t>(2)); // relnamespace
            col_indices.emplace_back(static_cast<int64_t>(3)); // relkind
            components::table::table_scan_state scan_state(&scan_resource);
            cls_table.initialize_scan(scan_state, col_indices);
            const auto& all_cols = cls_table.columns();
            std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
            all_types.reserve(all_cols.size());
            for (const auto& c : all_cols) {
                all_types.push_back(c.type());
            }
            const std::vector<std::size_t> projected{static_cast<std::size_t>(0),
                                                     static_cast<std::size_t>(2),
                                                     static_cast<std::size_t>(3)};
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
                    if (chunk.is_null(2, i)) {
                        // Rule 6: the namespace names the directory the file has to be
                        // recreated in. Nothing else in the row implies it.
                        error(log_,
                              "manager_disk_t::rehydrate_missing_user_storages_sync: pg_class row oid={} "
                              "carries no relnamespace; cannot place its .otbx and refusing to guess",
                              static_cast<unsigned>(oid));
                        ++unclosed;
                        continue;
                    }
                    const auto ns_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(2, i));
                    need_oids.emplace_back(oid, ns_oid);
                }
            }
        }
        if (need_oids.empty()) {
            return unclosed;
        }

        // Pass 2: one pg_attribute scan resolving every needed table's columns in attnum
        // (ordinal) order. Shared with the young-.otbx schema overlay (load_storage_disk_sync) —
        // same catalog, same read, one implementation.
        std::unordered_set<catalog::oid_t> wanted;
        wanted.reserve(need_oids.size());
        for (const auto& need : need_oids) {
            wanted.insert(need.first);
        }
        auto cols_by_relid = collect_catalog_columns_sync(wanted);

        // Pass 3: recreate the missing .otbx for each table at the standard path
        // ${db_root}/${relnamespace}/${oid}/table.otbx — the layout create_storage_disk uses.
        for (const auto& [oid, ns_oid] : need_oids) {
            auto otbx = config_.path / std::to_string(static_cast<unsigned>(ns_oid)) /
                        std::to_string(static_cast<unsigned>(oid)) / "table.otbx";

            // WHAT THIS WALK IS FOR, AND WHAT IT IS NOT FOR. It recreates the file of a table
            // whose .otbx was LOST — a freshly created file's directory entry is not fsynced, so a
            // crash can keep the catalog row and lose the file. A table whose .otbx is PRESENT is
            // a different state entirely: it was refused by the loader (a corrupt sidecar, a
            // rotten header) and is still every byte the operator has, so creating over it would
            // answer a refusal by destroying the thing that was refused. Both states arrive here
            // as "no storage for this oid"; only one of them is this walk's.
            std::error_code file_ec;
            if (std::filesystem::exists(otbx, file_ec) && !file_ec) {
                // AND ONE OF THE PRESENT FILES IS NOBODY'S FAULT. A file of exactly BLOCK_START
                // bytes is the never-checkpointed signature: the pre-replay walk DEFERS it because
                // the catalog rows describing its schema are still only in the WAL, and the
                // post-replay walk in base_spaces opens it as a legitimately empty table once
                // replay has put them back. That is a table mid-recovery, not a divergence, and
                // calling it one would report an error on every crash-before-first-checkpoint
                // restart.
                const auto file_bytes = std::filesystem::file_size(otbx, file_ec);
                if (!file_ec && file_bytes == components::table::storage::BLOCK_START) {
                    trace(log_,
                          "manager_disk_t::rehydrate_missing_user_storages_sync: oid={} has a never-checkpointed "
                          "{} — deferred to the post-replay walk, not rehydrated",
                          static_cast<unsigned>(oid),
                          otbx.string());
                    continue;
                }
                error(log_,
                      "manager_disk_t::rehydrate_missing_user_storages_sync: alive table oid={} has no storage but "
                      "its file {} is present — it did not load, and rehydrate does not create over a file that "
                      "exists. The catalog and the storage layer stay apart for this table.",
                      static_cast<unsigned>(oid),
                      otbx.string());
                ++unclosed;
                continue;
            }

            auto it = cols_by_relid.find(oid);
            if (it == cols_by_relid.end() || it->second.empty()) {
                // A LEGITIMATE SKIP THAT MUST STILL BE HEARD. Nothing in this process can
                // resolve columns pg_attribute does not have, and a zero-column storage is
                // worse than none — so the skip is right and a refusal here would repeat on
                // every start. It must not be silent, though: this is the one walk written to
                // notice exactly this divergence.
                error(log_,
                      "manager_disk_t::rehydrate_missing_user_storages_sync: alive table oid={} (ns={}) has a live "
                      "pg_class row, no storage and no pg_attribute columns — its .otbx cannot be rebuilt and the "
                      "catalog still names a table this engine cannot serve",
                      static_cast<unsigned>(oid),
                      static_cast<unsigned>(ns_oid));
                ++unclosed;
                continue;
            }
            auto defs = std::move(it->second);
            trace(log_,
                  "manager_disk_t::rehydrate_missing_user_storages_sync : oid={} ns={} cols={}",
                  static_cast<unsigned>(oid),
                  static_cast<unsigned>(ns_oid),
                  defs.size());
            std::filesystem::create_directories(otbx.parent_path());
            // Never computed here: the alive-oid scan is filtered to relkind 'r'/'m'
            // (computed tables have no pg_attribute schema and are recovered by WAL
            // replay synthesis instead — see the method comment).
            if (auto err = create_storage_disk_sync(oid, ns_oid, std::move(defs), otbx, /*is_computed=*/false);
                err.contains_error()) {
                error(log_,
                      "manager_disk_t::rehydrate_missing_user_storages_sync: could not recreate the lost .otbx of "
                      "alive table oid={} : {}",
                      static_cast<unsigned>(oid),
                      err.what.c_str());
                ++unclosed;
            }
        }
        return unclosed;
    }

    // Re-arm a DISK-backed column drop whose release a crash discarded. The window it closes, the
    // bootstrap ORDERING it depends on and the relkind='g' exclusion are stated at the declaration
    // in manager_disk.hpp; what follows is the evidence and the two decisions not derivable from it.
    //
    // WHY BOOTSTRAP AND NOT THE CHECKPOINT ROUND. The comparison needs the catalog, and the disk
    // agent holds none: at checkpoint it would be a cross-actor read from inside the per-entry
    // loop. Here the catalog is a synchronous read of agent 0's own slice on the single-threaded
    // pre-scheduler-start thread — no message, no await, no shared state.
    //
    // RE-ARMING ALONE IS A NO-OP, not a shortcut — this is the load-bearing decision, and it is
    // MEASURED rather than argued. release_dropped_column_blocks() proves NON-ownership per id, and
    // its second subtraction is "the live collection does not name it" (several columns pack into
    // one 256 KiB block, so a candidate id routinely still carries a survivor). Leave the column in
    // the collection and collect_disk_block_ids reports EVERY one of its blocks as live, so every
    // armed id is skipped and the set is drained having freed nothing: run with the rebuild removed
    // and the ids armed on their own, the durable root names the same 4 data blocks after the
    // restart as before it — byte for byte the do-nothing outcome — with no free-list overlap and
    // no unexplained block. The opposite risk — arming ids the collection still references, which
    // would be a BAD FREE — is exactly what that subtraction exists to make impossible, and it is
    // why the order is fixed: the column must leave the collection FIRST. So the drop is performed
    // with the very primitive the commit path uses, table_storage_t::drop_column, which does both
    // halves in the one order that is safe: name the ids, then rebuild.
    //
    // That rebuild is NOT a physical rewrite of the table. data_table_t(parent, removed_column)
    // SHARES every surviving column with the successor collection and simply forgets the dropped
    // one — zero blocks allocated, no segment rewritten, nothing written to the file. The bytes
    // only move at the next checkpoint, which is where the release belongs.
    //
    // Rule 6: a catalog read that fails must be loud, never a quiet skip. Bootstrap has no
    // statement to fail, so the shape is scan_storage_for_rebuild_sync's — log at error and change
    // NOTHING. Which way to fail is not symmetric: leaving a leak is recoverable (the next start
    // re-derives it from the same two durable facts), physically dropping a column the catalog does
    // describe is not. So every ambiguous reading refuses.
    //
    // WHAT THIS WALK COMPARES ON, AND WHY IT CANNOT BE THE NAME. "In the storage, not in the live
    // catalog" reads as "dropped" only while nothing can change a column's name in pg_attribute
    // without changing it in the storage in the same durable breath — and nothing can guarantee
    // that, because the two halves are durable at DIFFERENT points. An ALTER TABLE RENAME COLUMN's
    // catalog half is durable at the WAL commit marker; its storage half only at that table's NEXT
    // CHECKPOINT, unbounded later. A crash in between leaves a storage naming the old column
    // against a catalog naming the new one, and a name-keyed walk reads that as a DROP and
    // physically releases a SURVIVING column's blocks. Reversing the order does not help: the
    // storage's durability point is later either way, and the comparison destroys on divergence in
    // BOTH directions. The window is reachable from one ordinary statement, and it costs the column
    // with all of its data.
    //
    // So the key is the IDENTITY the catalog minted for the column — pg_attribute.attoid — which
    // the storage carries per column and serializes into the .otbx. A rename does not move it, so
    // the divergence stops being observable here; what a rename leaves behind is a stale storage
    // NAME, and this walk repairs that FROM the catalog instead of acting on it. It therefore does
    // not DEPEND on operator_alter_column_rename_t / manager_disk_t::rename_storage_column having
    // run — that path still writes the storage name at commit, which is the cheap way to keep the
    // two halves agreeing without a restart. On the oid the two states the name conflated are
    // finally distinct, and in opposite directions: a RENAMED column's attoid IS in the live
    // catalog (under another name), while an ALTER ADD COLUMN not yet materialised is an attoid in
    // the catalog with NO storage column — legal, common, and handled positively rather than by
    // being ignored (its identity is published forward so the INSERT that materialises it is born
    // identified).
    //
    // A storage column with NO attoid is refused, loudly, for the whole table. See the note at the
    // refusal itself for why that refusal is here and not on the load path.
    void manager_disk_t::rearm_dropped_column_blocks_sync() {
        if (agents_.empty() || agents_[0] == nullptr) {
            return;
        }
        // relkind 'r'/'m' only — see the note above on 'g'.
        auto live_oids = scan_live_table_oids_sync();
        std::unordered_set<catalog::oid_t> wanted;
        std::pmr::vector<catalog::oid_t> ordered{resource()};
        for (auto oid : live_oids) {
            // No loaded storage means no physical columns to compare against. Not an error:
            // rehydrate already recreated everything the catalog describes and whose file was
            // lost, and a record-only marker owns nothing to release.
            if (!has_storage(oid)) {
                continue;
            }
            if (wanted.insert(oid).second) {
                ordered.push_back(oid);
            }
        }
        if (ordered.empty()) {
            return;
        }

        auto cols_by_relid = collect_catalog_columns_sync(wanted);
        if (cols_by_relid.empty()) {
            // pg_attribute is unreadable / empty while user tables are loaded and describable.
            // Reading that as "every column of every table was dropped" is the one mistake this
            // walk must never make, so it stops here, loudly, having touched nothing.
            error(log_,
                  "manager_disk_t::rearm_dropped_column_blocks_sync: pg_attribute resolved NO columns for "
                  "{} loaded user table(s) — refusing to treat that as a drop; blocks released by a "
                  "pre-crash ALTER stay leaked until the catalog reads again",
                  ordered.size());
            return;
        }

        for (auto oid : ordered) {
            const std::size_t pool_idx = pool_idx_for_oid(oid, agents_.size());
            if (pool_idx >= agents_.size() || agents_[pool_idx] == nullptr) {
                continue;
            }
            // Same borrow the sibling bootstrap walks take: agent-owned entry, read
            // synchronously on the pre-scheduler-start thread (see storage_entry_sync).
            const collection_storage_entry_t* entry = agents_[pool_idx]->storage_entry_sync(oid);
            if (entry == nullptr || entry->storage == nullptr) {
                continue;
            }
            auto* owned = const_cast<collection_storage_entry_t*>(entry);
            if (owned->table_storage.construction_failed()) {
                continue; // the load already refused this file loudly
            }

            auto it = cols_by_relid.find(oid);
            if (it == cols_by_relid.end() || it->second.empty()) {
                // The catalog knows the table (pg_class listed it as 'r'/'m') but describes no
                // live column for it. "Every column was dropped" and "this table's
                // pg_attribute rows are missing" are indistinguishable from here, and one of
                // them ends in an emptied table, so neither is acted on.
                error(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} is a live 'r'/'m' table with "
                      "{} storage column(s) but NO live pg_attribute column — refusing to read that as a "
                      "drop; nothing was released",
                      static_cast<unsigned>(oid),
                      owned->table_storage.table().column_count());
                continue;
            }

            // THE COMPARISON IS BY attoid. Every live pg_attribute row carries one;
            // a row that does not is a catalog this walk cannot reason about, and the whole
            // table is left alone rather than compared on a weaker key.
            std::set<catalog::oid_t> live_attoids;
            for (const auto& def : it->second) {
                if (def.attoid() != 0) {
                    live_attoids.insert(static_cast<catalog::oid_t>(def.attoid()));
                }
            }
            std::size_t catalog_unidentified = 0;
            for (const auto& def : it->second) {
                if (def.attoid() == 0) {
                    ++catalog_unidentified;
                }
            }
            if (catalog_unidentified != 0) {
                error(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} has {} of {} live "
                      "pg_attribute column(s) with NO attoid — refusing to reconcile a catalog whose "
                      "columns are not identified; nothing was released",
                      static_cast<unsigned>(oid),
                      catalog_unidentified,
                      it->second.size());
                continue;
            }

            // The storage's own columns come from the file's serialized schema (load_from_disk
            // reads each attoid back), so this is the durable root's answer, not the catalog's.
            //
            // attoid == 0 on a LOADED column is a loud refusal of the whole table, and it is
            // refused HERE rather than at the load. Rule 6 forbids the quiet degradations —
            // skipping the column, or falling back to the name — because both put a physical drop
            // back on a key that cannot tell a rename apart. It does not follow that the refusal
            // belongs on the READ path: aborting data_table_t::load_from_disk over an unidentified
            // column would make the database unopenable over an accounting gap. Refusing here
            // leaves every byte in place, every query answerable, and the leak re-derivable by the
            // next start once whatever produced the 0 is fixed.
            const auto& storage_columns = owned->table_storage.table().columns();
            std::vector<std::string> unidentified;
            for (const auto& column : storage_columns) {
                if (column.attoid() == 0) {
                    unidentified.push_back(column.name());
                }
            }
            if (!unidentified.empty()) {
                error(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} has {} storage column(s) "
                      "carrying NO pg_attribute.attoid (first: '{}') — refusing to reconcile a schema whose "
                      "columns are not identified; nothing was released",
                      static_cast<unsigned>(oid),
                      unidentified.size(),
                      unidentified.front());
                continue;
            }

            // One pass over the storage schema, splitting it three ways against the catalog:
            //   * attoid absent from the live set   -> a real DROP; arm the release;
            //   * attoid present, name disagrees    -> a RENAME whose storage half a crash lost
            //                                          before the table's next checkpoint. Not a
            //                                          drop, and not a no-op either: the storage
            //                                          NAME is a cache of the catalog's, and the
            //                                          append path's column expansion and
            //                                          drop_column still address columns by it;
            //   * attoid present, name agrees       -> nothing to do.
            // Catalog attoids with no storage column are the mirror image — an ALTER ADD COLUMN
            // not materialised yet — and are handled after the drops, by publishing their
            // identity forward (see below). By NAME those last two are the SAME observation,
            // which is exactly why a name-keyed comparison has no guard to add on its own side.
            struct storage_rename_t {
                std::string from;
                std::string to;
            };
            std::vector<std::string> to_drop;
            std::vector<storage_rename_t> to_rename;
            for (const auto& column : storage_columns) {
                if (live_attoids.find(static_cast<catalog::oid_t>(column.attoid())) == live_attoids.end()) {
                    to_drop.push_back(column.name());
                    continue;
                }
                for (const auto& def : it->second) {
                    if (def.attoid() == column.attoid()) {
                        if (def.name() != column.name()) {
                            to_rename.push_back(storage_rename_t{column.name(), def.name()});
                        }
                        break;
                    }
                }
            }
            if (to_drop.size() >= owned->table_storage.table().column_count() && !to_drop.empty()) {
                // Not one attoid in common. That is a schema mismatch between the file and the
                // catalog, not a DROP COLUMN — acting on it would empty the table.
                error(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} shares NO column attoid with "
                      "its {} live pg_attribute column(s) — refusing to drop all {} storage columns",
                      static_cast<unsigned>(oid),
                      it->second.size(),
                      to_drop.size());
                continue;
            }

            // The forward half: publish the identity of every live catalog column this
            // storage does not carry, so the INSERT that eventually materialises it stamps the
            // right attoid. This is an OID-SET DIFFERENCE, not a name match — the storage's
            // attoids are known and complete (refused above otherwise) — and it is what makes a
            // crash between an ALTER TABLE ADD COLUMN's commit and the table's next checkpoint
            // survivable: the pg_attribute row is durable, the parked identity is not, and this
            // re-derives it from the catalog before any statement can run.
            for (const auto& def : it->second) {
                bool in_storage = false;
                for (const auto& column : storage_columns) {
                    if (column.attoid() == def.attoid()) {
                        in_storage = true;
                        break;
                    }
                }
                if (!in_storage) {
                    // The TYPE travels with the identity: this same list is what the storage
                    // adapter reads to answer the column with NULLs until an INSERT materialises it.
                    owned->note_column_identity(def.name(), def.attoid(), def.type());
                    trace(log_,
                          "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} published identity "
                          "attoid={} for catalog-only column '{}'",
                          static_cast<unsigned>(oid),
                          static_cast<unsigned>(def.attoid()),
                          def.name());
                }
            }

            // The storage's NAME for a surviving column is repaired from the catalog before any
            // drop, while `storage_columns` is still the live schema. A collision (two columns
            // trading names) is refused by data_table_t::rename_column; it is logged and left
            // alone — the divergence is inert (identity is the oid) and the next start retries.
            for (const auto& r : to_rename) {
                auto renamed = owned->rename_column(r.from, r.to);
                if (renamed.has_error()) {
                    error(log_,
                          "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} could not repair the "
                          "storage name of column '{}' to '{}': {} — the column and its data are untouched",
                          static_cast<unsigned>(oid),
                          r.from,
                          r.to,
                          renamed.error().what);
                    continue;
                }
                trace(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} repaired the storage name "
                      "'{}' -> '{}' from the catalog (a RENAME whose storage half a crash discarded)",
                      static_cast<unsigned>(oid),
                      r.from,
                      r.to);
            }

            if (to_drop.empty()) {
                continue;
            }

            for (const auto& attname : to_drop) {
                // The commit path's primitive, unchanged: NAME the outgoing column's blocks
                // into pending_released_blocks_, then rebuild the collection without it. The
                // release itself belongs to the next checkpoint round, which is now reachable
                // again — that is the whole repair.
                if (!owned->drop_column(attname, resource())) {
                    error(log_,
                          "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} column '{}' is in the "
                          "storage schema but drop_column refused it — its blocks stay leaked",
                          static_cast<unsigned>(oid),
                          attname);
                    continue;
                }
                trace(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} re-armed the release of "
                      "column '{}' dropped before the crash",
                      static_cast<unsigned>(oid),
                      attname);
            }
        }
    }

    std::unordered_map<components::catalog::oid_t, std::vector<components::table::column_definition_t>>
    manager_disk_t::collect_catalog_columns_sync(const std::unordered_set<components::catalog::oid_t>& wanted) const {
        // One scan of pg_attribute (agents_[0] owns every pg_* table; bootstrap thread,
        // pre-scheduler-start) grouping live (non-dropped) columns by attrelid. pg_attribute
        // layout: [0=attoid, 1=attrelid, 2=attname, 3=atttypid, 4=attnum, 5=attnotnull,
        // 6=atthasdefault, 7=attisdropped, 8=atttypspec, ...]. Each column's (attnum, name,
        // type) reconstructs the storage schema in ordinal order — the same order CREATE
        // TABLE registered. NOT-NULL is deliberately not part of the storage schema here:
        // it is enforced ABOVE storage, by operator_check_constraint over the materialised
        // row, from the catalog's own attnotnull.
        struct catalog_col_t {
            std::int32_t attnum{0};
            // pg_attribute.attoid — the column's IDENTITY, and from here on the ONLY
            // thing the bootstrap reconciliation compares on. Carried into the
            // column_definition_t so that every storage this function schemas (the rehydrated
            // .otbx, the young-file overlay) is born with its columns identified.
            catalog::oid_t attoid{catalog::INVALID_OID};
            std::string name;
            components::types::complex_logical_type type;
        };
        std::unordered_map<catalog::oid_t, std::vector<catalog_col_t>> raw_by_relid;
        std::unordered_map<catalog::oid_t, std::vector<components::table::column_definition_t>> result;
        if (wanted.empty() || agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        {
            const collection_storage_entry_t* attr_entry = agents_[0]->storage_entry_sync(pg_attribute_oid);
            if (attr_entry == nullptr) {
                return result;
            }
            auto& attr_table = const_cast<collection_storage_entry_t*>(attr_entry)->table_storage.table();
            if (attr_table.column_count() < 9 || attr_table.calculate_size() == 0) {
                return result;
            }
            core::pmr::otterbrix_resource scan_resource;
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
                    catalog_col_t rc;
                    rc.attoid = chunk.is_null(0, i)
                                    ? catalog::INVALID_OID
                                    : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
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
                        auto rc_type_r = catalog::decode_type_spec(resource_, typspec);
                        if (rc_type_r.has_error()) {
                            // DB-open path: a refusal here would brick the database, so the
                            // failure is loud in the log and the column keeps an UNKNOWN
                            // placeholder that every typed read refuses per-statement.
                            // (const method — log through a local handle, log_t is a
                            // shared_ptr-backed value.)
                            auto log = log_;
                            error(log,
                                  "manager_disk_t::collect_catalog_columns_sync: relid={} column '{}' "
                                  "atttypspec is unreadable: {}",
                                  static_cast<unsigned>(relid),
                                  rc.name,
                                  rc_type_r.error().what);
                            rc.type = components::types::complex_logical_type{components::types::logical_type::UNKNOWN};
                        } else {
                            rc.type = std::move(rc_type_r.value());
                        }
                    } else {
                        const auto atttypid = chunk.is_null(3, i)
                                                  ? catalog::INVALID_OID
                                                  : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                        rc.type = components::types::complex_logical_type(catalog::oid_to_builtin_type(atttypid));
                    }
                    if (!rc.name.empty() && !rc.type.has_alias()) {
                        rc.type.set_alias(rc.name);
                    }
                    raw_by_relid[relid].push_back(std::move(rc));
                }
            }
        }
        for (auto& [relid, cols] : raw_by_relid) {
            std::sort(cols.begin(), cols.end(), [](const catalog_col_t& a, const catalog_col_t& b) {
                return a.attnum < b.attnum;
            });
            std::vector<components::table::column_definition_t> defs;
            defs.reserve(cols.size());
            for (auto& c : cols) {
                defs.emplace_back(c.name, c.type);
                defs.back().set_attoid(static_cast<std::uint32_t>(c.attoid));
            }
            result.emplace(relid, std::move(defs));
        }
        return result;
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
        auto scan_r = entry->storage->scan_batched(batches,
                                                   /*filter=*/nullptr,
                                                   /*limit=*/-1,
                                                   /*projected_cols=*/nullptr,
                                                   components::table::transaction_data{});
        if (scan_r.has_error()) {
            // Rebuild feeds the post-restart index repopulate; a partial batch
            // set would silently rebuild an index that disagrees with the table.
            // Bootstrap has no statement to fail, so log LOUDLY and hand back
            // nothing — an unindexed table costs a scan, a wrong index lies.
            auto log = log_.clone();
            error(log,
                  "manager_disk_t::scan_storage_for_rebuild_sync: scan failed for oid={}: {} — "
                  "returning no batches (index left unrebuilt)",
                  static_cast<unsigned>(table_oid),
                  scan_r.error().what);
            batches.clear();
        }
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
        core::pmr::otterbrix_resource scan_resource;
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

    core::result_wrapper_t<char> manager_disk_t::relkind_for_oid_sync(components::catalog::oid_t table_oid) const {
        // See header. Same pg_class {0=oid, 3=relkind} sparse-scan shape as
        // scan_live_table_oids_sync; the LAST matching row wins (latest append).
        //
        // THE THREE LEGS BELOW ARE NOT ONE ANSWER. "pg_class is not there to scan" is a read
        // that did not happen; "pg_class is there and holds no row for this oid" is a read
        // that did, and its '\0' is the honest one every consumer already handles. An EMPTY
        // pg_class belongs with the second — a database where nothing has been created yet
        // genuinely has no row for the oid.
        char result = '\0';
        if (agents_.empty() || agents_[0] == nullptr) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"relkind_for_oid_sync: no catalog agent to read pg_class from; "
                                                  "the relkind of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      " is unknown, not 'regular'",
                                                  resource()});
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_class_oid);
        if (entry == nullptr) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"relkind_for_oid_sync: pg_class is not loaded; the relkind of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      " is unknown, not 'regular'",
                                                  resource()});
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() < 4) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"relkind_for_oid_sync: pg_class carries only " +
                                                      std::to_string(table.column_count()) +
                                                      " columns and cannot hold a relkind; the relkind of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      " is unknown, not 'regular'",
                                                  resource()});
        }
        if (table.calculate_size() == 0) {
            return result;
        }
        core::pmr::otterbrix_resource scan_resource;
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
                if (seen != table_oid)
                    continue;
                const auto kind = chunk.get_value<std::string_view>(3, i);
                if (kind.size() == 1) {
                    result = kind.front();
                }
            }
        }
        return result;
    }

    components::catalog::oid_t manager_disk_t::relnamespace_for_oid_sync(components::catalog::oid_t table_oid) const {
        // See header. Same pg_class sparse-scan shape as relkind_for_oid_sync, projecting
        // {0=oid, 2=relnamespace}; the LAST matching row wins (latest append).
        auto result = catalog::INVALID_OID;
        if (agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_class_oid);
        if (entry == nullptr) {
            return result;
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() < 3 || table.calculate_size() == 0) {
            return result;
        }
        core::pmr::otterbrix_resource scan_resource;
        std::vector<components::table::storage_index_t> col_indices;
        col_indices.emplace_back(static_cast<int64_t>(0));
        col_indices.emplace_back(static_cast<int64_t>(2));
        components::table::table_scan_state scan_state(&scan_resource);
        table.initialize_scan(scan_state, col_indices);
        const auto& all_cols = table.columns();
        std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
        all_types.reserve(all_cols.size());
        for (const auto& c : all_cols) {
            all_types.push_back(c.type());
        }
        const std::vector<std::size_t> projected{0, 2};
        while (true) {
            components::vector::data_chunk_t chunk(&scan_resource,
                                                   all_types,
                                                   projected,
                                                   components::vector::DEFAULT_VECTOR_CAPACITY);
            table.scan(chunk, scan_state);
            if (chunk.size() == 0)
                break;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(2, i))
                    continue;
                const auto seen = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (seen != table_oid)
                    continue;
                result = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(2, i));
            }
        }
        return result;
    }

    std::pmr::vector<pg_index_row_t> manager_disk_t::scan_alive_pg_index_sync() const {
        // Two single-pass catalog sweeps on agents_[0] (pg_index, then pg_attribute
        // for indkey) instead of O(N_indexes × C) per-index rescans. No pg_class
        // pass: everything below the planner is keyed by (table_oid, indexrelid),
        // the relname never leaves the catalog. Pre-scheduler-start, single-threaded.
        std::pmr::vector<pg_index_row_t> result{resource_};
        if (agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        const collection_storage_entry_t* idx_entry = agents_[0]->storage_entry_sync(pg_index_oid);
        if (idx_entry == nullptr) {
            return result;
        }
        auto& idx_table = const_cast<collection_storage_entry_t*>(idx_entry)->table_storage.table();
        // Local clone: the error() log helpers take log_t& and this method is const.
        auto log = log_.clone();
        if (idx_table.column_count() != 5) {
            // pg_index carries exactly [indexrelid, indrelid, indkey, indisvalid,
            // indtype]. Any other shape is catalog corruption — refuse to guess
            // which backend owns each index directory (rule 6: loud, no fallback).
            // REFUSE THE START, NOT THE PROCESS: this runs pre-scheduler on the
            // bootstrap thread (base_spaces), where std::runtime_error is the same
            // catchable startup refusal bootstrap_one already throws twice. An abort
            // here would be a SIGABRT driven purely by bytes read from disk,
            // uncatchable by the embedder and unretryable once the row is repaired.
            error(log,
                  "manager_disk_t::scan_alive_pg_index_sync: pg_index has {} columns, expected 5 "
                  "(indtype missing?) — catalog is corrupt, refusing to start",
                  idx_table.column_count());
            throw std::runtime_error("pg_index has " + std::to_string(idx_table.column_count()) +
                                     " columns, expected 5 — catalog is corrupt, refusing to start");
        }
        if (idx_table.calculate_size() == 0) {
            return result;
        }

        // Pass 1: scan pg_index. The raw indkey attoid CSV is stashed per-row and
        // resolved against pg_attribute in pass 2.
        std::pmr::vector<std::pmr::string> raw_indkeys{resource_};
        {
            core::pmr::otterbrix_resource scan_resource;
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0)); // indexrelid
            col_indices.emplace_back(static_cast<int64_t>(1)); // indrelid
            col_indices.emplace_back(static_cast<int64_t>(2)); // indkey
            col_indices.emplace_back(static_cast<int64_t>(3)); // indisvalid
            col_indices.emplace_back(static_cast<int64_t>(4)); // indtype
            components::table::table_scan_state scan_state(&scan_resource);
            idx_table.initialize_scan(scan_state, col_indices);
            std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
            for (std::size_t idx : {0u, 1u, 2u, 3u, 4u}) {
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
                    // indtype: NOT nullable, no default. A missing or unknown code
                    // means the restart cannot know which backend owns the index
                    // directory — reading bitcask files through a B+tree (or vice
                    // versa) silently corrupts them, so fail LOUDLY instead.
                    if (chunk.is_null(4, i)) {
                        // Same start-refusal contract as the column-count check above:
                        // loud, catchable, retryable once the row is deleted or repaired.
                        error(log,
                              "manager_disk_t::scan_alive_pg_index_sync: pg_index row "
                              "(indexrelid={}, indrelid={}) has NULL indtype — catalog is corrupt, refusing to start",
                              static_cast<unsigned>(row.oid),
                              static_cast<unsigned>(row.table_oid));
                        throw std::runtime_error("pg_index row (indexrelid=" +
                                                 std::to_string(static_cast<unsigned>(row.oid)) +
                                                 ") has NULL indtype — catalog is corrupt, refusing to start");
                    }
                    const auto indtype_v = chunk.get_value<std::string_view>(4, i);
                    row.type = indtype_v.size() == 1
                                   ? components::logical_plan::index_type_from_indtype_code(indtype_v.front())
                                   : components::logical_plan::index_type::no_valid;
                    if (row.type == components::logical_plan::index_type::no_valid) {
                        // Same start-refusal contract as the two checks above. The refusal
                        // to GUESS the owning backend stands: reading bitcask files through
                        // a B+tree (or vice versa) silently corrupts them.
                        error(log,
                              "manager_disk_t::scan_alive_pg_index_sync: pg_index row "
                              "(indexrelid={}, indrelid={}) has unknown indtype '{}' — catalog is corrupt, "
                              "refusing to start",
                              static_cast<unsigned>(row.oid),
                              static_cast<unsigned>(row.table_oid),
                              std::string(indtype_v.data(), indtype_v.size()));
                        throw std::runtime_error("pg_index row (indexrelid=" +
                                                 std::to_string(static_cast<unsigned>(row.oid)) +
                                                 ") has unknown indtype '" +
                                                 std::string(indtype_v.data(), indtype_v.size()) +
                                                 "' — catalog is corrupt, refusing to start");
                    }
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

        // Pass 2: resolve the stashed indkey CSVs to attnames via one pg_attribute
        // scan (build attoid → attname, then walk each row's CSV).
        std::pmr::unordered_map<catalog::oid_t, std::pmr::string> attoid_to_name{resource_};
        if (const collection_storage_entry_t* attr_entry = agents_[0]->storage_entry_sync(pg_attribute_oid)) {
            auto& attr_table = const_cast<collection_storage_entry_t*>(attr_entry)->table_storage.table();
            if (attr_table.column_count() >= 3 && attr_table.calculate_size() > 0) {
                core::pmr::otterbrix_resource scan_resource;
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
        core::pmr::otterbrix_resource scan_resource;
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

    std::pmr::vector<dropped_class_row_t> manager_disk_t::scan_dropped_oids_sync() {
        // See header. Strategy: scan pg_class with COMMITTED_ROWS (includes
        // tombstones) for every user OID ever recorded, then set-difference against
        // alive_user_oids_sync (which omits permanently-deleted) to isolate the
        // "DROP committed, GC pending" OIDs.
        std::pmr::vector<dropped_class_row_t> result{resource_};
        if (agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(pg_class_oid);
        if (entry == nullptr) {
            return result;
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        // Columns 0 (oid) and 2 (relnamespace) are both read below.
        if (table.column_count() < 3 || table.calculate_size() == 0) {
            return result;
        }
        core::pmr::otterbrix_resource scan_resource;
        // Sparse, non-adjacent projection of {0 = oid, 2 = relnamespace}: the chunk carries
        // pg_class's FULL type list plus the projected absolute indices and is read by ABSOLUTE
        // index, the same shape rehydrate_missing_user_storages_sync uses. A compacted 2-type
        // chunk mis-decodes the dictionary-encoded string columns in between.
        std::vector<components::table::storage_index_t> col_indices;
        col_indices.emplace_back(static_cast<int64_t>(0)); // pg_class.oid
        col_indices.emplace_back(static_cast<int64_t>(2)); // pg_class.relnamespace

        // create_index_scan exposes table_scan_type, so it can request COMMITTED_ROWS
        // (incl. tombstones); the plain scan_committed/scan APIs are hard-wired to
        // COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED.
        std::unordered_map<components::catalog::oid_t, components::catalog::oid_t> ns_by_user_oid;
        {
            components::table::table_scan_state scan_state(&scan_resource);
            table.initialize_scan(scan_state, col_indices);
            const auto& all_cols = table.columns();
            std::pmr::vector<components::types::complex_logical_type> all_types(&scan_resource);
            all_types.reserve(all_cols.size());
            for (const auto& c : all_cols) {
                all_types.push_back(c.type());
            }
            const std::vector<std::size_t> projected{static_cast<std::size_t>(0), static_cast<std::size_t>(2)};
            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource,
                                                       all_types,
                                                       projected,
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
                    if (seen < catalog::FIRST_USER_OID) {
                        continue;
                    }
                    const auto ns = chunk.is_null(2, i)
                                        ? catalog::INVALID_OID
                                        : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(2, i));
                    // The LAST row for an oid wins (latest append), matching every other
                    // pg_class sparse scan on this path.
                    ns_by_user_oid[seen] = ns;
                }
            }
        }

        // dropped = all - alive. Sentinel delete_id = 1 — see header comment.
        const auto alive = alive_user_oids_sync();
        for (const auto& [oid, ns_oid] : ns_by_user_oid) {
            if (alive.count(oid) != 0) {
                continue;
            }
            if (ns_oid == catalog::INVALID_OID) {
                // Rule 6: the namespace is what names the directory the file sits in. Guessing
                // one would either miss the file (leaking it forever) or point the sweep at
                // somebody else's directory. Report and leave the row to an operator.
                error(log_,
                      "manager_disk_t::scan_dropped_oids_sync: tombstoned pg_class row oid={} carries no "
                      "relnamespace; cannot locate its .otbx and refusing to guess",
                      static_cast<unsigned>(oid));
                continue;
            }
            result.push_back(dropped_class_row_t{oid, ns_oid, static_cast<std::uint64_t>(1)});
        }
        return result;
    }

    std::string manager_disk_t::read_setting_sync(std::string_view name) {
        // agents_[0] (catalog agent) owns pg_settings. Pre-scheduler-start, single-threaded.
        //
        // AN EMPTY STRING MEANS EXACTLY "NO ROW WITH THAT NAME", never "pg_settings is not loaded"
        // or "pg_settings has the wrong shape" — folding those in would silently revert a stored
        // setting to the caller's built-in default whenever the read could not be performed. Those
        // two states cannot legitimately occur after bootstrap_system_tables_sync (which seeds
        // pg_settings FIRST and refuses the start when any system table does not come up), so
        // reaching them here is a sequencing bug or catalog corruption — refused with the same
        // pre-scheduler std::runtime_error the bootstrap refusals use, never folded into "absent".
        // A manager with no agents at all (disk-less construction) has no stored settings by
        // construction; its empty answer is the honest one and stays.
        const auto settings_oid = catalog::well_known_oid::pg_settings_table;
        if (agents_.empty() || agents_[0] == nullptr) {
            return {};
        }
        const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(settings_oid);
        if (entry == nullptr) {
            throw std::runtime_error("read_setting_sync: pg_settings is not loaded — called before "
                                     "bootstrap_system_tables_sync, refusing to answer 'setting absent'");
        }
        auto& table = const_cast<collection_storage_entry_t*>(entry)->table_storage.table();
        if (table.column_count() < 2) {
            throw std::runtime_error("read_setting_sync: pg_settings has " + std::to_string(table.column_count()) +
                                     " columns, expected at least 2 — catalog is corrupt, refusing to answer "
                                     "'setting absent'");
        }
        if (table.calculate_size() == 0) {
            return {};
        }
        core::pmr::otterbrix_resource scan_resource;
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
