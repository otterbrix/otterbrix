#include "manager_disk_impl.hpp"

#include <charconv>
#include <stdexcept>

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    namespace {
        namespace wk = components::catalog::well_known_oid;

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
                // int8_type is otterbrix's 1-byte signed int ("int1"), not PostgreSQL's 8-byte "int8" (aliased below).
                {wk::boolean_type, "bool"},
                {wk::int8_type, "int1"},
                {wk::uint8_type, "uint1"},
                {wk::int16_type, "int2"},
                {wk::uint16_type, "uint2"},
                {wk::int32_type, "int4"},
                {wk::uint32_type, "uint4"},
                {wk::int64_type, "int8"},
                {wk::uint64_type, "uint8"},
                {wk::int128_type, "int16"},
                {wk::uint128_type, "uint16"},
                {wk::float32_type, "float4"},
                {wk::float64_type, "float8"},
                {wk::string_type, "string"},
                {wk::timestamp_type, "timestamp"},
                {wk::date_type, "date"},
                {wk::time_type, "time"},
                {wk::blob_type, "blob"},
                {wk::numeric_type, "numeric"},
                {wk::uuid_type, "uuid"},
                {wk::int64_type, "int8_t"},
                {wk::string_type, "text"},
                {wk::string_type, "varchar"},
                {wk::string_type, "bpchar"},
                {wk::string_type, "name"},
                {wk::blob_type, "bytea"},
                {wk::boolean_type, "boolean"},
                {wk::int8_type, "tinyint"},
                {wk::uint8_type, "utinyint"},
                {wk::int16_type, "smallint"},
                {wk::uint16_type, "usmallint"},
                {wk::int32_type, "integer"},
                {wk::int32_type, "int"},
                {wk::uint32_type, "uinteger"},
                {wk::uint32_type, "uint"},
                {wk::int64_type, "bigint"},
                {wk::uint64_type, "ubigint"},
                {wk::int128_type, "hugeint"},
                {wk::uint128_type, "uhugeint"},
                {wk::float64_type, "double"},
                {wk::float64_type, "double precision"},
                {wk::numeric_type, "decimal"},
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
        // Refuses a relative-path database under the process CWD (every binding fills config_disk::path).
        if (config_.path.empty()) {
            error(log_,
                  "manager_disk_t::bootstrap_system_tables_sync: config_disk::path is empty — there is no "
                  "directory to bootstrap pg_catalog into; refusing");
            return;
        }
        const auto sys_db_oid = catalog::well_known_oid::main_database;
        const std::filesystem::path sys_dir = config_.path / std::to_string(static_cast<unsigned>(sys_db_oid));
        std::filesystem::create_directories(sys_dir);

        auto has_builtin_seed_rows = [](catalog::oid_t tbl_oid) {
            return tbl_oid == catalog::well_known_oid::pg_settings_table || tbl_oid == pg_database_oid ||
                   tbl_oid == pg_namespace_oid_tbl || tbl_oid == pg_type_oid || tbl_oid == pg_proc_oid;
        };

        auto rows_in_sync = [&](catalog::oid_t tbl_oid) -> std::uint64_t {
            if (agents_.empty() || agents_[0] == nullptr) {
                return 0;
            }
            const collection_storage_entry_t* entry = agents_[0]->storage_entry_sync(tbl_oid);
            if (entry == nullptr) {
                return 0;
            }
            // const_cast, here and at every other site in this file: the agent thread is idle during
            // bootstrap, so mutating behind the const pointer races with nothing.
            return const_cast<collection_storage_entry_t*>(entry)->table_storage.table().calculate_size();
        };

        auto bootstrap_one = [&](const components::catalog::system_table_def_t& def) -> bool {
            const auto tbl_oid = def.relation_oid;
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
                    // Throws: pre-scheduler here makes std::runtime_error catchable by the embedder.
                    if (auto err = load_storage_disk_sync(tbl_oid, sys_db_oid, otbx, def.columns);
                        err.contains_error()) {
                        error(log_,
                              "bootstrap REFUSED , system table {} oid={} could not be opened: {}",
                              std::string(def.name),
                              static_cast<unsigned>(tbl_oid),
                              err.what.c_str());
                        throw std::runtime_error("a pg_catalog system table could not be opened, refusing to start: " +
                                                 std::string(err.what.c_str()));
                    }
                    // A crash before first-checkpoint leaves an empty file treated as fresh for the 5 builtin tables.
                    needs_seeding = has_builtin_seed_rows(tbl_oid) && rows_in_sync(tbl_oid) == 0;
                } else {
                    trace(log_,
                          "manager_disk_t::bootstrap_system_tables_sync creating disk : {} oid={}",
                          std::string(def.name),
                          static_cast<unsigned>(tbl_oid));
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
            if (agents_.empty() || agents_[0] == nullptr || !agents_[0]->has_storage_sync(tbl_oid)) {
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

        // direct_append_sync answers with the start row, not a count, so require_seeded re-checks the total afterwards.
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

        // pg_settings must bootstrap first — seeding elsewhere reads the timezone via direct_append_sync.
        if (const auto* settings_def = catalog::find_system_table(pg_settings_oid)) {
            if (bootstrap_one(*settings_def)) {
                freshly_created.insert(catalog::well_known_oid::pg_settings_table);
                auto row = make_row(resource(), settings_def->columns, [&](data_chunk_t& chunk, auto*) {
                    chunk.set_value(0, 0, std::string_view("TimeZone"));
                    // Lowercase deliberately: timezone_to_offset needs lowercase; "UTC" here would warn on every start.
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

        for (const auto& def : components::catalog::all_system_tables()) {
            if (bootstrap_one(def)) {
                freshly_created.insert(def.relation_oid);
            }
        }

        {
            const bool self_rows_missing = [&] {
                if (freshly_created.count(pg_class_oid) != 0) {
                    return true;
                }
                auto rk = relkind_for_oid_sync(catalog::well_known_oid::pg_class_table);
                return !rk.has_error() && rk.value() == '\0';
            }();
            const bool self_rows_catch_up = self_rows_missing && freshly_created.count(pg_class_oid) == 0;
            if (self_rows_missing) {
                if (const auto* cls_def = catalog::find_system_table(pg_class_oid)) {
                    const auto before = rows_in_sync(pg_class_oid);
                    std::uint64_t written = 0;
                    for (const auto& def : catalog::all_system_tables()) {
                        const char relkind_ch = def.relkind;
                        auto row = make_row(resource(), cls_def->columns, [&](data_chunk_t& chunk, auto*) {
                            chunk.set_value(0, 0, def.relation_oid);
                            chunk.set_value(1, 0, def.name);
                            chunk.set_value(2, 0, def.namespace_oid);
                            chunk.set_value(3, 0, std::string_view{&relkind_ch, 1});
                            chunk.set_value(4, 0, std::string_view{"d"}); // disk-backed
                        });
                        seed_row(pg_class_oid, cls_def->name, row);
                        ++written;
                    }
                    require_seeded(pg_class_oid, cls_def->name, before + written);
                }
                if (const auto* att_def = catalog::find_system_table(pg_attribute_oid)) {
                    const auto before = rows_in_sync(pg_attribute_oid);
                    std::uint64_t written = 0;
                    for (const auto& def : catalog::all_system_tables()) {
                        std::int32_t attnum = 0;
                        for (const auto& col : def.columns) {
                            ++attnum;
                            // Deterministic identity: a derived attoid never meets the oid generator.
                            const auto attoid = static_cast<catalog::oid_t>(def.relation_oid * 100 +
                                                                            static_cast<catalog::oid_t>(attnum));
                            const auto atttypid = catalog::builtin_type_to_oid(col.type().type());
                            const std::string typspec = catalog::encode_type_spec(col.type());
                            auto row = make_row(resource(), att_def->columns, [&](data_chunk_t& chunk, auto*) {
                                chunk.set_value(0, 0, attoid);
                                chunk.set_value(1, 0, def.relation_oid);
                                chunk.set_value(2, 0, std::string_view{col.name()});
                                chunk.set_value(3, 0, atttypid);
                                chunk.set_value(4, 0, attnum);
                                chunk.set_value(5, 0, col.is_not_null());
                                chunk.set_value(6, 0, false); // atthasdefault: system columns carry none
                                chunk.set_value(7, 0, false); // attisdropped
                                chunk.set_value(8, 0, std::string_view{typspec});
                                chunk.set_value(9, 0, std::string_view{});
                                chunk.set_value(10, 0, std::int64_t{0}); // added_at_commit_id
                                chunk.set_value(11, 0, std::int64_t{0}); // dropped_at_commit_id
                            });
                            seed_row(pg_attribute_oid, att_def->name, row);
                            ++written;
                        }
                    }
                    require_seeded(pg_attribute_oid, att_def->name, before + written);
                }
            }
            if (self_rows_catch_up) {
                for (const auto tbl_oid : {pg_class_oid, pg_attribute_oid}) {
                    const collection_storage_entry_t* entry = nullptr;
                    if (!agents_.empty() && agents_[0] != nullptr) {
                        entry = agents_[0]->storage_entry_sync(tbl_oid);
                    }
                    if (entry != nullptr) {
                        auto cp_r = const_cast<collection_storage_entry_t*>(entry)->table_storage.checkpoint();
                        if (cp_r.has_error()) {
                            warn(log_,
                                 "manager_disk bootstrap: catalog self-row checkpoint failed for oid={}",
                                 static_cast<unsigned>(tbl_oid));
                        }
                    }
                }
            }
        }

        if (freshly_created.empty() ||
            freshly_created == std::unordered_set<catalog::oid_t>{catalog::well_known_oid::pg_settings_table}) {
            if (freshly_created.count(catalog::well_known_oid::pg_settings_table)) {
                constexpr auto settings_oid = catalog::well_known_oid::pg_settings_table;
                const collection_storage_entry_t* entry = nullptr;
                if (!agents_.empty() && agents_[0] != nullptr) {
                    entry = agents_[0]->storage_entry_sync(settings_oid);
                }
                if (entry != nullptr) {
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
            const collection_storage_entry_t* entry = nullptr;
            if (!agents_.empty() && agents_[0] != nullptr) {
                entry = agents_[0]->storage_entry_sync(tbl_oid);
            }
            if (entry != nullptr) {
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
        // agents_[0] (catalog agent) owns all catalog SFBM entries; scans below run pre-scheduler, single-threaded.
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
            // UINTEGER guards pg_settings, whose id_col 0 is a name string, not an oid.
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
        if (agents_.empty() || agents_[0] == nullptr) {
            return 0;
        }

        // pg_attribute is the only system table carrying commit-id columns.
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
        // Reads via the same default-transaction_data path resolve_table takes, folding the backfill's MVCC UPDATE.
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
        if (config_.path.empty()) {
            return;
        }
        if (!std::filesystem::exists(config_.path)) {
            return;
        }
        // Layout: ${config_.path}/${database_oid}/${table_oid}/table.otbx; system tables are already loaded here.
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
                if (has_storage(tbl_oid))
                    continue;
                auto otbx = tbl_entry.path() / "table.otbx";
                if (!std::filesystem::exists(otbx))
                    continue;
                trace(log_,
                      "manager_disk_t::load_user_table_storages_sync : oid={} db_oid={}",
                      static_cast<unsigned>(tbl_oid),
                      static_cast<unsigned>(db_oid));
                // A never-checkpointed .otbx whose rows are still in the WAL is deferred here, not an error.
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
        // The four early returns below use error_t, not 0 — 0 is also what a healthy, empty database answers.
        if (agents_.empty() || agents_[0] == nullptr) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"rehydrate_missing_user_storages_sync: there is no catalog agent to "
                                                  "read pg_class from; no alive table could be examined",
                                                  resource()});
        }
        if (config_.path.empty()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"rehydrate_missing_user_storages_sync: config_disk::path is empty — "
                                                  "no directory to recreate a lost .otbx in; refusing",
                                                  resource()});
        }
        std::size_t unclosed = 0;

        // pg_class layout: [oid, relname, relnamespace, relkind, relstoragemode]; avoids misplacing the .otbx.
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
                return core::error_t(core::error_code_t::data_corruption,
                                     std::pmr::string{"rehydrate_missing_user_storages_sync: pg_class carries only " +
                                                          std::to_string(cls_table.column_count()) +
                                                          " columns and cannot be scanned for alive tables",
                                                      resource()});
            }
            if (cls_table.calculate_size() == 0) {
                return std::size_t{0};
            }
            core::pmr::otterbrix_resource scan_resource;
            // Sparse [oid,relnamespace,relkind] scan via projected_cols, read by absolute index — a compacted chunk
            // mis-decodes the dictionary-encoded relkind column (segfault).
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0));
            col_indices.emplace_back(static_cast<int64_t>(2));
            col_indices.emplace_back(static_cast<int64_t>(3));
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
                    // Only relkinds with physical row storage; views, computed tables, sequences etc. have none.
                    if (relkind != catalog::relkind::regular && relkind != catalog::relkind::materialized_view) {
                        continue;
                    }
                    if (chunk.is_null(2, i)) {
                        // The namespace names the directory the file must be recreated in; nothing else implies it.
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

        std::unordered_set<catalog::oid_t> wanted;
        wanted.reserve(need_oids.size());
        for (const auto& need : need_oids) {
            wanted.insert(need.first);
        }
        auto cols_by_relid = collect_catalog_columns_sync(wanted);

        for (const auto& [oid, ns_oid] : need_oids) {
            auto otbx = config_.path / std::to_string(static_cast<unsigned>(ns_oid)) /
                        std::to_string(static_cast<unsigned>(oid)) / "table.otbx";

            // Recreates a table whose .otbx was LOST (unfsynced dir entry after a crash) — not one PRESENT but
            // refused by the loader, which is still every byte the operator has; creating over it destroys that.
            std::error_code file_ec;
            if (std::filesystem::exists(otbx, file_ec) && !file_ec) {
                // BLOCK_START byte count is the on-disk signature of a never-checkpointed file.
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
                // A zero-column storage is worse than none, so the skip is right — but it must not be silent.
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

    // Verified no-op alone: with the rebuild removed, the durable root names the same blocks after restart
    // (several columns pack per 256 KiB block), so a column must leave via table_storage_t::drop_column before
    // its blocks are armed. Compared by attoid, never name: a RENAME's catalog half is durable at the WAL commit
    // marker, its storage half only at the table's next checkpoint.
    void manager_disk_t::rearm_dropped_column_blocks_sync() {
        if (agents_.empty() || agents_[0] == nullptr) {
            return;
        }
        auto live_oids = scan_live_table_oids_sync();
        std::unordered_set<catalog::oid_t> wanted;
        std::pmr::vector<catalog::oid_t> ordered{resource()};
        for (auto oid : live_oids) {
            // Not an error: rehydrate already recreated everything the catalog describes and whose file was lost.
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
                error(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} is a live 'r'/'m' table with "
                      "{} storage column(s) but NO live pg_attribute column — refusing to read that as a "
                      "drop; nothing was released",
                      static_cast<unsigned>(oid),
                      owned->table_storage.table().column_count());
                continue;
            }

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

            // attoid==0 on a loaded column is refused here, not at load — aborting the load would brick the database.
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
                // Sharing NO attoid with the catalog is a schema mismatch, not a DROP COLUMN.
                error(log_,
                      "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} shares NO column attoid with "
                      "its {} live pg_attribute column(s) — refusing to drop all {} storage columns",
                      static_cast<unsigned>(oid),
                      it->second.size(),
                      to_drop.size());
                continue;
            }

            for (const auto& def : it->second) {
                bool in_storage = false;
                for (const auto& column : storage_columns) {
                    if (column.attoid() == def.attoid()) {
                        in_storage = true;
                        break;
                    }
                }
                if (!in_storage) {
                    owned->note_column_identity(def.name(), def.attoid(), def.type());
                    trace(log_,
                          "manager_disk_t::rearm_dropped_column_blocks_sync: oid={} published identity "
                          "attoid={} for catalog-only column '{}'",
                          static_cast<unsigned>(oid),
                          static_cast<unsigned>(def.attoid()),
                          def.name());
                }
            }

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
        // NOT-NULL is enforced above storage, not in this scan.
        namespace att = catalog::pg_attribute_col;
        // Uses the widest ordinal read below, not a hand-written count — a literal `< 9` once left attdefspec out.
        constexpr std::uint64_t widest_read = att::attdefspec;
        struct catalog_col_t {
            std::int32_t attnum{0};
            catalog::oid_t attoid{catalog::INVALID_OID};
            std::string name;
            components::types::complex_logical_type type;
            // Decoded on resource_, not the scan's local arena, since the value outlives this function.
            std::optional<components::types::logical_value_t> default_value;
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
            if (attr_table.column_count() <= widest_read || attr_table.calculate_size() == 0) {
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
                    if (chunk.is_null(att::attrelid, i))
                        continue;
                    const auto relid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(att::attrelid, i));
                    if (wanted.find(relid) == wanted.end())
                        continue;
                    if (!chunk.is_null(att::attisdropped, i) && chunk.get_value<bool>(att::attisdropped, i))
                        continue;
                    catalog_col_t rc;
                    rc.attoid = chunk.is_null(att::attoid, i)
                                    ? catalog::INVALID_OID
                                    : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(att::attoid, i));
                    if (!chunk.is_null(att::attname, i)) {
                        auto attname_v = chunk.get_value<std::string_view>(att::attname, i);
                        rc.name.assign(attname_v.data(), attname_v.size());
                    }
                    rc.attnum = chunk.is_null(att::attnum, i) ? 0 : chunk.get_value<std::int32_t>(att::attnum, i);
                    std::string typspec;
                    if (!chunk.is_null(att::atttypspec, i)) {
                        auto typspec_v = chunk.get_value<std::string_view>(att::atttypspec, i);
                        typspec.assign(typspec_v.data(), typspec_v.size());
                    }
                    if (!typspec.empty()) {
                        auto rc_type_r = catalog::decode_type_spec(resource_, typspec);
                        if (rc_type_r.has_error()) {
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
                        const auto atttypid =
                            chunk.is_null(att::atttypid, i)
                                ? catalog::INVALID_OID
                                : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(att::atttypid, i));
                        rc.type = components::types::complex_logical_type(catalog::oid_to_builtin_type(atttypid));
                    }
                    if (!rc.name.empty() && !rc.type.has_alias()) {
                        rc.type.set_alias(rc.name);
                    }
                    // The default is part of the schema: dropping it on load reads NULL until the first INSERT.
                    if (!chunk.is_null(att::attdefspec, i)) {
                        auto defspec_v = chunk.get_value<std::string_view>(att::attdefspec, i);
                        std::string defspec(defspec_v.data(), defspec_v.size());
                        auto def_ec = catalog::decode_default_spec(resource_, rc.type, defspec, rc.default_value);
                        if (def_ec.contains_error()) {
                            auto log = log_;
                            error(log,
                                  "manager_disk_t::collect_catalog_columns_sync: relid={} column '{}' "
                                  "attdefspec is unreadable: {}",
                                  static_cast<unsigned>(relid),
                                  rc.name,
                                  def_ec.what);
                            rc.default_value.reset();
                        }
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
                defs.back().set_default_value(std::move(c.default_value));
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
        // REGULAR scan drops committed-deleted tombstones; scan_segment would let them leak back through a WHERE.
        auto scan_r = entry->storage->scan_batched(batches,
                                                   /*filter=*/nullptr,
                                                   /*limit=*/-1,
                                                   /*projected_cols=*/nullptr,
                                                   components::table::transaction_data{});
        if (scan_r.has_error()) {
            // A partial batch set would silently rebuild a disagreeing index.
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
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"relkind_for_oid_sync: pg_class carries only " + std::to_string(table.column_count()) +
                                     " columns and cannot hold a relkind; the relkind of oid " +
                                     std::to_string(static_cast<unsigned>(table_oid)) + " is unknown, not 'regular'",
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
        std::pmr::vector<pg_index_row_t> result{resource_};
        if (agents_.empty() || agents_[0] == nullptr) {
            return result;
        }
        const collection_storage_entry_t* idx_entry = agents_[0]->storage_entry_sync(pg_index_oid);
        if (idx_entry == nullptr) {
            return result;
        }
        auto& idx_table = const_cast<collection_storage_entry_t*>(idx_entry)->table_storage.table();
        auto log = log_.clone();
        if (idx_table.column_count() != 5) {
            // pg_index layout: [indexrelid, indrelid, indkey, indisvalid, indtype].
            // std::runtime_error, catchable like bootstrap_one, not an uncatchable SIGABRT.
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

        std::pmr::vector<std::pmr::string> raw_indkeys{resource_};
        {
            core::pmr::otterbrix_resource scan_resource;
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0)); // indexrelid/indrelid/indkey/indisvalid/indtype
            col_indices.emplace_back(static_cast<int64_t>(1));
            col_indices.emplace_back(static_cast<int64_t>(2));
            col_indices.emplace_back(static_cast<int64_t>(3));
            col_indices.emplace_back(static_cast<int64_t>(4));
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
                    // indtype is not nullable; an unknown code means the restart can't tell which backend owns it.
                    if (chunk.is_null(4, i)) {
                        error(log,
                              "manager_disk_t::scan_alive_pg_index_sync: pg_index row "
                              "(indexrelid={}, indrelid={}) has NULL indtype — catalog is corrupt, refusing to start",
                              static_cast<unsigned>(row.oid),
                              static_cast<unsigned>(row.table_oid));
                        throw std::runtime_error(
                            "pg_index row (indexrelid=" + std::to_string(static_cast<unsigned>(row.oid)) +
                            ") has NULL indtype — catalog is corrupt, refusing to start");
                    }
                    const auto indtype_v = chunk.get_value<std::string_view>(4, i);
                    row.type = indtype_v.size() == 1
                                   ? components::logical_plan::index_type_from_indtype_code(indtype_v.front())
                                   : components::logical_plan::index_type::no_valid;
                    if (row.type == components::logical_plan::index_type::no_valid) {
                        error(log,
                              "manager_disk_t::scan_alive_pg_index_sync: pg_index row "
                              "(indexrelid={}, indrelid={}) has unknown indtype '{}' — catalog is corrupt, "
                              "refusing to start",
                              static_cast<unsigned>(row.oid),
                              static_cast<unsigned>(row.table_oid),
                              std::string(indtype_v.data(), indtype_v.size()));
                        throw std::runtime_error(
                            "pg_index row (indexrelid=" + std::to_string(static_cast<unsigned>(row.oid)) +
                            ") has unknown indtype '" + std::string(indtype_v.data(), indtype_v.size()) +
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

        std::pmr::unordered_map<catalog::oid_t, std::pmr::string> attoid_to_name{resource_};
        if (const collection_storage_entry_t* attr_entry = agents_[0]->storage_entry_sync(pg_attribute_oid)) {
            auto& attr_table = const_cast<collection_storage_entry_t*>(attr_entry)->table_storage.table();
            if (attr_table.column_count() >= 3 && attr_table.calculate_size() > 0) {
                core::pmr::otterbrix_resource scan_resource;
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
        // Scans pg_class with COMMITTED_ROWS (includes tombstones), then set-differences against alive_user_oids_sync.
        std::pmr::vector<dropped_class_row_t> result{resource_};
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
        col_indices.emplace_back(static_cast<int64_t>(0)); // pg_class.oid, then pg_class.relnamespace
        col_indices.emplace_back(static_cast<int64_t>(2));

        // create_index_scan is used since it exposes table_scan_type; plain scan APIs omit tombstones.
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
                    ns_by_user_oid[seen] = ns;
                }
            }
        }

        // delete_id is a sentinel 1, not a real commit id.
        const auto alive = alive_user_oids_sync();
        for (const auto& [oid, ns_oid] : ns_by_user_oid) {
            if (alive.count(oid) != 0) {
                continue;
            }
            if (ns_oid == catalog::INVALID_OID) {
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
        // Empty means exactly "no row with that name" — never "not loaded" or "wrong shape", which can't occur
        // after bootstrap (which seeds pg_settings first and refuses the start otherwise).
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
        col_indices.emplace_back(static_cast<int64_t>(0)); // name column, then setting column
        col_indices.emplace_back(static_cast<int64_t>(1));
        components::table::table_scan_state scan_state(&scan_resource);
        table.initialize_scan(scan_state, col_indices);
        std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
        types.push_back(table.columns()[0].type());
        types.push_back(table.columns()[1].type());
        // pg_settings is append-only: returns the last row, so a SET TIMEZONE append supersedes the seeded default.
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
