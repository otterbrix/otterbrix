#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    void manager_disk_t::sync(address_pack pack) {
        constexpr static int manager_wal = 0;
        manager_wal_ = std::get<manager_wal>(pack);
    }

    void manager_disk_t::create_agent(int count_agents) {
        for (int i = 0; i < count_agents; i++) {
            auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
            trace(log_, "manager_disk create_agent : {}", name_agent);
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), this, config_.path, log_);
            agents_.emplace_back(std::move(agent));
        }
    }

    manager_disk_t::unique_future<void> manager_disk_t::flush(session_id_t session, wal::id_t wal_id) {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        co_return;
    }

    manager_disk_t::unique_future<wal::id_t> manager_disk_t::checkpoint_all(session_id_t session,
                                                                            wal::id_t current_wal_id) {
        trace(log_, "manager_disk_t::checkpoint_all , session : {} , wal_id : {}", session.data(), current_wal_id);

        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        size_t disk_table_count = 0;
        // System tables (well-known OIDs) must be checkpointed BEFORE user tables.
        std::vector<std::pair<components::catalog::oid_t, collection_storage_entry_t*>> ordered;
        ordered.reserve(storages_.size());
        // System tables (well-known OIDs < FIRST_USER_OID).
        for (auto& [oid, entry] : storages_) {
            if (oid < components::catalog::FIRST_USER_OID) {
                ordered.emplace_back(oid, entry.get());
            }
        }
        // User tables.
        for (auto& [oid, entry] : storages_) {
            if (oid >= components::catalog::FIRST_USER_OID) {
                ordered.emplace_back(oid, entry.get());
            }
        }
        // Use the actual on-disk path captured at storage-creation time
        // (entry->otbx_path). User-table sidecars must land under their own
        // database_oid directory so WAL replay's sidecar filter can skip
        // already-checkpointed records on restart.
        for (auto& [tbl_oid, entry] : ordered) {
            if (entry->table_storage.mode() == storage_mode_t::DISK) {
                if (entry->otbx_path.empty()) {
                    // DISK-mode entry with no stored path — created via a legacy
                    // path or test fixture. Skip — we have nowhere reliable to put
                    // the sidecar.
                    continue;
                }
                trace(log_, "manager_disk_t::checkpoint_all checkpointing : oid={}",
                      static_cast<unsigned>(tbl_oid));

                const auto& otbx_path = entry->otbx_path;
                auto prev_path = otbx_path;
                prev_path += ".prev";

                // Backup current checkpoint before overwriting (file stays open for block_manager)
                std::error_code copy_error;
                if (std::filesystem::exists(otbx_path)) {
                    std::filesystem::copy_file(otbx_path, prev_path,
                                               std::filesystem::copy_options::overwrite_existing,
                                               copy_error);
                    if (copy_error) {
                        warn(log_, "manager_disk_t::checkpoint_all , failed to copy {} to {} : {}",
                             otbx_path.string(), prev_path.string(), copy_error.message());
                    }
                }

                // Write new checkpoint (2 fsync inside checkpoint(wal_id))
                entry->table_storage.table().compact();
                entry->table_storage.checkpoint(current_wal_id);

                // Persist checkpoint_wal_id to sidecar so WAL replay on next startup
                // can filter records by wal_id > checkpoint_wal_id deterministically
                // (replaces the row-count heuristic in base_spaces). Sidecar is written
                // after the .otbx fsync — if the .otbx is the new good state but the
                // sidecar write crashes, replay will conservatively replay all records
                // (idempotent on disk-backed tables since rows are addressed by row_id).
                {
                    auto sidecar_path = otbx_path;
                    sidecar_path += ".wal_id";
                    auto tmp_path = sidecar_path;
                    tmp_path += ".tmp";
                    std::ofstream sidecar(tmp_path, std::ios::binary | std::ios::trunc);
                    if (sidecar.is_open()) {
                        auto v = static_cast<uint64_t>(current_wal_id);
                        sidecar.write(reinterpret_cast<const char*>(&v), sizeof(v));
                        sidecar.close();
                        std::error_code rename_error;
                        std::filesystem::rename(tmp_path, sidecar_path, rename_error);
                        if (rename_error) {
                            warn(log_, "manager_disk_t::checkpoint_all sidecar rename failed: {}", rename_error.message());
                        }
                    }
                }

                // Delete backup after successful checkpoint
                if (std::filesystem::exists(prev_path)) {
                    std::error_code remove_error;
                    std::filesystem::remove(prev_path, remove_error);
                }

                ++disk_table_count;
                // Tally min(prev_checkpoint_wal_id_) across DISK tables for safe WAL truncation.
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
            }

        }

        if (!agents_.empty()) {
            // Persist WAL ID only if all tables are DISK mode.
            // If any IN_MEMORY tables exist, WAL records are still needed for replay.
            bool has_in_memory = false;
            for (const auto& [name, entry] : storages_) {
                if (entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
                    has_in_memory = true;
                    break;
                }
            }
            if (current_wal_id > 0 && !has_in_memory) {
                auto [needs_sched2, future2] =
                    actor_zeta::otterbrix::send(agent(), &agent_disk_t::fix_wal_id, wal::id_t{current_wal_id});
                if (needs_sched2) {
                    scheduler_->enqueue(agents_[0].get());
                }
                co_await std::move(future2);
            }

            trace(log_, "manager_disk_t::checkpoint_all complete");
            // W-TORN: return min(prev_checkpoint_wal_id_) across DISK tables, used as truncate_before lower bound.
            // 0 if any IN_MEMORY table exists (their WAL records are still needed for replay) or if no DISK tables.
            if (has_in_memory || disk_table_count == 0) {
                co_return wal::id_t{0};
            }
            co_return min_prev_id;
        }

        trace(log_, "manager_disk_t::checkpoint_all complete (no agents)");
        co_return wal::id_t{0};
    }

    manager_disk_t::unique_future<void> manager_disk_t::vacuum_all(session_id_t session,
                                                                   uint64_t lowest_active_start_time) {
        trace(log_, "manager_disk_t::vacuum_all , session : {}", session.data());

        for (auto& [oid, entry] : storages_) {
            trace(log_, "manager_disk_t::vacuum_all cleaning : oid={}", static_cast<unsigned>(oid));
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
            table.compact();
        }

        trace(log_, "manager_disk_t::vacuum_all complete");
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::maybe_cleanup(execution_context_t ctx,
                                                                      uint64_t lowest_active_start_time) {
        // ctx.table_oid identifies the table whose GC threshold the executor
        // wants to check (typically the just-deleted DML target). INVALID_OID
        // -> no-op (executor guards against this but be defensive).
        if (ctx.table_oid == components::catalog::INVALID_OID) {
            co_return;
        }
        auto it = storages_.find(ctx.table_oid);
        if (it == storages_.end()) {
            co_return;
        }

        auto& table = it->second->table_storage.table();
        auto rg = table.row_group();
        auto total = rg->total_rows();
        if (total == 0) {
            co_return;
        }

        auto committed = rg->committed_row_count();
        auto deleted = total - committed;

        static constexpr double gc_threshold = 0.3;
        if (static_cast<double>(deleted) / static_cast<double>(total) > gc_threshold) {
            if (lowest_active_start_time < components::table::TRANSACTION_ID_START) {
                co_return;
            }
            trace(log_,
                  "manager_disk_t::maybe_cleanup: oid={}, deleted {}/{}, running compact",
                  static_cast<unsigned>(ctx.table_oid),
                  deleted,
                  total);
            // Compact reads via scan_committed(COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) which
            // depends on intact version metadata to filter tombstones. Calling cleanup_versions
            // before compact strips that metadata and makes scan return 0 rows — the bug
            // documented previously. Compact alone is correct: it rebuilds the row_group from
            // currently-visible committed rows and finalizes them as committed-at-0.
            // cleanup_versions afterwards is unnecessary because the new collection's rows
            // are all txn{0,0} (no version chain to clean).
            table.compact();
        }

        co_return;
    }

    // --- Synchronous storage creation (for init before schedulers start) ---

    void manager_disk_t::create_storage_with_columns_sync(components::catalog::oid_t table_oid,
                                                          components::catalog::oid_t /*database_oid*/,
                                                          std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_with_columns_sync , oid : {}",
              static_cast<unsigned>(table_oid));
        storages_.emplace(table_oid,
                           std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
    }

    void manager_disk_t::create_storage_disk_sync(components::catalog::oid_t table_oid,
                                                  components::catalog::oid_t /*database_oid*/,
                                                  std::vector<components::table::column_definition_t> columns,
                                                  const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::create_storage_disk_sync , oid : {} , path : {}",
              static_cast<unsigned>(table_oid),
              otbx_path.string());
        storages_.emplace(table_oid,
                           std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
    }

    void manager_disk_t::load_storage_disk_sync(components::catalog::oid_t table_oid,
                                                 components::catalog::oid_t /*database_oid*/,
                                                 const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::load_storage_disk_sync , oid : {} , path : {}",
              static_cast<unsigned>(table_oid),
              otbx_path.string());
        auto prev_path = otbx_path;
        prev_path += ".prev";
        const bool otbx_exists = std::filesystem::exists(otbx_path);
        const bool prev_exists = std::filesystem::exists(prev_path);

        if (!otbx_exists && prev_exists) {
            warn(log_,
                 "load_storage_disk_sync: {} missing, promoting .prev",
                 otbx_path.string());
            std::error_code ec;
            std::filesystem::rename(prev_path, otbx_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN promote .prev failed: " + ec.message());
            }
            storages_.emplace(table_oid, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
            return;
        }

        try {
            storages_.emplace(table_oid, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
        } catch (const std::exception& e) {
            warn(log_,
                 "load_storage_disk_sync: failed to load {} : {}",
                 otbx_path.string(),
                 e.what());
            if (!prev_exists) {
                throw;
            }
            auto broken_path = otbx_path;
            broken_path += ".broken";
            std::error_code ec;
            std::filesystem::rename(otbx_path, broken_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN move corrupt otbx aside failed: " + ec.message());
            }
            std::filesystem::rename(prev_path, otbx_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN promote .prev failed after corrupt otbx: " + ec.message());
            }
            warn(log_,
                 "load_storage_disk_sync: recovered {} from .prev (corrupt original kept as .broken)",
                 otbx_path.string());
            storages_.emplace(table_oid, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
            return;
        }

        if (prev_exists) {
            std::error_code ec;
            std::filesystem::remove(prev_path, ec);
        }

        auto sidecar_path = otbx_path;
        sidecar_path += ".wal_id";
        if (std::filesystem::exists(sidecar_path)) {
            std::ifstream sidecar(sidecar_path, std::ios::binary);
            uint64_t v = 0;
            if (sidecar.read(reinterpret_cast<char*>(&v), sizeof(v)) && sidecar.gcount() == sizeof(v)) {
                auto it = storages_.find(table_oid);
                if (it != storages_.end()) {
                    it->second->table_storage.set_checkpoint_wal_id(wal::id_t{v});
                }
            }
        }
    }

    wal::id_t manager_disk_t::peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                                                components::catalog::oid_t database_oid) const noexcept {
        auto it = storages_.find(table_oid);
        if (it != storages_.end()) {
            return it->second->table_storage.checkpoint_wal_id();
        }
        if (config_.path.empty() || table_oid == components::catalog::INVALID_OID
            || database_oid == components::catalog::INVALID_OID) {
            return wal::id_t{0};
        }
        auto sidecar = config_.path / std::to_string(static_cast<unsigned>(database_oid))
                                    / std::to_string(static_cast<unsigned>(table_oid))
                                    / "table.otbx.wal_id";
        std::ifstream f(sidecar, std::ios::binary);
        uint64_t v = 0;
        if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
            static_cast<std::streamsize>(sizeof(v)) == f.gcount()) {
            return wal::id_t{v};
        }
        return wal::id_t{0};
    }

    void manager_disk_t::load_storage_for_wal_replay_sync(components::catalog::oid_t table_oid,
                                                           components::catalog::oid_t database_oid) {
        if (has_storage(table_oid) || config_.path.empty() ||
            table_oid == components::catalog::INVALID_OID ||
            database_oid == components::catalog::INVALID_OID) {
            return;
        }
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid))
                                      / std::to_string(static_cast<unsigned>(table_oid))
                                      / "table.otbx";
        if (!std::filesystem::exists(otbx_path)) {
            return; // in-memory table — WAL replay creates it from the first INSERT chunk
        }
        try {
            load_storage_disk_sync(table_oid, database_oid, otbx_path);
        } catch (const std::exception& e) {
            warn(log_, "load_storage_for_wal_replay_sync: failed to load {}: {}",
                 otbx_path.string(), e.what());
        }
    }

    // Shared helpers for catalog row construction. Used by bootstrap_system_tables_sync
    // and by the ddl_*_sync methods further below. Single anonymous namespace shared by both.
} // namespace services::disk
