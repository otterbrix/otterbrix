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

        // W-TORN crash-safe checkpoint per DISK table:
        //   1. copy_file(otbx → otbx.prev)         // backup (block_manager keeps writing into open inode of otbx)
        //   2. table.compact() + table.checkpoint(current_wal_id)
        //        — flush data + 1st fsync, write header + 2nd fsync
        //        — updates per-table prev_checkpoint_wal_id_ ← old, checkpoint_wal_id_ ← current_wal_id
        //   3. remove(otbx.prev)                   // backup no longer needed
        // Crash points:
        //   pre-1: only otbx, valid. Replay from checkpoint_wal_id_.
        //   1..2:  both files exist, otbx mid-write. Recovery: load_storage_disk_sync falls back to .prev.
        //   2..3:  both files, otbx is the new good checkpoint. load() prefers otbx, falls back to .prev on CRC fail.
        //   post-3: only otbx, new. Replay from new checkpoint_wal_id_.
        // Returned wal_id: min(prev_checkpoint_wal_id_) across DISK tables — the safe truncation lower bound.
        // Truncating WAL up to checkpoint_wal_id_ would lose records that .prev (still pointing at prev state)
        // would need on recovery; min(prev) keeps WAL coverage for the worst-case .prev fallback.
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        size_t disk_table_count = 0;
        // pg_catalog.* must be checkpointed BEFORE user tables (Appendix A breaking change #3
        // in the migration doc). Rationale: the catalog is the source of truth for what user
        // tables exist. If a user table is checkpointed first and we crash before pg_class is
        // flushed, recovery sees a "phantom" on-disk storage that the catalog doesn't know
        // about. system-first ordering preserves the invariant "every persisted user table is
        // also persisted in pg_class". The collection_full_name_t.database == "pg_catalog"
        // tag identifies system rows.
        std::vector<std::pair<const collection_full_name_t*, collection_storage_entry_t*>> ordered;
        ordered.reserve(storages_.size());
        for (auto& [name, entry] : storages_) {
            if (name.database == "pg_catalog") {
                ordered.emplace_back(&name, entry.get());
            }
        }
        for (auto& [name, entry] : storages_) {
            if (name.database != "pg_catalog") {
                ordered.emplace_back(&name, entry.get());
            }
        }
        for (auto& [name_ptr, entry] : ordered) {
            const auto& name = *name_ptr;
            if (entry->table_storage.mode() == storage_mode_t::DISK) {
                trace(log_, "manager_disk_t::checkpoint_all checkpointing : {}", name.to_string());

                auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
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

        for (auto& [name, entry] : storages_) {
            trace(log_, "manager_disk_t::vacuum_all cleaning : {}", name.to_string());
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
            table.compact();
        }

        trace(log_, "manager_disk_t::vacuum_all complete");
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::maybe_cleanup(execution_context_t ctx,
                                                                      uint64_t lowest_active_start_time) {
        auto it = storages_.find(ctx.name);
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

        // Cleanup if > 30% of rows are deleted
        static constexpr double gc_threshold = 0.3;
        if (static_cast<double>(deleted) / static_cast<double>(total) > gc_threshold) {
            // Skip GC if any active txn could still see the soon-to-be-removed tombstones.
            // lowest_active_start_time == TRANSACTION_ID_START means no active txn — only
            // safe in that case. Otherwise an in-flight reader might be expecting to see
            // a row our compact would drop. Mirrors the safety check in scan_committed.
            if (lowest_active_start_time < components::table::TRANSACTION_ID_START) {
                co_return;
            }
            trace(log_,
                  "manager_disk_t::maybe_cleanup: {}, deleted {}/{}, running compact",
                  ctx.name.to_string(),
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

    void manager_disk_t::create_storage_with_columns_sync(const collection_full_name_t& name,
                                                          std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_with_columns_sync , name : {}", name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
    }

    void manager_disk_t::create_storage_disk_sync(const collection_full_name_t& name,
                                                  std::vector<components::table::column_definition_t> columns,
                                                  const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::create_storage_disk_sync , name : {} , path : {}",
              name.to_string(),
              otbx_path.string());
        storages_.emplace(name,
                          std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
    }

    void manager_disk_t::load_storage_disk_sync(const collection_full_name_t& name,
                                                const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::load_storage_disk_sync , name : {} , path : {}",
              name.to_string(),
              otbx_path.string());
        // W-TORN recovery: if otbx is corrupt or missing, fall back to otbx.prev (the previous good checkpoint).
        // .prev exists only after a crash between checkpoint_all step 1 (copy) and step 3 (remove).
        auto prev_path = otbx_path;
        prev_path += ".prev";
        const bool otbx_exists = std::filesystem::exists(otbx_path);
        const bool prev_exists = std::filesystem::exists(prev_path);

        if (!otbx_exists && prev_exists) {
            // Only .prev — promote it.
            warn(log_,
                 "load_storage_disk_sync: {} missing, promoting .prev",
                 otbx_path.string());
            std::error_code ec;
            std::filesystem::rename(prev_path, otbx_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN promote .prev failed: " + ec.message());
            }
            storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
            return;
        }

        try {
            storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
        } catch (const std::exception& e) {
            // otbx is corrupt — try the .prev backup.
            warn(log_,
                 "load_storage_disk_sync: failed to load {} : {}",
                 otbx_path.string(),
                 e.what());
            if (!prev_exists) {
                // No backup — propagate.
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
            storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
            return;
        }

        // Successful load of otbx. Stale .prev (left by a crash that completed step 2 but not step 3)
        // is no longer needed — clean it up.
        if (prev_exists) {
            std::error_code ec;
            std::filesystem::remove(prev_path, ec);
        }

        // Read checkpoint_wal_id sidecar (if present) so WAL replay knows up to which
        // wal_id this storage is current. Missing sidecar → checkpoint_wal_id_=0 (treat
        // as never-checkpointed; replay from start). Read-time corruption (short read) is
        // also treated as never-checkpointed since the safe fallback is to replay all records.
        auto sidecar_path = otbx_path;
        sidecar_path += ".wal_id";
        if (std::filesystem::exists(sidecar_path)) {
            std::ifstream sidecar(sidecar_path, std::ios::binary);
            uint64_t v = 0;
            if (sidecar.read(reinterpret_cast<char*>(&v), sizeof(v)) && sidecar.gcount() == sizeof(v)) {
                auto it = storages_.find(name);
                if (it != storages_.end()) {
                    it->second->table_storage.set_checkpoint_wal_id(wal::id_t{v});
                }
            }
        }
    }

    wal::id_t manager_disk_t::peek_checkpoint_wal_id_from_disk(const collection_full_name_t& name) const noexcept {
        // Fast path: storage already loaded.
        auto it = storages_.find(name);
        if (it != storages_.end()) {
            return it->second->table_storage.checkpoint_wal_id();
        }
        // Slow path: read the .wal_id sidecar file directly.
        if (config_.path.empty() || name.database.empty() || name.collection.empty()) {
            return wal::id_t{0};
        }
        auto sidecar = config_.path / name.database / "main" / name.collection / "table.otbx.wal_id";
        std::ifstream f(sidecar, std::ios::binary);
        uint64_t v = 0;
        if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
            static_cast<std::streamsize>(sizeof(v)) == f.gcount()) {
            return wal::id_t{v};
        }
        return wal::id_t{0};
    }

    void manager_disk_t::load_storage_for_wal_replay_sync(const collection_full_name_t& name) {
        if (has_storage(name) || config_.path.empty() ||
            name.database.empty() || name.collection.empty()) {
            return;
        }
        auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
        if (!std::filesystem::exists(otbx_path)) {
            return; // in-memory table — WAL replay creates it from the first INSERT chunk
        }
        try {
            load_storage_disk_sync(name, otbx_path);
        } catch (const std::exception& e) {
            warn(log_, "load_storage_for_wal_replay_sync: failed to load {}: {}",
                 otbx_path.string(), e.what());
        }
    }

    // Shared helpers for catalog row construction. Used by bootstrap_system_tables_sync
    // and by the ddl_*_sync methods further below. Single anonymous namespace shared by both.
} // namespace services::disk
