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
        // Roles align with pool_idx_for_oid: slot 0 = CATALOG (pg_* system
        // tables); slots 1..N-1 = USER_POOL (user tables hashed by
        // oid % (N-1)).
        for (int i = 0; i < count_agents; i++) {
            const std::size_t slot = agents_.size();
            auto name_agent = "agent_disk_" + std::to_string(slot + 1);
            trace(log_, "manager_disk create_agent : {}", name_agent);
            const agent_role_t role = (slot == 0) ? agent_role_t::CATALOG : agent_role_t::USER_POOL;
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), this, config_.path, log_, role, slot);
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

        // Router fanout: send checkpoint_inner to every agent in parallel;
        // each runs the full compact + checkpoint + sidecar sequence per DISK
        // entry and returns min(prev_checkpoint_wal_id_) across its slice
        // (max() sentinel when the agent owns no DISK entry).
        std::pmr::vector<unique_future<wal::id_t>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                   &agent_disk_t::checkpoint_inner,
                                                                   session,
                                                                   current_wal_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }

        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        for (auto& f : agent_futures) {
            auto agent_min = co_await std::move(f);
            min_prev_id = std::min(min_prev_id, agent_min);
        }

        if (!agents_.empty()) {
            // IN_MEMORY-twin WAL-seal suppression. The std::min tally over
            // per-agent max() sentinels cannot distinguish "no DISK entry +
            // no IN_MEMORY twin" (safe to seal) from "no DISK entry +
            // IN_MEMORY twin present" (must NOT seal — IN_MEMORY tables
            // still need WAL replay records). The has_in_memory_inner_sync
            // probe restores that signal: a mailbox-free sync read across
            // every agent's slice (legal here because the manager body has
            // just awaited agent mailbox completion — agents are idle, and
            // the manager is the only writer to the agent slices).
            bool any_in_memory = false;
            for (const auto& agent_ptr : agents_) {
                if (agent_ptr != nullptr && agent_ptr->has_in_memory_inner_sync()) {
                    any_in_memory = true;
                    break;
                }
            }

            // Persist WAL ID only when (a) at least one agent had a real
            // DISK checkpoint to advance (sentinel max() => "no DISK entry"
            // across the pool) AND (b) no IN_MEMORY twin exists anywhere.
            const bool all_disk_checkpointed = (min_prev_id != std::numeric_limits<wal::id_t>::max());
            const bool safe_to_seal = all_disk_checkpointed && !any_in_memory;
            if (current_wal_id > 0 && safe_to_seal) {
                auto [needs_sched2, future2] =
                    actor_zeta::otterbrix::send(agent(), &agent_disk_t::fix_wal_id, wal::id_t{current_wal_id});
                if (needs_sched2) {
                    scheduler_->enqueue(agents_[0].get());
                }
                co_await std::move(future2);
            }

            trace(log_, "manager_disk_t::checkpoint_all complete");
            if (!safe_to_seal) {
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

        // Per-agent vacuum_inner is the canonical cleanup_versions + compact
        // path.
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                   &agent_disk_t::vacuum_inner,
                                                                   session,
                                                                   lowest_active_start_time);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }

        for (auto& f : agent_futures) {
            co_await std::move(f);
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

        // Pure router. The threshold check + compact run in the agent twin
        // (maybe_cleanup_inner) so the row_group rebuild is serialized by the
        // agent's mailbox with every other same-oid access. Running the compact
        // manager-side via a storage_entry_sync borrow would duplicate the
        // compact and race against agent-side scans. INVALID_OID is filtered
        // above; agent logs a no-op on not-owned / null entry.
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(ctx.table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::maybe_cleanup_inner,
                                                                   ctx.table_oid,
                                                                   lowest_active_start_time);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_await std::move(fut);
        }

        co_return;
    }

    // --- Synchronous storage creation (for init before schedulers start) ---

    void manager_disk_t::create_storage_with_columns_sync(components::catalog::oid_t table_oid,
                                                          components::catalog::oid_t /*database_oid*/,
                                                          std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_with_columns_sync , oid : {}", static_cast<unsigned>(table_oid));
        // The routed agent slice is the canonical owner. IN_MEMORY entry is
        // constructed on the agent's resource() and ownership is transferred
        // via bootstrap_inner_sync (rvalue unique_ptr move).
        if (!agents_.empty()) {
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto entry = std::make_unique<collection_storage_entry_t>(agent->resource(), std::move(columns));
            const bool ok = agent->bootstrap_inner_sync(table_oid, std::move(entry));
            if (!ok) {
                trace(log_,
                      "manager_disk_t::create_storage_with_columns_sync: agent[{}] already owned oid {}",
                      pool_idx,
                      static_cast<unsigned>(table_oid));
            }
        }
    }

    void manager_disk_t::create_storage_disk_sync(components::catalog::oid_t table_oid,
                                                  components::catalog::oid_t /*database_oid*/,
                                                  std::vector<components::table::column_definition_t> columns,
                                                  const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::create_storage_disk_sync , oid : {} , path : {}",
              static_cast<unsigned>(table_oid),
              otbx_path.string());
        // The routed agent slice is the canonical SFBM owner. The
        // single_file_block_manager_t is constructed on the agent thread via
        // bootstrap_create_disk_inner_sync; the manager never opens the
        // .otbx (would race the exclusive WRITE_LOCK).
        if (agents_.empty()) {
            return;
        }
        const std::size_t pool_idx_c = pool_idx_for_oid(table_oid, agents_.size());
        trace(log_,
              "manager_disk_t::create_storage_disk_sync: create oid={} pool_idx={} path={}",
              static_cast<unsigned>(table_oid),
              pool_idx_c,
              otbx_path.string());
        auto& agent = agents_[pool_idx_c];
        const bool ok = agent->bootstrap_create_disk_inner_sync(table_oid, std::move(columns), otbx_path);
        if (!ok) {
            trace(log_,
                  "manager_disk_t::create_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                  pool_idx_c,
                  static_cast<unsigned>(table_oid),
                  otbx_path.string());
        }
    }

    void manager_disk_t::load_storage_disk_sync(components::catalog::oid_t table_oid,
                                                components::catalog::oid_t /*database_oid*/,
                                                const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::load_storage_disk_sync , oid : {} , path : {}",
              static_cast<unsigned>(table_oid),
              otbx_path.string());

        // SFBM ownership lives on the routed agent. single_file_block_manager_t
        // holds an exclusive WRITE_LOCK on the underlying `.otbx` (posix
        // advisory lock, per-process semantics — closing either fd releases
        // it for both). Double-construct of the same OID would race the lock
        // and corrupt fsync/mmap pairing; only the agent thread opens it.
        const std::size_t pool_idx =
            agents_.empty() ? 0 : pool_idx_for_oid(table_oid, agents_.size());
        trace(log_,
              "manager_disk_t::load_storage_disk_sync: load oid={} pool_idx={} path={}",
              static_cast<unsigned>(table_oid),
              pool_idx,
              otbx_path.string());

        // Pre-read the sidecar wal_id (if present) BEFORE constructing the
        // SFBM so bootstrap_disk_inner_sync can seed table_storage
        // .set_checkpoint_wal_id atomically on the agent thread. Sidecar
        // reads and corrupt-recovery filesystem operations (.prev rename)
        // stay on the manager thread because they are pre-scheduler-start
        // filesystem-only steps with no actor ownership.
        auto read_sidecar_wal_id = [&](const std::filesystem::path& base) -> wal::id_t {
            auto sidecar = base;
            sidecar += ".wal_id";
            if (!std::filesystem::exists(sidecar)) {
                return wal::id_t{0};
            }
            std::ifstream f(sidecar, std::ios::binary);
            uint64_t v = 0;
            if (f.read(reinterpret_cast<char*>(&v), sizeof(v)) && f.gcount() == sizeof(v)) {
                return wal::id_t{v};
            }
            return wal::id_t{0};
        };

        // Transfer helper — routed agent owns the SFBM. Sidecar wal_id is
        // passed through so the SFBM picks up the checkpoint floor atomically.
        auto transfer_to_agent = [&](const std::filesystem::path& path) -> bool {
            if (agents_.empty()) {
                return false;
            }
            auto& agent = agents_[pool_idx];
            const auto sidecar_id = read_sidecar_wal_id(path);
            const bool ok = agent->bootstrap_disk_inner_sync(table_oid, path, sidecar_id);
            if (!ok) {
                // Duplicate-key (agent already owns the OID). The incoming
                // SFBM is dropped by bootstrap_disk_inner_sync's
                // pre-construction probe so no WRITE_LOCK race occurs.
                trace(log_,
                      "manager_disk_t::load_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                      pool_idx,
                      static_cast<unsigned>(table_oid),
                      path.string());
            }
            return ok;
        };

        auto prev_path = otbx_path;
        prev_path += ".prev";
        const bool otbx_exists = std::filesystem::exists(otbx_path);
        const bool prev_exists = std::filesystem::exists(prev_path);

        if (!otbx_exists && prev_exists) {
            warn(log_, "load_storage_disk_sync: {} missing, promoting .prev", otbx_path.string());
            std::error_code ec;
            std::filesystem::rename(prev_path, otbx_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN promote .prev failed: " + ec.message());
            }
            transfer_to_agent(otbx_path);
            return;
        }

        // Corrupt-recovery branch (rename .otbx → .broken, .prev → .otbx,
        // retry) needs to detect SFBM-open failure. The agent's
        // bootstrap_disk_inner_sync is noexcept but its make_unique<>
        // call CAN throw on corrupt files. To preserve corrupt-recovery
        // semantics deterministically, construct a probe entry on the
        // manager thread first (catches std::exception in try/catch), then
        // destroy it (releasing the file_handle_t — posix advisory lock is
        // per-process, so closing this fd releases the lock entirely and
        // the agent's subsequent open reacquires it cleanly). The
        // close-then-reopen window is single-threaded (pre-scheduler-start),
        // so no other thread races in.
        try {
            auto probe = std::make_unique<collection_storage_entry_t>(resource(), otbx_path);
            probe.reset(); // release WRITE_LOCK before agent reopens on agent thread
        } catch (const std::exception& e) {
            warn(log_, "load_storage_disk_sync: failed to load {} : {}", otbx_path.string(), e.what());
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
            transfer_to_agent(otbx_path);
            return;
        }
        if (prev_exists) {
            std::error_code ec;
            std::filesystem::remove(prev_path, ec);
        }
        transfer_to_agent(otbx_path);
    }

    wal::id_t manager_disk_t::peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                                               components::catalog::oid_t database_oid) const noexcept {
        // Probe the routed agent slice (canonical SFBM owner); if the agent
        // has not yet loaded the entry, fall back to reading the sidecar
        // directly (bootstrap path).
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx < agents_.size() && agents_[idx] != nullptr) {
                if (const auto* entry = agents_[idx]->storage_entry_sync(table_oid); entry != nullptr) {
                    return entry->table_storage.checkpoint_wal_id();
                }
            }
        }
        if (config_.path.empty() || table_oid == components::catalog::INVALID_OID ||
            database_oid == components::catalog::INVALID_OID) {
            return wal::id_t{0};
        }
        auto sidecar = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                       std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx.wal_id";
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
        if (has_storage(table_oid) || config_.path.empty() || table_oid == components::catalog::INVALID_OID ||
            database_oid == components::catalog::INVALID_OID) {
            return;
        }
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                         std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
        if (!std::filesystem::exists(otbx_path)) {
            return; // in-memory table — WAL replay creates it from the first INSERT chunk
        }
        try {
            load_storage_disk_sync(table_oid, database_oid, otbx_path);
        } catch (const std::exception& e) {
            warn(log_, "load_storage_for_wal_replay_sync: failed to load {}: {}", otbx_path.string(), e.what());
        }
    }

    // Shared helpers for catalog row construction. Used by bootstrap_system_tables_sync
    // and by the ddl_*_sync methods further below. Single anonymous namespace shared by both.
} // namespace services::disk
