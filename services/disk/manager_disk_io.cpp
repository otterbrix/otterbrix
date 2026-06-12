#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    void manager_disk_t::sync(disk_sync_pack_t pack) {
        manager_wal_ = pack.wal;
        // Fan the WAL address into every agent so the CATALOG agent can write physical
        // WAL records for catalog DDL on its own thread. Bootstrap-only (single-threaded,
        // agents already spawned in the ctor). No-op when no agents (empty config path).
        for (auto& agent : agents_) {
            if (agent != nullptr) {
                agent->set_manager_wal_sync(pack.wal);
            }
        }
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
                                                                            wal::id_t current_wal_id,
                                                                            uint64_t compact_watermark) {
        trace(log_,
              "manager_disk_t::checkpoint_all , session : {} , wal_id : {} , compact_watermark : {}",
              session.data(),
              current_wal_id,
              compact_watermark);

        // Fan checkpoint_inner to every agent; each returns a checkpoint_result_t with
        // min(prev_checkpoint_wal_id_) over its DISK entries (max() sentinel when it owns
        // none) AND a has_in_memory flag — folding the former post-await
        // has_in_memory_inner_sync read into the fan-out so no synchronous cross-actor
        // slice read remains.
        std::pmr::vector<unique_future<checkpoint_result_t>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::checkpoint_inner,
                                                                  session,
                                                                  current_wal_id,
                                                                  uint64_t{compact_watermark});
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }

        // Aggregate: min over min_prev_checkpoint_wal_id AND OR over has_in_memory.
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        bool any_in_memory = false;
        for (auto& f : agent_futures) {
            auto agent_result = co_await std::move(f);
            min_prev_id = std::min(min_prev_id, agent_result.min_prev_checkpoint_wal_id);
            any_in_memory = any_in_memory || agent_result.has_in_memory;
        }

        if (!agents_.empty()) {
            // IN_MEMORY-twin WAL-seal suppression. The min() tally can't tell "no
            // DISK entry + no IN_MEMORY twin" (safe to seal) from "no DISK entry +
            // IN_MEMORY twin" (must NOT seal — those tables still need replay
            // records). any_in_memory comes from the checkpoint_inner fan-out above,
            // so no synchronous slice read is needed here.

            // Seal only when some agent actually checkpointed a DISK entry (min_prev_id
            // still max() => none did) AND no IN_MEMORY twin exists anywhere.
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
                                                                   uint64_t lowest_active_start_time,
                                                                   uint64_t compact_watermark) {
        trace(log_, "manager_disk_t::vacuum_all , session : {}", session.data());

        // Per-agent vacuum_inner runs the canonical cleanup_versions + compact.
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::vacuum_inner,
                                                                  session,
                                                                  lowest_active_start_time,
                                                                  uint64_t{compact_watermark});
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

    manager_disk_t::unique_future<void>
    manager_disk_t::maybe_cleanup_many(execution_context_t /*ctx*/,
                                       std::pmr::vector<components::catalog::oid_t> table_oids,
                                       uint64_t compact_watermark) {
        // Each table_oid routes to its owning agent's maybe_cleanup_inner so the
        // threshold check + compact (row_group rebuild) is mailbox-serialized with
        // every same-oid access. Running it manager-side via a storage_entry_sync
        // borrow would duplicate the compact and race agent-side scans. INVALID_OID
        // entries are skipped (callers guard against them but be defensive).
        //
        // Two-phase fan-out: send every per-oid message collecting futures, then
        // await all. maybe_cleanup_inner is per-oid, so co-owned oids that hash to
        // the same agent enqueue several messages; same-target mailbox FIFO
        // preserves their order, so awaiting is completion-sync only.
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(table_oids.size());
        for (const auto table_oid : table_oids) {
            if (table_oid == components::catalog::INVALID_OID) {
                continue;
            }
            if (agents_.empty()) {
                break;
            }
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::maybe_cleanup_inner,
                                                                  table_oid,
                                                                  uint64_t{compact_watermark});
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }

        co_return;
    }

    // --- Synchronous storage creation (for init before schedulers start) ---

    void manager_disk_t::create_storage_with_columns_sync(components::catalog::oid_t table_oid,
                                                          components::catalog::oid_t /*database_oid*/,
                                                          std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_with_columns_sync , oid : {}", static_cast<unsigned>(table_oid));
        // IN_MEMORY entry is constructed on the agent's resource() and ownership
        // transferred via bootstrap_inner_sync (rvalue unique_ptr move).
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
        // SFBM is constructed on the agent thread via bootstrap_create_disk_inner_sync;
        // the manager never opens .otbx (would race the exclusive WRITE_LOCK).
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

        // The SFBM holds an exclusive posix WRITE_LOCK on the .otbx (per-process:
        // closing either fd releases it for both). Double-constructing the same OID
        // would race the lock and corrupt fsync/mmap pairing, so only the agent
        // thread opens it.
        const std::size_t pool_idx = agents_.empty() ? 0 : pool_idx_for_oid(table_oid, agents_.size());
        trace(log_,
              "manager_disk_t::load_storage_disk_sync: load oid={} pool_idx={} path={}",
              static_cast<unsigned>(table_oid),
              pool_idx,
              otbx_path.string());

        // Pre-read the sidecar wal_id BEFORE constructing the SFBM so
        // bootstrap_disk_inner_sync can seed set_checkpoint_wal_id atomically on the
        // agent thread. These filesystem-only steps (sidecar read, .prev rename) stay
        // on the manager thread — pre-scheduler-start, no actor ownership.
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

        // Transfer to the agent, passing the sidecar wal_id so the SFBM picks up
        // the checkpoint floor atomically.
        auto transfer_to_agent = [&](const std::filesystem::path& path) -> bool {
            if (agents_.empty()) {
                return false;
            }
            auto& agent = agents_[pool_idx];
            const auto sidecar_id = read_sidecar_wal_id(path);
            const bool ok = agent->bootstrap_disk_inner_sync(table_oid, path, sidecar_id);
            if (!ok) {
                // Duplicate key: bootstrap_disk_inner_sync's pre-construction probe
                // drops the incoming SFBM, so no WRITE_LOCK race occurs.
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

        // Corrupt-recovery (rename .otbx → .broken, .prev → .otbx, retry) must detect
        // SFBM-open failure, but bootstrap_disk_inner_sync is noexcept (its
        // make_unique<> swallows the throw on corrupt files). So we probe-construct on
        // the manager thread to catch the exception, then destroy the probe to release
        // the WRITE_LOCK before the agent reopens (per-process lock: closing this fd
        // frees it entirely). The close-reopen window is single-threaded, no race.
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
