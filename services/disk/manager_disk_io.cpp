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
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), config_.path, log_, role, slot);
            agents_.emplace_back(std::move(agent));
        }
    }

    manager_disk_t::unique_future<void> manager_disk_t::flush(session_id_t session, wal::id_t wal_id) {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        co_return;
    }

    manager_disk_t::unique_future<wal::id_t>
    manager_disk_t::checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark) {
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

    manager_disk_t::unique_future<void>
    manager_disk_t::vacuum_all(session_id_t session, uint64_t lowest_active_start_time, uint64_t compact_watermark) {
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
                                                  const std::filesystem::path& otbx_path,
                                                  bool is_computed) {
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
        const bool ok = agent->bootstrap_create_disk_inner_sync(table_oid, std::move(columns), otbx_path, is_computed);
        if (!ok) {
            trace(log_,
                  "manager_disk_t::create_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                  pool_idx_c,
                  static_cast<unsigned>(table_oid),
                  otbx_path.string());
        }
    }

    core::error_t
    manager_disk_t::load_storage_disk_sync(components::catalog::oid_t table_oid,
                                           components::catalog::oid_t /*database_oid*/,
                                           const std::filesystem::path& otbx_path,
                                           std::vector<components::table::column_definition_t> catalog_columns) {
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
        // agent thread. These filesystem-only steps (sidecar scan + read) stay
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

        // A7.6: resolve the catalog schema overlay for a possibly-young file BEFORE any open.
        // System-table callers pass the builtin schema; user-table callers pass {} and the
        // columns come from pg_attribute (agents_[0], same read rehydrate uses — this runs on
        // the single-threaded bootstrap/recovery path).
        bool is_computed = false;
        if (catalog_columns.empty() && table_oid >= components::catalog::FIRST_USER_OID) {
            std::unordered_set<components::catalog::oid_t> wanted{table_oid};
            auto resolved = collect_catalog_columns_sync(wanted);
            if (auto it = resolved.find(table_oid); it != resolved.end()) {
                catalog_columns = std::move(it->second);
            }
            // B1a: computed (relkind='g') tables are disk-backed like everything
            // else, but their catalog schema is legitimately EMPTY (columns are
            // adopted from appended chunks and live in pg_computed_column, not
            // pg_attribute). Resolve the relkind so a young computed .otbx opens
            // schema-less instead of being deferred/refused, and so the entry
            // keeps its dynamic-schema append semantics across restarts.
            if (catalog_columns.empty()) {
                is_computed = relkind_for_oid_sync(table_oid) == components::catalog::relkind::computed;
            }
        }

        // The sidecar wal_id is read once, up front: the agent seeds its checkpoint floor from
        // it, and the young-file contradiction check below consults it in the REFUSING
        // direction.
        const auto sidecar_id = read_sidecar_wal_id(otbx_path);

        // Transfer to the agent, passing the sidecar wal_id so the SFBM picks up
        // the checkpoint floor atomically.
        auto transfer_to_agent = [&](const std::filesystem::path& path) -> bool {
            if (agents_.empty()) {
                return false;
            }
            auto& agent = agents_[pool_idx];
            const bool ok = agent->bootstrap_disk_inner_sync(table_oid, path, sidecar_id, catalog_columns, is_computed);
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

        // A7.5: crash recovery is the two-slot shadow-paged root INSIDE the .otbx
        // (load_existing_database's slot reconciliation); no external backup exists and no
        // file-shuffle recovery runs here any more. Refusals below share one contract: the
        // .otbx is reported and left byte-identical — no rename, no truncation, no quarantine
        // copy, and (since the load-path open carries no create flag) no 0-byte file
        // manufactured for a missing one.
        if (!std::filesystem::exists(otbx_path)) {
            // Callers guard existence, so arriving here means the file vanished between their
            // check and this load: refuse loudly rather than let the probe interpret rubble.
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() +
                                                      " does not exist (a DISK table's file was expected here; "
                                                      "refusing to create an empty one)",
                                                  resource()});
        }

        // A stray sidecar in the engine-owned `table.otbx.*` namespace — e.g. the whole-file
        // backup or quarantine files a build predating A7.5 left behind — makes the on-disk
        // state ambiguous. Guessing which file is authoritative is what rule 6 forbids, and
        // silently deleting the stray would destroy the operator's evidence: refuse loudly and
        // touch nothing.
        if (auto sidecar_err = verify_otbx_sidecars(otbx_path, resource()); sidecar_err.contains_error()) {
            warn(log_, "load_storage_disk_sync: {}", sidecar_err.what.c_str());
            return sidecar_err;
        }

        // A7.6: a file of exactly BLOCK_START bytes is the never-checkpointed signature
        // (three header sectors, no blocks — the witness load_existing_database itself
        // trusts). A checkpointed file never has this size: its blocks put it past
        // BLOCK_START. Two consequences, checked in the REFUSING-first order:
        //
        //   1. Contradiction: the `.wal_id` sidecar is written solely by a COMMITTED
        //      checkpoint, so "no checkpointed content" and "a checkpoint committed at wal
        //      id N" cannot both be true. Something rebuilt or truncated the .otbx out from
        //      under its sidecar; opening it as empty would silently discard whatever that
        //      checkpoint held. The sidecar is consulted in the refusing direction ONLY —
        //      its absence proves nothing and legalises nothing (a separate file, losable on
        //      its own), which is exactly why it is not the youth witness.
        //   2. Defer: opening a young file as an empty table requires the catalog's schema;
        //      when none could be resolved, this walk simply ran before the catalog knows
        //      the table (the bootstrap walk precedes WAL replay). Defer rather than
        //      refuse: the post-replay walk re-visits every unloaded .otbx once the catalog
        //      is repopulated, and a file this size that is NOT a valid young database is
        //      refused loudly there by the real open.
        {
            std::error_code size_ec;
            const auto file_bytes = std::filesystem::file_size(otbx_path, size_ec);
            if (!size_ec && file_bytes == components::table::storage::BLOCK_START) {
                if (sidecar_id > wal::id_t{0}) {
                    return core::error_t(
                        core::error_code_t::data_corruption,
                        std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() +
                                             " carries no checkpointed content (never-checkpointed signature, " +
                                             std::to_string(file_bytes) +
                                             " bytes), but its .wal_id sidecar records a committed checkpoint at "
                                             "wal id " +
                                             std::to_string(static_cast<uint64_t>(sidecar_id)) +
                                             ". The two cannot both be true; refusing to open the table as empty. "
                                             "Both files are left byte-identical.",
                                         resource()});
                }
                if (catalog_columns.empty() && !is_computed) {
                    trace(log_,
                          "manager_disk_t::load_storage_disk_sync: {} is never-checkpointed (size == BLOCK_START) "
                          "and the catalog does not know oid {} yet — deferring the load until after WAL replay",
                          otbx_path.string(),
                          static_cast<unsigned>(table_oid));
                    return core::error_t::no_error();
                }
            }
        }

        // The DISK load ctor records open/metadata failure in table_storage.construction_failed()
        // rather than throwing (bootstrap_disk_inner_sync is noexcept and reachable on the agent
        // thread). We probe-construct on the manager thread to read that flag, then destroy the
        // probe to release the WRITE_LOCK before the agent reopens (per-process lock: closing this
        // fd frees it entirely). The close-reopen window is single-threaded, no race. This function
        // runs on the bootstrap thread (NOT the agent message thread); a failed open returns a
        // core::error_t to the open/bootstrap caller instead of throwing, carrying the block
        // manager's full slot diagnostics — with the external backup gone, that log line is the
        // operator's only remaining tool.
        bool probe_failed = false;
        std::string probe_error;
        {
            auto probe = std::make_unique<collection_storage_entry_t>(resource(), otbx_path, catalog_columns, is_computed);
            if (probe->table_storage.construction_failed()) {
                probe_failed = true;
                probe_error = probe->table_storage.construction_error().what.c_str();
            }
            probe.reset(); // release WRITE_LOCK before agent reopens on agent thread
        }
        if (probe_failed) {
            warn(log_, "load_storage_disk_sync: failed to load {} : {}", otbx_path.string(), probe_error);
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() + " : " + probe_error, resource()});
        }
        transfer_to_agent(otbx_path);
        return core::error_t::no_error();
    }

    core::error_t verify_otbx_sidecars(const std::filesystem::path& otbx_path, std::pmr::memory_resource* resource) {
        // The engine owns every name that extends the table file's own (`table.otbx.*`). The
        // complete set this build writes: the `.wal_id` checkpoint sidecar and its `.tmp`
        // staging file (a crash between the tmp write and the rename legitimately leaves the
        // latter behind). Anything else in that namespace — the whole-file backup / quarantine
        // sidecars of builds predating A7.5 included — is refused, by name, without being
        // renamed or deleted: the stray is the operator's evidence of WHICH build wrote the
        // directory, and guessing over an ambiguous on-disk state is forbidden (rule 6).
        // Files outside the namespace (unrelated droppings) are not this engine's to police.
        const auto dir = otbx_path.parent_path();
        const auto base = otbx_path.filename().string();
        const std::string wal_id_name = base + ".wal_id";
        const std::string wal_id_tmp_name = wal_id_name + ".tmp";
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            const auto name = entry.path().filename().string();
            if (name == base || name == wal_id_name || name == wal_id_tmp_name) {
                continue;
            }
            if (name.rfind(base + ".", 0) == 0) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{"refusing to open " + otbx_path.string() + ": unexpected sidecar '" + name +
                                         "' sits next to the table file. This build (A7.5) recovers solely from "
                                         "the two-slot root inside the .otbx and writes only the .wal_id sidecar; "
                                         "a leftover backup or quarantine file from an earlier build makes the "
                                         "on-disk state ambiguous. Nothing was modified — remove or archive '" +
                                         entry.path().string() + "' and reopen.",
                                     resource});
            }
        }
        if (ec) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"verify_otbx_sidecars: cannot list " + dir.string() + ": " +
                                                      ec.message(),
                                                  resource});
        }
        return core::error_t::no_error();
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
        // A7.6: pass no overlay — load_storage_disk_sync resolves a user table's columns from
        // pg_attribute itself, and by replay time the catalog rows (checkpointed or replayed
        // ahead of every user record) are in place.
        if (auto err = load_storage_disk_sync(table_oid, database_oid, otbx_path, {}); err.contains_error()) {
            warn(log_, "load_storage_for_wal_replay_sync: failed to load {}: {}", otbx_path.string(), err.what.c_str());
        }
    }

    // Shared helpers for catalog row construction. Used by bootstrap_system_tables_sync
    // and by the ddl_*_sync methods further below. Single anonymous namespace shared by both.
} // namespace services::disk
