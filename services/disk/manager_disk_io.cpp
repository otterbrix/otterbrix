#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    void manager_disk_t::set_manager_wal_sync(actor_zeta::address_t address) {
        // Fan the WAL address into every agent so the CATALOG agent can write physical
        // WAL records for catalog DDL on its own thread. Bootstrap-only (single-threaded,
        // agents already spawned in the ctor). No-op when no agents (empty config path).
        // The manager itself keeps no copy — nothing here ever read one.
        for (auto& agent : agents_) {
            if (agent != nullptr) {
                agent->set_manager_wal_sync(address);
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

    manager_disk_t::unique_future<wal::id_t>
    manager_disk_t::checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark) {
        trace(log_,
              "manager_disk_t::checkpoint_all , session : {} , wal_id : {} , compact_watermark : {}",
              session.data(),
              current_wal_id,
              compact_watermark);

        // Fan checkpoint_inner to every agent; each returns a checkpoint_result_t carrying
        // min(prev_checkpoint_wal_id_) over its entries (max() sentinel when it owns none).
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

        // Aggregate: min over min_prev_checkpoint_wal_id, plus round tallies. The return type
        // stays wal::id_t (the WAL round's contract), so the tallies' only channel to the
        // operator is the log line below -- without it, a round that deferred everything and
        // one that checkpointed everything looked the same, and the auto-round couldn't see
        // its floor was pinned.
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        uint64_t deferred = 0;
        uint64_t rewritten = 0;
        uint64_t advanced = 0;
        for (auto& f : agent_futures) {
            auto agent_result = co_await std::move(f);
            min_prev_id = std::min(min_prev_id, agent_result.min_prev_checkpoint_wal_id);
            deferred += agent_result.deferred;
            rewritten += agent_result.rewritten;
            advanced += agent_result.advanced;
        }

        if (deferred > 0) {
            // Observed as boundaries 31/55/55/135 with one truncation deleting nothing: every
            // dirty entry sat behind a gate (usually MVCC compact), its unchanged prev pinned
            // the floor, and the round truncated nothing. Structurally safe -- the pinned floor
            // is what keeps the deferred tables' replay records alive -- but must not be silent,
            // or the WAL grows round after round while every health line reports success.
            //
            // The floor is min(prev) over every entry, and a deferred entry's prev doesn't
            // move, so `deferred > 0` is the exact condition for "something is holding the
            // floor" -- the earlier `deferred > 0 && rewritten == 0` proxy went blind in a
            // MIXED round (one table gated, another rewritten) where the floor is held just
            // the same.
            //
            // Split into two log levels because the two shapes aren't the same news: a round
            // that rewrote nothing means the WAL only grows if it repeats, while a round that
            // rewrote something with one entry waiting is the ordinary steady state of a busy
            // database (an open cursor or live version stamp defers every round by design) --
            // warning on that would spam every round for a cursor's whole life.
            const auto floor_reported = static_cast<std::uint64_t>(min_prev_id);
            if (rewritten == 0) {
                warn(log_,
                     "manager_disk_t::checkpoint_all , session : {} , the round rewrote NOTHING and deferred {} "
                     "entr{} ({} unchanged advanced) — the WAL floor stays at {} and cannot move past the "
                     "deferred tables this round",
                     session.data(),
                     deferred,
                     deferred == 1 ? "y" : "ies",
                     advanced,
                     floor_reported);
            } else {
                info(log_,
                     "manager_disk_t::checkpoint_all , session : {} , the round deferred {} entr{} and rewrote {} "
                     "({} unchanged advanced) — the WAL floor is held at {} by the deferred tables",
                     session.data(),
                     deferred,
                     deferred == 1 ? "y" : "ies",
                     rewritten,
                     advanced,
                     floor_reported);
            }
        }

        if (!agents_.empty()) {
            // The sentinel means "no entry reported a floor", not "no entry was checkpointed":
            // every entry contributes prev_checkpoint_wal_id to the min in all three shapes --
            // committed this round (prev <- the superseded root's id), DEFERRED by
            // checkpoint_inner (prev unchanged, pinning the floor at its still-durable root so
            // replay records stay reachable), or UNCHANGED (not a deferral, so prev <- current
            // advances exactly as a rewrite would -- otherwise the WAL would stop truncating
            // altogether). min_prev_id survives as max() only when the agents own nothing
            // checkpointable; sealing then would hand truncate_before max() (delete the whole
            // WAL), so report 0 ("do not truncate") instead.
            const bool wal_floor_reported = (min_prev_id != std::numeric_limits<wal::id_t>::max());

            trace(log_,
                  "manager_disk_t::checkpoint_all complete , rewritten : {} , advanced : {} , deferred : {}",
                  rewritten,
                  advanced,
                  deferred);
            if (!wal_floor_reported) {
                co_return wal::id_t{0};
            }
            co_return min_prev_id;
        }

        trace(log_, "manager_disk_t::checkpoint_all complete (no agents)");
        co_return wal::id_t{0};
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::vacuum_all(session_id_t session,
                                                                       uint64_t lowest_active_start_time) {
        trace(log_, "manager_disk_t::vacuum_all , session : {}", session.data());

        // Per-agent vacuum_inner runs the canonical cleanup_versions. It answers how many of
        // ITS storages it renumbered; this hop only sums the slices, because the set of
        // storages is partitioned across the agents and nothing here knows a slice's contents.
        std::pmr::vector<unique_future<uint64_t>> agent_futures{resource()};
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

        uint64_t renumbered = 0;
        for (auto& f : agent_futures) {
            renumbered += co_await std::move(f);
        }

        trace(log_, "manager_disk_t::vacuum_all complete , renumbered storages : {}", renumbered);
        co_return renumbered;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::maybe_cleanup_many(execution_context_t /*ctx*/,
                                       std::pmr::vector<components::catalog::oid_t> table_oids,
                                       uint64_t compact_watermark) {
        // Each table_oid routes to its owning agent's maybe_cleanup_inner so the threshold check +
        // compact (row_group rebuild) is mailbox-serialized with every same-oid access. Running it
        // manager-side via a storage_entry_sync borrow would duplicate the compact and race
        // agent-side scans. INVALID_OID entries are skipped (defensively).
        //
        // Two-phase fan-out: send every per-oid message collecting futures, then await all.
        // maybe_cleanup_inner is per-oid, so co-owned oids that hash to the same agent enqueue
        // several messages; same-target mailbox FIFO preserves their order, so awaiting is
        // completion-sync only.
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

    core::error_t manager_disk_t::create_storage_disk_sync(components::catalog::oid_t table_oid,
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
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"create_storage_disk_sync: no disk agents to own oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)),
                                                  resource()});
        }

        // The WAL-replay synthesis leg of "every storage column carries its attoid": base_spaces'
        // replay synthesises a storage for a table whose .otbx was lost, out of the WAL chunk's
        // column types, which carry only a name. Left at 0, those columns would stay unidentified
        // and the bootstrap reconciliation would refuse the whole table for good.
        //
        // The catalog is final by this point (system-table records replay first), so identity IS
        // knowable here. Binding by name destroys nothing on a miss -- a column with no catalog
        // row just stays unidentified (the relkind='g' case, described by pg_computed_column
        // instead). A no-op for every other caller: bootstrap_system_tables_sync runs before
        // pg_attribute holds anything, and rehydrate_missing_user_storages_sync already passes
        // columns stamped by collect_catalog_columns_sync (set_attoid is idempotent).
        {
            bool needs_identity = false;
            for (const auto& col : columns) {
                if (col.attoid() == 0) {
                    needs_identity = true;
                    break;
                }
            }
            if (needs_identity) {
                std::unordered_set<components::catalog::oid_t> wanted;
                wanted.insert(table_oid);
                auto cols_by_relid = collect_catalog_columns_sync(wanted);
                auto found = cols_by_relid.find(table_oid);
                if (found != cols_by_relid.end()) {
                    for (auto& col : columns) {
                        if (col.attoid() != 0) {
                            continue;
                        }
                        for (const auto& def : found->second) {
                            if (def.name() == col.name() && def.attoid() != 0) {
                                col.set_attoid(def.attoid());
                                break;
                            }
                        }
                    }
                }
            }
        }
        const std::size_t pool_idx_c = pool_idx_for_oid(table_oid, agents_.size());
        trace(log_,
              "manager_disk_t::create_storage_disk_sync: create oid={} pool_idx={} path={}",
              static_cast<unsigned>(table_oid),
              pool_idx_c,
              otbx_path.string());
        // Whether the file was there BEFORE this call, decided before the call can change it.
        // It is the only thing that separates "the stump this create just made" from "a file
        // that was already on disk", and only the first may be removed below.
        std::error_code pre_ec;
        const bool existed_before = std::filesystem::exists(otbx_path, pre_ec) && !pre_ec;

        auto& agent = agents_[pool_idx_c];
        const bool ok = agent->bootstrap_create_disk_inner_sync(table_oid, std::move(columns), otbx_path, is_computed);
        if (ok) {
            return core::error_t::no_error();
        }
        // One `false`, two unrelated causes the agent can't narrow (already-owned oid, or a
        // construction that failed to build the .otbx) -- reading both as "already owns" turned
        // a device refusing the very first write into a trace line about a duplicate. The
        // post-condition asks the real question instead: does the owning agent hold this oid now.
        if (agent->has_storage_sync(table_oid)) {
            trace(log_,
                  "manager_disk_t::create_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                  pool_idx_c,
                  static_cast<unsigned>(table_oid),
                  otbx_path.string());
            return core::error_t::no_error();
        }
        // A refusal may not leave behind the thing that blocks the retry: FILE_CREATE_NEW still
        // leaves a zero-byte, header-less stump when the first write fails. Left alone, the next
        // start takes the LOAD leg, refuses it as "not a database," and rehydrate declines to
        // create over an existing file -- a transient device error would brick the table forever.
        // Removing it destroys no evidence (it never held a byte, made seconds ago, named in the
        // log line below). A file that already existed is never touched here -- FILE_CREATE_NEW
        // would have failed outright on it.
        if (!existed_before) {
            std::error_code stump_ec;
            if (std::filesystem::exists(otbx_path, stump_ec) && !stump_ec &&
                std::filesystem::file_size(otbx_path, stump_ec) == 0 && !stump_ec) {
                std::filesystem::remove(otbx_path, stump_ec);
            }
        }
        error(log_,
              "manager_disk_t::create_storage_disk_sync: agent[{}] could not create oid {} at {} — no storage "
              "came up for it",
              pool_idx_c,
              static_cast<unsigned>(table_oid),
              otbx_path.string());
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"create_storage_disk_sync: could not create the .otbx for oid " +
                                                  std::to_string(static_cast<unsigned>(table_oid)) + " at " +
                                                  otbx_path.string(),
                                              resource()});
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

        // Pre-read the sidecar wal_id before constructing the SFBM so bootstrap_disk_inner_sync can
        // seed set_checkpoint_wal_id atomically on the agent thread. Filesystem-only, so it stays
        // on the manager thread (pre-scheduler-start, no actor ownership).
        //
        // Absent and unreadable are different answers: wal::id_t{0} means "no checkpoint ever
        // committed" and disarms the young-file contradiction check below, so handing it back for
        // a sidecar that exists but couldn't be read would report the opposite fact. It's also not
        // a reason to refuse the table -- a short sidecar isn't corruption (the write is
        // staged-then-published atomic, but an older build or outside damage can still leave one
        // short, and the .otbx opens fine either way), and refusing the open would be the whole
        // database's end for a system table (bootstrap_one throws with no catch). So the read
        // reports, the table opens with its floor marked unreadable, and the replay filter drops
        // that table's records instead of duplicating them.
        auto read_sidecar_wal_id = [&](const std::filesystem::path& base) -> core::result_wrapper_t<wal::id_t> {
            auto sidecar = base;
            sidecar += ".wal_id";
            std::error_code exists_ec;
            const bool present = std::filesystem::exists(sidecar, exists_ec);
            if (exists_ec) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"load_storage_disk_sync: cannot stat the checkpoint sidecar " +
                                                          sidecar.string() + ": " + exists_ec.message(),
                                                      resource()});
            }
            if (!present) {
                return wal::id_t{0};
            }
            std::ifstream f(sidecar, std::ios::binary);
            uint64_t v = 0;
            if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
                f.gcount() == static_cast<std::streamsize>(sizeof(v))) {
                return wal::id_t{v};
            }
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"load_storage_disk_sync: the checkpoint sidecar " + sidecar.string() +
                                     " exists but does not hold a wal id, so this table's checkpoint floor is "
                                     "unknown — it is NOT never-checkpointed. The table opens with the floor marked "
                                     "unreadable and its WAL records are not replayed; both files are left "
                                     "byte-identical, and the next committed checkpoint rewrites the sidecar.",
                                 resource()});
        };

        // Resolve the catalog schema overlay for a possibly-young file BEFORE any open.
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
            // Computed (relkind='g') tables are disk-backed like everything
            // else, but their catalog schema is legitimately EMPTY (columns are
            // adopted from appended chunks and live in pg_computed_column, not
            // pg_attribute). Resolve the relkind so a young computed .otbx opens
            // schema-less instead of being deferred/refused, and so the entry
            // keeps its dynamic-schema append semantics across restarts.
            if (catalog_columns.empty()) {
                // A relkind that couldn't be read is not "regular": carrying on that assumption
                // only defers a young file (deferral needs an exact BLOCK_START-byte .otbx); a
                // checkpointed table past that size would instead open as an ordinary row-storage
                // table with its dynamic-schema semantics silently gone.
                //
                // Refusing is per-table and can't brick: this block is guarded by
                // `table_oid >= FIRST_USER_OID` (no system table reaches it), and every way
                // relkind_for_oid_sync can fail is repaired once pg_class comes up, which
                // bootstrap_system_tables_sync refuses to start without.
                auto relkind_r = relkind_for_oid_sync(table_oid);
                if (relkind_r.has_error()) {
                    error(log_,
                          "manager_disk_t::load_storage_disk_sync: could not read the relkind of oid {}: {}",
                          static_cast<unsigned>(table_oid),
                          relkind_r.error().what.c_str());
                    return core::error_t(
                        core::error_code_t::data_corruption,
                        std::pmr::string{"load_storage_disk_sync: the relkind of oid " +
                                             std::to_string(static_cast<unsigned>(table_oid)) +
                                             " could not be read, so " + otbx_path.string() +
                                             " cannot be opened without guessing whether it is a document table; "
                                             "refusing. Nothing was modified: " + relkind_r.error().what.c_str(),
                                         resource()});
                }
                is_computed = relkind_r.value() == components::catalog::relkind::computed;
            }
        }

        // Read once, up front: the agent seeds its checkpoint floor from it, and the young-file
        // contradiction check below consults it only in the refusing direction. An unreadable
        // floor does not stop the open -- the .otbx itself is fine, only whether it already
        // absorbed some WAL records is unknown, a question only replay asks -- so the failure is
        // carried as a loud flag on the entry rather than a refusal (see read_sidecar_wal_id
        // above for why a refusal here would end a system table's database).
        auto sidecar_r = read_sidecar_wal_id(otbx_path);
        const bool sidecar_readable = !sidecar_r.has_error();
        if (!sidecar_readable) {
            error(log_,
                  "manager_disk_t::load_storage_disk_sync: oid {} comes up with an UNKNOWN checkpoint floor: {}",
                  static_cast<unsigned>(table_oid),
                  sidecar_r.error().what.c_str());
        }
        const auto sidecar_id = sidecar_readable ? sidecar_r.value() : wal::id_t{0};

        // Transfer to the agent, passing the sidecar wal_id so the SFBM picks up
        // the checkpoint floor atomically.
        // Same one-bool-two-outcomes shape as the create leg, and the same separation: the
        // agent answers false both for an oid already in its slice and for a load that failed
        // on the agent thread, and only the first is a legitimate skip. Whichever it was, the
        // question is whether the owning agent holds a storage for the oid afterwards.
        auto transfer_to_agent = [&](const std::filesystem::path& path) -> core::error_t {
            if (agents_.empty()) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"load_storage_disk_sync: no disk agents to own oid " +
                                                          std::to_string(static_cast<unsigned>(table_oid)),
                                                      resource()});
            }
            auto& agent = agents_[pool_idx];
            if (agent->bootstrap_disk_inner_sync(table_oid,
                                                 path,
                                                 sidecar_id,
                                                 sidecar_readable,
                                                 catalog_columns,
                                                 is_computed)) {
                return core::error_t::no_error();
            }
            if (agent->has_storage_sync(table_oid)) {
                // Duplicate key: bootstrap_disk_inner_sync's pre-construction probe
                // drops the incoming SFBM, so no WRITE_LOCK race occurs.
                trace(log_,
                      "manager_disk_t::load_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                      pool_idx,
                      static_cast<unsigned>(table_oid),
                      path.string());
                return core::error_t::no_error();
            }
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"load_storage_disk_sync: agent could not take ownership of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) + " from " +
                                                      path.string(),
                                                  resource()});
        };

        // Crash recovery is the two-slot shadow-paged root inside the .otbx
        // (load_existing_database's slot reconciliation); no external backup and no file-shuffle
        // recovery run here any more. Every refusal below shares one contract: the .otbx is
        // reported and left byte-identical -- no rename, no truncation, no quarantine copy, no
        // 0-byte file manufactured for a missing one.
        if (!std::filesystem::exists(otbx_path)) {
            // Callers guard existence, so arriving here means the file vanished between their
            // check and this load: refuse loudly rather than let the probe interpret rubble.
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() +
                                                      " does not exist (a DISK table's file was expected here; "
                                                      "refusing to create an empty one)",
                                                  resource()});
        }

        // A stray sidecar in the engine-owned `table.otbx.*` namespace (e.g. a pre-shadow-paging
        // backup or quarantine file) makes the on-disk state ambiguous. Guessing which file is
        // authoritative is a forbidden guess, and deleting the stray would destroy the
        // operator's evidence -- refuse loudly and touch nothing.
        if (auto sidecar_err = verify_otbx_sidecars(otbx_path, resource()); sidecar_err.contains_error()) {
            warn(log_, "load_storage_disk_sync: {}", sidecar_err.what.c_str());
            return sidecar_err;
        }

        // A file of exactly BLOCK_START bytes is the never-checkpointed signature (three header
        // sectors, no blocks); a checkpointed file's blocks always put it past that size. Two
        // consequences, refusing-first:
        //
        //   1. Contradiction: a `.wal_id` sidecar only exists for a table that committed a root,
        //      so "no checkpointed content" and "a checkpoint at wal id N" can't both be true --
        //      something rebuilt or truncated the .otbx out from under its sidecar, and opening
        //      it as empty would silently discard whatever that checkpoint held. Consulted only
        //      in the refusing direction: an unreadable or absent sidecar proves nothing (a
        //      zero-length one next to a young .otbx is the legal crash image of a first
        //      checkpoint whose rename landed but data didn't), and a sidecar written by a
        //      skip-rewrite round can't reach a young file (a young table is
        //      modified-since-checkpoint by construction).
        //   2. Defer: a young file needs the catalog's schema to open as empty; if none resolved,
        //      this walk simply ran before the catalog knows the table (bootstrap precedes WAL
        //      replay). Defer rather than refuse -- the post-replay walk revisits every unloaded
        //      .otbx once the catalog is repopulated, and refuses loudly there if the file turns
        //      out not to be a valid young database.
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

        // The DISK load ctor records open/metadata failure via construction_failed() rather than
        // throwing (bootstrap_disk_inner_sync is noexcept, reachable on the agent thread). Probed
        // on the manager thread to read that flag, then destroyed to release the WRITE_LOCK before
        // the agent reopens (per-process lock; closing this fd frees it, and the window is
        // single-threaded so there's no race). With no external backup, this error is the
        // operator's only diagnostic.
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
        return transfer_to_agent(otbx_path);
    }

    core::error_t verify_otbx_sidecars(const std::filesystem::path& otbx_path, std::pmr::memory_resource* resource) {
        // The engine owns every name extending the table file's own (`table.otbx.*`). This build
        // writes only the `.wal_id` sidecar and its `.tmp` staging file (a crash between the tmp
        // write and the rename legitimately leaves the latter behind); anything else in that
        // namespace -- a pre-shadow-paging backup or quarantine file included -- is refused by
        // name rather than renamed or deleted, since the stray is the operator's evidence of
        // which build wrote the directory (no guessing over an ambiguous state). Files
        // outside the namespace are not this engine's to police.
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

    core::result_wrapper_t<wal::id_t>
    manager_disk_t::peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                                     components::catalog::oid_t database_oid) const {
        // Probe the routed agent slice first: a loaded entry already carries the floor its own
        // load read, so no file is touched -- and it carries the failure too, since a table
        // loaded over an unreadable sidecar holds an UNKNOWN floor, not 0 (returning 0 would be
        // exactly the "never checkpointed" answer this function exists to stop producing).
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx < agents_.size() && agents_[idx] != nullptr) {
                if (const auto* entry = agents_[idx]->storage_entry_sync(table_oid); entry != nullptr) {
                    if (!entry->table_storage.checkpoint_wal_id_known()) {
                        return core::error_t(
                            core::error_code_t::data_corruption,
                            std::pmr::string{"peek_checkpoint_wal_id_from_disk: oid " +
                                                 std::to_string(static_cast<unsigned>(table_oid)) +
                                                 " was loaded over a checkpoint sidecar that holds no wal id; its "
                                                 "floor is unknown and replaying this table would re-apply records "
                                                 "the checkpointed file already holds",
                                             resource()});
                    }
                    return entry->table_storage.checkpoint_wal_id();
                }
            }
        }
        // No loaded entry: read the sidecar directly (pre-replay bootstrap path) -- not a
        // fallback, since answering wal::id_t{0} on every read failure would tell the replay
        // filter "never checkpointed" and re-apply records on top of what the checkpoint already
        // holds. Only "no sidecar exists" may answer 0, and that includes the three checks below:
        // none of them establishes that, only that this function lacks enough to look for one.
        if (table_oid == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: asked for the checkpoint floor "
                                                  "of INVALID_OID; there is no table to answer about",
                                                  resource()});
        }
        if (config_.path.empty()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: config_disk::path is empty, so "
                                                  "there is no directory to look for the sidecar of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) + " in",
                                                  resource()});
        }
        if (database_oid == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: the catalog does not name the "
                                                  "namespace of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      ", so its sidecar cannot be located — which is not the same as "
                                                      "no sidecar existing",
                                                  resource()});
        }
        auto sidecar = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                       std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx.wal_id";
        std::error_code exists_ec;
        const bool present = std::filesystem::exists(sidecar, exists_ec);
        if (exists_ec) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: cannot stat " +
                                                      sidecar.string() + ": " + exists_ec.message(),
                                                  resource()});
        }
        if (!present) {
            return wal::id_t{0};
        }
        std::ifstream f(sidecar, std::ios::binary);
        uint64_t v = 0;
        if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
            static_cast<std::streamsize>(sizeof(v)) == f.gcount()) {
            return wal::id_t{v};
        }
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string{"peek_checkpoint_wal_id_from_disk: the checkpoint sidecar " +
                                                  sidecar.string() +
                                                  " exists but does not hold a wal id; its checkpoint floor is "
                                                  "unknown and replaying this table would re-apply records the "
                                                  "checkpointed file already holds",
                                              resource()});
    }

    core::error_t manager_disk_t::load_storage_for_wal_replay_sync(components::catalog::oid_t table_oid,
                                                                   components::catalog::oid_t database_oid) {
        if (has_storage(table_oid) || config_.path.empty() || table_oid == components::catalog::INVALID_OID ||
            database_oid == components::catalog::INVALID_OID) {
            return core::error_t::no_error();
        }
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                         std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
        if (!std::filesystem::exists(otbx_path)) {
            // NOTHING TO READ, and that is a legal state here: replay legitimately runs ahead
            // of a table's first checkpoint, and the caller synthesises the storage from the
            // record's own chunk. Kept distinct from the refusal below precisely because the
            // caller's response to it is to CREATE a file at this path.
            return core::error_t::no_error();
        }
        // Pass no overlay: load_storage_disk_sync resolves a user table's columns from
        // pg_attribute itself, already in place by replay time. A load failure here must not be
        // swallowed into a warn -- that would leave replay synthesising a fresh storage over a
        // file that exists but didn't open, making the committed rows the .otbx already holds
        // unreachable. Report instead; the caller stops.
        if (auto err = load_storage_disk_sync(table_oid, database_oid, otbx_path, {}); err.contains_error()) {
            error(log_,
                  "load_storage_for_wal_replay_sync: failed to load {}: {}",
                  otbx_path.string(),
                  err.what.c_str());
            return err;
        }
        return core::error_t::no_error();
    }

    // Shared helpers for catalog row construction. Used by bootstrap_system_tables_sync
    // and by the ddl_*_sync methods further below. Single anonymous namespace shared by both.
} // namespace services::disk
