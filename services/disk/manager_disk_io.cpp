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
        // NOTHING IS FLUSHED HERE, AND THE DECLARATION SAYS SO AT LENGTH (manager_disk.hpp):
        // durability belongs to checkpoint_all. This body is a trace so the call is at least
        // visible; it is neither a stub waiting to be filled in from this side nor a step any
        // caller may read as a barrier.
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

        // Aggregate: min over min_prev_checkpoint_wal_id, plus the #304 round tallies —
        // the return type stays wal::id_t (the WAL round's contract), so the tallies'
        // channel to the operator is the log line below: without it a round that
        // deferred EVERYTHING and a round that checkpointed everything answered with
        // the same shape, and the auto-round could not see that its floor was pinned.
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
            // The state observed as "boundaries 31/55/55/135, one truncation deleting
            // nothing": every dirty entry sat behind a gate (usually the MVCC compact
            // gate under sustained writes), its unchanged prev pinned the floor, and the
            // round honestly truncated nothing. Structurally safe — the pinned floor is
            // exactly what keeps the deferred tables' replay records alive — but it must
            // not be silent, or the WAL grows round after round with every health line
            // reporting success.
            //
            // THE MECHANISM, STATED CORRECTLY: the floor this round reports is min(prev) over
            // EVERY entry, and a deferred entry's prev does not move. So the floor cannot be
            // carried past a deferred table, whatever the other entries did — it is bounded by
            // that entry, not necessarily equal to it. `deferred > 0` is therefore the exact
            // condition for "some table is holding the floor", and the earlier
            // `deferred > 0 && rewritten == 0` was a proxy that went blind in the MIXED round —
            // one table held by a gate, another rewritten — where the floor is held just the
            // same.
            //
            // THE LEVEL IS SPLIT BECAUSE THE TWO SHAPES ARE NOT THE SAME NEWS. A round that
            // rewrote NOTHING is the state above: nothing moved, and if it repeats the WAL only
            // grows. A round that rewrote something while one entry waited is the ordinary
            // steady state of a busy database — an open streaming cursor or a live version stamp
            // defers its table EVERY round, by design — so warning on it would put a line in the
            // log on every round for the life of a cursor and teach every reader to skip it.
            // Both carry min_prev_id, which is the number truncate_before actually acts on.
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
            // The sentinel means "no entry reported a WAL floor", NOT "no entry was checkpointed".
            // EVERY entry an agent owns contributes its prev_checkpoint_wal_id to the min, in all
            // three shapes:
            //   * the ones that committed a new root this round (prev <- the id the superseded
            //     root was taken at);
            //   * the ones checkpoint_inner DEFERRED (degraded storage, open cursor, version stamps
            //     above the watermark, a failed checkpoint) with their prev UNCHANGED, so the table
            //     pins the floor at the id its still-durable root was taken at — which is what
            //     keeps the records it would need for replay out of truncate_before's reach;
            //   * the ones that were UNCHANGED and so were not rewritten. These are not deferrals —
            //     nothing is outstanding — so prev <- current advances exactly as a rewrite would
            //     have. Skipping the work must not change the arithmetic: an unchanged entry that
            //     kept a frozen prev would hold the floor at the last round that happened to write
            //     it and the WAL would stop truncating altogether.
            // So min_prev_id survives as max() only when the agents own nothing checkpointable at
            // all. Sealing then would hand truncate_before max(), i.e. delete the whole WAL —
            // report id 0 ("do not truncate") instead.
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

        // The WAL-REPLAY SYNTHESIS leg of "every storage column carries its attoid".
        //
        // base_spaces' replay synthesises a storage for a table whose .otbx was lost (the directory
        // entry of a freshly created file is not fsynced) out of the WAL chunk's COLUMN TYPES,
        // which carry a name in the alias and nothing else. Left at 0, those columns are
        // unidentified and the bootstrap reconciliation has to refuse the whole table for good.
        //
        // The catalog is final at that point — system-table records replay first and sequentially,
        // so pg_class and pg_attribute are already repopulated — which makes this exactly the
        // moment the identity IS knowable. Binding it by NAME here destroys nothing on a miss: a
        // column that finds no catalog row simply stays unidentified (the relkind='g' case, whose
        // columns are described by pg_computed_column and which the reconciliation never walks).
        // Identity is established ONCE, at materialisation; from then on the oid is what everything
        // keys on. A no-op on every other caller — bootstrap_system_tables_sync runs before
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
        // ONE `false`, TWO UNRELATED OUTCOMES, AND THE AGENT CANNOT NARROW IT. Its probe
        // returns false for an oid already in the slice, and its construction check returns
        // false for an .otbx that could not be built (agent_disk_t::bootstrap_create_disk_inner_sync).
        // Reading both as "already owns" is what turned a device that refused the very first
        // write into a trace line about a duplicate. The post-condition separates them without
        // touching the agent's contract: whatever the reason, the question the caller actually
        // has is whether the owning agent holds a storage for this oid now.
        if (agent->has_storage_sync(table_oid)) {
            trace(log_,
                  "manager_disk_t::create_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                  pool_idx_c,
                  static_cast<unsigned>(table_oid),
                  otbx_path.string());
            return core::error_t::no_error();
        }
        // A REFUSAL MAY NOT LEAVE BEHIND THE ONE THING THAT BLOCKS THE RETRY. The create opens with
        // FILE_CREATE_NEW, so a create whose very first write was refused still leaves the file the
        // OPEN made: zero bytes, no header, no root. On the next start that file takes the LOAD leg
        // and is refused as "not a database" — and with rehydrate declining to create over a file
        // that exists, a transient device error would have turned into a table nobody can ever open
        // again. Removing it destroys no evidence: it never held a byte, this call made it seconds
        // ago, and the log line below names it. A file that ALREADY EXISTED when this call started
        // is not this call's to touch — the create never opened it (FILE_CREATE_NEW fails
        // outright). This is the single place the cleanup lives, so every caller — bootstrap,
        // rehydrate, replay synthesis — gets it.
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

        // Pre-read the sidecar wal_id BEFORE constructing the SFBM so bootstrap_disk_inner_sync can
        // seed set_checkpoint_wal_id atomically on the agent thread. These filesystem-only steps
        // stay on the manager thread — pre-scheduler-start, no actor ownership.
        //
        // ABSENT AND UNREADABLE ARE DIFFERENT ANSWERS. wal::id_t{0} is this table's word for "no
        // checkpoint ever committed", and it is what seeds the storage's checkpoint floor and what
        // disarms the young-file contradiction check below. Handing it back for a sidecar that
        // EXISTS but could not be read reports a broken read as a fact about the table's history,
        // and the fact is the opposite one.
        //
        // WHAT IT IS *NOT* IS A REASON TO REFUSE THE TABLE. A short sidecar is not corruption:
        // the staged-then-published sidecar writer (agent_disk.cpp) makes the write atomic, but a file written by
        // an older build or damaged from outside can still be short — and the .otbx it sits next to
        // opens perfectly either way. Refusing the OPEN over it is a per-table refusal for a user
        // table and the END of the database for a system one (bootstrap_one throws, base_spaces.cpp
        // has no catch, and nothing in the process can repair the file). So the read reports, the
        // caller opens the table with its floor marked UNREADABLE, and the replay filter — which
        // already has a third answer for exactly this — drops that table's records instead of
        // duplicating them.
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
                // A relkind THAT COULD NOT BE READ IS NOT "REGULAR", and logging that while
                // carrying on is the same conflation one line further down. "A broken read merely
                // sends the file down the deferral leg" holds for exactly one file size: deferral
                // needs `catalog_columns empty && !is_computed` AND an .otbx of exactly BLOCK_START
                // bytes. A table that HAS been checkpointed is past that size, so the identical
                // state opens it as an ordinary row-storage table with its dynamic-schema semantics
                // gone.
                //
                // Refusing is per-table and cannot brick: this whole block is guarded by
                // `table_oid >= FIRST_USER_OID`, so no system table reaches it, and the three ways
                // relkind_for_oid_sync reports (no catalog agent, pg_class not loaded, pg_class too
                // short) are all repaired by pg_class coming up — which bootstrap_system_tables_sync
                // refuses to start without.
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

        // The sidecar wal_id is read once, up front: the agent seeds its checkpoint floor from
        // it, and the young-file contradiction check below consults it in the REFUSING
        // direction.
        //
        // A FLOOR THAT COULD NOT BE READ DOES NOT STOP THE OPEN. The .otbx is a different file
        // and it is fine; what is unknown is which WAL records it already absorbed, which is a
        // question only replay asks. So the failure is carried, loudly, as a flag on the
        // entry rather than as a refusal — see read_sidecar_wal_id above for why a refusal
        // here is the whole database for a system table.
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

        // Crash recovery is the two-slot shadow-paged root INSIDE the .otbx
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
        // backup or quarantine files a build predating shadow paging left behind — makes the on-disk
        // state ambiguous. Guessing which file is authoritative is what rule 6 forbids, and
        // silently deleting the stray would destroy the operator's evidence: refuse loudly and
        // touch nothing.
        if (auto sidecar_err = verify_otbx_sidecars(otbx_path, resource()); sidecar_err.contains_error()) {
            warn(log_, "load_storage_disk_sync: {}", sidecar_err.what.c_str());
            return sidecar_err;
        }

        // A file of exactly BLOCK_START bytes is the never-checkpointed signature (three header
        // sectors, no blocks — the witness load_existing_database itself trusts). A checkpointed
        // file never has this size: its blocks put it past BLOCK_START. Two consequences, checked
        // in the REFUSING-first order:
        //
        //   1. Contradiction: a `.wal_id` sidecar only ever exists for a table that has committed a
        //      root, so "no checkpointed content" and "a checkpoint committed at wal id N" cannot
        //      both be true. Something rebuilt or truncated the .otbx out from under its sidecar;
        //      opening it as empty would silently discard whatever that checkpoint held. The
        //      sidecar is consulted in the refusing direction ONLY — its absence proves nothing and
        //      legalises nothing (a separate file, losable on its own), which is exactly why it is
        //      not the youth witness.
        //      AN UNREADABLE SIDECAR DOES NOT ARM IT, and that is not a weakening: the check needs
        //      the sidecar to RECORD a committed checkpoint, and a file that yielded no wal id
        //      records nothing. A zero-length one next to a young .otbx is precisely the crash image
        //      of a FIRST checkpoint whose rename landed and whose data did not — a legal state, not
        //      a contradiction — so refusing over it would refuse a recoverable database, and for a
        //      system table refuse it forever.
        //      THE SIDECAR IS ALSO WRITTEN BY A ROUND THAT SKIPPED ITS REWRITE because the table was
        //      unchanged. That second kind cannot reach a young file: "unchanged" is
        //      table_storage_t::needs_checkpoint, and a young file's table was BUILT rather than
        //      loaded, so it is modified-since-checkpoint by construction.
        //   2. Defer: opening a young file as an empty table requires the catalog's schema; when
        //      none could be resolved, this walk simply ran before the catalog knows the table (the
        //      bootstrap walk precedes WAL replay). Defer rather than refuse: the post-replay walk
        //      re-visits every unloaded .otbx once the catalog is repopulated, and a file this size
        //      that is NOT a valid young database is refused loudly there by the real open.
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
        // fd frees it entirely). The close-reopen window is single-threaded, no race. A failed open
        // returns a core::error_t instead of throwing, carrying the block manager's full slot
        // diagnostics — with no external backup, that log line is the operator's only tool.
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
        // The engine owns every name that extends the table file's own (`table.otbx.*`). The
        // complete set this build writes: the `.wal_id` checkpoint sidecar and its `.tmp`
        // staging file (a crash between the tmp write and the rename legitimately leaves the
        // latter behind). Anything else in that namespace — the whole-file backup / quarantine
        // sidecars of builds predating shadow paging included — is refused, by name, without being
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

    core::result_wrapper_t<wal::id_t>
    manager_disk_t::peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                                     components::catalog::oid_t database_oid) const {
        // Probe the routed agent slice (canonical SFBM owner) first: a loaded entry already
        // carries the floor its own load read, so no file is touched. AND IT CARRIES THE
        // FAILURE TOO: a table loaded over a sidecar that yielded no wal id holds an UNKNOWN
        // floor, not 0, and reading its checkpoint_wal_id() would hand back exactly the
        // "never checkpointed" answer this function exists to stop producing.
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
        // No loaded entry: read the sidecar directly (the pre-replay bootstrap path). This second
        // leg is NOT a "fall back" — an unchecked stream whose every failure answers wal::id_t{0}
        // makes a different claim: 0 tells the replay filter this table was never checkpointed, so
        // every record it has is applied again, ON TOP of the checkpointed content the unreadable
        // sidecar was describing. Only "no sidecar exists" may answer 0.
        //
        // AND THAT RULE APPLIES TO THESE THREE TOO. A shared `return 0` would re-commit the same
        // conflation: none of them establishes that no sidecar exists, they establish that this
        // function was not given enough to go looking for one.
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
        // Pass no overlay — load_storage_disk_sync resolves a user table's columns from
        // pg_attribute itself, and by replay time the catalog rows (checkpointed or replayed
        // ahead of every user record) are in place.
        // COULD NOT READ. Swallowing this into a warn would leave replay walking on as if the
        // table simply had no file yet — synthesising a fresh storage at this very path, over a
        // file that exists and did not open, so the committed rows the .otbx already holds stop
        // being reachable with one warn line to show for it. Report; the caller stops.
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
