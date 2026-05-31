#pragma once

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/log/log.hpp>
#include <core/executor.hpp>
#include <components/table/data_table.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/timezones.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <components/context/pg_catalog_swap.hpp>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <unordered_map>

namespace services::disk {

    using path_t = std::filesystem::path;
    using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;

    class manager_disk_t;
    using name_t = std::string;
    using session_id_t = ::components::session::session_id_t;

    class base_manager_disk_t;

    // Version B* Step 2: collection_storage_entry_t is defined at namespace
    // scope in manager_disk.hpp (after table_storage_t). agent_disk_t owns a
    // forward-declared slice map; all operations on the entries live in
    // agent_disk.cpp which includes manager_disk.hpp for the full definition.
    // The user-provided destructor in agent_disk.cpp means the unordered_map
    // template body is only instantiated inside the .cpp — incomplete value
    // type in the header is therefore safe.
    struct collection_storage_entry_t;

    // Version B* Step 7: dropped_storage_entry_t promoted to namespace scope
    // in manager_disk.hpp so agent_disk_t can own a std::pmr::vector slice.
    // Same forward-decl-in-header / full-definition-via-cpp-include pattern as
    // collection_storage_entry_t above — the user-provided destructor in
    // agent_disk.cpp defers std::pmr::vector instantiation past this header.
    struct dropped_storage_entry_t;

    /// Version B* storages_ migration — agent role classifier.
    /// agent_disk_0 is the CATALOG agent: it co-locates oid_gen_, stored_catalog_,
    /// file_wal_id_, and (after migration completes) the pg_* system storages.
    /// agent_disk_[1..N-1] are USER_POOL agents: hash-routed by table_oid % (N-1)
    /// to spread user-table storage ownership across schedulers.
    /// Match must align with manager_disk_t::pool_idx_for_oid: idx 0 ↔ CATALOG.
    enum class agent_role_t : std::uint8_t
    {
        CATALOG = 0, // agent 0: pg_* system tables + oid_gen_ + stored_catalog_
        USER_POOL = 1 // agents 1..N-1: user tables routed by oid hash
    };

    class agent_disk_t final : public actor_zeta::basic_actor<agent_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        /// Default-constructed agent: CATALOG role, pool_idx = 0.
        /// Existing call-sites (manager_disk_t::create_agent loop) keep this form
        /// until Step 3 starts threading explicit role/pool_idx through.
        agent_disk_t(std::pmr::memory_resource* resource, manager_disk_t* manager, const path_t& path_db, log_t& log);

        /// Role-aware constructor — used once the manager_disk's create_agent loop
        /// is updated to assign agent 0 = CATALOG and agents 1..N-1 = USER_POOL
        /// with their respective pool_idx. Until then, this overload is only
        /// exercised by future Step 2/3 wiring; the legacy constructor delegates
        /// to it with {CATALOG, 0}.
        agent_disk_t(std::pmr::memory_resource* resource,
                     manager_disk_t* manager,
                     const path_t& path_db,
                     log_t& log,
                     agent_role_t role,
                     std::size_t pool_idx);

        ~agent_disk_t();

        auto make_type() const noexcept -> const char*;

        // ──────────────── Step 2 storages_ slice (real member) ────────────────
        //
        // The eight-step ownership transfer from manager_disk_t::storages_
        // is COMPLETE (Step 8.11 wrap deleted the manager-side map; Step
        // 8.12 dropped the legacy bootstrap_record_oid_sync entry point and
        // collapsed dead manager-side bodies). Audit-trail summary:
        //   Steps 1–4 (landed): identity + skeleton; promote
        //           collection_storage_entry_t to namespace scope; plant
        //           agent-side storages_ slice; WAL-replay direct_*_sync
        //           forwarded to the owning agent.
        //   Step 5 (landed): storage_scan / mutation set mailbox handlers
        //           fan out from manager_disk to the owning agent
        //           (dispatch_traits entries on agent_disk_t).
        //   Step 6 (landed): checkpoint_all / vacuum_all /
        //           on_horizon_advanced fanout to every agent via mailbox
        //           sends; per-agent inner handlers own the body.
        //   Step 7 (landed): per-agent dropped_storages_ slice + handoff
        //           overloads (bootstrap + mailbox); on_horizon_advanced_
        //           inner walks the slice and removes entries whose
        //           dropped_at_commit_id < new_horizon.
        //   Step 8 (landed):
        //     - §8.1.B / §8.1.C: catalog + user DISK SFBM ownership
        //       transferred to the agent (bootstrap_disk_inner_sync /
        //       bootstrap_create_disk_inner_sync plant real entries; the
        //       legacy DISK record-only marker pattern is gone).
        //     - §8.4: manager-side GC sweep + ack DELETED — per-agent
        //       on_horizon_advanced_inner is the canonical sweep.
        //     - §8.11 wrap: manager_disk_t::storages_ +
        //       dropped_storages_ DELETED; has_storage / total_rows_sync /
        //       checkpoint_wal_id_sync pure agent delegations.
        //     - §8.12 cleanup: bootstrap_record_oid_sync DELETED;
        //       get_storage(oid) DELETED; has_in_memory_inner_sync added
        //       to preserve the checkpoint_all WAL-seal suppression
        //       semantic.

        /// Pre-scheduler-start (bootstrap-only) probe — does this agent currently
        /// own the storage for `oid`? Looks up the local slice map. Returns
        /// false during Step 2 for every OID (slice starts empty); Step 3 will
        /// populate it via bootstrap_inner_sync from manager_disk's create /
        /// load paths so that this answer becomes authoritative for OIDs in
        /// this agent's routing slice.
        ///
        /// NOT a mailbox handler — bootstrap-only. After scheduler.start callers
        /// must go through the (Step 5) storage_* mailbox handlers.
        [[nodiscard]] bool has_storage_sync(components::catalog::oid_t oid) const noexcept;

        // ──────────────── Step 8.5 manager-router delegation helpers ──────────
        //
        // Pre-scheduler-start (bootstrap) AND in-mailbox sync probes used by the
        // three manager_disk_t public accessors (has_storage / total_rows_sync /
        // checkpoint_wal_id_sync). The manager-side accessors are called from:
        //   - tests (test_d4_lazy_load — bootstrap-time inspection),
        //   - base_spaces (Phase 2c bootstrap, before scheduler.start),
        //   - manager_disk_io::peek_checkpoint_wal_id_from_disk (bootstrap path).
        // All call sites are either single-threaded bootstrap or already inside
        // the manager's mailbox lock, so a synchronous probe into the routed
        // agent's slice is safe (Constraint #11 pre-scheduler carve-out).
        //
        // Post Step 8.11 wrap + Step 8.12 cleanup the agent slice is the SOLE
        // source of truth (manager_disk_t::storages_ deleted):
        //   - has_storage_inner_sync       — present-in-slice probe.
        //   - total_rows_inner_sync        — 0 for not-owned OIDs; null entries
        //                                    (defensive, unreachable post §8.1.B/C)
        //                                    skipped.
        //   - checkpoint_wal_id_inner_sync — same semantics as above for the
        //                                    persisted .otbx.wal_id tally.
        //
        // Manager-side accessor (Step 8.5): pure delegation to the routed
        // agent — no fallback path remains.
        [[nodiscard]] uint64_t total_rows_inner_sync(components::catalog::oid_t oid) const noexcept;
        [[nodiscard]] wal::id_t checkpoint_wal_id_inner_sync(components::catalog::oid_t oid) const noexcept;

        // ──────────────── Step 8.12 has_in_memory_inner_sync probe ────────
        //
        // Version B* Step 8.12 — restore the pre-cutover `checkpoint_all`
        // semantic that suppresses the WAL ID seal whenever ANY IN_MEMORY
        // entry exists. Before storages_ was split across agents, the
        // manager's checkpoint_all body returned `wal::id_t{0}` if it
        // observed any IN_MEMORY entry in its canonical map; this signalled
        // "do not advance the WAL ID floor because IN_MEMORY tables still
        // need replay records". After Step 8.11 wrap deleted manager.
        // storages_, the per-agent `checkpoint_inner` skips IN_MEMORY twins
        // entirely — the only signal left at the manager is the std::min
        // aggregation of the agents' `min(prev_checkpoint_wal_id_)` tallies,
        // which uses `numeric_limits::max()` as the "no DISK entry" sentinel.
        // That sentinel cannot distinguish "no DISK entry AND no IN_MEMORY
        // twin" (safe to seal) from "no DISK entry BUT IN_MEMORY twin
        // present" (must NOT seal).
        //
        // This probe restores the missing signal: a const, sync, mailbox-free
        // walk over the agent's storages_ slice that returns true iff any
        // entry is a real (non-null) IN_MEMORY twin. DISK record-only markers
        // (null unique_ptr from bootstrap_record_oid_sync, NOW DEAD per the
        // Step 8.1.B/C SFBM transfer audit) are by definition not IN_MEMORY,
        // and even if any remain they are reported as "not IN_MEMORY" because
        // their entry is null (can't read .mode()). Mirrors the
        // total_rows_inner_sync / checkpoint_wal_id_inner_sync sync-probe
        // pattern (Constraint #11 pre-scheduler-start carve-out + manager
        // mailbox post-start serialization — the manager is the only writer
        // to the agent slice, agent mailbox serializes against this read).
        [[nodiscard]] bool has_in_memory_inner_sync() const noexcept;

        // ──────────────── Step 8.11.A storage_entry_sync accessor ──────────
        //
        // Version B* Step 8.11.A — const raw-pointer accessor into this
        // agent's storages_ slice. Returns a borrowed pointer to the
        // collection_storage_entry_t owned by the slice, or nullptr when
        // the OID is not in this agent's slice (caller treats nullptr as
        // "absent"; post Step 8.11 wrap the slice is the SOLE source of
        // truth — no manager-side fallback exists).
        //
        // Constraint #11 carve-out — raw-pointer read into the agent's slice
        // is permitted ONLY through this sync accessor because (a) the
        // entry is owned by std::unique_ptr in the agent's slice (stable
        // address for the lifetime of the slice entry, the agent's mailbox
        // serializes all writes to storages_ post-start), (b) the slice is
        // populated either pre-scheduler-start (bootstrap_inner_sync from
        // create_storage_with_columns_sync) or via the mailbox handler
        // drop_storage_inner, so a sync read from the manager thread is
        // race-free while the agent thread is idle (single-threaded actor
        // mailbox semantics). Callers MUST treat the returned pointer as
        // borrowed — do NOT store across mailbox-yields, do NOT delete.
        //
        // Migration status: APPLIED across all 11 catalog + 4 generic call
        // sites documented in the Step 8.9 audit (Step 8.11.B catalog wave
        // + Step 8.11.C generic-table_oid wave); every manager-side read of
        // storages_ has been retired in favour of this accessor.
        //
        // NOT a mailbox handler — safe to call from the manager thread
        // before scheduler.start AND from inside a manager mailbox handler
        // post-start (the manager is the only thread that mutates the
        // agent slice via bootstrap_*_sync, and post-start mutations go
        // through agent mailboxes which serialize against this read).
        [[nodiscard]] const collection_storage_entry_t*
        storage_entry_sync(components::catalog::oid_t oid) const noexcept;

        /// Pre-scheduler-start (bootstrap-only) — ownership-transfer entry
        /// point. Takes an rvalue `std::unique_ptr<collection_storage_entry_t>`
        /// (Constraint #11: move-only, never a borrowed raw pointer) and
        /// inserts it into this agent's storages_ slice keyed by `oid`.
        ///
        /// Returns true if the insert succeeded, false if `oid` was already
        /// present in the slice (caller is expected to log and drop the
        /// duplicate — the existing entry retains ownership). Step 3 callers
        /// in manager_disk's create_storage_with_columns_sync /
        /// load_storage_disk_sync paths route entries here based on
        /// pool_idx_for_oid; until Step 3 wires that routing, this method is
        /// invoked only by tests / future migration code.
        [[nodiscard]] bool
        bootstrap_inner_sync(components::catalog::oid_t oid,
                             std::unique_ptr<collection_storage_entry_t> entry) noexcept;

        // Step 8.12 audit (2026-05-31): `bootstrap_record_oid_sync(oid)`
        // — the Step 3 DISK record-only-marker entry point — is DELETED.
        // Audit walk confirmed zero call sites in production code after
        // §8.1.B (catalog SFBM transfer) and §8.1.C (user SFBM transfer)
        // both landed: every DISK OID now reaches the agent slice via
        // `bootstrap_disk_inner_sync` (load) or
        // `bootstrap_create_disk_inner_sync` (create), which populate the
        // slice with a REAL `collection_storage_entry_t` owning the live
        // SFBM rather than a null marker. The null-entry skip branches in
        // the inner handlers (`storage_*_inner`, `checkpoint_inner`,
        // `vacuum_inner`, etc.) remain as defensive no-ops but are no
        // longer reachable through normal control flow.

        // ──────────────── Step 8.1 DISK ownership constructors ────────────
        //
        // Version B* Step 8.1 — agent-owned DISK construction primitives.
        //
        // These two helpers construct the `collection_storage_entry_t`
        // (single_file_block_manager_t holder) directly on the agent thread.
        // single_file_block_manager_t holds an exclusive file_handle_t
        // WRITE_LOCK on the underlying `.otbx` — opening it twice in the same
        // process produces a footgun (closing either fd releases the
        // posix-advisory lock for both). Therefore these primitives may be
        // invoked ONLY when the manager-side `storages_.emplace` for the same
        // OID is SKIPPED.
        //
        // bootstrap_disk_inner_sync — load existing `.otbx`. Mirrors
        //   `collection_storage_entry_t(resource, otbx_path)`. Seeds
        //   checkpoint_wal_id from the optional `sidecar_wal_id` (read from
        //   the `.otbx.wal_id` sidecar by the caller — manager_disk_io's
        //   load_storage_disk_sync). Returns true on insert success, false on
        //   duplicate key (existing entry retains ownership; incoming load
        //   is dropped by the caller).
        //
        // bootstrap_create_disk_inner_sync — create new `.otbx`. Mirrors
        //   `collection_storage_entry_t(resource, columns, otbx_path)`.
        //   Used by manager_disk_io's create_storage_disk_sync path. Same
        //   contract as bootstrap_disk_inner_sync.
        //
        // Both are NOT mailbox handlers — bootstrap-only (Constraint #11
        // pre-scheduler-start carve-out). Safe to call from the manager_disk
        // thread before scheduler.start because no other thread has touched
        // the agent's resource() yet.
        //
        // Roadmap status (Step 8.1.A landed): API surface only. No call site
        // routes through these yet — manager_disk_t::storages_ remains the
        // canonical SFBM owner for both catalog and user DISK entries. The
        // wiring lands incrementally in 8.1.B (catalog SFBM cutover, with
        // matching resolve/bootstrap reader migration as a pre-requisite) and
        // 8.2 (user-table cutover).
        [[nodiscard]] bool
        bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                   const std::filesystem::path& otbx_path,
                                   wal::id_t sidecar_wal_id) noexcept;

        [[nodiscard]] bool
        bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                          std::vector<components::table::column_definition_t> columns,
                                          const std::filesystem::path& otbx_path) noexcept;

        /// Read-only role/pool introspection — used by router fanout (Step 5+)
        /// and by tests/diagnostics. NOT a mailbox handler; safe to call from
        /// the manager_disk thread before scheduler.start.
        [[nodiscard]] agent_role_t role() const noexcept { return role_; }
        [[nodiscard]] std::size_t pool_idx() const noexcept { return pool_idx_; }

        // ──────────────── Step 4 WAL-replay direct_* sync helpers ─────────────
        //
        // Pre-scheduler-start (WAL replay phase in base_spaces). Post Step
        // 8.11 wrap the agent slice is the SOLE source of truth: the
        // manager-side direct_*_sync methods are pure routers that compute
        // pool_idx_for_oid and forward here. The helpers below apply the
        // canonical mutation against the entry in the local slice. Null
        // entries (defensive — legacy DISK record-only markers, unreachable
        // post §8.1.B/C) and missing OIDs are logged + no-op'd (the slice's
        // routing decision is authoritative for THIS agent only; another
        // agent may own the OID).
        //
        // NOT mailbox handlers — bootstrap-only (Phase 4 base_spaces WAL
        // replay runs synchronously before scheduler.start). After Step 5,
        // post-start mutations go through the storage_* mailbox handlers.
        void direct_append_sync(components::catalog::oid_t table_oid,
                                components::vector::data_chunk_t& data,
                                core::date::timezone_offset_t session_tz,
                                const components::table::transaction_data& txn);
        void direct_delete_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                uint64_t count,
                                const components::table::transaction_data& txn);
        void direct_update_sync(components::catalog::oid_t table_oid,
                                const std::pmr::vector<int64_t>& row_ids,
                                components::vector::data_chunk_t& new_data);

        unique_future<void> fix_wal_id(wal::id_t wal_id);

        // ──────────────── Step 5 storage_* mailbox handlers ───────────────────
        //
        // Version B* Step 5 router-fanout entry points. manager_disk_t::storage_*
        // mailbox handlers forward to agents_[pool_idx_for_oid(table_oid)] via
        // actor_zeta::otterbrix::send (mailbox-only, Constraint #11 — no shared
        // state crosses the actor boundary; rvalue unique_ptr / by-value PMR
        // containers carry the payload).
        //
        // Read-path (storage_scan): returns a new unique_ptr<data_chunk_t>
        // owned by the caller's coroutine frame; agent-side storage_t::scan
        // reads against the entry in the slice.
        //
        // Post Step 8.11 wrap the agent slice is the SOLE source of truth —
        // the manager-side storages_ map is gone. When this agent's local
        // slice does NOT own the OID, the agent returns nullptr and the
        // caller surfaces an empty chunk (no manager-side fallback exists).
        // Null entries in the slice (defensive — legacy DISK record-only
        // markers, unreachable post §8.1.B/C) also yield nullptr.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     components::catalog::oid_t table_oid,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);

        // ──────────────── Step 5 mutation-set mailbox handlers ────────────
        //
        // Version B* Step 5 — core mutation fanout targets. Post Step 8.11
        // wrap these inner handlers are the SOLE owners of each mutation;
        // the manager-side bodies in manager_disk_storage.cpp are pure
        // routers (data partitioned + dispatched per pool_idx_for_oid).
        //
        // Outcomes — when this agent's local slice does NOT own the OID, or
        // the entry is null (defensive — legacy DISK record-only marker,
        // unreachable post §8.1.B/C), the handler is a logged no-op.
        //
        // storage_append_inner — minimal append against entry->storage. The
        //   manager-side router owns the schema-adoption / column-expansion
        //   / type-promotion / dedup / NOT NULL enforcement pipeline before
        //   dispatch; this handler applies the final aligned write.
        //   Returns void.
        unique_future<void> storage_append_inner(components::catalog::oid_t table_oid,
                                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                                 components::table::transaction_data txn);

        // storage_publish_commits_inner — MVCC visibility flip. Manager body
        //   iterates `ranges` and calls commit_append per range; agent does
        //   the same against IN_MEMORY twins in its slice. Ranges whose
        //   table_oid isn't owned by this agent are skipped. PMR vector
        //   chosen for the ranges payload (Constraint #11).
        unique_future<void>
        storage_publish_commits_inner(uint64_t commit_id,
                                      std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        // storage_publish_deletes_inner — MVCC delete commit. Manager body
        //   iterates `tables` and calls commit_all_deletes(txn_id, commit_id)
        //   per oid; agent does the same against IN_MEMORY twins it owns.
        //   txn_id and commit_id are by-value; the table-OID list is a
        //   std::pmr::vector (PMR-friendly, Constraint #11).
        unique_future<void> storage_publish_deletes_inner(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          std::pmr::vector<components::catalog::oid_t> tables);

        // ──────────────── Step 5 abort-path + completion handlers ─────────
        //
        // Version B* Step 5 (final cleanup) — abort-path mirrors for the
        // mutation set. The manager-side bodies in manager_disk_storage.cpp
        // remain canonical until Step 8; these inner handlers apply the
        // SAME revert / update / delete / fetch against the agent's
        // IN_MEMORY twin so the slice stays in lockstep.
        //
        // Same fallback contract as storage_append_inner / publish_*_inner:
        // not-owned OIDs and DISK record-only markers (Step 3) are logged
        // + no-op'd (for void handlers) or return a null unique_ptr (for
        // storage_fetch_inner so the manager body falls back). Constraint
        // #11: mailbox-only, no shared mutable state — PMR vectors by
        // value, no raw pointers crossing the actor boundary, no
        // exceptions.

        // storage_revert_appends_inner — batched abort. Manager body
        //   reverse-iterates `ranges` and calls revert_append per range;
        //   agent does the same against IN_MEMORY twins it owns. Ranges
        //   whose table_oid isn't owned by this agent are skipped. PMR
        //   vector for the ranges payload (matches publish_commits_inner).
        unique_future<void>
        storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        // storage_revert_append_inner — single-OID abort (lone INSERT
        //   rollback). Mirrors manager_disk_t::storage_revert_append's
        //   body: looks up the agent twin and calls revert_append on the
        //   storage_t adapter.
        unique_future<void> storage_revert_append_inner(components::catalog::oid_t table_oid,
                                                        int64_t row_start,
                                                        uint64_t count);

        // storage_update_inner — single-OID UPDATE mutation against the
        //   agent twin. Manager body owns the canonical update against
        //   manager_disk_t::storages_ and observes the (delta, count) pair;
        //   agent fanout returns void — Step 8 will make the agent
        //   authoritative and propagate the pair from here.
        unique_future<void> storage_update_inner(components::catalog::oid_t table_oid,
                                                 components::vector::vector_t row_ids,
                                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                                 components::table::transaction_data txn);

        // storage_delete_rows_inner — single-OID DELETE mutation. Manager
        //   body owns the canonical delete and the returned row count;
        //   agent fanout mirrors against the twin for lockstep.
        unique_future<void> storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                                      components::vector::vector_t row_ids,
                                                      uint64_t count,
                                                      components::table::transaction_data txn);

        // storage_fetch_inner — read-path mirror for point-fetches by
        //   row_id. Returns nullptr when the agent doesn't own the OID
        //   (or owns it as a DISK record-only marker) so the manager body
        //   falls back — same contract as storage_scan above.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch_inner(components::catalog::oid_t table_oid,
                            components::vector::vector_t row_ids,
                            uint64_t count);

        // ──────────────── Step 5 batched-scan + metadata mailbox handlers ─
        //
        // Version B* Step 5 (final cleanup) — remaining read-path mirrors:
        // scan_batched + scan_segment + types + total_rows. Same fallback
        // contract as storage_scan / storage_fetch_inner: not-owned OIDs
        // and DISK record-only markers return an empty/zero sentinel so the
        // manager body falls back. Constraint #11: PMR containers by value,
        // no shared mutable state, no exceptions.
        //
        // storage_scan_batched_inner — batched + projected scan. Returns a
        //   PMR vector of data_chunk_t batches (≤ DEFAULT_VECTOR_CAPACITY
        //   rows each). When the agent doesn't own the OID, returns an
        //   EMPTY pmr-vector — the manager body distinguishes "agent ran +
        //   produced 0 batches" from "agent didn't own" by re-issuing the
        //   read against its canonical map only when the agent slice has
        //   no entry. (See manager-side comment for the precise gate.)
        unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        storage_scan_batched_inner(components::catalog::oid_t table_oid,
                                   std::unique_ptr<components::table::table_filter_t> filter,
                                   int64_t limit,
                                   std::vector<size_t> projected_cols,
                                   components::table::transaction_data txn);

        // storage_scan_segment_inner — start-offset / count window scan.
        //   Returns nullptr when the agent doesn't own the OID (same as
        //   storage_scan / storage_fetch_inner) so the manager body falls
        //   back. The agent runs storage_t::scan_segment against its
        //   IN_MEMORY twin.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment_inner(components::catalog::oid_t table_oid,
                                   int64_t start,
                                   uint64_t count);

        // storage_types_inner — schema metadata accessor. Returns an
        //   EMPTY pmr-vector as the not-owned sentinel; a real but
        //   schema-less twin also returns empty (storage_t::types() does
        //   the same), so the manager body falls back whenever the
        //   returned vector is empty.
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types_inner(components::catalog::oid_t table_oid);

        // storage_total_rows_inner — row-count metadata accessor. Returns
        //   0 as the not-owned sentinel; manager body falls back when the
        //   agent answers 0 (an empty twin and a not-owned OID are
        //   indistinguishable from the caller's perspective — both reduce
        //   to "ask the manager"). This is acceptable because the
        //   manager-side body is still authoritative until Step 8.
        unique_future<uint64_t> storage_total_rows_inner(components::catalog::oid_t table_oid);

        // ──────────────── Step 6 fanout-only mailbox handlers ─────────────────
        //
        // Version B* Step 6 — router fanout for the three fanout-only manager
        // methods: checkpoint_all / vacuum_all / on_horizon_advanced. Manager-
        // side bodies remain authoritative for the canonical storages_ map (Step
        // 8 deletes them); the agent inner handlers iterate THIS agent's
        // storages_ slice in parallel so the slice stays in lockstep with the
        // canonical map. Constraint #11: mailbox-only fanout, std::pmr
        // containers, no shared state crosses the actor boundary.
        //
        // checkpoint_inner — iterates the agent's slice and runs the same
        //   checkpoint sequence (compact + checkpoint(wal_id) + sidecar) per
        //   DISK-mode entry the agent owns. Returns min(prev_checkpoint_wal_id_)
        //   across the agent's DISK entries, OR max() sentinel when the agent
        //   owns no DISK entries. The caller (manager_disk_t::checkpoint_all)
        //   aggregates with std::min across all agents AND the manager-side
        //   tally. DISK record-only markers (Step 3 null entries) and IN_MEMORY
        //   entries are skipped — only the manager owns the live single_file_
        //   block_manager_t until Step 8.
        unique_future<wal::id_t> checkpoint_inner(session_id_t session, wal::id_t current_wal_id);

        // vacuum_inner — iterates the agent's slice and runs cleanup_versions +
        //   compact per entry. Mirrors manager_disk_t::vacuum_all's body. Void
        //   return — no aggregation needed.
        unique_future<void> vacuum_inner(session_id_t session, uint64_t lowest_active_start_time);

        // ──────────────── Step 8.7 single-route maintenance handler ────────
        //
        // maybe_cleanup_inner — Version B* Step 8.7: canonical body for
        //   maybe_cleanup against THIS agent's slice for the single target
        //   OID. Unlike vacuum_inner / checkpoint_inner (Step 6 fanout),
        //   maybe_cleanup is keyed by ctx.table_oid and only the owning
        //   agent needs to act — manager_disk_t::maybe_cleanup is a pure
        //   router (post Step 8.11 wrap) that computes
        //   pool_idx_for_oid(table_oid) and sends to that one agent.
        //
        //   Body preserves the original semantics: if deleted/total > 0.3
        //   and lowest_active_start_time is past the TRANSACTION_ID_START
        //   horizon, run table.compact() (cleanup_versions intentionally
        //   omitted — see body comment for rationale).
        //
        //   Outcomes: not-owned OIDs are logged no-ops; null entries
        //   (defensive — legacy DISK record-only markers, unreachable post
        //   §8.1.B/C) are logged no-ops. Constraint #11: by-value oid +
        //   uint64, no shared state crosses the actor boundary. No
        //   exceptions (Constraint #1).
        unique_future<void> maybe_cleanup_inner(components::catalog::oid_t table_oid,
                                                 uint64_t lowest_active_start_time);

        // on_horizon_advanced_inner — Version B* Step 7: canonical sweep
        //   over the agent's dropped_storages_ slice (post Step 8.4.D / 8.11
        //   wrap; manager-side sweep + slice DELETED). Physically removes
        //   entries whose dropped_at_commit_id < new_horizon.
        //   std::error_code overloads on every std::filesystem::remove call
        //   — exceptions FORBIDDEN.
        //
        //   Also fires the on_subscriber_empty(DISK_KIND) ack send to the
        //   dispatcher once this agent's slice has drained (gated on
        //   manager_dispatcher_addr_ != empty_address(), plumbed via
        //   set_manager_dispatcher_sync from base_spaces during bootstrap).
        //   Idempotent at the dispatcher (clear-while-cleared is a no-op;
        //   the dispatcher idempotently collapses N agent acks into one
        //   disk_has_dropped_ flag flip). Void return.
        unique_future<void> on_horizon_advanced_inner(uint64_t new_horizon);

        // ──────────────── Step 7 dropped_storages_ slice bootstrap ────────────
        //
        // Pre-scheduler-start helper (bootstrap path): push-back into the
        // agent's local dropped_storages_ slice synchronously. Used by
        // manager_disk_t::register_dropped_storage_sync when it knows the
        // schedulers have not started yet (base_spaces PHASE 2c catalog rebuild
        // — dec 18 V1 carve-out). NOT a mailbox handler.
        void register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                  uint64_t dropped_at_commit_id,
                                                  std::filesystem::path path,
                                                  std::pmr::vector<std::filesystem::path> sidecar_paths);

        // Runtime DROP path mailbox handler: manager_disk_t::mark_storage_
        // dropped's mailbox body forwards into this via actor_zeta::otterbrix::
        // send so the per-agent slice gets the entry without crossing the
        // actor boundary as shared mutable state (Constraint #11). Internally
        // delegates to register_dropped_storage_inner_sync.
        unique_future<void> register_dropped_storage_inner(components::catalog::oid_t oid,
                                                            uint64_t dropped_at_commit_id,
                                                            std::filesystem::path path,
                                                            std::pmr::vector<std::filesystem::path> sidecar_paths);

        // ──────────────── Step 8.3 drop_storage fanout mailbox handler ─────
        //
        // Version B* Step 8.3 — runtime DROP TABLE storages_ erase.
        // manager_disk_t::drop_storage is a pure mailbox router (post Step
        // 8.11 wrap) that forwards here via actor_zeta::otterbrix::send.
        // This handler is the SOLE owner of the canonical erase + physical
        // .otbx removal (Step 8.4.B moved the file removal here too).
        //
        // Idempotent: erasing a missing key is a no-op. IN_MEMORY twins
        // and DISK entries are handled uniformly by std::pmr::unordered_
        // map::erase. The single_file_block_manager_t WRITE_LOCK is
        // released exactly once when erase drops the unique_ptr; null
        // entries (defensive — legacy DISK record-only markers, unreachable
        // post §8.1.B/C) reduce to a routing-slot release.
        //
        // Constraint #11: mailbox-only forward, no shared mutable state
        // crossing the actor boundary (oid is by value), no exceptions.
        unique_future<void> drop_storage_inner(components::catalog::oid_t oid);

        // ──────────────── Step 8.11.C drop_column fanout mailbox handler ───
        //
        // Version B* Step 8.11.C — physical column compaction routed to the
        // agent that owns the entry. compact_relkind_g_storage
        // (manager_disk_ddl.cpp) reads the column set through
        // storage_entry_sync (Step 8.11.A const accessor); the mutation half
        // (collection_storage_entry_t::drop_column) is non-const and runs
        // here because it rewrites table_storage and recreates the storage
        // adapter in place. Post Step 8.11 wrap the agent slice is the SOLE
        // source of truth — no manager-side fallback.
        //
        // Outcomes:
        //   - OID not in this agent's slice → logged no-op,
        //   - Null entry (defensive — legacy DISK record-only marker,
        //     unreachable post §8.1.B/C) → logged no-op (DISK drop_column is
        //     out-of-scope per table_storage_t::drop_column on DISK),
        //   - Idempotent re-issue → logged "not found".
        //
        // Constraint #11: mailbox-only (entry point via dispatch), no shared
        // mutable state crosses the actor boundary (oid by value;
        // column_name is a std::pmr::string moved by value), no exceptions.
        // resource() is used to rebuild the storage adapter on the agent's
        // own arena.
        unique_future<void> drop_column_inner(components::catalog::oid_t table_oid,
                                              std::pmr::string column_name);

        // ──────────────── Step 8 dispatcher plumbing ──────────────────────────
        //
        // Bootstrap-only helper — base_spaces wires the manager_dispatcher_t
        // address into every agent immediately after spawning agents and
        // before scheduler.start. Used by on_horizon_advanced_inner to send
        // the on_subscriber_empty(DISK_KIND) ack directly to the dispatcher
        // once this agent's dropped_storages_ slice has drained. Post Step
        // 8.4.D / 8.11 wrap the per-agent ack is the SOLE source of the
        // DISK_KIND empty signal (manager-side ack DELETED). Constraint #11:
        // address_t is an actor-zeta handle (mailbox endpoint), not mutable
        // state — safe to copy across actors.
        //
        // NOT a mailbox handler; safe to call from the manager_disk thread or
        // from base_spaces before scheduler.start. Single-threaded by
        // construction at the bootstrap call site.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        using dispatch_traits = actor_zeta::dispatch_traits<&agent_disk_t::fix_wal_id,
                                                            &agent_disk_t::storage_scan,
                                                            &agent_disk_t::storage_append_inner,
                                                            &agent_disk_t::storage_publish_commits_inner,
                                                            &agent_disk_t::storage_publish_deletes_inner,
                                                            &agent_disk_t::storage_revert_appends_inner,
                                                            &agent_disk_t::storage_revert_append_inner,
                                                            &agent_disk_t::storage_update_inner,
                                                            &agent_disk_t::storage_delete_rows_inner,
                                                            &agent_disk_t::storage_fetch_inner,
                                                            &agent_disk_t::storage_scan_batched_inner,
                                                            &agent_disk_t::storage_scan_segment_inner,
                                                            &agent_disk_t::storage_types_inner,
                                                            &agent_disk_t::storage_total_rows_inner,
                                                            &agent_disk_t::checkpoint_inner,
                                                            &agent_disk_t::vacuum_inner,
                                                            &agent_disk_t::maybe_cleanup_inner,
                                                            &agent_disk_t::on_horizon_advanced_inner,
                                                            &agent_disk_t::register_dropped_storage_inner,
                                                            &agent_disk_t::drop_storage_inner,
                                                            &agent_disk_t::drop_column_inner>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        const name_t name_;
        log_t log_;
        path_t path_;
        core::filesystem::local_file_system_t fs_;
        file_ptr file_wal_id_;

        // Version B* Step 1: routing identity.
        agent_role_t role_;
        std::size_t pool_idx_;

        // Version B* Step 2: storages_ slice owned by this agent. PMR-allocated
        // from the actor's resource() (initialized in agent_disk.cpp ctor).
        // collection_storage_entry_t is forward-declared at namespace scope
        // above; the full definition lives in manager_disk.hpp and is reached
        // via the manager_disk.hpp include inside agent_disk.cpp. The user-
        // provided destructor (defined in agent_disk.cpp) defers unordered_map
        // template instantiation past this header, so the incomplete value
        // type is safe here.
        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // Version B* Step 8.6: per-agent dropped_storages_ slice — CANONICAL
        // owner of GC state. Populated by register_dropped_storage_inner
        // from (1) base_spaces Phase 2c catalog scan rebuild via
        // manager_disk_t::register_dropped_storage_sync and (2) the runtime
        // DROP path manager_disk_t::mark_storage_dropped (mailbox-routed).
        // on_horizon_advanced_inner walks this slice, removes entries whose
        // dropped_at_commit_id < new_horizon, and emits the
        // on_subscriber_empty(DISK_KIND) ack once the slice drains.
        //
        // Version B* Step 8.11: the manager-side dropped_storages_ mirror
        // has been DELETED — this per-agent slice is now the SOLE owner of
        // GC state.
        //
        // PMR-allocated from the actor's resource() — same arena as
        // storages_ above; std::pmr::vector body instantiated in
        // agent_disk.cpp (forward-decl above; full def via manager_disk.hpp).
        std::pmr::vector<dropped_storage_entry_t> dropped_storages_;

        // Version B* Step 8: manager_dispatcher_t address. Plumbed by
        // base_spaces via set_manager_dispatcher_sync after spawn and before
        // scheduler.start. Used by on_horizon_advanced_inner to send the
        // on_subscriber_empty(DISK_KIND) ack directly to the dispatcher once
        // this agent's dropped_storages_ slice has drained. Empty by default;
        // the ack path is gated on != empty_address() so test fixtures that
        // skip the wire-up (no dispatcher) still pass through cleanly.
        // Constraint #11: address_t is a mailbox handle, not mutable state.
        actor_zeta::address_t manager_dispatcher_addr_{actor_zeta::address_t::empty_address()};
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
