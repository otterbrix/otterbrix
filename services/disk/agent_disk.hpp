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

    // Forward-declared here; full definitions live in manager_disk.hpp.
    // agent_disk_t owns slice containers whose value types are incomplete in
    // this header — safe because the user-provided destructor in
    // agent_disk.cpp defers template instantiation past this header.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    /// Agent role classifier (storages_ partition).
    /// agent_disk_0 is the CATALOG agent: it co-locates oid_gen_, stored_catalog_,
    /// file_wal_id_, and the pg_* system storages.
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
        agent_disk_t(std::pmr::memory_resource* resource, manager_disk_t* manager, const path_t& path_db, log_t& log);

        /// Role-aware constructor. agent 0 = CATALOG; agents 1..N-1 = USER_POOL
        /// with their respective pool_idx (matches pool_idx_for_oid contract).
        agent_disk_t(std::pmr::memory_resource* resource,
                     manager_disk_t* manager,
                     const path_t& path_db,
                     log_t& log,
                     agent_role_t role,
                     std::size_t pool_idx);

        ~agent_disk_t();

        auto make_type() const noexcept -> const char*;

        // ──────────────── storages_ slice (sole owner) ─────────────────
        //
        // Catalog + user DISK SFBM ownership lives on the agent; the manager
        // is a pure router. has_in_memory_inner_sync preserves the
        // checkpoint_all WAL-seal suppression semantic; per-agent
        // on_horizon_advanced_inner is the canonical GC sweep.

        /// Pre-scheduler-start (bootstrap-only) probe — does this agent currently
        /// own the storage for `oid`? Authoritative for OIDs in this agent's
        /// routing slice.
        ///
        /// NOT a mailbox handler — bootstrap-only. After scheduler.start callers
        /// must go through the storage_* mailbox handlers.
        [[nodiscard]] bool has_storage_sync(components::catalog::oid_t oid) const noexcept;

        // ──────────────── manager-router delegation helpers ──────────
        //
        // Sync probes used by the three manager_disk_t public accessors
        // (has_storage / total_rows_sync / checkpoint_wal_id_sync). All call
        // sites are either single-threaded bootstrap or already inside the
        // manager's mailbox lock, so a synchronous probe into the routed
        // agent's slice is safe (pre-scheduler carve-out).
        //
        // 0 / wal::id_t{0} returned for not-owned OIDs. Null entries
        // (defensive — unreachable in current state) are skipped.
        [[nodiscard]] uint64_t total_rows_inner_sync(components::catalog::oid_t oid) const noexcept;
        [[nodiscard]] wal::id_t checkpoint_wal_id_inner_sync(components::catalog::oid_t oid) const noexcept;

        // ──────────────── has_in_memory_inner_sync probe ────────
        //
        // checkpoint_all must suppress the WAL ID seal whenever ANY IN_MEMORY
        // entry exists (those tables still need WAL records for replay). The
        // per-agent checkpoint_inner skips IN_MEMORY twins entirely, so the
        // std::min(prev_checkpoint_wal_id_) aggregation alone cannot
        // distinguish "no DISK entry AND no IN_MEMORY twin" (safe to seal)
        // from "no DISK entry BUT IN_MEMORY twin present" (must NOT seal).
        //
        // This sync, mailbox-free walk over the agent's slice returns true
        // iff any entry is a real (non-null) IN_MEMORY twin. Same carve-out
        // as the other *_inner_sync probes — agent mailbox serializes
        // against this read.
        [[nodiscard]] bool has_in_memory_inner_sync() const noexcept;

        // ──────────────── storage_entry_sync accessor ──────────
        //
        // Const raw-pointer accessor into this agent's storages_ slice.
        // Returns nullptr when the OID is not in this agent's slice.
        //
        // Raw-pointer read carve-out: permitted because (a) the entry is
        // owned by std::unique_ptr (stable address for the slice entry's
        // lifetime; the agent's mailbox serializes all writes to storages_
        // post-start), (b) the slice is populated either pre-scheduler-start
        // (bootstrap_inner_sync) or via the mailbox handler
        // drop_storage_inner. Sync read from the manager thread is race-free
        // while the agent thread is idle (single-threaded actor mailbox
        // semantics). Callers MUST treat the returned pointer as borrowed —
        // do NOT store across mailbox-yields, do NOT delete.
        //
        // NOT a mailbox handler — safe to call from the manager thread
        // pre-scheduler.start AND from inside a manager mailbox handler
        // post-start.
        [[nodiscard]] const collection_storage_entry_t*
        storage_entry_sync(components::catalog::oid_t oid) const noexcept;

        /// Pre-scheduler-start (bootstrap-only) — ownership-transfer entry
        /// point. Takes an rvalue `std::unique_ptr<collection_storage_entry_t>`
        /// (move-only, never a borrowed raw pointer) and inserts it into this
        /// agent's storages_ slice keyed by `oid`.
        ///
        /// Returns true if the insert succeeded, false if `oid` was already
        /// present in the slice (caller is expected to log and drop the
        /// duplicate — the existing entry retains ownership).
        [[nodiscard]] bool
        bootstrap_inner_sync(components::catalog::oid_t oid,
                             std::unique_ptr<collection_storage_entry_t> entry) noexcept;

        // ──────────────── DISK ownership constructors ────────────
        //
        // Construct the `collection_storage_entry_t`
        // (single_file_block_manager_t holder) directly on the agent thread.
        // single_file_block_manager_t holds an exclusive file_handle_t
        // WRITE_LOCK on the underlying `.otbx` — opening it twice in the same
        // process produces a footgun (closing either fd releases the
        // posix-advisory lock for both). Therefore these primitives may be
        // invoked ONLY when no other emplace for the same OID happens.
        //
        // bootstrap_disk_inner_sync — load existing `.otbx`. Seeds
        //   checkpoint_wal_id from the optional `sidecar_wal_id` (read from
        //   the `.otbx.wal_id` sidecar by the caller). Returns true on insert
        //   success, false on duplicate key.
        //
        // bootstrap_create_disk_inner_sync — create new `.otbx`. Same
        //   contract as bootstrap_disk_inner_sync.
        //
        // Both are NOT mailbox handlers — bootstrap-only (pre-scheduler-start
        // carve-out). Safe to call from the manager_disk thread before
        // scheduler.start because no other thread has touched the agent's
        // resource() yet.
        [[nodiscard]] bool
        bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                   const std::filesystem::path& otbx_path,
                                   wal::id_t sidecar_wal_id) noexcept;

        [[nodiscard]] bool
        bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                          std::vector<components::table::column_definition_t> columns,
                                          const std::filesystem::path& otbx_path) noexcept;

        /// Read-only role/pool introspection — used by router fanout and
        /// by tests/diagnostics. NOT a mailbox handler; safe to call from
        /// the manager_disk thread before scheduler.start.
        [[nodiscard]] agent_role_t role() const noexcept { return role_; }
        [[nodiscard]] std::size_t pool_idx() const noexcept { return pool_idx_; }

        // ──────────────── WAL-replay direct_* sync helpers ─────────────
        //
        // Pre-scheduler-start (WAL replay phase in base_spaces). The manager-
        // side direct_*_sync methods are pure routers that forward here.
        // The helpers apply the canonical mutation against the local slice;
        // missing OIDs are logged + no-op'd (another agent may own them).
        //
        // NOT mailbox handlers — bootstrap-only (base_spaces WAL replay
        // runs synchronously before scheduler.start). Post-start
        // mutations go through the storage_* mailbox handlers.
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

        // ──────────────── storage_* mailbox handlers ───────────────────
        //
        // Router-fanout entry points. manager_disk_t::storage_* mailbox
        // handlers forward to agents_[pool_idx_for_oid(table_oid)] via
        // actor_zeta::otterbrix::send (mailbox-only, no shared state across
        // actor boundary; rvalue unique_ptr / by-value PMR containers carry
        // the payload). When this agent's slice does NOT own the OID, the
        // agent returns nullptr and the caller surfaces an empty chunk.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     components::catalog::oid_t table_oid,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);

        // ──────────────── mutation-set mailbox handlers ────────────
        //
        // Core mutation fanout targets. These inner handlers are the SOLE
        // owners of each mutation; the manager-side bodies are pure routers.
        // Not-owned OIDs are logged + no-op'd.
        //
        // storage_append_inner — canonical append against entry->storage.
        //   Owns the FULL preprocessing pipeline (schema adoption / dynamic
        //   schema growth / column expansion / NOT NULL enforcement / dedup /
        //   type promotion) so that every same-oid access — preprocessing
        //   reads included — is serialized by this agent's mailbox. The
        //   manager-side body is a pure router. Returns (start_row, count)
        //   of the canonical write; (0, 0) on no-op/violation.
        unique_future<std::pair<uint64_t, uint64_t>>
        storage_append_inner(components::catalog::oid_t table_oid,
                             std::unique_ptr<components::vector::data_chunk_t> data,
                             components::table::transaction_data txn,
                             core::date::timezone_offset_t session_tz);

        // storage_publish_commits_inner — MVCC visibility flip. Iterates
        //   `ranges` and calls commit_append per range against owned twins;
        //   ranges whose table_oid isn't owned are skipped.
        unique_future<void>
        storage_publish_commits_inner(uint64_t commit_id,
                                      std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        // storage_publish_deletes_inner — MVCC delete commit. Iterates
        //   `tables` and calls commit_all_deletes(txn_id, commit_id) per
        //   owned twin.
        unique_future<void> storage_publish_deletes_inner(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          std::pmr::vector<components::catalog::oid_t> tables);

        // ──────────────── abort-path + completion handlers ─────────
        //
        // Apply the canonical revert / update / delete / fetch against the
        // agent's slice. Not-owned OIDs are logged no-ops (void handlers) or
        // return a null unique_ptr (fetch). PMR vectors by value, no raw
        // pointers across the actor boundary, no exceptions.

        // storage_revert_appends_inner — batched abort. Reverse-iterates
        //   ranges (preserves append-order opposite) and calls revert_append
        //   per range against owned twins.
        unique_future<void>
        storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        // storage_revert_append_inner — single-OID abort (lone INSERT rollback).
        unique_future<void> storage_revert_append_inner(components::catalog::oid_t table_oid,
                                                        int64_t row_start,
                                                        uint64_t count);

        // storage_update_inner — single-OID UPDATE mutation against the
        //   agent twin. Returns storage_t::update's (updated, appended)
        //   pair; (0, 0) on no-op.
        unique_future<std::pair<int64_t, uint64_t>>
        storage_update_inner(components::catalog::oid_t table_oid,
                             components::vector::vector_t row_ids,
                             std::unique_ptr<components::vector::data_chunk_t> data,
                             components::table::transaction_data txn);

        // storage_delete_rows_inner — single-OID DELETE mutation. Returns
        //   the deleted-row count; 0 on no-op.
        unique_future<uint64_t> storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                                          components::vector::vector_t row_ids,
                                                          uint64_t count,
                                                          components::table::transaction_data txn);

        // storage_fetch_inner — read-path mirror for point-fetches by row_id.
        //   Returns nullptr when the agent doesn't own the OID.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch_inner(components::catalog::oid_t table_oid,
                            components::vector::vector_t row_ids,
                            uint64_t count);

        // ──────────────── batched-scan + metadata mailbox handlers ─
        //
        // Read-path: scan_batched + scan_segment + types + total_rows.
        // Not-owned OIDs return an empty/zero sentinel.
        //
        // storage_scan_batched_inner — batched + projected scan. Returns a
        //   PMR vector of data_chunk_t batches (≤ DEFAULT_VECTOR_CAPACITY rows
        //   each).
        unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        storage_scan_batched_inner(components::catalog::oid_t table_oid,
                                   std::unique_ptr<components::table::table_filter_t> filter,
                                   int64_t limit,
                                   std::vector<size_t> projected_cols,
                                   components::table::transaction_data txn);

        // storage_scan_segment_inner — start-offset / count window scan.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment_inner(components::catalog::oid_t table_oid,
                                   int64_t start,
                                   uint64_t count);

        // storage_types_inner — schema metadata accessor.
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types_inner(components::catalog::oid_t table_oid);

        // storage_column_names_inner — column names in storage-column order
        //   (index in the vector == storage column index). Lets the manager
        //   resolve name-keyed filters to column indices without borrowing
        //   the agent-owned columns() across the actor boundary. Empty
        //   vector means "not owned" / record-only marker.
        unique_future<std::pmr::vector<std::string>>
        storage_column_names_inner(components::catalog::oid_t table_oid);

        // storage_total_rows_inner — row-count metadata accessor. 0 means
        //   either "not owned" or "empty twin" — both equivalent for callers.
        unique_future<uint64_t> storage_total_rows_inner(components::catalog::oid_t table_oid);

        // ──────────────── fanout-only mailbox handlers ─────────────────
        //
        // Router fanout for checkpoint_all / vacuum_all / on_horizon_advanced.
        // Each agent iterates its own storages_ slice in parallel.
        //
        // checkpoint_inner — runs (compact + checkpoint(wal_id) + sidecar) per
        //   DISK-mode entry the agent owns. Returns min(prev_checkpoint_wal_id_)
        //   across the agent's DISK entries, or max() sentinel when the agent
        //   owns no DISK entries (caller aggregates with std::min across all
        //   agents). IN_MEMORY entries are skipped.
        unique_future<wal::id_t> checkpoint_inner(session_id_t session, wal::id_t current_wal_id);

        // vacuum_inner — cleanup_versions + compact per entry.
        unique_future<void> vacuum_inner(session_id_t session, uint64_t lowest_active_start_time);

        // ──────────────── single-route maintenance handler ────────
        //
        // maybe_cleanup_inner — single-OID target (manager_disk_t::maybe_cleanup
        //   computes pool_idx_for_oid(table_oid) and sends to that one agent).
        //   If deleted/total > 0.3 and lowest_active_start_time is past the
        //   TRANSACTION_ID_START horizon, runs table.compact().
        //   cleanup_versions is intentionally omitted — scan_committed depends
        //   on intact version metadata to filter tombstones, which
        //   cleanup_versions would strip before compact rebuilds the row_group.
        unique_future<void> maybe_cleanup_inner(components::catalog::oid_t table_oid,
                                                 uint64_t lowest_active_start_time);

        // on_horizon_advanced_inner — canonical sweep over the agent's
        //   dropped_storages_ slice. Physically removes entries whose
        //   dropped_at_commit_id < new_horizon. Exceptions FORBIDDEN —
        //   std::error_code overloads on every filesystem::remove call.
        //
        //   Also fires on_subscriber_empty(DISK_KIND) to the dispatcher once
        //   this agent's slice has drained (gated on manager_dispatcher_addr_).
        //   Dispatcher idempotently collapses N agent acks into one
        //   disk_has_dropped_ flag flip.
        unique_future<void> on_horizon_advanced_inner(uint64_t new_horizon);

        // ──────────────── dropped_storages_ slice bootstrap ────────────
        //
        // Pre-scheduler-start helper: synchronously push-back into the agent's
        // dropped_storages_ slice. Used by base_spaces catalog rebuild via
        // manager_disk_t::register_dropped_storage_sync. NOT a mailbox handler.
        void register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                  uint64_t dropped_at_commit_id,
                                                  std::filesystem::path path,
                                                  std::pmr::vector<std::filesystem::path> sidecar_paths);

        // Runtime DROP path mailbox handler. manager_disk_t::mark_storage_dropped
        // forwards here so the per-agent slice gets the entry without sharing
        // mutable state across the actor boundary. Internally delegates to
        // register_dropped_storage_inner_sync.
        unique_future<void> register_dropped_storage_inner(components::catalog::oid_t oid,
                                                            uint64_t dropped_at_commit_id,
                                                            std::filesystem::path path,
                                                            std::pmr::vector<std::filesystem::path> sidecar_paths);

        // ──────────────── drop_storage fanout mailbox handler ─────
        //
        // Runtime DROP TABLE storages_ erase. manager_disk_t::drop_storage is
        // a pure router that forwards here. Sole owner of the canonical erase
        // + physical .otbx removal.
        //
        // Idempotent on a missing key. The single_file_block_manager_t
        // WRITE_LOCK is released exactly once when erase drops the unique_ptr.
        unique_future<void> drop_storage_inner(components::catalog::oid_t oid);

        // ──────────────── drop_column fanout mailbox handler ───
        //
        // Physical column compaction routed to the owning agent.
        // compact_relkind_g_storage reads the column set through
        // storage_entry_sync (const); the mutation half
        // (collection_storage_entry_t::drop_column) is non-const because it
        // rewrites table_storage and recreates the storage adapter in place.
        // resource() rebuilds the adapter on the agent's own arena.
        //
        // Idempotent re-issue logs "not found"; DISK drop_column is
        // out-of-scope (table_storage_t::drop_column returns false on DISK).
        unique_future<void> drop_column_inner(components::catalog::oid_t table_oid,
                                              std::pmr::string column_name);

        // ──────────────── dispatcher plumbing ──────────────────────────
        //
        // Bootstrap-only helper — base_spaces wires the manager_dispatcher_t
        // address into every agent immediately after spawn and before
        // scheduler.start. on_horizon_advanced_inner uses it to send the
        // on_subscriber_empty(DISK_KIND) ack to the dispatcher once this
        // agent's dropped_storages_ slice has drained. address_t is an
        // actor-zeta mailbox handle, not mutable state — safe to copy across
        // actors.
        //
        // NOT a mailbox handler; single-threaded by construction at the
        // bootstrap call site.
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
                                                            &agent_disk_t::storage_column_names_inner,
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

        agent_role_t role_;
        std::size_t pool_idx_;

        // storages_ slice owned by this agent. PMR-allocated from the
        // actor's resource(). collection_storage_entry_t is forward-declared
        // (full definition in manager_disk.hpp, reached via the manager_disk.hpp
        // include inside agent_disk.cpp). The user-provided destructor defined
        // in agent_disk.cpp defers unordered_map template instantiation past
        // this header, so the incomplete value type is safe here.
        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // Per-agent dropped_storages_ slice — sole owner of GC state.
        // Populated by register_dropped_storage_inner from (1) base_spaces
        // catalog scan rebuild and (2) the runtime DROP path
        // manager_disk_t::mark_storage_dropped. on_horizon_advanced_inner
        // walks this slice, removes entries whose
        // dropped_at_commit_id < new_horizon, and emits the
        // on_subscriber_empty(DISK_KIND) ack once the slice drains.
        std::pmr::vector<dropped_storage_entry_t> dropped_storages_;

        // manager_dispatcher_t address, plumbed via set_manager_dispatcher_sync.
        // Used by on_horizon_advanced_inner to send the on_subscriber_empty
        // (DISK_KIND) ack once dropped_storages_ has drained. Empty by default;
        // the ack path is gated on != empty_address() so test fixtures without
        // a dispatcher still pass through cleanly. address_t is a mailbox
        // handle, not mutable state.
        actor_zeta::address_t manager_dispatcher_addr_{actor_zeta::address_t::empty_address()};
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
