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

    // Forward-declared (full definitions in manager_disk.hpp). agent_disk_t's slice
    // maps use these as incomplete value types — safe because the user-provided
    // destructor in agent_disk.cpp defers template instantiation past this header.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    /// Agent role / storages_ partition. agent 0 = CATALOG (pg_* tables + oid_gen_ +
    /// stored_catalog_ + file_wal_id_); agents 1..N-1 = USER_POOL (user tables hashed
    /// by table_oid). MUST align with manager_disk_t::pool_idx_for_oid: idx 0 ↔ CATALOG.
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

        // storages_ slice: this agent is the SOLE owner of its DISK SFBMs; the
        // manager is a pure router.

        /// Bootstrap-only probe: does this agent own the storage for `oid`?
        /// NOT a mailbox handler — after scheduler.start, callers must go through
        /// the storage_* mailbox handlers.
        [[nodiscard]] bool has_storage_sync(components::catalog::oid_t oid) const noexcept;

        // Sync probes for the manager_disk_t public accessors (has_storage /
        // total_rows_sync / checkpoint_wal_id_sync). Safe because every call site is
        // single-threaded bootstrap or already inside the manager's mailbox lock.
        // Not-owned OIDs return 0 / wal::id_t{0}.
        [[nodiscard]] uint64_t total_rows_inner_sync(components::catalog::oid_t oid) const noexcept;
        [[nodiscard]] wal::id_t checkpoint_wal_id_inner_sync(components::catalog::oid_t oid) const noexcept;

        // checkpoint_all must NOT seal the WAL ID floor while any IN_MEMORY entry
        // exists (those tables still need replay records), but checkpoint_inner skips
        // IN_MEMORY twins, so the min(prev_checkpoint_wal_id_) tally can't detect
        // them. This mailbox-free slice walk returns the missing signal. Same
        // sync-read safety as the other *_inner_sync probes.
        [[nodiscard]] bool has_in_memory_inner_sync() const noexcept;

        // Const raw-pointer accessor into the storages_ slice; nullptr when the OID
        // isn't owned. The unique_ptr gives the entry a stable address and the agent
        // mailbox serializes all writes to storages_, so a sync read is race-free
        // while the agent thread is idle. Callers MUST treat the pointer as borrowed:
        // do NOT store it across a mailbox-yield, do NOT delete it. Not a mailbox
        // handler — safe from the manager thread pre-start or inside a manager
        // mailbox handler post-start.
        [[nodiscard]] const collection_storage_entry_t*
        storage_entry_sync(components::catalog::oid_t oid) const noexcept;

        /// Bootstrap-only ownership-transfer: moves the rvalue entry into the
        /// storages_ slice keyed by `oid`. Returns false if `oid` was already present
        /// (caller logs and drops the duplicate; the existing entry keeps ownership).
        [[nodiscard]] bool
        bootstrap_inner_sync(components::catalog::oid_t oid,
                             std::unique_ptr<collection_storage_entry_t> entry) noexcept;

        // DISK ownership constructors: build the SFBM-holding entry directly on the
        // agent thread. The SFBM holds an exclusive posix WRITE_LOCK on the .otbx
        // (per-process: closing either fd releases it for both), so these may run
        // ONLY when no other emplace for the same OID can race. Bootstrap-only, not
        // mailbox handlers — safe pre-scheduler-start because nothing else has touched
        // the agent's resource() yet. Both return false on duplicate key.
        //   bootstrap_disk_inner_sync       — load existing .otbx; seeds
        //     checkpoint_wal_id from the caller-supplied sidecar_wal_id.
        //   bootstrap_create_disk_inner_sync — create new .otbx.
        [[nodiscard]] bool
        bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                   const std::filesystem::path& otbx_path,
                                   wal::id_t sidecar_wal_id) noexcept;

        [[nodiscard]] bool
        bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                          std::vector<components::table::column_definition_t> columns,
                                          const std::filesystem::path& otbx_path) noexcept;

        /// Read-only role/pool introspection. Not a mailbox handler.
        [[nodiscard]] agent_role_t role() const noexcept { return role_; }
        [[nodiscard]] std::size_t pool_idx() const noexcept { return pool_idx_; }

        // WAL-replay direct_* helpers: the manager-side direct_*_sync routers forward
        // here to apply the mutation against the local slice (missing OIDs no-op).
        // Bootstrap-only — base_spaces WAL replay runs synchronously before
        // scheduler.start; post-start mutations use the storage_* mailbox handlers.
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

        // storage_* mailbox handlers: the manager forwards to
        // agents_[pool_idx_for_oid(table_oid)] via send (mailbox-only; payload by
        // rvalue unique_ptr / by-value PMR containers, no shared state). A not-owned
        // OID returns nullptr and the caller surfaces an empty chunk.
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     components::catalog::oid_t table_oid,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit,
                     components::table::transaction_data txn);

        // Mutation handlers: these inner bodies are the SOLE owner of each mutation;
        // manager-side bodies are pure routers. Not-owned OIDs no-op.
        //
        // storage_append_inner — canonical append. Owns the FULL preprocessing
        //   pipeline (schema adoption/growth, column expansion, NOT NULL, dedup,
        //   type promotion), so even the preprocessing reads are mailbox-serialized
        //   with every same-oid access. Returns (start_row, count); (0,0) on no-op.
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

        // Abort-path + completion handlers (revert / update / delete / fetch).
        // Not-owned OIDs no-op (or return null for fetch).

        // storage_revert_appends_inner — batched abort. Reverse-iterates ranges to
        //   unwind in append-order opposite.
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

        // Read-path handlers (scan_batched / scan_segment / types / total_rows).
        // Not-owned OIDs return an empty/zero sentinel.
        //
        // storage_scan_batched_inner — batched + projected scan; returns a PMR vector
        //   of data_chunk_t batches (≤ DEFAULT_VECTOR_CAPACITY rows each).
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

        // storage_column_names_inner — names in storage-column order (vector index ==
        //   storage column index), so the manager can resolve name-keyed filters to
        //   indices without borrowing the agent's columns() across the boundary.
        //   Empty vector means "not owned" / record-only marker.
        unique_future<std::pmr::vector<std::string>>
        storage_column_names_inner(components::catalog::oid_t table_oid);

        // storage_total_rows_inner — row-count metadata accessor. 0 means
        //   either "not owned" or "empty twin" — both equivalent for callers.
        unique_future<uint64_t> storage_total_rows_inner(components::catalog::oid_t table_oid);

        // Fanout handlers for checkpoint_all / vacuum_all / on_horizon_advanced —
        // each agent iterates its own storages_ slice in parallel.
        //
        // checkpoint_inner — (compact + checkpoint(wal_id) + sidecar) per DISK entry.
        //   Returns min(prev_checkpoint_wal_id_) over the agent's DISK entries, or
        //   max() sentinel when it owns none. IN_MEMORY entries are skipped.
        unique_future<wal::id_t> checkpoint_inner(session_id_t session, wal::id_t current_wal_id);

        // vacuum_inner — cleanup_versions + compact per entry.
        unique_future<void> vacuum_inner(session_id_t session, uint64_t lowest_active_start_time);

        // maybe_cleanup_inner — single-OID target. If deleted/total > 0.3 and
        //   lowest_active_start_time is past TRANSACTION_ID_START, runs table.compact().
        //   cleanup_versions is intentionally omitted: scan_committed needs intact
        //   version metadata to filter tombstones, which cleanup_versions would strip
        //   before compact rebuilds the row_group.
        unique_future<void> maybe_cleanup_inner(components::catalog::oid_t table_oid,
                                                 uint64_t lowest_active_start_time);

        // on_horizon_advanced_inner — sweeps dropped_storages_, removing entries whose
        //   dropped_at_commit_id < new_horizon. Exceptions FORBIDDEN: std::error_code
        //   overloads on every filesystem::remove. Acks on_subscriber_empty(DISK_KIND)
        //   once the slice drains (gated on manager_dispatcher_addr_); the dispatcher
        //   idempotently collapses N agent acks into one disk_has_dropped_ flip.
        unique_future<void> on_horizon_advanced_inner(uint64_t new_horizon);

        // Pre-scheduler-start helper: push-back into dropped_storages_ (base_spaces
        // catalog rebuild via register_dropped_storage_sync). Not a mailbox handler.
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

        // Runtime DROP TABLE storages_ erase (sole owner of the canonical erase +
        // .otbx removal). Idempotent on a missing key; the SFBM WRITE_LOCK is
        // released exactly once when erase drops the unique_ptr.
        unique_future<void> drop_storage_inner(components::catalog::oid_t oid);

        // Physical column compaction. The mutation half
        // (collection_storage_entry_t::drop_column) is non-const — it rewrites
        // table_storage and recreates the storage adapter on the agent's arena.
        // Idempotent re-issue logs "not found"; DISK is out-of-scope
        // (table_storage_t::drop_column returns false on DISK).
        unique_future<void> drop_column_inner(components::catalog::oid_t table_oid,
                                              std::pmr::string column_name);

        // Bootstrap-only: base_spaces wires the manager_dispatcher_t address into
        // every agent before scheduler.start. on_horizon_advanced_inner uses it to
        // ack on_subscriber_empty(DISK_KIND) once dropped_storages_ drains. The
        // address is a mailbox handle (not mutable state), safe to copy. Not a
        // mailbox handler; single-threaded at the bootstrap call site.
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

        // This agent's storage slice (incomplete value type safe via the deferred
        // instantiation noted at the top of this header).
        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // Per-agent GC slice — sole owner of GC state. Populated by
        // register_dropped_storage_inner; on_horizon_advanced_inner removes entries
        // whose dropped_at_commit_id < new_horizon and acks on_subscriber_empty
        // (DISK_KIND) once it drains.
        std::pmr::vector<dropped_storage_entry_t> dropped_storages_;

        // Empty by default; the ack path in on_horizon_advanced_inner is gated on
        // != empty_address() so test fixtures without a dispatcher pass cleanly.
        actor_zeta::address_t manager_dispatcher_addr_{actor_zeta::address_t::empty_address()};
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
