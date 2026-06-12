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
#include <set>
#include <string>
#include <components/context/execution_context.hpp>
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
    // Catalog-DDL _inner handlers take the same by-value context the manager routers do.
    using execution_context_t = ::components::execution_context_t;

    class base_manager_disk_t;

    // Forward-declared (full definitions in manager_disk.hpp). agent_disk_t's slice
    // maps use these as incomplete value types — safe because the user-provided
    // destructor in agent_disk.cpp defers template instantiation past this header.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    // Plain cross-mailbox result for checkpoint_inner. Folds the IN_MEMORY-twin
    // signal (formerly read post-await via has_in_memory_inner_sync) into the
    // fan-out return so checkpoint_all needs no synchronous slice read afterwards.
    // Plain std fields only (no pmr) — safe to copy across the mailbox by value.
    //   min_prev_checkpoint_wal_id — min(prev_checkpoint_wal_id_) over this agent's
    //     DISK entries, or wal::id_t max() sentinel when it owns no DISK entry.
    //   has_in_memory — true iff this agent owns >= 1 IN_MEMORY storage entry (the
    //     same predicate has_in_memory_inner_sync computed); gates WAL-floor sealing.
    struct checkpoint_result_t {
        wal::id_t min_prev_checkpoint_wal_id;
        bool has_in_memory;
    };

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

        // storages_ slice: this agent is the SOLE owner of its DISK SFBMs; the
        // manager is a pure router.

        /// Bootstrap-only probe: does this agent own the storage for `oid`?
        /// NOT a mailbox handler — after scheduler.start, callers must go through
        /// the storage_* mailbox handlers.
        [[nodiscard]] bool has_storage_sync(components::catalog::oid_t oid) const noexcept;

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

        // Runtime CREATE mailbox handlers. The manager routers forward by oid (+ columns
        // by value / path as string) so the entry is built with the AGENT's OWN
        // resource() on the agent thread — no entry crosses the mailbox and the manager
        // touches no storage state. Each returns a plain bool: false on duplicate key,
        // mirroring the bootstrap helpers' contract. The bodies reuse the existing
        // bootstrap_*_inner_sync helpers (now called intra-actor).
        //   create_storage_inner               — IN_MEMORY schema-less entry.
        //   create_storage_with_columns_inner  — IN_MEMORY entry with columns.
        //   create_storage_disk_inner          — create_directories(parent) on the agent
        //     thread, then construct the new .otbx SFBM entry.
        unique_future<bool> create_storage_inner(components::catalog::oid_t oid);
        unique_future<bool>
        create_storage_with_columns_inner(components::catalog::oid_t oid,
                                          std::vector<components::table::column_definition_t> columns);
        unique_future<bool>
        create_storage_disk_inner(components::catalog::oid_t oid,
                                  std::vector<components::table::column_definition_t> columns,
                                  std::filesystem::path otbx_path);

        // WAL-replay direct_* helpers: the manager-side direct_*_sync routers forward
        // here to apply the mutation against the local slice (missing OIDs no-op).
        // Bootstrap-only — base_spaces WAL replay runs synchronously before
        // scheduler.start; post-start mutations use the storage_* mailbox handlers.
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

        // storage_revert_deletes_inner — MVCC delete abort. Iterates `tables`
        //   and calls revert_all_deletes(txn_id) per owned twin, un-stamping
        //   this txn's pending delete marks back to NOT_DELETED_ID.
        unique_future<void> storage_revert_deletes_inner(uint64_t txn_id,
                                                         std::pmr::vector<components::catalog::oid_t> tables);

        // Abort-path + completion handlers (revert / update / delete / fetch).
        // Not-owned OIDs no-op (or return null for fetch).

        // storage_revert_appends_inner — batched abort. Reverse-iterates ranges to
        //   unwind in append-order opposite.
        unique_future<void>
        storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges);

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

        // scan_by_keys_inner — batched keyed scan for one owned table. Resolves the
        //   key column NAMES to storage indices once, then loops the key-tuples of the
        //   columnar `keys` chunk: per row i it builds an eq-AND filter over the shared
        //   key columns (constant = keys.value(j, i)) and scans, collecting the matching
        //   row_ids. result[i] == match row_ids for key-tuple i; result has one
        //   (possibly empty) entry per key. A not-owned OID / unknown column / arity
        //   mismatch yields a same-length result of empty rows (or empty when keys is
        //   empty). The whole batch is one mailbox message so name resolution happens
        //   once and every key scan is serialized against same-oid mutations.
        unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
        scan_by_keys_inner(components::catalog::oid_t table_oid,
                           std::pmr::vector<std::string> key_col_names,
                           components::vector::data_chunk_t keys,
                           components::table::transaction_data txn);

        // read_chunks_by_key_inner — columnar row-data scan for ONE key-tuple on one owned
        //   table. `keys` is a single-row data_chunk whose column j holds key_col_names[j];
        //   the handler resolves the key column NAMES to storage indices, builds an eq-AND
        //   filter (constant = keys.value(j, 0)) and returns the matching rows as batched
        //   data_chunk_t (<= DEFAULT_VECTOR_CAPACITY rows each), all columns, no row limit.
        //   The filter constant is a logical_value_t — the irreducible filter-API floor,
        //   same as scan_by_keys_inner; it never crosses the mailbox. A not-owned OID /
        //   record-only marker / unknown column / empty keys yields an empty vector.
        unique_future<std::pmr::vector<components::vector::data_chunk_t>>
        read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::string> key_col_names,
                                 components::vector::data_chunk_t keys,
                                 components::table::transaction_data txn);

        // read_chunks_by_keys_inner — batched multi-key columnar row-data scan for one owned
        //   table. `keys` is an N-row data_chunk whose column j holds key_col_names[j] and whose
        //   row i is the i-th key-tuple. The handler resolves the key column NAMES to storage
        //   indices ONCE, then for each key row builds an eq-AND filter (constant = keys.value(j, i))
        //   and scans, returning the matching rows as batched data_chunk_t (all columns, no row
        //   limit). result[i] == matched chunks for key-tuple i; the outer vector always has one
        //   (possibly empty) entry per key in input order, so result.size() == keys.size() on
        //   EVERY path — mirroring scan_by_keys_inner. A not-owned OID / record-only marker /
        //   unknown column / arity mismatch yields a same-length vector of empty entries (or empty
        //   when keys is empty). The filter constant is a logical_value_t — the irreducible
        //   filter-API floor, same as read_chunks_by_key_inner; it never crosses the mailbox.
        unique_future<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                  std::pmr::vector<std::string> key_col_names,
                                  components::vector::data_chunk_t keys,
                                  components::table::transaction_data txn);

        // storage_types_inner — schema metadata accessor.
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types_inner(components::catalog::oid_t table_oid);

        // storage_total_rows_inner — row-count metadata accessor. 0 means
        //   either "not owned" or "empty twin" — both equivalent for callers.
        unique_future<uint64_t> storage_total_rows_inner(components::catalog::oid_t table_oid);

        // Fanout handlers for checkpoint_all / vacuum_all / on_horizon_advanced —
        // each agent iterates its own storages_ slice in parallel.
        //
        // checkpoint_inner — (compact + checkpoint(wal_id) + sidecar) per DISK entry.
        //   Returns a checkpoint_result_t whose min_prev_checkpoint_wal_id is
        //   min(prev_checkpoint_wal_id_) over the agent's DISK entries (max() sentinel
        //   when it owns none, IN_MEMORY entries skipped) and whose has_in_memory flags
        //   whether this agent owns >= 1 IN_MEMORY entry — folding the signal
        //   checkpoint_all formerly read via has_in_memory_inner_sync into the fan-out.
        //   compact_watermark is the dispatcher's visible-to-all horizon
        //   (txn_compact_watermark_msg); compact() refuses the rebuild when any
        //   version stamp is above it, and the entry's checkpoint is then SKIPPED
        //   for this round — the .otbx format has no version metadata, so
        //   persisting a non-compacted table would resurrect dead/uncommitted
        //   rows on recovery. The skipped entry keeps its old file/sidecar and
        //   still feeds prev_checkpoint_wal_id into the min() so the WAL keeps
        //   every record it needs for replay.
        unique_future<checkpoint_result_t>
        checkpoint_inner(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);

        // vacuum_inner — cleanup_versions + compact per entry. compact_watermark:
        //   same visible-to-all horizon contract as checkpoint_inner.
        unique_future<void>
        vacuum_inner(session_id_t session, uint64_t lowest_active_start_time, uint64_t compact_watermark);

        // maybe_cleanup_inner — single-OID target. If deleted/total > 0.3, runs
        //   table.compact(compact_watermark). compact_watermark is the dispatcher's
        //   visible-to-all horizon (txn_publish_msg return) forwarded verbatim;
        //   compact() itself refuses the rebuild when any version stamp is above
        //   it (active snapshot / in-flight commit still needs the history).
        //   The agent mailbox serializing the row_groups_ swap covers the data
        //   race side; the watermark covers version-history visibility.
        //   cleanup_versions is intentionally omitted: scan_committed
        //   needs intact version metadata to filter tombstones, which cleanup_versions
        //   would strip before compact rebuilds the row_group.
        unique_future<void> maybe_cleanup_inner(components::catalog::oid_t table_oid, uint64_t compact_watermark);

        // on_horizon_advanced_inner — sweeps dropped_storages_, removing entries whose
        //   dropped_at_commit_id < new_horizon. Exceptions FORBIDDEN: std::error_code
        //   overloads on every filesystem::remove. Acks on_subscriber_empty(DISK_KIND)
        //   once the slice drains (gated on manager_dispatcher_addr_); the dispatcher
        //   idempotently collapses N agent acks into one disk_has_dropped_ flip.
        unique_future<void> on_horizon_advanced_inner(uint64_t new_horizon);

        // storage_dropped_committed_inner — DROP-GC value-space remap. A GC entry
        //   recorded by register_dropped_storage_inner_sync carries dropped_at_commit_id
        //   in TXN-ID space (>= 2^62) because the cascade-delete operator only knew
        //   the in-flight txn_id. on_horizon_advanced_inner compares against a
        //   commit-id horizon, so the TXN-ID placeholder would never be reclaimed.
        //   Once the transaction commits, manager_disk fans this out to every agent;
        //   each rewrites its own dropped_storages_ entries whose dropped_at_commit_id
        //   equals txn_id to the real commit_id, moving them into commit-id space.
        unique_future<void> storage_dropped_committed_inner(uint64_t txn_id, uint64_t commit_id);

        // storage_drop_aborted_inner — DROP-rollback un-mark. The abort mirror of
        //   storage_dropped_committed_inner: instead of remapping a GC entry's
        //   dropped_at_commit_id into commit-id space, it ERASES every
        //   dropped_storages_ entry whose dropped_at_commit_id == txn_id. A DROP
        //   TABLE inside a transaction records its GC entry in TXN-ID space via
        //   register_dropped_storage_inner_sync; if the transaction ABORTS the table must
        //   survive, so manager_disk fans this out to every agent and each removes
        //   the matching entries so on_horizon_advanced never reclaims the live .otbx.
        unique_future<void> storage_drop_aborted_inner(uint64_t txn_id);

        // GC-slice push-back into dropped_storages_. Not a mailbox handler. Called
        // pre-scheduler-start by base_spaces catalog rebuild and at runtime by
        // mark_storage_dropped_many_inner (single-threaded on the agent at both sites).
        void register_dropped_storage_inner_sync(components::catalog::oid_t oid,
                                                  uint64_t dropped_at_commit_id,
                                                  std::filesystem::path path,
                                                  std::pmr::vector<std::filesystem::path> sidecar_paths);

        // Batched DROP: one message per agent, looping the canonical singular erase
        // over this agent's oid slice (manager partitioned by pool_idx_for_oid). Each
        // oid is idempotent on a missing key (over-routed oid = no-op).
        unique_future<void> drop_storage_many_inner(std::pmr::vector<components::catalog::oid_t> oids);

        // Catalog DDL handlers (Track A): the manager-side append_pg_catalog_row /
        // delete_pg_catalog_rows / update_pg_attribute_commit_id_fields /
        // compact_relkind_g_storage / mark_storage_dropped_many bodies move HERE so the
        // catalog scan + mutation run on this (agent-0 / CATALOG) thread instead of
        // the manager loop borrowing the agent's slice. All catalog OIDs route to
        // agents_[0] via pool_idx_for_oid. WAL is written via manager_wal_addr_
        // (empty in WAL-disabled fixtures, guarded). Not-owned OIDs no-op.
        //
        // append_pg_catalog_row_inner — crash-safe single-row append: WAL physical_insert
        //   first (so a crash before storage update can be replayed), then append on this
        //   agent's own slice. Returns (table_oid, start_row, count); count is 0 when
        //   txn.transaction_id == 0 or the append wrote nothing.
        unique_future<components::pg_catalog_append_range_t>
        append_pg_catalog_row_inner(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    components::vector::data_chunk_t row);

        // delete_pg_catalog_rows_inner — scan this agent's slice for rows whose
        //   column[oid_col_idx] == target_oid, WAL physical_delete, then delete via the
        //   agent's own direct_delete_sync. No-op if not owned or no match.
        unique_future<void> delete_pg_catalog_rows_inner(execution_context_t ctx,
                                                         components::catalog::oid_t table_oid,
                                                         std::int64_t oid_col_idx,
                                                         components::catalog::oid_t target_oid);

        // update_pg_attribute_commit_id_field_inner — patch the pg_attribute row keyed
        //   by attoid: read the full row, mutate col 10 (added_at) or 11 (dropped_at) to
        //   commit_id, WAL physical_update full-width, then write back via the agent's
        //   own direct_update_sync.
        unique_future<void>
        update_pg_attribute_commit_id_field_inner(execution_context_t ctx,
                                                  components::catalog::oid_t attoid,
                                                  components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                                  std::uint64_t commit_id);

        // compact_relkind_g_storage_inner — whole-op intra-agent: read own slice
        //   (mode + columns), compute the columns NOT in live_attnames, drop each via
        //   entry->drop_column on its own slice, return the dropped count. DISK-mode /
        //   missing / already-compact returns 0.
        unique_future<std::uint64_t> compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                                     std::set<std::string> live_attnames);

        // mark_storage_dropped_many_inner — batched DROP-mark: one message per agent
        //   carries that agent's whole oid slice (manager partitioned by pool_idx_for_oid)
        //   plus the shared dropped_at_commit_id. Loops the canonical per-oid mark body
        //   (mark_storage_dropped_one_local) over the slice. Each oid reads its otbx_path
        //   + derives .wal_id/.prev sidecars from this agent's own slice, then records the
        //   GC entry via register_dropped_storage_inner_sync. IN_MEMORY storages leave the
        //   path empty (uniform GC bookkeeping, no-op sweep). Over-routed oids no-op.
        unique_future<void>
        mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                        uint64_t dropped_at_commit_id);

        // Bootstrap-only: base_spaces wires the manager_dispatcher_t address into
        // every agent before scheduler.start. on_horizon_advanced_inner uses it to
        // ack on_subscriber_empty(DISK_KIND) once dropped_storages_ drains. The
        // address is a mailbox handle (not mutable state), safe to copy. Not a
        // mailbox handler; single-threaded at the bootstrap call site.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap-only: base_spaces wires the WAL manager's address into every agent
        // (via manager_disk_t::sync fan-out) before scheduler.start. The CATALOG agent
        // (agent 0) uses it to write physical WAL records for catalog DDL directly
        // (append/delete/update pg_* rows), so that work runs on the agent thread instead
        // of the manager loop. A mailbox handle (not mutable state), safe to copy. Not a
        // mailbox handler; single-threaded at the bootstrap call site.
        void set_manager_wal_sync(actor_zeta::address_t address);

        using dispatch_traits = actor_zeta::dispatch_traits<&agent_disk_t::fix_wal_id,
                                                            &agent_disk_t::storage_scan,
                                                            &agent_disk_t::storage_append_inner,
                                                            &agent_disk_t::storage_publish_commits_inner,
                                                            &agent_disk_t::storage_publish_deletes_inner,
                                                            &agent_disk_t::storage_revert_deletes_inner,
                                                            &agent_disk_t::storage_revert_appends_inner,
                                                            &agent_disk_t::storage_update_inner,
                                                            &agent_disk_t::storage_delete_rows_inner,
                                                            &agent_disk_t::storage_fetch_inner,
                                                            &agent_disk_t::storage_scan_batched_inner,
                                                            &agent_disk_t::storage_scan_segment_inner,
                                                            &agent_disk_t::scan_by_keys_inner,
                                                            &agent_disk_t::read_chunks_by_key_inner,
                                                            &agent_disk_t::read_chunks_by_keys_inner,
                                                            &agent_disk_t::storage_types_inner,
                                                            &agent_disk_t::storage_total_rows_inner,
                                                            &agent_disk_t::checkpoint_inner,
                                                            &agent_disk_t::vacuum_inner,
                                                            &agent_disk_t::maybe_cleanup_inner,
                                                            &agent_disk_t::on_horizon_advanced_inner,
                                                            &agent_disk_t::storage_dropped_committed_inner,
                                                            &agent_disk_t::storage_drop_aborted_inner,
                                                            &agent_disk_t::drop_storage_many_inner,
                                                            &agent_disk_t::append_pg_catalog_row_inner,
                                                            &agent_disk_t::delete_pg_catalog_rows_inner,
                                                            &agent_disk_t::update_pg_attribute_commit_id_field_inner,
                                                            &agent_disk_t::compact_relkind_g_storage_inner,
                                                            &agent_disk_t::mark_storage_dropped_many_inner,
                                                            &agent_disk_t::create_storage_inner,
                                                            &agent_disk_t::create_storage_with_columns_inner,
                                                            &agent_disk_t::create_storage_disk_inner>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // Non-mailbox committed-scan over an OWNED slice entry (D6: callers on the agent
        // thread — storage_scan_batched_inner and read_chunks_by_keys_inner — read their own
        // slice directly here, never by self-sending a mailbox message). Returns empty when
        // the oid isn't owned or is a record-only marker. `filter` may be nullptr;
        // `projected_cols` may be nullptr for all columns.
        std::pmr::vector<components::vector::data_chunk_t>
        scan_batched_local(components::catalog::oid_t table_oid,
                           components::table::table_filter_t* filter,
                           int64_t limit,
                           const std::vector<std::size_t>* projected_cols,
                           const components::table::transaction_data& txn);

        // Canonical single-oid erase + .otbx removal, used by
        // drop_storage_many_inner. Synchronous; agent-thread callers only.
        void drop_storage_one_local(components::catalog::oid_t oid);

        // Canonical single-oid DROP-mark: read otbx_path + derive .wal_id/.prev
        // sidecars from this agent's own slice, then record the GC entry via
        // register_dropped_storage_inner_sync. Used by mark_storage_dropped_many_inner,
        // which loops it over its oid slice. Synchronous; agent-thread callers only.
        void mark_storage_dropped_one_local(components::catalog::oid_t table_oid,
                                            uint64_t dropped_at_commit_id);

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
        // register_dropped_storage_inner_sync; on_horizon_advanced_inner removes entries
        // whose dropped_at_commit_id < new_horizon and acks on_subscriber_empty
        // (DISK_KIND) once it drains.
        std::pmr::vector<dropped_storage_entry_t> dropped_storages_;

        // Empty by default; the ack path in on_horizon_advanced_inner is gated on
        // != empty_address() so test fixtures without a dispatcher pass cleanly.
        actor_zeta::address_t manager_dispatcher_addr_{actor_zeta::address_t::empty_address()};

        // WAL manager address for CATALOG-agent DDL (set via set_manager_wal_sync at
        // bootstrap). Empty by default so WAL-disabled fixtures skip the WAL write.
        actor_zeta::address_t manager_wal_addr_{actor_zeta::address_t::empty_address()};
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
