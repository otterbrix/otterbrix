#pragma once

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include "disk_contract.hpp" // fetch_batch_t reply payload for storage_fetch_next_batch_inner
#include <atomic>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/log/log.hpp>
#include <components/storage/storage.hpp> // scan_position_t for the index-resume active_scan_t
#include <components/table/data_table.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/timezones.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <set>
#include <string>
#include <unordered_map>

namespace services::disk {

#ifdef DEV_MODE
    // Test-observable count of checkpoint ROUNDS (not tables) reaching this agent: a round
    // rewrites every disk table in the slice whole, so cost is O(data) per round.
    uint64_t table_checkpoints() noexcept;
    void reset_table_checkpoints() noexcept;

    // Test-observable count of publish/revert legs that found no storage on the routed
    // (owning) agent for an oid — always a missed visibility flip/unwind, never a
    // misroute (pool_idx_for_oid already picked the owner). Each miss also logs at error.
    uint64_t publish_revert_misses() noexcept;
    void reset_publish_revert_misses() noexcept;

    // Test-observable checkpoint-round tallies: entries deferred vs. rewritten (see
    // checkpoint_result_t). reset together.
    uint64_t checkpoint_entries_deferred() noexcept;
    uint64_t checkpoint_entries_rewritten() noexcept;
    void reset_checkpoint_entry_tallies() noexcept;

    // Deterministic BETWEEN-BATCHES pause for streaming scans — the interleaving seam of the
    // cursor-vs-concurrent-DDL tests. The window where an open cursor meets a schema change
    // only exists between two fetches of the same cursor, and a timing-based repro hits it in
    // a fraction of runs; this gate turns the window into a held door.
    //
    // Consulted by the streaming scan sources (full_scan / transfer_scan source_next)
    // immediately before every cursor ADVANCE — never before OPEN, so the first batch always
    // flows. While hold() answers true the source performs one cross-actor no-op round-trip
    // and asks again: the await parks the source's nested coroutine, so no actor thread ever
    // blocks and every mailbox — executor, disk manager, the cursor's owner agent — stays
    // free to run whatever statement the test slides into the window. A blocking wait anywhere
    // on an actor thread could not work at all: the agent serving the cursor is by oid routing
    // the SAME agent a DDL's physical half lands on, so holding its mailbox would deadlock the
    // very statement the test is interleaving.
    //
    // Plain virtual interface like the other dev seams (no std::function); process-wide,
    // DEV_MODE-only (release builds compile the consultation out), one nullptr load per
    // ADVANCE while unarmed.
    struct scan_advance_gate_t {
        virtual ~scan_advance_gate_t() = default;
        // true = keep holding this scan between batches; false = let the next fetch go.
        virtual bool hold(components::catalog::oid_t table_oid, uint64_t cursor_id) = 0;
    };
    void dev_set_scan_advance_gate(scan_advance_gate_t* gate); // nullptr = off
    scan_advance_gate_t* dev_scan_advance_gate();
#endif

    using path_t = std::filesystem::path;

    using session_id_t = ::components::session::session_id_t;
    // Catalog-DDL _inner handlers take the same by-value context the manager routers do.
    using execution_context_t = ::components::execution_context_t;

    // Test-observable row count shipped back by storage_reduce_inner (rows crossing the
    // agent->coordinator mailbox) — proves aggregate pushdown ships only the finalized rows,
    // not the raw scan. DEV_MODE-only, mirrors
    // services::collection::executor::dml_flush_count().
#ifdef DEV_MODE
    uint64_t pushdown_reply_rows() noexcept;
    void reset_pushdown_reply_rows() noexcept;

    // Test-observable scan count for read_chunks_by_keys_inner: one bump per full table pass,
    // independent of how many key tuples are batched into it.
    uint64_t catalog_key_scans() noexcept;
    void reset_catalog_key_scans() noexcept;
#endif

    // Forward-declared (full definitions in manager_disk.hpp). agent_disk_t's slice
    // maps use these as incomplete value types — safe because the user-provided
    // destructor in agent_disk.cpp defers template instantiation past this header.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    // Streaming single-pass hash semi-join used by scan_by_keys_inner. result[i] = row_ids
    // matching input key-tuple i (input order, empty if none). Streams `storage` exactly ONCE
    // regardless of key count — O(table_rows + nkeys), not O(nkeys * table_rows). Exposed at
    // namespace scope for tests. A scan failure is returned, not swallowed: a partial result
    // would read to FK/UNIQUE checks as "these keys matched nothing".
    core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    fk_hash_semijoin(std::pmr::memory_resource* resource,
                     components::storage::storage_t& storage,
                     const std::pmr::vector<std::uint64_t>& key_col_indices,
                     components::vector::data_chunk_t& keys,
                     components::table::transaction_data txn);

    // Plain cross-mailbox result for checkpoint_inner (std fields only, no pmr — safe to copy
    // by value). min_prev_checkpoint_wal_id = min(prev_checkpoint_wal_id_) over this agent's
    // entries, or wal::id_t max() when it owns none. No IN_MEMORY-suppression flag any more:
    // every table is now a file.
    struct checkpoint_result_t {
        wal::id_t min_prev_checkpoint_wal_id;
        // Per-entry tallies distinguishing "checkpointed" from "deferred" (both return a
        // floor). deferred = degraded/cursor/MVCC/failed gate; rewritten = new root committed;
        // advanced = unchanged entry, wal-id chain advanced without a rewrite.
        uint64_t deferred{0};
        uint64_t rewritten{0};
        uint64_t advanced{0};
    };

    /// Agent role / storages_ partition. agent 0 = CATALOG (pg_* tables + oid_gen_ +
    /// stored_catalog_); agents 1..N-1 = USER_POOL (user tables hashed
    /// by table_oid). MUST align with manager_disk_t::pool_idx_for_oid: idx 0 ↔ CATALOG.
    enum class agent_role_t : std::uint8_t
    {
        CATALOG = 0,  // agent 0: pg_* system tables + oid_gen_ + stored_catalog_
        USER_POOL = 1 // agents 1..N-1: user tables routed by oid hash
    };

    class agent_disk_t final : public actor_zeta::basic_actor<agent_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        /// Default-constructed agent: CATALOG role, pool_idx = 0.
        agent_disk_t(std::pmr::memory_resource* resource, const path_t& path_db, log_t& log);

        /// Role-aware constructor. agent 0 = CATALOG; agents 1..N-1 = USER_POOL
        /// with their respective pool_idx (matches pool_idx_for_oid contract).
        agent_disk_t(std::pmr::memory_resource* resource,
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

        // compact() gate: an open cursor holds an ABSOLUTE row position into the un-swapped
        // collection, so compact()'s row_groups_ swap would shift rows under it; checkpoint_inner
        // defers such oids. Public: manager_disk_t routes an observability probe here.
        [[nodiscard]] bool has_active_scan_for_oid(components::catalog::oid_t oid) const noexcept {
            for (const auto& [_cursor, scan] : active_scans_) {
                if (scan.table_oid == oid) {
                    return true;
                }
            }
            return false;
        }

        // Const raw-pointer accessor into the storages_ slice; nullptr when the OID
        // isn't owned. The unique_ptr gives the entry a stable address and the agent
        // mailbox serializes all writes to storages_, so a sync read is race-free
        // while the agent thread is idle. Callers MUST treat the pointer as borrowed:
        // do NOT store it across a mailbox-yield, do NOT delete it. Not a mailbox
        // handler — safe from the manager thread pre-start or inside a manager
        // mailbox handler post-start.
        [[nodiscard]] const collection_storage_entry_t*
        storage_entry_sync(components::catalog::oid_t oid) const noexcept;

        // Ownership ctors: build the SFBM-holding entry on the agent thread (holds the .otbx
        // WRITE_LOCK) — bootstrap-only, before any concurrent emplace for the OID can race.
        //   bootstrap_disk_inner_sync — `sidecar_readable=false` marks the floor UNREADABLE
        //     instead of 0, because 0 already means "never checkpointed, replay everything" (see
        //     table_storage_t::checkpoint_wal_id_known()).
        //   bootstrap_create_disk_inner_sync — `is_computed` (relkind='g') is resolved by the
        //     caller, never inferred from an empty column set.
        [[nodiscard]] bool
        bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                  const std::filesystem::path& otbx_path,
                                  wal::id_t sidecar_wal_id,
                                  bool sidecar_readable,
                                  std::vector<components::table::column_definition_t> catalog_columns,
                                  bool is_computed) noexcept;

        [[nodiscard]] bool bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                                            std::vector<components::table::column_definition_t> columns,
                                                            const std::filesystem::path& otbx_path,
                                                            bool is_computed) noexcept;

        // Runtime CREATE handler: entry built with the agent's OWN resource() on the agent
        // thread, so nothing crosses the mailbox. Returns false on duplicate key.
        //   create_storage_disk_inner — create_directories(parent), then construct the .otbx
        //     SFBM entry.
        unique_future<bool> create_storage_disk_inner(components::catalog::oid_t oid,
                                                      std::vector<components::table::column_definition_t> columns,
                                                      std::filesystem::path otbx_path,
                                                      bool is_computed);

        // WAL-replay direct_* helpers: apply the mutation against the local slice. Bootstrap-only
        // — replay runs before scheduler.start; post-start mutations use the storage_* handlers.
        // A missing storage is a REFUSAL (core::error_t), not a no-op: the agent is chosen by
        // pool_idx_for_oid before forwarding, so an absent entry means the owner has no storage,
        // and a dropped replay mutation is a journalled change recovery silently skipped.
        [[nodiscard]] core::error_t no_replay_storage_error(const char* who, components::catalog::oid_t table_oid);
        [[nodiscard]] core::error_t direct_delete_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       uint64_t count,
                                                       const components::table::transaction_data& txn);
        [[nodiscard]] core::error_t direct_update_sync(components::catalog::oid_t table_oid,
                                                       const std::pmr::vector<int64_t>& row_ids,
                                                       components::vector::data_chunk_t& new_data);
        // WAL-replay of PHYSICAL_ADD_COLUMN: re-apply the schema columns carried by
        // `schema_chunk` (0-row; column j's alias-tagged type IS new column j) to the
        // local slice ahead of the dependent PHYSICAL_INSERT. Idempotent by column name.
        [[nodiscard]] core::error_t direct_add_column_sync(components::catalog::oid_t table_oid,
                                                           const components::vector::data_chunk_t& schema_chunk);

        // Mutation handlers: sole owner of each mutation; manager-side bodies are pure routers.
        // A not-owned oid is a REFUSAL on every leg below (see the direct_* note above), not a
        // no-op — only an empty request (no rows asked/written) is a plain success.
        // storage_append_inner — canonical WAL-FIRST append: allocates start_row, writes WAL
        //   (PHYSICAL_ADD_COLUMN then PHYSICAL_INSERT), THEN materializes — atomic within one
        //   mailbox handler. Returns (start_row, count); (0,0) only for an EMPTY chunk, an error
        //   otherwise (keeps a routing refusal from looking like a zero-length empty batch).
        unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append_inner(execution_context_t ctx,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::vector::data_chunk_t> data);

        // storage_publish_commits_inner — MVCC visibility flip per range. A range whose oid
        //   this agent has no storage for reports loudly (report_publish_revert_miss in the .cpp).
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

        // Abort-path + completion handlers (revert / update / delete / fetch). A missing storage
        // on the routed owner is loud everywhere: wrapped legs refuse, void legs report per-oid.

        // storage_revert_appends_inner — batched abort. Reverse-iterates ranges to
        //   unwind in append-order opposite.
        unique_future<void>
        storage_revert_appends_inner(std::pmr::vector<components::pg_catalog_append_range_t> ranges);

        // storage_update_inner — single-OID UPDATE mutation against the
        //   agent twin. Reply wraps storage_t::update's (updated, appended) pair so a
        //   write_conflict / out_of_memory travels back to operator_update as a value;
        //   (0, 0) for an EMPTY chunk, an error when this agent has no storage to update.
        unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update_inner(components::catalog::oid_t table_oid,
                             components::vector::vector_t row_ids,
                             std::unique_ptr<components::vector::data_chunk_t> data,
                             components::table::transaction_data txn);

        // storage_delete_rows_inner — single-OID DELETE mutation. Wrapper carries the deleted
        //   count on success, an error when this agent can't delete at all (no oid / no storage).
        //   A count below the requested one is NOT a refusal — an already-stamped row is skipped
        //   by design (chunk_vector_info::delete_rows).
        unique_future<core::result_wrapper_t<uint64_t>>
        storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                  components::vector::vector_t row_ids,
                                  uint64_t count,
                                  components::table::transaction_data txn);

        // storage_fetch_inner — read-path mirror for point-fetches by row_id. An oid this agent
        //   has no storage for is an ERROR (an empty chunk vector is what an all-invisible fetch
        //   legitimately returns too). Under SNAPSHOT, invisible rows are dropped so a chunk can
        //   be SHORTER than its window; RAW keeps every row regardless (CREATE INDEX backfill
        //   reads deleted rows on purpose). `limit` caps AFTER the visibility drop, not on the id
        //   count — applying it to ids would spend budget on rows the reader never gets.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch_inner(components::catalog::oid_t table_oid,
                            components::vector::vector_t row_ids,
                            uint64_t count,
                            std::vector<size_t> projected_cols,
                            components::table::transaction_data txn,
                            components::table::fetch_visibility_t visibility,
                            int64_t limit);

        // Read-path handlers (scan_batched / fetch_next_batch / types / total_rows). Not-owned
        // OIDs refuse; see the note above the mutation handlers.
        // storage_scan_inner — batched + projected scan; the reply wraps a PMR vector of
        //   data_chunk_t batches (≤ DEFAULT_VECTOR_CAPACITY rows each), carrying any
        //   buffer-pool OOM / data_corruption the table-layer scan left in scan_error as a
        //   value (no throw across the mailbox).
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_scan_inner(components::catalog::oid_t table_oid,
                           std::unique_ptr<components::table::table_filter_t> filter,
                           int64_t limit,
                           std::vector<size_t> projected_cols,
                           components::table::transaction_data txn);

        // storage_fetch_next_batch_inner — streaming fetch-next scan source. POSITION-ONLY
        //   index-resume: the cursor stores only the absolute resume position + scan params;
        //   each fetch re-seeks a TRANSIENT scan state, reads ONE batch, releases pins — peak
        //   memory is one batch. cursor_id==0 -> OPEN (mint cursor, first batch); cursor_id!=0
        //   -> ADVANCE. A drained cursor (exhausted / limit reached) erases the entry and replies
        //   an EMPTY chunk + cursor_id. An OPEN on a not-owned oid REFUSES — a drained-looking
        //   reply here would read as "this table is empty" for a scan that never ran. An unknown
        //   cursor on ADVANCE still replies drained (the drain path already erased it).
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch_inner(session_id_t session,
                                       components::catalog::oid_t table_oid,
                                       uint64_t cursor_id,
                                       std::unique_ptr<components::table::table_filter_t> filter,
                                       int64_t limit,
                                       std::vector<size_t> projected_cols,
                                       components::table::transaction_data txn);

        // storage_close_cursor_inner — release a fetch-next cursor abandoned before it drained;
        // also lifts the compact() gate on its oid. Idempotent (unknown id = no-op).
        unique_future<void> storage_close_cursor_inner(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       uint64_t cursor_id);

        // storage_open_scan_hold_inner — mint a position-less active_scans_ entry so
        //   checkpoint_inner defers compact() on this oid while a reader's absolute row ids are
        //   in flight between actors (see disk_contract::storage_open_scan_hold). Released by
        //   storage_close_cursor_inner. A not-owned oid REFUSES — a granted hold on nothing
        //   would read as protection.
        unique_future<core::result_wrapper_t<uint64_t>>
        storage_open_scan_hold_inner(session_id_t session, components::catalog::oid_t table_oid);

        // storage_reduce_inner — aggregate-pushdown REDUCE: runs GROUP BY over this agent's OWN
        //   slice, replies ALL final aggregated rows in ONE reply (bounded by #groups, no
        //   cursor). A not-owned/record-only oid REFUSES rather than reducing over empty input —
        //   a scalar aggregate's empty-input row ("COUNT=0") would masquerade as a real answer.
        //   An empty OWNED slice still folds to that row correctly.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce_inner(session_id_t session,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::table::table_filter_t> filter,
                             std::vector<size_t> projected_cols,
                             components::table::transaction_data txn,
                             components::operators::pushed_aggregate_spec_t spec);

        // scan_by_keys_inner — batched keyed scan for one owned table: resolves key column NAMES
        //   once, then fk_hash_semijoin streams the table ONCE, O(table_rows + nkeys). result[i]
        //   = matches for key-tuple i. A not-owned OID / bad column / arity mismatch is a
        //   core::error_t, never an "all-empty result" — that reads as "nothing references this
        //   key", which would let ON DELETE CASCADE drop a parent whose children stay.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys_inner(components::catalog::oid_t table_oid,
                           std::pmr::vector<std::string> key_col_names,
                           components::vector::data_chunk_t keys,
                           components::table::transaction_data txn);

        // read_chunks_by_key_inner — columnar scan for ONE key-tuple: eq-AND filter over the
        //   resolved key columns, all columns, no row limit. A read that cannot be performed
        //   (bad oid/column/empty keys) is a core::error_t, never an empty result — which means
        //   "matched nothing".
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::uint64_t> key_col_indices,
                                 components::vector::data_chunk_t keys,
                                 std::pmr::vector<std::uint64_t> projected_cols,
                                 components::table::transaction_data txn);

        // read_chunks_by_keys_inner — batched twin of read_chunks_by_key_inner, one eq-AND scan
        //   per key row. result.size() == keys.size() on EVERY path (one entry per key, in
        //   order); a read that cannot be performed is a core::error_t, so an empty entry means
        //   only "this key matched nothing".
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                  std::pmr::vector<std::uint64_t> key_col_indices,
                                  components::vector::data_chunk_t keys,
                                  std::pmr::vector<std::uint64_t> projected_cols,
                                  components::table::transaction_data txn);

        // storage_types_inner — schema metadata accessor. No storage for the oid is an error:
        //   an empty type list is a legitimate "no schema adopted" answer too.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types_inner(components::catalog::oid_t table_oid);

        // storage_total_rows_inner — row-count accessor. 0 means "no rows"; "no storage for the
        //   oid" travels the wrapper instead, since callers can't tell the two apart otherwise.
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows_inner(components::catalog::oid_t table_oid);

        // Fanout handlers for checkpoint_all / vacuum_all / on_horizon_advanced — each agent
        // iterates its own storages_ slice in parallel.
        // checkpoint_inner — compact + checkpoint(wal_id) + sidecar, per entry. Returns
        //   min(prev_checkpoint_wal_id_) over the agent's entries (max() sentinel if none).
        //   compact_watermark gates compact(): a version stamp above it skips the entry's
        //   checkpoint this round (.otbx carries no version metadata, so persisting a
        //   non-compacted table would resurrect dead/uncommitted rows on recovery); the skipped
        //   entry keeps its old file/sidecar and still feeds the min().
        unique_future<checkpoint_result_t>
        checkpoint_inner(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark);

        // vacuum_inner — cleanup_versions per entry; does NOT compact (a compact without a
        //   committed checkpoint header cannot return space under the split free pool, only
        //   spend it — see the long note at maybe_cleanup_inner's definition). Moves no row, so
        //   a caller owes no index rebuild: only data_table_t::compact() renumbers, and that
        //   belongs to the checkpoint round.
        unique_future<void> vacuum_inner(session_id_t session, uint64_t lowest_active_start_time);

        // maybe_cleanup_inner — single-OID target. Compacts NOTHING (see vacuum_inner); kept as
        //   a handler because operator_commit_transaction still sends it per touched oid.
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

        // Catalog DDL handlers (Track A): append_pg_catalog_row / delete_pg_catalog_rows /
        // update_pg_attribute_commit_id_fields / compact_relkind_g_storage /
        // mark_storage_dropped_many bodies live HERE so catalog scan+mutation run on the CATALOG
        // (agent-0) thread. WAL via manager_wal_addr_ (empty when WAL-disabled).
        // append_pg_catalog_row_inner — crash-safe single-row append: WAL physical_insert first,
        //   then append on this agent's slice. Returns (table_oid, start_row, count) or why
        //   nothing was written — the error channel matters because a zero-count range reads as
        //   a no-op everywhere, so CREATE TABLE would report success over a catalog write that
        //   never happened.
        unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row_inner(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    components::vector::data_chunk_t row);

        // delete_pg_catalog_rows_inner — scan for column[oid_col_idx] == target_oid, WAL
        //   physical_delete, then direct_delete_sync. Returns the deleted count or why it
        //   deleted none; a refused journal record also stops the delete (storage must not move
        //   ahead of a journal with no record to replay it from). The scan runs under ctx.txn so
        //   "the caller read this row" and "this body can see it" are the same claim.
        unique_future<core::result_wrapper_t<std::uint64_t>>
        delete_pg_catalog_rows_inner(execution_context_t ctx,
                                     components::catalog::oid_t table_oid,
                                     std::int64_t oid_col_idx,
                                     components::catalog::oid_t target_oid);

        // update_pg_attribute_commit_id_field_inner — patch pg_attribute's added_at/dropped_at
        //   (col 10/11) for `attoid`: WAL physical_update full-width, then direct_update_sync.
        //   Returns why it could not patch, if it could not; a refused journal record also stops
        //   the storage patch. Runs at STEP 4 of operator_commit_transaction_t, BELOW the durable
        //   commit marker — the caller reports the error, it does NOT un-commit.
        unique_future<core::error_t>
        update_pg_attribute_commit_id_field_inner(execution_context_t ctx,
                                                  components::catalog::oid_t attoid,
                                                  components::pg_attribute_commit_id_backfill_t::kind_t kind,
                                                  std::uint64_t commit_id);

        // compact_relkind_g_storage_inner — whole-op intra-agent: read own slice, compute the
        //   columns NOT in live_attnames, drop each via entry->drop_column on its own slice,
        //   return the dropped count. Missing / already-compact returns 0. There is no gate
        //   refusing file-backed tables here; the note at the definition says why acting is the
        //   safe reading and what a refusal would cost.
        unique_future<std::uint64_t> compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                                     std::set<std::string> live_attnames);

        // drop_storage_column_inner: release the ONE column `attname` from this
        //   agent's own slice for `table_oid`, via the same entry->drop_column primitive the
        //   compact leg above calls. Unlike the subtractive VACUUM leg this caller NAMES its
        //   column, so there is no live set to re-derive and no gap in that derivation to turn
        //   into a drop of a surviving one. Both share the same split: the rebuild now, the block
        //   release at the next checkpoint.
        //   true  = the column was in the schema and is gone;
        //   false = the storage exists but never carried it (ALTER ADD COLUMN never touches
        //           storage), so there is nothing physical to release;
        //   error = no materialized storage for the oid here — see disk_contract.hpp.
        unique_future<core::result_wrapper_t<bool>> drop_storage_column_inner(components::catalog::oid_t table_oid,
                                                                             std::string attname);

        // rename_storage_column_inner: rename ONE column of this agent's slice for `table_oid`.
        //   The physical half of ALTER TABLE RENAME COLUMN. It keeps the storage's cached name
        //   in step with the catalog's from the moment of the commit, which is what the append
        //   path's column expansion and drop_column (both name-addressed) need. It is no longer
        //   an invariant the bootstrap reconciliation depends on: that walk compares
        //   pg_attribute.attoid and repairs a stale storage name from the catalog.
        //   true  = renamed;
        //   false = the storage exists but never carried `old_attname` (an ALTER ADD COLUMN
        //           that no INSERT has materialized yet is legitimately nothing to rename);
        //   error = no materialized storage for the oid here, or `new_attname` is already a
        //           column of that storage.
        unique_future<core::result_wrapper_t<bool>> rename_storage_column_inner(components::catalog::oid_t table_oid,
                                                                                std::string old_attname,
                                                                                std::string new_attname);

        // mark_storage_dropped_many_inner — batched DROP-mark: one message per agent
        //   carries that agent's whole oid slice (manager partitioned by pool_idx_for_oid)
        //   plus the shared dropped_at_commit_id. Loops the canonical per-oid mark body
        //   (mark_storage_dropped_one_local) over the slice. Each oid reads its otbx_path
        //   + derives the .wal_id sidecar from this agent's own slice, then records the
        //   GC entry via register_dropped_storage_inner_sync. Over-routed oids no-op.
        unique_future<void> mark_storage_dropped_many_inner(std::pmr::vector<components::catalog::oid_t> table_oids,
                                                            uint64_t dropped_at_commit_id);

        // note_column_identity_inner — park a pg_attribute.attoid on this agent's entry for
        //   `table_oid` against the column NAME it was minted for, so the schema-growth stage of
        //   storage_append_inner can stamp it onto the storage column the moment it materialises
        //   the column. See collection_storage_entry_t::note_column_identity for why the identity
        //   has to arrive BEFORE the column, and who publishes it. Not-owned oids no-op. The
        //   column's TYPE AND DEFAULT ride along (added_column_type_t): the same list is what
        //   table_storage_adapter_t reads to answer a published-but-unmaterialised column, and it
        //   can build neither the column nor the constant every pre-existing row reads without
        //   them. The default arrives ENCODED (pg_attribute.attdefspec's own text) and is decoded
        //   here, against the type that came with it.
        unique_future<void>
        note_column_identity_inner(components::catalog::oid_t table_oid,
                                   std::string attname,
                                   std::uint32_t attoid,
                                   components::pg_attribute_commit_id_backfill_t::added_column_type_t type);

        // Bootstrap-only: base_spaces wires the manager_dispatcher_t address into
        // every agent before scheduler.start. on_horizon_advanced_inner uses it to
        // ack on_subscriber_empty(DISK_KIND) once dropped_storages_ drains. The
        // address is a mailbox handle (not mutable state), safe to copy. Not a
        // mailbox handler; single-threaded at the bootstrap call site.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap-only: base_spaces wires the WAL manager's address into every agent
        // (via manager_disk_t::set_manager_wal_sync fan-out) before scheduler.start. The CATALOG agent
        // (agent 0) uses it to write physical WAL records for catalog DDL directly
        // (append/delete/update pg_* rows), so that work runs on the agent thread instead
        // of the manager loop. A mailbox handle (not mutable state), safe to copy. Not a
        // mailbox handler; single-threaded at the bootstrap call site.
        void set_manager_wal_sync(actor_zeta::address_t address);

        using dispatch_traits = actor_zeta::dispatch_traits<&agent_disk_t::storage_append_inner,
                                                            &agent_disk_t::storage_publish_commits_inner,
                                                            &agent_disk_t::storage_publish_deletes_inner,
                                                            &agent_disk_t::storage_revert_deletes_inner,
                                                            &agent_disk_t::storage_revert_appends_inner,
                                                            &agent_disk_t::storage_update_inner,
                                                            &agent_disk_t::storage_delete_rows_inner,
                                                            &agent_disk_t::storage_fetch_inner,
                                                            &agent_disk_t::storage_scan_inner,
                                                            &agent_disk_t::storage_fetch_next_batch_inner,
                                                            &agent_disk_t::storage_close_cursor_inner,
                                                            &agent_disk_t::storage_reduce_inner,
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
                                                            &agent_disk_t::drop_storage_column_inner,
                                                            &agent_disk_t::rename_storage_column_inner,
                                                            &agent_disk_t::mark_storage_dropped_many_inner,
                                                            &agent_disk_t::note_column_identity_inner,
                                                            &agent_disk_t::create_storage_disk_inner,
                                                            // Appended LAST — positional msg ids.
                                                            &agent_disk_t::storage_open_scan_hold_inner>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // Non-mailbox committed-scan over an OWNED slice entry (callers on the agent
        // thread — storage_scan_inner and read_chunks_by_keys_inner — read their own
        // slice directly here, never by self-sending a mailbox message). Refuses with
        // missing_table when the oid isn't owned or is a record-only marker — an empty batch
        // list is what "no matching rows" looks like, so a scan that could not run must not
        // borrow that shape (the keyed twin of this rule is validate_key_col_indices).
        // `filter` may be nullptr; `projected_cols` may be nullptr for all columns. The wrapper
        // also carries any buffer-pool OOM / data_corruption surfaced by the table-layer scan.
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
        scan_local(components::catalog::oid_t table_oid,
                   components::table::table_filter_t* filter,
                   int64_t limit,
                   const std::vector<std::size_t>* projected_cols,
                   const components::table::transaction_data& txn);

        // Canonical single-oid erase + .otbx removal, used by
        // drop_storage_many_inner. Synchronous; agent-thread callers only.
        void drop_storage_one_local(components::catalog::oid_t oid);

        // Canonical single-oid DROP-mark: read otbx_path + derive the .wal_id
        // sidecar from this agent's own slice, then record the GC entry via
        // register_dropped_storage_inner_sync. Used by mark_storage_dropped_many_inner,
        // which loops it over its oid slice. Synchronous; agent-thread callers only.
        void mark_storage_dropped_one_local(components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);

        log_t log_;
        path_t path_;

        // The agent's role is not stored: it is `pool_idx_ == 0` by construction (idx 0 is the
        // CATALOG agent — see agent_role_t and manager_disk_t::pool_idx_for_oid), and the ctor
        // parameter is used only to log which one this is.
        std::size_t pool_idx_;

        // This agent's storage slice (incomplete value type safe via the deferred
        // instantiation noted at the top of this header).
        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // Per-cursor streaming-scan state for storage_fetch_next_batch_inner, driven per-batch by
        // the streaming scan sources (full_scan / transfer_scan source_next) through
        // storage_fetch_next_batch (OPEN/ADVANCE), one batch per round-trip. POSITION-ONLY
        // index-resume: the entry holds NO buffered batches and NO live scan state — only the
        // absolute resume position (`pos`) plus the immutable scan params needed to re-seek the
        // table each fetch. On every fetch the handler rebuilds a TRANSIENT table_scan_state from
        // `pos`, reads ONE batch, advances `pos`, and lets the scan state (with its pins) destruct,
        // so ZERO pins survive a mailbox round-trip and peak scan memory is one batch. Agent-owned
        // (the agent thread serializes every handler, so this map needs no lock and is never
        // shared). Keyed by the agent-minted cursor_id = (session, counter), which scopes a source
        // to one query.
        struct active_scan_t {
            // Identity of one OPEN-time column: pg_attribute's attoid when the storage column
            // carries one (0 on never-stamped columns), plus the name it had at open. attoid is
            // the primary identity — it survives a RENAME — and (name, type) closes the
            // attoid-0 case, unique even across a computed table's same-named type variants.
            struct open_column_t {
                std::uint32_t attoid{0};
                std::string name;
            };
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID}; // gates compact() on this oid
            components::storage::scan_position_t pos; // absolute resume position (re-seek each fetch)
            std::unique_ptr<components::table::table_filter_t>
                filter;                                    // owned; bound into the transient state per fetch
            std::vector<std::size_t> projected_cols;       // empty == all columns
            components::table::transaction_data txn{0, 0}; // MVCC snapshot for the whole scan
            int64_t matched_limit{-1};                     // post-filter matched-row cap (-1 == unbounded)
            uint64_t matched_emitted{0};                   // running matched rows handed out (enforces matched_limit)
            // OPEN-time schema snapshot, aligned with the storage's types() of that moment
            // (physical columns first, then the published-but-unmaterialized tail). The cursor's
            // projection and filter were bound POSITIONALLY against THIS schema, and the plan
            // above the scan keeps addressing reply chunks by THESE ordinals — so every fetch
            // checks the live schema against the snapshot and answers in this shape, whatever
            // DDL commits between two fetches (see storage_fetch_next_batch_inner).
            std::vector<open_column_t> open_columns;
            std::vector<components::types::complex_logical_type> open_types;
        };
        std::pmr::unordered_map<uint64_t, active_scan_t> active_scans_;
        // Monotonic per-agent cursor-id counter, combined with the session at mint time so the id
        // is (session, counter). 0 is reserved for the OPEN request sentinel.
        uint64_t next_scan_cursor_id_{1};


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
