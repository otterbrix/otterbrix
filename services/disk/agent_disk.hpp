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
    // Test-observable count of checkpoint ROUNDS reaching this agent (rounds, not tables, so the
    // number does not move when the catalog gains a table). A compacting round rewrites every disk
    // table in the agent's slice whole, so it costs O(data) and a load that triggers one per commit
    // is quadratic in the rows already written.
    uint64_t table_checkpoints() noexcept;
    void reset_table_checkpoints() noexcept;
#endif

    using path_t = std::filesystem::path;

    using session_id_t = ::components::session::session_id_t;
    // Catalog-DDL _inner handlers take the same by-value context the manager routers do.
    using execution_context_t = ::components::execution_context_t;

    // Test-observable counter of ROWS the agent ships back for an aggregate-pushdown
    // reduce — the sum of data_chunk_t::size() over the FINAL aggregated chunks
    // produced by the reduce in storage_reduce_inner, i.e. exactly the
    // rows that cross the agent->coordinator mailbox. This is the direct measurement of
    // aggregate-pushdown traffic reduction: a scalar aggregate must reply exactly 1 row and
    // a GROUP BY exactly one row per group, regardless of how many raw rows the agent
    // scanned. Tests reset it to 0, run one aggregate, then assert the reply row count is
    // TINY (<< the scanned input), proving only the finalized partial crossed the wire. Bumped
    // ONLY on the spec/reduce path (a raw scan never touches it). Process-global + relaxed:
    // coarse instrumentation, not a synchronization primitive; off every hot path. DEV_MODE-
    // only, mirroring services::collection::executor::dml_flush_count() — production binaries
    // carry neither the counter nor these accessors.
#ifdef DEV_MODE
    uint64_t pushdown_reply_rows() noexcept;
    void reset_pushdown_reply_rows() noexcept;

    // Test-observable counter of storage scans issued by the KEYED catalog read
    // (read_chunks_by_keys_inner). Each bump is one full pass over a pg_* table, and a whole
    // batch of key tuples is answered in ONE pass — the count does not grow with key count.
    uint64_t catalog_key_scans() noexcept;
    void reset_catalog_key_scans() noexcept;
#endif

    // Forward-declared (full definitions in manager_disk.hpp). agent_disk_t's slice
    // maps use these as incomplete value types — safe because the user-provided
    // destructor in agent_disk.cpp defers template instantiation past this header.
    struct collection_storage_entry_t;
    struct dropped_storage_entry_t;

    // Streaming single-pass hash semi-join used by scan_by_keys_inner. Returns
    // result[i] = row_ids of every row of `storage` whose key columns equal input key-tuple i
    // (one bucket per input key, input order; empty when nothing matches). Column j of `keys`
    // holds the value for stored column key_col_indices[j]. STREAMS `storage` exactly ONCE
    // (fetch_next_batch) regardless of key count — O(table_rows + nkeys), NOT O(nkeys *
    // table_rows). Exposed at namespace scope so tests can drive it against a counting storage.
    // A scan failure is returned, not swallowed: a partial result here reads to
    // operator_fk_check / operator_unique_constraint as "these keys matched nothing",
    // i.e. a constraint that passes because the rows were never read.
    core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    fk_hash_semijoin(std::pmr::memory_resource* resource,
                     components::storage::storage_t& storage,
                     const std::pmr::vector<std::uint64_t>& key_col_indices,
                     components::vector::data_chunk_t& keys,
                     components::table::transaction_data txn);

    // Plain cross-mailbox result for checkpoint_inner. Plain std fields only (no pmr) —
    // safe to copy across the mailbox by value.
    //   min_prev_checkpoint_wal_id — min(prev_checkpoint_wal_id_) over this agent's
    //     entries, or wal::id_t max() sentinel when it owns none.
    // It used to carry a second field, `has_in_memory`, which suppressed WAL-floor sealing
    // while any table still lived only in memory. B4 removed the mode that produced it: every
    // table is a file, so there is nothing left to suppress on.
    struct checkpoint_result_t {
        wal::id_t min_prev_checkpoint_wal_id;
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

        // compact() gate: a live index-resume cursor holds an ABSOLUTE row position into the current
        // (un-swapped) collection, so compact()'s atomic row_groups_ swap on that oid would shift the
        // positions out from under the cursor (R17 result drift). true == at least one open cursor
        // targets `oid`; the three compact sites skip such oids. Public because manager_disk_t routes
        // the observability probe (has_active_scan_for_oid_sync) to the owning agent. Agent-thread
        // only (active_scans_ is agent-owned and the mailbox serializes every access), so no lock.
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

        // Ownership constructors: build the SFBM-holding entry directly on the
        // agent thread. The SFBM holds an exclusive posix WRITE_LOCK on the .otbx
        // (per-process: closing either fd releases it for both), so these may run
        // ONLY when no other emplace for the same OID can race. Bootstrap-only, not
        // mailbox handlers — safe pre-scheduler-start because nothing else has touched
        // the agent's resource() yet. Both return false on duplicate key.
        //   bootstrap_disk_inner_sync       — load existing .otbx; seeds
        //     checkpoint_wal_id from the caller-supplied sidecar_wal_id. `catalog_columns`
        //     is the A7.6 schema overlay for a never-checkpointed file (see
        //     table_storage_t's load ctor); ignored for a checkpointed one. `is_computed`
        //     marks a relkind='g' table (empty catalog schema is legal; dynamic-schema
        //     append semantics survive the restart).
        //   bootstrap_create_disk_inner_sync — create new .otbx. B1b: `is_computed` is
        //     the relkind='g' fact, resolved by the caller (see
        //     manager_disk_t::create_storage_disk[_sync]); the entry no longer infers it
        //     from an empty column set.
        [[nodiscard]] bool
        bootstrap_disk_inner_sync(components::catalog::oid_t oid,
                                  const std::filesystem::path& otbx_path,
                                  wal::id_t sidecar_wal_id,
                                  std::vector<components::table::column_definition_t> catalog_columns,
                                  bool is_computed) noexcept;

        [[nodiscard]] bool bootstrap_create_disk_inner_sync(components::catalog::oid_t oid,
                                                            std::vector<components::table::column_definition_t> columns,
                                                            const std::filesystem::path& otbx_path,
                                                            bool is_computed) noexcept;

        // Runtime CREATE mailbox handlers. The manager routers forward by oid (+ columns
        // by value / path as string) so the entry is built with the AGENT's OWN
        // resource() on the agent thread — no entry crosses the mailbox and the manager
        // touches no storage state. Each returns a plain bool: false on duplicate key,
        // mirroring the bootstrap helpers' contract. The bodies reuse the existing
        // bootstrap_*_inner_sync helpers (now called intra-actor).
        //   create_storage_disk_inner          — create_directories(parent) on the agent
        //     thread, then construct the new .otbx SFBM entry.
        unique_future<bool> create_storage_disk_inner(components::catalog::oid_t oid,
                                                      std::vector<components::table::column_definition_t> columns,
                                                      std::filesystem::path otbx_path,
                                                      bool is_computed);

        // WAL-replay direct_* helpers: the manager-side direct_*_sync routers forward
        // here to apply the mutation against the local slice.
        // Bootstrap-only — base_spaces WAL replay runs synchronously before
        // scheduler.start; post-start mutations use the storage_* mailbox handlers.
        //
        // A MISSING STORAGE IS A REFUSAL, NOT A NO-OP, and the distinction is the whole
        // reason these return core::error_t. The manager picks this agent with
        // pool_idx_for_oid(table_oid) before it forwards, so "not owned by this agent" was
        // never true of the leg that logged it: the owner is decided by the routing and this
        // agent IS the owner. What an absent entry means is that the owner has no storage for
        // the table, and on the REPLAY path a mutation dropped there is a journalled change
        // that recovery silently declined to restore — rows the WAL says are deleted staying
        // alive, an update that never lands. The caller cannot re-derive it from anywhere.
        // The shared refusal the three helpers below build (see the note above them).
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

        // Mutation handlers: these inner bodies are the SOLE owner of each mutation;
        // manager-side bodies are pure routers.
        //
        // A NOT-OWNED OID IS A REFUSAL ON EVERY LEG BELOW, not a no-op. The agent is chosen
        // by pool_idx_for_oid BEFORE the send, so the agent reading "I do not own this oid"
        // IS the owner and the branch really means "the owner has no storage" — which is a
        // mutation that did not happen and a read that never reached data. Each leg used to
        // answer that with its own natural empty value; every one of those values is also a
        // correct answer to a real question, so nothing above could tell them apart. Only an
        // EMPTY REQUEST (no rows asked for, no rows to write) is still a plain success.
        //
        // storage_append_inner — canonical WAL-FIRST append. Owns the FULL
        //   preprocessing pipeline (schema adoption/growth, column expansion,
        //   NOT NULL, dedup, type promotion), then — because it runs on this agent's
        //   mailbox (one handler coroutine at a time, atomic across the WAL co_await) —
        //   it allocates the start_row WITHOUT materializing, writes the WAL records
        //   (PHYSICAL_ADD_COLUMN for any dynamic schema growth, then PHYSICAL_INSERT
        //   carrying the final start_row + count), and only THEN materializes the
        //   append. User + catalog inserts share ONE write ordering. Returns
        //   (start_row, count); (0,0) for an EMPTY chunk, and an error when this agent has
        //   no storage to append to. Takes the full execution_context
        //   (session/txn/tz/db_oid) because the agent owns the WAL write.
        // Reply wraps the pair so a buffer-pool OOM or write_conflict surfaced by the
        // table-layer append chain travels back to operator_insert as a value (no throw
        // across the mailbox) — and so the routing refusal does not arrive dressed as the
        // zero-length range an empty batch legitimately produces.
        unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append_inner(execution_context_t ctx,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::vector::data_chunk_t> data);

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
        //   agent twin. Reply wraps storage_t::update's (updated, appended) pair so a
        //   write_conflict / out_of_memory travels back to operator_update as a value;
        //   (0, 0) for an EMPTY chunk, an error when this agent has no storage to update.
        unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update_inner(components::catalog::oid_t table_oid,
                             components::vector::vector_t row_ids,
                             std::unique_ptr<components::vector::data_chunk_t> data,
                             components::table::transaction_data txn);

        // storage_delete_rows_inner — single-OID DELETE mutation. The wrapper carries the
        //   deleted-row count on success and an error when this agent cannot perform the
        //   delete at all (the oid is not in its slice, or the entry has no storage). The
        //   two used to be the same value — 0 — and the callers, having no channel to read,
        //   dropped the reply entirely; an ON DELETE CASCADE could then delete nothing and
        //   still let its parent row go. A count below the requested one is NOT a refusal:
        //   an already-stamped row is skipped by design (chunk_vector_info::delete_rows).
        unique_future<core::result_wrapper_t<uint64_t>>
        storage_delete_rows_inner(components::catalog::oid_t table_oid,
                                  components::vector::vector_t row_ids,
                                  uint64_t count,
                                  components::table::transaction_data txn);

        // storage_fetch_inner — read-path mirror for point-fetches by row_id.
        //   Returns the fetched rows as a vector of ≤DEFAULT_VECTOR_CAPACITY chunks; an
        //   oid this agent has no storage for is an ERROR, because an empty chunk vector
        //   is what a fetch whose rows are all invisible to `txn` legitimately returns.
        //   The wrapper also carries the buffer-pool OOM / data_corruption the point-fetch
        //   surfaced — the first window error aborts the batch (a partial answer must not ship).
        //   Under fetch_visibility_t::SNAPSHOT the table layer drops every row `txn` may
        //   not see, so a produced chunk can be SHORTER than its window; each chunk's
        //   row_ids are stamped by the producer (collection_t::fetch) with the rows it
        //   actually carries, and this handler no longer re-stamps them from the request.
        //   RAW keeps every row whatever its version stamps say — the CREATE INDEX
        //   backfill reads deleted rows on purpose.
        //   `limit` (-1 == uncapped) caps the reply at that many rows AFTER the visibility
        //   drop, and stops the window loop as soon as the budget is spent — the reason it
        //   lives here and not in the caller: a cap applied to the requested IDS spends
        //   budget on rows the reader never receives. It is a truncation and nothing more:
        //   the capped reply is the uncapped reply's prefix.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_fetch_inner(components::catalog::oid_t table_oid,
                            components::vector::vector_t row_ids,
                            uint64_t count,
                            std::vector<size_t> projected_cols,
                            components::table::transaction_data txn,
                            components::table::fetch_visibility_t visibility,
                            int64_t limit);

        // Read-path handlers (scan_batched / scan_segment / types / total_rows).
        // Not-owned OIDs refuse; see the note above the mutation handlers.
        //
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

        // storage_fetch_next_batch_inner — streaming fetch-next scan source (STEP 3 / phase B).
        //   POSITION-ONLY index-resume: unlike storage_scan_inner (which materializes the
        //   whole batch vector), the cursor in active_scans_ stores ONLY the absolute resume
        //   position + the scan params; every fetch re-seeks a TRANSIENT scan state from that
        //   position (storage_t::fetch_next_batch), reads ONE batch, advances the stored position,
        //   and releases the pins before returning — so peak scan memory is one batch and ZERO
        //   pins survive the round-trip.
        //     cursor_id==0  -> OPEN: resolve the owned entry, snapshot max_row = total_rows, move
        //                      (filter, projected_cols, txn, limit) into a position-only cursor,
        //                      mint id = (session, next_scan_cursor_id_++) per R16, store it in
        //                      active_scans_, then advance once.
        //     cursor_id!=0  -> ADVANCE: look up active_scans_[cursor_id], re-seek to its stored
        //                      position and read exactly one batch.
        //   A produced batch (cardinality>0) replies {batch, cursor_id}. A drained cursor (scan
        //   exhausted / matched-row limit reached) ERASES active_scans_[cursor_id] and replies an
        //   EMPTY chunk (cardinality 0) + cursor_id. An OPEN (cursor_id==0) over a not-owned oid
        //   / record-only marker REFUSES: the drained sentinel on a FIRST batch reads as "this
        //   table is empty", a fact about the table asserted by a scan that never started. An
        //   unknown cursor on ADVANCE still replies drained — the drain path erases the entry
        //   itself, so not knowing a cursor IS that cursor being finished. Buffer-pool OOM /
        //   data_corruption ride the wrapper as a value (no throw across the mailbox), like
        //   scan_local.
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch_inner(session_id_t session,
                                       components::catalog::oid_t table_oid,
                                       uint64_t cursor_id,
                                       std::unique_ptr<components::table::table_filter_t> filter,
                                       int64_t limit,
                                       std::vector<size_t> projected_cols,
                                       components::table::transaction_data txn);

        // storage_close_cursor_inner — release a fetch-next cursor abandoned before it drained
        // (A4). Erases active_scans_[cursor_id], which also lifts the compact() gate this
        // cursor held on its oid. Idempotent: an unknown id is a no-op, since the drain path
        // erases the entry itself.
        unique_future<void> storage_close_cursor_inner(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       uint64_t cursor_id);

        // storage_reduce_inner — the aggregate-pushdown REDUCE, a dedicated protocol
        //   leg (NOT a scan mode): runs the whole GROUP BY over this agent's OWN slice via
        //   operator_group rebuilt from the POD spec (WHERE rides `filter`, projection
        //   rides `projected_cols`), and replies ALL final aggregated rows in ONE reply — the
        //   result is bounded by #groups, so no cursor exists. A not-owned / record-only oid
        //   REFUSES; it used to reduce over the EMPTY input and emit the scalar aggregate's
        //   single row, which is a fact about a table ("your COUNT is 0") synthesized from a
        //   read that reached no storage and identical to what a real empty table produces.
        //   An empty OWNED slice still folds to that single row — that half is correct.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce_inner(session_id_t session,
                             components::catalog::oid_t table_oid,
                             std::unique_ptr<components::table::table_filter_t> filter,
                             std::vector<size_t> projected_cols,
                             components::table::transaction_data txn,
                             components::operators::pushed_aggregate_spec_t spec);

        // scan_by_keys_inner — batched keyed scan for one owned table. Resolves the
        //   key column NAMES to storage indices once, then delegates to the streaming
        //   single-pass hash semi-join fk_hash_semijoin: builds ONE typed hash of the
        //   input key set and STREAMS the table exactly once (fetch_next_batch), bucketing
        //   each row_id into every matching key. result[i] == match row_ids for key-tuple i;
        //   result has one (possibly empty) entry per key, so an empty entry means exactly one
        //   thing: this key matched nothing. A not-owned OID / unknown column / arity mismatch
        //   is a core::error_t — this comment used to promise "a same-length result of empty
        //   rows", which is the affirmative answer "nothing references that key" and is what
        //   let ON DELETE CASCADE drop a parent whose children stayed; the code stopped doing
        //   it, the sentence did not. The whole batch is one mailbox message so name resolution happens once,
        //   the scan is O(table_rows + nkeys) (NOT one full scan per key), and it is
        //   serialized against same-oid mutations.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
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
        //   same as scan_by_keys_inner; it never crosses the mailbox. A read that cannot be
        //   performed (not-owned OID / record-only marker / unknown column / empty key columns)
        //   is a core::error_t — never an empty result, which means "matched nothing".
        unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key_inner(components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::uint64_t> key_col_indices,
                                 components::vector::data_chunk_t keys,
                                 std::pmr::vector<std::uint64_t> projected_cols,
                                 components::table::transaction_data txn);

        // read_chunks_by_keys_inner — batched multi-key columnar row-data scan for one owned
        //   table. `keys` is an N-row data_chunk whose column j holds key_col_names[j] and whose
        //   row i is the i-th key-tuple. The handler resolves the key column NAMES to storage
        //   indices ONCE, then for each key row builds an eq-AND filter (constant = keys.value(j, i))
        //   and scans, returning the matching rows as batched data_chunk_t (all columns, no row
        //   limit). result[i] == matched chunks for key-tuple i; the outer vector always has one
        //   (possibly empty) entry per key in input order, so result.size() == keys.size() on
        //   EVERY path — mirroring scan_by_keys_inner. A read that cannot be performed (not-owned
        //   OID / record-only marker / unknown column / arity mismatch / a scan io_error) is a
        //   core::error_t; an empty entry therefore means exactly one thing, "this key matched
        //   nothing". The filter constant is a logical_value_t — the irreducible filter-API floor,
        //   same as read_chunks_by_key_inner; it never crosses the mailbox.
        unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys_inner(components::catalog::oid_t table_oid,
                                  std::pmr::vector<std::uint64_t> key_col_indices,
                                  components::vector::data_chunk_t keys,
                                  std::pmr::vector<std::uint64_t> projected_cols,
                                  components::table::transaction_data txn);

        // storage_types_inner — schema metadata accessor. An oid this agent has no storage
        //   for is an error: an EMPTY type list is what a storage with no adopted schema
        //   answers, and operator_resolve_table maps live columns onto that list by name, so
        //   an empty one silently describes a table with no columns.
        unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types_inner(components::catalog::oid_t table_oid);

        // storage_total_rows_inner — row-count metadata accessor. 0 used to mean either
        //   "not owned" or "empty twin", and the two are NOT equivalent for callers: one is
        //   a fact about a table, the other is a read that never happened. 0 now means only
        //   the first; the second travels the wrapper.
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows_inner(components::catalog::oid_t table_oid);

        // Fanout handlers for checkpoint_all / vacuum_all / on_horizon_advanced —
        // each agent iterates its own storages_ slice in parallel.
        //
        // checkpoint_inner — (compact + checkpoint(wal_id) + sidecar) per entry.
        //   Returns a checkpoint_result_t whose min_prev_checkpoint_wal_id is
        //   min(prev_checkpoint_wal_id_) over the agent's entries (max() sentinel when it
        //   owns none).
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

        // vacuum_inner — cleanup_versions per entry. Compaction is NOT done here.
        //   compact_watermark: same visible-to-all horizon contract as checkpoint_inner.
        //   ITEM B: under A7.2's split free pool a compact whose release is never committed by
        //   a header cannot return space, only spend it, so compaction is one indivisible unit
        //   with the checkpoint that commits it — and checkpoint_inner already performs that
        //   unit. It used to run here for in-memory entries, which had no checkpoint round of
        //   their own; that mode is gone. See the long note at maybe_cleanup_inner's definition
        //   for the full reasoning, including why "checkpoint after compacting" here would
        //   duplicate rows on recovery.
        unique_future<void>
        vacuum_inner(session_id_t session, uint64_t lowest_active_start_time, uint64_t compact_watermark);

        // maybe_cleanup_inner — single-OID target. NOTHING is compacted here (ITEM B, as
        //   above): the compaction of a file-backed table belongs to the checkpoint round that
        //   can commit the release, and every table is file-backed. The handler stays on the
        //   contract because operator_commit_transaction sends it per touched oid; see its
        //   definition for the census that settled the question.
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
        //   agent's own slice. Returns (table_oid, start_row, count) or the reason nothing
        //   was written; count is 0 when the write was a DIRECT WRITE (nothing to publish)
        //   or the caller handed in an empty row.
        //
        //   THE ERROR CHANNEL IS THE POINT. This is the DDL write path, and the range alone
        //   cannot tell "wrote nothing" from "could not write": a failed pg_class append used
        //   to come back as a zero-count range, which every caller reads as a no-op, so
        //   CREATE TABLE reported success over a catalog that never received the table. The
        //   three ways it can fail — a cast the guard does not cover, a refused append
        //   (write_conflict / out_of_memory), and a catalog oid whose owning agent holds no
        //   storage — all travel this wrapper now.
        unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
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

        // compact_relkind_g_storage_inner — whole-op intra-agent: read own slice, compute the
        //   columns NOT in live_attnames, drop each via entry->drop_column on its own slice,
        //   return the dropped count. Missing / already-compact returns 0. B4 removed the gate
        //   that used to refuse every file-backed table here; the note at the definition says
        //   why acting is the safe reading and what the refusal cost.
        unique_future<std::uint64_t> compact_relkind_g_storage_inner(components::catalog::oid_t table_oid,
                                                                     std::set<std::string> live_attnames);

        // B3c1 — drop_storage_column_inner: release the ONE column `attname` from this
        //   agent's own slice for `table_oid`, via the same entry->drop_column primitive the
        //   compact leg above calls. Unlike the subtractive VACUUM leg this caller NAMES its
        //   column, so there is no live set to re-derive and no gap in that derivation to turn
        //   into a drop of a surviving one. Both share B3c's split: the rebuild now, the block
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
        //   pg_attribute.attoid (RN-oid) and repairs a stale storage name from the catalog.
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

        // note_column_identity_inner — RN-oid. Park a pg_attribute.attoid on this agent's
        //   entry for `table_oid` against the column NAME it was minted for, so that the
        //   schema-growth stage of storage_append_inner can stamp it onto the storage column
        //   at the moment it materialises the column. See
        //   collection_storage_entry_t::note_column_identity for why the identity has to
        //   arrive BEFORE the column, and who publishes it. Not-owned oids no-op, exactly like
        //   every other mutation handler here.
        //   The column's TYPE rides along: the same list is what table_storage_adapter_t reads to
        //   answer a published-but-unmaterialised column with NULLs, and it cannot build a column
        //   without one.
        unique_future<void> note_column_identity_inner(components::catalog::oid_t table_oid,
                                                       std::string attname,
                                                       std::uint32_t attoid,
                                                       components::types::complex_logical_type type);

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
                                                            &agent_disk_t::create_storage_disk_inner>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // Non-mailbox committed-scan over an OWNED slice entry (D6: callers on the agent
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
        // parameter is used only to log which one this is. A `role_` member sat here, written
        // once and never read; clang's -Wunused-private-field is what named it.
        std::size_t pool_idx_;

        // This agent's storage slice (incomplete value type safe via the deferred
        // instantiation noted at the top of this header).
        std::pmr::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

        // ACTIVE (active_scan_t / active_scans_ / next_scan_cursor_id_): the position-only bounded
        // fetch-next scan state. The streaming scan sources (full_scan / transfer_scan source_next)
        // drive it per-batch via storage_fetch_next_batch (OPEN/ADVANCE), one batch per round-trip.
        // Per-cursor streaming-scan state for storage_fetch_next_batch_inner. POSITION-ONLY
        // index-resume (STEP 3): the entry holds NO buffered batches and NO live scan state —
        // only the absolute resume position (`pos`) plus the immutable scan params needed to
        // re-seek the table each fetch. On every fetch the handler rebuilds a TRANSIENT
        // table_scan_state from `pos`, reads ONE batch, advances `pos`, and lets the scan state
        // (with its pins) destruct, so ZERO pins survive a mailbox round-trip and peak scan
        // memory is one batch. Agent-owned (the agent thread serializes every handler, so this
        // map needs no lock and is never shared). Keyed by the agent-minted cursor_id =
        // (session counter), which scopes a source to one query.
        struct active_scan_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID}; // gates compact() on this oid
            components::storage::scan_position_t pos; // absolute resume position (re-seek each fetch)
            std::unique_ptr<components::table::table_filter_t>
                filter;                                    // owned; bound into the transient state per fetch
            std::vector<std::size_t> projected_cols;       // empty == all columns
            components::table::transaction_data txn{0, 0}; // MVCC snapshot for the whole scan
            int64_t matched_limit{-1};                     // post-filter matched-row cap (-1 == unbounded)
            uint64_t matched_emitted{0};                   // running matched rows handed out (enforces matched_limit)
        };
        std::pmr::unordered_map<uint64_t, active_scan_t> active_scans_;
        // Monotonic per-agent cursor-id counter, combined with the session at mint time so the id
        // is (session, counter) per R16. 0 is reserved for the OPEN request sentinel.
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
