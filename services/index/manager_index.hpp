#pragma once

#include "index_contract.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include "bitcask_index_agent.hpp"
#include "btree_index_agent.hpp"
#include "index_agent_contract.hpp"
#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <components/catalog/catalog_codes.hpp>
#include <components/index/forward.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <condition_variable>
#include <core/file/local_file_system.hpp>
#include <limits>
#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>

namespace services::index {

#ifdef DEV_MODE
    // Test-observable count of full index repopulations (clear + rebuild). Called by VACUUM and
    // CHECKPOINT; a DELETE must not cause one, so a test can tell "the delete rebuilt the index"
    // apart from "the shutdown checkpoint did", which a profile cannot.
    uint64_t index_repopulations() noexcept;
    void reset_index_repopulations() noexcept;

    // Test-only: counts reads dispatched to a disk agent (index_agent_contract::read_rows). A row
    // assertion can't tell an agent round trip from a manager-side answer; this can.
    uint64_t index_agent_reads() noexcept;
    void reset_index_agent_reads() noexcept;

    // Test-only: counts chunk-column lookups while matching an index key. Should be O(chunks); a
    // regression to per-row matching shows up as O(rows).
    uint64_t index_key_column_probes() noexcept;
    void reset_index_key_column_probes() noexcept;

    // Test-only: committed delete batches currently held back from the stores (see
    // deferred_deletes_). Process-wide; not reset-able. A number that only climbs names a pinned
    // snapshot, not a leak.
    uint64_t index_deferred_deletes() noexcept;

    // Test-only: counts insert batches staged into an index agent (index_agent_contract::stage_inserts).
    // Distinguishes "fed the index" from "fed every index of the table" when row counts can't.
    uint64_t index_stage_insert_batches() noexcept;
    void reset_index_stage_insert_batches() noexcept;

    // Test-only: how many of the batches above came from a manager other than the first to stage
    // since the reset. Non-zero means the counting window was shared by two live managers.
    uint64_t index_stage_insert_foreign_batches() noexcept;
#endif

    // Manager holds ROUTING only; rows/search/per-txn buffer live on the agent. One container, not
    // an engine map beside an agent map -- two maps answering "what serves this oid" can disagree
    // after a partial teardown.
    struct index_record_t {
        // pg_index.indexrelid -- the index's ONLY identity below the planner.
        components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
        // Sole owner of this index's key set: the agent only ever sees resolved (value, row id)
        // pairs, never a column name.
        components::index::keys_base_storage_t keys;
        // Copied from the agent class at spawn: `type` feeds get_indexed_descriptions, `ordered`
        // routes a misrouted range predicate to an error instead of a round trip that refuses.
        components::logical_plan::index_type type{components::logical_plan::index_type::no_valid};
        bool ordered{false};
        // Mailbox this index is reached through; ownership lives in the per-family vectors below.
        actor_zeta::address_t address{actor_zeta::address_t::empty_address()};
        // data_table_t::compact_epoch() this index's rows were built against. Set by the builder
        // (create_index's backfill capture, repopulate_table on rebuild success) from a value it
        // read BEFORE its table scan, so a compact interleaving the build leaves the stamp too
        // LOW — a refusal at storage_fetch, never a wrong row. 0 at bootstrap: both the table's
        // counter and this stamp restart at 0 together (compaction is runtime-only).
        uint64_t built_compact_epoch{0};
    };

    using index_records_t = std::pmr::vector<index_record_t>;

    // Routing lookups over one table's records. Free functions, not manager methods, so they're
    // testable without an actor.

    // The index registered under this indexrelid, or nullptr.
    [[nodiscard]] const index_record_t* match_index_relid(const index_records_t& records,
                                                          components::catalog::oid_t index_oid) noexcept;

    // The index over this key set BUILT BY THIS BACKEND, or nullptr. index_type::no_valid
    // ("the plan named no preference") matches nothing by construction.
    [[nodiscard]] const index_record_t* match_index(const index_records_t& records,
                                                    const components::index::keys_base_storage_t& keys,
                                                    components::logical_plan::index_type type);

    // Untyped lookup: caller named no backend, so this picks ORDERED FIRST. An unordered index
    // refuses a range predicate that an ordered twin over the same keys could answer.
    [[nodiscard]] const index_record_t* match_index(const index_records_t& records,
                                                    const components::index::keys_base_storage_t& keys);

    // Key sets as a SET, not a bag: an ordered and a hashed index over one column is ONE indexed
    // key set, not two. Multiplicity is available, exactly, from indexed_descriptions below.
    [[nodiscard]] std::pmr::vector<components::index::keys_base_storage_t>
    indexed_keys(const index_records_t& records, std::pmr::memory_resource* resource);

    // (key set, backend) per registered index -- what lets the planner tell an ordered
    // index from a hashed one over the SAME column.
    [[nodiscard]] std::pmr::vector<components::index::index_description_t>
    indexed_descriptions(const index_records_t& records, std::pmr::memory_resource* resource);

    // Chunk column carrying `keys`, or key_column_absent if the chunk doesn't carry every key
    // column. Resolved once per chunk per index. Multi-column keys: only the first key's column is
    // checked (todo on the index side).
    inline constexpr std::size_t key_column_absent = std::numeric_limits<std::size_t>::max();
    [[nodiscard]] std::size_t resolve_key_column(const components::index::keys_base_storage_t& keys,
                                                 const components::vector::data_chunk_t& chunk);

    class manager_index_t final : public actor_zeta::actor::actor_mixin<manager_index_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        manager_index_t(std::pmr::memory_resource* resource,
                        actor_zeta::scheduler_raw scheduler,
                        log_t& log,
                        std::filesystem::path path_db = {},
                        uint64_t bitcask_flush_threshold = 1000,
                        uint64_t bitcask_segment_record_limit = 100,
                        uint64_t btree_flush_threshold = 1000);
        ~manager_index_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        // Public for observability/tests. Held in a loop-thread-local
        // std::pmr::list (chosen for iterator stability across push and resume).
        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };

        // Senders only deliver: the message is released into inbox_ and pump_cv_
        // is notified. ALL processing runs on loop_thread_, lock-free on the
        // DML/DDL path. (See the event-loop fields below.)
        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
        requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>) [[nodiscard]] ReturnType
            enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args);

        // Single-threaded callers only (NOT a mailbox handler): catalog-scan
        // rebuild and, internally, the mark_table_dropped handler.
        void mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id);

        // Runtime DROP TABLE mailbox handler; thin coroutine wrapper around
        // mark_table_dropped_sync (see index_contract).
        unique_future<void>
        mark_table_dropped(session_id_t session, components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);

        // DROP-GC value-space remap (see index_contract). mark_table_dropped[_sync]
        // recorded dropped_table_agents_[oid] in TXN-ID space (>= 2^62); after the
        // transaction commits, this rewrites every entry whose value equals txn_id to
        // the real commit_id so on_horizon_advanced can eventually reclaim it.
        unique_future<void> table_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // Abort mirror of table_dropped_committed (see index_contract): dropped_table_agents_[oid]
        // holds a TXN-ID (>= 2^62) until commit, so on ABORT this erases every entry equal to
        // txn_id, keeping the table indexed.
        unique_future<void> table_drop_aborted(session_id_t session, uint64_t txn_id);

        // Wired by base_spaces before scheduler.start. Used to send the
        // on_subscriber_empty ack once dropped_table_agents_ empties.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap helpers, called from base_spaces::bootstrap_indexes_sync BEFORE
        // scheduler.start (single-threaded, so direct mutation is safe) to seed the manager into
        // steady state.

        // Register the table with an EMPTY record list. Empty != absent: absent means CREATE INDEX
        // on this table would be a bookkeeping bug; empty means known but indexless.
        void bootstrap_engine_sync(components::catalog::oid_t oid);

        // Register one existing on-disk index (per alive pg_index row) and raise its disk agent.
        // Raised HERE, not handed in, so pg_index.indtype -> class stays a single decision
        // (spawn_disk_agent below); thresholds come from the manager's own configuration, so
        // bootstrap and runtime CREATE INDEX raise identical agents.
        // committed_commit_ids: WAL-replay commit ids for the hashed family's txn-log recover gate
        // (commit ids, not txn ids -- only the commit clock survives a restart).
        // Returns the reason the index could not open (unregistered table, duplicate row,
        // unsupported type, storage failed to open); nothing is registered on that path.
        [[nodiscard]] core::error_t bootstrap_index_sync(components::catalog::oid_t table_oid,
                                                         components::catalog::oid_t index_oid,
                                                         components::logical_plan::index_type type,
                                                         components::index::keys_base_storage_t keys,
                                                         std::pmr::set<std::uint64_t> committed_commit_ids);

        // Restore a dropped-table entry from pg_class.delete_id (alias of
        // mark_table_dropped_sync).
        void bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id);

#ifdef DEV_MODE
        // Raw, non-owning handles for tests that need to drive an agent's own mailbox directly
        // (an address_t can't be resumed). One accessor per family; never used on a decision path.
        [[nodiscard]] std::pmr::vector<bitcask_index_agent_t*> owned_bitcask_agents_sync();
        [[nodiscard]] std::pmr::vector<btree_index_agent_t*> owned_btree_agents_sync();
#endif

        // No bootstrap-time repopulate: CHECKPOINT compaction renumbers row_ids but bootstrap runs
        // pre-scheduler (no mailbox round trip available). Guarded instead by rebuild_marker_path_
        // below, read by base_spaces::bootstrap_indexes_sync to decline wiring stale indexes.

        // One (table, index) pair recorded as "renumbered, not yet rebuilt" -- see
        // rebuild_marker_path_. Read at bootstrap by base_otterbrix_t::bootstrap_indexes_sync.
        struct pending_index_rebuild_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
            components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
        };

        // Pairs a previous process armed and never cleared. An index named here holds
        // PRE-COMPACT row ids and must not be wired. Empty on a clean start.
        [[nodiscard]] std::pmr::vector<pending_index_rebuild_t> pending_index_rebuilds_sync() const;

        // Collection lifecycle
        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

        // DML: txn-aware bulk index operations.
        unique_future<core::error_t> insert_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                                 uint64_t start_row_id,
                                                 uint64_t count);
        unique_future<core::error_t> delete_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                                 std::pmr::vector<int64_t> row_ids);
        unique_future<core::error_t> update_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> old_data,
                                                 std::pmr::vector<components::vector::data_chunk_t> new_data,
                                                 std::pmr::vector<int64_t> row_ids,
                                                 int64_t new_start_row_id);

        // Post-append reconciliation ask (see index_contract): the appended sub-ranges the
        // transaction's mirror sends did not stage, empty when the table has no index.
        unique_future<std::pmr::vector<index_row_range_t>>
        unmirrored_ranges(execution_context_t ctx,
                          components::catalog::oid_t table_oid,
                          std::pmr::vector<index_row_range_t> ranges);

        // MVCC commit/revert/cleanup. commit_* return core::error_t (no_error() = success); the
        // bitcask write path is assert+abort terminal today, so success is currently the only
        // value returned. The batch form sends every oid's fan-out then awaits all, returning the
        // first contains_error().
        unique_future<core::error_t> commit_inserts(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        // Does NOT touch a store: records the batch in deferred_deletes_ and subscribes to the
        // horizon; on_horizon_advanced sends it once no live snapshot needs the rows. Always
        // answers no_error() -- a later erase failure is logged where it happens.
        unique_future<core::error_t> commit_deletes(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        // Engine-level pending-delete clear: discards this txn's mark_delete
        // entries from every index of the table's engine (the abort mirror of
        // revert_insert; aborted DELETE markers never reach disk, so no disk fan-out).
        unique_future<void> revert_delete(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);

        // Runtime index rebuild driver (see index_contract). Returns the oids
        // whose engine holds >= 1 index, EXCLUDING oids in dropped_table_agents_.
        unique_future<std::pmr::vector<components::catalog::oid_t>> all_indexed_oids(session_id_t session);

        // Repopulate from a post-compact scan: agent clear() fan-out, then txn_id=0 re-insert keyed
        // by chunk.row_ids. Errors, before any clearing, if a non-empty chunk carries no physical
        // row_ids. See index_contract.
        unique_future<core::error_t> repopulate_table(session_id_t session,
                                                      components::catalog::oid_t table_oid,
                                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                      uint64_t row_count,
                                                      core::date::timezone_offset_t session_tz,
                                                      uint64_t built_compact_epoch);

        // DDL: returns the reason the index could not open, or no_error(); never silently
        // downgrades to in-memory. No "index id" returned -- identity below the planner is the
        // caller's own indexrelid.
        unique_future<core::error_t> create_index(session_id_t session,
                                                  components::catalog::oid_t table_oid,
                                                  components::catalog::oid_t index_oid,
                                                  components::index::keys_base_storage_t keys,
                                                  components::logical_plan::index_type type,
                                                  core::date::timezone_offset_t session_tz,
                                                  uint64_t built_compact_epoch);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // Query (txn-aware). See the contract for what the wrapper distinguishes; in
        // short, an EMPTY id set now means "no row matches" and nothing else. The reply pairs
        // the ids with the record's built_compact_epoch (index_search_result_t).
        unique_future<core::result_wrapper_t<index_search_result_t>>
        search(session_id_t session,
               components::catalog::oid_t table_oid,
               components::index::keys_base_storage_t keys,
               components::types::logical_value_t value,
               components::expressions::compare_type compare,
               uint64_t start_time,
               uint64_t txn_id,
               core::date::timezone_offset_t session_tz);

        unique_future<core::result_wrapper_t<index_search_result_t>>
        search_with_preferred_type(session_id_t session,
                                   components::catalog::oid_t table_oid,
                                   components::index::keys_base_storage_t keys,
                                   components::types::logical_value_t value,
                                   components::expressions::compare_type compare,
                                   components::logical_plan::index_type preferred_type,
                                   uint64_t start_time,
                                   uint64_t txn_id,
                                   core::date::timezone_offset_t session_tz);

        unique_future<core::error_t> flush_all_indexes(session_id_t session);

        // Compact gate (see index_contract): returns the subset of the input
        // oids with NO engine in engines_ (safe to compact), input order
        // preserved; an engine means its positional row refs would break on compact.
        unique_future<std::pmr::vector<components::catalog::oid_t>>
        tables_without_indexes(session_id_t session, std::pmr::vector<components::catalog::oid_t> table_oids);

        // GC subscriber: drains dropped_table_agents_ (reap tables past the snapshot floor) THEN
        // deferred_deletes_ (send now-safe erases) -- in that order, since reaping a table takes
        // its held-back erases with it. Acks on_subscriber_empty(INDEX_KIND) only once BOTH are
        // empty, or the dispatcher's broadcast flag would switch off before all erases publish.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // CREATE INDEX catchup handler (see index_contract): locates the engine
        // for (table_oid, index_oid) and applies the record's key effect.
        unique_future<void> apply_wal_record_for_index(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t index_oid,
                                                       uint64_t wal_record_id,
                                                       uint8_t record_type,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::pmr::vector<components::vector::data_chunk_t> physical_data,
                                                       uint64_t physical_row_start,
                                                       uint64_t txn_id,
                                                       core::date::timezone_offset_t session_tz);

        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
        get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<std::pmr::vector<components::index::index_description_t>>
        get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid);

        using dispatch_traits = actor_zeta::implements<index_contract,
                                                       &manager_index_t::register_collection,
                                                       &manager_index_t::unregister_collection,
                                                       &manager_index_t::insert_rows,
                                                       &manager_index_t::delete_rows,
                                                       &manager_index_t::update_rows,
                                                       &manager_index_t::unmirrored_ranges,
                                                       &manager_index_t::commit_inserts,
                                                       &manager_index_t::commit_deletes,
                                                       &manager_index_t::revert_insert,
                                                       &manager_index_t::revert_delete,
                                                       &manager_index_t::cleanup_all_versions,
                                                       &manager_index_t::all_indexed_oids,
                                                       &manager_index_t::repopulate_table,
                                                       &manager_index_t::create_index,
                                                       &manager_index_t::drop_index,
                                                       &manager_index_t::search,
                                                       &manager_index_t::search_with_preferred_type,
                                                       &manager_index_t::flush_all_indexes,
                                                       &manager_index_t::tables_without_indexes,
                                                       &manager_index_t::get_indexed_keys,
                                                       &manager_index_t::get_indexed_descriptions,
                                                       &manager_index_t::on_horizon_advanced,
                                                       &manager_index_t::mark_table_dropped,
                                                       &manager_index_t::table_dropped_committed,
                                                       &manager_index_t::table_drop_aborted,
                                                       &manager_index_t::apply_wal_record_for_index>;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        std::filesystem::path path_db_;
        // Thresholds every raised agent is built with, both at bootstrap and runtime CREATE INDEX.
        // Not read from bitcask_index_disk_t::default_* per-call -- that would honour a configured
        // limit only for indexes existing at startup. spawn_disk_agent below is the sole reader.
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};

        // THE REGISTRY: one container per table, deliberately -- an engine map beside an address
        // map is two answers to "what serves this oid" that can disagree after a partial teardown.
        // A present-but-empty entry means registered with no index; create_index refuses a missing
        // entry rather than minting one (a bookkeeping bug upstream).
        std::pmr::unordered_map<components::catalog::oid_t, index_records_t> indexes_per_oid_;

        // Dropped-table markers (oid -> dropped_at_commit_id); on_horizon_advanced erases
        // indexes_per_oid_[oid] and drops its agents once the snapshot floor passes commit_id.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> dropped_table_agents_;

        // A committed delete not yet safe to publish: an index must not withhold a row id (a short
        // answer would be silently wrong), so the erase waits for the horizon to pass its commit_id
        // (disk indexes have no delete_id stamp, unlike cleanup_versions for in-memory ones). Keyed
        // by (table_oid, index_oid), not by address -- the address is re-resolved from
        // indexes_per_oid_ at sweep time. The rows stay in the agent's own pending_deletes_; this
        // only holds the schedule.
        struct deferred_delete_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
            components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
            // Carried rather than re-derived: the hashed family also journals writes under it.
            uint64_t txn_id{0};
            // Also what the hashed family's txn-log frame is stamped with; reusing the txn id here
            // was the reuse bug in bitcask_index_disk.cpp's recover_txn_log.
            uint64_t commit_id{0};
        };

        // Unbounded on purpose: evicting an entry would mean publishing an erase early, which is
        // exactly the bug this queue exists to prevent (index_deferred_deletes() is the meter).
        // Lost on restart it just leaves a superset, filtered at fetch like any other row.
        std::pmr::vector<deferred_delete_t> deferred_deletes_;

        // CREATE INDEX backfill-staging refusals, keyed by the build's transaction.
        // apply_wal_record_for_index returns void, so a refusal is recorded here and checked at
        // commit_inserts, which refuses the whole commit BEFORE any agent publishes. Cleared only
        // via the abort mirrors revert_insert/revert_delete; a retried commit refuses again.
        // First failure wins per transaction.
        std::pmr::unordered_map<uint64_t, core::error_t> catchup_failures_;

        // THE MIRROR LEDGER: appended row ranges each live transaction's insert_rows/update_rows
        // sends have staged, per table — what unmirrored_ranges subtracts a statement's appends
        // against. Recorded only when the table has at least one index (an unindexed table keeps
        // no ledger; a later build's RAW read covers its rows). Erased at commit_inserts and
        // revert_insert.
        std::pmr::unordered_map<
            uint64_t,
            std::pmr::unordered_map<components::catalog::oid_t, std::pmr::vector<index_row_range_t>>>
            mirrored_ranges_;

        // Durable marker for the gap between committing a compacted table and rebuilding its
        // indexes to the new row ids -- a kill -9 inside that gap must survive the restart, and no
        // in-process ordering can close a gap across a crash. One line per pending pair:
        //     ${path_db_}/index_rebuild_pending      "<table_oid> <index_oid>"
        // Armed at flush_all_indexes (the only entry point of both compacting orchestrations:
        // operator_checkpoint_t, manager_wal_replicate_t::run_auto_checkpoint), cleared only after
        // repopulate_table's rebuild succeeds. Unions on arm, never replaces, so an index a
        // previous start already declined to wire isn't dropped from the marker by a later round.
        [[nodiscard]] std::filesystem::path rebuild_marker_path_() const;
        [[nodiscard]] std::pmr::vector<pending_index_rebuild_t> read_rebuild_marker_() const;
        [[nodiscard]] core::error_t
        write_rebuild_marker_(const std::pmr::vector<pending_index_rebuild_t>& pending) const;
        // Union every currently registered (table, index) pair into the marker. Returns the
        // reason it could not be made durable: a round whose guard is not on the device may
        // not go on to renumber the rows the guard is about.
        [[nodiscard]] core::error_t arm_rebuild_marker_();
        // Drop this table's entries FOR THE INDEXES NAMED, and only those: a table can carry
        // a rebuilt index beside one an earlier start declined to wire, and the second is
        // still stale.
        [[nodiscard]] core::error_t clear_rebuild_marker_(components::catalog::oid_t table_oid,
                                                          const index_records_t& rebuilt);
        // The single-pair form, for DROP INDEX: the note loses its subject.
        [[nodiscard]] core::error_t forget_rebuild_marker_entry_(components::catalog::oid_t table_oid,
                                                                 components::catalog::oid_t index_oid);

        // Drops the held-back erases of a table/index WITHOUT publishing them, wherever the agent
        // they were owed to is about to be destroyed -- there's nothing left to publish, and an
        // entry outliving it would be a lookup into a torn-down record on the next sweep.
        void forget_deferred_deletes(components::catalog::oid_t table_oid);
        void forget_deferred_deletes(components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // Ownership, one vector per family: destroying an entry frees the agent and closes its
        // store. Two vectors, not one polymorphic vector -- the owning pointer's deleter
        // (actor_zeta::pmr::deleter_t) returns sizeof(STATIC T) to the pool, so erasing through a
        // common base would return the wrong size. Reaped by drop_index, unregister_collection and
        // on_horizon_advanced.
        std::pmr::vector<bitcask_index_agent_ptr> bitcask_agents_owned_;
        std::pmr::vector<btree_index_agent_ptr> btree_agents_owned_;

        // Agents detached from the manager on their way to destruction: nothing can address them
        // once here, so the terminal drop is safe to send after detaching (see drop_index).
        struct detached_agents_t {
            std::pmr::vector<bitcask_index_agent_ptr> bitcask;
            std::pmr::vector<btree_index_agent_ptr> btree;

            explicit detached_agents_t(std::pmr::memory_resource* resource)
                : bitcask(resource)
                , btree(resource) {}

            [[nodiscard]] bool empty() const noexcept { return bitcask.empty() && btree.empty(); }
        };

        // The ONE place pg_index.indtype picks a class. Returns the routing facts the registry
        // keeps, or the open failure, as a value -- nothing recorded on failure. `type`/`ordered`
        // come OUT (read from the chosen class), since what the catalog asked for and what the
        // family actually is can differ (a composite index is built by the ordered family but
        // published as `single`).
        struct spawned_agent_t {
            actor_zeta::address_t address;
            components::logical_plan::index_type type;
            bool ordered;
        };
        [[nodiscard]] core::result_wrapper_t<spawned_agent_t>
        spawn_disk_agent(components::catalog::oid_t table_oid,
                         components::catalog::oid_t index_oid,
                         components::logical_plan::index_type type,
                         std::pmr::set<std::uint64_t> committed_commit_ids);

        // Take every disk agent of `table_oid` out of the manager: its records leave
        // indexes_per_oid_ and its owners leave the vectors above. The agents are matched
        // by asking each one which table it serves, so there is no second map to disagree
        // with the owners.
        [[nodiscard]] detached_agents_t detach_table_agents(components::catalog::oid_t table_oid);

        // The same, for ONE index named by its indexrelid (DROP INDEX: the table's sibling
        // indexes must stay registered, so the record is trimmed out of the vector rather
        // than the whole entry erased).
        [[nodiscard]] detached_agents_t detach_index(components::catalog::oid_t table_oid,
                                                     components::catalog::oid_t index_oid);

        // Send the terminal drop to every detached agent and hand back the replies to await.
        // Scheduling goes through the pointers this frame holds, because schedule_agent() searches
        // the manager's vectors and these are no longer in them. Sends only -- no suspension
        // inside, so the caller keeps the two-phase send-all-then-await-all shape.
        [[nodiscard]] std::pmr::vector<unique_future<void>> send_drop_to_detached(detached_agents_t& dying,
                                                                                  session_id_t session);

        // Index metadata lives in pg_catalog.pg_index (no separate metadata file).
        core::filesystem::local_file_system_t fs_;

        // Target for the on_subscriber_empty(INDEX_KIND) ack; wired pre-start
        // via set_manager_dispatcher_sync.
        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};

        // Find disk agent by address and schedule it if needed
        void schedule_agent(const actor_zeta::address_t& addr, bool needs_sched);

        // Pending futures
        std::pmr::vector<unique_future<void>> pending_void_;
        void poll_pending();

        // Event-loop-in-thread state. The loop thread owns the in-flight behavior list locally;
        // senders only deliver into inbox_ and wake the loop via pump_cv_. mutex_ guards ONLY the
        // cv idle-wait -- never held across behavior creation, cont.resume() or behavior_t
        // destruction, so the DML/DDL path stays lock-free.
        std::mutex mutex_;
        // Wakes the loop thread out of its bounded idle wait.
        std::condition_variable pump_cv_;
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // Stores raw message* (boost::lockfree requires trivially-copyable):
        // release() on push, re-wrapped into message_ptr by the loop. Node
        // allocations are non-PMR (infra queue).
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
    };

    template<typename ReturnType, typename... Args>
    requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>)
        ReturnType manager_index_t::enqueue_impl(actor_zeta::actor::address_t sender,
                                                 actor_zeta::mailbox::message_id cmd,
                                                 Args&&... args) {
        using R = typename actor_zeta::type_traits::is_unique_future<ReturnType>::value_type;

        auto [msg, future] =
            actor_zeta::detail::make_message<R>(resource(), std::move(sender), cmd, std::forward<Args>(args)...);

        // Checked, not cast away: this hand-off has no bounded queue to refuse it, so it
        // can't fail today; unaddressed, `future` would hang forever. The needs-scheduling half
        // (.first) is ignored -- this manager runs its own loop thread, already woken above.
        if (enqueue_impl(std::move(msg)).second != actor_zeta::detail::enqueue_result::success) {
            error(log_, "manager_index_t::enqueue_impl: message refused; its reply will never arrive");
        }
        return std::move(future);
    }

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
