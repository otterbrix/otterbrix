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

    // Test-observable count of index reads DISPATCHED TO A DISK AGENT (one per
    // index_agent_contract::read_rows send). It separates "the SELECT returned the right rows" from
    // "the SELECT returned the right rows FROM THE AGENT": anything that registers, is chosen by
    // the planner and answers out of a manager-side copy returns exactly the same rows, and no row
    // assertion can tell the two apart. Zero means every read was answered locally.
    uint64_t index_agent_reads() noexcept;
    void reset_index_agent_reads() noexcept;

    // Test-observable count of CHUNK COLUMN INSPECTIONS performed while matching an index
    // key to a column. Resolved once per chunk per index this is O(chunks); a regression
    // back to matching per row shows up as O(rows).
    uint64_t index_key_column_probes() noexcept;
    void reset_index_key_column_probes() noexcept;

    // How many COMMITTED DELETE BATCHES are currently held back from the stores (one per (table,
    // index) per committing transaction -- see deferred_deletes_). Process-wide, so a test that may
    // share the binary with another manager measures a DIFFERENCE.
    //
    // It is the only handle on a state that is otherwise invisible: the SQL surface cannot tell
    // "the entry is still in the index because an older snapshot may want it" from "the entry is
    // still there because the sweep is broken", and a test that waited on wall-clock time for a
    // horizon-driven erase would be a flake generator. It is also the meter for the growth cost
    // named at the declaration: a number that only climbs is a pinned snapshot, not a leak in the
    // queue. NOT reset-able and never read on a decision path.
    uint64_t index_deferred_deletes() noexcept;

    // Test-observable count of INSERT BATCHES STAGED INTO AN INDEX AGENT (one per
    // index_agent_contract::stage_inserts send from this manager). It separates "the statement fed
    // the index" from "the statement fed EVERY index of the table": both answer the same rows,
    // because both stores dedup a repeated (key, row id) pair, so no row assertion anywhere can
    // tell an addressed feed from a fan-out. The number can.
    uint64_t index_stage_insert_batches() noexcept;
    void reset_index_stage_insert_batches() noexcept;

    // WHOSE sends the number above is made of. The counter is process-wide, and a count alone
    // cannot name its author: a SECOND live manager_index_t -- a previous space still draining, a
    // binary that opens two at once -- adds its rounds to the same total and looks exactly like the
    // fan-out the count exists to catch. So every staged batch also records the manager that staged
    // it, and this returns how many came from a manager OTHER than the first one to stage after the
    // reset. ZERO is the reading a comparison may be built on; non-zero means the window was shared
    // and the number is nobody's.
    //
    // Identity is the manager's address, so it is exact for any two managers ALIVE AT ONCE (the
    // only case that can pollute a window) and deliberately blind to a manager destroyed and
    // reallocated at the same address between windows -- a dead manager sends nothing, so that
    // reuse cannot forge a batch.
    uint64_t index_stage_insert_foreign_batches() noexcept;
#endif

    // ONE LIVE INDEX, AND EVERY FACT ABOUT IT THE MANAGER NEEDS. The manager holds ROUTING and
    // nothing else: the ROWS, the SEARCH and the per-transaction BUFFER are the agent's, because
    // they are halves of the same answer. And the routing is deliberately ONE container: splitting
    // it -- an engine map beside an agent map -- gives two containers answering the same question
    // ("what serves this oid") that can therefore disagree, an entry left in one after a partial
    // teardown outliving the other with nothing to report it.
    struct index_record_t {
        // pg_index.indexrelid -- the index's ONLY identity below the planner (rule 16).
        components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
        // The key set this index covers. The manager is its SOLE owner: the agent is fed
        // already-resolved (key value, row id) pairs and never sees a column name, so
        // there is no second copy to drift from this one.
        components::index::keys_base_storage_t keys;
        // Copied from the agent class's static index_type_v / supports_ordered_probe_v at spawn,
        // and read HERE, before any send: the type is what all_indexed_descriptions publishes to
        // the planner, and `ordered` is what turns a misrouted range predicate into a core::error_t
        // instead of a round trip that ends in a refusal.
        components::logical_plan::index_type type{components::logical_plan::index_type::no_valid};
        bool ordered{false};
        // The mailbox this index is reached through. The ONLY handle the manager keeps on
        // a decision path; ownership lives in the per-family vectors below.
        actor_zeta::address_t address{actor_zeta::address_t::empty_address()};
    };

    using index_records_t = std::pmr::vector<index_record_t>;

    // --- Routing lookups over one table's records ---------------------------------
    //
    // Free functions over the vector, not members of the manager, so they can be read and tested
    // without an actor: what they decide is pure.

    // The index registered under this indexrelid, or nullptr.
    [[nodiscard]] const index_record_t* match_index_relid(const index_records_t& records,
                                                          components::catalog::oid_t index_oid) noexcept;

    // The index over this key set BUILT BY THIS BACKEND, or nullptr. index_type::no_valid
    // ("the plan named no preference") matches nothing by construction.
    [[nodiscard]] const index_record_t* match_index(const index_records_t& records,
                                                    const components::index::keys_base_storage_t& keys,
                                                    components::logical_plan::index_type type);

    // UNTYPED lookup: the caller named no backend, so this picks one. Two indexes over the SAME key
    // set are legal (create_index rejects a duplicate only on the pair (keys, type)), so the pick
    // is a DECISION -- ORDERED FIRST -- and not an artefact of registration order: an unordered
    // index answers eq and nothing else and refuses a range, so handing back the hashed twin would
    // fail a probe the index beside it could have answered.
    [[nodiscard]] const index_record_t* match_index(const index_records_t& records,
                                                    const components::index::keys_base_storage_t& keys);

    // The key sets this table has an index on, as a SET. Both consumers ask an EXISTENCE question
    // and nothing else: the planner's context_storage_t::has_index_on and enrich_logical_plan's
    // stamp_table_has_indexes ("does this table have any index, so must DML mirror into it?"). A
    // table carrying an ordered AND a hashed index over one column has ONE indexed key set, not
    // two, and a bag would invite the next caller to read the repeat as two different columns.
    // Multiplicity is available, exactly, from indexed_descriptions below.
    [[nodiscard]] std::pmr::vector<components::index::keys_base_storage_t>
    indexed_keys(const index_records_t& records, std::pmr::memory_resource* resource);

    // (key set, backend) per registered index -- what lets the planner tell an ordered
    // index from a hashed one over the SAME column.
    [[nodiscard]] std::pmr::vector<components::index::index_description_t>
    indexed_descriptions(const index_records_t& records, std::pmr::memory_resource* resource);

    // Which chunk column carries `keys`, or key_column_absent when the chunk does not carry every
    // key column and the index is therefore skipped for it. Resolved ONCE per chunk per index: the
    // answer is a property of the chunk's column layout, not of a row. ALL key columns must be
    // present for the index to apply, but the value read is the FIRST key's column -- multi-column
    // index keys are still a todo on the index side.
    inline constexpr std::size_t key_column_absent = std::numeric_limits<std::size_t>::max();
    [[nodiscard]] std::size_t resolve_key_column(const components::index::keys_base_storage_t& keys,
                                                 const components::vector::data_chunk_t& chunk);

    // Bootstrap address bundle for sync() (a plain named struct -- no std::tuple; mirrors
    // services::wal::wal_sync_pack_t and manager_disk_t::disk_sync_pack_t). Namespace-scope, not
    // nested, so callers/tests use services::index::index_sync_pack_t. Carries manager_disk_t's
    // address so the index manager can address it after spawn (scan_segment index population).
    struct index_sync_pack_t {
        actor_zeta::address_t disk = actor_zeta::address_t::empty_address();
    };

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

        void sync(index_sync_pack_t pack);

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

        // DROP-rollback un-mark (see index_contract) -- the abort mirror of
        // table_dropped_committed. dropped_table_agents_[oid] was recorded in TXN-ID space (>=
        // 2^62); if the transaction ABORTS the table must stay indexed, so this ERASES every entry
        // whose value equals txn_id and on_horizon_advanced never reaps the engine.
        unique_future<void> table_drop_aborted(session_id_t session, uint64_t txn_id);

        // Wired by base_spaces before scheduler.start. Used to send the
        // on_subscriber_empty ack once dropped_table_agents_ empties.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap helpers, called from base_spaces::bootstrap_indexes_sync BEFORE
        // scheduler.start: single-threaded by construction, so direct mutation of the manager's
        // owned structures is safe. They seed the manager from the catalog scan so it starts in
        // steady state.

        // Register the table with the index manager: an EMPTY record list per live table
        // oid from the catalog scan. Empty is not the same as absent -- absent means the
        // manager has never heard of the table and CREATE INDEX on it is a bookkeeping
        // bug, empty means it is known and carries no index yet.
        void bootstrap_engine_sync(components::catalog::oid_t oid);

        // Register one existing on-disk index (per alive pg_index row, keyed by its indexrelid):
        // raise its disk agent and record it in indexes_per_oid_[oid].
        //
        // THE AGENT IS RAISED HERE, not handed in. Spawning it in the caller (base_spaces) and
        // moving it in would put a SECOND site that reads pg_index.indtype and picks a class from
        // it; that choice belongs in the one factory that owns it (spawn_disk_agent below). The
        // caller supplies only what the catalog scan read, and the thresholds come from the
        // manager's own configuration -- which is what makes bootstrap and runtime CREATE INDEX
        // raise IDENTICAL agents.
        //
        // committed_txn_ids: the WAL-replay set of committed transaction ids, used by the hashed
        // family's txn-log recover gate; the ordered family ignores it (no log).
        //
        // Returns the reason the index could not be brought up -- an unregistered table (bootstrap
        // order violated), a duplicate row, an unsupported type, or a storage that would not open.
        // Nothing is registered on those paths, so the caller has nothing to unwind: an index that
        // will not open costs a full scan, whereas aborting costs the whole engine its start.
        [[nodiscard]] core::error_t bootstrap_index_sync(components::catalog::oid_t table_oid,
                                                         components::catalog::oid_t index_oid,
                                                         components::logical_plan::index_type type,
                                                         components::index::keys_base_storage_t keys,
                                                         std::pmr::set<std::uint64_t> committed_txn_ids);

        // Restore a dropped-table entry from pg_class.delete_id (alias of
        // mark_table_dropped_sync).
        void bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id);

#ifdef DEV_MODE
        // Raw, NON-OWNING handles to the disk agents this manager currently owns. A test that wants
        // to lay out an interleaving by hand -- "the agent has handled the drop, the manager has
        // not been resumed yet" -- has to drive the agent's own mailbox, and an
        // actor_zeta::address_t cannot be resumed; the manager raises its agents itself, so it is
        // the only place a test can get one from. ONE ACCESSOR PER FAMILY, because there is no
        // common base to hand back and deliberately none coming. Never called on a decision path.
        [[nodiscard]] std::pmr::vector<bitcask_index_agent_t*> owned_bitcask_agents_sync();
        [[nodiscard]] std::pmr::vector<btree_index_agent_t*> owned_btree_agents_sync();
#endif

        // THERE IS NO BOOTSTRAP-TIME REPOPULATE, deliberately. CHECKPOINT compaction renumbers
        // physical row_ids while an on-disk index keeps the pre-compact ones, but rebuilding at
        // startup cannot fix that: bootstrap runs before the schedulers and a rebuild is a mailbox
        // round trip. It is answered instead by the durable guard below (rebuild_marker_path_),
        // read by base_spaces::bootstrap_indexes_sync, which DECLINES to wire the indexes it names.

        // ONE (table, index) PAIR RECORDED AS "RENUMBERED AND NOT YET REBUILT".
        //
        // The durable fact the compaction window needs; see rebuild_marker_path_ for the
        // whole argument. Read at bootstrap, by base_otterbrix_t::bootstrap_indexes_sync.
        struct pending_index_rebuild_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
            components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
        };

        // Bootstrap door (single-threaded, pre-scheduler-start, like the bootstrap_* calls
        // below): the pairs a previous process armed and never cleared. An index named here
        // stores PRE-COMPACT physical row ids and MUST NOT be wired -- see
        // rebuild_marker_path_. Empty on a clean start and on every in-memory topology.
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

        // MVCC commit/revert/cleanup. commit_* return core::error_t (no_error() = success) per the
        // contract; the bitcask write path is assert+abort terminal today, so success is currently
        // the only value returned. The batch form folds every oid's pending disk fan-out into a
        // single send-all-then-await-all pass: the first contains_error() across the batch is
        // returned, but all awaits drain.
        unique_future<core::error_t> commit_inserts(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        // commit_deletes DOES NOT TOUCH A STORE. It records the batch in deferred_deletes_ (see
        // there) and marks this manager a horizon subscriber; on_horizon_advanced sends the agents
        // their commit_deletes once no live snapshot can still want the rows. Nothing is left here
        // to fail, so it always answers no_error() -- an erase that fails LATER is logged where it
        // happens and leaves the index a superset, the safe direction.
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

        // Repopulate one table's indexes from a post-compact storage scan: disk agent clear()
        // fan-out, in-memory engine clear, then txn_id=0 re-insert of every row keyed by its
        // physical id from chunk.row_ids. Returns an error, before any clearing, when a non-empty
        // chunk carries no physical row_ids -- a producer defect. See index_contract.
        unique_future<core::error_t> repopulate_table(session_id_t session,
                                                      components::catalog::oid_t table_oid,
                                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                      uint64_t row_count,
                                                      core::date::timezone_offset_t session_tz);

        // DDL: index management. Returns the reason the index cannot be brought up (already
        // present, unknown table, unsupported type, or its on-disk storage failed to open), or
        // no_error(); a disk index is never silently downgraded to an in-memory one. No "index id"
        // comes back: an index's identity below the planner is its indexrelid, which the caller
        // already has.
        unique_future<core::error_t> create_index(session_id_t session,
                                                  components::catalog::oid_t table_oid,
                                                  components::catalog::oid_t index_oid,
                                                  components::index::keys_base_storage_t keys,
                                                  components::logical_plan::index_type type,
                                                  core::date::timezone_offset_t session_tz);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // Query (txn-aware). See the contract for what the wrapper distinguishes; in
        // short, an EMPTY vector now means "no row matches" and nothing else.
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        search(session_id_t session,
               components::catalog::oid_t table_oid,
               components::index::keys_base_storage_t keys,
               components::types::logical_value_t value,
               components::expressions::compare_type compare,
               uint64_t start_time,
               uint64_t txn_id,
               core::date::timezone_offset_t session_tz);

        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
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

        // GC subscriber, and TWO queues drain here, in this order:
        //
        //   1. dropped_table_agents_ -- entries whose dropped_at_commit_id is below the new
        //      snapshot floor lose their registry entry and their agents (reaped).
        //   2. deferred_deletes_ -- committed erases the floor has now reached are finally sent to
        //      their agents. Second, because step 1 destroys agents: a reaped table's held-back
        //      erases go WITH it rather than being addressed afterwards.
        //
        // The ack is sent once BOTH are empty: on_subscriber_empty(INDEX_KIND) clears the
        // dispatcher's selective-broadcast flag, so acking with erases still queued would switch
        // off the only signal that can ever publish them.
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
        // The thresholds every agent this manager raises is built with -- BOTH at bootstrap and at
        // runtime CREATE INDEX. Building from bitcask_index_disk_t::default_* /
        // btree_index_disk_t::default_* instead would honour a configured
        // `bitcask_segment_record_limit` for indexes that existed at startup and silently ignore it
        // for every index created afterwards, laying the same index out two different ways
        // depending on when it was created. spawn_disk_agent below is the only builder and it reads
        // these.
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};

        // THE REGISTRY: which indexes serve which table, and everything about them the manager
        // decides on (see index_record_t). ONE container, and that is the point: a per-table engine
        // map beside a per-oid address map would be two containers answering "what serves this
        // oid", exactly the shape that diverges under a partial teardown. A PRESENT-BUT-EMPTY entry
        // means "this table is registered and carries no index": register_collection /
        // bootstrap_engine_sync create it, and create_index refuses a table with no entry at all
        // rather than minting one, because that is a bookkeeping bug upstream.
        std::pmr::unordered_map<components::catalog::oid_t, index_records_t> indexes_per_oid_;

        // Dropped-table markers (oid -> dropped_at_commit_id). Populated by
        // mark_table_dropped[_sync]; drained by on_horizon_advanced once the
        // snapshot floor passes the commit_id, which erases indexes_per_oid_[oid] and
        // sends terminal drop messages to the agents it named.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> dropped_table_agents_;

        // ONE COMMITTED DELETE BATCH THAT MAY NOT REACH A STORE YET.
        //
        // An index may name rows a reader must not see -- the table drops those on the point fetch
        // (fetch_visibility_t::SNAPSHOT) -- but it may NOT withhold an id: a row the index never
        // names is never fetched and never filtered, so a SHORT index answer is a silently wrong
        // result nothing downstream can undo. So commit_deletes may not erase an entry while a live
        // snapshot still owns the row, and a snapshot owns it until its horizon reaches the
        // delete's commit_id (row_version_manager: delete_id > snapshot_horizon keeps the row
        // alive). An in-memory index gets this free from its delete_id stamp plus
        // cleanup_versions(lowest_active); a disk index has no stamp, so the wait is a QUEUE.
        //
        // KEYED BY (table_oid, index_oid), NEVER BY THE AGENT'S ADDRESS: the address is re-resolved
        // out of indexes_per_oid_ at sweep time, so an index dropped between the commit and the
        // sweep is simply not found instead of being posted to through a stale handle.
        //
        // THE ROWS ARE NOT HERE. They never left the agent's own pending_deletes_ bucket, which the
        // agent's commit_deletes publishes and clears. This queue holds the SCHEDULE only -- the
        // commit_id the manager was handed and the snapshot floor it subscribes to, two facts
        // nothing below the mailbox has.
        struct deferred_delete_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
            components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
            // Which bucket the agent must publish when the wait is over. The hashed family
            // also journals under it, which is why it is carried rather than re-derived.
            uint64_t txn_id{0};
            // The horizon this batch is waiting for.
            uint64_t commit_id{0};
        };

        // THE PRICE, STATED: this queue is IN MEMORY and has NO CAP. A long-lived snapshot pins
        // lowest_active, the sweep never fires, and both this vector and the agents' matching
        // buckets grow for as long as that snapshot is held. Not negotiable downwards: EVICTING an
        // entry means publishing an erase early, which is precisely the defect this queue exists to
        // remove, so there is no cap that is not also a correctness bug. index_deferred_deletes()
        // above is the meter; a number that only climbs names a pinned snapshot, not a leak here.
        //
        // An entry lost to a restart leaves the erase unapplied, so the index keeps naming a row
        // the table has deleted -- a SUPERSET, the safe direction, filtered on the fetch like any
        // other. (The separate, known, unfixed staleness of post-compact row_ids is neither relied
        // on nor made worse: repopulate_table drops the table's entries before it clears its
        // agents.)
        //
        // A VECTOR, not a map: the only two consumers walk ALL of it (the horizon sweep and a
        // table/index teardown filter), and entries arrive in commit order.
        std::pmr::vector<deferred_delete_t> deferred_deletes_;

        // CREATE INDEX CATCHUP REFUSALS, KEYED BY THE BUILD'S TRANSACTION.
        //
        // apply_wal_record_for_index's contract returns void -- the operator that drives it cannot
        // be handed an error -- so logging and dropping a catchup record the registry could not
        // place, or a staging an agent refused, would let the build publish an index missing those
        // rows. The refusal is recorded here instead and surfaces at the one door the build must
        // pass to publish: commit_inserts refuses the whole commit of a transaction with an entry
        // here, BEFORE any agent is told to publish. The entry is NOT consumed by that refusal (a
        // retried commit refuses again); it leaves only through the abort mirrors revert_insert /
        // revert_delete, the path a failed statement takes. First failure wins per transaction.
        std::pmr::unordered_map<uint64_t, core::error_t> catchup_failures_;

        // THE DURABLE "THESE INDEXES NAME PRE-COMPACT ROWS" FACT, AND WHY IT IS A FILE.
        //
        // A compacting round is two durable acts with a gap between them:
        // agent_disk_t::checkpoint_inner commits the compacted table, and the rebuild that follows
        // makes the indexes name the new ids at its own force_flush. Inside that gap the DEVICE
        // holds a post-compact table under pre-compact indexes, and nothing repairs it afterwards
        // -- bootstrap rebuilds no index (see above) and WAL replay maintains none -- so a kill -9
        // there is PERMANENT, and both resulting failures are SILENT: an id naming no row group is
        // dropped by collection_t::fetch (a short answer), an id that now belongs to a different
        // survivor is gathered as the match (a wrong answer). No ordering of the round's own steps
        // closes a gap between two durable acts; only a fact that OUTLIVES THE PROCESS can be read
        // on the other side of it, and that fact is this file:
        //
        //     ${path_db_}/index_rebuild_pending      one "<table_oid> <index_oid>" per line
        //
        // ARMED BEFORE THE COMPACTION, CLEARED ONLY AFTER THE REBUILD LANDED. The arm rides
        // flush_all_indexes, the FIRST step of both compacting orchestrations
        // (operator_checkpoint_t and manager_wal_replicate_t::run_auto_checkpoint) and sent from
        // nowhere else in the tree, so "a compacting round is starting" and "this handler ran" are
        // the same event. The clear rides repopulate_table, on the path where every agent reported
        // success -- clearing anywhere earlier would disarm the guard before the thing it guards
        // had happened.
        //
        // THE ARM UNIONS, IT DOES NOT REPLACE: an index that a previous start declined to wire is
        // not in indexes_per_oid_, so a replacing write would drop its entry and the start after
        // that would wire the stale store. Entries leave only through the rebuild that fixes them
        // and through drop_index.
        //
        // THE PRICE, NAMED: the window opens at flush_all_indexes, so a kill inside a round that
        // had not yet compacted anything still costs those indexes -- declined at the next start,
        // re-created by the operator. Conservative on purpose; the alternative is answering from an
        // index that may name rows that no longer exist.
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

        // Drop the held-back erases of a whole table / of one index, WITHOUT publishing them.
        // Called wherever the thing they were owed to is being taken away: the agent is about to be
        // destroyed, its buckets with it, so there is nothing left to publish and an entry that
        // outlived it would be a lookup into a torn-down record on the next sweep. Not a fallback
        // -- the erase is genuinely moot once the index is gone.
        void forget_deferred_deletes(components::catalog::oid_t table_oid);
        void forget_deferred_deletes(components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // OWNERSHIP, ONE VECTOR PER FAMILY. These keep the agents alive so the addresses recorded
        // above stay valid, and destroying an entry IS what frees an agent and closes its store.
        //
        // Two vectors and not one because an owning pointer here is std::unique_ptr<T,
        // actor_zeta::pmr::deleter_t>, whose deleter returns sizeof(STATIC T) bytes to the pool
        // (actor-zeta detail/memory.hpp deallocate_ptr): a single vector would need a common base
        // for both families, and destroying either agent through it would hand the pool the wrong
        // size. The erasure the manager actually needs is for the WORK, and actor_zeta::address_t
        // already carries its own concrete-typed enqueue thunk, so the family is known only where
        // it must be -- raising and freeing.
        //
        // Reaped -- detached, sent the terminal drop, awaited, destroyed -- by drop_index,
        // unregister_collection and on_horizon_advanced. On resource_, like the maps above (rule
        // 8).
        std::pmr::vector<bitcask_index_agent_ptr> bitcask_agents_owned_;
        std::pmr::vector<btree_index_agent_ptr> btree_agents_owned_;

        // Disk agents taken OUT of the manager and into a handler's frame, on their way to
        // destruction. Nothing outside can address them any more, which is the whole point: the
        // terminal drop is sent only after the owner has been detached, so nothing can post a
        // request behind it (see drop_index). Destroying this struct destroys the agents.
        struct detached_agents_t {
            std::pmr::vector<bitcask_index_agent_ptr> bitcask;
            std::pmr::vector<btree_index_agent_ptr> btree;

            explicit detached_agents_t(std::pmr::memory_resource* resource)
                : bitcask(resource)
                , btree(resource) {}

            [[nodiscard]] bool empty() const noexcept { return bitcask.empty() && btree.empty(); }
        };

        // THE ONE PLACE pg_index.indtype picks a class. Raises the agent for (table_oid,
        // index_oid), records its owner in the matching vector above, and hands back the routing
        // facts the registry keeps -- or the reason its store would not open, as a value. Nothing
        // is recorded on the failure path.
        //
        // `type` and `ordered` come OUT rather than in, copied from the chosen class's static
        // index_type_v / supports_ordered_probe_v: what the catalog asked for and what the family
        // actually is are not the same word (a composite index is built by the ordered family and
        // published as `single`), and the registry must carry what it IS.
        struct spawned_agent_t {
            actor_zeta::address_t address;
            components::logical_plan::index_type type;
            bool ordered;
        };
        [[nodiscard]] core::result_wrapper_t<spawned_agent_t>
        spawn_disk_agent(components::catalog::oid_t table_oid,
                         components::catalog::oid_t index_oid,
                         components::logical_plan::index_type type,
                         std::pmr::set<std::uint64_t> committed_txn_ids);

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

        // Address of manager_disk_t (for scan_segment when populating indexes)
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();

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

        // The delivery result is CHECKED, not cast away (rule 14). This manager's enqueue_impl is a
        // pure hand-off -- it releases the message into inbox_ and wakes the loop, and there is no
        // bounded queue to refuse it -- so today it cannot fail. If it ever does, `future` below is
        // left with no producer and its awaiter waits forever. The needs-scheduling half is
        // deliberately dropped: this manager runs its own loop thread and enqueue_impl already woke
        // it.
        if (enqueue_impl(std::move(msg)).second != actor_zeta::detail::enqueue_result::success) {
            error(log_, "manager_index_t::enqueue_impl: message refused; its reply will never arrive");
        }
        return std::move(future);
    }

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
