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

#include "index_agent_disk.hpp"
#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <components/catalog/catalog_codes.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <condition_variable>
#include <core/file/local_file_system.hpp>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace services::index {

    // INDEXES_METADATA_FILENAME retired. Index metadata lives in
    // pg_catalog.pg_index now; this constant is kept as a comment so anyone reading
    // legacy data dirs can still recognize the filename.

    // Bootstrap address bundle for sync() (plain named struct — no std::tuple,
    // mirrors services::wal::wal_sync_pack_t and manager_disk_t::disk_sync_pack_t).
    // Namespace-scope (not nested) so callers/tests use
    // services::index::index_sync_pack_t. Carries manager_disk_t's address so the
    // index manager can address it after spawn (scan_segment index population).
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

        // DROP-rollback un-mark (see index_contract) — the abort mirror of
        // table_dropped_committed. mark_table_dropped[_sync] recorded
        // dropped_table_agents_[oid] in TXN-ID space (>= 2^62); if the transaction
        // ABORTS the table must stay indexed, so this ERASES every
        // dropped_table_agents_ entry whose value equals txn_id, un-marking the DROP
        // so on_horizon_advanced never reaps the engine.
        unique_future<void> table_drop_aborted(session_id_t session, uint64_t txn_id);

        // Wired by base_spaces before scheduler.start. Used to send the
        // on_subscriber_empty ack once dropped_table_agents_ empties.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap helpers, called from base_spaces::bootstrap_indexes_sync
        // BEFORE scheduler.start: single-threaded by construction, so direct
        // mutation of the manager's owned structures is safe. They seed the
        // manager from the catalog scan so it starts in steady state.

        // Empty in-memory index_engine_t per live table oid from the catalog scan.
        void bootstrap_engine_sync(components::catalog::oid_t oid);

        // Register one existing on-disk index (per alive pg_index row). The
        // owning disk-agent pointer is passed in (spawn stays in base_spaces);
        // its address is wired into engines_[oid] + disk_agents_per_oid_.
        void bootstrap_index_sync(components::catalog::oid_t table_oid,
                                  std::pmr::string name,
                                  components::logical_plan::index_type type,
                                  components::index::keys_base_storage_t keys,
                                  actor_zeta::address_t disk_agent_addr,
                                  index_agent_disk_ptr disk_agent_owned,
                                  disk_hash_table_ptr shared_hash_storage = nullptr);

        // Restore a dropped-table entry from pg_class.delete_id (alias of
        // mark_table_dropped_sync).
        void bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id);

        // Repopulate the in-memory index from a post-restart storage scan.
        // CHECKPOINT compaction renumbers physical row_ids from 0, but the
        // on-disk btree keeps pre-compact ids; without this rebuild, equality
        // lookups return stale row_ids that no longer map to live rows. The
        // on-disk btree is deliberately left untouched — its stale entries are
        // harmless (collection_t::fetch skips out-of-bounds row_ids) and DML
        // refreshes it over time.
        void bootstrap_repopulate_sync(components::catalog::oid_t table_oid,
                                       std::unique_ptr<components::vector::data_chunk_t> chunk,
                                       uint64_t row_count);

        // Collection lifecycle
        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

        // DML: txn-aware bulk index operations.
        unique_future<void> insert_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        uint64_t start_row_id,
                                        uint64_t count);
        unique_future<void> delete_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        std::pmr::vector<int64_t> row_ids);
        unique_future<void> update_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> old_data,
                                        std::unique_ptr<components::vector::data_chunk_t> new_data,
                                        std::pmr::vector<int64_t> row_ids,
                                        int64_t new_start_row_id);

        // MVCC commit/revert/cleanup. commit_* return core::error_t (no_error()
        // ↔ success) per the contract; the bitcask write path is assert+abort
        // terminal today, so success is currently the only value returned.
        // The batch form folds every oid's pending disk fan-out into a single
        // send-all-then-await-all pass (see the contract): the first
        // contains_error() across the batch is returned, but all awaits drain.
        unique_future<core::error_t> commit_inserts(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
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

        // Repopulate one table's indexes from a post-compact storage chunk: disk
        // agent clear() fan-out, in-memory engine clear, then txn_id=0 re-insert
        // of every row (storage_row = i). See index_contract for details.
        unique_future<void> repopulate_table(session_id_t session,
                                             components::catalog::oid_t table_oid,
                                             std::unique_ptr<components::vector::data_chunk_t> chunk,
                                             uint64_t row_count,
                                             core::date::timezone_offset_t session_tz);

        // DDL: index management
        unique_future<uint32_t> create_index(session_id_t session,
                                             components::catalog::oid_t table_oid,
                                             index_name_t index_name,
                                             components::index::keys_base_storage_t keys,
                                             components::logical_plan::index_type type,
                                             core::date::timezone_offset_t session_tz);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        // Query (txn-aware)
        unique_future<std::pmr::vector<int64_t>> search(session_id_t session,
                                                        components::catalog::oid_t table_oid,
                                                        components::index::keys_base_storage_t keys,
                                                        components::types::logical_value_t value,
                                                        components::expressions::compare_type compare,
                                                        uint64_t start_time,
                                                        uint64_t txn_id,
                                                        core::date::timezone_offset_t session_tz);

        unique_future<std::pmr::vector<int64_t>>
        search_with_preferred_type(session_id_t session,
                                   components::catalog::oid_t table_oid,
                                   components::index::keys_base_storage_t keys,
                                   components::types::logical_value_t value,
                                   components::expressions::compare_type compare,
                                   components::logical_plan::index_type preferred_type,
                                   uint64_t start_time,
                                   uint64_t txn_id,
                                   core::date::timezone_offset_t session_tz);

        unique_future<void> flush_all_indexes(session_id_t session);

        // Compact gate (see index_contract): returns the subset of the input
        // oids with NO engine in engines_ (safe to compact), input order
        // preserved; an engine means its positional row refs would break on compact.
        unique_future<std::pmr::vector<components::catalog::oid_t>>
        tables_without_indexes(session_id_t session, std::pmr::vector<components::catalog::oid_t> table_oids);

        // GC subscriber: erases dropped_table_agents_ entries whose
        // dropped_at_commit_id is below the new snapshot floor. When that map
        // drains it acks on_subscriber_empty(INDEX_KIND) to manager_dispatcher_,
        // clearing the selective-broadcast flag so no further on_horizon_advanced
        // arrives until a new DROP TABLE re-marks the subscriber.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // CREATE INDEX catchup handler (see index_contract): locates the engine
        // for (table_oid, index_oid) and applies the record's key effect.
        unique_future<void> apply_wal_record_for_index(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t index_oid,
                                                       uint64_t wal_record_id,
                                                       uint8_t record_type,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::unique_ptr<components::vector::data_chunk_t> physical_data,
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
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};

        // Per-collection in-memory index engines (keyed by table oid). Sole
        // owner — no engine state is shared with other actors. Populated by
        // bootstrap_engine_sync at startup or lazily by register_collection.
        std::pmr::unordered_map<components::catalog::oid_t, components::index::index_engine_ptr> engines_;

        // Dropped-table markers (oid -> dropped_at_commit_id). Populated by
        // mark_table_dropped[_sync]; drained by on_horizon_advanced once the
        // snapshot floor passes the commit_id, which erases engines_[oid] and
        // sends terminal drop messages to its disk_agents_per_oid_ entry.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> dropped_table_agents_;

        // Disk persistence actor addresses grouped by table oid; the commit_*
        // fan-out and on_horizon_advanced GC route through here.
        std::pmr::unordered_map<components::catalog::oid_t, std::pmr::vector<actor_zeta::address_t>>
            disk_agents_per_oid_;

        // Owning pointers to the disk agents, kept alive for the manager's
        // lifetime so the addresses in disk_agents_per_oid_ stay valid. Reaped
        // when the owning table is GC'd by on_horizon_advanced.
        std::vector<index_agent_disk_ptr> disk_agents_owned_;

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

        // Event-loop-in-thread state. The loop thread owns the in-flight
        // behavior list locally; senders only deliver into inbox_ and wake the
        // loop via pump_cv_. mutex_ guards ONLY the cv idle-wait — it is never
        // held across behavior creation, cont.resume() or behavior_t
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

        (void) enqueue_impl(std::move(msg));
        return std::move(future);
    }

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
