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
#include <components/catalog/catalog_codes.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/file/local_file_system.hpp>
#include <boost/lockfree/queue.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace services::index {

    // INDEXES_METADATA_FILENAME retired. Index metadata lives in
    // pg_catalog.pg_index now; this constant is kept as a comment so anyone reading
    // legacy data dirs can still recognize the filename.

    class manager_index_t final : public actor_zeta::actor::actor_mixin<manager_index_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t>;

        manager_index_t(
            std::pmr::memory_resource* resource,
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

        // Public — required for observability/tests. pending_msg holds the
        // deferred message until the loop thread creates the behavior coroutine.
        // behavior is move-only and lives in a std::pmr::list (local to the
        // loop thread) to keep iterators stable across push and resume
        // re-suspensions.
        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };

        // Producer-side enqueue for actor_mixin + coroutine behavior(). Senders
        // only deliver: the message is released into the lock-free inbox_ and
        // pump_cv_ is notified. ALL processing (behavior creation, resume,
        // cleanup) happens on the dedicated loop_thread_, which owns a local
        // std::pmr::list<in_flight_entry_t> and drains inbox_ each pass. No
        // locks are taken on the DML/DDL path.
        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
        requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>) [[nodiscard]] ReturnType
            enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args);

        void sync(address_pack pack);

        // Bootstrap helper — catalog scan rebuild populates this. Also called
        // internally by the mark_table_dropped mailbox handler. NOT a mailbox
        // handler — single-threaded callers only.
        void mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id);

        /// Runtime DROP TABLE path — sent from operator_dynamic_cascade_delete
        /// (inside the executor actor) so the (oid, dropped_at_commit_id) pair
        /// lands in dropped_table_agents_ for the next horizon-advance GC
        /// sweep. Thin coroutine wrapper around mark_table_dropped_sync.
        unique_future<void> mark_table_dropped(session_id_t session,
                                               components::catalog::oid_t table_oid,
                                               uint64_t dropped_at_commit_id);

        /// Bootstrap helper — base_spaces wires dispatcher address before
        /// scheduler.start. Used for on_subscriber_empty ack after
        /// dropped_table_agents_ empties.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Bootstrap helpers — called from base_spaces::bootstrap_indexes_sync
        // BEFORE scheduler.start, single-threaded by construction (direct
        // mutation is safe pre-start). They populate the manager's owned data
        // structures (engines_, disk_agents_owned_, disk_agents_per_oid_,
        // dropped_table_agents_) from the catalog scan results so the manager
        // starts steady state with a complete view.

        // Create an empty in-memory index_engine_t for an oid. Called once per
        // live table oid discovered by the catalog scan.
        void bootstrap_engine_sync(components::catalog::oid_t oid);

        // Register a single existing on-disk index. Called once per alive
        // pg_index row. Spawns/owns the disk persistence actor (the owning
        // pointer is passed in to keep spawn responsibility in base_spaces)
        // and wires its address into engines_[oid] + the disk_agents_per_oid_
        // fan-out map.
        void bootstrap_index_sync(components::catalog::oid_t table_oid,
                                  std::pmr::string name,
                                  components::logical_plan::index_type type,
                                  components::index::keys_base_storage_t keys,
                                  actor_zeta::address_t disk_agent_addr,
                                  index_agent_disk_ptr disk_agent_owned);

        // Restore a dropped-table entry recovered from pg_class.delete_id.
        // Alias of mark_table_dropped_sync kept under the bootstrap_* naming
        // so the bootstrap call site reads consistently.
        void bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id);

        // Repopulate the in-memory index from a post-restart storage scan.
        // CHECKPOINT compaction renumbers physical row_ids contiguously from 0,
        // but the on-disk btree retains pre-compact row_ids. Without rebuilding
        // from storage, post-restart equality lookups return stale row_ids
        // that no longer map to live storage rows. Called by base_spaces after
        // bootstrap_index_sync wires the engine, passing a freshly scanned
        // chunk so the in-memory storage_ holds rows keyed by current physical
        // row_ids. The on-disk btree is intentionally not touched here — its
        // stale entries are harmless because collection_t::fetch silently
        // skips out-of-bounds row_ids, and runtime DML will refresh the disk
        // btree over time.
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

        // MVCC commit/revert/cleanup. core::error_t carries terminal error
        // state for the commit path (no_error() ↔ success). The underlying
        // bitcask write path is assert+abort terminal today, but the
        // signature is the long-term shape so the executor commit-path can
        // already start handling result.contains_error() without a second
        // migration once core::error_t propagates upward.
        unique_future<core::error_t>
        commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<core::error_t>
        commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);
        unique_future<void> rebuild_indexes(session_id_t session, components::catalog::oid_t table_oid);

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

        unique_future<std::pmr::vector<int64_t>> search_with_preferred_type(
            session_id_t session,
            components::catalog::oid_t table_oid,
            components::index::keys_base_storage_t keys,
            components::types::logical_value_t value,
            components::expressions::compare_type compare,
            components::logical_plan::index_type preferred_type,
            uint64_t start_time,
            uint64_t txn_id,
            core::date::timezone_offset_t session_tz);


        unique_future<bool>
        has_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        unique_future<void> flush_all_indexes(session_id_t session);

        // event-driven GC subscriber. Walks dropped_table_agents_
        // and erases routing entries whose dropped_at_commit_id is below the
        // new snapshot floor. On drain sends on_subscriber_empty(INDEX_KIND)
        // ack to dispatcher via manager_dispatcher_ (wired by base_spaces
        // through set_manager_dispatcher_sync) so the selective-broadcast flag
        // clears and no further on_horizon_advanced broadcasts arrive until a
        // new DROP TABLE re-marks the subscriber.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Mailbox handler called per-WAL-record by
        // operator_create_index_backfill during the CREATE INDEX catchup
        // loop. Locates the index engine for the (table_oid, index_oid) pair
        // and applies the record's effect (insert / delete / update key) on
        // the build's in-memory index_engine_t.
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
                                                       &manager_index_t::commit_insert,
                                                       &manager_index_t::commit_delete,
                                                       &manager_index_t::revert_insert,
                                                       &manager_index_t::cleanup_all_versions,
                                                       &manager_index_t::rebuild_indexes,
                                                       &manager_index_t::create_index,
                                                       &manager_index_t::drop_index,
                                                       &manager_index_t::search,
                                                       &manager_index_t::search_with_preferred_type,
                                                       &manager_index_t::has_index,
                                                       &manager_index_t::flush_all_indexes,
                                                       &manager_index_t::get_indexed_keys,
                                                       &manager_index_t::get_indexed_descriptions,
                                                       &manager_index_t::on_horizon_advanced,
                                                       &manager_index_t::mark_table_dropped,
                                                       &manager_index_t::apply_wal_record_for_index>;

    private:

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        std::filesystem::path path_db_;
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};

        // Per-collection in-memory index engines (keyed by table oid). The
        // single, authoritative owner — manager_index_t is the sole holder (no
        // shared mutable state between actors). The map is populated either by
        // bootstrap_engine_sync at base_spaces startup (for catalog-known
        // tables) or lazily by register_collection (for runtime CREATE TABLE).
        std::pmr::unordered_map<components::catalog::oid_t, components::index::index_engine_ptr> engines_;

        // Dropped per-table marker map. Populated by mark_table_dropped_sync
        // (catalog scan rebuild) and mark_table_dropped (runtime DROP TABLE
        // path). Drained by on_horizon_advanced once the snapshot floor passes
        // the recorded commit_id; the corresponding engine in engines_ is
        // erased and disk_agents_per_oid_ entry is sent terminal drop messages.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> dropped_table_agents_;

        // Per-index disk persistence actor addresses, grouped by table oid.
        // Used by commit_insert / commit_delete fan-out and by on_horizon_advanced
        // GC. Populated by create_index (runtime CREATE INDEX) and
        // bootstrap_index_sync (catalog rebuild).
        std::pmr::unordered_map<components::catalog::oid_t,
                                 std::pmr::vector<actor_zeta::address_t>> disk_agents_per_oid_;

        // Owning collection of disk persistence agents. The owning pointer
        // remains here for the lifetime of the manager so addresses stored in
        // disk_agents_per_oid_ stay valid. Agents are reaped when the owning
        // table is GC'd by on_horizon_advanced.
        std::vector<index_agent_disk_ptr> disk_agents_owned_;

        // indexes_METADATA file + write/read/remove_indexes_from_metafile retired
        // — all index metadata lives in pg_catalog.pg_index now.
        core::filesystem::local_file_system_t fs_;

        // Address of manager_disk_t (for scan_segment when populating indexes)
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();

        // selective broadcast — base_spaces wires this before
        // scheduler.start via set_manager_dispatcher_sync. Used by
        // on_horizon_advanced to send on_subscriber_empty(INDEX_KIND) ack once
        // dropped_table_agents_ has drained.
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
        // lock-free inbox: senders only deliver; ALL processing happens on
        // loop_thread_. Stores raw message* (boost::lockfree requires
        // trivially-copyable): release() on push, re-wrapped into message_ptr
        // by the loop. Node allocations are non-PMR (infra queue).
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

        // Delivers into the lock-free inbox via the message_ptr overload.
        (void) enqueue_impl(std::move(msg));
        return std::move(future);
    }

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
