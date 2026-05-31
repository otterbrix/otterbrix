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
#include "index_result.hpp"
#include "index_table_agent.hpp"
#include <components/catalog/catalog_codes.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/file/local_file_system.hpp>
#include <mutex>
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
        using run_fn_t = std::function<void()>;

        manager_index_t(
            std::pmr::memory_resource* resource,
            actor_zeta::scheduler_raw scheduler,
            log_t& log,
            std::filesystem::path path_db = {},
            uint64_t bitcask_flush_threshold = 1000,
            uint64_t bitcask_segment_record_limit = 100,
            uint64_t btree_flush_threshold = 1000,
            run_fn_t run_fn = [] { std::this_thread::yield(); });
        ~manager_index_t() = default;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(address_pack pack);

        // Block D V4 bootstrap helper — called from base_spaces Phase 3.5 before
        // scheduler start. NOT a regular message handler; direct call only
        // during bootstrap (rule 11 base_spaces exception).
        void register_table_agent_sync(components::catalog::oid_t oid, actor_zeta::address_t addr);

        // Block D V4 bootstrap helper — exposes the per-oid index_engine_t raw
        // pointer so base_spaces Phase 3.5 can hand it to the matching
        // index_table_agent_t via set_engine_sync. Returns nullptr when no
        // engine has been registered for the oid yet (engines_ is populated
        // lazily by register_collection). The returned pointer is heap-stable
        // because engines_ stores core::pmr::unique_ptr<index_engine_t>: the
        // engine object lives behind the unique_ptr, not in the map slot, so
        // unordered_map rehashes do not invalidate the address.
        // NOT a mailbox handler — direct call only during base_spaces
        // bootstrap before scheduler start (rule 11 exception).
        components::index::index_engine_t* engine_for_oid_sync(components::catalog::oid_t oid) const noexcept;

        // Block D Final cleanup — engine ownership migration. Pops the
        // index_engine_t for `oid` out of engines_ (creating one on demand if
        // the entry was absent, mirroring register_collection's lazy-init
        // semantics) and returns it as a core::pmr::unique_ptr. The caller
        // (today: base_spaces Phase 3.5) is expected to immediately hand the
        // result to the matching index_table_agent_t::set_engine_owned_sync.
        //
        // After this call, engines_[oid] has no entry and the manager-side
        // engines_ fallbacks for routed DML / query handlers will miss. That
        // is INTENTIONAL — for migrated oids the per_table_agents_ router path
        // owns the engine and answers all requests; the engines_ fallback only
        // remains for oids that have not yet been migrated (e.g. runtime
        // CREATE TABLE before its agent is spawned).
        //
        // NOT a mailbox handler — direct call only during base_spaces
        // bootstrap before scheduler start (rule 11 exception).
        components::index::index_engine_ptr take_engine_ownership_sync(components::catalog::oid_t oid);

        // Bootstrap helper — dec 37 catalog scan rebuild populates this. Also
        // called internally by the mark_table_dropped mailbox handler. NOT a
        // mailbox handler — single-threaded callers only.
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

        // MVCC commit/revert/cleanup
        // Block B Parallel A.B3: commit_insert / commit_delete now carry their
        // error state in a typed result_t (kept as `success` today because the
        // underlying bitcask write path is assert+abort terminal — see
        // index_result.hpp). The signature is the long-term shape so the
        // executor commit-path can already start handling `result.failed()`
        // without a second migration once core::error_t propagates upward.
        unique_future<result_t>
        commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<result_t>
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

        // dec 33 V1 event-driven GC subscriber. Walks dropped_table_agents_
        // and erases routing entries whose dropped_at_commit_id is below the
        // new snapshot floor. On drain sends on_subscriber_empty(INDEX_KIND)
        // ack to dispatcher via manager_dispatcher_ (wired by base_spaces
        // through set_manager_dispatcher_sync) so the selective-broadcast flag
        // clears and no further on_horizon_advanced broadcasts arrive until a
        // new DROP TABLE re-marks the subscriber.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Block C §3.5 dec 31 V1.d — mailbox handler called per-WAL-record by
        // operator_create_index_backfill during Phase 2.5 catchup. Locates the
        // index engine for the (table_oid, index_oid) pair and applies the
        // record's effect (insert / delete / update key) on the build's
        // in-memory index_engine_t.
        //
        // V1.d real-impl: the chunk + storage-row metadata travel with the
        // message so the handler can call engine->insert_row /
        // mark_delete_row directly. INSERT, DELETE, and UPDATE are all
        // fully wired:
        //   - INSERT: physical_data carries the WAL chunk; insert each row
        //     starting at physical_row_start.
        //   - DELETE: WAL ships only row_ids, so the operator pre-fetches
        //     the OLD key chunk via storage_fetch(row_ids) and forwards it
        //     as physical_data; the handler loops mark_delete_row over the
        //     recovered chunk.
        //   - UPDATE: operator splits into two messages — original UPDATE
        //     (NEW-insert half, uses rec.physical_data) followed by a
        //     synthesized DELETE carrying the recovered OLD chunk
        //     (OLD-delete half, same recovery pattern as standalone DELETE).
        // Best-effort fall-through when storage_fetch can't recover rows
        // (rows physically gone, no disk in test harness): handler logs +
        // skips; the V1.a bounded-retry guard in the operator catches
        // persistent divergence.
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
        run_fn_t run_fn_;
        log_t log_;
        std::filesystem::path path_db_;
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};
        std::mutex mutex_;

        // Per-collection in-memory index engines (keyed by table oid).
        //
        // Block D Final cleanup — engine ownership migration STATUS: partial.
        // The agent now exposes set_engine_owned_sync and this manager exposes
        // take_engine_ownership_sync; base_spaces Phase 3.5 calls the pair so
        // every oid known at bootstrap has its engine moved into the owning
        // index_table_agent_t. For migrated oids this map has no entry —
        // routed DML/query handlers reach the agent via per_table_agents_ and
        // never look up engines_.
        //
        // The map is still populated lazily by register_collection for
        // oids without a registered per-table agent (e.g. runtime CREATE
        // TABLE before base_spaces has had a chance to bootstrap an agent
        // for the new oid). The engines_-based DDL handlers (create_index /
        // drop_index / apply_wal_record_for_index) continue to operate on
        // engines_[oid] for those legacy paths. Migrating DDL to forward
        // through the per-table agent is the next cleanup step — after that,
        // engines_ can be removed entirely.
        //
        // Constraint #11 (no shared mutable state between actors): for the
        // migrated oids the per-table agent is the sole owner; nothing in
        // manager_index touches the engine_t once take_engine_ownership_sync
        // has handed it off.
        std::pmr::unordered_map<components::catalog::oid_t, components::index::index_engine_ptr> engines_;

        // Block D: per-table agent addresses (keyed by table oid). Populated via
        // register_table_agent_sync during base_spaces bootstrap. Used by later
        // Block D commits to route per-table DML/search messages to the owning
        // index_table_agent_t.
        std::pmr::unordered_map<components::catalog::oid_t, actor_zeta::address_t> per_table_agents_;

        // dec 33 V1 symmetric GC — oids whose owning table has been dropped,
        // keyed by the commit_id at which the drop became visible. Populated
        // by mark_table_dropped_sync (catalog scan rebuild + DROP TABLE
        // operator). Drained by on_horizon_advanced once the snapshot floor
        // passes the recorded commit_id.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> dropped_table_agents_;

        // Per-index disk persistence (child actors)
        std::vector<index_agent_disk_ptr> disk_agents_;

        // indexes_METADATA file + write/read/remove_indexes_from_metafile retired
        // — all index metadata lives in pg_catalog.pg_index now.
        core::filesystem::local_file_system_t fs_;

        // Address of manager_disk_t (for scan_segment when populating indexes)
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();

        // dec 33/38 selective broadcast — base_spaces wires this before
        // scheduler.start via set_manager_dispatcher_sync. Used by
        // on_horizon_advanced to send on_subscriber_empty(INDEX_KIND) ack once
        // dropped_table_agents_ has drained.
        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};

        // Find disk agent by address and schedule it if needed
        void schedule_agent(const actor_zeta::address_t& addr, bool needs_sched);

        // Pending futures
        std::pmr::vector<unique_future<void>> pending_void_;
        void poll_pending();

        actor_zeta::behavior_t current_behavior_;
    };

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
