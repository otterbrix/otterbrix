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
    // A DELETE must never cause one (clear+rebuild), unlike VACUUM/CHECKPOINT.
    uint64_t index_repopulations() noexcept;
    void reset_index_repopulations() noexcept;

    uint64_t index_agent_reads() noexcept;
    void reset_index_agent_reads() noexcept;

    // Should stay O(chunks), not regress to O(rows).
    uint64_t index_key_column_probes() noexcept;
    void reset_index_key_column_probes() noexcept;

    // Never reset: an ever-climbing number names a pinned snapshot, not a leak.
    uint64_t index_deferred_deletes() noexcept;

    uint64_t index_stage_insert_batches() noexcept;
    void reset_index_stage_insert_batches() noexcept;

    uint64_t index_stage_insert_foreign_batches() noexcept;
#endif

    // Manager holds routing only; rows/search/per-txn state live on the agent.
    struct index_record_t {
        components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
        components::index::keys_base_storage_t keys;
        components::logical_plan::index_type type{components::logical_plan::index_type::no_valid};
        bool ordered{false};
        actor_zeta::address_t address{actor_zeta::address_t::empty_address()};
        // Read before the builder's table scan, so an interleaving compact leaves the stamp too low.
        uint64_t built_compact_epoch{0};
    };

    using index_records_t = std::pmr::vector<index_record_t>;

    [[nodiscard]] const index_record_t* match_index_relid(const index_records_t& records,
                                                          components::catalog::oid_t index_oid) noexcept;

    // no_valid matches nothing by construction.
    [[nodiscard]] const index_record_t* match_index(const index_records_t& records,
                                                    const components::index::keys_base_storage_t& keys,
                                                    components::logical_plan::index_type type);

    // Untyped lookup picks the ordered index first: an unordered index refuses a range predicate
    // an ordered twin could answer.
    [[nodiscard]] const index_record_t* match_index(const index_records_t& records,
                                                    const components::index::keys_base_storage_t& keys);

    [[nodiscard]] std::pmr::vector<components::index::keys_base_storage_t>
    indexed_keys(const index_records_t& records, std::pmr::memory_resource* resource);

    [[nodiscard]] std::pmr::vector<components::index::index_description_t>
    indexed_descriptions(const index_records_t& records, std::pmr::memory_resource* resource);

    // Multi-column keys: only the first key's column is checked (todo on the index side).
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

        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
        requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>) [[nodiscard]] ReturnType
            enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args);

        void mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id);

        unique_future<void>
        mark_table_dropped(session_id_t session, components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);

        // dropped_table_agents_[oid] is recorded in TXN-ID space (>= 2^62), rewritten here once commit_id is allocated.
        unique_future<void> table_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        unique_future<void> table_drop_aborted(session_id_t session, uint64_t txn_id);

        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        // Empty != absent: absent means CREATE INDEX on this table would be a bookkeeping bug.
        void bootstrap_engine_sync(components::catalog::oid_t oid);

        [[nodiscard]] core::error_t bootstrap_index_sync(components::catalog::oid_t table_oid,
                                                         components::catalog::oid_t index_oid,
                                                         components::logical_plan::index_type type,
                                                         components::index::keys_base_storage_t keys,
                                                         std::pmr::set<std::uint64_t> committed_commit_ids);

        void bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id);

#ifdef DEV_MODE
        // Raw, non-owning handles (an address_t can't be resumed).
        [[nodiscard]] std::pmr::vector<bitcask_index_agent_t*> owned_bitcask_agents_sync();
        [[nodiscard]] std::pmr::vector<btree_index_agent_t*> owned_btree_agents_sync();
#endif

        struct pending_index_rebuild_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
            components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
        };

        // An index named here holds pre-compact row ids and must not be wired.
        [[nodiscard]] std::pmr::vector<pending_index_rebuild_t> pending_index_rebuilds_sync() const;

        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

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

        unique_future<std::pmr::vector<index_row_range_t>>
        unmirrored_ranges(execution_context_t ctx,
                          components::catalog::oid_t table_oid,
                          std::pmr::vector<index_row_range_t> ranges);

        unique_future<core::error_t> commit_inserts(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        // Does not touch a store: records the batch, sent once on_horizon_advanced allows it.
        unique_future<core::error_t> commit_deletes(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> revert_delete(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);

        unique_future<std::pmr::vector<components::catalog::oid_t>> all_indexed_oids(session_id_t session);

        unique_future<core::error_t> repopulate_table(session_id_t session,
                                                      components::catalog::oid_t table_oid,
                                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                      uint64_t row_count,
                                                      core::date::timezone_offset_t session_tz,
                                                      uint64_t built_compact_epoch);

        // Refuses loudly rather than answering with an index that does not exist.
        unique_future<core::error_t> create_index(session_id_t session,
                                                  components::catalog::oid_t table_oid,
                                                  components::catalog::oid_t index_oid,
                                                  components::index::keys_base_storage_t keys,
                                                  components::logical_plan::index_type type,
                                                  core::date::timezone_offset_t session_tz,
                                                  uint64_t built_compact_epoch);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

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

        // An engine's positional row refs would break on compact.
        unique_future<std::pmr::vector<components::catalog::oid_t>>
        tables_without_indexes(session_id_t session, std::pmr::vector<components::catalog::oid_t> table_oids);

        // Drains dropped_table_agents_ then deferred_deletes_: reaping a table takes its held-back erases with it.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

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
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};

        std::pmr::unordered_map<components::catalog::oid_t, index_records_t> indexes_per_oid_;

        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> dropped_table_agents_;

        // The erase waits for the horizon to pass its commit_id; rows stay in the agent's own
        // pending_deletes_, this only holds the schedule.
        struct deferred_delete_t {
            components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
            components::catalog::oid_t index_oid{components::catalog::INVALID_OID};
            uint64_t txn_id{0};
            // Must stay distinct from txn_id (reuse bug in bitcask_index_disk.cpp's recover_txn_log).
            uint64_t commit_id{0};
        };

        // Unbounded on purpose: evicting an entry would mean publishing an erase early.
        std::pmr::vector<deferred_delete_t> deferred_deletes_;

        // apply_wal_record_for_index returns void, so a refusal is recorded here and checked at commit_inserts.
        std::pmr::unordered_map<uint64_t, core::error_t> catchup_failures_;

        // The mirror ledger: what unmirrored_ranges subtracts a statement's appends against.
        std::pmr::unordered_map<
            uint64_t,
            std::pmr::unordered_map<components::catalog::oid_t, std::pmr::vector<index_row_range_t>>>
            mirrored_ranges_;

        // Durable marker for the gap between committing a compacted table and rebuilding its
        // indexes, so a kill -9 inside that gap survives the restart. One line per pending pair:
        //     ${path_db_}/index_rebuild_pending      "<table_oid> <index_oid>"
        [[nodiscard]] std::filesystem::path rebuild_marker_path_() const;
        [[nodiscard]] std::pmr::vector<pending_index_rebuild_t> read_rebuild_marker_() const;
        [[nodiscard]] core::error_t
        write_rebuild_marker_(const std::pmr::vector<pending_index_rebuild_t>& pending) const;
        [[nodiscard]] core::error_t arm_rebuild_marker_();
        [[nodiscard]] core::error_t clear_rebuild_marker_(components::catalog::oid_t table_oid,
                                                          const index_records_t& rebuilt);
        [[nodiscard]] core::error_t forget_rebuild_marker_entry_(components::catalog::oid_t table_oid,
                                                                 components::catalog::oid_t index_oid);

        void forget_deferred_deletes(components::catalog::oid_t table_oid);
        void forget_deferred_deletes(components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // Two vectors, not one polymorphic vector: the owning deleter returns sizeof(STATIC T) to
        // the pool, so erasing through a common base would free the wrong size.
        std::pmr::vector<bitcask_index_agent_ptr> bitcask_agents_owned_;
        std::pmr::vector<btree_index_agent_ptr> btree_agents_owned_;

        struct detached_agents_t {
            std::pmr::vector<bitcask_index_agent_ptr> bitcask;
            std::pmr::vector<btree_index_agent_ptr> btree;

            explicit detached_agents_t(std::pmr::memory_resource* resource)
                : bitcask(resource)
                , btree(resource) {}

            [[nodiscard]] bool empty() const noexcept { return bitcask.empty() && btree.empty(); }
        };

        // A detached agent keeps its own mailbox. Destroying it while something is still queued makes
        // close_impl CANCEL that message (actor-zeta impl/mailbox/default_mailbox.ipp), and a
        // cancellation is indistinguishable from a lost one; kept alive, the agent answers it itself,
        // because drop() set is_dropped_ and every other handler refuses on that. Pinned by
        // test_index_agent_lifetime.cpp.
        //
        // Freed by the next horizon advance, which is when the scheduler has long since drained them.
        detached_agents_t parked_agents_;

        void park_detached(detached_agents_t&& dying);

        // `type`/`ordered` come out: a composite index is built by the ordered family but published as `single`.
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

        // Agents are matched by asking each one which table it serves, so there is no second map to disagree with.
        [[nodiscard]] detached_agents_t detach_table_agents(components::catalog::oid_t table_oid);

        // DROP INDEX: sibling indexes must stay registered.
        [[nodiscard]] detached_agents_t detach_index(components::catalog::oid_t table_oid,
                                                     components::catalog::oid_t index_oid);

        // schedule_agent() can't find these pointers in the manager's vectors anymore.
        [[nodiscard]] std::pmr::vector<unique_future<void>> send_drop_to_detached(detached_agents_t& dying,
                                                                                  session_id_t session);

        core::filesystem::local_file_system_t fs_;

        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};

        void schedule_agent(const actor_zeta::address_t& addr, bool needs_sched);

        std::pmr::vector<unique_future<void>> pending_void_;
        void poll_pending();

        // mutex_ guards only the cv idle-wait, so the DML/DDL path stays lock-free.
        std::mutex mutex_;
        std::condition_variable pump_cv_;
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
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

        // Checked, not cast away: unaddressed, `future` would hang forever.
        if (enqueue_impl(std::move(msg)).second != actor_zeta::detail::enqueue_result::success) {
            error(log_, "manager_index_t::enqueue_impl: message refused; its reply will never arrive");
        }
        return std::move(future);
    }

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
