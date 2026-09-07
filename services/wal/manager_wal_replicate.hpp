#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <services/wal/wal.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>

#include <boost/lockfree/queue.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

namespace services::wal {

#ifdef DEV_MODE
    uint64_t auto_checkpoint_rounds() noexcept;
    void reset_auto_checkpoint_rounds() noexcept;
#endif

    class manager_wal_replicate_t final : public actor_zeta::actor::actor_mixin<manager_wal_replicate_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using session_id_t = components::session::session_id_t;

        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };

#ifdef DEV_MODE
        // Guards against spawning a worker per storage namespace dir: test_wal_storage_namespace_dirs.
        std::size_t active_worker_count() const noexcept { return wal_actors_.size(); }
#endif

        // disk/index feed auto-checkpoint; the dispatcher's mailbox arrives later via set_manager_dispatcher_sync.
        manager_wal_replicate_t(std::pmr::memory_resource* resource,
                                actor_zeta::scheduler_raw scheduler,
                                configuration::config_wal config,
                                log_t& log,
                                actor_zeta::address_t disk_address,
                                actor_zeta::address_t index_address);
        ~manager_wal_replicate_t();

        std::pmr::memory_resource* resource() const noexcept;
        const char* make_type() const noexcept;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);
        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        // base_spaces wires this before scheduler.start; empty in test topologies without a dispatcher.
        void set_manager_dispatcher_sync(actor_zeta::address_t address);

        unique_future<core::result_wrapper_t<std::vector<record_t>>> load(session_id_t session, wal::id_t wal_id);

        // commit_id is written into the COMMIT record so replay can rebuild published_horizon_.
        unique_future<core::result_wrapper_t<wal::id_t>> commit_txn(session_id_t session,
                                                                    uint64_t txn_id,
                                                                    wal_sync_mode sync_mode,
                                                                    components::catalog::oid_t database_oid,
                                                                    uint64_t commit_id);

        unique_future<core::error_t> truncate_before(session_id_t session, wal::id_t checkpoint_wal_id);

        unique_future<wal::id_t> current_wal_id(session_id_t session);

        // Self-sent by commit_txn when WAL growth trips the threshold; fire-and-forget.
        unique_future<void> run_auto_checkpoint(session_id_t session);

        unique_future<core::result_wrapper_t<wal::id_t>>
        write_physical_insert(session_id_t session,
                              components::catalog::oid_t table_oid,
                              std::pmr::vector<components::vector::data_chunk_t> chunks,
                              uint64_t row_start,
                              uint64_t row_count,
                              uint64_t txn_id,
                              components::catalog::oid_t database_oid);

        unique_future<core::result_wrapper_t<wal::id_t>> write_physical_delete(session_id_t session,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::pmr::vector<int64_t> row_ids,
                                                                               uint64_t count,
                                                                               uint64_t txn_id,
                                                                               components::catalog::oid_t database_oid);

        unique_future<core::result_wrapper_t<wal::id_t>>
        write_physical_update(session_id_t session,
                              components::catalog::oid_t table_oid,
                              std::pmr::vector<int64_t> row_ids,
                              std::pmr::vector<components::vector::data_chunk_t> new_data,
                              uint64_t count,
                              uint64_t txn_id,
                              components::catalog::oid_t database_oid);

        unique_future<core::result_wrapper_t<wal::id_t>>
        write_physical_add_column(session_id_t session,
                                  components::catalog::oid_t table_oid,
                                  std::unique_ptr<components::vector::data_chunk_t> schema_chunk,
                                  uint64_t column_count,
                                  uint64_t txn_id,
                                  components::catalog::oid_t database_oid);

        using dispatch_traits = actor_zeta::implements<wal_contract,
                                                       &manager_wal_replicate_t::load,
                                                       &manager_wal_replicate_t::commit_txn,
                                                       &manager_wal_replicate_t::truncate_before,
                                                       &manager_wal_replicate_t::current_wal_id,
                                                       &manager_wal_replicate_t::run_auto_checkpoint,
                                                       &manager_wal_replicate_t::write_physical_insert,
                                                       &manager_wal_replicate_t::write_physical_delete,
                                                       &manager_wal_replicate_t::write_physical_update,
                                                       &manager_wal_replicate_t::write_physical_add_column>;

        wal::id_t next_wal_id();

        // Reads only; commit_txn resets the counter, run_auto_checkpoint rebases after the checkpoint.
        bool needs_auto_checkpoint() const noexcept {
            return config_.on && config_.auto_checkpoint_threshold_bytes > 0 &&
                   wal_bytes_since_checkpoint_.load(std::memory_order_relaxed) >=
                       config_.auto_checkpoint_threshold_bytes;
        }
        void reset_auto_checkpoint_bytes() noexcept { wal_bytes_since_checkpoint_.store(0, std::memory_order_relaxed); }
        // Rebases on the CURRENT directory size, called after a checkpoint truncates the WAL.
        void rebase_auto_checkpoint_window() noexcept {
            wal_bytes_at_last_checkpoint_.store(total_wal_bytes(), std::memory_order_relaxed);
            reset_auto_checkpoint_bytes();
        }

        // The one exit of every round, so an abandoned round releases the dedup guard just like a completed one.
        void end_auto_checkpoint_round() noexcept;

        std::uintmax_t total_wal_bytes() const noexcept;

    private:
        wal_worker_t* get_or_create_worker(components::catalog::oid_t database_oid);

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        configuration::config_wal config_;
        log_t log_;
        bool enabled_;
        atomic_id_t global_id_{0};
        // Written from the commit_txn coroutine, read by the dispatcher thread via needs_auto_checkpoint().
        std::atomic<std::uintmax_t> wal_bytes_since_checkpoint_{0};

        // Holding the TOTAL directory size in the "since" counter instead makes every commit after
        // the first threshold trip re-trip it -- measured at one checkpoint per commit, 1009 of
        // them for 10k rows.
        std::atomic<std::uintmax_t> wal_bytes_at_last_checkpoint_{0};

        actor_zeta::address_t manager_disk_;
        actor_zeta::address_t manager_dispatcher_;
        actor_zeta::address_t manager_index_;

        // Single-actor state on loop_thread_ only, no atomic; prevents a commit burst from stacking checkpoints.
        bool auto_checkpoint_in_flight_{false};

        std::unordered_map<components::catalog::oid_t, wal_worker_ptr> wal_actors_;

        // Set when the ctor's segment scan couldn't read a segment; while set, every write/commit/truncate refuses.
        core::error_t recovery_error_;

        std::pmr::vector<unique_future<void>> pending_auto_checkpoint_{resource_};
        void poll_auto_checkpoint_();

        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_; // guards the idle wait condition only.
        std::condition_variable pump_cv_;
    };

} // namespace services::wal
