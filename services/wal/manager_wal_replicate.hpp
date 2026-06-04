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

namespace services::wal {

    class manager_wal_replicate_t final : public actor_zeta::actor::actor_mixin<manager_wal_replicate_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;
        using session_id_t = components::session::session_id_t;

        // Public so observability/tests can inspect. The event loop owns the
        // lifetime of each entry in its local in_flight list.
        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };

        manager_wal_replicate_t(std::pmr::memory_resource* resource,
                                actor_zeta::scheduler_raw scheduler,
                                configuration::config_wal config,
                                log_t& log);
        ~manager_wal_replicate_t();

        std::pmr::memory_resource* resource() const noexcept;
        const char* make_type() const noexcept;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);
        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(address_pack pack);

        // Contract handlers.
        unique_future<std::vector<record_t>> load(session_id_t session, wal::id_t wal_id);

        // commit_id (MVCC version from transaction_manager_t::commit()) is
        // written into the COMMIT record so replay can rebuild published_horizon_.
        unique_future<wal::id_t> commit_txn(session_id_t session,
                                            uint64_t txn_id,
                                            wal_sync_mode sync_mode,
                                            components::catalog::oid_t database_oid,
                                            uint64_t commit_id);

        unique_future<void> truncate_before(session_id_t session, wal::id_t checkpoint_wal_id);

        unique_future<wal::id_t> current_wal_id(session_id_t session);

        unique_future<wal::id_t> auto_checkpoint_wal_id(session_id_t session);

        unique_future<wal::id_t> write_physical_insert(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                                                       uint64_t row_start,
                                                       uint64_t row_count,
                                                       uint64_t txn_id,
                                                       components::catalog::oid_t database_oid);

        unique_future<wal::id_t> write_physical_delete(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       uint64_t count,
                                                       uint64_t txn_id,
                                                       components::catalog::oid_t database_oid);

        unique_future<wal::id_t> write_physical_update(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::unique_ptr<components::vector::data_chunk_t> new_data,
                                                       uint64_t count,
                                                       uint64_t txn_id,
                                                       components::catalog::oid_t database_oid);

        // Mailbox twins of the _sync helpers for callers that run inside an
        // actor (e.g. operator_create_index_backfill in the executor) and so
        // cannot make sync inter-actor calls. Same active_build_start_positions_ set.
        unique_future<void> register_active_build(session_id_t session, wal::id_t build_start_wal_position);
        unique_future<void> unregister_active_build(session_id_t session, wal::id_t build_start_wal_position);

        using dispatch_traits = actor_zeta::implements<wal_contract,
                                                       &manager_wal_replicate_t::load,
                                                       &manager_wal_replicate_t::commit_txn,
                                                       &manager_wal_replicate_t::truncate_before,
                                                       &manager_wal_replicate_t::current_wal_id,
                                                       &manager_wal_replicate_t::auto_checkpoint_wal_id,
                                                       &manager_wal_replicate_t::write_physical_insert,
                                                       &manager_wal_replicate_t::write_physical_delete,
                                                       &manager_wal_replicate_t::write_physical_update,
                                                       &manager_wal_replicate_t::register_active_build,
                                                       &manager_wal_replicate_t::unregister_active_build>;

        // Global WAL ID counter — shared across all per-database workers.
        wal::id_t next_wal_id();

        // Returns true (and resets the flag) if WAL bytes since last checkpoint exceeded the
        // configured threshold. The caller (dispatcher execute_ddl_inline) then triggers
        // checkpoint_all on disk and calls reset_auto_checkpoint_bytes() after the checkpoint.
        bool needs_auto_checkpoint() const noexcept {
            return config_.on && config_.auto_checkpoint_threshold_bytes > 0 &&
                   wal_bytes_since_checkpoint_.load(std::memory_order_relaxed) >=
                       config_.auto_checkpoint_threshold_bytes;
        }
        void reset_auto_checkpoint_bytes() noexcept {
            wal_bytes_since_checkpoint_.store(0, std::memory_order_relaxed);
        }

        // Compute total WAL directory bytes by scanning segment files.
        std::uintmax_t total_wal_bytes() const noexcept;

        // Retention guard helpers. operator_create_index registers its
        // build_start_wal_position before backfill and unregisters on publish or
        // failure. Sync (not mailbox handlers) because the operator pipeline
        // calls them directly, outside the mailbox. This is safe only while that
        // pipeline runs single-threaded relative to the wal dispatcher; multi-DB
        // would force these onto the mailbox.
        void register_active_build_sync(wal::id_t build_start_wal_position);
        void unregister_active_build_sync(wal::id_t build_start_wal_position);

    private:
        // Workers keyed by database_oid; today every record routes to the
        // main_database worker (single-worker model).
        wal_worker_t* get_or_create_worker(components::catalog::oid_t database_oid);

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        configuration::config_wal config_;
        log_t log_;
        bool enabled_;
        atomic_id_t global_id_{0};
        // Atomic — written from commit_txn coroutine, read by the dispatcher
        // thread via needs_auto_checkpoint(). Plain uintmax_t raced.
        std::atomic<std::uintmax_t> wal_bytes_since_checkpoint_{0};

        actor_zeta::address_t manager_disk_;
        actor_zeta::address_t manager_dispatcher_;

        std::unordered_map<components::catalog::oid_t, wal_worker_ptr> wal_actors_;

        // Retention guard: build_start_wal_position of every in-flight CREATE
        // INDEX backfill. truncate_before clamps to min(this set) so concurrent
        // catchup never misses a truncated record. Empty => no clamp.
        std::pmr::set<wal::id_t> active_build_start_positions_{resource_};

        // Event loop runs on its own thread. Senders only deliver into inbox_;
        // ALL message processing (behavior creation, coroutine resume, cleanup)
        // happens on loop_thread_. See manager_dispatcher_t for the same model.
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // Stores raw message* (boost::lockfree requires trivially-copyable):
        // release() on push, re-wrapped into message_ptr by the loop. Node
        // allocations are non-PMR (infra queue).
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_; // guards the idle wait condition only.
        std::condition_variable pump_cv_;
    };

} // namespace services::wal
