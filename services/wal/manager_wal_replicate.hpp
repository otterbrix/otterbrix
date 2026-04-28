#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <services/wal/wal_contract.hpp>
#include <services/wal/wal.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>

#include <functional>
#include <thread>
#include <unordered_map>

namespace services::wal {

    class manager_wal_replicate_t final
        : public actor_zeta::actor::actor_mixin<manager_wal_replicate_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;
        using session_id_t = components::session::session_id_t;

        manager_wal_replicate_t(std::pmr::memory_resource* resource,
                                actor_zeta::scheduler_raw scheduler,
                                configuration::config_wal config,
                                log_t& log);
        ~manager_wal_replicate_t();

        std::pmr::memory_resource* resource() const noexcept;
        const char* make_type() const noexcept;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);
        std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        using run_fn_t = std::function<void()>;

        void sync(address_pack pack);
        void set_run_fn(run_fn_t fn) { run_fn_ = std::move(fn); }

        // Contract handlers
        unique_future<std::vector<record_t>> load(session_id_t session, wal::id_t wal_id);

        unique_future<wal::id_t> commit_txn(session_id_t session,
                                       uint64_t txn_id,
                                       wal_sync_mode sync_mode,
                                       std::string database_name);

        unique_future<void> truncate_before(session_id_t session, wal::id_t checkpoint_wal_id);

        unique_future<wal::id_t> current_wal_id(session_id_t session);

        unique_future<wal::id_t>
        write_physical_insert(session_id_t session,
                              std::string database,
                              std::string collection,
                              std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                              uint64_t row_start,
                              uint64_t row_count,
                              uint64_t txn_id);

        unique_future<wal::id_t>
        write_physical_delete(session_id_t session,
                              std::string database,
                              std::string collection,
                              std::pmr::vector<int64_t> row_ids,
                              uint64_t count,
                              uint64_t txn_id);

        unique_future<wal::id_t>
        write_physical_update(session_id_t session,
                              std::string database,
                              std::string collection,
                              std::pmr::vector<int64_t> row_ids,
                              std::unique_ptr<components::vector::data_chunk_t> new_data,
                              uint64_t count,
                              uint64_t txn_id);

        using dispatch_traits =
            actor_zeta::implements<wal_contract,
                                   &manager_wal_replicate_t::load,
                                   &manager_wal_replicate_t::commit_txn,
                                   &manager_wal_replicate_t::truncate_before,
                                   &manager_wal_replicate_t::current_wal_id,
                                   &manager_wal_replicate_t::write_physical_insert,
                                   &manager_wal_replicate_t::write_physical_delete,
                                   &manager_wal_replicate_t::write_physical_update>;

        // Global WAL ID counter — shared across all per-database workers.
        wal::id_t next_wal_id();

    private:
        wal_worker_t* get_or_create_worker(const std::string& database);

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        configuration::config_wal config_;
        log_t log_;
        bool enabled_;
        atomic_id_t global_id_{0};

        actor_zeta::address_t manager_disk_;
        actor_zeta::address_t manager_dispatcher_;

        std::unordered_map<std::string, wal_worker_ptr> wal_actors_;

        run_fn_t run_fn_{[] { std::this_thread::yield(); }};
        actor_zeta::behavior_t current_behavior_;
    };

} // namespace services::wal
