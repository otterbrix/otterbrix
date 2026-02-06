#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>

#include <boost/filesystem.hpp>
#include <components/log/log.hpp>

#include <components/configuration/configuration.hpp>
#include <components/session/session.hpp>
#include <core/executor.hpp>
#include <core/spinlock/spinlock.hpp>

#include "base.hpp"
#include "wal.hpp"
#include "wal_contract.hpp"

#include <components/logical_plan/param_storage.hpp>

namespace services::wal {

    class manager_wal_replicate_t final : public actor_zeta::actor::actor_mixin<manager_wal_replicate_t> {
        using session_id_t = components::session::session_id_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t
        {
            manager_disk = 0,
            manager_dispatcher = 1
        };

        manager_wal_replicate_t(std::pmr::memory_resource*,
                                actor_zeta::scheduler_raw,
                                configuration::config_wal,
                                log_t&);
        ~manager_wal_replicate_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]]
        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(address_pack pack);

        unique_future<std::vector<record_t>> load(session_id_t session, services::wal::id_t wal_id);
        unique_future<services::wal::id_t> create_database(session_id_t session, components::logical_plan::node_create_database_ptr data);
        unique_future<services::wal::id_t> drop_database(session_id_t session, components::logical_plan::node_drop_database_ptr data);
        unique_future<services::wal::id_t> create_collection(session_id_t session, components::logical_plan::node_create_collection_ptr data);
        unique_future<services::wal::id_t> drop_collection(session_id_t session, components::logical_plan::node_drop_collection_ptr data);
        unique_future<services::wal::id_t> insert_one(session_id_t session, components::logical_plan::node_insert_ptr data);
        unique_future<services::wal::id_t> insert_many(session_id_t session, components::logical_plan::node_insert_ptr data);
        unique_future<services::wal::id_t> delete_one(session_id_t session,
                                       components::logical_plan::node_delete_ptr data,
                                       components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> delete_many(session_id_t session,
                                        components::logical_plan::node_delete_ptr data,
                                        components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> update_one(session_id_t session,
                                       components::logical_plan::node_update_ptr data,
                                       components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> update_many(session_id_t session,
                                        components::logical_plan::node_update_ptr data,
                                        components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> create_index(session_id_t session, components::logical_plan::node_create_index_ptr data);

        using dispatch_traits = actor_zeta::implements<
            wal_contract,
            &manager_wal_replicate_t::load,
            &manager_wal_replicate_t::create_database,
            &manager_wal_replicate_t::drop_database,
            &manager_wal_replicate_t::create_collection,
            &manager_wal_replicate_t::drop_collection,
            &manager_wal_replicate_t::insert_one,
            &manager_wal_replicate_t::insert_many,
            &manager_wal_replicate_t::delete_one,
            &manager_wal_replicate_t::delete_many,
            &manager_wal_replicate_t::update_one,
            &manager_wal_replicate_t::update_many,
            &manager_wal_replicate_t::create_index
        >;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        configuration::config_wal config_;
        log_t log_;

        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_dispatcher_ = actor_zeta::address_t::empty_address();
        std::unordered_map<std::string, actor_zeta::address_t> dispatcher_to_address_book_;
        std::vector<wal_replicate_ptr> dispatchers_;
        spin_lock lock_;

        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<std::vector<record_t>>> pending_load_;

        void create_wal_worker(int count_worker);
        void poll_pending();

        actor_zeta::behavior_t current_behavior_;
    };

    class manager_wal_replicate_empty_t final
        : public actor_zeta::actor::actor_mixin<manager_wal_replicate_empty_t> {
        using session_id_t = components::session::session_id_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        manager_wal_replicate_empty_t(std::pmr::memory_resource*, actor_zeta::scheduler::sharing_scheduler*, log_t&);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;
        void sync(address_pack pack);

        unique_future<std::vector<record_t>> load(session_id_t session, services::wal::id_t wal_id);
        unique_future<services::wal::id_t> create_database(session_id_t session, components::logical_plan::node_create_database_ptr data);
        unique_future<services::wal::id_t> drop_database(session_id_t session, components::logical_plan::node_drop_database_ptr data);
        unique_future<services::wal::id_t> create_collection(session_id_t session, components::logical_plan::node_create_collection_ptr data);
        unique_future<services::wal::id_t> drop_collection(session_id_t session, components::logical_plan::node_drop_collection_ptr data);
        unique_future<services::wal::id_t> insert_one(session_id_t session, components::logical_plan::node_insert_ptr data);
        unique_future<services::wal::id_t> insert_many(session_id_t session, components::logical_plan::node_insert_ptr data);
        unique_future<services::wal::id_t> delete_one(session_id_t session,
                                       components::logical_plan::node_delete_ptr data,
                                       components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> delete_many(session_id_t session,
                                        components::logical_plan::node_delete_ptr data,
                                        components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> update_one(session_id_t session,
                                       components::logical_plan::node_update_ptr data,
                                       components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> update_many(session_id_t session,
                                        components::logical_plan::node_update_ptr data,
                                        components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> create_index(session_id_t session, components::logical_plan::node_create_index_ptr data);

        using dispatch_traits = actor_zeta::implements<
            wal_contract,
            &manager_wal_replicate_empty_t::load,
            &manager_wal_replicate_empty_t::create_database,
            &manager_wal_replicate_empty_t::drop_database,
            &manager_wal_replicate_empty_t::create_collection,
            &manager_wal_replicate_empty_t::drop_collection,
            &manager_wal_replicate_empty_t::insert_one,
            &manager_wal_replicate_empty_t::insert_many,
            &manager_wal_replicate_empty_t::delete_one,
            &manager_wal_replicate_empty_t::delete_many,
            &manager_wal_replicate_empty_t::update_one,
            &manager_wal_replicate_empty_t::update_many,
            &manager_wal_replicate_empty_t::create_index
        >;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler::sharing_scheduler* scheduler_;
        log_t log_;
        std::pmr::vector<unique_future<services::wal::id_t>> pending_void_;
    };

} //namespace services::wal