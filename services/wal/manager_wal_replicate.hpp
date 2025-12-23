#pragma once

#include <actor-zeta.hpp>

#include <boost/filesystem.hpp>
#include <components/log/log.hpp>

#include <components/configuration/configuration.hpp>
#include <components/session/session.hpp>
#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>

#include "base.hpp"
#include "route.hpp"
#include "wal.hpp"

#include <components/logical_plan/param_storage.hpp>

namespace services::wal {

    class manager_wal_replicate_t final : public actor_zeta::cooperative_supervisor<manager_wal_replicate_t> {
        using session_id_t = components::session::session_id_t;

    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t
        {
            manager_disk = 0,
            manager_dispatcher = 1
        };

        void sync(address_pack& pack) {
            manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
            manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
        }

        manager_wal_replicate_t(std::pmr::memory_resource*,
                                actor_zeta::scheduler_raw,
                                configuration::config_wal,
                                log_t&);
        ~manager_wal_replicate_t() override;

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char*;

        void create_wal_worker();
        void load(const session_id_t& session, services::wal::id_t wal_id);
        void create_database(const session_id_t& session, components::logical_plan::node_create_database_ptr data);
        void drop_database(const session_id_t& session, components::logical_plan::node_drop_database_ptr data);
        void create_collection(const session_id_t& session, components::logical_plan::node_create_collection_ptr data);
        void drop_collection(const session_id_t& session, components::logical_plan::node_drop_collection_ptr data);
        void insert_one(const session_id_t& session, components::logical_plan::node_insert_ptr data);
        void insert_many(const session_id_t& session, components::logical_plan::node_insert_ptr data);
        void delete_one(const session_id_t& session,
                        components::logical_plan::node_delete_ptr data,
                        components::logical_plan::parameter_node_ptr params);
        void delete_many(const session_id_t& session,
                         components::logical_plan::node_delete_ptr data,
                         components::logical_plan::parameter_node_ptr params);
        void update_one(const session_id_t& session,
                        components::logical_plan::node_update_ptr data,
                        components::logical_plan::parameter_node_ptr params);
        void update_many(const session_id_t& session,
                         components::logical_plan::node_update_ptr data,
                         components::logical_plan::parameter_node_ptr params);
        void create_index(const session_id_t& session, components::logical_plan::node_create_index_ptr data);

    private:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;

        actor_zeta::scheduler_raw e_;
        configuration::config_wal config_;
        log_t log_;
        // Behaviors
        actor_zeta::behavior_t create_;
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t create_database_;
        actor_zeta::behavior_t drop_database_;
        actor_zeta::behavior_t create_collection_;
        actor_zeta::behavior_t drop_collection_;
        actor_zeta::behavior_t insert_one_;
        actor_zeta::behavior_t insert_many_;
        actor_zeta::behavior_t delete_one_;
        actor_zeta::behavior_t delete_many_;
        actor_zeta::behavior_t update_one_;
        actor_zeta::behavior_t update_many_;
        actor_zeta::behavior_t core_sync_;
        actor_zeta::behavior_t create_index_;

        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_dispatcher_ = actor_zeta::address_t::empty_address();
        std::unordered_map<std::string, actor_zeta::address_t> dispatcher_to_address_book_;
        std::vector<wal_replicate_ptr> dispatchers_;
        spin_lock lock_;
    };

    class manager_wal_replicate_empty_t final
        : public actor_zeta::cooperative_supervisor<manager_wal_replicate_empty_t> {
        using session_id_t = components::session::session_id_t;
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

    public:
        //TODO continue
        manager_wal_replicate_empty_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, log_t&);
        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char*;

    private:
        log_t log_;
        actor_zeta::behavior_t always_success_;

        auto enqueue_impl(actor_zeta::message_ptr, actor_zeta::execution_unit*) -> void override;
        actor_zeta::scheduler_raw e_;
        spin_lock lock_;

        auto always_success(const session_id_t& session, components::logical_plan::node_ptr&) -> void {
            actor_zeta::send(current_message()->sender(),
                             address(),
                             services::wal::handler_id(services::wal::route::success),
                             session,
                             services::wal::id_t(0));
        }
    };

} //namespace services::wal
