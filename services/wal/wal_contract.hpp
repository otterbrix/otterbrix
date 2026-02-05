#pragma once

#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

namespace services::wal {

    using session_id_t = components::session::session_id_t;

    struct wal_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        actor_zeta::unique_future<std::vector<record_t>> load(session_id_t session, id_t wal_id);

        actor_zeta::unique_future<id_t> create_database(session_id_t session,
            components::logical_plan::node_create_database_ptr data);
        actor_zeta::unique_future<id_t> drop_database(session_id_t session,
            components::logical_plan::node_drop_database_ptr data);

        actor_zeta::unique_future<id_t> create_collection(session_id_t session,
            components::logical_plan::node_create_collection_ptr data);
        actor_zeta::unique_future<id_t> drop_collection(session_id_t session,
            components::logical_plan::node_drop_collection_ptr data);

        actor_zeta::unique_future<id_t> insert_one(session_id_t session,
            components::logical_plan::node_insert_ptr data);
        actor_zeta::unique_future<id_t> insert_many(session_id_t session,
            components::logical_plan::node_insert_ptr data);

        actor_zeta::unique_future<id_t> delete_one(session_id_t session,
            components::logical_plan::node_delete_ptr data,
            components::logical_plan::parameter_node_ptr params);
        actor_zeta::unique_future<id_t> delete_many(session_id_t session,
            components::logical_plan::node_delete_ptr data,
            components::logical_plan::parameter_node_ptr params);

        actor_zeta::unique_future<id_t> update_one(session_id_t session,
            components::logical_plan::node_update_ptr data,
            components::logical_plan::parameter_node_ptr params);
        actor_zeta::unique_future<id_t> update_many(session_id_t session,
            components::logical_plan::node_update_ptr data,
            components::logical_plan::parameter_node_ptr params);

        actor_zeta::unique_future<id_t> create_index(session_id_t session,
            components::logical_plan::node_create_index_ptr data);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &wal_contract::load,
            &wal_contract::create_database,
            &wal_contract::drop_database,
            &wal_contract::create_collection,
            &wal_contract::drop_collection,
            &wal_contract::insert_one,
            &wal_contract::insert_many,
            &wal_contract::delete_one,
            &wal_contract::delete_many,
            &wal_contract::update_one,
            &wal_contract::update_many,
            &wal_contract::create_index
        >;

        wal_contract() = delete;
    };

} // namespace services::wal
