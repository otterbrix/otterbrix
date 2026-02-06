#pragma once

#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/address.hpp>

#include <components/document/document.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/base/operators/operator_write_data.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/result.hpp>
#include <services/disk/index_disk.hpp>
#include <services/wal/base.hpp>

namespace services::collection {
    class context_collection_t;
}

namespace services::disk {

    using session_id_t = components::session::session_id_t;
    using document_ids_t = components::base::operators::operator_write_data_t::ids_t;
    using index_name_t = std::string;

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        actor_zeta::unique_future<result_load_t> load(session_id_t session);

        actor_zeta::unique_future<void> load_indexes(session_id_t session, actor_zeta::address_t dispatcher_address);

        actor_zeta::unique_future<void> append_database(session_id_t session, database_name_t database);
        actor_zeta::unique_future<void> remove_database(session_id_t session, database_name_t database);

        actor_zeta::unique_future<void> append_collection(session_id_t session,
                                              database_name_t database,
                                              collection_name_t collection);
        actor_zeta::unique_future<void> remove_collection(session_id_t session,
                                              database_name_t database,
                                              collection_name_t collection);

        actor_zeta::unique_future<void> write_documents(session_id_t session,
                                            database_name_t database,
                                            collection_name_t collection,
                                            std::pmr::vector<components::document::document_ptr> documents);

        actor_zeta::unique_future<void> remove_documents(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             document_ids_t documents);

        actor_zeta::unique_future<void> flush(session_id_t session, services::wal::id_t wal_id);

        actor_zeta::unique_future<actor_zeta::address_t> create_index_agent(
            session_id_t session,
            components::logical_plan::node_create_index_ptr index,
            services::collection::context_collection_t* collection);
        actor_zeta::unique_future<void> drop_index_agent(session_id_t session,
                                             index_name_t index_name,
                                             services::collection::context_collection_t* collection);
        actor_zeta::unique_future<void> drop_index_agent_success(session_id_t session);

        actor_zeta::unique_future<void> index_insert_many(
            session_id_t session,
            index_name_t index_name,
            std::vector<std::pair<components::document::value_t, components::document::document_id_t>> values);
        actor_zeta::unique_future<void> index_insert(session_id_t session,
                                         index_name_t index_name,
                                         components::types::logical_value_t key,
                                         components::document::document_id_t doc_id);
        actor_zeta::unique_future<void> index_remove(session_id_t session,
                                         index_name_t index_name,
                                         components::types::logical_value_t key,
                                         components::document::document_id_t doc_id);

        actor_zeta::unique_future<void> index_insert_by_agent(session_id_t session,
                                                  actor_zeta::address_t agent_address,
                                                  components::types::logical_value_t key,
                                                  components::document::document_id_t doc_id);
        actor_zeta::unique_future<void> index_remove_by_agent(session_id_t session,
                                                  actor_zeta::address_t agent_address,
                                                  components::types::logical_value_t key,
                                                  components::document::document_id_t doc_id);
        actor_zeta::unique_future<index_disk_t::result> index_find_by_agent(
            session_id_t session,
            actor_zeta::address_t agent_address,
            components::types::logical_value_t key,
            components::expressions::compare_type compare);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &disk_contract::load,
            &disk_contract::load_indexes,
            &disk_contract::append_database,
            &disk_contract::remove_database,
            &disk_contract::append_collection,
            &disk_contract::remove_collection,
            &disk_contract::write_documents,
            &disk_contract::remove_documents,
            &disk_contract::flush,
            &disk_contract::create_index_agent,
            &disk_contract::drop_index_agent,
            &disk_contract::drop_index_agent_success,
            &disk_contract::index_insert_many,
            &disk_contract::index_insert,
            &disk_contract::index_remove,
            &disk_contract::index_insert_by_agent,
            &disk_contract::index_remove_by_agent,
            &disk_contract::index_find_by_agent
        >;

        disk_contract() = delete;
    };

}
