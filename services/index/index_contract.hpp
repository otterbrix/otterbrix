#pragma once

#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/index/forward.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

namespace services::index {

    using session_id_t = components::session::session_id_t;
    using index_name_t = std::string;

    struct index_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Collection lifecycle
        unique_future<void> register_collection(session_id_t session,
                                                 collection_full_name_t name);
        unique_future<void> unregister_collection(session_id_t session,
                                                   collection_full_name_t name);

        // DML: bulk index operations
        unique_future<void> insert_rows(session_id_t session,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> data,
                                         uint64_t start_row_id,
                                         uint64_t count);
        unique_future<void> delete_rows(session_id_t session,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> data,
                                         std::pmr::vector<size_t> row_ids);
        unique_future<void> update_rows(session_id_t session,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> old_data,
                                         std::unique_ptr<components::vector::data_chunk_t> new_data,
                                         std::pmr::vector<size_t> row_ids);

        // DDL: index management
        unique_future<uint32_t> create_index(session_id_t session,
                                              collection_full_name_t name,
                                              index_name_t index_name,
                                              components::index::keys_base_storage_t keys,
                                              components::logical_plan::index_type type);
        unique_future<void> drop_index(session_id_t session,
                                        collection_full_name_t name,
                                        index_name_t index_name);

        // Query
        unique_future<std::pmr::vector<int64_t>> search(
            session_id_t session,
            collection_full_name_t name,
            components::index::keys_base_storage_t keys,
            components::types::logical_value_t value,
            components::expressions::compare_type compare);

        unique_future<bool> has_index(session_id_t session,
                                       collection_full_name_t name,
                                       index_name_t index_name);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &index_contract::register_collection,
            &index_contract::unregister_collection,
            &index_contract::insert_rows,
            &index_contract::delete_rows,
            &index_contract::update_rows,
            &index_contract::create_index,
            &index_contract::drop_index,
            &index_contract::search,
            &index_contract::has_index
        >;

        index_contract() = delete;
    };

} // namespace services::index
