#pragma once

#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/address.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/result.hpp>
#include <services/wal/base.hpp>

namespace services::disk {

    using session_id_t = components::session::session_id_t;
    using document_ids_t = components::operators::operator_write_data_t::ids_t;

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

        actor_zeta::unique_future<void> write_data_chunk(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             std::unique_ptr<components::vector::data_chunk_t> data);
        actor_zeta::unique_future<void> remove_documents(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             document_ids_t documents);

        actor_zeta::unique_future<void> flush(session_id_t session, services::wal::id_t wal_id);

        // Storage management
        actor_zeta::unique_future<void> create_storage(session_id_t session,
                                                       collection_full_name_t name);
        actor_zeta::unique_future<void> create_storage_with_columns(
            session_id_t session,
            collection_full_name_t name,
            std::vector<components::table::column_definition_t> columns);
        actor_zeta::unique_future<void> drop_storage(session_id_t session,
                                                     collection_full_name_t name);

        // Storage queries
        actor_zeta::unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, collection_full_name_t name);
        actor_zeta::unique_future<uint64_t> storage_total_rows(session_id_t session,
                                                                collection_full_name_t name);
        actor_zeta::unique_future<uint64_t> storage_calculate_size(session_id_t session,
                                                                    collection_full_name_t name);
        actor_zeta::unique_future<std::vector<components::table::column_definition_t>>
        storage_columns(session_id_t session, collection_full_name_t name);
        actor_zeta::unique_future<bool> storage_has_schema(session_id_t session,
                                                            collection_full_name_t name);
        actor_zeta::unique_future<void> storage_adopt_schema(
            session_id_t session,
            collection_full_name_t name,
            std::pmr::vector<components::types::complex_logical_type> types);

        // Storage data operations
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     collection_full_name_t name,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit);
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      collection_full_name_t name,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        actor_zeta::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session,
                             collection_full_name_t name,
                             int64_t start,
                             uint64_t count);

        actor_zeta::unique_future<std::pair<uint64_t, uint64_t>> storage_append(
            session_id_t session,
            collection_full_name_t name,
            std::unique_ptr<components::vector::data_chunk_t> data);
        actor_zeta::unique_future<void> storage_update(
            session_id_t session,
            collection_full_name_t name,
            components::vector::vector_t row_ids,
            std::unique_ptr<components::vector::data_chunk_t> data);
        actor_zeta::unique_future<uint64_t> storage_delete_rows(
            session_id_t session,
            collection_full_name_t name,
            components::vector::vector_t row_ids,
            uint64_t count);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &disk_contract::load,
            &disk_contract::load_indexes,
            &disk_contract::append_database,
            &disk_contract::remove_database,
            &disk_contract::append_collection,
            &disk_contract::remove_collection,
            &disk_contract::write_data_chunk,
            &disk_contract::remove_documents,
            &disk_contract::flush,
            // Storage management
            &disk_contract::create_storage,
            &disk_contract::create_storage_with_columns,
            &disk_contract::drop_storage,
            // Storage queries
            &disk_contract::storage_types,
            &disk_contract::storage_total_rows,
            &disk_contract::storage_calculate_size,
            &disk_contract::storage_columns,
            &disk_contract::storage_has_schema,
            &disk_contract::storage_adopt_schema,
            // Storage data operations
            &disk_contract::storage_scan,
            &disk_contract::storage_fetch,
            &disk_contract::storage_scan_segment,
            &disk_contract::storage_append,
            &disk_contract::storage_update,
            &disk_contract::storage_delete_rows
        >;

        disk_contract() = delete;
    };

} // namespace services::disk
