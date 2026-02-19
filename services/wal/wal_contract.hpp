#pragma once

#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

namespace services::wal {

    using session_id_t = components::session::session_id_t;

    struct wal_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        actor_zeta::unique_future<std::vector<record_t>> load(session_id_t session, id_t wal_id);

        actor_zeta::unique_future<id_t> create_index(session_id_t session,
            components::logical_plan::node_create_index_ptr data);
        actor_zeta::unique_future<id_t> drop_index(session_id_t session,
            components::logical_plan::node_drop_index_ptr data);

        actor_zeta::unique_future<id_t> commit_txn(session_id_t session,
            uint64_t transaction_id);

        actor_zeta::unique_future<id_t> write_physical_insert(
            session_id_t session,
            std::string database,
            std::string collection,
            std::unique_ptr<components::vector::data_chunk_t> data_chunk,
            uint64_t row_start,
            uint64_t row_count,
            uint64_t txn_id);

        actor_zeta::unique_future<id_t> write_physical_delete(
            session_id_t session,
            std::string database,
            std::string collection,
            std::pmr::vector<int64_t> row_ids,
            uint64_t count,
            uint64_t txn_id);

        actor_zeta::unique_future<id_t> write_physical_update(
            session_id_t session,
            std::string database,
            std::string collection,
            std::pmr::vector<int64_t> row_ids,
            std::unique_ptr<components::vector::data_chunk_t> new_data,
            uint64_t count,
            uint64_t txn_id);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &wal_contract::load,
            &wal_contract::create_index,
            &wal_contract::drop_index,
            &wal_contract::commit_txn,
            &wal_contract::write_physical_insert,
            &wal_contract::write_physical_delete,
            &wal_contract::write_physical_update
        >;

        wal_contract() = delete;
    };

} // namespace services::wal
