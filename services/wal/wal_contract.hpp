#pragma once

#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_sync_mode.hpp>

namespace services::wal {

    using session_id_t = components::session::session_id_t;

    struct wal_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        actor_zeta::unique_future<std::vector<record_t>> load(session_id_t session, id_t wal_id);

        actor_zeta::unique_future<id_t> commit_txn(session_id_t session,
                                                   uint64_t transaction_id,
                                                   wal_sync_mode sync_mode,
                                                   components::catalog::oid_t database_oid,
                                                   uint64_t commit_id);

        actor_zeta::unique_future<void> truncate_before(session_id_t session, id_t checkpoint_wal_id);

        actor_zeta::unique_future<id_t> current_wal_id(session_id_t session);

        // Returns current wal_id and resets the byte counter if the auto-checkpoint threshold
        // has been exceeded; returns 0 if no checkpoint is needed. Called by dispatcher after
        // commit_txn to decide whether to trigger checkpoint_all + truncate_before.
        actor_zeta::unique_future<id_t> auto_checkpoint_wal_id(session_id_t session);

        // database_oid identifies the target WAL worker for multi-database
        // routing. Operators pass it; manager_wal_replicate routes via
        // wal_actors_[database_oid]. Until catalog_resolve_database_t populates
        // execution_context_t.database_oid, callers pass main_database.
        actor_zeta::unique_future<id_t>
        write_physical_insert(session_id_t session,
                              components::catalog::oid_t table_oid,
                              std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                              uint64_t row_start,
                              uint64_t row_count,
                              uint64_t txn_id,
                              components::catalog::oid_t database_oid);

        actor_zeta::unique_future<id_t> write_physical_delete(session_id_t session,
                                                              components::catalog::oid_t table_oid,
                                                              std::pmr::vector<int64_t> row_ids,
                                                              uint64_t count,
                                                              uint64_t txn_id,
                                                              components::catalog::oid_t database_oid);

        actor_zeta::unique_future<id_t>
        write_physical_update(session_id_t session,
                              components::catalog::oid_t table_oid,
                              std::pmr::vector<int64_t> row_ids,
                              std::unique_ptr<components::vector::data_chunk_t> new_data,
                              uint64_t count,
                              uint64_t txn_id,
                              components::catalog::oid_t database_oid);

        // Retention guard for CREATE INDEX Phase 2.5 catchup.
        // operator_create_index_backfill (running inside the executor actor)
        // registers its build_start_wal_position before backfill and unregisters
        // on success or fail; truncate_before clamps to min(active set) so the
        // catchup loop never observes truncated records.
        actor_zeta::unique_future<void> register_active_build(session_id_t session, id_t build_start_wal_position);
        actor_zeta::unique_future<void> unregister_active_build(session_id_t session, id_t build_start_wal_position);

        using dispatch_traits = actor_zeta::dispatch_traits<&wal_contract::load,
                                                            &wal_contract::commit_txn,
                                                            &wal_contract::truncate_before,
                                                            &wal_contract::current_wal_id,
                                                            &wal_contract::auto_checkpoint_wal_id,
                                                            &wal_contract::write_physical_insert,
                                                            &wal_contract::write_physical_delete,
                                                            &wal_contract::write_physical_update,
                                                            &wal_contract::register_active_build,
                                                            &wal_contract::unregister_active_build>;

        wal_contract() = delete;
    };

} // namespace services::wal
