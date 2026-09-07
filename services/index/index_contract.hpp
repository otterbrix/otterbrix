#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/index/forward.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <core/result_wrapper.hpp>

namespace services::index {

    using session_id_t = components::session::session_id_t;
    using transaction_data = components::table::transaction_data;
    using execution_context_t = components::execution_context_t;

    // One contiguous run of physical row ids: [row_start, row_start + row_count).
    struct index_row_range_t {
        uint64_t row_start{0};
        uint64_t row_count{0};
    };

    // built_compact_epoch is captured before the send, so a rebuild can only make it too LOW -- a refusal, not a lie.
    struct index_search_result_t {
        std::pmr::vector<int64_t> row_ids;
        uint64_t built_compact_epoch{0};
    };

    struct index_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        unique_future<void> register_collection(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<void> unregister_collection(session_id_t session, components::catalog::oid_t table_oid);

        // Rows are indexed in vector order with contiguous row-ids based at start_row_id/new_start_row_id.
        unique_future<core::error_t> insert_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                                 uint64_t start_row_id,
                                                 uint64_t count);
        unique_future<core::error_t> delete_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                                 std::pmr::vector<int64_t> row_ids);
        unique_future<core::error_t> update_rows(execution_context_t ctx,
                                                 components::catalog::oid_t table_oid,
                                                 std::pmr::vector<components::vector::data_chunk_t> old_data,
                                                 std::pmr::vector<components::vector::data_chunk_t> new_data,
                                                 std::pmr::vector<int64_t> row_ids,
                                                 int64_t new_start_row_id);

        // The manager decides "must these rows be staged?" since the plan-time stamp can be stale.
        unique_future<std::pmr::vector<index_row_range_t>>
        unmirrored_ranges(execution_context_t ctx,
                          components::catalog::oid_t table_oid,
                          std::pmr::vector<index_row_range_t> ranges);

        // Not mirrors: commit_deletes defers to on_horizon_advanced to avoid hiding a row a reader still owns.
        unique_future<core::error_t> commit_inserts(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(execution_context_t ctx,
                                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                                    uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> revert_delete(execution_context_t ctx, components::catalog::oid_t table_oid);
        unique_future<void> cleanup_all_versions(session_id_t session, uint64_t lowest_active);
        // Excludes oids mid-GC (dropped_table_agents_); the vacuum operator repopulate_table's each.
        unique_future<std::pmr::vector<components::catalog::oid_t>> all_indexed_oids(session_id_t session);

        // Re-inserts rows keyed by physical id from chunk.row_ids, not position (compaction shifts positions).
        unique_future<core::error_t> repopulate_table(session_id_t session,
                                                      components::catalog::oid_t table_oid,
                                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                      uint64_t row_count,
                                                      core::date::timezone_offset_t session_tz,
                                                      uint64_t built_compact_epoch);

        // index_oid = pg_index.indexrelid, the index's only identity below the planner boundary.
        unique_future<core::error_t> create_index(session_id_t session,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t index_oid,
                                             components::index::keys_base_storage_t keys,
                                             components::logical_plan::index_type type,
                                             core::date::timezone_offset_t session_tz,
                                             uint64_t built_compact_epoch);
        unique_future<void>
        drop_index(session_id_t session, components::catalog::oid_t table_oid, components::catalog::oid_t index_oid);

        // Result is wrapped since a bare empty vector conflated "no engine", "no index", and a real empty match.
        unique_future<core::result_wrapper_t<index_search_result_t>>
        search(session_id_t session,
               components::catalog::oid_t table_oid,
               components::index::keys_base_storage_t keys,
               components::types::logical_value_t value,
               components::expressions::compare_type compare,
               uint64_t start_time,
               uint64_t txn_id,
               core::date::timezone_offset_t session_tz);
        unique_future<core::result_wrapper_t<index_search_result_t>>
        search_with_preferred_type(session_id_t session,
                                   components::catalog::oid_t table_oid,
                                   components::index::keys_base_storage_t keys,
                                   components::types::logical_value_t value,
                                   components::expressions::compare_type compare,
                                   components::logical_plan::index_type preferred_index_type,
                                   uint64_t start_time,
                                   uint64_t txn_id,
                                   core::date::timezone_offset_t session_tz);

        // Reports the first refusal, or an unflushed index could read as flushed to a truncating checkpoint.
        unique_future<core::error_t> flush_all_indexes(session_id_t session);

        // compact() shifts row positions, invalidating positional refs an in-memory index holds.
        unique_future<std::pmr::vector<components::catalog::oid_t>>
        tables_without_indexes(session_id_t session, std::pmr::vector<components::catalog::oid_t> table_oids);

        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
        get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid);
        unique_future<std::pmr::vector<components::index::index_description_t>>
        get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid);

        // Drains dropped_table_agents_ first, then deferred_deletes_; acks only once both are empty.
        unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Pairs with manager_dispatcher_t::on_drop_resource_marked(INDEX_KIND) for the next GC sweep.
        unique_future<void>
        mark_table_dropped(session_id_t session, components::catalog::oid_t table_oid, uint64_t dropped_at_commit_id);

        // mark_table_dropped recorded the value in TXN-ID space; this remaps it once commit_id is known.
        unique_future<void> table_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // Abort mirror of table_dropped_committed: erases, not remaps, since the table must stay indexed.
        unique_future<void> table_drop_aborted(session_id_t session, uint64_t txn_id);

        // Only the insert leg is applied; an undecided delete could withhold an id from a still-live row.
        unique_future<void> apply_wal_record_for_index(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       components::catalog::oid_t index_oid,
                                                       uint64_t wal_record_id,
                                                       uint8_t record_type,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::pmr::vector<components::vector::data_chunk_t> physical_data,
                                                       uint64_t physical_row_start,
                                                       uint64_t txn_id,
                                                       core::date::timezone_offset_t session_tz);

        using dispatch_traits = actor_zeta::dispatch_traits<&index_contract::register_collection,
                                                            &index_contract::unregister_collection,
                                                            &index_contract::insert_rows,
                                                            &index_contract::delete_rows,
                                                            &index_contract::update_rows,
                                                            &index_contract::unmirrored_ranges,
                                                            &index_contract::commit_inserts,
                                                            &index_contract::commit_deletes,
                                                            &index_contract::revert_insert,
                                                            &index_contract::revert_delete,
                                                            &index_contract::cleanup_all_versions,
                                                            &index_contract::all_indexed_oids,
                                                            &index_contract::repopulate_table,
                                                            &index_contract::create_index,
                                                            &index_contract::drop_index,
                                                            &index_contract::search,
                                                            &index_contract::search_with_preferred_type,
                                                            &index_contract::flush_all_indexes,
                                                            &index_contract::tables_without_indexes,
                                                            &index_contract::get_indexed_keys,
                                                            &index_contract::get_indexed_descriptions,
                                                            &index_contract::on_horizon_advanced,
                                                            &index_contract::mark_table_dropped,
                                                            &index_contract::table_dropped_committed,
                                                            &index_contract::table_drop_aborted,
                                                            &index_contract::apply_wal_record_for_index>;

        index_contract() = delete;
    };

} // namespace services::index
