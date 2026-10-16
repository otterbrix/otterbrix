#pragma once

// Shared committed-row scan helper for the disk service. Extracted from
// manager_disk_impl.hpp so BOTH the manager TUs and agent_disk.cpp can use it
// (agent-side catalog DDL handlers scan their own slice on the agent thread).

#include <components/table/column_state.hpp>        // storage_index_t
#include <components/table/data_table.hpp>          // data_table_t
#include <components/table/row_version_manager.hpp> // transaction_data
#include <components/table/table_state.hpp>         // table_scan_state
#include <components/vector/data_chunk.hpp>         // data_chunk_t
#include <components/vector/indexing_vector.hpp>    // DEFAULT_VECTOR_CAPACITY

#include <initializer_list>
#include <iterator>
#include <memory_resource>
#include <vector>

namespace services::disk::detail {

    // ---------------------------------------------------------------------------
    // inline_scan: scan `table` as `txn` sees it; fn(chunk, row_index) per visible
    // row, false stops early. chunk.data[i] holds col_indices[i].
    // txn is not optional. transaction_data::committed() leaves snapshot_horizon at its
    // maximum, so it sees every COMMITTED row and no pending write — pass whatever txn a preceding
    // txn-carrying read (read_chunks_by_key, scan_by_keys, ...) used, or the scan disagrees with it
    // about visibility and misreads "not found".
    // ---------------------------------------------------------------------------

    namespace detail_impl_ {
        template<typename Range, typename Fn>
        void inline_scan_range(components::table::data_table_t& table,
                               const Range& col_indices,
                               std::pmr::memory_resource* resource,
                               components::table::transaction_data txn,
                               Fn&& fn) {
            std::vector<components::table::storage_index_t> col_ids;
            const auto& all_cols = table.columns();
            std::pmr::vector<components::types::complex_logical_type> read_types(resource);
            read_types.reserve(static_cast<std::size_t>(std::distance(std::begin(col_indices), std::end(col_indices))));
            for (auto idx : col_indices) {
                col_ids.emplace_back(static_cast<uint64_t>(idx));
                read_types.push_back(all_cols[static_cast<std::size_t>(idx)].type());
            }

            components::table::table_scan_state state(resource);
            table.initialize_scan(state, col_ids, components::table::transaction_data::committed());
            // initialize_scan resets both states to the default snapshot; re-stamp txn
            // after it (same pattern as table_storage_adapter_t's txn-aware scans).
            state.table_state.txn = txn;
            state.local_state.txn = txn;

            while (true) {
                components::vector::data_chunk_t chunk(resource,
                                                       read_types,
                                                       components::vector::DEFAULT_VECTOR_CAPACITY);
                table.scan(chunk, state);
                if (chunk.size() == 0)
                    break;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (!fn(chunk, i))
                        return;
                }
            }
        }
    } // namespace detail_impl_

    template<typename Fn>
    void inline_scan(components::table::data_table_t& table,
                     std::initializer_list<std::int64_t> col_indices,
                     std::pmr::memory_resource* resource,
                     components::table::transaction_data txn,
                     Fn&& fn) {
        detail_impl_::inline_scan_range(table, col_indices, resource, txn, std::forward<Fn>(fn));
    }

    template<typename Fn>
    void inline_scan(components::table::data_table_t& table,
                     const std::vector<std::int64_t>& col_indices,
                     std::pmr::memory_resource* resource,
                     components::table::transaction_data txn,
                     Fn&& fn) {
        detail_impl_::inline_scan_range(table, col_indices, resource, txn, std::forward<Fn>(fn));
    }

    // const overload: data_table_t::scan is read-only but not declared const,
    // so the const_cast is safe.
    template<typename Fn>
    void inline_scan(const components::table::data_table_t& table,
                     std::initializer_list<std::int64_t> col_indices,
                     std::pmr::memory_resource* resource,
                     components::table::transaction_data txn,
                     Fn&& fn) {
        detail_impl_::inline_scan_range(const_cast<components::table::data_table_t&>(table),
                                        col_indices,
                                        resource,
                                        txn,
                                        std::forward<Fn>(fn));
    }

    template<typename Fn>
    void inline_scan(const components::table::data_table_t& table,
                     const std::vector<std::int64_t>& col_indices,
                     std::pmr::memory_resource* resource,
                     components::table::transaction_data txn,
                     Fn&& fn) {
        detail_impl_::inline_scan_range(const_cast<components::table::data_table_t&>(table),
                                        col_indices,
                                        resource,
                                        txn,
                                        std::forward<Fn>(fn));
    }

} // namespace services::disk::detail
