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
    // inline_scan: scan a data_table_t AS `txn` SEES IT, projecting the given column
    // indices.  Calls fn(chunk, row_index) for every visible row; returning false from
    // fn stops the scan early.
    //
    // THE TRANSACTION IS NOT OPTIONAL, and it used not to be passed at all. The scan
    // this helper drives is a REGULAR one (data_table_t::scan -> row_group_t::scan ->
    // templated_scan<REGULAR>), so collection_scan_state::txn is what decides which rows
    // exist for it, and a default-constructed transaction_data{0, 0} is NOT "see
    // everything": row_version_manager_t reads it as horizon 0 with no owning
    // transaction, i.e. only rows whose insert id is literally 0 — the direct writes
    // made outside any explicit transaction. Rows a transaction has appended but not yet
    // committed carry insert_id == transaction_id and are invisible to it, and so are
    // rows any earlier transaction committed (their insert id is a commit id, which is
    // above horizon 0).
    //
    // That mismatch is a defect generator: a caller that READ a row through a
    // txn-carrying route (read_chunks_by_key, scan_by_keys, ...) and then scans for the
    // same row here would not find it, and every such body reads "not found" as an
    // answer about the catalog rather than about its own snapshot. Passing the SAME
    // transaction_data the read used is the whole contract; `transaction_data{}` is a
    // deliberate "committed direct writes only" and has to be written out at the call
    // site to be chosen.
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
            // row_group_t::scan_committed writes to result.data[column.primary_index()] —
            // i.e. it indexes by storage column position, not by row in `col_ids`. So the
            // chunk must have a slot at every storage column index that appears in
            // col_indices. Use the projected_cols ctor to allocate buffers only for the
            // requested columns (other slots are placeholders, no data buffer).
            std::pmr::vector<components::types::complex_logical_type> all_types(resource);
            all_types.reserve(all_cols.size());
            for (const auto& c : all_cols) {
                all_types.push_back(c.type());
            }
            std::vector<size_t> projected;
            projected.reserve(static_cast<std::size_t>(std::distance(std::begin(col_indices), std::end(col_indices))));
            for (auto idx : col_indices) {
                col_ids.emplace_back(static_cast<uint64_t>(idx));
                projected.push_back(static_cast<std::size_t>(idx));
            }

            components::table::table_scan_state state(resource);
            table.initialize_scan(state, col_ids);
            // Same two-line stamp table_storage_adapter_t's txn-aware scans use:
            // initialize_scan leaves both collection states on their default snapshot, and
            // the scan reads them, not this frame's argument.
            state.table_state.txn = txn;
            state.local_state.txn = txn;

            while (true) {
                components::vector::data_chunk_t chunk(resource,
                                                       all_types,
                                                       projected,
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
