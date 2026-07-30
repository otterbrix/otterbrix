#pragma once

// Shared committed-row scan helper for the disk service. Extracted from
// manager_disk_impl.hpp so BOTH the manager TUs and agent_disk.cpp can use it
// (agent-side catalog DDL handlers scan their own slice on the agent thread).

#include <components/table/column_state.hpp>     // storage_index_t
#include <components/table/data_table.hpp>       // data_table_t
#include <components/table/table_state.hpp>      // table_scan_state
#include <components/vector/data_chunk.hpp>      // data_chunk_t
#include <components/vector/indexing_vector.hpp> // DEFAULT_VECTOR_CAPACITY

#include <initializer_list>
#include <iterator>
#include <memory_resource>
#include <vector>

namespace services::disk::detail {

    // ---------------------------------------------------------------------------
    // inline_scan: scan all committed rows of a data_table_t, projecting the given
    // column indices.  Calls fn(chunk, row_index) for every row; returning false
    // from fn stops the scan early.
    // ---------------------------------------------------------------------------

    namespace detail_impl_ {
        // `txn` is the snapshot the scan reads under. transaction_data{0,0} is the
        // "see all committed" view every existing caller wants; a REAL transaction is what a
        // caller wants when it is looking for rows ITS OWN transaction has just written and
        // not yet published — those are invisible to {0,0}, and the rows it deleted are
        // still visible to it.
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
                     Fn&& fn) {
        detail_impl_::inline_scan_range(table,
                                        col_indices,
                                        resource,
                                        components::table::transaction_data{0, 0},
                                        std::forward<Fn>(fn));
    }

    template<typename Fn>
    void inline_scan(components::table::data_table_t& table,
                     const std::vector<std::int64_t>& col_indices,
                     std::pmr::memory_resource* resource,
                     Fn&& fn) {
        detail_impl_::inline_scan_range(table,
                                        col_indices,
                                        resource,
                                        components::table::transaction_data{0, 0},
                                        std::forward<Fn>(fn));
    }

    // Snapshot overload: read under a REAL transaction rather than "see all committed".
    // The caller that needs this is one patching rows its own uncommitted statement just
    // wrote — see agent_disk_t::update_pg_attribute_commit_id_field_inner.
    template<typename Fn>
    void inline_scan_in_txn(components::table::data_table_t& table,
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
                     Fn&& fn) {
        detail_impl_::inline_scan_range(const_cast<components::table::data_table_t&>(table),
                                        col_indices,
                                        resource,
                                        components::table::transaction_data{0, 0},
                                        std::forward<Fn>(fn));
    }

    template<typename Fn>
    void inline_scan(const components::table::data_table_t& table,
                     const std::vector<std::int64_t>& col_indices,
                     std::pmr::memory_resource* resource,
                     Fn&& fn) {
        detail_impl_::inline_scan_range(const_cast<components::table::data_table_t&>(table),
                                        col_indices,
                                        resource,
                                        components::table::transaction_data{0, 0},
                                        std::forward<Fn>(fn));
    }

} // namespace services::disk::detail
