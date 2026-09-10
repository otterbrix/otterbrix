#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

namespace components::storage {

    // No pins or live scan state may survive a call — only this resume position crosses the mailbox.
    struct scan_position_t {
        int64_t next_row{0}; // absolute source row to resume from
        int64_t max_row{0};  // exclusive source-row upper bound (table total_rows snapshot)
        bool drained{false}; // set once the underlying scan reports no more rows
    };

    struct appended_range_t {
        int64_t start_row{0};
        uint64_t count{0};
    };

    class storage_t {
    public:
        virtual ~storage_t() = default;

        virtual std::pmr::vector<types::complex_logical_type> types() const = 0;
        virtual const std::vector<table::column_definition_t>& columns() const = 0;
        virtual size_t column_count() const = 0;
        virtual bool has_schema() const = 0;
        virtual void adopt_schema(const std::pmr::vector<types::complex_logical_type>& types) = 0;

        virtual uint64_t total_rows() const = 0;
        virtual uint64_t calculate_size() = 0;

        virtual void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) = 0;
        virtual void scan(vector::data_chunk_t& output,
                          const table::table_filter_t* filter,
                          int64_t limit,
                          table::transaction_data /*txn*/) {
            scan(output, filter, limit);
        }

        virtual void scan_projected(vector::data_chunk_t& output,
                                    const table::table_filter_t* filter,
                                    int limit,
                                    const std::vector<size_t>& /*projected_cols*/) {
            scan(output, filter, limit);
        }
        virtual void scan_projected(vector::data_chunk_t& output,
                                    const table::table_filter_t* filter,
                                    int limit,
                                    const std::vector<size_t>& projected_cols,
                                    table::transaction_data /*txn*/) {
            scan_projected(output, filter, limit, projected_cols);
        }

        // `projected_cols == nullptr` means every column; errors surface as buffer-pool OOM / data_corruption.
        [[nodiscard]] virtual core::result_wrapper_t<bool> scan_batched(std::pmr::vector<vector::data_chunk_t>& batches,
                                                                        const table::table_filter_t* filter,
                                                                        int64_t limit,
                                                                        const std::vector<size_t>* projected_cols,
                                                                        table::transaction_data txn) {
            auto t = types();
            vector::data_chunk_t one(resource(), t);
            if (projected_cols) {
                scan_projected(one, filter, static_cast<int>(limit), *projected_cols, txn);
            } else {
                scan(one, filter, limit, txn);
            }
            if (one.size() > 0) {
                batches.push_back(std::move(one));
            }
            return true;
        }

        // Advances `pos.next_row`/`pos.drained`; its cursor dies within the call, so ZERO pins survive it.
        [[nodiscard]] virtual core::result_wrapper_t<bool> fetch_next_batch(vector::data_chunk_t& output,
                                                                            scan_position_t& pos,
                                                                            const table::table_filter_t* filter,
                                                                            const std::vector<size_t>* projected_cols,
                                                                            table::transaction_data txn) {
            if (pos.drained) {
                return true;
            }
            if (projected_cols) {
                scan_projected(output, filter, -1, *projected_cols, txn);
            } else {
                scan(output, filter, -1, txn);
            }
            pos.next_row = pos.max_row;
            pos.drained = true;
            return true;
        }

        // `true` alone does NOT mean no error — check output's column_fetch_state::fetch_error.
        [[nodiscard]] virtual core::result_wrapper_t<bool> fetch(vector::data_chunk_t& output,
                                                                 const vector::vector_t& row_ids,
                                                                 uint64_t count,
                                                                 const std::vector<size_t>& projected_cols,
                                                                 const table::transaction_data& txn,
                                                                 table::fetch_visibility_t visibility) = 0;

        // No default overload: an NDEBUG-compiled-away assert would silently reuse a failed append's start_row.
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t> append(vector::data_chunk_t& data,
                                                                      table::transaction_data txn) = 0;

        // Delete-stamp + append. There is no in-place form: an update that rewrote a row where it lay
        // would take the old version away from every snapshot still entitled to read it.
        [[nodiscard]] virtual core::result_wrapper_t<appended_range_t>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data txn) = 0;

        // A count alone cannot separate "deleted nothing" from "stopped part-way": a row id that
        // names no row group is a refusal, and it travels here. txn_id 0 marks a write that commits
        // as it lands; there is no separate untransactional form.
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t>
        delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t txn_id) = 0;
        virtual void commit_append(uint64_t /*commit_id*/, int64_t /*row_start*/, uint64_t /*count*/) {}
        // Rolling back an append can itself fail; the caller is on an abort path and can do no
        // more than record it, but it must be told rather than left to guess.
        [[nodiscard]] virtual core::error_t revert_append(int64_t /*row_start*/, uint64_t /*count*/) {
            return core::error_t::no_error();
        }
        virtual void commit_all_deletes(uint64_t /*txn_id*/, uint64_t /*commit_id*/) {}
        virtual void revert_all_deletes(uint64_t /*txn_id*/) {}

        virtual std::pmr::memory_resource* resource() const = 0;
    };

} // namespace components::storage