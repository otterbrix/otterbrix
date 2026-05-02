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

namespace components::storage {

    class storage_t {
    public:
        virtual ~storage_t() = default;

        virtual std::pmr::vector<types::complex_logical_type> types() const = 0;
        virtual const std::vector<table::column_definition_t>& columns() const = 0;
        virtual size_t column_count() const = 0;
        virtual bool has_schema() const = 0;
        virtual void adopt_schema(const std::pmr::vector<types::complex_logical_type>& types) = 0;
        virtual void overlay_not_null(const std::string& col_name) = 0;

        virtual uint64_t total_rows() const = 0;
        virtual uint64_t calculate_size() = 0;

        virtual void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) = 0;
        virtual void scan(vector::data_chunk_t& output,
                          const table::table_filter_t* filter,
                          int64_t limit,
                          table::transaction_data /*txn*/) {
            scan(output, filter, limit);
        }

        // Scan only a subset of columns. Caller is expected to have constructed `output`
        // as a sparse data_chunk_t with placeholder vectors for columns outside projected_cols.
        // Default implementation falls back to full scan.
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

        // Batched scan: emit one ≤DEFAULT_VECTOR_CAPACITY chunk per scan vector directly,
        // avoiding the accumulate-then-split round-trip. `projected_cols == nullptr` means
        // scan all columns; otherwise sparse projection.
        // Default implementation: do a regular scan into one chunk; subclasses can override.
        virtual void scan_batched(std::vector<vector::data_chunk_t>& batches,
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
        }

        virtual void fetch(vector::data_chunk_t& output, const vector::vector_t& row_ids, uint64_t count) = 0;

        virtual void scan_segment(int64_t start,
                                  uint64_t count,
                                  const std::function<void(vector::data_chunk_t& chunk)>& callback) = 0;

        virtual uint64_t parallel_scan(const std::function<void(vector::data_chunk_t& chunk)>& callback) = 0;

        virtual uint64_t append(vector::data_chunk_t& data) = 0;

        virtual void update(vector::vector_t& row_ids, vector::data_chunk_t& data) = 0;
        virtual std::pair<int64_t, uint64_t>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data /*txn*/) {
            update(row_ids, data);
            return {0, 0};
        }

        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) = 0;

        // Txn-aware overloads with default fallbacks
        virtual uint64_t append(vector::data_chunk_t& data, table::transaction_data /*txn*/) { return append(data); }
        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t /*txn_id*/) {
            return delete_rows(row_ids, count);
        }
        virtual void commit_append(uint64_t /*commit_id*/, int64_t /*row_start*/, uint64_t /*count*/) {}
        virtual void revert_append(int64_t /*row_start*/, uint64_t /*count*/) {}
        virtual void commit_all_deletes(uint64_t /*txn_id*/, uint64_t /*commit_id*/) {}

        virtual std::pmr::memory_resource* resource() const = 0;
    };

} // namespace components::storage