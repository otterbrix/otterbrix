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

        virtual uint64_t total_rows() const = 0;
        virtual uint64_t calculate_size() = 0;

        virtual void scan(vector::data_chunk_t& output,
                          const table::table_filter_t* filter,
                          int limit) = 0;

        virtual void fetch(vector::data_chunk_t& output,
                           const vector::vector_t& row_ids,
                           uint64_t count) = 0;

        virtual void scan_segment(int64_t start,
                                  uint64_t count,
                                  const std::function<void(vector::data_chunk_t& chunk)>& callback) = 0;

        virtual uint64_t parallel_scan(const std::function<void(vector::data_chunk_t& chunk)>& callback) = 0;

        virtual uint64_t append(vector::data_chunk_t& data) = 0;

        virtual void update(vector::vector_t& row_ids,
                            vector::data_chunk_t& data) = 0;
        virtual void update(vector::vector_t& row_ids,
                            vector::data_chunk_t& data,
                            table::transaction_data txn) {
            (void)txn;
            update(row_ids, data);
        }

        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) = 0;

        // Txn-aware overloads with default fallbacks
        virtual uint64_t append(vector::data_chunk_t& data, table::transaction_data txn) {
            (void)txn;
            return append(data);
        }
        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t txn_id) {
            (void)txn_id;
            return delete_rows(row_ids, count);
        }
        virtual void commit_append(uint64_t /*commit_id*/, int64_t /*row_start*/, uint64_t /*count*/) {}
        virtual void revert_append(int64_t /*row_start*/, uint64_t /*count*/) {}
        virtual void commit_all_deletes(uint64_t /*txn_id*/, uint64_t /*commit_id*/) {}

        virtual std::pmr::memory_resource* resource() const = 0;
    };

} // namespace components::storage
