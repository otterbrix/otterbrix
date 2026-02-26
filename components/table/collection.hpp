#pragma once
#include <atomic>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <functional>

#include "column_data.hpp"
#include "row_version_manager.hpp"
#include "table_state.hpp"

#include "column_definition.hpp"

namespace components::table::storage {
    struct row_group_pointer_t;
    class partial_block_manager_t;
} // namespace components::table::storage

namespace components::table {

    class data_table_t;

    class row_group_segment_tree_t : public segment_tree_t<row_group_t, true> {
    public:
        explicit row_group_segment_tree_t(collection_t& collection);
        ~row_group_segment_tree_t() override = default;

    protected:
        collection_t& collection_;
        uint64_t current_row_group_;
        uint64_t max_row_group_;
    };

    class collection_t {
    public:
        collection_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::pmr::vector<types::complex_logical_type> types,
                     int64_t row_start,
                     uint64_t total_rows = 0,
                     uint64_t row_group_size = vector::DEFAULT_VECTOR_CAPACITY);

        uint64_t total_rows() const;
        uint64_t committed_row_count() const;

        bool is_empty() const;

        void append_row_group(std::unique_lock<std::mutex>& l, int64_t start_row);
        row_group_t* append_row_group(int64_t start_row);
        row_group_t* row_group(int64_t index);

        void initialize_scan(collection_scan_state& state, const std::vector<storage_index_t>& column_ids);
        void initialize_create_index_scan(create_index_scan_state& state);
        void initialize_scan_with_offset(collection_scan_state& state,
                                         const std::vector<storage_index_t>& column_ids,
                                         int64_t start_row,
                                         int64_t end_row);
        static bool initialize_scan_in_row_group(collection_scan_state& state,
                                                 collection_t& collection,
                                                 row_group_t& row_group,
                                                 uint64_t vector_index,
                                                 int64_t max_row);

        bool scan(const std::vector<storage_index_t>& column_ids,
                  const std::function<bool(vector::data_chunk_t& chunk)>& fun);
        bool scan(const std::function<bool(vector::data_chunk_t& chunk)>& fun);

        void fetch(vector::data_chunk_t& result,
                   const std::vector<storage_index_t>& column_ids,
                   const vector::vector_t& row_identifiers,
                   uint64_t fetch_count,
                   column_fetch_state& state);

        void initialize_append(table_append_state& state);
        bool append(vector::data_chunk_t& chunk, table_append_state& state);
        void finalize_append(table_append_state& state, transaction_data txn);
        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count);
        void revert_append(int64_t row_start, uint64_t count);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void cleanup_append(int64_t start, uint64_t count);

        void merge_storage(collection_t& data);

        uint64_t delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id);
        void update(int64_t* ids, const std::vector<uint64_t>& column_ids, vector::data_chunk_t& updates);
        void update_column(vector::vector_t& row_ids,
                           const std::vector<uint64_t>& column_path,
                           vector::data_chunk_t& updates);

        std::vector<column_segment_info> get_column_segment_info();
        const std::pmr::vector<types::complex_logical_type>& types() const;
        void adopt_types(std::pmr::vector<types::complex_logical_type> types);

        std::shared_ptr<collection_t> add_column(column_definition_t& new_column);
        std::shared_ptr<collection_t> remove_column(uint64_t col_idx);
        // TODO: type casting
        // std::shared_ptr<collection_t> alter_type(uint64_t changed_idx, const types::complex_logical_type &target_type,
        // std::vector<storage_index_t> bound_columns);

        std::vector<storage::row_group_pointer_t> checkpoint(storage::partial_block_manager_t& partial_block_manager);

        storage::block_manager_t& block_manager() { return block_manager_; }

        uint64_t allocation_size() const { return allocation_size_; }

        uint64_t row_group_size() const { return row_group_size_; }

        row_group_segment_tree_t* row_group_tree() { return row_groups_.get(); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        uint64_t calculate_size();
        void cleanup_versions(uint64_t lowest_active_start_time);

        void set_total_rows(uint64_t total) { total_rows_ = total; }

    private:
        bool is_empty(std::unique_lock<std::mutex>&) const;

        std::pmr::memory_resource* resource_;
        storage::block_manager_t& block_manager_;
        uint64_t row_group_size_;
        std::atomic<uint64_t> total_rows_;
        std::pmr::vector<types::complex_logical_type> types_;
        int64_t row_start_;
        std::shared_ptr<row_group_segment_tree_t> row_groups_;
        uint64_t allocation_size_;
    };

} // namespace components::table