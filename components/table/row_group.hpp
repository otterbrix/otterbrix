#pragma once
#include "column_data.hpp"
#include "row_version_manager.hpp"
#include "storage/data_pointer.hpp"

namespace components::vector {
    class data_chunk_t;
}

namespace components::table {
    class row_version_manager_t;

    constexpr static uint64_t MAX_ROW_GROUP_SIZE = uint64_t(1) << 30;

    class data_table_t;
    enum class table_scan_type : uint8_t;
    class scan_filter_info;
    class collection_scan_state;
    class column_definition_t;
    class collection_t;

    class row_group_t : public segment_base_t<row_group_t> {
    public:
        friend class column_data_t;

        row_group_t(collection_t* collection, int64_t start, uint64_t count);
        ~row_group_t() = default;

    private:
        collection_t* collection_;
        std::atomic<row_version_manager_t*> version_info_ = nullptr;
        std::shared_ptr<row_version_manager_t> owned_version_info_;
        uint64_t current_version_ = 0;
        std::vector<std::shared_ptr<column_data_t>> columns_;

    public:
        void move_to_collection(collection_t* collection, int64_t new_start);
        collection_t& collection() { return *collection_; }

        storage::block_manager_t& block_manager();

        // TODO: type casting
        // std::unique_ptr<row_group_t> alter_type(collection_t* collection, const types::complex_logical_type &target_type, uint64_t changed_idx,
        // collection_scan_state &scan_state, vector::data_chunk_t &scan_chunk);
        std::unique_ptr<row_group_t> add_column(collection_t* collection,
                                                column_definition_t& new_column,
                                                const types::logical_value_t& default_value,
                                                vector::vector_t& intermediate);
        std::unique_ptr<row_group_t> remove_column(collection_t* collection, uint64_t removed_column);

        void initialize_empty(const std::pmr::vector<types::complex_logical_type>& types);

        bool initialize_scan(collection_scan_state& state);
        bool initialize_scan_with_offset(collection_scan_state& state, uint64_t vector_offset);
        bool check_zonemap_segments(collection_scan_state& state);
        void scan(collection_scan_state& state, vector::data_chunk_t& result);
        void scan_committed(collection_scan_state& state, vector::data_chunk_t& result, table_scan_type type);

        bool check_predicate(int64_t row_id, const table_filter_t* filter);

        void fetch_row(column_fetch_state& state,
                       const std::vector<storage_index_t>& column_ids,
                       int64_t row_id,
                       vector::data_chunk_t& result,
                       uint64_t result_idx);

        void append_version_info(transaction_data txn, uint64_t count);

        void commit_append(uint64_t commit_id, uint64_t row_group_start, uint64_t count);
        void revert_append(uint64_t row_group_start);

        uint64_t delete_rows(data_table_t& table, int64_t* row_ids, uint64_t count);
        uint64_t delete_rows(uint64_t vector_idx, int64_t rows[], uint64_t count);
        uint64_t delete_rows(data_table_t& table, int64_t* row_ids, uint64_t count, uint64_t transaction_id);
        void commit_delete(uint64_t commit_id, uint64_t vector_idx, const delete_info& info);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);

        uint64_t committed_row_count();

        void initialize_append(row_group_append_state& append_state);
        void append(row_group_append_state& append_state, vector::data_chunk_t& chunk, uint64_t append_count);

        void update(vector::data_chunk_t& updates,
                    int64_t* ids,
                    uint64_t offset,
                    uint64_t count,
                    const std::vector<uint64_t>& column_ids);
        void update_column(vector::data_chunk_t& updates,
                           vector::vector_t& row_ids,
                           const std::vector<uint64_t>& column_path);

        void get_column_segment_info(uint64_t row_group_index, std::vector<column_segment_info>& result);

        storage::row_group_pointer_t write_to_disk(storage::partial_block_manager_t& partial_block_manager);
        void create_from_pointer(const storage::row_group_pointer_t& pointer);

        uint64_t allocation_size() const { return allocation_size_; }

        void next_vector(collection_scan_state& state);

        uint64_t row_group_size() const;
        row_version_manager_t& get_or_create_version_info();
        std::shared_ptr<row_version_manager_t> get_or_create_version_info_ptr();

        uint64_t calculate_size();

    private:
        uint64_t indexing_vector(uint64_t vector_idx, vector::indexing_vector_t& indexing_vector, uint64_t max_count);
        uint64_t indexing_vector(transaction_data txn, uint64_t vector_idx, vector::indexing_vector_t& indexing_vector, uint64_t max_count);
        uint64_t
        commited_indexing_vector(uint64_t vector_idx, vector::indexing_vector_t& indexing_vector, uint64_t max_count);
        std::shared_ptr<row_version_manager_t> get_or_create_version_info_internal();
        row_version_manager_t* version_info();
        void set_version_info(std::shared_ptr<row_version_manager_t> version);
        column_data_t& get_column(uint64_t c);
        column_data_t& get_column(const storage_index_t& c);
        uint64_t get_column_count() const;
        std::vector<std::shared_ptr<column_data_t>>& columns();

        void filter_indexing(std::pmr::memory_resource* resource,
                             uint64_t vector_index,
                             vector::indexing_vector_t& indexing,
                             const table_filter_t* filter,
                             uint64_t& approved_tuple_count);

        template<table_scan_type TYPE>
        void templated_scan(collection_scan_state& state, vector::data_chunk_t& result);

        bool has_unloaded_deletes() const;

        std::mutex row_group_lock_;
        std::vector<storage::meta_block_pointer_t> column_pointers_;
        std::unique_ptr<std::atomic<bool>[]> is_loaded_;
        std::vector<storage::meta_block_pointer_t> deletes_pointers_;
        std::atomic<bool> deletes_is_loaded_;
        uint64_t allocation_size_;
    };
} // namespace components::table