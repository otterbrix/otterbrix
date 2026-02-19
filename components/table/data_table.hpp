#pragma once

#include "collection.hpp"
#include "storage/metadata_reader.hpp"
#include "storage/metadata_writer.hpp"

namespace components::table {

    class data_table_t {
    public:
        data_table_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::vector<column_definition_t> column_definitions,
                     std::string name = "temp");
        data_table_t(data_table_t& parent, column_definition_t& new_column);
        data_table_t(data_table_t& parent, uint64_t removed_column);
        data_table_t(data_table_t& parent,
                     uint64_t changed_idx,
                     const types::complex_logical_type& target_type,
                     const std::vector<storage_index_t>& bound_columns);

        [[nodiscard]] std::pmr::vector<types::complex_logical_type> copy_types() const;
        const std::vector<column_definition_t>& columns() const;
        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& types);

        void initialize_scan(table_scan_state& state,
                             const std::vector<storage_index_t>& column_ids,
                             const table_filter_t* filter = nullptr);

        uint64_t max_threads() const;

        void scan(vector::data_chunk_t& result, table_scan_state& state);

        void fetch(vector::data_chunk_t& result,
                   const std::vector<storage_index_t>& column_ids,
                   const vector::vector_t& row_ids,
                   uint64_t fetch_count,
                   column_fetch_state& state);

        std::unique_ptr<table_delete_state>
        initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        uint64_t delete_rows(table_delete_state& state, vector::vector_t& row_ids, uint64_t count);
        uint64_t delete_rows(table_delete_state& state, vector::vector_t& row_ids, uint64_t count, uint64_t transaction_id);

        std::unique_ptr<table_update_state>
        initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        void update(table_update_state& state,
                    vector::vector_t& row_ids,
                    // const std::vector<uint64_t>& column_ids,
                    vector::data_chunk_t& data);
        void update_column(vector::vector_t& row_ids,
                           const std::vector<uint64_t>& column_path,
                           vector::data_chunk_t& updates);

        void append_lock(table_append_state& state);
        void initialize_append(table_append_state& state);
        void append(vector::data_chunk_t& chunk, table_append_state& state);
        void finalize_append(table_append_state& state);
        void finalize_append(table_append_state& state, transaction_data txn);
        void commit_append(int64_t row_start, int64_t count);
        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count);
        void revert_append(int64_t row_start, uint64_t count);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void scan_table_segment(int64_t start_row,
                                uint64_t count,
                                const std::function<void(vector::data_chunk_t& chunk)>& function);

        void merge_storage(collection_t& data);

        void set_as_root() { is_root_ = true; }

        bool is_root() { return is_root_; }

        uint64_t column_count() const;

        std::vector<column_segment_info> get_column_segment_info();
        bool create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type);

        std::unique_ptr<constraint_state>
        initialize_constraint_state(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        std::string table_name() const;
        void set_table_name(std::string new_name);

        uint64_t row_group_size() const;

        std::shared_ptr<collection_t> row_group() const;

        uint64_t calculate_size();
        void cleanup_versions(uint64_t lowest_active_start_time);
        void compact();

        std::shared_ptr<parallel_table_scan_state_t>
        create_parallel_scan_state(const std::vector<storage_index_t>& column_ids,
                                    const table_filter_t* filter = nullptr);
        bool next_parallel_chunk(parallel_table_scan_state_t& parallel_state,
                                  table_scan_state& local_state,
                                  vector::data_chunk_t& result);

        void checkpoint(storage::metadata_writer_t& writer);
        static std::unique_ptr<data_table_t> load_from_disk(std::pmr::memory_resource* resource,
                                                             storage::block_manager_t& block_manager,
                                                             storage::metadata_reader_t& reader);

    private:
        void initialize_scan_with_offset(table_scan_state& state,
                                         const std::vector<storage_index_t>& column_ids,
                                         int64_t start_row,
                                         int64_t end_row);

        std::pmr::memory_resource* resource_;
        std::vector<column_definition_t> column_definitions_;
        std::mutex append_lock_;
        std::shared_ptr<collection_t> row_groups_;
        std::atomic<bool> is_root_;
        std::string name_;
    };

} // namespace components::table