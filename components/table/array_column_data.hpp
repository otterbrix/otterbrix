#pragma once
#include "column_data.hpp"
#include "validity_column_data.hpp"

namespace components::table {

    class array_column_data_t : public column_data_t {
    public:
        array_column_data_t(std::pmr::memory_resource* resource,
                            storage::block_manager_t& block_manager,
                            uint64_t column_index,
                            int64_t start_row,
                            types::complex_logical_type type,
                            column_data_t* parent = nullptr);

        std::unique_ptr<column_data_t> child_column;
        validity_column_data_t validity;

        void set_start(int64_t new_start) override;
        filter_propagate_result_t check_zonemap(column_scan_state& state, table_filter_t& filter) override;

        void initialize_scan(column_scan_state& state) override;
        void initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) override;

        uint64_t
        scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t scan_count) override;
        uint64_t scan_committed(uint64_t vector_index,
                                column_scan_state& state,
                                vector::vector_t& result,
                                bool allow_updates,
                                uint64_t scan_count) override;
        uint64_t scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) override;

        void skip(column_scan_state& state, uint64_t count = vector::DEFAULT_VECTOR_CAPACITY) override;

        void initialize_append(column_append_state& state) override;
        void append(column_append_state& state, vector::vector_t& vector, uint64_t count) override;
        void revert_append(int64_t start_row) override;
        uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) override;
        void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx) override;
        void update(uint64_t column_index,
                    vector::vector_t& update_vector,
                    int64_t* row_ids,
                    uint64_t update_count) override;
        void update_column(const std::vector<uint64_t>& column_path,
                           vector::vector_t& update_vector,
                           int64_t* row_ids,
                           uint64_t update_count,
                           uint64_t depth) override;

        void get_column_segment_info(uint64_t row_group_index,
                                     std::vector<uint64_t> col_path,
                                     std::vector<column_segment_info>& result) override;
    };

} // namespace components::table