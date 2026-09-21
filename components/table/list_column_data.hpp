#pragma once
#include "column_data.hpp"
#include "validity_column_data.hpp"

namespace components::table {

    class list_column_data_t : public column_data_t {
    public:
        list_column_data_t(std::pmr::memory_resource* resource,
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

        uint64_t scan(column_scan_state& state, vector::vector_t& result, uint64_t scan_count) override;
        uint64_t scan_committed(column_scan_state& state, vector::vector_t& result, uint64_t scan_count) override;
        uint64_t scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) override;

        void skip(column_scan_state& state, uint64_t count = vector::DEFAULT_VECTOR_CAPACITY) override;

        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(column_append_state& state) override;
        [[nodiscard]] core::result_wrapper_t<bool>
        append(column_append_state& state, vector::vector_t& vector, uint64_t count) override;
        [[nodiscard]] core::result_wrapper_t<bool> revert_append(int64_t start_row) override;
        uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) override;
        void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx) override;

        void get_column_segment_info(uint64_t row_group_index,
                                     std::vector<uint64_t> col_path,
                                     std::vector<column_segment_info>& result) override;

        // Disk load: own offset segments + validity (child_columns[0]) + element child
        // (child_columns[1]).
        [[nodiscard]] core::result_wrapper_t<bool>
        initialize_column(const persistent_column_data_t& persistent_data) override;

        // Base walk covers only the own offsets segments; validity and element are collected
        // here. See the contract on column_data_t::collect_disk_block_ids.
        void collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const override;

    private:
        // child_columns[0] = validity, child_columns[1] = element column's persistent form.
        [[nodiscard]] core::result_wrapper_t<bool>
        checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                            persistent_column_data_t& persistent) override;

        // The cumulative child offset stored for `row_idx`. Forwards the fetch state's pin/read
        // refusal: a bare uint64_t return would drop it, answering with a GARBAGE offset instead.
        [[nodiscard]] core::result_wrapper_t<uint64_t> fetch_list_offset(int64_t row_idx);
        // The per-element row ids an in-place LIST update writes, or a refusal when the new
        // cell's length differs from the stored one (in-place can't move neighbours aside).
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<int64_t>>
        gather_child_update(vector::vector_t& update_vector,
                            int64_t* row_ids,
                            uint64_t update_count,
                            vector::vector_t& child_update_out);
    };

} // namespace components::table