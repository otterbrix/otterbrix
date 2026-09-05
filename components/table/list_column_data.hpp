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

        uint64_t
        scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t scan_count) override;
        uint64_t scan_committed(uint64_t vector_index,
                                column_scan_state& state,
                                vector::vector_t& result,
                                bool allow_updates,
                                uint64_t scan_count) override;
        uint64_t scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) override;

        void skip(column_scan_state& state, uint64_t count = vector::DEFAULT_VECTOR_CAPACITY) override;

        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(column_append_state& state) override;
        [[nodiscard]] core::result_wrapper_t<bool>
        append(column_append_state& state, vector::vector_t& vector, uint64_t count) override;
        [[nodiscard]] core::result_wrapper_t<bool> revert_append(int64_t start_row) override;
        uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) override;
        void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx) override;
        [[nodiscard]] core::result_wrapper_t<bool> update(uint64_t column_index,
                                                          vector::vector_t& update_vector,
                                                          int64_t* row_ids,
                                                          uint64_t update_count) override;
        [[nodiscard]] core::result_wrapper_t<bool> update_column(const std::vector<uint64_t>& column_path,
                                                                 vector::vector_t& update_vector,
                                                                 int64_t* row_ids,
                                                                 uint64_t update_count,
                                                                 uint64_t depth) override;

        void get_column_segment_info(uint64_t row_group_index,
                                     std::vector<uint64_t> col_path,
                                     std::vector<column_segment_info>& result) override;

        // Disk load: own offset segments + the persisted validity bitmap (child_columns[0])
        // + the persisted element child (child_columns[1]).
        [[nodiscard]] core::result_wrapper_t<bool>
        initialize_column(const persistent_column_data_t& persistent_data) override;

        // Compact reclaim: the base walk covers only the own offsets segments; the
        // validity child and the element column are collected here. See the contract on
        // column_data_t::collect_disk_block_ids.
        void collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const override;

    private:
        // Checkpoint NVI hook: child_columns[0] = validity, child_columns[1] = the element
        // column's persistent form.
        [[nodiscard]] core::result_wrapper_t<bool>
        checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                            persistent_column_data_t& persistent) override;

        // The cumulative child offset stored for `row_idx`. Returns invalid_parameter when
        // the row names no offsets segment and forwards the fetch state's pin/read refusal: a
        // bare uint64_t return here would drop the local fetch state's fetch_error, so a failed
        // pin would answer with a GARBAGE list offset (rule 6).
        [[nodiscard]] core::result_wrapper_t<uint64_t> fetch_list_offset(int64_t row_idx);
        // The per-element row ids an in-place LIST update has to write, or a refusal when the
        // new cell's length differs from the stored one — in-place cannot move a row's
        // neighbours aside. Both callers (update / update_column) already return
        // result_wrapper_t<bool>, so the refusal reaches data_table_t::update unchanged
        // instead of unwinding across the disk agent's mailbox (rules 2/9).
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<int64_t>>
        gather_child_update(vector::vector_t& update_vector,
                            int64_t* row_ids,
                            uint64_t update_count,
                            vector::vector_t& child_update_out);
    };

} // namespace components::table