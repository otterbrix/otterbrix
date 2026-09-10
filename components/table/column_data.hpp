#pragma once

#include "base_statistics.hpp"
#include "column_segment.hpp"
#include "column_state.hpp"
#include "segment_tree.hpp"
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/types/tri_bool.hpp>

namespace components::table {

#ifdef DEV_MODE
    // Must stay zero: a live pin surviving a segment swap leaves a buffer_handle_t pointing at freed memory.
    uint64_t transitions_with_live_pin() noexcept;
    // Denominator, so a test can tell "none happened" from "none needed a live pin".
    uint64_t segment_transitions() noexcept;
    void reset_transitions_with_live_pin() noexcept;
#endif

    struct persistent_column_data_t;

    namespace storage {
        class block_manager_t;
        class partial_block_manager_t;
    } // namespace storage

    enum class filter_propagate_result_t : uint8_t
    {
        NO_PRUNING_POSSIBLE = 0,
        ALWAYS_TRUE = 1,
        ALWAYS_FALSE = 2,
        TRUE_OR_NULL = 3,
        FALSE_OR_NULL = 4
    };
    // A comparison against NULL is UNKNOWN, not FALSE: collapsing it would let NOT resurrect NULL rows.
    using filter_match_t = types::tri_bool_t;

    constexpr uint64_t MAX_ROW_ID = 1ULL << 55;

    // TOP-LEVEL columns only are refcount-shared; wrapping a nested (unique_ptr) child double-frees it.
    class column_data_t : public boost::intrusive_ref_counter<column_data_t> {
        friend class column_segment_t;
        friend class column_data_checkpointer_t;
        friend class column_checkpoint_state_t;

    public:
        column_data_t(std::pmr::memory_resource* resource,
                      storage::block_manager_t& block_manager,
                      uint64_t column_index,
                      int64_t start_row,
                      types::complex_logical_type type,
                      column_data_t* parent);
        virtual ~column_data_t() = default;

        virtual filter_propagate_result_t check_zonemap(column_scan_state& state, table_filter_t& filter);
        filter_propagate_result_t check_segment_zonemap(column_scan_state& state, table_filter_t& filter);

        storage::block_manager_t& block_manager() { return block_manager_; }
        virtual uint64_t max_entry();

        uint64_t allocation_size() const { return allocation_size_; }

        virtual void set_start(int64_t new_start);
        const types::complex_logical_type& root_type() const;
        const types::complex_logical_type& type() const { return type_; }
        virtual scan_vector_type
        get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result);
        virtual void initialize_scan(column_scan_state& state);
        virtual void initialize_scan_with_offset(column_scan_state& state, int64_t row_idx);
        // The vector index sizes the last, partial vector of the column
        uint64_t scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result);
        uint64_t scan_committed(uint64_t vector_index, column_scan_state& state, vector::vector_t& result);
        virtual uint64_t scan(column_scan_state& state, vector::vector_t& result, uint64_t scan_count);
        virtual uint64_t scan_committed(column_scan_state& state, vector::vector_t& result, uint64_t scan_count);

        virtual uint64_t scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count);

        virtual void select(uint64_t vector_index,
                            column_scan_state& state,
                            vector::vector_t& result,
                            vector::indexing_vector_t& indexing,
                            uint64_t count);
        virtual void select_committed(uint64_t vector_index,
                                      column_scan_state& state,
                                      vector::vector_t& result,
                                      vector::indexing_vector_t& indexing,
                                      uint64_t count);
        virtual void filter_scan(uint64_t vector_index,
                                 column_scan_state& state,
                                 vector::vector_t& result,
                                 vector::indexing_vector_t& indexing,
                                 uint64_t count);
        virtual void filter_scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           vector::indexing_vector_t& indexing,
                                           uint64_t count);

        virtual void skip(column_scan_state& state, uint64_t count = vector::DEFAULT_VECTOR_CAPACITY);

        [[nodiscard]] virtual core::result_wrapper_t<bool> initialize_append(column_append_state& state);
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        append(column_append_state& state, vector::vector_t& vector, uint64_t count);
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t count);
        // `start_row` is COLLECTION-ABSOLUTE; a failed rollback pin must be REPORTED, not asserted away.
        [[nodiscard]] virtual core::result_wrapper_t<bool> revert_append(int64_t start_row);

        virtual uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result);
        virtual void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx);

        virtual void get_column_segment_info(uint64_t row_group_index,
                                             std::vector<uint64_t> col_path,
                                             std::vector<column_segment_info>& result);

        // create_column's constructors can't state this: a throw there would hang the disk agent's mailbox.
        [[nodiscard]] static core::error_t validate_column_type(const types::complex_logical_type& type,
                                                                std::pmr::memory_resource* resource);

        static std::unique_ptr<column_data_t> create_column(std::pmr::memory_resource* resource,
                                                            storage::block_manager_t& block_manager,
                                                            uint64_t column_index,
                                                            int64_t start_row,
                                                            const types::complex_logical_type& type,
                                                            column_data_t* parent = nullptr);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        uint64_t count() const noexcept { return count_; }
        int64_t start() const noexcept { return start_; }
        const base_statistics_t& statistics() const noexcept { return statistics_; }
        base_statistics_t& statistics() noexcept { return statistics_; }

        // Hands the record to checkpoint_children (NVI hook below), validity FIRST.
        [[nodiscard]] core::result_wrapper_t<persistent_column_data_t>
        checkpoint(storage::partial_block_manager_t& partial_block_manager);
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        initialize_column(const persistent_column_data_t& persistent_data);

        // Caller owns `pbm` and MUST flush_partial_blocks() before a re-pointed segment can be
        // evicted or reloaded, or a live segment could load() an unflushed block.
        [[nodiscard]] virtual core::result_wrapper_t<bool> transition_to_disk(storage::partial_block_manager_t& pbm);

        // Mirrors checkpoint_children's recursion, not transition_to_disk's; skipping the override orphans blocks.
        virtual void collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const;

    protected:
        [[nodiscard]] core::result_wrapper_t<bool> apend_transient_segment(std::unique_lock<std::mutex>& l,
                                                                           int64_t start_row);

        [[nodiscard]] core::result_wrapper_t<bool> transition_segment_to_disk(std::unique_lock<std::mutex>& l,
                                                                              uint64_t segment_index,
                                                                              storage::partial_block_manager_t& pbm);

        uint64_t
        scan_vector(column_scan_state& state, vector::vector_t& result, uint64_t remaining, scan_vector_type scan_type);

        uint64_t vector_count(uint64_t vector_index) const;

        int64_t start_;
        std::atomic<uint64_t> count_;
        storage::block_manager_t& block_manager_;

    private:
        // NVI hook of checkpoint(): child_columns[0] must always be VALIDITY, or reload loses every NULL.
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                            persistent_column_data_t& persistent);

    protected:
        uint64_t column_index_;
        types::complex_logical_type type_;
        column_data_t* parent_;
        segment_tree_t<column_segment_t> data_;
        // Single-owner: see the proof on data_table_t (components/table/data_table.hpp).
        uint64_t allocation_size_;
        base_statistics_t statistics_;

        std::pmr::memory_resource* resource_;
    };

} // namespace components::table