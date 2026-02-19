#pragma once

#include "base_statistics.hpp"
#include "column_segment.hpp"
#include "column_state.hpp"
#include "segment_tree.hpp"
#include "update_segment.hpp"

namespace components::table {

    struct persistent_column_data_t;

    namespace storage {
        class block_manager_t;
        class partial_block_manager_t;
    }

    enum class filter_propagate_result_t : uint8_t
    {
        NO_PRUNING_POSSIBLE = 0,
        ALWAYS_TRUE = 1,
        ALWAYS_FALSE = 2,
        TRUE_OR_NULL = 3,
        FALSE_OR_NULL = 4
    };
    constexpr uint64_t MAX_ROW_ID = 1ULL << 55; // 2^55

    class column_data_t {
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
        bool has_updates() const;
        virtual scan_vector_type
        get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result);
        virtual void initialize_scan(column_scan_state& state);
        virtual void initialize_scan_with_offset(column_scan_state& state, int64_t row_idx);
        uint64_t scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result);
        uint64_t
        scan_committed(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, bool allow_updates);
        virtual uint64_t
        scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t scan_count);
        virtual uint64_t scan_committed(uint64_t vector_index,
                                        column_scan_state& state,
                                        vector::vector_t& result,
                                        bool allow_updates,
                                        uint64_t scan_count);

        virtual void scan_committed_range(uint64_t row_group_start,
                                          uint64_t offset_in_row_group,
                                          uint64_t count,
                                          vector::vector_t& result);
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
                                      uint64_t count,
                                      bool allow_updates);
        virtual void filter(uint64_t vector_index,
                            column_scan_state& state,
                            vector::vector_t& result,
                            vector::indexing_vector_t& indexing,
                            uint64_t& count,
                            const table_filter_t& filter);
        virtual void filter_scan(uint64_t vector_index,
                                 column_scan_state& state,
                                 vector::vector_t& result,
                                 vector::indexing_vector_t& indexing,
                                 uint64_t count);
        virtual void filter_scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           vector::indexing_vector_t& indexing,
                                           uint64_t count,
                                           bool allow_updates);

        virtual void skip(column_scan_state& state, uint64_t count = vector::DEFAULT_VECTOR_CAPACITY);

        virtual void initialize_append(column_append_state& state);
        virtual void append(column_append_state& state, vector::vector_t& vector, uint64_t count);
        virtual void append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t count);
        virtual void revert_append(int64_t start_row);

        virtual bool check_predicate(int64_t row_id, const table_filter_t* filter);
        virtual uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result);
        virtual void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx);

        virtual void
        update(uint64_t column_index, vector::vector_t& update_vector, int64_t* row_ids, uint64_t update_count);
        virtual void update_column(const std::vector<uint64_t>& column_path,
                                   vector::vector_t& update_vector,
                                   int64_t* row_ids,
                                   uint64_t update_count,
                                   uint64_t depth);

        virtual void get_column_segment_info(uint64_t row_group_index,
                                             std::vector<uint64_t> col_path,
                                             std::vector<column_segment_info>& result);

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

        persistent_column_data_t checkpoint(storage::partial_block_manager_t& partial_block_manager);
        virtual void initialize_column(const persistent_column_data_t& persistent_data);
        void initialize_column_validity(const persistent_column_data_t& persistent_data);

    protected:
        void apend_transient_segment(std::unique_lock<std::mutex>& l, int64_t start_row);

        uint64_t
        scan_vector(column_scan_state& state, vector::vector_t& result, uint64_t remaining, scan_vector_type scan_type);
        template<bool SCAN_COMMITTED, bool ALLOW_UPDATES>
        uint64_t
        scan_vector(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t target_scan);

        void clear_updates();
        void fetch_updates(uint64_t vector_index,
                           vector::vector_t& result,
                           uint64_t result_offset,
                           uint64_t scan_count,
                           bool allow_updates,
                           bool scan_committed);
        void fetch_update_row(int64_t row_id, vector::vector_t& result, uint64_t result_idx);
        void update_internal(uint64_t column_index,
                             vector::vector_t& update_vector,
                             int64_t* row_ids,
                             uint64_t update_count,
                             vector::vector_t& base_vector);

        uint64_t vector_count(uint64_t vector_index) const;

        int64_t start_;
        std::atomic<uint64_t> count_;
        storage::block_manager_t& block_manager_;
        uint64_t column_index_;
        types::complex_logical_type type_;
        column_data_t* parent_;
        segment_tree_t<column_segment_t> data_;
        mutable std::mutex update_lock_;
        std::unique_ptr<update_segment_t> updates_;
        uint64_t allocation_size_;
        base_statistics_t statistics_;

        std::pmr::memory_resource* resource_;
    };

} // namespace components::table