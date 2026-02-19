#pragma once

#include <optional>

#include <components/vector/vector.hpp>

#include "base_statistics.hpp"
#include "segment_tree.hpp"
#include "storage/block_handle.hpp"

namespace components::table {
    namespace storage {
        class block_manager_t;
        class buffer_manager_t;
        class block_handle_t;
    } // namespace storage

    class table_filter_t;
    struct column_append_state;
    struct column_segment_state;
    struct column_scan_state;
    struct column_fetch_state;
    struct compressed_segment_state;

    enum class scan_vector_type : uint8_t
    {
        SCAN_FLAT_VECTOR = 0,
        SCAN_ENTIRE_VECTOR = 1
    };

    class column_segment_t : public segment_base_t<column_segment_t> {
    public:
        friend class column_data_t;
        column_segment_t(std::shared_ptr<storage::block_handle_t> block,
                         const types::complex_logical_type& type,
                         int64_t start,
                         uint64_t count,
                         uint32_t block_id,
                         uint64_t offset,
                         uint64_t segment_size,
                         std::unique_ptr<column_segment_state> segment_state_p = nullptr);

        column_segment_t(column_segment_t&& other) noexcept;
        column_segment_t(column_segment_t&& other, int64_t start);

        types::complex_logical_type type;
        uint64_t type_size;
        std::shared_ptr<storage::block_handle_t> block;

        static std::unique_ptr<column_segment_t> create_segment(storage::buffer_manager_t& block_manager,
                                                                const types::complex_logical_type& type,
                                                                int64_t start,
                                                                uint64_t segment_size,
                                                                uint64_t block_size);

        void initialize_scan(column_scan_state& state);
        void scan(column_scan_state& state,
                  uint64_t scan_count,
                  vector::vector_t& result,
                  uint64_t result_offset,
                  scan_vector_type scan_type);

        bool check_predicate(int64_t row_id, const table_filter_t* filter);
        void fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx);

        static uint64_t filter_indexing(vector::indexing_vector_t& indexing,
                                        vector::vector_t& vector,
                                        vector::unified_vector_format& uvf,
                                        const table_filter_t& filter,
                                        uint64_t scan_count,
                                        uint64_t& approved_tuple_count);

        void skip(column_scan_state& state);

        uint64_t segment_size() const;
        void resize(uint64_t segment_size);

        void initialize_append(column_append_state& state);
        uint64_t
        append(column_append_state& state, vector::unified_vector_format& data, uint64_t offset, uint64_t count);
        uint64_t finalize_append(column_append_state& state);
        void revert_append(uint64_t start_row);

        uint64_t block_id() { return block_id_; }

        storage::block_manager_t& block_manager() const { return block->block_manager; }

        uint64_t block_offset() { return offset_; }

        int64_t relative_index(int64_t row_index) {
            assert(row_index >= start);
            assert(row_index <= start + static_cast<int64_t>(count));
            return row_index - start;
        }

        compressed_segment_state* segment_state() { return segment_state_.get(); }

        const std::optional<base_statistics_t>& segment_statistics() const { return segment_statistics_; }
        void set_segment_statistics(base_statistics_t stats) { segment_statistics_ = std::move(stats); }

    private:
        void scan(column_scan_state& state, uint64_t scan_count, vector::vector_t& result);
        void
        scan_partial(column_scan_state& state, uint64_t scan_count, vector::vector_t& result, uint64_t result_offset);

        uint64_t block_id_;
        uint64_t offset_;
        uint64_t segment_size_;
        std::unique_ptr<compressed_segment_state> segment_state_;
        std::optional<base_statistics_t> segment_statistics_;
    };

} // namespace components::table