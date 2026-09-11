#pragma once

#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include "base_statistics.hpp"
#include "compression/compression_type.hpp"
#include "segment_tree.hpp"
#include "storage/block_handle.hpp"

namespace components::table {
    namespace storage {
        class block_manager_t;
        class buffer_manager_t;
        class block_handle_t;
        class partial_block_manager_t;
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
        // Width of ONE element in this segment's RAW payload -- NOT always type.size() (a LIST
        // segment stores a uint64 child-offset per row). See impl::stored_element_size in the .cpp.
        uint64_t type_size;
        std::shared_ptr<storage::block_handle_t> block;

        // Returns an out_of_memory error_t when the backing transient block cannot be
        // registered; otherwise the new segment.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<column_segment_t>>
        create_segment(storage::buffer_manager_t& block_manager,
                       const types::complex_logical_type& type,
                       int64_t start,
                       uint64_t segment_size,
                       uint64_t block_size);

        // Latched failure of the RELOAD constructor (no return channel, must not throw on the
        // open path). The one failure mode: a corrupt big-string overflow list
        // (uncompressed_string_segment_state::register_block finds a duplicate id). Read by
        // column_data_t::initialize_column right after construction.
        [[nodiscard]] bool has_construction_error() const noexcept { return construction_error_.contains_error(); }
        [[nodiscard]] const core::error_t& construction_error() const noexcept { return construction_error_; }

        void initialize_scan(column_scan_state& state);
        void scan(column_scan_state& state,
                  uint64_t scan_count,
                  vector::vector_t& result,
                  uint64_t result_offset,
                  scan_vector_type scan_type);

        void fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx);

        void skip(column_scan_state& state);

        uint64_t segment_size() const;

        // Big-string overflow persistence: a STRING value >= DEFAULT_STRING_BLOCK_LIMIT lives in
        // a separate overflow block, and the dictionary marker naming it is TRANSIENT. These two
        // entry points let column_checkpoint_state_t move the payload into the file and rewrite
        // the markers on-disk. They live here because the dictionary layout is private to the .cpp.

        // Cheap pre-check: does any row use an overflow marker? False for a column with no big
        // strings (lets the checkpoint skip the segment copy). A too-small segment answers TRUE
        // so persist_string_overflow reports the corruption instead of copying through silently.
        bool
        references_string_overflow(const std::byte* segment_data, uint64_t segment_size, uint64_t tuple_count) const;

        // `segment_copy` is a WRITABLE byte copy of this segment's payload. For every overflow
        // marker in it: copy the payload through `pbm` into a fresh file block and rewrite the
        // marker to name it; allocated ids are appended to `out_blocks`. The LIVE segment is
        // untouched and stays readable through its existing blocks.
        [[nodiscard]] core::result_wrapper_t<bool> persist_string_overflow(std::byte* segment_copy,
                                                                           uint64_t segment_size,
                                                                           uint64_t tuple_count,
                                                                           storage::partial_block_manager_t& pbm,
                                                                           std::vector<uint64_t>& out_blocks);

        // A partially-filled STRING segment keeps its dictionary pressed against the END of the
        // allocation, leaving the unused middle as zero slack; persisting the whole allocation
        // writes that slack to the file. This slides the dictionary of `segment_copy` down
        // against the offset array, rewrites the stored dictionary end, and returns the tight
        // byte size — the caller persists only that prefix. The LIVE segment is untouched; the
        // read side resolves every string relative to the stored dictionary end, so the trimmed
        // image reloads unchanged. Returns data_corruption on an inconsistent image.
        [[nodiscard]] core::result_wrapper_t<uint64_t>
        compact_string_dictionary(std::byte* segment_copy, uint64_t segment_size, uint64_t tuple_count) const;

        // OOM-propagating: pin/allocate failures surface as out_of_memory.
        [[nodiscard]] core::result_wrapper_t<bool> resize(uint64_t segment_size);

        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(column_append_state& state);
        [[nodiscard]] core::result_wrapper_t<uint64_t>
        append(column_append_state& state, vector::unified_vector_format& data, uint64_t offset, uint64_t count);
        [[nodiscard]] core::result_wrapper_t<uint64_t> finalize_append(column_append_state& state);
        // Returns out_of_memory when the dictionary/bitmap rollback pin fails: skipping the
        // rollback silently leaves the reverted payload spliced onto the next appended value.
        [[nodiscard]] core::result_wrapper_t<bool> revert_append(uint64_t start_row);

        uint64_t block_id() { return block_id_; }

        // Geometry only: a segment that has not reached the disk yet has no block manager, and
        // the numbers are the same either way.
        uint64_t block_size() const { return block->block_size(); }

        uint64_t block_offset() { return offset_; }

        int64_t relative_index(int64_t row_index) {
            assert(row_index >= start);
            assert(row_index <= start + static_cast<int64_t>(count));
            return row_index - start;
        }

        compressed_segment_state* segment_state() { return segment_state_.get(); }

        const base_statistics_t& segment_statistics() const { return segment_statistics_; }
        void set_segment_statistics(base_statistics_t stats) { segment_statistics_ = std::move(stats); }

        compression::compression_type compression() const { return compression_; }
        void set_compression(compression::compression_type c) { compression_ = c; }

    private:
        void scan(column_scan_state& state, uint64_t scan_count, vector::vector_t& result);
        void
        scan_partial(column_scan_state& state, uint64_t scan_count, vector::vector_t& result, uint64_t result_offset);

        uint64_t block_id_;
        uint64_t offset_;
        uint64_t segment_size_;
        std::unique_ptr<compressed_segment_state> segment_state_;
        base_statistics_t segment_statistics_;
        compression::compression_type compression_{compression::compression_type::UNCOMPRESSED};
        // See has_construction_error(). State like any other: the move constructors carry it
        // (test_construction_error_move.cpp pins that a latched error survives a move).
        core::error_t construction_error_{core::error_t::no_error()};
    };

} // namespace components::table