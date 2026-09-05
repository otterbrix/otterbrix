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
        // Width of ONE element in this segment's RAW payload -- NOT always type.size(). A LIST
        // segment stores one uint64 child-offset per row while sizeof(list_entry_t) is 16, so
        // the two differ there. Every raw-byte consumer (the checkpoint's CONSTANT/RLE/
        // DICTIONARY analysis, the compressed scan/fetch paths) reads this and nothing else;
        // taking the logical size instead overran the result vector by 2x on a reloaded LIST
        // column. See impl::stored_element_size in the .cpp.
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

        // Latched failure of the RELOAD constructor. A constructor has no return channel and
        // MUST NOT throw here: segments are built while a table is being opened, on the agent
        // thread (rules 2/6/9 -- loud, but an abort on the open path makes the database
        // unopenable). The one thing that can fail is the persisted big-string overflow list:
        // uncompressed_string_segment_state::register_block refuses a block id the same list
        // already named, because the writer dedupes and a duplicate therefore means the
        // metadata stream is corrupt. column_data_t::initialize_column reads this immediately
        // after constructing the segment and turns it into the data_corruption it already has
        // a result_wrapper_t for.
        [[nodiscard]] bool has_construction_error() const noexcept {
            return construction_error_.contains_error();
        }
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

        // --- F1: big-string overflow persistence -------------------------------------------
        // A STRING value >= DEFAULT_STRING_BLOCK_LIMIT does not fit the segment dictionary; the
        // dictionary holds a 16-byte (block id, offset) marker and the bytes live in a separate
        // overflow block. Checkpointing the segment block verbatim therefore persists MARKERS
        // WITHOUT PAYLOAD, and the marker names a TRANSIENT block that dies with the process.
        // These two entry points let column_checkpoint_state_t move the payload into the file
        // and rewrite the markers into the on-disk id domain. They live here because the
        // dictionary layout is private to this translation unit.

        // Cheap pre-check on the pinned segment payload: does any row use an overflow marker?
        // False for every string column with no big strings, which lets the checkpoint skip the
        // segment copy entirely. A segment too small to hold its own offset array answers TRUE,
        // so persist_string_overflow gets to report the corruption instead of it being copied
        // through silently.
        bool
        references_string_overflow(const std::byte* segment_data, uint64_t segment_size, uint64_t tuple_count) const;

        // `segment_copy` is a WRITABLE byte copy of this segment's payload (segment_size bytes,
        // starting at the segment, not at the block). For every overflow marker in it: read the
        // payload from wherever it currently lives (a transient block for a freshly appended
        // segment, a file block for a segment reloaded from an earlier checkpoint), copy that
        // payload through `pbm` into a freshly allocated file block, and rewrite the marker in
        // `segment_copy` to name the new block and offset. The allocated block ids are appended
        // to `out_blocks` for data_pointer_t::overflow_blocks. The LIVE segment is untouched and
        // stays readable through its existing blocks.
        [[nodiscard]] core::result_wrapper_t<bool>
        persist_string_overflow(std::byte* segment_copy,
                                uint64_t segment_size,
                                uint64_t tuple_count,
                                storage::partial_block_manager_t& pbm,
                                std::vector<uint64_t>& out_blocks);

        // OOM-propagating: pin/allocate failures surface as out_of_memory.
        [[nodiscard]] core::result_wrapper_t<bool> resize(uint64_t segment_size);

        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(column_append_state& state);
        [[nodiscard]] core::result_wrapper_t<uint64_t>
        append(column_append_state& state, vector::unified_vector_format& data, uint64_t offset, uint64_t count);
        [[nodiscard]] core::result_wrapper_t<uint64_t> finalize_append(column_append_state& state);
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
        // See has_construction_error(). Not moved by the move constructors on purpose: a
        // segment is only ever moved AFTER initialize_column has read this.
        core::error_t construction_error_{core::error_t::no_error()};
    };

} // namespace components::table