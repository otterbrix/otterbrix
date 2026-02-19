#include "column_checkpoint_state.hpp"

#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/storage/block_manager.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_manager.hpp>

namespace components::table {

    column_checkpoint_state_t::column_checkpoint_state_t(column_data_t& column_data,
                                                         storage::partial_block_manager_t& partial_block_manager)
        : column_data_(column_data)
        , partial_block_manager_(partial_block_manager) {}

    void column_checkpoint_state_t::flush_segment(column_segment_t& segment,
                                                   uint64_t row_start,
                                                   uint64_t tuple_count) {
        auto& block_manager = column_data_.block_manager();
        auto segment_size = segment.segment_size();

        // get block allocation (may share block with other small segments)
        auto allocation = partial_block_manager_.get_block_allocation(segment_size);

        // pin the segment's buffer to get data
        auto handle = block_manager.buffer_manager.pin(segment.block);
        auto* data = handle.ptr();

        // write segment data into partial block manager's buffer (deferred disk write)
        if (data && segment_size > 0) {
            partial_block_manager_.write_to_block(allocation.block_id, allocation.offset_in_block, data, segment_size);
        }

        // record data pointer
        storage::data_pointer_t dp;
        dp.row_start = row_start;
        dp.tuple_count = tuple_count;
        dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
        dp.compression = compression::compression_type::UNCOMPRESSED;
        dp.segment_size = segment_size;
        data_pointers_.push_back(dp);
    }

    persistent_column_data_t column_checkpoint_state_t::get_persistent_data() const {
        persistent_column_data_t result;
        result.data_pointers = data_pointers_;
        return result;
    }

} // namespace components::table
