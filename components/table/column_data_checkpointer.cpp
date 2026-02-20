#include "column_data_checkpointer.hpp"

#include <components/table/column_checkpoint_state.hpp>
#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>

namespace components::table {

    column_data_checkpointer_t::column_data_checkpointer_t(column_data_t& column_data,
                                                            storage::partial_block_manager_t& partial_block_manager)
        : column_data_(column_data)
        , partial_block_manager_(partial_block_manager) {}

    persistent_column_data_t column_data_checkpointer_t::checkpoint() {
        column_checkpoint_state_t state(column_data_, partial_block_manager_);

        // Collect per-segment stats while flushing
        std::vector<base_statistics_t> seg_stats;
        for (auto& segment : column_data_.data_.segments()) {
            state.flush_segment(segment, static_cast<uint64_t>(segment.start), segment.count);
            seg_stats.push_back(segment.segment_statistics());
        }

        auto result = state.get_persistent_data();
        if (column_data_.statistics_.has_stats()) {
            result.statistics = column_data_.statistics_;
        }
        result.segment_statistics = std::move(seg_stats);
        return result;
    }

} // namespace components::table
