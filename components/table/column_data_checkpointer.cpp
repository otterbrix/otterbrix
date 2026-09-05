#include "column_data_checkpointer.hpp"

#include <memory_resource>
#include <string>

#include <components/table/column_checkpoint_state.hpp>
#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/update_segment.hpp>

namespace components::table {

    column_data_checkpointer_t::column_data_checkpointer_t(column_data_t& column_data,
                                                           storage::partial_block_manager_t& partial_block_manager)
        : column_data_(column_data)
        , partial_block_manager_(partial_block_manager) {}

    core::result_wrapper_t<persistent_column_data_t> column_data_checkpointer_t::checkpoint() {
        // The update overlay (column_data_.updates_) is not a segment, and this function walks
        // only data_.segments(): checkpointing while the overlay holds a value would write
        // PRE-update bytes, then let the caller seal away the WAL record that was the value's
        // only remaining copy. Folding it in is data_table_t::compact's job, which is why
        // agent_disk_t::checkpoint_inner compacts every entry before checkpointing it, and its
        // failed-round retry (the one path that skips the rebuild) checks
        // table_storage_t::has_pending_update_overlay() first. Reaching here with an overlay
        // outstanding means the round is refused instead: a check, not an assert that
        // vanishes under NDEBUG. Safe to refuse mid-round: table_storage_t::checkpoint's
        // rollback discriminates by registry_alive(), so earlier columns' already-flushed blocks
        // are kept and only the round's abandoned tail is released; no header is written.
        if (column_data_.updates_ && column_data_.updates_->has_updates()) {
            std::pmr::string what{"column_data_checkpointer_t::checkpoint: column ", column_data_.resource()};
            what.append(std::to_string(column_data_.column_index_).c_str());
            if (column_data_.type().has_alias()) {
                what.append(" ('");
                what.append(std::string(column_data_.type().alias()).c_str());
                what.append("')");
            }
            what.append(" carries a committed-update overlay, which a checkpoint cannot serialize: the "
                        "rebuild that folds it into the column's segments has to run first");
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(what)};
        }

        column_checkpoint_state_t state(column_data_, partial_block_manager_);

        // Collect per-segment stats while flushing
        std::vector<base_statistics_t> seg_stats;
        for (auto& segment : column_data_.data_.segments()) {
            // flush_segment returns out_of_memory when pinning the segment buffer fails;
            // propagate it up the checkpoint chain to the agent_disk boundary.
            auto flushed = state.flush_segment(segment, static_cast<uint64_t>(segment.start), segment.count);
            if (flushed.has_error()) {
                return flushed.convert_error<persistent_column_data_t>();
            }
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
