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
        // THE UPDATE OVERLAY IS NOT A SEGMENT, AND THIS FUNCTION WRITES ONLY SEGMENTS.
        //
        // A committed update — data_table_t::update -> column_data_t::update_internal, the
        // WAL-replay PHYSICAL_UPDATE path — lands in column_data_.updates_, not in
        // data_.segments(). The loop below walks the segments and nothing else, so a
        // checkpoint taken while the overlay holds a value writes the PRE-update bytes and
        // reports success; the caller advances the `.otbx.wal_id` sidecar on that success,
        // sealing away the journal record that is the only remaining copy of the value. The
        // row silently reverts at the next start, with nothing left to restore it from.
        //
        // Folding the overlay in is the REBUILD's job, and the rebuild already does it: the
        // scan inside data_table_t::compact reads through the overlay and appends the merged
        // values into a fresh collection whose columns carry no overlay at all. That is why
        // agent_disk_t::checkpoint_inner compacts every entry immediately before
        // checkpointing it, and why its failed-round retry — the one path that skips the
        // rebuild — consults table_storage_t::has_pending_update_overlay() before doing so.
        // Reaching here with an overlay outstanding means no rebuild ran, so the round is
        // refused instead of writing a stale value.
        //
        // Loud, not fatal (rule 6): the refusal travels the chain the caller already reads
        // (row_group_t::write_to_disk -> data_table_t::checkpoint -> table_storage_t::
        // checkpoint -> checkpoint_inner), where it defers the entry with its wal-id fields
        // unchanged and keeps its WAL records.
        //
        // WHAT MAKES THAT COST THE ROUND AND NOT THE TABLE IS THE ROLLBACK, NOT THE POSITION
        // OF THIS LINE. The refusal is per COLUMN inside a whole-table round, and
        // row_group_t::write_to_disk runs the columns in order: by the time a later column
        // reaches here, the earlier ones have already flushed segments into freshly allocated
        // blocks and re-pointed their live tails. Those blocks are given back by
        // table_storage_t::checkpoint, which calls single_file_block_manager_t::
        // roll_back_uncommitted_round on every failure below (and excluding) write_header --
        // and that rollback DISCRIMINATES by registry_alive(), so a re-pointed segment that a
        // live column still owns is kept while the round's abandoned tail is released. The
        // durable root is untouched either way: no header slot is written on this path.
        //
        // A CHECK AND NOT AN assert, deliberately: under NDEBUG an assert disappears and the
        // branch it guarded becomes exactly the silent wrong answer described above.
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
