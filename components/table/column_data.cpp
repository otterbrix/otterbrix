#include "column_data.hpp"

#include <atomic>

#include <cstdio>
#include <cstring>

#include <components/types/types.hpp>
#include <components/vector/validation.hpp>

#include "array_column_data.hpp"
#include "column_checkpoint_state.hpp"
#include "column_data_checkpointer.hpp"
#include "column_state.hpp"
#include "list_column_data.hpp"
#include "persistent_column_data.hpp"
#include "row_group.hpp"
#include "standard_column_data.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_handle.hpp"
#include "storage/buffer_manager.hpp"
#include "storage/partial_block_manager.hpp"
#include "struct_column_data.hpp"
#include "validity_column_data.hpp"

namespace components::table {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_transitions_with_live_pin{0};
        std::atomic<uint64_t> g_segment_transitions{0};
    } // namespace

    uint64_t transitions_with_live_pin() noexcept {
        return g_transitions_with_live_pin.load(std::memory_order_relaxed);
    }
    uint64_t segment_transitions() noexcept { return g_segment_transitions.load(std::memory_order_relaxed); }
    void reset_transitions_with_live_pin() noexcept {
        g_transitions_with_live_pin.store(0, std::memory_order_relaxed);
        g_segment_transitions.store(0, std::memory_order_relaxed);
    }
#endif
    column_data_t::column_data_t(std::pmr::memory_resource* resource,
                                 storage::block_manager_t& block_manager,
                                 uint64_t column_index,
                                 int64_t start_row,
                                 types::complex_logical_type type,
                                 column_data_t* parent)
        : start_(start_row)
        , count_(0)
        , block_manager_(block_manager)
        , column_index_(column_index)
        , type_(std::move(type))
        , parent_(std::move(parent))
        , allocation_size_(0)
        , statistics_(resource, type_.type())
        , resource_(resource) {}

    filter_propagate_result_t column_data_t::check_zonemap(column_scan_state&, table_filter_t&) {
        if (!statistics_.has_stats()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        if (statistics_.min_value().is_null() || statistics_.max_value().is_null()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        if (has_update_segment()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        // Pruning needs a constant filter's (column, op, constant); a filter graph doesn't expose one.
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    filter_propagate_result_t column_data_t::check_segment_zonemap(column_scan_state& state, table_filter_t&) {
        if (!state.current || !state.current->segment_statistics().has_stats()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        auto& seg_stats = state.current->segment_statistics();
        if (!seg_stats.has_stats() || seg_stats.min_value().is_null() || seg_stats.max_value().is_null()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    uint64_t column_data_t::max_entry() { return count_; }

    void column_data_t::set_start(int64_t new_start) {
        start_ = new_start;
        uint64_t offset = 0;
        for (auto& segment : data_.segments()) {
            segment.start = start_ + static_cast<int64_t>(offset);
            offset += segment.count;
        }
        if (!data_.reinitialize()) {
            std::fprintf(stderr,
                         "components::table::column_data_t::set_start: segment starts are not contiguous after "
                         "re-basing; the row_start map was left untouched\n");
        }
    }

    const types::complex_logical_type& column_data_t::root_type() const {
        if (parent_) {
            return parent_->root_type();
        }
        return type_;
    }

    bool column_data_t::has_update_segment() const { return updates_.get(); }

    scan_vector_type
    column_data_t::get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result) {
        if (result.get_vector_type() != vector::vector_type::FLAT) {
            return scan_vector_type::SCAN_ENTIRE_VECTOR;
        }
        if (has_update_segment()) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        if (!state.current) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        uint64_t remaining_in_segment =
            static_cast<uint64_t>(state.current->start) + state.current->count - static_cast<uint64_t>(state.row_index);
        if (remaining_in_segment < scan_count) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        return scan_vector_type::SCAN_FLAT_VECTOR;
    }

    void column_data_t::initialize_scan(column_scan_state& state) {
        state.current = data_.root_segment();
        state.row_index = state.current ? state.current->start : 0;
        state.internal_index = state.row_index;
        state.initialized = false;
        state.scan_state.reset();
    }

    void column_data_t::initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) {
        state.current = data_.get_segment(row_idx);
        state.row_index = row_idx;
        if (!state.current) {
            state.initialized = false;
            state.scan_error =
                core::error_t(core::error_code_t::invalid_parameter,
                              std::pmr::string("column scan: the seek row names no segment of this column", resource_));
            return;
        }
        state.internal_index = state.current->start;
        state.initialized = false;
        state.scan_state.reset();
        state.last_offset = 0;
    }

    uint64_t column_data_t::scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result) {
        auto target_count = vector_count(vector_index);
        return scan(vector_index, state, result, target_count);
    }

    uint64_t column_data_t::scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           bool allow_updates) {
        auto target_count = vector_count(vector_index);
        return scan_committed(vector_index, state, result, allow_updates, target_count);
    }

    uint64_t column_data_t::scan(uint64_t vector_index,
                                 column_scan_state& state,
                                 vector::vector_t& result,
                                 uint64_t scan_count) {
        return scan_vector<false, true>(vector_index, state, result, scan_count);
    }

    uint64_t column_data_t::scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           bool allow_updates,
                                           uint64_t scan_count) {
        if (allow_updates) {
            return scan_vector<true, true>(vector_index, state, result, scan_count);
        } else {
            return scan_vector<true, false>(vector_index, state, result, scan_count);
        }
    }

    // Deliberately no scan_committed_range: it read through a scan_state whose scan_error nobody checked.

    uint64_t column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        return scan_count_with_updates(state, result, count);
    }

    uint64_t
    column_data_t::scan_count_with_updates(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        // Capture result_offset/row_index before scanning: scan_vector advances both.
        const uint64_t result_offset = state.result_offset;
        const int64_t range_start = state.row_index - start_;
        auto scanned = scan_vector(state, result, count, scan_vector_type::SCAN_FLAT_VECTOR);
        if (updates_ && scanned > 0) {
            result.flatten(result_offset + scanned);
            updates_->fetch_committed_range(range_start, scanned, result, result_offset);
        }
        return scanned;
    }

    void column_data_t::select(uint64_t vector_index,
                               column_scan_state& state,
                               vector::vector_t& result,
                               vector::indexing_vector_t& indexing,
                               uint64_t s_count) {
        scan(vector_index, state, result);
        result.slice(indexing, s_count);
    }

    void column_data_t::select_committed(uint64_t vector_index,
                                         column_scan_state& state,
                                         vector::vector_t& result,
                                         vector::indexing_vector_t& indexing,
                                         uint64_t s_count,
                                         bool allow_updates) {
        scan_committed(vector_index, state, result, allow_updates);
        result.slice(indexing, s_count);
    }

    void column_data_t::filter_scan(uint64_t vector_index,
                                    column_scan_state& state,
                                    vector::vector_t& result,
                                    vector::indexing_vector_t& indexing,
                                    uint64_t count) {
        scan(vector_index, state, result);
        result.slice(indexing, count);
    }

    void column_data_t::filter_scan_committed(uint64_t vector_index,
                                              column_scan_state& state,
                                              vector::vector_t& result,
                                              vector::indexing_vector_t& indexing,
                                              uint64_t count,
                                              bool allow_updates) {
        scan_committed(vector_index, state, result, allow_updates);
        result.slice(indexing, count);
    }

    void column_data_t::skip(column_scan_state& state, uint64_t count) { state.next(count); }

    core::result_wrapper_t<bool> column_data_t::initialize_append(column_append_state& state) {
        auto l = data_.lock();
        if (data_.is_empty(l)) {
            auto created = apend_transient_segment(l, start_);
            if (created.has_error()) {
                return created; // out_of_memory
            }
        }
        auto segment = data_.last_segment(l);
        // A disk-loaded segment is READ-ONLY (shared buffer); is_reloadable(), not block_offset()==0,
        // is the real test -- the checkpointer can pack the first column at offset 0 too.
        const bool is_disk_loaded = segment->block && segment->block->is_reloadable();
        if (is_disk_loaded || segment->block_offset() != 0) {
            auto created = apend_transient_segment(l, segment->start + static_cast<int64_t>(segment->count));
            if (created.has_error()) {
                return created; // out_of_memory
            }
            segment = data_.last_segment(l);
        }
        state.current = segment;
        auto init = state.current->initialize_append(state);
        if (init.has_error()) {
            return init;
        }
        return true;
    }

    core::result_wrapper_t<bool>
    column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        base_statistics_t batch_stats(resource_, type_.type());
        batch_stats.update(vector, count);
        statistics_.merge(batch_stats);
        if (state.current) {
            if (state.current->segment_statistics().has_stats()) {
                auto merged = state.current->segment_statistics();
                merged.merge(batch_stats);
                state.current->set_segment_statistics(std::move(merged));
            } else {
                state.current->set_segment_statistics(std::move(batch_stats));
            }
        }
        vector::unified_vector_format uvf(vector.resource(), count);
        vector.to_unified_format(count, uvf);
        return append_data(state, uvf, count);
    }

    core::result_wrapper_t<bool>
    column_data_t::append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t append_count) {
        uint64_t offset = 0;
        this->count_ += append_count;
        // A local partial_block_manager packs filled segments via the checkpoint's own allocator,
        // flushed at the end so every re-pointed block is durable before the append returns.
        storage::partial_block_manager_t pbm(block_manager_);
        bool any_transitioned = false;
        while (true) {
            auto appended = state.current->append(state, uvf, offset, append_count);
            if (appended.has_error()) {
                return appended.convert_error<bool>();
            }
            uint64_t copied_elements = appended.value();
            if (copied_elements == append_count) {
                break;
            }

            {
                auto l = data_.lock();
                // Capture the filled segment's index before appending the next one: state.current moves off it below.
                const uint64_t filled_index = data_.segment_count(l) - 1;
                // Release the pin before the swap frees its block_handle_t, or it unpins through
                // freed memory (see the [appendpin] test).
                state.handle.reset();
                auto created =
                    apend_transient_segment(l, state.current->start + static_cast<int64_t>(state.current->count));
                if (created.has_error()) {
                    return created; // out_of_memory
                }
                auto transitioned = transition_segment_to_disk(l, filled_index, pbm);
                if (transitioned.has_error()) {
                    return transitioned;
                }
                any_transitioned = true;
                state.current = data_.last_segment(l);
                auto init = state.current->initialize_append(state);
                if (init.has_error()) {
                    return init;
                }
            }
            offset += copied_elements;
            append_count -= copied_elements;
        }
        if (any_transitioned) {
            if (auto flushed = pbm.flush_partial_blocks(); flushed.has_error()) {
                return flushed; // io_error
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> column_data_t::revert_append(int64_t start_row) {
        auto l = data_.lock();
        auto last_segment = data_.last_segment(l);
        if (!last_segment) {
            return true; // no segments -> nothing was appended -> nothing to revert
        }
        if (start_row >= last_segment->start + static_cast<int64_t>(last_segment->count)) {
            assert(start_row == last_segment->start + static_cast<int64_t>(last_segment->count));
            return true;
        }
        uint64_t segment_index;
        if (!data_.try_segment_index(l, start_row, segment_index)) {
            // Names a row the tree doesn't bracket; truncating nearby would manufacture the desync revert undoes.
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("column revert: no segment brackets the revert row", resource_));
        }
        auto segment = data_.segment_at(l, static_cast<int64_t>(segment_index));
        auto& transient = *segment;

        data_.erase_segments(l, segment_index);

        count_ = static_cast<uint64_t>(start_row - start_);
        segment->next = nullptr;
        return transient.revert_append(static_cast<uint64_t>(start_row));
    }

    uint64_t column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        assert(row_id >= 0);
        assert(row_id >= start_);
        state.row_index = start_ + (row_id - start_) / static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY *
                                                                            vector::DEFAULT_VECTOR_CAPACITY);
        state.current = data_.get_segment(state.row_index);
        if (!state.current) {
            // get_segment's miss (null) rides the scan state's error channel every fetch() caller reads.
            state.scan_error =
                core::error_t(core::error_code_t::invalid_parameter,
                              std::pmr::string("column fetch: the row id names no segment of this column", resource_));
            return 0;
        }
        state.internal_index = state.current->start;
        return scan_vector(state, result, vector::DEFAULT_VECTOR_CAPACITY, scan_vector_type::SCAN_FLAT_VECTOR);
    }

    void
    column_data_t::fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx) {
        auto segment = data_.get_segment(row_id);
        if (!segment) {
            state.fetch_error =
                core::error_t(core::error_code_t::invalid_parameter,
                              std::pmr::string("column fetch: the row id names no segment of this column", resource_));
            return;
        }

        segment->fetch_row(state, row_id, result, result_idx);

        fetch_update_row(row_id, result, result_idx);
    }

    core::result_wrapper_t<bool> column_data_t::update(uint64_t column_index,
                                                       vector::vector_t& update_vector,
                                                       int64_t* row_ids,
                                                       uint64_t update_count) {
        vector::vector_t base_vector(resource_, type_, count_);
        column_scan_state state;
        auto fetch_count = fetch(state, row_ids[0], base_vector);
        // Pre-image is prior version for update_internal; unread on failure, rollback would materialise "" silently.
        if (state.has_error()) {
            return state.scan_error;
        }

        base_vector.flatten(fetch_count);
        return update_internal(column_index, update_vector, row_ids, update_count, base_vector);
    }

    core::result_wrapper_t<bool> column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                                              vector::vector_t& update_vector,
                                                              int64_t* row_ids,
                                                              uint64_t update_count,
                                                              uint64_t) {
        return column_data_t::update(column_path[0], update_vector, row_ids, update_count);
    }

    void column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                std::vector<uint64_t> col_path,
                                                std::vector<column_segment_info>& result) {
        assert(!col_path.empty());

        std::string col_path_str = "[";
        for (uint64_t i = 0; i < col_path.size(); i++) {
            if (i > 0) {
                col_path_str += ", ";
            }
            col_path_str += std::to_string(col_path[i]);
        }
        col_path_str += "]";

        uint64_t segment_idx = 0;
        auto segment = data_.root_segment();
        while (segment) {
            column_segment_info column_info;
            column_info.row_group_index = row_group_index;
            column_info.column_id = col_path[0];
            column_info.column_path = col_path_str;
            column_info.segment_idx = segment_idx;
            column_info.segment_start = segment->start;
            column_info.segment_count = segment->count;
            column_info.has_updates = has_update_segment();
            const bool disk_backed = segment->block && segment->block->is_reloadable();
            column_info.segment_type = disk_backed ? "PERSISTENT" : "TRANSIENT";
            column_info.block_id = disk_backed ? static_cast<uint32_t>(segment->block_id()) : 0;
            column_info.block_offset = segment->block_offset();
            auto segment_state = segment->segment_state();
            if (segment_state) {
                column_info.segment_info = segment_state->segment_info();
                column_info.additional_blocks = segment_state->additional_blocks();
            }
            result.emplace_back(column_info);

            segment_idx++;
            segment = data_.next_segment(segment);
        }
    }

    core::error_t column_data_t::validate_column_type(const types::complex_logical_type& type,
                                                      std::pmr::memory_resource* resource) {
        // Checked before any node exists (a constructor can't refuse); STRUCT must be named, UNION is exempt
        // because create_union deliberately leaves the alias empty.
        const auto physical = type.to_physical_type();
        if (physical == types::physical_type::STRUCT) {
            if (type.type() != types::logical_type::UNION && type.is_unnamed()) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string("a table column cannot be built from an unnamed struct type", resource));
            }
            // child_types() is an unchecked cast on non-struct extensions, valid only under the STRUCT test above.
            for (const auto& child : type.child_types()) {
                if (auto err = validate_column_type(child, resource); err.contains_error()) {
                    return err;
                }
            }
            return core::error_t::no_error();
        }
        if (physical == types::physical_type::LIST || physical == types::physical_type::ARRAY) {
            return validate_column_type(type.child_type(), resource);
        }
        return core::error_t::no_error();
    }

    std::unique_ptr<column_data_t> column_data_t::create_column(std::pmr::memory_resource* resource,
                                                                storage::block_manager_t& block_manager,
                                                                uint64_t column_index,
                                                                int64_t start_row,
                                                                const types::complex_logical_type& type,
                                                                column_data_t* parent) {
        if (type.to_physical_type() == types::physical_type::STRUCT) {
            return std::make_unique<struct_column_data_t>(resource,
                                                          block_manager,
                                                          column_index,
                                                          start_row,
                                                          type,
                                                          parent);
        } else if (type.to_physical_type() == types::physical_type::LIST) {
            return std::make_unique<list_column_data_t>(resource, block_manager, column_index, start_row, type, parent);
        } else if (type.to_physical_type() == types::physical_type::ARRAY) {
            return std::make_unique<array_column_data_t>(resource,
                                                         block_manager,
                                                         column_index,
                                                         start_row,
                                                         type,
                                                         parent);
        } else if (type.to_physical_type() == types::physical_type::BIT) {
            return std::make_unique<validity_column_data_t>(resource, block_manager, column_index, start_row, *parent);
        }
        return std::make_unique<standard_column_data_t>(resource, block_manager, column_index, start_row, type, parent);
    }

    core::result_wrapper_t<bool> column_data_t::apend_transient_segment(std::unique_lock<std::mutex>& l,
                                                                        int64_t start_row) {
        const auto block_size = block_manager_.block_size();
        const auto type_size = type_.size();

        // Sized to what a segment holds, not a whole block: 8 KiB for BIGINT vs 256 KiB (97% empty). A
        // 17-column table sized-by-block ran out of its 4 GiB pool at 492 500 rows holding only 493 MiB.
        const auto vector_segment_size = vector::DEFAULT_VECTOR_CAPACITY * type_size;

        uint64_t segment_size = block_size < vector_segment_size ? block_size : vector_segment_size;
        auto new_segment =
            column_segment_t::create_segment(block_manager_.buffer_manager, type_, start_row, segment_size, block_size);
        if (new_segment.has_error()) {
            return new_segment.convert_error<bool>();
        }
        allocation_size_ += segment_size;
        data_.append_segment(l, std::move(new_segment.value()));
        return true;
    }

    core::result_wrapper_t<bool> column_data_t::transition_segment_to_disk(std::unique_lock<std::mutex>& l,
                                                                           uint64_t segment_index,
                                                                           storage::partial_block_manager_t& pbm) {
        auto* segment = data_.segment_at(l, static_cast<int64_t>(segment_index));
        if (!segment) {
            return true;
        }

        if (segment->block && segment->block->is_reloadable()) {
            return true; // already disk-backed
        }
        if (segment->block_offset() != 0) {
            return true; // shares a block with other segments (loaded) -- not a fresh transient
        }
        if (segment->compression() != compression::compression_type::UNCOMPRESSED) {
            return true;
        }
        // STRUCT/ARRAY/LIST/INVALID own no storage of their own here -- nothing to re-point.
        const auto phys = segment->type.to_physical_type();
        if (phys == types::physical_type::INVALID || phys == types::physical_type::STRUCT ||
            phys == types::physical_type::ARRAY || phys == types::physical_type::LIST) {
            return true;
        }

        // An EMPTY segment is left managed rather than persist a degenerate segment_size==0 disk segment.
        if (segment->count.load() == 0) {
            return true;
        }

        // Snapshot the segment metadata before touching the pin: the segment is destroyed by the swap below.
        const int64_t seg_start = segment->start;
        const uint64_t seg_count = segment->count.load();
        const uint64_t alloc_segment_size = segment->segment_size();
        const uint64_t block_offset = segment->block_offset();
        assert(alloc_segment_size <= block_manager_.block_size());
        const bool has_stats = segment->segment_statistics().has_stats();
        base_statistics_t seg_stats =
            has_stats ? segment->segment_statistics() : base_statistics_t(resource_, type_.type());

        // STRING isn't a raw prefix copy: its dictionary grows down from the end of the allocation, so
        // it re-serializes through the checkpoint's own pipeline instead, byte-identical to a checkpoint copy.
        if (phys == types::physical_type::STRING) {
            std::pmr::vector<std::byte> rewritten(alloc_segment_size, std::byte{0}, resource_);
            {
                auto pinned = block_manager_.buffer_manager.pin(segment->block);
                if (pinned.has_error()) {
                    return pinned.convert_error<bool>();
                }
                std::memcpy(rewritten.data(), pinned.value().ptr() + block_offset, alloc_segment_size);
            }
            auto compacted = segment->compact_string_dictionary(rewritten.data(), alloc_segment_size, seg_count);
            if (compacted.has_error()) {
                return compacted.convert_error<bool>(); // data_corruption
            }
            const uint64_t tight_size = compacted.value();
            std::vector<uint64_t> overflow_ids;
            if (segment->references_string_overflow(rewritten.data(), tight_size, seg_count)) {
                auto persisted =
                    segment->persist_string_overflow(rewritten.data(), tight_size, seg_count, pbm, overflow_ids);
                if (persisted.has_error()) {
                    return persisted; // out_of_memory / data_corruption
                }
            }
            const auto string_alloc = pbm.get_block_allocation(tight_size);
            pbm.write_to_block(string_alloc.block_id, string_alloc.offset_in_block, rewritten.data(), tight_size);
            auto string_block_handle = block_manager_.register_block(string_alloc.block_id);
            // Adopted markers name real file blocks, kept alive by the reload constructor's registration
            // (test_string_write_through gate H).
            std::unique_ptr<column_segment_state> overflow_state;
            if (!overflow_ids.empty()) {
                overflow_state = std::make_unique<column_segment_state>();
                overflow_state->blocks = std::move(overflow_ids);
            }
            auto disk_segment = std::make_unique<column_segment_t>(string_block_handle,
                                                                   type_,
                                                                   seg_start,
                                                                   seg_count,
                                                                   static_cast<uint32_t>(string_alloc.block_id),
                                                                   string_alloc.offset_in_block,
                                                                   tight_size,
                                                                   std::move(overflow_state));
            if (disk_segment->has_construction_error()) {
                return core::error_t(disk_segment->construction_error());
            }
            disk_segment->set_compression(compression::compression_type::UNCOMPRESSED);
            if (has_stats) {
                disk_segment->set_segment_statistics(std::move(seg_stats));
            }
#ifdef DEV_MODE
            g_segment_transitions.fetch_add(1, std::memory_order_relaxed);
            if (segment->block && segment->block->readers() > 0) {
                g_transitions_with_live_pin.fetch_add(1, std::memory_order_relaxed);
            }
#endif
            data_.replace_segment_at_index(l, segment_index, std::move(disk_segment));
            return true;
        }

        // Tightly-used extent, not the full block, or packing would exceed 0.8*block_size.
        uint64_t used_bytes;
        if (phys == types::physical_type::BIT) {
            const uint64_t vectors =
                (seg_count + vector::DEFAULT_VECTOR_CAPACITY - 1) / vector::DEFAULT_VECTOR_CAPACITY;
            used_bytes = vectors * vector::validity_mask_t::STANDARD_MASK_SIZE;
        } else {
            used_bytes = seg_count * type_.size();
        }
        if (used_bytes > alloc_segment_size) {
            used_bytes = alloc_segment_size;
        }
        const uint64_t segment_size = used_bytes;

        const auto alloc = pbm.get_block_allocation(segment_size);

        // On pin OOM, alloc.block_id stays allocated (a packed block may be shared); freeing it would corrupt others.
        {
            auto& buffer_manager = block_manager_.buffer_manager;
            auto pinned = buffer_manager.pin(segment->block);
            if (pinned.has_error()) {
                return pinned.convert_error<bool>();
            }
            auto* payload = pinned.value().ptr() + block_offset;
            pbm.write_to_block(alloc.block_id, alloc.offset_in_block, payload, segment_size);
        }

        auto block_handle = block_manager_.register_block(alloc.block_id);
        auto new_segment = std::make_unique<column_segment_t>(block_handle,
                                                              type_,
                                                              seg_start,
                                                              seg_count,
                                                              static_cast<uint32_t>(alloc.block_id),
                                                              alloc.offset_in_block,
                                                              segment_size);
        new_segment->set_compression(compression::compression_type::UNCOMPRESSED);
        if (has_stats) {
            new_segment->set_segment_statistics(std::move(seg_stats));
        }

#ifdef DEV_MODE
        g_segment_transitions.fetch_add(1, std::memory_order_relaxed);
        if (segment->block && segment->block->readers() > 0) {
            g_transitions_with_live_pin.fetch_add(1, std::memory_order_relaxed);
        }
#endif
        data_.replace_segment_at_index(l, segment_index, std::move(new_segment));
        return true;
    }

    void column_data_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const {
        // One entry per reloadable segment, not per dedicated block -- packing shares blocks; the caller dedupes.
        for (auto& segment : const_cast<segment_tree_t<column_segment_t>&>(data_).segments()) {
            if (segment.block && segment.block->is_reloadable()) {
                out.push_back(segment.block->block_id());
            }
            if (auto* state = segment.segment_state()) {
                for (uint64_t extra_id : state->additional_blocks()) {
                    out.push_back(extra_id);
                }
            }
        }
    }

    core::result_wrapper_t<bool> column_data_t::transition_to_disk(storage::partial_block_manager_t& pbm) {
        auto l = data_.lock();
        const uint64_t count = data_.segment_count(l);
        for (uint64_t i = 0; i < count; i++) {
            auto transitioned = transition_segment_to_disk(l, i, pbm);
            if (transitioned.has_error()) {
                return transitioned; // io_error / out_of_memory
            }
        }
        return true;
    }

    uint64_t column_data_t::scan_vector(column_scan_state& state,
                                        vector::vector_t& result,
                                        uint64_t remaining,
                                        scan_vector_type scan_type) {
        if (scan_type == scan_vector_type::SCAN_FLAT_VECTOR && result.get_vector_type() != vector::vector_type::FLAT) {
            // Unreachable today (callers only pass flat results); kept as a live guard, not a comment.
            state.scan_error = core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("column scan: a flat-vector scan was asked for a non-flat result", resource_));
            return 0;
        }
        state.previous_states.clear();
        if (!state.initialized) {
            if (!state.current) {
                if (!state.has_error()) {
                    state.scan_error =
                        core::error_t(core::error_code_t::invalid_parameter,
                                      std::pmr::string("column scan: no current segment to scan", resource_));
                }
                return 0;
            }
            state.current->initialize_scan(state);
            if (state.has_error()) {
                return 0;
            }
            state.internal_index = state.current->start;
            state.initialized = true;
        }
        assert(data_.has_segment(state.current));
        assert(state.internal_index <= state.row_index);
        if (state.internal_index < state.row_index) {
            state.current->skip(state);
        }
        assert(state.current->type == type_);
        uint64_t initial_remaining = remaining;
        while (remaining > 0) {
            assert(state.row_index >= state.current->start &&
                   state.row_index <= state.current->start + static_cast<int64_t>(state.current->count));
            uint64_t scan_count = std::min(remaining,
                                           static_cast<uint64_t>(state.current->start) + state.current->count -
                                               static_cast<uint64_t>(state.row_index));
            uint64_t result_offset = state.result_offset + initial_remaining - remaining;
            if (scan_count > 0) {
                // Not a per-row fetch_row loop: that defeats the handle cache `state` provides -- 400 578
                // pins to scan 200 000 rows across 195 segments.
                state.current->scan(state, scan_count, result, result_offset, scan_type);

                state.row_index += static_cast<int64_t>(scan_count);
                remaining -= scan_count;
            }

            if (remaining > 0) {
                auto next = data_.next_segment(state.current);
                if (!next) {
                    break;
                }
                state.previous_states.emplace_back(std::move(state.scan_state));
                state.current = next;
                state.current->initialize_scan(state);
                if (state.has_error()) {
                    state.internal_index = state.row_index;
                    return initial_remaining - remaining;
                }
                state.segment_checked = false;
                assert(state.row_index >= state.current->start &&
                       state.row_index <= state.current->start + static_cast<int64_t>(state.current->count));
            }
        }
        state.internal_index = state.row_index;
        return initial_remaining - remaining;
    }

    template<bool SCAN_COMMITTED, bool ALLOW_UPDATES>
    uint64_t column_data_t::scan_vector(uint64_t vector_index,
                                        column_scan_state& state,
                                        vector::vector_t& result,
                                        uint64_t target_scan) {
        auto scan_type = get_vector_scan_type(state, target_scan, result);
        auto scan_count = scan_vector(state, result, target_scan, scan_type);
        if (scan_type != scan_vector_type::SCAN_ENTIRE_VECTOR) {
            auto update_index = vector_index - static_cast<uint64_t>(start_) / vector::DEFAULT_VECTOR_CAPACITY;
            fetch_updates(state, update_index, result, state.result_offset, scan_count, ALLOW_UPDATES, SCAN_COMMITTED);
        }
        return scan_count;
    }

    void column_data_t::fetch_updates(column_scan_state& state,
                                      uint64_t vector_index,
                                      vector::vector_t& result,
                                      uint64_t result_offset,
                                      uint64_t scan_count,
                                      bool allow_updates,
                                      bool scan_committed) {
        if (!updates_) {
            return;
        }
        if (!allow_updates) {
            // A snapshot with no update overlay was requested, but this column carries one (caller: create_index_scan).
            state.scan_error =
                core::error_t(core::error_code_t::index_create_fail,
                              std::pmr::string("index build scan: the column has outstanding updates", resource_));
            return;
        }
        result.flatten(scan_count);
        if (scan_committed) {
            updates_->fetch_committed(vector_index, result_offset, result);
        } else {
            updates_->fetch_updates(vector_index, result_offset, result);
        }
    }

    void column_data_t::fetch_update_row(int64_t row_id, vector::vector_t& result, uint64_t result_idx) {
        if (!updates_) {
            return;
        }
        updates_->fetch_row(row_id, result, result_idx);
    }

    core::result_wrapper_t<bool> column_data_t::update_internal(uint64_t column_index,
                                                                vector::vector_t& update_vector,
                                                                int64_t* row_ids,
                                                                uint64_t update_count,
                                                                vector::vector_t& base_vector) {
        if (!updates_) {
            updates_ = std::make_unique<update_segment_t>(*this);
        }
        return updates_->update(column_index, update_vector, row_ids, update_count, base_vector);
    }

    uint64_t column_data_t::vector_count(uint64_t vector_index) const {
        uint64_t current_row = vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        return std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY,
                                  static_cast<uint64_t>(start_) + count_ - current_row);
    }

    core::result_wrapper_t<persistent_column_data_t>
    column_data_t::checkpoint(storage::partial_block_manager_t& partial_block_manager) {
        column_data_checkpointer_t checkpointer(*this, partial_block_manager);
        auto persistent = checkpointer.checkpoint();
        if (persistent.has_error()) {
            return persistent;
        }
        // Own entry count: nested nodes without own segments can't re-derive it on load.
        persistent.value().count = count_.load();
        // NVI hook (no-op for flat columns); without it the checkpoint silently dropped list/struct/array elements.
        auto children = checkpoint_children(partial_block_manager, persistent.value());
        if (children.has_error()) {
            return children.convert_error<persistent_column_data_t>();
        }
        // A separate, short-lived partial_block_manager re-points the live tail and flushes here (flush-before-evict).
        storage::partial_block_manager_t repoint_pbm(block_manager_);
        auto repointed = transition_to_disk(repoint_pbm);
        if (repointed.has_error()) {
            return repointed.convert_error<persistent_column_data_t>();
        }
        if (auto flushed = repoint_pbm.flush_partial_blocks(); flushed.has_error()) {
            return flushed.convert_error<persistent_column_data_t>(); // io_error
        }
        return persistent;
    }

    core::result_wrapper_t<bool>
    column_data_t::checkpoint_children(storage::partial_block_manager_t& /*partial_block_manager*/,
                                       persistent_column_data_t& /*persistent*/) {
        return true; // flat column: no child columns to persist
    }

    core::result_wrapper_t<bool> column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        auto l = data_.lock();
        for (uint32_t i = 0; i < persistent_data.data_pointers.size(); i++) {
            const auto& dp = persistent_data.data_pointers[i];
            if (dp.segment_size > block_manager_.block_size()) {
                return core::error_t(core::error_code_t::data_corruption,
                                     std::pmr::string("column load: segment_size exceeds the block size", resource_));
            }
            auto block_handle = block_manager_.register_block(dp.block_pointer.block_id);
            // Without persisted overflow blocks, a reloaded big-string marker can't resolve, aborting the process.
            std::unique_ptr<column_segment_state> overflow_state;
            if (!dp.overflow_blocks.empty()) {
                overflow_state = std::make_unique<column_segment_state>();
                overflow_state->blocks = dp.overflow_blocks;
            }
            auto segment = std::make_unique<column_segment_t>(block_handle,
                                                              type_,
                                                              static_cast<int64_t>(dp.row_start),
                                                              dp.tuple_count,
                                                              static_cast<uint32_t>(dp.block_pointer.block_id),
                                                              dp.block_pointer.offset,
                                                              dp.segment_size,
                                                              std::move(overflow_state));
            // The reload ctor's only failure (a corrupt overflow list) has no return channel; it latches here.
            if (segment->has_construction_error()) {
                return core::error_t(segment->construction_error());
            }
            segment->set_compression(dp.compression);
            if (i < persistent_data.segment_statistics.size() && persistent_data.segment_statistics[i].has_stats()) {
                segment->set_segment_statistics(persistent_data.segment_statistics[i]);
            }
            data_.append_segment(l, std::move(segment));
        }
        // The persisted count is authoritative: disagreement with the segment sum is data_corruption, not adopted.
        if (persistent_data.count == 0) {
            uint64_t total = 0;
            for (const auto& dp : persistent_data.data_pointers) {
                total += dp.tuple_count;
            }
            if (total != 0) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string("column load: the persisted row count is zero but the segments carry rows",
                                     resource_));
            }
        }
        count_ = persistent_data.count;
        if (persistent_data.statistics.has_stats()) {
            statistics_ = persistent_data.statistics;
        }
        return true;
    }

} // namespace components::table