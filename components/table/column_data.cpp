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
        std::atomic<uint64_t> g_segment_placements{0};
    } // namespace

    uint64_t transitions_with_live_pin() noexcept {
        return g_transitions_with_live_pin.load(std::memory_order_relaxed);
    }
    uint64_t segment_transitions() noexcept { return g_segment_transitions.load(std::memory_order_relaxed); }
    uint64_t segment_placements() noexcept { return g_segment_placements.load(std::memory_order_relaxed); }
    void reset_transitions_with_live_pin() noexcept {
        g_transitions_with_live_pin.store(0, std::memory_order_relaxed);
        g_segment_transitions.store(0, std::memory_order_relaxed);
        g_segment_placements.store(0, std::memory_order_relaxed);
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
        if (!data_.contiguous()) {
            std::fprintf(stderr,
                         "components::table::column_data_t::set_start: segment starts are not contiguous after "
                         "re-basing\n");
        }
    }

    const types::complex_logical_type& column_data_t::root_type() const {
        if (parent_) {
            return parent_->root_type();
        }
        return type_;
    }

    scan_vector_type
    column_data_t::get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result) {
        if (result.get_vector_type() != vector::vector_type::FLAT) {
            return scan_vector_type::SCAN_ENTIRE_VECTOR;
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
        return scan(state, result, target_count);
    }

    uint64_t column_data_t::scan_committed(uint64_t vector_index, column_scan_state& state, vector::vector_t& result) {
        auto target_count = vector_count(vector_index);
        return scan_committed(state, result, target_count);
    }

    uint64_t column_data_t::scan(column_scan_state& state, vector::vector_t& result, uint64_t scan_count) {
        auto scan_type = get_vector_scan_type(state, scan_count, result);
        return scan_vector(state, result, scan_count, scan_type);
    }

    uint64_t column_data_t::scan_committed(column_scan_state& state, vector::vector_t& result, uint64_t scan_count) {
        // The segments hold committed data only, so a committed scan reads exactly what scan() reads.
        return column_data_t::scan(state, result, scan_count);
    }

    // Deliberately no scan_committed_range: it read through a scan_state whose scan_error nobody checked.

    uint64_t column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        return scan_vector(state, result, count, scan_vector_type::SCAN_FLAT_VECTOR);
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
                                         uint64_t s_count) {
        scan_committed(vector_index, state, result);
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
                                              uint64_t count) {
        scan_committed(vector_index, state, result);
        result.slice(indexing, count);
    }

    void column_data_t::skip(column_scan_state& state, uint64_t count) { state.next(count); }

    core::result_wrapper_t<bool> column_data_t::initialize_append(column_append_state& state) {
        assert(state.pbm != nullptr && "an append state is built with the collection's packer");
        if (data_.is_empty()) {
            auto created = apend_transient_segment(start_);
            if (created.has_error()) {
                return created; // out_of_memory
            }
        }
        auto segment = data_.last_segment();
        // A disk-loaded segment is READ-ONLY (shared buffer); is_reloadable(), not block_offset()==0,
        // is the real test -- the checkpointer can pack the first column at offset 0 too.
        const bool is_disk_loaded = segment->block && segment->block->is_reloadable();
        if (is_disk_loaded || segment->block_offset() != 0) {
            auto created = apend_transient_segment(segment->start + static_cast<int64_t>(segment->count));
            if (created.has_error()) {
                return created; // out_of_memory
            }
            segment = data_.last_segment();
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
        // The collection's packer, shared by every column; the collection flushes it once per append.
        storage::partial_block_manager_t& pbm = *state.pbm;
        while (true) {
            auto appended = state.current->append(state, uvf, offset, append_count);
            if (appended.has_error()) {
                return appended.convert_error<bool>();
            }
            uint64_t copied_elements = appended.value();
            this->count_ += copied_elements;
            if (copied_elements == append_count) {
                break;
            }

            {
                // Capture the filled segment's index before appending the next one: state.current moves off it below.
                const uint64_t filled_index = data_.segment_count() - 1;
                // Release the pin before the swap frees its block_handle_t, or it unpins through
                // freed memory (see the [appendpin] test).
                state.handle.reset();
                auto created =
                    apend_transient_segment(state.current->start + static_cast<int64_t>(state.current->count));
                if (created.has_error()) {
                    return created; // out_of_memory
                }
                auto transitioned = transition_segment_to_disk(filled_index, pbm);
                if (transitioned.has_error()) {
                    return transitioned;
                }
                state.current = data_.last_segment();
                auto init = state.current->initialize_append(state);
                if (init.has_error()) {
                    return init;
                }
            }
            offset += copied_elements;
            append_count -= copied_elements;
        }
        return true;
    }

    void column_data_t::snapshot_counts(append_cut_t& cut) const { cut.counts.push_back(count_); }

    core::result_wrapper_t<bool> column_data_t::revert_append(cut_cursor_t& cut) {
        if (cut.exhausted()) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("column revert: the cut names fewer columns than the table holds", resource_));
        }
        const uint64_t kept = cut.take();
        if (kept > count_) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("column revert: the cut keeps more rows than the column holds", resource_));
        }
        const int64_t start_row = start_ + static_cast<int64_t>(kept);
        count_ = kept;
        auto last_segment = data_.last_segment();
        if (!last_segment) {
            return true;
        }
        if (start_row >= last_segment->start + static_cast<int64_t>(last_segment->count)) {
            assert(start_row == last_segment->start + static_cast<int64_t>(last_segment->count));
            return true;
        }
        uint64_t segment_index;
        if (!data_.try_segment_index(start_row, segment_index)) {
            // Names a row the tree doesn't bracket; truncating nearby would manufacture the desync revert undoes.
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("column revert: no segment brackets the revert row", resource_));
        }
        auto segment = data_.segment_at(static_cast<int64_t>(segment_index));
        if (segment->start == start_row) {
            data_.erase_segments(segment_index);
            return true;
        }
        data_.erase_segments(segment_index + 1);
        segment->revert_append(static_cast<uint64_t>(start_row));
        return true;
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
        switch (physical) {
            case types::physical_type::NA:
            case types::physical_type::UNKNOWN:
            case types::physical_type::INVALID:
            case types::physical_type::BIT:
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string("a table column cannot be built from a type without storage",
                                                      resource));
            default:
                break;
        }
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
        if (physical == types::physical_type::ARRAY) {
            const auto* array = type.extension_as<types::array_logical_type_extension>();
            if (array == nullptr || array->size() == 0) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string("a table column cannot be built from an array of no elements", resource));
            }
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

    core::result_wrapper_t<bool> column_data_t::apend_transient_segment(int64_t start_row) {
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
        data_.append_segment(std::move(new_segment.value()));
        return true;
    }

    // The disk twin of a transient segment is built and switched in once the packer has the twin's
    // block on the file. A transient that was unwound since (its block handle expired, and its column
    // may be gone with it), replaced or truncated keeps what it has: the twin would name rows the live
    // segment no longer holds.
    class column_data_t::repoint_t final : public storage::placement_t {
    public:
        repoint_t(column_data_t& column,
                  uint64_t index,
                  column_segment_t& transient,
                  uint64_t segment_size,
                  base_statistics_t stats,
                  bool has_stats,
                  std::vector<uint64_t> overflow_ids)
            : column_(column)
            , index_(index)
            , transient_(&transient)
            , transient_block_(transient.block)
            , start_(transient.start)
            , count_(transient.count.load())
            , segment_size_(segment_size)
            , stats_(std::move(stats))
            , has_stats_(has_stats)
            , overflow_ids_(std::move(overflow_ids)) {}

    private:
        bool alive_impl() const override { return !transient_block_.expired(); }

        bool adopt_impl(const storage::partial_block_allocation_t& at) override {
            if (transient_block_.expired()) {
                return false; // unwound with a refused append; nothing of the column is touched
            }
            auto* live = column_.data_.segment_at(static_cast<int64_t>(index_));
            if (live != transient_ || live->count.load() != count_) {
                return false; // replaced or truncated since the placement
            }
            auto block_handle = column_.block_manager_.register_block(at.block_id);
            // Adopted markers name real file blocks, kept alive by the reload constructor's registration
            // (test_string_write_through gate H).
            std::unique_ptr<column_segment_state> overflow_state;
            if (!overflow_ids_.empty()) {
                overflow_state = std::make_unique<column_segment_state>();
                overflow_state->blocks = std::move(overflow_ids_);
            }
            auto twin = std::make_unique<column_segment_t>(block_handle,
                                                           column_.type_,
                                                           start_,
                                                           count_,
                                                           static_cast<uint32_t>(at.block_id),
                                                           at.offset_in_block,
                                                           segment_size_,
                                                           std::move(overflow_state));
            // The reload constructor's one failure is a duplicate id in the overflow list, and these ids are
            // the packer's own: unreachable, and a transient left in place loses nothing.
            assert(!twin->has_construction_error());
            if (twin->has_construction_error()) {
                return false;
            }
            twin->set_compression(compression::compression_type::UNCOMPRESSED);
            if (has_stats_) {
                twin->set_segment_statistics(std::move(stats_));
            }
#ifdef DEV_MODE
            g_segment_transitions.fetch_add(1, std::memory_order_relaxed);
            if (live->block && live->block->readers() > 0) {
                g_transitions_with_live_pin.fetch_add(1, std::memory_order_relaxed);
            }
#endif
            column_.data_.replace_segment_at_index(index_, std::move(twin));
            return true;
        }

        column_data_t& column_;
        uint64_t index_;
        column_segment_t* transient_;
        std::weak_ptr<storage::block_handle_t> transient_block_;
        int64_t start_;
        uint64_t count_;
        uint64_t segment_size_;
        base_statistics_t stats_;
        bool has_stats_;
        std::vector<uint64_t> overflow_ids_;
    };

    core::result_wrapper_t<bool> column_data_t::transition_segment_to_disk(uint64_t segment_index,
                                                                           storage::partial_block_manager_t& pbm) {
        auto* segment = data_.segment_at(static_cast<int64_t>(segment_index));
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
#ifdef DEV_MODE
            g_segment_placements.fetch_add(1, std::memory_order_relaxed);
#endif
            auto placed = pbm.place(rewritten.data(),
                                    tight_size,
                                    std::make_unique<repoint_t>(*this,
                                                                segment_index,
                                                                *segment,
                                                                tight_size,
                                                                std::move(seg_stats),
                                                                has_stats,
                                                                std::move(overflow_ids)));
            if (placed.has_error()) {
                return placed.convert_error<bool>();
            }
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

        // The pin comes first, so a pin that fails (out_of_memory) has issued no block id.
        auto pinned = block_manager_.buffer_manager.pin(segment->block);
        if (pinned.has_error()) {
            return pinned.convert_error<bool>();
        }
#ifdef DEV_MODE
        g_segment_placements.fetch_add(1, std::memory_order_relaxed);
#endif
        auto placed = pbm.place(pinned.value().ptr() + block_offset,
                                segment_size,
                                std::make_unique<repoint_t>(*this,
                                                            segment_index,
                                                            *segment,
                                                            segment_size,
                                                            std::move(seg_stats),
                                                            has_stats,
                                                            std::vector<uint64_t>{}));
        if (placed.has_error()) {
            return placed.convert_error<bool>();
        }
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

    core::result_wrapper_t<bool> column_data_t::transition_own_segments(storage::partial_block_manager_t& pbm) {
        const uint64_t count = data_.segment_count();
        for (uint64_t i = 0; i < count; i++) {
            auto transitioned = transition_segment_to_disk(i, pbm);
            if (transitioned.has_error()) {
                return transitioned; // io_error / out_of_memory
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> column_data_t::transition_to_disk(storage::partial_block_manager_t& pbm) {
        auto own = transition_own_segments(pbm);
        if (own.has_error()) {
            return own;
        }
        return transition_children(pbm);
    }

    core::result_wrapper_t<bool> column_data_t::transition_children(storage::partial_block_manager_t& /*pbm*/) {
        return true; // flat column: no child columns to place
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
        // The live tail is re-pointed through the SAME packer as the root copies of every column and
        // row group of this checkpoint; its segments switch once collection_t::checkpoint flushed it.
        // Rejected: a packer per column, flushed here -- every live tail and its validity child's took
        // a dedicated 256 KiB block each round: 67 blocks and a 17.6 MB file for 100 rows x 32
        // INTEGER (test_checkpoint_blocks).
        auto repointed = transition_own_segments(partial_block_manager);
        if (repointed.has_error()) {
            return repointed.convert_error<persistent_column_data_t>();
        }
        return persistent;
    }

    core::result_wrapper_t<bool>
    column_data_t::checkpoint_children(storage::partial_block_manager_t& /*partial_block_manager*/,
                                       persistent_column_data_t& /*persistent*/) {
        return true; // flat column: no child columns to persist
    }

    core::result_wrapper_t<bool> column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
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
            data_.append_segment(std::move(segment));
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