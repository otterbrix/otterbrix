#include "data_table.hpp"

#include <algorithm>
#include <atomic>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/types/type_spec_codec.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <cstdlib>
#include <unordered_set>

#include "row_group.hpp"

namespace components::table {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_table_scan_rows_streamed{0};
    } // namespace
    uint64_t table_scan_rows_streamed() noexcept { return g_table_scan_rows_streamed.load(std::memory_order_relaxed); }
    void reset_table_scan_rows_streamed() noexcept { g_table_scan_rows_streamed.store(0, std::memory_order_relaxed); }
#endif

    namespace {
        // The only place turning a row_group_pointer_t into ids, shared by the checkpoint writer and load_from_disk.
        void collect_pointer_blocks(const storage::column_data_pointers_t& node, std::pmr::vector<uint64_t>& out) {
            for (const auto& segment : node.segments) {
                out.push_back(segment.block_pointer.block_id);
                for (uint64_t overflow : segment.overflow_blocks) {
                    out.push_back(overflow);
                }
            }
            for (const auto& child : node.children) {
                collect_pointer_blocks(child, out);
            }
        }

        void collect_root_blocks(const std::vector<storage::row_group_pointer_t>& row_groups,
                                 std::pmr::vector<uint64_t>& out) {
            for (const auto& rgp : row_groups) {
                for (const auto& column : rgp.data_pointers) {
                    collect_pointer_blocks(column, out);
                }
                // Delete bitmaps are part of the root too -- reclaiming one would drop committed deletes.
                for (const auto& deletes : rgp.deletes_pointers) {
                    out.push_back(deletes.block_pointer.block_id);
                    for (uint64_t overflow : deletes.overflow_blocks) {
                        out.push_back(overflow);
                    }
                }
            }
            std::sort(out.begin(), out.end());
            out.erase(std::unique(out.begin(), out.end()), out.end());
        }
    } // namespace

    data_table_t::data_table_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::vector<column_definition_t> column_definitions,
                               std::string name)
        : resource_(resource)
        , column_definitions_(std::move(column_definitions))
        , is_root_(true)
        , name_(std::move(name))
        , prepared_(resource) {
        // Plain `new`, never the pmr resource: `delete` is the matching deallocation (ref count lives inside).
        this->row_groups_ =
            boost::intrusive_ptr<collection_t>(new collection_t(resource_, block_manager, copy_types(), 0));
    }

    data_table_t::data_table_t(data_table_t& parent, column_definition_t& new_column)
        : resource_(parent.resource_)
        , is_root_(true)
        , compact_epoch_(parent.compact_epoch_)
        , prepared_(parent.prepared_, parent.resource_) {
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def);
        }
        column_definitions_.emplace_back(new_column);

        auto extended = parent.row_groups_->add_column(new_column);
        if (extended.has_error()) {
            // A constructor can't return, so the backfill refusal latches: the parent stays root and
            // shares its collection read-only.
            construction_error_ = extended.error();
            column_definitions_.pop_back();
            this->row_groups_ = parent.row_groups_;
            is_root_ = false;
            return;
        }
        this->row_groups_ = std::move(extended.value());

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent, uint64_t removed_column)
        : resource_(parent.resource_)
        , is_root_(true)
        , compact_epoch_(parent.compact_epoch_)
        , prepared_(parent.prepared_, parent.resource_) {
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def);
        }

        assert(removed_column < column_definitions_.size());
        column_definitions_.erase(column_definitions_.begin() + static_cast<int64_t>(removed_column));

        uint64_t storage_idx = 0;
        for (uint64_t i = 0; i < column_definitions_.size(); i++) {
            auto& col = column_definitions_[i];
            col.set_oid(i);
            col.set_storage_oid(storage_idx++);
        }

        this->row_groups_ = parent.row_groups_->remove_column(removed_column);

        parent.is_root_ = false;
    }

    [[nodiscard]] std::pmr::vector<types::complex_logical_type> data_table_t::copy_types() const {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(column_definitions_.size());
        for (auto& it : column_definitions_) {
            types.push_back(it.type());
        }
        return types;
    }

    const std::vector<column_definition_t>& data_table_t::columns() const { return column_definitions_; }

    std::pmr::vector<std::uint32_t> data_table_t::column_attoids() const {
        std::pmr::vector<std::uint32_t> attoids(resource_);
        attoids.reserve(column_definitions_.size());
        for (const auto& column : column_definitions_) {
            attoids.push_back(column.attoid());
        }
        return attoids;
    }

    std::vector<uint64_t> data_table_t::visible_columns(const transaction_data& txn) const {
        std::vector<uint64_t> positions;
        positions.reserve(column_definitions_.size());
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            if (column_definitions_[position].visible_to(txn)) {
                positions.push_back(position);
            }
        }
        return positions;
    }

    void data_table_t::stamp_column_identity(uint64_t position, std::uint32_t attoid) {
        assert(position < column_definitions_.size() && "stamp_column_identity: no such column");
        column_definitions_[position].set_attoid(attoid);
        mark_modified();
    }

    void data_table_t::stamp_column_added(uint64_t position, uint64_t stamp) {
        assert(position < column_definitions_.size() && "stamp_column_added: no such column");
        column_definitions_[position].set_added_at(stamp);
        mark_modified();
    }

    void data_table_t::stamp_column_dropped(uint64_t position, uint64_t stamp) {
        assert(position < column_definitions_.size() && "stamp_column_dropped: no such column");
        column_definitions_[position].set_dropped_at(stamp);
        mark_modified();
    }

    uint64_t data_table_t::publish_column_stamps(uint64_t txn_id, uint64_t commit_id) {
        if (txn_id == 0) {
            return 0;
        }
        uint64_t published = 0;
        for (auto& column : column_definitions_) {
            if (column.added_at() == txn_id) {
                column.set_added_at(commit_id);
                ++published;
            }
            if (column.dropped_at() == txn_id) {
                column.set_dropped_at(commit_id);
                // name is free to be used by other columns
                // and empty name is not addressable in query
                column.set_name(std::string{});
                ++published;
            }
        }
        if (published != 0) {
            mark_modified();
        }
        return published;
    }

    uint64_t data_table_t::revert_column_stamps(uint64_t txn_id) {
        if (txn_id < TRANSACTION_ID_START) {
            return 0;
        }
        uint64_t reverted = 0;
        for (auto& column : column_definitions_) {
            if (column.added_at() == txn_id) {
                column.set_added_at(ABORTED_ID);
                ++reverted;
            }
            if (column.dropped_at() == txn_id) {
                column.set_dropped_at(NOT_DELETED_ID);
                ++reverted;
            }
        }
        if (reverted != 0) {
            mark_modified();
        }
        return reverted;
    }

    write_decision_t data_table_t::decide_claim(const transaction_data& txn, uint64_t stamp) noexcept {
        if (stamp == NOT_DELETED_ID) {
            return write_decision_t::proceed;
        }
        if (txn.transaction_id != 0 && stamp == txn.transaction_id) {
            return write_decision_t::skip;
        }
        return write_decision_t::conflict;
    }

    bool data_table_t::stands_at_commit(const transaction_data& txn, uint64_t stamp) const noexcept {
        if (stamp == NOT_DELETED_ID || stamp == ABORTED_ID) {
            return false;
        }
        if (stamp < TRANSACTION_ID_START) {
            return true;
        }
        return stamp == txn.transaction_id || std::find(prepared_.begin(), prepared_.end(), stamp) != prepared_.end();
    }

    // TODO: NOT NULL should be handled as any other constraint
    core::error_t data_table_t::refuse_nulls_at_commit(const transaction_data& txn,
                                                       const std::pmr::vector<storage_index_t>& columns,
                                                       std::span<const row_range_t> ranges) {
        constexpr uint64_t max_batch_rows = vector::DEFAULT_VECTOR_CAPACITY;
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(columns.size());
        for (const auto& column : columns) {
            types.push_back(column_definitions_[column.primary_index()].type());
        }
        vector::data_chunk_t cells(resource_, types, max_batch_rows);
        vector::vector_t row_ids(resource_, types::complex_logical_type(types::logical_type::BIGINT), max_batch_rows);
        std::pmr::vector<uint64_t> inserted(max_batch_rows, resource_);
        std::pmr::vector<uint64_t> deleted(max_batch_rows, resource_);
        column_fetch_state state;

        for (const auto& range : ranges) {
            for (uint64_t done = 0; done < range.count; done += max_batch_rows) {
                uint64_t batch = std::min(max_batch_rows, range.count - done);
                int64_t first_row = range.row_start + static_cast<int64_t>(done);
                row_groups_->fill_stamps(first_row, batch, inserted.data(), deleted.data());
                auto* live_rows = row_ids.data<int64_t>();
                uint64_t live = 0;
                for (uint64_t offset = 0; offset < batch; offset++) {
                    if (stands_at_commit(txn, inserted[offset]) && !stands_at_commit(txn, deleted[offset])) {
                        live_rows[live++] = first_row + static_cast<int64_t>(offset);
                    }
                }
                if (live == 0) {
                    continue;
                }
                cells.reset();
                row_groups_->fetch(cells, columns, row_ids, live, state, {}, txn, fetch_visibility_t::RAW);
                if (state.fetch_error.contains_error()) {
                    return state.fetch_error;
                }
                for (uint64_t column = 0; column < columns.size(); column++) {
                    const auto& cells_of_column = cells.data[column];
                    for (uint64_t cell = 0; cell < cells.size(); cell++) {
                        if (!cells_of_column.is_null(cell)) {
                            continue;
                        }
                        std::pmr::string what{"commit refused: column '", resource_};
                        what +=
                            std::pmr::string{column_definitions_[columns[column].primary_index()].name(), resource_};
                        what += std::pmr::string{"' of table '", resource_};
                        what += std::pmr::string{name_, resource_};
                        what += std::pmr::string{"' is NOT NULL without a default, and a row that would be live "
                                                 "after this commit holds NULL in it",
                                                 resource_};
                        return core::error_t{core::error_code_t::invalid_constraint, std::move(what)};
                    }
                }
            }
        }
        return core::error_t::no_error();
    }

    core::error_t data_table_t::prepare(const transaction_data& txn, const std::pmr::vector<row_range_t>& own_appends) {
        assert(txn.transaction_id >= TRANSACTION_ID_START && "data_table_t::prepare: not a transaction");
        // Empty unless a rule applies, which most commits never meet
        std::pmr::vector<storage_index_t> added_columns(resource_);
        std::pmr::vector<storage_index_t> unseen_columns(resource_);
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            const auto& column = column_definitions_[position];
            bool fills_with_a_value = column.has_default_value() && !column.default_value().is_null();
            if (!column.is_not_null() || fills_with_a_value || !stands_at_commit(txn, column.added_at()) ||
                stands_at_commit(txn, column.dropped_at())) {
                continue;
            }
            if (column.added_at() == txn.transaction_id) {
                added_columns.emplace_back(position);
            } else if (!column.visible_to(txn)) {
                unseen_columns.emplace_back(position);
            }
        }
        if (!added_columns.empty()) {
            row_range_t every_row{0, row_groups_->total_rows()};
            if (auto refused = refuse_nulls_at_commit(txn, added_columns, {&every_row, 1}); refused.contains_error()) {
                return refused;
            }
        }
        if (!unseen_columns.empty() && !own_appends.empty()) {
            if (auto refused = refuse_nulls_at_commit(txn, unseen_columns, own_appends); refused.contains_error()) {
                return refused;
            }
        }
        if (std::find(prepared_.begin(), prepared_.end(), txn.transaction_id) == prepared_.end()) {
            prepared_.push_back(txn.transaction_id);
        }
        return core::error_t::no_error();
    }

    void data_table_t::release_prepared(uint64_t txn_id) {
        prepared_.erase(std::remove(prepared_.begin(), prepared_.end(), txn_id), prepared_.end());
    }

    vector::vector_t data_table_t::default_column(uint64_t position, const vector::data_chunk_t& chunk) const {
        const auto& column = column_definitions_[position];
        if (column.has_default_value() && !column.default_value().is_null()) {
            vector::vector_t filled(resource_, column.default_value(), chunk.capacity());
            filled.flatten(chunk.size());
            filled.set_type_alias(column.name());
            return filled;
        }
        auto type = column.type();
        type.set_alias(column.name());
        vector::vector_t filled(resource_, type, chunk.capacity());
        filled.validity().set_all_invalid(chunk.size());
        return filled;
    }

    core::error_t data_table_t::take_payload_column(uint64_t position,
                                                    vector::vector_t& payload,
                                                    std::vector<vector::vector_t>& physical) const {
        const auto& column = column_definitions_[position];
        if (payload.type() != column.type()) {
            std::pmr::string what{"write to table '", resource_};
            what += std::pmr::string{name_, resource_};
            what += std::pmr::string{"': column '", resource_};
            what += std::pmr::string{column.name(), resource_};
            what += std::pmr::string{"' stores a type other than the payload carries; nothing was written", resource_};
            return core::error_t{core::error_code_t::schema_error, std::move(what)};
        }
        payload.set_type_alias(column.name());
        physical.push_back(std::move(payload));
        return core::error_t::no_error();
    }

    core::error_t data_table_t::widen_insert(const transaction_data& txn, vector::data_chunk_t& chunk) const {
        const auto visible = visible_columns(txn);
        assert(chunk.column_count() == visible.size() && "widen_insert: payload width is validated");
        std::vector<vector::vector_t> physical;
        physical.reserve(column_definitions_.size());
        uint64_t next_visible = 0;
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            if (next_visible < visible.size() && visible[next_visible] == position) {
                if (auto taken = take_payload_column(position, chunk.data[next_visible], physical);
                    taken.contains_error()) {
                    return taken;
                }
                next_visible++;
            } else {
                physical.push_back(default_column(position, chunk));
            }
        }
        chunk.data = std::move(physical);
        return core::error_t::no_error();
    }

    core::error_t data_table_t::widen_update(const transaction_data& txn,
                                             const vector::vector_t& row_ids,
                                             vector::data_chunk_t& chunk) {
        const auto visible = visible_columns(txn);
        assert(chunk.column_count() == visible.size() && "widen_update: payload width is validated");

        // Read before the delete stamps the rows
        std::pmr::vector<storage_index_t> hidden_ids(resource_);
        std::pmr::vector<types::complex_logical_type> hidden_types(resource_);
        uint64_t next_visible = 0;
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            if (next_visible < visible.size() && visible[next_visible] == position) {
                next_visible++;
                continue;
            }
            hidden_ids.emplace_back(static_cast<int64_t>(position));
            hidden_types.push_back(column_definitions_[position].type());
        }
        vector::data_chunk_t old_values(resource_, hidden_types, chunk.capacity());
        if (!hidden_ids.empty()) {
            column_fetch_state state;
            state.result_outlives_pins = true;
            row_groups_->fetch(old_values, hidden_ids, row_ids, chunk.size(), state, {}, txn, fetch_visibility_t::RAW);
            if (state.fetch_error.contains_error()) {
                return state.fetch_error;
            }
            if (old_values.size() != chunk.size()) {
                std::pmr::string what{"update of table '", resource_};
                what += std::pmr::string{name_, resource_};
                what += std::pmr::string{"': a row id names no row of it; nothing was written", resource_};
                return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
            }
        }

        std::vector<vector::vector_t> physical;
        physical.reserve(column_definitions_.size());
        next_visible = 0;
        uint64_t next_hidden = 0;
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            if (next_visible < visible.size() && visible[next_visible] == position) {
                if (auto taken = take_payload_column(position, chunk.data[next_visible], physical);
                    taken.contains_error()) {
                    return taken;
                }
                next_visible++;
            } else {
                auto& kept = old_values.data[next_hidden++];
                kept.set_type_alias(column_definitions_[position].name());
                physical.push_back(std::move(kept));
            }
        }
        chunk.data = std::move(physical);
        return core::error_t::no_error();
    }

    core::error_t data_table_t::widen_by_name(const transaction_data& txn, vector::data_chunk_t& chunk) const {
        std::pmr::vector<bool> placed(chunk.column_count(), false, resource_);
        std::vector<vector::vector_t> physical;
        physical.reserve(column_definitions_.size());
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            const auto& column = column_definitions_[position];
            uint64_t source = chunk.column_count();
            if (column.visible_to(txn)) {
                for (uint64_t candidate = 0; candidate < chunk.column_count(); candidate++) {
                    const auto& type = chunk.data[candidate].type();
                    if (!placed[candidate] && type.has_alias() && type.alias() == column.name() &&
                        type.type() == column.type().type()) {
                        source = candidate;
                        break;
                    }
                }
            }
            if (source == chunk.column_count()) {
                physical.push_back(default_column(position, chunk));
                continue;
            }
            placed[source] = true;
            if (auto taken = take_payload_column(position, chunk.data[source], physical); taken.contains_error()) {
                return taken;
            }
        }
        for (uint64_t candidate = 0; candidate < chunk.column_count(); candidate++) {
            if (!placed[candidate]) {
                std::pmr::string what{"write to table '", resource_};
                what += std::pmr::string{name_, resource_};
                what += std::pmr::string{"': the payload names a column the writer does not see", resource_};
                return core::error_t{core::error_code_t::schema_error, std::move(what)};
            }
        }
        chunk.data = std::move(physical);
        return core::error_t::no_error();
    }

    core::error_t data_table_t::widen_by_attoid(std::span<const std::uint32_t> attoids,
                                                vector::data_chunk_t& chunk) const {
        assert(attoids.size() == chunk.column_count() && "widen_by_attoid: every payload column names its attoid");
        std::vector<vector::vector_t> physical;
        physical.reserve(column_definitions_.size());
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            const auto attoid = column_definitions_[position].attoid();
            assert(attoid != 0 && "widen_by_attoid: a regular table's column always carries its attoid");
            const auto source = std::find(attoids.begin(), attoids.end(), attoid);
            if (source == attoids.end()) {
                physical.push_back(default_column(position, chunk));
                continue;
            }
            const auto index = static_cast<uint64_t>(source - attoids.begin());
            if (auto taken = take_payload_column(position, chunk.data[index], physical); taken.contains_error()) {
                return taken;
            }
        }
        chunk.data = std::move(physical);
        return core::error_t::no_error();
    }

    uint64_t data_table_t::find_visible_column(const transaction_data& txn, std::uint32_t attoid) const {
        if (attoid == 0) {
            return storage::INVALID_INDEX;
        }
        for (uint64_t position = 0; position < column_definitions_.size(); position++) {
            const auto& column = column_definitions_[position];
            if (column.attoid() == attoid && column.visible_to(txn)) {
                return position;
            }
        }
        return storage::INVALID_INDEX;
    }

    std::pmr::vector<types::complex_logical_type> data_table_t::visible_types(const transaction_data& txn) const {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(column_definitions_.size());
        for (const auto& column : column_definitions_) {
            if (column.visible_to(txn)) {
                types.push_back(column.type());
            }
        }
        return types;
    }

    // Adopted columns carry no pg_attribute.attoid: this only runs on a schema-less table (relkind='g').
    void data_table_t::adopt_schema(const std::pmr::vector<types::complex_logical_type>& types) {
        assert(column_definitions_.empty() && "adopt_schema can only be called on schema-less table");
        column_definitions_.reserve(types.size());
        for (const auto& type : types) {
            column_definitions_.emplace_back(type.alias(), type);
        }
        row_groups_->adopt_types(std::pmr::vector<types::complex_logical_type>(types, resource_));
        mark_modified();
    }

    std::pmr::vector<storage_index_t> data_table_t::to_physical_columns(const std::vector<storage_index_t>& column_ids,
                                                                        const std::vector<uint64_t>& visible) const {
        std::pmr::vector<storage_index_t> physical_ids(resource_);
        physical_ids.reserve(column_ids.size());
        for (const auto& id : column_ids) {
            storage_index_t physical = id;
            if (!id.is_row_id_column()) {
                const auto index = id.primary_index();
                assert(index < visible.size() && "to_physical_columns: the id names a column this transaction "
                                                 "cannot see");
                physical.set_index(visible[index]);
            }
            physical_ids.push_back(std::move(physical));
        }
        return physical_ids;
    }

    void data_table_t::initialize_scan(table_scan_state& state,
                                       const std::vector<storage_index_t>& column_ids,
                                       transaction_data txn,
                                       const table_filter_t* filter) {
        state.set_visible_to_physical(visible_columns(txn));
        state.initialize(column_ids, filter);
        state.table_state.txn = txn;
        state.local_state.txn = txn;
        row_groups_->initialize_scan(state.table_state, column_ids);
    }

    void data_table_t::initialize_scan_with_offset(table_scan_state& state,
                                                   const std::vector<storage_index_t>& column_ids,
                                                   transaction_data txn,
                                                   int64_t start_row,
                                                   int64_t end_row) {
        state.set_visible_to_physical(visible_columns(txn));
        state.initialize(column_ids);
        state.table_state.txn = txn;
        state.local_state.txn = txn;
        row_groups_->initialize_scan_with_offset(state.table_state, column_ids, start_row, end_row);
    }

    uint64_t data_table_t::row_group_size() const { return row_groups_->row_group_size(); }

    // A counted copy, so it (and its registered block handles) stays alive after compact() swaps a new one in.
    boost::intrusive_ptr<collection_t> data_table_t::row_group() const { return row_groups_; }

    void data_table_t::collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out) const {
        row_groups_->collect_column_disk_block_ids(column_index, out);
    }

    uint64_t data_table_t::calculate_size() { return row_groups_->calculate_size(); }

    void data_table_t::cleanup_versions(uint64_t lowest_active_start_time) {
        row_groups_->cleanup_versions(lowest_active_start_time);
    }

    bool data_table_t::compact(uint64_t compact_watermark) {
        // Compacting a superseded ALTER parent would free blocks its successor references (proven unreachable).
        assert(is_root_);
        auto total = row_groups_->total_rows();
        if (total == 0) {
            return true;
        }

        // A degraded block manager must not be rebuilt on top of: its latches never let write_header
        // promote pending_free_, so this would extend the file by a fresh copy every round forever.
        // Measured: +19 blocks per round on a 12k-row table after a single failed fsync.
        if (row_groups_->block_manager().degraded()) {
            return false;
        }

        // MVCC safety gate: the rebuild collapses version history, safe only below the caller's watermark.
        if (row_groups_->has_version_above(compact_watermark)) {
            return false;
        }

        // By reference: `auto` would copy this vector onto the default resource (its allocator doesn't propagate).
        const auto& types = row_groups_->types();
        auto new_collection = boost::intrusive_ptr<collection_t>(
            new collection_t(resource_,
                             row_groups_->block_manager(),
                             std::pmr::vector<types::complex_logical_type>(types, resource_),
                             0));

        {
            table_append_state append_state(resource_);
            // compact is best-effort: an out_of_memory here leaves the original collection untouched.
            if (new_collection->initialize_append(append_state).has_error()) {
                return false;
            }

            std::vector<storage_index_t> column_ids;
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                column_ids.emplace_back(i);
            }

            table_scan_state state(resource_);
            state.initialize(column_ids);
            state.table_state.txn = transaction_data::committed();
            state.local_state.txn = transaction_data::committed();
            row_groups_->initialize_scan_with_offset(state.table_state, column_ids, 0, static_cast<int64_t>(total));

            auto scan_types = copy_types();
            vector::data_chunk_t chunk(resource_, scan_types, vector::DEFAULT_VECTOR_CAPACITY);
            while (true) {
                state.table_state.scan(chunk);
                // A scan failure must not look like end-of-table: that would swap in a truncated collection.
                if (state.table_state.has_error()) {
                    return false;
                }
                if (chunk.size() == 0) {
                    break;
                }
                if (new_collection->append(chunk, append_state).has_error()) {
                    return false;
                }
                chunk.reset();
            }

            new_collection->finalize_append(append_state, transaction_data::committed());
        }

        auto old_collection = row_groups_;

        row_groups_ = std::move(new_collection);
        // Fresh blocks now, released outgoing ones -- compact must be followed by a checkpoint (its only caller).
        mark_modified();

        if (old_collection) {
            std::pmr::vector<uint64_t> reclaimable{resource_};
            old_collection->collect_disk_block_ids(reclaimable);
            release_disk_blocks(old_collection->block_manager(), std::move(reclaimable));
        }
        // The swap may have renumbered row ids; every index answer stamped with the old epoch is refused from here on.
        ++compact_epoch_;
        return true;
    }

    void data_table_t::scan(vector::data_chunk_t& result, table_scan_state& state) {
        state.table_state.scan(result);
#ifdef DEV_MODE
        g_table_scan_rows_streamed.fetch_add(result.size(), std::memory_order_relaxed);
#endif
    }

    void data_table_t::scan_batched(const std::pmr::vector<types::complex_logical_type>& types,
                                    const std::vector<size_t>* projected_cols,
                                    std::pmr::vector<vector::data_chunk_t>& batches,
                                    table_scan_state& state,
                                    std::pmr::memory_resource* resource) {
        state.table_state.scan_batched(types, projected_cols, batches, resource);
    }

    core::result_wrapper_t<bool> data_table_t::fetch_next_batch(vector::data_chunk_t& result,
                                                                const std::vector<storage_index_t>& column_ids,
                                                                const table_filter_t* filter,
                                                                transaction_data txn,
                                                                int64_t& next_row,
                                                                int64_t max_row,
                                                                bool& drained) {
        if (drained || next_row >= max_row) {
            drained = true;
            return true;
        }
        // Walks past empty/all-deleted vectors (the caller treats an empty batch as end-of-scan).
        while (next_row < max_row) {
            // Transient per-batch scan state: released when `state` destructs, so nothing pinned crosses the mailbox.
            table_scan_state state(resource_);
            initialize_scan_with_offset(state, column_ids, txn, next_row, max_row);
            state.filter = filter;
            auto& css = state.table_state;

            // Capture the seeked group's absolute end before the read, so the advance stays within its bounds.
            const row_group_t* seeked_group = css.row_group;
            const int64_t group_end =
                seeked_group != nullptr
                    ? std::min(seeked_group->start + static_cast<int64_t>(seeked_group->count.load()), max_row)
                    : max_row;

            const bool produced = css.next_batch(result);
            if (css.has_error()) {
                return css.scan_error;
            }

            // css.vector_index*CAP accounts for empty vectors skipped within the group, not a blind step.
            const int64_t prev_row = next_row;
            const int64_t scanned_to = static_cast<int64_t>(css.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            next_row = std::min({scanned_to, group_end, max_row});

            if (seeked_group == nullptr || next_row <= prev_row) {
                drained = true;
                return true;
            }
            if (produced) {
                if (next_row >= max_row) {
                    drained = true;
                }
                return true;
            }
        }
        drained = true;
        return true;
    }

    bool data_table_t::create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        return state.table_state.scan_committed(result, type);
    }

    std::string data_table_t::table_name() const { return name_; }

    void data_table_t::set_table_name(std::string new_name) {
        name_ = std::move(new_name);
        mark_modified();
    }

    core::result_wrapper_t<bool> data_table_t::rename_column(const std::string& old_name, const std::string& new_name) {
        // Collision check first, over the whole list, so a refusal changes nothing.
        uint64_t idx = column_definitions_.size();
        for (uint64_t i = 0; i < column_definitions_.size(); ++i) {
            // Dropped column is kept, in case of a rollback, but name is free to use
            if (column_definitions_[i].dropped_at() != NOT_DELETED_ID) {
                continue;
            }
            const auto& col_name = column_definitions_[i].name();
            if (col_name == new_name) {
                std::pmr::string msg{"data_table_t::rename_column: table '", resource_};
                msg += std::pmr::string{name_, resource_};
                msg += std::pmr::string{"' already has a column named '", resource_};
                msg += std::pmr::string{new_name, resource_};
                msg += std::pmr::string{"'", resource_};
                return core::error_t{core::error_code_t::schema_error, std::move(msg)};
            }
            if (col_name == old_name) {
                idx = i;
            }
        }
        if (idx == column_definitions_.size()) {
            return false; // not a column of this storage — see the contract on the declaration
        }
        column_definitions_[idx].set_name(new_name);
        mark_modified();
        return true;
    }

    void data_table_t::fetch(vector::data_chunk_t& result,
                             const std::vector<storage_index_t>& column_ids,
                             const vector::vector_t& row_identifiers,
                             uint64_t fetch_count,
                             column_fetch_state& state,
                             const std::vector<size_t>& projected_cols,
                             const transaction_data& txn,
                             fetch_visibility_t visibility) {
        const auto physical_ids = to_physical_columns(column_ids, visible_columns(txn));
        row_groups_->fetch(result, physical_ids, row_identifiers, fetch_count, state, projected_cols, txn, visibility);
    }

    std::unique_ptr<constraint_state> data_table_t::initialize_constraint_state(
        const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        return std::make_unique<constraint_state>(bound_constraints);
    }

    core::result_wrapper_t<bool> data_table_t::append_lock(table_append_state& state) {
        state.append_locked = true;
        // write_conflict, not a throw: under -fno-exceptions an actor-zeta coroutine throw is swallowed silently.
        if (!is_root_) {
            return core::error_t(core::error_code_t::write_conflict,
                                 std::pmr::string("Transaction conflict: adding entries to a table that has "
                                                  "been altered!",
                                                  resource_));
        }
        state.row_start = static_cast<int64_t>(row_groups_->total_rows());
        state.current_row = state.row_start;
        return true;
    }

    core::result_wrapper_t<bool> data_table_t::initialize_append(table_append_state& state) {
        assert(state.append_locked &&
               "data_table_t::append_lock should be called before data_table_t::initialize_append");
        if (!state.append_locked) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("data_table_t::append_lock must precede initialize_append", resource_));
        }
        mark_modified();
        return row_groups_->initialize_append(state); // out_of_memory
    }

    core::result_wrapper_t<bool> data_table_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
        assert(is_root_);
        mark_modified();
        return row_groups_->append(chunk, state); // out_of_memory
    }

    void data_table_t::finalize_append(table_append_state& state, transaction_data txn) {
        row_groups_->finalize_append(state, txn);
        mark_modified();
    }

    void data_table_t::commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) {
        row_groups_->commit_append(commit_id, row_start, count);
        mark_modified();
    }

    core::result_wrapper_t<bool> data_table_t::revert_append(int64_t row_start, uint64_t count) {
        if (row_groups_->is_tail(row_start, count)) {
            auto reverted = row_groups_->revert_append(row_start, count);
            mark_modified();
            return reverted;
        }
        // Not the tail: truncating would take the rows behind it down too.
        row_groups_->abort_append(row_start, count);
        mark_modified();
        return true;
    }

    void data_table_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        row_groups_->commit_all_deletes(txn_id, commit_id);
        mark_modified();
    }

    void data_table_t::revert_all_deletes(uint64_t txn_id) {
        row_groups_->revert_all_deletes(txn_id);
        mark_modified();
    }

    void data_table_t::merge_storage(collection_t& data) {
        row_groups_->merge_storage(data);
        mark_modified();
    }

    std::unique_ptr<table_delete_state>
    data_table_t::initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        auto result = std::make_unique<table_delete_state>(resource_);
        if (result->has_delete_constraints) {
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                result->col_ids.emplace_back(column_definitions_[i].storage_oid());
                types.emplace_back(column_definitions_[i].type());
            }
            result->constraint = std::make_unique<constraint_state>(bound_constraints);
        }
        return result;
    }

    core::result_wrapper_t<uint64_t> data_table_t::delete_rows(table_delete_state&,
                                                               vector::vector_t& row_identifiers,
                                                               uint64_t count,
                                                               const transaction_data& txn) {
        assert(row_identifiers.type().type() == types::logical_type::BIGINT);
        if (count == 0) {
            return core::result_wrapper_t<uint64_t>{uint64_t{0}};
        }

        row_identifiers.flatten(count);
        const auto* requested = row_identifiers.data<int64_t>();

        std::pmr::vector<int64_t> claimed(resource_);
        claimed.reserve(count);
        for (uint64_t i = 0; i < count; i++) {
            switch (decide_claim(txn, row_groups_->delete_stamp(requested[i]))) {
                case write_decision_t::proceed:
                    claimed.push_back(requested[i]);
                    break;
                case write_decision_t::skip:
                    break;
                case write_decision_t::conflict: {
                    std::pmr::string what{"could not delete row ", resource_};
                    what += std::pmr::string{std::to_string(requested[i]), resource_};
                    what += std::pmr::string{" of table '", resource_};
                    what += std::pmr::string{name_, resource_};
                    what += std::pmr::string{"': it already carries a delete stamp that is not this transaction's",
                                             resource_};
                    return core::error_t{core::error_code_t::write_conflict, std::move(what)};
                }
            }
        }
        if (claimed.empty()) {
            return core::result_wrapper_t<uint64_t>{uint64_t{0}};
        }

        mark_modified();
        auto* ids = claimed.data();
        count = claimed.size();

        uint64_t pos = 0;
        uint64_t delete_count = 0;
        while (pos < count) {
            uint64_t start = pos;
            bool is_transaction_delete = static_cast<uint64_t>(ids[pos]) >= MAX_ROW_ID;
            for (pos++; pos < count; pos++) {
                bool row_is_transaction_delete = static_cast<uint64_t>(ids[pos]) >= MAX_ROW_ID;
                if (row_is_transaction_delete != is_transaction_delete) {
                    break;
                }
            }
            uint64_t current_offset = start;
            uint64_t current_count = pos - start;

            vector::vector_t offset_ids(row_identifiers, current_offset, pos);
            VALUE_OR_RETURN(auto deleted,
                            row_groups_->delete_rows(*this, ids + current_offset, current_count, txn.transaction_id));
            delete_count += deleted;
        }
        return core::result_wrapper_t<uint64_t>{delete_count};
    }

    uint64_t data_table_t::column_count() const { return column_definitions_.size(); }

    std::vector<column_segment_info> data_table_t::get_column_segment_info() {
        return row_groups_->get_column_segment_info();
    }

    core::result_wrapper_t<bool> data_table_t::checkpoint(storage::metadata_writer_t& writer) {
        storage::partial_block_manager_t partial_block_manager(row_groups_->block_manager());

        auto row_group_pointers_res = row_groups_->checkpoint(partial_block_manager);
        if (row_group_pointers_res.has_error()) {
            return row_group_pointers_res.convert_error<bool>(); // out_of_memory
        }
        const auto& row_group_pointers = row_group_pointers_res.value();

        writer.write_string(name_);

        // Full type spec, not a bare logical_type byte: a byte tag loses DECIMAL width/scale (UB-adjacent).
        writer.write<uint32_t>(static_cast<uint32_t>(column_definitions_.size()));
        std::pmr::vector<std::byte> type_spec(resource_);
        for (const auto& col : column_definitions_) {
            writer.write_string(col.name());
            type_spec.clear();
            auto encoded = types::encode_type_spec(col.type(), type_spec);
            if (encoded.has_error()) {
                return encoded; // schema_error: this column type cannot be persisted
            }
            writer.write<uint32_t>(static_cast<uint32_t>(type_spec.size()));
            writer.write_data(type_spec.data(), type_spec.size());
            writer.write<uint8_t>(col.is_not_null() ? 1 : 0);
            // Identity (attoid), not name: a rename can race the catalog ahead of the next checkpoint.
            writer.write<uint32_t>(col.attoid());
            writer.write<uint64_t>(col.added_at());
            writer.write<uint64_t>(col.dropped_at());
        }

        writer.write<uint32_t>(static_cast<uint32_t>(row_group_pointers.size()));
        for (const auto& rgp : row_group_pointers) {
            rgp.serialize(writer);
        }

        if (auto flush_r = writer.flush(); flush_r.has_error()) {
            return flush_r;
        }

        // Earliest AND latest point to reclaim the superseded root: ids go to pending_free_, not reusable_.
        std::pmr::vector<uint64_t> new_root_blocks(resource_);
        collect_root_blocks(row_group_pointers, new_root_blocks);
        auto reclaimed = row_groups_->block_manager().reclaim_superseded_root(new_root_blocks);
        if (reclaimed.has_error()) {
            return reclaimed.convert_error<bool>();
        }
        return true;
    }

    core::result_wrapper_t<std::unique_ptr<data_table_t>>
    data_table_t::load_from_disk(std::pmr::memory_resource* resource,
                                 storage::block_manager_t& block_manager,
                                 storage::metadata_reader_t& reader) {
        auto name = reader.read_string();

        auto col_count = reader.read<uint32_t>();
        std::vector<column_definition_t> columns;
        columns.reserve(col_count);
        std::pmr::vector<std::byte> type_spec(resource);
        for (uint32_t i = 0; i < col_count; i++) {
            auto col_name = reader.read_string();
            auto spec_size = reader.read<uint32_t>();
            if (reader.has_error()) {
                return core::error_t(reader.error()); // bail before sizing a buffer off garbage
            }
            type_spec.resize(spec_size);
            reader.read_data(type_spec.data(), spec_size);
            auto not_null = reader.read<uint8_t>() != 0;
            // 0 means the column never learned its attoid; not refused here (reconcile_storage_with_catalog_sync).
            const auto attoid = reader.read<uint32_t>();
            const auto added_at = reader.read<uint64_t>();
            const auto dropped_at = reader.read<uint64_t>();
            if (reader.has_error()) {
                return core::error_t(reader.error());
            }
            // Aliases included: a separate set_alias on bare DECIMAL fabricates a GENERIC extension (UB).
            auto col_type = types::decode_type_spec(resource, type_spec.data(), type_spec.size());
            if (col_type.has_error()) {
                return col_type.convert_error<std::unique_ptr<data_table_t>>(); // data_corruption
            }
            columns.emplace_back(std::move(col_name), std::move(col_type.value()), not_null);
            columns.back().set_attoid(attoid);
            columns.back().set_added_at(added_at);
            columns.back().set_dropped_at(dropped_at);
        }

        auto table = std::make_unique<data_table_t>(resource, block_manager, std::move(columns), std::move(name));

        uint64_t total_loaded_rows = 0;
        auto rg_count = reader.read<uint32_t>();
        // The loader defines what the durable root references, from the same stream the table is built from.
        std::pmr::vector<uint64_t> durable_blocks(resource);
        std::vector<storage::row_group_pointer_t> loaded_pointers;
        loaded_pointers.reserve(rg_count);
        for (uint32_t i = 0; i < rg_count; i++) {
            auto pointer = storage::row_group_pointer_t::deserialize(reader);

            auto* rg = table->row_groups_->append_row_group(static_cast<int64_t>(pointer.row_start));
            if (rg) {
                auto created = rg->create_from_pointer(pointer);
                if (created.has_error()) {
                    return created.convert_error<std::unique_ptr<data_table_t>>(); // data_corruption
                }
                total_loaded_rows += pointer.tuple_count;
            }
            loaded_pointers.push_back(std::move(pointer));
        }
        table->row_groups_->set_total_rows(total_loaded_rows);

        // A read past the metadata chain's end leaves a sticky data_corruption error; surface it.
        if (reader.has_error()) {
            return core::error_t(reader.error());
        }

        // Only now, with the stream proven whole -- a half-read pointer list would let reclaim free live blocks.
        collect_root_blocks(loaded_pointers, durable_blocks);
        block_manager.adopt_durable_root_data_blocks(durable_blocks);

        // The one place modified-since-checkpoint clears outside a commit; else every restart rewrites every table.
        table->clear_modified_since_checkpoint();
        return table;
    }

#ifdef DEV_MODE
    const collection_t* data_table_t::collection_identity() const {
        // The owning side: row_group() must hand back exactly this object; a pre-swap holder still names the old one.
        return row_groups_.get();
    }

    uint64_t data_table_t::collection_owner_count() const {
        return row_groups_ ? static_cast<uint64_t>(row_groups_->use_count()) : 0;
    }
#endif

} // namespace components::table