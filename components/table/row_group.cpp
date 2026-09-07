#include "row_group.hpp"

#include <atomic>

#include <algorithm>
#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/buffer_manager.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <cstdlib>
#include <limits>
#include <unordered_map>
#include <vector/data_chunk.hpp>

#include "collection.hpp"
#include "row_version_manager.hpp"
#include "struct_column_data.hpp"
#include <components/expressions/execution_dag_builder.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::table {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_gathered_borrowed_strings{0};
        std::atomic<uint64_t> g_predicate_row_fetches{0};
        std::atomic<uint64_t> g_string_materializations{0};
        std::atomic<uint64_t> g_gather_rows_fetched{0};
        std::atomic<uint64_t> g_escaping_borrowed_cells{0};
    } // namespace

    uint64_t gathered_borrowed_strings() noexcept {
        return g_gathered_borrowed_strings.load(std::memory_order_relaxed);
    }
    uint64_t string_materializations() noexcept { return g_string_materializations.load(std::memory_order_relaxed); }
    void note_string_materialization() noexcept { g_string_materializations.fetch_add(1, std::memory_order_relaxed); }
    uint64_t gather_rows_fetched() noexcept { return g_gather_rows_fetched.load(std::memory_order_relaxed); }
    void note_gather_row_fetched() noexcept { g_gather_rows_fetched.fetch_add(1, std::memory_order_relaxed); }
    uint64_t escaping_borrowed_cells() noexcept { return g_escaping_borrowed_cells.load(std::memory_order_relaxed); }
    void note_escaping_borrowed_cells(uint64_t cells) noexcept {
        g_escaping_borrowed_cells.fetch_add(cells, std::memory_order_relaxed);
    }
    uint64_t predicate_row_fetches() noexcept { return g_predicate_row_fetches.load(std::memory_order_relaxed); }
    void reset_gathered_borrowed_strings() noexcept {
        g_gathered_borrowed_strings.store(0, std::memory_order_relaxed);
        g_predicate_row_fetches.store(0, std::memory_order_relaxed);
        g_string_materializations.store(0, std::memory_order_relaxed);
        g_gather_rows_fetched.store(0, std::memory_order_relaxed);
        g_escaping_borrowed_cells.store(0, std::memory_order_relaxed);
    }
#endif

    namespace {
        // Bridges create_column's exclusive ownership into a row group's shared column model (0 -> 1 refcount).
        boost::intrusive_ptr<column_data_t> adopt_column(std::unique_ptr<column_data_t> column) {
            return boost::intrusive_ptr<column_data_t>(column.release());
        }
    } // namespace

    row_group_t::row_group_t(collection_t* collection, int64_t start, uint64_t count)
        : segment_base_t(start, count)
        , collection_(collection)
        , allocation_size_(0) {}

    void row_group_t::move_to_collection(collection_t* collection, int64_t new_start) {
        collection_ = collection;
        start = new_start;
        for (auto& column : columns()) {
            column->set_start(new_start);
        }
        // The manager's start_ feeds fetch()'s absolute->local rebase, so it must move with the group.
        if (auto* vinfo = version_info_.load()) {
            vinfo->set_start(new_start);
        }
    }

    std::vector<boost::intrusive_ptr<column_data_t>>& row_group_t::columns() {
        for (uint64_t c = 0; c < get_column_count(); c++) {
            get_column(c);
        }
        return columns_;
    }

    uint64_t row_group_t::get_column_count() const { return columns_.size(); }

    uint64_t row_group_t::row_group_size() const { return collection_->row_group_size(); }

    column_data_t& row_group_t::get_column(const storage_index_t& c) { return get_column(c.primary_index()); }

    column_data_t& row_group_t::get_column(uint64_t c) {
        assert(c < columns_.size());
        if (!is_loaded_) {
            assert(columns_[c]);
            return *columns_[c];
        }
        if (is_loaded_[c]) {
            assert(columns_[c]);
            return *columns_[c];
        }
        if (columns_[c]) {
            assert(is_loaded_[c]);
            return *columns_[c];
        }
        assert(column_pointers_.size() == columns_.size() && "Lazy loading a column but the pointer was not set");
        assert(false && "row_group_t::get_column: unknown error");
        std::abort();
    }

    storage::block_manager_t& row_group_t::block_manager() { return collection_->block_manager(); }

    void row_group_t::initialize_empty(const std::pmr::vector<types::complex_logical_type>& types) {
        assert(columns_.empty());
        for (uint64_t i = 0; i < types.size(); i++) {
            auto column_data =
                column_data_t::create_column(collection_->resource(), block_manager(), i, start, types[i]);
            columns_.push_back(adopt_column(std::move(column_data)));
        }
    }

    bool row_group_t::initialize_scan_with_offset(collection_scan_state& state, uint64_t vector_offset) {
        auto& column_ids = state.column_ids();
        state.row_group = this;
        // row_number/vector_index stay collection-absolute: group-relative would underflow a segment
        // search after a checkpoint re-bases groups.
        auto row_number = start + static_cast<int64_t>(vector_offset * vector::DEFAULT_VECTOR_CAPACITY);
        state.vector_index = static_cast<uint64_t>(row_number) / vector::DEFAULT_VECTOR_CAPACITY;
        const int64_t group_count = static_cast<int64_t>(count.load());
        if (group_count == 0 || start >= state.max_row) {
            state.max_row_group_row = static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            return false;
        }
        state.max_row_group_row = std::min(start + group_count, state.max_row);
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (!column.is_row_id_column()) {
                auto& column_data = get_column(column);
                column_data.initialize_scan_with_offset(state.column_scans[i], row_number);
            } else {
                state.column_scans[i].current = nullptr;
            }
        }
        return true;
    }

    bool row_group_t::initialize_scan(collection_scan_state& state) {
        auto& column_ids = state.column_ids();
        state.row_group = this;
        state.max_row_group_row +=
            start > state.max_row ? 0 : std::min(static_cast<int64_t>(count.load()), state.max_row - start);
        if (state.max_row_group_row == 0) {
            return false;
        }
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            auto column = column_ids[i];
            if (!column.is_row_id_column()) {
                auto& column_data = get_column(column);
                column_data.initialize_scan(state.column_scans[i]);
            } else {
                state.column_scans[i].current = nullptr;
            }
        }
        return true;
    }

    core::result_wrapper_t<std::unique_ptr<row_group_t>>
    row_group_t::add_column(collection_t* new_collection,
                            column_definition_t& new_column,
                            const std::optional<types::logical_value_t>& default_value,
                            vector::vector_t& result) {
        auto added_column = column_data_t::create_column(collection_->resource(),
                                                         block_manager(),
                                                         get_column_count(),
                                                         start,
                                                         new_column.type());

        uint64_t rows_to_write = count;
        if (rows_to_write > 0) {
            const types::logical_value_t fill_value =
                default_value.has_value() ? *default_value
                                          : types::logical_value_t{collection_->resource(), new_column.type()};
            column_append_state state;
            auto init = added_column->initialize_append(state);
            if (init.has_error()) {
                return init.convert_error<std::unique_ptr<row_group_t>>();
            }
            for (uint64_t i = 0; i < rows_to_write; i += vector::DEFAULT_VECTOR_CAPACITY) {
                uint64_t rows_in_this_vector = std::min<uint64_t>(rows_to_write - i, vector::DEFAULT_VECTOR_CAPACITY);
                result.reference(fill_value);
                if (!default_value.has_value()) {
                    result.set_null(true);
                }
                auto appended = added_column->append(state, result, rows_in_this_vector);
                if (appended.has_error()) {
                    return appended.convert_error<std::unique_ptr<row_group_t>>();
                }
            }
        }

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        row_group->columns_ = columns();
        row_group->columns_.push_back(adopt_column(std::move(added_column)));

        return row_group;
    }

    std::unique_ptr<row_group_t> row_group_t::remove_column(collection_t* new_collection, uint64_t removed_column) {
        assert(removed_column < columns_.size());

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        auto& cols = columns();
        for (uint64_t i = 0; i < cols.size(); i++) {
            if (i != removed_column) {
                row_group->columns_.push_back(cols[i]);
            }
        }

        return row_group;
    }

    void row_group_t::next_vector(collection_scan_state& state) {
        state.vector_index++;
        const auto& column_ids = state.column_ids();
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (column.is_row_id_column()) {
                continue;
            }
            get_column(column).skip(state.column_scans[i]);
        }
    }

    // The decision is indexed by vector offset, so a caller holding a visibility selection reads straight into it.
    core::result_wrapper_t<vector::vector_t>
    row_group_t::evaluate_predicate(const table_filter_t& filter, int64_t base_row, uint64_t count) {
        auto* res = collection_->resource();
        // A bound ordinal past the last materialized column is legal (ALTER TABLE ADD COLUMN not yet backfilled);
        // such a column reads its catalog DEFAULT via fill_published_default instead of get_column().
        const size_t materialized = get_column_count();
        std::vector<size_t> referenced;
        size_t width = 0;
        std::pmr::vector<types::complex_logical_type> chunk_types(res);
        for (const auto& binding : filter.graph->input_bindings()) {
            referenced.push_back(binding.column);
            width = std::max(width, binding.column + 1);
            if (binding.column >= chunk_types.size()) {
                chunk_types.resize(binding.column + 1, types::complex_logical_type{types::logical_type::BIGINT});
            }
            chunk_types[binding.column] = filter.graph->slot_type(binding.slot);
        }
        for (size_t column = 0; column < width && column < materialized; column++) {
            bool bound = false;
            for (size_t r : referenced) {
                if (r == column) {
                    bound = true;
                    break;
                }
            }
            if (!bound) {
                chunk_types[column] = get_column(column).type();
            }
        }
        vector::data_chunk_t rows{res, chunk_types, referenced, count};
        // State stays outside the loop (child per column, not shared) so pins live until run_graph reads `rows`.
        column_fetch_state fetch_state;
        size_t child_slot = 0;
        for (size_t column : referenced) {
            auto& column_state = fetch_state.child(child_slot++);
            if (column >= materialized) {
                fill_published_default(rows.data[column], collection_->published_column(column - materialized), count);
                continue;
            }
            for (uint64_t row = 0; row < count; row++) {
#ifdef DEV_MODE
                g_predicate_row_fetches.fetch_add(1, std::memory_order_relaxed);
#endif
                get_column(column)
                    .fetch_row(column_state, base_row + static_cast<int64_t>(row), rows.data[column], row);
                if (fetch_state.absorb_error(column_state)) {
                    return fetch_state.fetch_error;
                }
            }
        }
        rows.set_cardinality(count);
        auto decided = expressions::run_graph(filter.graph.get(), filter.parameters, rows, filter.context);
        if (decided.has_error()) {
            return decided.error();
        }
        return std::move(decided.value().data.front());
    }

    // Pruning needs a constant filter's bound to compare against the segment min/max, and a graph exposes none.
    bool row_group_t::check_zonemap_segments(collection_scan_state&) { return true; }

    void row_group_t::filter_indexing(std::pmr::memory_resource* resource,
                                      uint64_t vector_index,
                                      vector::indexing_vector_t& indexing,
                                      const table_filter_t* filter,
                                      uint64_t vector_count,
                                      uint64_t& approved_tuple_count,
                                      core::error_t& error) {
        const int64_t base_row = static_cast<int64_t>(vector_index * vector::DEFAULT_VECTOR_CAPACITY);
        auto decided = evaluate_predicate(*filter, base_row, vector_count);
        if (decided.has_error()) {
            error = decided.error();
            return;
        }
        const auto& decisions = decided.value();
        vector::indexing_vector_t new_indexing(resource, approved_tuple_count);
        uint64_t result_count = 0;
        for (uint64_t i = 0; i < approved_tuple_count; i++) {
            auto idx = indexing.get_index(i);
            new_indexing.set_index(result_count, idx);
            result_count += !decisions.is_null(idx) && decisions.get_value<bool>(idx);
        }
        indexing = new_indexing;
        approved_tuple_count = result_count;
    }

    template<table_scan_type TYPE>
    void row_group_t::templated_scan(collection_scan_state& state, vector::data_chunk_t& result) {
        constexpr bool ALLOW_UPDATES = TYPE != table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES;
        const auto& column_ids = state.column_ids();
        auto* filter = state.filter();
        for (auto& column_state : state.column_scans) {
            column_state.result_offset = result.size();
        }
        while (true) {
            if (static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY) >= state.max_row_group_row) {
                return;
            }
            int64_t current_row = static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            auto max_count =
                std::min(vector::DEFAULT_VECTOR_CAPACITY, static_cast<size_t>(state.max_row_group_row - current_row));
            if (!check_zonemap_segments(state)) {
                continue;
            }

            uint64_t count;
            if (TYPE == table_scan_type::REGULAR) {
                // REGULAR scans have no see-all fallback: state.txn's snapshot fields drive MVCC visibility.
                count =
                    state.row_group->indexing_vector(state.txn, state.vector_index, state.valid_indexing, max_count);
                if (count == 0) {
                    next_vector(state);
                    continue;
                }
            } else {
                count = max_count;
            }
            validate_chunk_capacity(result, result.size() + count);

            if (count == max_count && !filter) {
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    size_t out_idx = column.is_row_id_column() ? i : column.primary_index();
                    if (column.is_row_id_column()) {
                        assert(result.data[out_idx].type().type() == types::logical_type::BIGINT);
                        result.data[out_idx].sequence(static_cast<int64_t>(start + current_row), 1, count);
                    } else {
                        auto& col_data = get_column(column);
                        if (TYPE == table_scan_type::REGULAR) {
                            col_data.scan(state.vector_index, state.column_scans[i], result.data[out_idx]);
                        } else {
                            col_data.scan_committed(state.vector_index,
                                                    state.column_scans[i],
                                                    result.data[out_idx],
                                                    ALLOW_UPDATES);
                        }
                    }
                }
                state.valid_indexing = vector::indexing_vector_t(result.resource(), 0, result.capacity());
                for (auto& cs : state.column_scans) {
                    if (cs.has_error()) {
                        state.scan_error = cs.scan_error;
                        return;
                    }
                }
            } else {
                uint64_t approved_tuple_count = count;
                vector::indexing_vector_t indexing(result.resource(), result.capacity());
                if (count != max_count) {
                    indexing = state.valid_indexing;
                } else {
                    indexing.reset(nullptr);
                }
                if (filter) {
                    assert(ALLOW_UPDATES);
                    filter_indexing(collection_->resource(),
                                    state.vector_index,
                                    indexing,
                                    filter,
                                    max_count,
                                    approved_tuple_count,
                                    state.scan_error);
                    if (state.has_error()) {
                        return;
                    }
                }
                if (approved_tuple_count == 0) {
                    for (uint64_t i = 0; i < column_ids.size(); i++) {
                        auto& col_idx = column_ids[i];
                        if (col_idx.is_row_id_column()) {
                            continue;
                        }
                        auto& col_data = get_column(col_idx);
                        col_data.skip(state.column_scans[i]);
                    }
                    state.vector_index++;
                    continue;
                }
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    auto& column = column_ids[i];
                    size_t out_idx = column.is_row_id_column() ? i : column.primary_index();
                    if (column.is_row_id_column()) {
                        assert(result.data[out_idx].type().type() == types::logical_type::BIGINT);
                        result.data[out_idx].set_vector_type(vector::vector_type::FLAT);
                        auto result_data = result.data[out_idx].data<int64_t>();
                        for (size_t indexing_idx = 0; indexing_idx < approved_tuple_count; indexing_idx++) {
                            result_data[indexing_idx] =
                                start + current_row + static_cast<int64_t>(indexing.get_index(indexing_idx));
                        }
                    } else {
                        auto& col_data = get_column(column);
                        if (TYPE == table_scan_type::REGULAR) {
                            // Selective filter: gather only surviving rows via fetch_row instead of scanning+slicing
                            // (measured ~7x fewer decompressed rows at 0.2% survival; per-row gather wins below ~20%
                            // survival). No set_vector_type(FLAT): forcing it would reset an already-FLAT constant-size
                            // STRUCT buffer (e.g. INTERVAL) and crash fetch_row.
                            const bool late_materialize =
                                filter != nullptr && approved_tuple_count * uint64_t{5} < max_count;
                            if (late_materialize) {
                                const uint64_t base = state.vector_index * vector::DEFAULT_VECTOR_CAPACITY;
                                const uint64_t off = state.column_scans[i].result_offset;
                                column_fetch_state fetch_state;
                                // Chunk outlives our pins: strings must be copied, not borrowed from a released block.
                                fetch_state.result_outlives_pins = true;
#ifdef DEV_MODE
                                if (!fetch_state.result_outlives_pins &&
                                    result.data[out_idx].type().to_physical_type() == types::physical_type::STRING) {
                                    g_gathered_borrowed_strings.fetch_add(approved_tuple_count,
                                                                          std::memory_order_relaxed);
                                }
#endif
                                for (uint64_t k = 0; k < approved_tuple_count; k++) {
                                    col_data.fetch_row(fetch_state,
                                                       static_cast<int64_t>(base + indexing.get_index(k)),
                                                       result.data[out_idx],
                                                       off + k);
                                    // A pin OOM leaves the cell unwritten; abort via scan_error, not garbage.
                                    if (fetch_state.fetch_error.contains_error()) {
                                        state.scan_error = fetch_state.fetch_error;
                                        return;
                                    }
                                }
                                col_data.skip(state.column_scans[i], max_count);
                            } else {
                                vector::vector_t select_vector(result.resource(),
                                                               result.data[out_idx].type(),
                                                               max_count);
                                auto prev_offset = state.column_scans[i].result_offset;
                                state.column_scans[i].result_offset = 0;
                                col_data.select(state.vector_index,
                                                state.column_scans[i],
                                                select_vector,
                                                indexing,
                                                approved_tuple_count);
                                state.column_scans[i].result_offset = prev_offset;
                                vector::vector_ops::copy(select_vector,
                                                         result.data[out_idx],
                                                         approved_tuple_count,
                                                         0,
                                                         state.column_scans[i].result_offset);
                            }
                        } else {
                            col_data.select_committed(state.vector_index,
                                                      state.column_scans[i],
                                                      result.data[out_idx],
                                                      indexing,
                                                      approved_tuple_count,
                                                      ALLOW_UPDATES);
                        }
                    }
                }

                for (auto& cs : state.column_scans) {
                    if (cs.has_error()) {
                        state.scan_error = cs.scan_error;
                        return;
                    }
                }

                assert(approved_tuple_count > 0);
                count = approved_tuple_count;
                state.valid_indexing = indexing;
            }
            auto* row_ids_data = result.row_ids.data<int64_t>();
            const int64_t row_id_base = static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            const uint64_t write_start = result.size();
            for (uint64_t i = 0; i < count; i++) {
                row_ids_data[write_start + i] = row_id_base + static_cast<int64_t>(state.valid_indexing.get_index(i));
            }
            result.set_cardinality(result.size() + count);
            state.vector_index++;
            for (auto& column_state : state.column_scans) {
                column_state.result_offset += count;
            }
            break;
        }
    }

    void row_group_t::scan(collection_scan_state& state, vector::data_chunk_t& result) {
        templated_scan<table_scan_type::REGULAR>(state, result);
    }

    void row_group_t::scan_committed(collection_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        switch (type) {
            case table_scan_type::COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS>(state, result);
                break;
            case table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES:
                templated_scan<table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES>(state, result);
                break;
            case table_scan_type::LATEST_COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS>(state, result);
                break;
            default:
                assert(false && "Unrecognized table scan type");
                std::abort();
        }
    }

    void row_group_t::fetch_row(column_fetch_state& state,
                                const std::vector<storage_index_t>& column_ids,
                                int64_t row_id,
                                vector::data_chunk_t& result,
                                uint64_t result_idx,
                                const std::vector<size_t>& projected_cols) {
        for (uint64_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            // Mapping is positional (column_ids[i] -> result.data[i]); skip unwanted columns instead of dropping them.
            if (!projected_cols.empty() &&
                std::find(projected_cols.begin(), projected_cols.end(), static_cast<size_t>(col_idx)) ==
                    projected_cols.end()) {
                continue;
            }
            auto& column = column_ids[col_idx];
            auto& result_vector = result.data[col_idx];
            assert(result_vector.get_vector_type() == vector::vector_type::FLAT);
            assert(!result_vector.is_null(result_idx));
            if (column.is_row_id_column()) {
                assert(result_vector.type().to_physical_type() == types::physical_type::INT64);
                result_vector.set_vector_type(vector::vector_type::FLAT);
                auto data = result_vector.data<int64_t>();
                data[result_idx] = row_id;
            } else {
                auto& col_data = get_column(column);
                auto& column_state = state.child(col_idx);
                col_data.fetch_row(column_state, row_id, result_vector, result_idx);
                if (state.absorb_error(column_state)) {
                    return;
                }
            }
        }
    }

    bool row_group_t::is_visible(const transaction_data& txn, int64_t row_id) {
        auto* versions = version_info();
        if (!versions) {
            // No manager: everything is committed-and-visible, same as fetch() over a null chunk_info.
            return true;
        }
        return versions->fetch(txn, static_cast<uint64_t>(row_id));
    }

    uint64_t row_group_t::delete_stamp(int64_t row_id) {
        auto* versions = version_info();
        if (!versions) {
            return NOT_DELETED_ID;
        }
        return versions->delete_stamp(static_cast<uint64_t>(row_id));
    }

    void row_group_t::append_version_info(transaction_data txn, uint64_t count) {
        uint64_t row_group_start = this->count.load();
        uint64_t row_group_end = row_group_start + count;
        if (row_group_end > row_group_size()) {
            row_group_end = row_group_size();
        }
        this->count = row_group_end;
        get_or_create_version_info().append_version_info(txn, count, row_group_start, row_group_end);
    }

    void row_group_t::commit_append(uint64_t commit_id, uint64_t row_group_start, uint64_t count) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->commit_append(commit_id, row_group_start, count);
        }
        if (commit_id > current_version_) {
            current_version_ = commit_id;
        }
    }

    core::result_wrapper_t<bool> row_group_t::revert_append(uint64_t row_group_start) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->revert_append(row_group_start);
        }
        // row_group_start is row-group-local; a column's start_ is absolute, so truncate at start + local, or
        // stale tails let a later scan overrun the result vector. Best-effort: first refusal wins, count still shrinks.
        core::error_t first_error = core::error_t::no_error();
        for (uint64_t c = 0; c < get_column_count(); c++) {
            auto reverted = get_column(c).revert_append(this->start + static_cast<int64_t>(row_group_start));
            if (reverted.has_error() && !first_error.contains_error()) {
                first_error = reverted.error();
            }
        }
        if (row_group_start < this->count.load()) {
            this->count = row_group_start;
        }
        if (first_error.contains_error()) {
            return first_error;
        }
        return true;
    }

    core::result_wrapper_t<bool> row_group_t::initialize_append(row_group_append_state& append_state) {
        append_state.row_group = this;
        append_state.offset_in_row_group = count;
        append_state.states = std::make_unique<column_append_state[]>(get_column_count());
        for (uint64_t i = 0; i < get_column_count(); i++) {
            auto& col_data = get_column(i);
            auto init = col_data.initialize_append(append_state.states[i]);
            if (init.has_error()) {
                return init; // out_of_memory
            }
        }
        return true;
    }

    core::result_wrapper_t<bool>
    row_group_t::append(row_group_append_state& state, vector::data_chunk_t& chunk, uint64_t append_count) {
        assert(chunk.column_count() == get_column_count());
        for (uint64_t i = 0; i < get_column_count(); i++) {
            auto& col_data = get_column(i);
            auto prev_allocation_size = col_data.allocation_size();
            auto appended = col_data.append(state.states[i], chunk.data[i], append_count);
            allocation_size_ += col_data.allocation_size() - prev_allocation_size;
            if (appended.has_error()) {
                return appended; // out_of_memory
            }
        }
        state.offset_in_row_group += append_count;
        return true;
    }

    core::result_wrapper_t<bool> row_group_t::update(vector::data_chunk_t& update_chunk,
                                                     int64_t* ids,
                                                     uint64_t offset,
                                                     uint64_t count,
                                                     const std::vector<uint64_t>& column_ids) {
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            auto column = column_ids[i];
            assert(column != std::numeric_limits<uint64_t>::max());
            auto& col_data = get_column(column);
            assert(col_data.type().type() == update_chunk.data[i].type().type());
            core::result_wrapper_t<bool> updated = [&]() -> core::result_wrapper_t<bool> {
                if (offset > 0) {
                    vector::vector_t sliced_vector(update_chunk.data[i], offset, count);
                    sliced_vector.flatten(count);
                    return col_data.update(column, sliced_vector, ids + offset, count);
                }
                return col_data.update(column, update_chunk.data[i], ids, count);
            }();
            if (updated.has_error()) {
                return updated; // out_of_memory / data_corruption / io_error
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> row_group_t::update_column(vector::data_chunk_t& updates,
                                                            vector::vector_t& row_ids,
                                                            const std::vector<uint64_t>& column_path,
                                                            uint64_t offset,
                                                            uint64_t count) {
        assert(updates.column_count() == 1);
        auto ids = row_ids.data<int64_t>();

        if (column_path.empty() || column_path[0] >= columns_.size()) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("row group update: the column path names no column of this row group",
                                 collection_->resource()));
        }
        auto primary_column_idx = column_path[0];
        auto& col_data = get_column(primary_column_idx);
        if (offset > 0) {
            vector::vector_t sliced_vector(updates.data[0], offset, count);
            sliced_vector.flatten(count);
            return col_data.update_column(column_path, sliced_vector, ids + offset, count, 1);
        }
        return col_data.update_column(column_path, updates.data[0], ids, count, 1);
    }

    uint64_t row_group_t::committed_row_count() {
        auto* vi = version_info_.load();
        if (vi) {
            return count - vi->committed_deleted_count(count);
        }
        return count;
    }

    bool row_group_t::has_version_above(uint64_t watermark) {
        auto* vi = version_info_.load();
        if (!vi) {
            return false;
        }
        return vi->has_version_above(watermark, count);
    }

    void row_group_t::get_column_segment_info(uint64_t row_group_index, std::vector<column_segment_info>& result) {
        for (uint64_t col_idx = 0; col_idx < get_column_count(); col_idx++) {
            auto& col_data = get_column(col_idx);
            col_data.get_column_segment_info(row_group_index, {col_idx}, result);
        }
    }

    void row_group_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) {
        for (auto& column : columns_) {
            if (column) {
                column->collect_disk_block_ids(out);
            }
        }
    }

    void row_group_t::collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out) {
        if (column_index >= columns_.size() || !columns_[column_index]) {
            return;
        }
        columns_[column_index]->collect_disk_block_ids(out);
    }

    class version_delete_state {
    public:
        version_delete_state(row_group_t& info,
                             uint64_t current_version,
                             data_table_t& table,
                             int64_t base_row,
                             bool is_txn = false)
            : info(info)
            , table(table)
            , current_chunk(storage::INVALID_INDEX)
            , current_version(current_version)
            , base_row(base_row)
            , delete_count(0)
            , count(0)
            , is_txn_(is_txn) {}

        row_group_t& info;
        data_table_t& table;
        uint64_t current_chunk;
        uint64_t current_version;
        int64_t rows[vector::DEFAULT_VECTOR_CAPACITY];
        int64_t base_row;
        uint64_t chunk_row;
        uint64_t delete_count;
        uint64_t count;
        bool is_txn_;

        void delete_row(int64_t row_id);
        void flush();
    };

    uint64_t row_group_t::delete_rows(uint64_t vector_idx, int64_t rows[], uint64_t count) {
        const auto delete_id = ++current_version_;
        auto deleted = get_or_create_version_info().delete_rows(vector_idx, delete_id, rows, count);
        ++current_version_;
        return deleted;
    }

    uint64_t row_group_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id) {
        const bool is_txn = !is_direct_write_txn(transaction_id);
        version_delete_state del_state(*this, transaction_id, table, start, is_txn);

        for (uint64_t i = 0; i < count; i++) {
            assert(ids[i] >= 0);
            assert(ids[i] >= start && ids[i] < start + static_cast<int64_t>(this->count));
            del_state.delete_row(ids[i]);
        }
        del_state.flush();
        return del_state.delete_count;
    }

    void row_group_t::commit_delete(uint64_t commit_id, uint64_t vector_idx, const delete_info& info) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->commit_delete(vector_idx, commit_id, info);
        }
    }

    void row_group_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->commit_all_deletes(txn_id, commit_id);
        }
        if (commit_id >= current_version_) {
            current_version_ = commit_id + 1;
        }
    }

    void row_group_t::revert_all_deletes(uint64_t txn_id) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->revert_all_deletes(txn_id);
        }
        // No current_version_ advance: unlike commit there is no new commit_id to publish.
    }

    row_version_manager_t& row_group_t::get_or_create_version_info() {
        auto vinfo = version_info();
        if (vinfo) {
            return *vinfo;
        }
        return *get_or_create_version_info_internal();
    }

    boost::intrusive_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_ptr() {
        auto vinfo = version_info();
        if (vinfo) {
            return owned_version_info_;
        }
        return get_or_create_version_info_internal();
    }

    uint64_t row_group_t::calculate_size() {
        vector::indexing_vector_t temp_indexing(collection().resource(), count);
        transaction_data td(0, 0);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return indexing_vector(td,
                               static_cast<uint64_t>(start) / vector::DEFAULT_VECTOR_CAPACITY,
                               temp_indexing,
                               count);
    }

    uint64_t row_group_t::indexing_vector(transaction_data txn,
                                          uint64_t vector_idx,
                                          vector::indexing_vector_t& indexing_vector,
                                          uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        // vector_idx is collection-absolute, version slots group-local: a later group would scan every row as visible.
        assert(start % static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY) == 0);
        const uint64_t base_vector_idx = static_cast<uint64_t>(start) / vector::DEFAULT_VECTOR_CAPACITY;
        assert(vector_idx >= base_vector_idx);
        return vinfo->indexing_vector(txn, vector_idx - base_vector_idx, indexing_vector, max_count);
    }

    boost::intrusive_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_internal() {
        if (!owned_version_info_) {
            // Plain `new`, never the pmr resource: the ref count lives inside the manager, so `delete` matches.
            auto new_info = boost::intrusive_ptr<row_version_manager_t>(new row_version_manager_t(start));
            set_version_info(std::move(new_info));
        }
        return owned_version_info_;
    }

    row_version_manager_t* row_group_t::version_info() {
        // Deliberately unconditional: no "load delete info from disk" branch here.
        return version_info_;
    }

    void row_group_t::set_version_info(boost::intrusive_ptr<row_version_manager_t> version) {
        // Own first, publish second (see row_group.hpp): every caller today moves a group from no-manager to
        // manager, so ownership completes before the seq_cst store exposes the pointer to lock-free readers.
        // A caller clearing/replacing an existing manager must store the atomic first, or risk a dangling pointer.
        owned_version_info_ = std::move(version);
        version_info_ = owned_version_info_.get();
    }

    void version_delete_state::delete_row(int64_t row_id) {
        assert(row_id >= base_row);
        // row_id is collection-absolute; row_version_manager_t slots are group-local, so rebase by base_row.
        const uint64_t local_row = static_cast<uint64_t>(row_id - base_row);
        uint64_t vector_idx = local_row / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t idx_in_vector = local_row - vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
        if (current_chunk != vector_idx) {
            flush();

            current_chunk = vector_idx;
            chunk_row = vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
        }
        rows[count++] = static_cast<int64_t>(idx_in_vector);
    }

    void version_delete_state::flush() {
        if (count == 0) {
            return;
        }
        uint64_t actual_delete_count;
        if (is_txn_) {
            actual_delete_count =
                info.get_or_create_version_info().delete_rows(current_chunk, current_version, rows, count);
        } else {
            actual_delete_count = info.delete_rows(current_chunk, rows, count);
        }
        delete_count += actual_delete_count;
        count = 0;
    }
    namespace {
        storage::column_data_pointers_t to_column_pointers(const persistent_column_data_t& persistent) {
            storage::column_data_pointers_t out;
            out.count = persistent.count;
            out.segments = persistent.data_pointers;
            out.children.reserve(persistent.child_columns.size());
            for (const auto& child : persistent.child_columns) {
                out.children.push_back(to_column_pointers(*child));
            }
            return out;
        }

        persistent_column_data_t from_column_pointers(std::pmr::memory_resource* resource,
                                                      const storage::column_data_pointers_t& pointers) {
            persistent_column_data_t persistent(resource);
            persistent.count = pointers.count;
            persistent.data_pointers = pointers.segments;
            persistent.child_columns.reserve(pointers.children.size());
            for (const auto& child : pointers.children) {
                persistent.child_columns.push_back(
                    std::make_unique<persistent_column_data_t>(from_column_pointers(resource, child)));
            }
            return persistent;
        }
    } // namespace

    core::result_wrapper_t<storage::row_group_pointer_t>
    row_group_t::write_to_disk(storage::partial_block_manager_t& partial_block_manager) {
        storage::row_group_pointer_t pointer;
        pointer.row_start = static_cast<uint64_t>(start);
        pointer.tuple_count = count;

        auto col_count = get_column_count();
        pointer.data_pointers.reserve(col_count);

        for (uint64_t i = 0; i < col_count; i++) {
            auto persistent = columns_[i]->checkpoint(partial_block_manager);
            if (persistent.has_error()) {
                return persistent.convert_error<storage::row_group_pointer_t>(); // out_of_memory
            }
            pointer.data_pointers.push_back(to_column_pointers(persistent.value()));
        }

        return pointer;
    }

    core::result_wrapper_t<bool> row_group_t::transition_to_disk() {
        // One partial_block_manager per closed row group: all its columns' segments pack into shared
        // blocks instead of one block per segment (avoids a ~127x over-allocation for narrow columns).
        storage::partial_block_manager_t pbm(block_manager());
        for (uint64_t i = 0; i < columns_.size(); i++) {
            if (!columns_[i]) {
                continue;
            }
            auto transitioned = columns_[i]->transition_to_disk(pbm);
            if (transitioned.has_error()) {
                return transitioned; // io_error / out_of_memory
            }
        }
        // Flush before returning (synchronous, like checkpoint): the flush-before-evict point compact also reuses.
        if (auto flushed = pbm.flush_partial_blocks(); flushed.has_error()) {
            return flushed; // io_error: the re-pointed segments' blocks are not on disk
        }
        return true;
    }

    core::result_wrapper_t<bool> row_group_t::create_from_pointer(const storage::row_group_pointer_t& pointer) {
        count = pointer.tuple_count;
        auto col_count = get_column_count();
        // Built from the same metadata stream as this pointer, so a mismatch means corruption, not a subset to load.
        if (pointer.data_pointers.size() != col_count) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("row group load: column tree count does not match the table's columns",
                                 collection().resource()));
        }

        for (uint64_t i = 0; i < col_count; i++) {
            auto pcd = from_column_pointers(columns_[i]->resource(), pointer.data_pointers[i]);
            auto initialized = columns_[i]->initialize_column(pcd);
            if (initialized.has_error()) {
                return initialized; // data_corruption
            }
        }
        return true;
    }

#ifdef DEV_MODE
    const column_data_t* row_group_t::column_identity(uint64_t c) const {
        assert(c < columns_.size());
        return columns_[c].get();
    }

    uint64_t row_group_t::column_owner_count(uint64_t c) const {
        assert(c < columns_.size());
        return columns_[c] ? columns_[c]->use_count() : 0;
    }

    const row_version_manager_t* row_group_t::version_manager_identity() const {
        return owned_version_info_.get();
    }

    const row_version_manager_t* row_group_t::version_manager_published() const {
        return version_info_.load();
    }

    uint64_t row_group_t::version_manager_owner_count() const {
        return owned_version_info_ ? static_cast<uint64_t>(owned_version_info_->use_count()) : 0;
    }
#endif

} // namespace components::table