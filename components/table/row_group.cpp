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
        // create_column hands back EXCLUSIVE ownership because that is what the nested children
        // need. A row group's TOP-LEVEL columns are shared with its ALTER successors instead, so
        // this is the one place the two ownership models meet: the fresh object is released from
        // the unique_ptr and adopted by its own reference count (0 -> 1).
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
        // Version slots are group-local, so a move never re-slots them — but the
        // manager's start_ feeds fetch()'s absolute→local rebase and the chunk_info
        // start labels, so it must move with the group. (Only merge_storage reaches
        // this; without the rebase the manager would keep answering point-fetches
        // for the group's OLD position.)
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
        // The absolute row this seek lands on. `vector_offset` is group-relative; `start` is the
        // group's collection-absolute origin in the CURRENT (post-checkpoint) segment tree.
        auto row_number = start + static_cast<int64_t>(vector_offset * vector::DEFAULT_VECTOR_CAPACITY);
        // vector_index and max_row_group_row use the SAME cumulative collection-absolute convention as
        // the continuous initialize_scan() path: vector_index*CAP is an absolute collection row, and
        // max_row_group_row is the absolute end-of-scan row. This keeps every downstream
        // `vector_index*CAP` / `current_row` computation in templated_scan and filter_indexing
        // (row-id assignment AND the check_predicate column lookups) addressing absolute rows the live
        // tree brackets — instead of group-relative positions that, after a checkpoint re-bases the
        // groups, would seek a column segment by a row below its row_start and underflow the segment
        // search. Geometry-agnostic: derived purely from `start`/`count` of the resolved group, never
        // from a fixed row_group_size.
        state.vector_index = static_cast<uint64_t>(row_number) / vector::DEFAULT_VECTOR_CAPACITY;
        const int64_t group_count = static_cast<int64_t>(count.load());
        // Nothing to scan if the group ends at/before the seek floor or is past the scan ceiling.
        if (group_count == 0 || start >= state.max_row) {
            state.max_row_group_row = static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            return false;
        }
        state.max_row_group_row = std::min(start + group_count, state.max_row);
        // No assert on column_scans: a column-less scan (see table_scan_state::column_ids) is
        // legal and leaves this vector empty, and the loop below then runs zero times.
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
        // Same as initialize_scan_with_offset: an empty column list is a row-count-only scan.
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
            // DDL ADD COLUMN backfill path (synchronous, not an actor append boundary). The
            // append chain reports out_of_memory; that answer now RIDES this function's own
            // channel. The asserts that used to stand here vanished under NDEBUG and the loop
            // then broke silently: the successor shipped with a new column SHORTER than count,
            // and every scan of it read past the column's end (rule 6).
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
        // Structural sharing, deliberately: the successor's row group points at the SAME column
        // objects as this one, and the intrusive count in each column keeps them alive until the
        // last of the two row groups is gone. A deep copy here would double the table's memory and
        // silently fork the two views of the same rows.
        row_group->columns_ = columns();
        row_group->columns_.push_back(adopt_column(std::move(added_column)));

        return std::move(row_group);
    }

    std::unique_ptr<row_group_t> row_group_t::remove_column(collection_t* new_collection, uint64_t removed_column) {
        assert(removed_column < columns_.size());

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        // Same structural sharing as add_column: every surviving column object is SHARED with the
        // successor's row group, not copied.
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

    // Materialise the columns the graph binds for one vector's rows and run it. The decision comes
    // back indexed by vector offset, so a caller holding a visibility selection reads straight into it.
    core::result_wrapper_t<vector::vector_t>
    row_group_t::evaluate_predicate(const table_filter_t& filter, int64_t base_row, uint64_t count) {
        auto* res = collection_->resource();
        // The chunk presents every bound column at its own storage ordinal, so a bound slot resolves
        // to it; the projected ctor allocates buffers ONLY for those, the rest stay placeholders.
        //
        // A BOUND ORDINAL PAST THE LAST MATERIALIZED COLUMN is not a bug and not out of bounds:
        // the graph was typed against the CATALOG's column list (storage_types), and pg_attribute
        // can legally name a column this storage has not materialized yet — ALTER TABLE ADD
        // COLUMN writes the catalog row and stops, the physical column is created by the first
        // INSERT that carries it. Such a column is NULL in every existing row, so it is fed to
        // the graph as an all-invalid vector of the type the graph itself carries for that slot
        // (the same "type + set_all_invalid" shape the append path uses for a column an incoming
        // chunk omits). Calling get_column() on it would abort inside row_group_t::get_column.
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
        // Unbound ordinals below `width` are gaps the graph never reads; they only need a type to
        // construct a (buffer-less) placeholder with, and the storage's own is the honest one.
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
        // ONE state for the whole function, a CHILD of it per column — the shape
        // row_group_t::fetch_row already uses, and both halves of it are load-bearing here.
        //
        // The child is what stops two bound columns from sharing child_states: a struct
        // column reads its fields through child(0), so two struct-typed bound columns used
        // to alias each other's field states. Harmless while handles are keyed by block id
        // and the flag/error are homogeneous, but an aliasing invariant nobody stated.
        //
        // The single OUTER state is what keeps the pins: result_outlives_pins is false here
        // (this chunk dies inside the call), so a fetched long string is a view into a
        // pinned block, and `rows` is read by run_graph BELOW this loop. Declaring the state
        // inside the loop instead would release each column's pins one iteration early —
        // the same one-line "fix" that looks equivalent and is not.
        column_fetch_state fetch_state;
        size_t child_slot = 0;
        for (size_t column : referenced) {
            auto& column_state = fetch_state.child(child_slot++);
            if (column >= materialized) {
                rows.data[column].validity().set_all_invalid(count);
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

    // Pruning is off while a filter is only a graph: it used to read a constant filter's bound
    // against the segment min/max, and the graph does not expose one yet.
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
        // The decision is indexed by vector offset, so the visible rows read straight out of it.
        // UNKNOWN drops the row, exactly as a false does.
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
        // Sync result_offset with current chunk cardinality (handles chunk reset between calls)
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
                // REGULAR scans have no see-all fallback: state.txn must be a real
                // transaction_data, as its snapshot fields drive MVCC visibility.
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
                    // Write into the output slot corresponding to the storage column index
                    // (for row_id column, write into slot i as the caller expects it there).
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
                // Aggregate any per-column pin OOM raised by the scans above.
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
                            // Late materialization: for a SELECTIVE filter, gather ONLY the surviving rows
                            // (fetch_row — the same primitive check_predicate used to build the selection)
                            // directly into the result instead of scanning the whole vector and slicing, so a
                            // wide non-filter column decompresses approved_tuple_count rows, not max_count
                            // (measured ~7x at 0.2% survival on a wide table). Gated on selectivity: below ~20%
                            // survival the per-row gather wins; above it the bulk select() is competitive.
                            // fetch_row is updates-aware for every column kind (base scan_count applies the
                            // overlay), so STRUCT/ARRAY/LIST and updated columns gather safely too. No
                            // set_vector_type(FLAT) here: result vectors are already FLAT and carry their
                            // auxiliary buffer from chunk construction; forcing FLAT would reset a constant-size
                            // STRUCT buffer (e.g. INTERVAL) and crash fetch_row.
                            const bool late_materialize =
                                filter != nullptr && approved_tuple_count * uint64_t{5} < max_count;
                            if (late_materialize) {
                                const uint64_t base = state.vector_index * vector::DEFAULT_VECTOR_CAPACITY;
                                const uint64_t off = state.column_scans[i].result_offset;
                                column_fetch_state fetch_state;
                                // This chunk is returned to the caller; our pins are not. Strings
                                // must be copied into the result rather than borrowed from a block
                                // we are about to release.
                                fetch_state.result_outlives_pins = true;
#ifdef DEV_MODE
                                // Guards the line above rather than the gather itself: if the flag
                                // is ever dropped, every string cell this branch fills goes back to
                                // being a view into a block whose pin dies with fetch_state.
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
                                    // A pin OOM inside fetch_row (get_or_insert_handle) leaves this
                                    // cell unwritten; abort the scan through scan_error exactly like
                                    // the bulk legs below — never "succeed" with garbage cells.
                                    // Checked per fetch so the FIRST error aborts (fetch_error is
                                    // sticky but later failures would overwrite it).
                                    if (fetch_state.fetch_error.contains_error()) {
                                        state.scan_error = fetch_state.fetch_error;
                                        return;
                                    }
                                }
                                // Advance the sequential scan state past this vector (gather did not scan it).
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

                // Aggregate any per-column pin OOM raised by select/select_committed.
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
            // The mapping below is POSITIONAL — column_ids[i] lands in result.data[i] — so a caller
            // that wants fewer columns cannot simply pass a shorter list: that compacts the chunk and
            // every consumer indexing by ordinal reads the wrong column. Skipping here instead leaves
            // the unwanted slots as the untouched stubs they already are, and every wanted column
            // keeps its ordinal.
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
                // Per-column child state (not the ONE shared `state`): a struct column's
                // validity used to live in state.child(0) for EVERY struct-typed top-level
                // column, so two of them aliased each other's children — harmless only
                // because handles are keyed by block id and the flag/error are homogeneous,
                // but an aliasing invariant nobody stated. child() re-stamps
                // result_outlives_pins on hand-out and absorb_error lifts the refusal into
                // the state the callers of THIS function actually read; the first failing
                // column aborts the row, as in the struct's own field walk.
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
            // No manager == no insert stamp and no tombstone recorded for this group.
            // Everything in it is committed-and-visible, which is the same answer
            // row_version_manager_t::fetch gives over a null chunk_info.
            return true;
        }
        return versions->fetch(txn, static_cast<uint64_t>(row_id));
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
        // Update current_version_ so that scans without explicit txn data can see committed rows
        if (commit_id > current_version_) {
            current_version_ = commit_id;
        }
    }

    core::result_wrapper_t<bool> row_group_t::revert_append(uint64_t row_group_start) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->revert_append(row_group_start);
        }
        // Truncate every column back to row_group_start too. row_group_start is row-group-LOCAL
        // (see collection_t::revert_append), but a column's own coordinates are ABSOLUTE and its
        // start_ equals this row group's start, so the absolute truncation row is start + local.
        // Without this the column segments and their count_ keep the reverted rows: a later scan
        // sized by the row group's (reduced) count then over-reads the stale column tail and writes
        // past the result vector (heap-buffer-overflow in fetch_row).
        //
        // Best-effort across columns: each column's truncation is independent, so one refusal
        // must not leave the remaining columns un-truncated (that is the over-read above).
        // The FIRST refusal is kept and reported after the walk; the count still shrinks —
        // a stale tail beyond the reduced count is invisible, which is the safe direction.
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

        // The path arrives from the caller's plan/journal, so an impossible ordinal is a loud
        // refusal on the channel this function already returns, not an assert that vanishes
        // under NDEBUG and indexes columns_ out of range (rules 2/6).
        if (column_path.empty() || column_path[0] >= columns_.size()) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("row group update: the column path names no column of this row group",
                                 collection_->resource()));
        }
        auto primary_column_idx = column_path[0];
        auto& col_data = get_column(primary_column_idx);
        // Same slicing contract as update() above: [offset, offset + count) of the caller's
        // chunk/ids is this row group's share of the statement.
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
            // No version info — every row is plain committed, visible to all.
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
        // Only walk columns already materialized in memory; an unloaded/disk-loaded column is iterated
        // via columns_ lazily by get_column, which is what we want (its segments carry disk block ids).
        for (auto& column : columns_) {
            if (column) {
                column->collect_disk_block_ids(out);
            }
        }
    }

    void row_group_t::collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out) {
        // Same materialized-only rule as the whole-row-group walk above: a column this row group
        // never materialized owns no in-memory segments to read block ids off. An index past the
        // end is a row group that predates the column (dynamic schema growth appends columns to
        // LATER row groups only), not an error.
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
        // The is_txn == false leg is what gives DIRECT_WRITE_TXN_ID its meaning: it stamps an
        // immediately-committed version id, so no publish/revert is owed afterwards. Named
        // rather than re-derived, because two disk-side handlers branch on the same sentinel.
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
        // Advance current_version_ past commit_id so that committed deletes
        // are visible to scans using committed_version_operator
        if (commit_id >= current_version_) {
            current_version_ = commit_id + 1;
        }
    }

    void row_group_t::revert_all_deletes(uint64_t txn_id) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->revert_all_deletes(txn_id);
        }
        // No current_version_ advance: revert un-marks pending deletes back to
        // NOT_DELETED_ID, restoring visibility. Unlike commit there is no new
        // commit_id to publish, so the version watermark stays where it was.
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
        // Metadata accounting, not a user scan: a UINT64_MAX horizon + empty
        // in_flight set is a see-all snapshot covering every committed row.
        transaction_data td(0, 0);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        // indexing_vector takes the scan state's collection-ABSOLUTE vector index;
        // this group's first vector sits at start / CAPACITY. (The old `index`
        // argument — the segment ordinal — only coincided with it while every
        // group held exactly one vector.)
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
        // `vector_idx` arrives collection-ABSOLUTE (the scan state's convention, see
        // initialize_scan_with_offset); version slots are GROUP-LOCAL. Convert here,
        // where `start` is authoritative, so the reader consults the same slot the
        // append path wrote — for any group past the first the absolute index used
        // to fall off the end of vector_info_ and every row (committed or not)
        // scanned as visible.
        assert(start % static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY) == 0);
        const uint64_t base_vector_idx = static_cast<uint64_t>(start) / vector::DEFAULT_VECTOR_CAPACITY;
        assert(vector_idx >= base_vector_idx);
        return vinfo->indexing_vector(txn, vector_idx - base_vector_idx, indexing_vector, max_count);
    }

    boost::intrusive_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_internal() {
        if (!owned_version_info_) {
            // Plain `new`, never the pmr resource: the reference count lives inside the manager,
            // so the counter's `delete` is the matching deallocation. Nothing was lost by giving
            // up make_shared's single object+control-block allocation — no weak_ptr, aliasing
            // pointer, custom deleter or shared_from_this was ever taken on a manager, and the
            // intrusive count is one allocation too, not two.
            auto new_info = boost::intrusive_ptr<row_version_manager_t>(new row_version_manager_t(start));
            set_version_info(std::move(new_info));
        }
        return owned_version_info_;
    }

    row_version_manager_t* row_group_t::version_info() {
        // The unreachable "unloaded deletes" branch that used to sit here — a stub for a
        // delete-info disk load that does not exist, whose body would have DISCARDED this row
        // group's version state (set_version_info(nullptr)) instead of loading anything — is
        // GONE, together with the never-populated deletes_pointers_ / never-initialised
        // deletes_is_loaded_ pair that armed it. Reviving on-disk delete info means writing a
        // real load here, not resurrecting a branch whose only observable behaviour was the
        // silent degradation rule 6 forbids.
        return version_info_;
    }

    void row_group_t::set_version_info(boost::intrusive_ptr<row_version_manager_t> version) {
        // Own FIRST, publish SECOND — the sole writer of the member pair, and what holds their
        // invariant (see row_group.hpp).
        //
        // Every REACHABLE caller moves this row group from "no manager" to "a manager":
        // get_or_create_version_info_internal only calls in under `if (!owned_version_info_)`,
        // and add_column / remove_column call it on a row group they have just constructed, whose
        // owner is still null and which no other reader can reach yet. So the object is fully
        // owned and alive before the raw pointer naming it becomes visible, and the seq_cst store
        // orders the object's construction ahead of the publication for the seq_cst loads on the
        // read path.
        //
        // The order is correct ONLY for that transition. A caller that CLEARED or REPLACED an
        // existing manager would release the owner on the first line — dropping what may be the
        // last reference and destroying the object — while version_info_ still named it, so a
        // reader on the lock-free path could pick up a dangling pointer. Should such a caller
        // ever be added, the atomic must be stored first. (No clearing caller exists in the
        // tree: the one that used to — version_info()'s unreachable unloaded-deletes stub —
        // was removed together with its arming fields.)
        owned_version_info_ = std::move(version);
        version_info_ = owned_version_info_.get();
    }

    void version_delete_state::delete_row(int64_t row_id) {
        assert(row_id >= base_row);
        // row_id is collection-ABSOLUTE; row_version_manager_t slots are GROUP-LOCAL
        // (slot 0 = the group's first vector). Rebase by the group's start (base_row)
        // before slicing into vectors, so the tombstone lands in the slot the scan
        // and the committed_deleted_count / has_version_above walks actually read.
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
        // persistent_column_data_t <-> column_data_pointers_t: the row-group pointer stores
        // the recursive segment layout (count + own segments + children) of every column
        // NODE, so nested columns (LIST/STRUCT/ARRAY) round-trip their children. Statistics
        // are intentionally not part of the row-group pointer (as before).
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
        // Own ONE partial_block_manager for this closed row group: ALL its columns' (and validity children's)
        // segments are PACKED together into shared blocks, so a row group of narrow columns shares a handful
        // of blocks instead of one block per segment (the ~127x over-allocation B2 fixes).
        storage::partial_block_manager_t pbm(block_manager());
        // Only transition columns already materialized in memory (a freshly-appended row group). Disk-loaded /
        // unloaded columns are already disk-backed (block_id < MAXIMUM) -> nothing to do; don't force a load.
        for (uint64_t i = 0; i < columns_.size(); i++) {
            if (!columns_[i]) {
                continue;
            }
            auto transitioned = columns_[i]->transition_to_disk(pbm);
            if (transitioned.has_error()) {
                return transitioned; // io_error / out_of_memory
            }
        }
        // Flush BEFORE returning: this runs synchronously on the single disk-agent thread (like checkpoint),
        // so once it returns every re-pointed segment's packed block is durable on disk and a subsequent scan
        // or eviction can safely load() it. THIS is the flush-before-evict point for the per-row-group-close
        // append path (and, transitively, for compact, which rebuilds via this same append path).
        if (auto flushed = pbm.flush_partial_blocks(); flushed.has_error()) {
            return flushed; // io_error: the re-pointed segments' blocks are not on disk
        }
        return true;
    }

    core::result_wrapper_t<bool> row_group_t::create_from_pointer(const storage::row_group_pointer_t& pointer) {
        count = pointer.tuple_count;
        auto col_count = get_column_count();
        // The table was just built from the SAME metadata stream this pointer came from, so
        // the counts can only diverge on a corrupt stream. The old std::min() silently loaded
        // a subset — a half-valid table is worse than a loud failure.
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
        // Deliberately does NOT lazily load: identity is asked of a column this row group already
        // holds, and forcing a load would materialize the very object under test. A null answer
        // means "this row group has no column object here", which the caller must reject.
        return columns_[c].get();
    }

    uint64_t row_group_t::column_owner_count(uint64_t c) const {
        assert(c < columns_.size());
        // A live column is owned by at least one row group, so 0 can only mean "no object".
        return columns_[c] ? columns_[c]->use_count() : 0;
    }

    const row_version_manager_t* row_group_t::version_manager_identity() const {
        // The OWNING side. Deliberately does NOT create one on demand: a group that never
        // appended has no manager, and creating one here would be the gate manufacturing the
        // object it is meant to observe.
        return owned_version_info_.get();
    }

    const row_version_manager_t* row_group_t::version_manager_published() const {
        // The published side — exactly what the lock-free readers (version_info(),
        // committed_row_count, has_version_above, move_to_collection) see.
        return version_info_.load();
    }

    uint64_t row_group_t::version_manager_owner_count() const {
        // A live manager is owned by at least one row group, so 0 can only mean "no object".
        return owned_version_info_ ? static_cast<uint64_t>(owned_version_info_->use_count()) : 0;
    }
#endif

} // namespace components::table