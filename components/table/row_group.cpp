#include "row_group.hpp"

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
#include <components/vector/vector_operations.hpp>

namespace components::table {

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
    }

    std::vector<std::shared_ptr<column_data_t>>& row_group_t::columns() {
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
        std::lock_guard l(row_group_lock_);
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
            columns_.push_back(std::move(column_data));
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
        assert(!state.column_scans.empty());
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
        assert(!state.column_scans.empty());
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

    std::unique_ptr<row_group_t> row_group_t::add_column(collection_t* new_collection,
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
            // DDL ADD COLUMN backfill path (synchronous, not an actor append boundary). The append
            // chain reports out_of_memory; this constructor-style caller cannot return a
            // result_wrapper_t, so assert and stop the backfill on exhaustion. (A graceful DDL abort
            // would require the data_table ADD COLUMN constructors to be converted to return errors.)
            auto init = added_column->initialize_append(state);
            assert(!init.has_error() && "row_group::add_column: initialize_append OOM");
            for (uint64_t i = 0; i < rows_to_write && !init.has_error(); i += vector::DEFAULT_VECTOR_CAPACITY) {
                uint64_t rows_in_this_vector = std::min<uint64_t>(rows_to_write - i, vector::DEFAULT_VECTOR_CAPACITY);
                result.reference(fill_value);
                if (!default_value.has_value()) {
                    result.set_null(true);
                }
                auto appended = added_column->append(state, result, rows_in_this_vector);
                assert(!appended.has_error() && "row_group::add_column: append OOM");
                if (appended.has_error()) {
                    break;
                }
            }
        }

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        row_group->columns_ = columns();
        row_group->columns_.push_back(std::move(added_column));

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

    // Chunk layout for an expression_filter_t evaluation, cached for the duration of one
    // row-group scan: the referenced top-level columns, the padded type list and the one-row
    // chunk skeleton are pure functions of the immutable filter.column_paths and the column
    // types, so they are built once per scan instead of per candidate row. Keyed by filter
    // identity — the filter tree is owned by the scan state and stable while the scan runs.
    class expression_filter_layout_cache_t {
    public:
        struct layout_t {
            std::vector<size_t> referenced;
            std::pmr::vector<types::complex_logical_type> chunk_types;
            vector::data_chunk_t row;
        };

        explicit expression_filter_layout_cache_t(std::pmr::memory_resource* resource)
            : layouts_(resource) {}

        layout_t* find(const expression_filter_t* filter) {
            auto it = layouts_.find(filter);
            return it == layouts_.end() ? nullptr : &it->second;
        }

        layout_t& insert(const expression_filter_t* filter, layout_t&& layout) {
            return layouts_.emplace(filter, std::move(layout)).first->second;
        }

    private:
        std::pmr::unordered_map<const expression_filter_t*, layout_t> layouts_;
    };

    // A trailing subscript index into an ARRAY/LIST column (e.g. WHERE v[k] = x)
    // addresses an element, not a STRUCT sub-column. Materialize the row's list
    // value and compare the addressed (0-based) element against the filter.
    //
    // A NULL list, an out-of-range subscript, or a NULL element all yield a NULL
    // operand, so the comparison is UNKNOWN — not FALSE (see filter_match_t).
    static filter_match_t check_array_element_predicate(column_data_t& column,
                                                        int64_t row_id,
                                                        uint64_t element_index,
                                                        const table_filter_t* filter,
                                                        core::error_t& error) {
        column_fetch_state fetch_state;
        vector::vector_t result(column.resource(), column.type(), 1);
        column.fetch_row(fetch_state, row_id, result, 0);
        if (fetch_state.fetch_error.contains_error()) {
            error = fetch_state.fetch_error;
            return filter_match_t::no;
        }
        if (!result.validity().row_is_valid(0)) {
            return filter_match_t::unknown;
        }
        auto list_value = result.value(0);
        const auto& elements = list_value.children();
        if (element_index >= elements.size()) {
            return filter_match_t::unknown;
        }
        const auto& element_value = elements[element_index];
        if (element_value.is_null()) {
            return filter_match_t::unknown;
        }
        if (filter->filter_class == table_filter_type::SET_MEMBERSHIP) {
            return filter->cast<set_membership_filter_t>().contains(element_value) ? filter_match_t::yes
                                                                                   : filter_match_t::no;
        }
        if (filter->filter_class == table_filter_type::REGEX) {
            return filter->cast<regex_filter_t>().matches(element_value.value<std::string_view>()) ? filter_match_t::yes
                                                                                                   : filter_match_t::no;
        }
        return filter->cast<constant_filter_t>().compare(element_value) ? filter_match_t::yes : filter_match_t::no;
    }

    filter_match_t row_group_t::check_expression_predicate(int64_t row_id,
                                                           const expression_filter_t& filter,
                                                           expression_filter_layout_cache_t& expression_layouts,
                                                           core::error_t& error) {
        if (!filter.evaluator) {
            // Reached the per-row check without an agent-attached evaluator (see
            // expression_evaluator_t). Fail cleanly with an error instead of dereferencing null.
            error = core::error_t{core::error_code_t::physical_plan_error,
                                  std::pmr::string{"expression_filter_t reached check_predicate without an evaluator",
                                                   collection_->resource()}};
            return filter_match_t::no;
        }
        auto* res = collection_->resource();
        auto* layout = expression_layouts.find(&filter);
        if (!layout) {
            // Referenced top-level column indices (path[0]); the chunk must present each such column at
            // its own index so a value_getter's chunk.at(path) resolves it. Sub-paths (struct.field,
            // v[i]) are navigated inside the fetched top-level column value, so only path[0] is fetched.
            std::vector<size_t> referenced;
            size_t width = 0;
            for (const auto& path : filter.column_paths) {
                if (path.empty()) {
                    continue;
                }
                size_t top = path.front();
                width = std::max(width, top + 1);
                if (std::find(referenced.begin(), referenced.end(), top) == referenced.end()) {
                    referenced.push_back(top);
                }
            }
            // Build a chunk `width` columns wide so data[top] exists for every referenced column, but
            // (via the projected ctor) allocate real buffers ONLY for the referenced columns — the
            // padding columns keep index positions stable and are never read. A placeholder type is used
            // for padding so unreferenced columns are not force-loaded just to learn their type.
            std::pmr::vector<types::complex_logical_type> chunk_types{res};
            chunk_types.reserve(width);
            for (size_t i = 0; i < width; i++) {
                if (std::find(referenced.begin(), referenced.end(), i) != referenced.end()) {
                    chunk_types.push_back(get_column(i).type());
                } else {
                    chunk_types.emplace_back(types::logical_type::BOOLEAN);
                }
            }
            vector::data_chunk_t row{res, chunk_types, referenced, 1};
            layout =
                &expression_layouts.insert(&filter, {std::move(referenced), std::move(chunk_types), std::move(row)});
        }
        // fetch_row only flips validity valid->invalid (a reused slot would keep a previous row's
        // NULL sticky) and a LIST fetch appends to the child vector, so the referenced column
        // vectors are re-created fresh per row; the chunk skeleton, padding columns and layout
        // are reused across the scan.
        for (size_t top : layout->referenced) {
            layout->row.data[top] = vector::vector_t{res, layout->chunk_types[top], 1};
        }
        column_fetch_state fetch_state;
        for (size_t top : layout->referenced) {
            get_column(top).fetch_row(fetch_state, row_id, layout->row.data[top], 0);
            if (fetch_state.fetch_error.contains_error()) {
                error = fetch_state.fetch_error;
                return filter_match_t::no;
            }
        }
        layout->row.set_cardinality(1);
        auto checked = filter.evaluator->evaluate(layout->row, 0);
        if (checked.has_error()) {
            error = checked.error();
            return filter_match_t::no;
        }
        // The evaluator is three-valued (filter_match_t aliases types::tri_bool_t): an UNKNOWN
        // from a NULL operand flows through, so a NOT above this filter cannot resurrect the row.
        return checked.value();
    }

    // IS NULL / IS NOT NULL over an ARRAY/LIST element. Nullness is always TRUE or FALSE, never
    // UNKNOWN: an element is NULL when the whole cell is NULL, when the subscript is out of range,
    // or when that element itself is NULL.
    static filter_match_t check_array_element_is_null(column_data_t& column,
                                                      int64_t row_id,
                                                      uint64_t element_index,
                                                      bool want_null,
                                                      core::error_t& error) {
        column_fetch_state fetch_state;
        vector::vector_t result(column.resource(), column.type(), 1);
        column.fetch_row(fetch_state, row_id, result, 0);
        if (fetch_state.fetch_error.contains_error()) {
            error = fetch_state.fetch_error;
            return filter_match_t::no;
        }
        bool element_is_null;
        if (!result.validity().row_is_valid(0)) {
            element_is_null = true;
        } else {
            auto array_value = result.value(0);
            const auto& elements = array_value.children();
            element_is_null = element_index >= elements.size() || elements[element_index].is_null();
        }
        const bool matches = want_null ? element_is_null : !element_is_null;
        return matches ? filter_match_t::yes : filter_match_t::no;
    }

    filter_match_t row_group_t::check_predicate(int64_t row_id,
                                                const table_filter_t* filter,
                                                expression_filter_layout_cache_t& expression_layouts,
                                                core::error_t& error) {
        // An expression_filter_t (WHERE f(col) OP const) aliases a constant comparison filter_type
        // but has a different layout and its own multi-column evaluation, so intercept it BEFORE the
        // filter_type switch (which would mis-cast it to constant_filter_t in the default arm).
        if (filter->filter_class == table_filter_type::EXPRESSION) {
            return check_expression_predicate(row_id, filter->cast<expression_filter_t>(), expression_layouts, error);
        }
        switch (filter->filter_type) {
            case expressions::compare_type::union_or: {
                // OR fold via tri_or: TRUE dominates (a TRUE disjunct still rescues a NULL row),
                // UNKNOWN otherwise absorbs.
                auto& conjunction_or = filter->cast<conjunction_or_filter_t>();
                auto acc = filter_match_t::no;
                for (auto& child_filter : conjunction_or.child_filters) {
                    auto child = check_predicate(row_id, child_filter.get(), expression_layouts, error);
                    if (error.contains_error()) {
                        return filter_match_t::no;
                    }
                    acc = types::tri_or(acc, child);
                    if (acc == filter_match_t::yes) {
                        break; // TRUE dominates OR — no later child can change it
                    }
                }
                return acc;
            }
            case expressions::compare_type::union_and: {
                // AND fold via tri_and: FALSE dominates (a FALSE conjunct excludes decisively),
                // UNKNOWN otherwise absorbs.
                auto& conjunction_and = filter->cast<conjunction_and_filter_t>();
                auto acc = filter_match_t::yes;
                for (auto& child_filter : conjunction_and.child_filters) {
                    auto child = check_predicate(row_id, child_filter.get(), expression_layouts, error);
                    if (error.contains_error()) {
                        return filter_match_t::no;
                    }
                    acc = types::tri_and(acc, child);
                    if (acc == filter_match_t::no) {
                        break; // FALSE dominates AND — no later child can change it
                    }
                }
                return acc;
            }
            case expressions::compare_type::union_not: {
                // NOT over the disjunction of the children: tri_or the children, then tri_not.
                // Crucially NOT UNKNOWN is UNKNOWN — a row excluded because its operand was
                // NULL must not be flipped back in. This is what makes the validity gate in
                // column_data_t::check_predicate safe to add.
                auto& conjunction_not = filter->cast<conjunction_not_filter_t>();
                auto acc = filter_match_t::no;
                for (auto& child_filter : conjunction_not.child_filters) {
                    auto child = check_predicate(row_id, child_filter.get(), expression_layouts, error);
                    if (error.contains_error()) {
                        return filter_match_t::no;
                    }
                    acc = types::tri_or(acc, child);
                    if (acc == filter_match_t::yes) {
                        break; // the negation is already settled to FALSE
                    }
                }
                return types::tri_not(acc);
            }
            case expressions::compare_type::invalid: {
                assert(false && "invalid type for filter selection");
                std::abort();
            }
            case expressions::compare_type::is_null:
            case expressions::compare_type::is_not_null: {
                auto& null_filter = filter->cast<is_null_filter_t>();
                const bool want_null = filter->filter_type == expressions::compare_type::is_null;
                column_data_t* column = &get_column(null_filter.table_indices.front());
                for (size_t i = 1; i < null_filter.table_indices.size(); i++) {
                    // An intermediate ARRAY/LIST index addresses an element, not a STRUCT
                    // sub-column: descend it through the element accessor rather than casting the
                    // array column to a struct (which would dereference a bogus sub-columns entry).
                    if (column->type().type() == types::logical_type::ARRAY ||
                        column->type().type() == types::logical_type::LIST) {
                        return check_array_element_is_null(*column,
                                                           row_id,
                                                           null_filter.table_indices[i],
                                                           want_null,
                                                           error);
                    }
                    column =
                        static_cast<struct_column_data_t*>(column)->sub_columns[null_filter.table_indices[i]].get();
                }
                // IS NULL / IS NOT NULL are the two predicates that interrogate nullness
                // itself: they are always TRUE or FALSE, never UNKNOWN.
                bool is_valid = column->check_validity(row_id);
                bool matches = want_null ? !is_valid : is_valid;
                return matches ? filter_match_t::yes : filter_match_t::no;
            }
            default: {
                // Column-vs-column (`a.x OP a.y`): fetch both column values for this row and compare. A NULL
                // operand makes the comparison UNKNOWN (see filter_match_t). Checked before the
                // single-column dispatch below (a distinct multi-column filter type).
                if (filter->filter_class == table_filter_type::COLUMN_COLUMN) {
                    const auto& cc = filter->cast<column_column_filter_t>();
                    auto resolve = [&](const std::pmr::vector<uint64_t>& path) -> column_data_t* {
                        column_data_t* c = &get_column(path.front());
                        for (size_t i = 1; i < path.size(); i++) {
                            c = static_cast<struct_column_data_t*>(c)->sub_columns[path[i]].get();
                        }
                        return c;
                    };
                    column_data_t* lcol = resolve(cc.left_indices);
                    column_data_t* rcol = resolve(cc.right_indices);
                    column_fetch_state lstate, rstate;
                    vector::vector_t lvec(lcol->resource(), lcol->type(), 1);
                    vector::vector_t rvec(rcol->resource(), rcol->type(), 1);
                    lcol->fetch_row(lstate, row_id, lvec, 0);
                    if (lstate.fetch_error.contains_error()) {
                        error = lstate.fetch_error;
                        return filter_match_t::no;
                    }
                    rcol->fetch_row(rstate, row_id, rvec, 0);
                    if (rstate.fetch_error.contains_error()) {
                        error = rstate.fetch_error;
                        return filter_match_t::no;
                    }
                    if (!lvec.validity().row_is_valid(0) || !rvec.validity().row_is_valid(0)) {
                        return filter_match_t::unknown;
                    }
                    auto lval = lvec.value(0);
                    auto rval = rvec.value(0);
                    // Canonical comparator semantics (the shared helper simple_predicate's
                    // make_comparator also runs): bidirectional promotion with the session timezone
                    // the filter captured at plan time — never a one-way zero-tz cast that drops a
                    // row whose value merely overflows the narrower side's type. NULL operands never
                    // reach it (gated to unknown above).
                    auto matched = compare_values_promoting(lval, rval, cc.filter_type, cc.session_tz);
                    if (matched.has_error()) {
                        error = matched.error();
                        return filter_match_t::no;
                    }
                    return matched.value() ? filter_match_t::yes : filter_match_t::no;
                }
                // Works for both constant_filter_t and set_membership_filter_t.

                // A NULL on the CONSTANT side is UNKNOWN for every row, exactly as a NULL column
                // value is: `WHERE x > NULL` selects nothing, and must not be flipped in by NOT.
                // This is decided before the column is even read.
                if (filter->filter_class == table_filter_type::CONSTANT_COMPARISON &&
                    filter->cast<constant_filter_t>().constant.is_null()) {
                    return filter_match_t::unknown;
                }

                const auto& indices = table_filter_table_indices(filter);
                column_data_t* column = &get_column(indices.front());
                for (size_t i = 1; i < indices.size(); i++) {
                    if (column->type().type() == types::logical_type::ARRAY ||
                        column->type().type() == types::logical_type::LIST) {
                        return check_array_element_predicate(*column, row_id, indices[i], filter, error);
                    }
                    column = static_cast<struct_column_data_t*>(column)->sub_columns[indices[i]].get();
                }
                auto match = column->check_predicate(row_id, filter, error);

                // `x IN (1, NULL)`: a hit is TRUE, but a miss is UNKNOWN rather than FALSE, because
                // the NULL element might have been the match.
                if (match == filter_match_t::no) {
                    if (filter->filter_class == table_filter_type::SET_MEMBERSHIP) {
                        for (const auto& v : filter->cast<set_membership_filter_t>().values) {
                            if (v.is_null()) {
                                return filter_match_t::unknown;
                            }
                        }
                    }
                }
                return match;
            }
        }
    }

    bool row_group_t::check_zonemap_segments(collection_scan_state& state) {
        auto* f = state.filter();
        if (!f) {
            return true;
        }
        // An expression_filter_t has no single column/constant pair to prune against and its
        // filter_type aliases a constant comparison — never treat it as a constant_filter_t here
        // (its layout differs). Let every segment through to the per-row check_predicate.
        if (f->filter_class == table_filter_type::EXPRESSION) {
            return true;
        }
        // For constant comparison filters, check if any column's zonemap prunes this segment
        if (f->filter_type == expressions::compare_type::eq || f->filter_type == expressions::compare_type::gt ||
            f->filter_type == expressions::compare_type::gte || f->filter_type == expressions::compare_type::lt ||
            f->filter_type == expressions::compare_type::lte) {
            // Support both constant_filter_t and set_membership_filter_t for zonemap pruning.
            const auto& cf_indices = table_filter_table_indices(f);
            if (!cf_indices.empty()) {
                auto col_idx = cf_indices.front();
                if (col_idx < get_column_count()) {
                    auto& col = get_column(col_idx);
                    column_scan_state dummy;
                    auto result = col.check_zonemap(dummy, const_cast<table_filter_t&>(*f));
                    if (result == filter_propagate_result_t::ALWAYS_FALSE) {
                        next_vector(state);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    void row_group_t::filter_indexing(std::pmr::memory_resource* resource,
                                      uint64_t vector_index,
                                      vector::indexing_vector_t& indexing,
                                      const table_filter_t* filter,
                                      expression_filter_layout_cache_t& expression_layouts,
                                      uint64_t& approved_tuple_count,
                                      core::error_t& error) {
        vector::indexing_vector_t new_indexing(resource, approved_tuple_count);
        uint64_t result_count = 0;
        for (uint64_t i = 0; i < approved_tuple_count; i++) {
            auto idx = indexing.get_index(i);
            new_indexing.set_index(result_count, idx);
            // Only TRUE selects a row: UNKNOWN (a NULL operand) is excluded, exactly as FALSE is.
            auto match = check_predicate(static_cast<int64_t>(idx + vector_index * vector::DEFAULT_VECTOR_CAPACITY),
                                         filter,
                                         expression_layouts,
                                         error);
            result_count += (match == filter_match_t::yes) ? 1 : 0;
            if (error.contains_error()) {
                // OOM during predicate evaluation: stop; caller copies to scan_error.
                return;
            }
        }
        indexing = new_indexing;
        approved_tuple_count = result_count;
    }

    template<table_scan_type TYPE>
    void row_group_t::templated_scan(collection_scan_state& state, vector::data_chunk_t& result) {
        constexpr bool ALLOW_UPDATES = TYPE != table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES;
        const auto& column_ids = state.column_ids();
        auto* filter = state.filter();
        // Layouts for any expression_filter_t inside `filter`, built once for this row-group scan
        // and reused by every per-row check (see expression_filter_layout_cache_t).
        expression_filter_layout_cache_t expression_layouts{collection_->resource()};
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
                                    expression_layouts,
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
                                uint64_t result_idx) {
        for (uint64_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
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
                col_data.fetch_row(state, row_id, result_vector, result_idx);
            }
        }
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

    void row_group_t::revert_append(uint64_t row_group_start) {
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
        for (uint64_t c = 0; c < get_column_count(); c++) {
            get_column(c).revert_append(this->start + static_cast<int64_t>(row_group_start));
        }
        if (row_group_start < this->count.load()) {
            this->count = row_group_start;
        }
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
                return updated; // write_conflict / out_of_memory
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> row_group_t::update_column(vector::data_chunk_t& updates,
                                                            vector::vector_t& row_ids,
                                                            const std::vector<uint64_t>& column_path) {
        assert(updates.column_count() == 1);
        auto ids = row_ids.data<int64_t>();

        auto primary_column_idx = column_path[0];
        assert(primary_column_idx != std::numeric_limits<uint64_t>::max());
        assert(primary_column_idx < columns_.size());
        auto& col_data = get_column(primary_column_idx);
        return col_data.update_column(column_path, updates.data[0], ids, updates.size(), 1);
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

    bool row_group_t::has_unloaded_deletes() const {
        if (deletes_pointers_.empty()) {
            return false;
        }
        return !deletes_is_loaded_;
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
        const bool is_txn = transaction_id != 0;
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

    std::shared_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_ptr() {
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
        return indexing_vector(td, index, temp_indexing, count);
    }

    uint64_t row_group_t::indexing_vector(transaction_data txn,
                                          uint64_t vector_idx,
                                          vector::indexing_vector_t& indexing_vector,
                                          uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        return vinfo->indexing_vector(txn, vector_idx, indexing_vector, max_count);
    }

    std::shared_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_internal() {
        std::lock_guard lock(row_group_lock_);
        if (!owned_version_info_) {
            auto new_info = std::make_shared<row_version_manager_t>(start);
            set_version_info(std::move(new_info));
        }
        return owned_version_info_;
    }

    row_version_manager_t* row_group_t::version_info() {
        if (!has_unloaded_deletes()) {
            return version_info_;
        }
        std::lock_guard lock(row_group_lock_);
        if (!has_unloaded_deletes()) {
            return version_info_;
        }
        set_version_info(nullptr);
        deletes_is_loaded_ = true;
        return version_info_;
    }

    void row_group_t::set_version_info(std::shared_ptr<row_version_manager_t> version) {
        owned_version_info_ = std::move(version);
        version_info_ = owned_version_info_.get();
    }

    void version_delete_state::delete_row(int64_t row_id) {
        assert(row_id >= 0);
        uint64_t vector_idx = static_cast<uint64_t>(row_id) / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t idx_in_vector = static_cast<uint64_t>(row_id) - vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
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
    core::result_wrapper_t<storage::row_group_pointer_t>
    row_group_t::write_to_disk(storage::partial_block_manager_t& partial_block_manager) {
        storage::row_group_pointer_t pointer;
        pointer.row_start = static_cast<uint64_t>(start);
        pointer.tuple_count = count;

        auto col_count = get_column_count();
        pointer.data_pointers.resize(col_count);

        for (uint64_t i = 0; i < col_count; i++) {
            auto persistent = columns_[i]->checkpoint(partial_block_manager);
            if (persistent.has_error()) {
                return persistent.convert_error<storage::row_group_pointer_t>(); // out_of_memory
            }
            pointer.data_pointers[i] = std::move(persistent.value().data_pointers);
        }

        return pointer;
    }

    core::result_wrapper_t<bool> row_group_t::transition_to_disk() {
        // Own ONE partial_block_manager for this closed row group: ALL its columns' (and validity children's)
        // segments are PACKED together into shared blocks, so a row group of narrow columns shares a handful
        // of blocks instead of one block per segment (the ~127x over-allocation B2 fixes). In-memory tables
        // buffer nothing (transition_segment_to_disk early-returns), so the flush below is a harmless no-op.
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
        pbm.flush_partial_blocks();
        return true;
    }

    void row_group_t::create_from_pointer(const storage::row_group_pointer_t& pointer) {
        count = pointer.tuple_count;
        auto col_count = get_column_count();
        auto ptrs_count = pointer.data_pointers.size();
        auto min_count = std::min(col_count, static_cast<uint64_t>(ptrs_count));

        for (uint64_t i = 0; i < min_count; i++) {
            persistent_column_data_t pcd(columns_[i]->resource());
            pcd.data_pointers = pointer.data_pointers[i];
            columns_[i]->initialize_column(pcd);
        }
    }

} // namespace components::table