#include "data_chunk.hpp"
#include "vector_operations.hpp"

#include <algorithm>
#include <stdexcept>

namespace components::vector {

    data_chunk_t::data_chunk_t(std::pmr::memory_resource* resource,
                               const std::pmr::vector<types::complex_logical_type>& types,
                               uint64_t capacity)
        : resource_(resource)
        , capacity_(capacity)
        , row_ids(resource, types::logical_type::BIGINT, capacity)
        , schema_(resource) {
        assert(capacity <= DEFAULT_VECTOR_CAPACITY);
        for (uint64_t i = 0; i < types.size(); i++) {
            data.emplace_back(resource_, types[i], capacity_);
        }
    }

    data_chunk_t::data_chunk_t(std::pmr::memory_resource* resource,
                               const std::pmr::vector<types::complex_logical_type>& all_types,
                               const std::vector<size_t>& projected_cols,
                               uint64_t capacity)
        : resource_(resource)
        , capacity_(capacity)
        , row_ids(resource, types::logical_type::BIGINT, capacity)
        , schema_(resource) {
        assert(capacity <= DEFAULT_VECTOR_CAPACITY);
        // Build a fast lookup: which column indices need real buffers
        std::vector<bool> needed(all_types.size(), false);
        for (size_t idx : projected_cols) {
            if (idx < all_types.size()) {
                needed[idx] = true;
            }
        }
        data.reserve(all_types.size());
        for (size_t i = 0; i < all_types.size(); i++) {
            if (needed[i]) {
                data.emplace_back(resource_, all_types[i], capacity_);
            } else {
                // Placeholder: type info only, no buffer allocation
                data.emplace_back(resource_, all_types[i], false, false, 0);
            }
        }
    }

    // The schema memo is derived from `data`, so a move deliberately does not carry it: the
    // moved-to chunk rebuilds it from the columns it just took over, on its next read.
    data_chunk_t::data_chunk_t(data_chunk_t&& other) noexcept
        : resource_(other.resource_)
        , count_(other.count_)
        , capacity_(other.capacity_)
        , row_ids(std::move(other.row_ids))
        , data(std::move(other.data))
        , schema_(other.resource_) {}

    data_chunk_t& data_chunk_t::operator=(data_chunk_t&& other) noexcept {
        // Drop the memo BEFORE resource_ is reseated. Its storage came from this chunk's
        // current resource and has to be returned there — not to the one being adopted —
        // so the release is a destructor call, which is the only way to get that guarantee
        // out of a container whose allocator does not propagate on move assignment.
        schema_.~schema_storage_t();
        ::new (static_cast<void*>(&schema_)) schema_storage_t{other.resource_};
        resource_ = other.resource_;
        count_ = other.count_;
        capacity_ = other.capacity_;
        row_ids = std::move(other.row_ids);
        data = std::move(other.data);
        return *this;
    }

    column_schema_t column_schema_t::clone(std::pmr::memory_resource* resource) const {
        column_schema_t copy{resource};
        copy.attoid = attoid;
        copy.name.assign(name.data(), name.size());
        copy.type = type;
        return copy;
    }

    void data_chunk_t::sync_schema() const {
        while (schema_.size() > data.size()) {
            schema_.pop_back();
        }
        if (schema_.size() < data.size()) {
            schema_.reserve(data.size());
            while (schema_.size() < data.size()) {
                schema_.emplace_back(resource_);
            }
        }
        for (uint64_t i = 0; i < data.size(); i++) {
            const auto& column_type = data[i].type();
            // vector_t::name() is the column's own answer and needs no guard in front of it —
            // an unnamed column returns an empty view (M3-B5).
            const std::string_view column_name = data[i].name();
            auto& record = schema_[i];
            // M3-B4: the identity is DERIVED, like the name and the type, and assigned
            // OUTSIDE the guard below. It cannot ride inside it: two chunk columns may be
            // fully type-equal and still be different columns (duplicate names are legal —
            // test_computed_schema.cpp), so after operator_group.cpp erases a positional
            // range out of the middle, the column that slides into slot i can compare equal
            // to the record already there while carrying a different attoid. A guarded
            // assignment would leave the old column's identity describing the new one. A
            // 4-byte store is cheaper than the comparison that would protect it.
            record.attoid = data[i].attoid();
            // Two questions, two checks: a type comparison cannot see a rename (the name is
            // not on the type — M3-B5), so the name is reconciled against the column's own
            // name. Each guard is here to skip a copy, not to decide anything — assigning
            // both unconditionally would be correct and slower, since schema() is read per
            // chunk on hot paths.
            if (record.name != column_name) {
                record.name.assign(column_name.data(), column_name.size());
            }
            if (record.type != column_type) {
                record.type = column_type;
            }
        }
    }

    const std::pmr::vector<column_schema_t>& data_chunk_t::schema() const {
        sync_schema();
        return schema_;
    }

    void data_chunk_t::set_column_name(uint64_t col_idx, std::string_view name) {
        assert(col_idx < data.size());
        data[col_idx].set_name(name);
    }

    void data_chunk_t::set_column_attoid(uint64_t col_idx, catalog::oid_t attoid) {
        assert(col_idx < data.size());
        data[col_idx].set_attoid(attoid);
    }

    std::pmr::vector<types::complex_logical_type> schema_types(std::pmr::memory_resource* resource,
                                                               const schema_t& schema) {
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(schema.size());
        for (const auto& record : schema) {
            types.push_back(record.type);
        }
        return types;
    }

    schema_t clone_schema(std::pmr::memory_resource* resource, const schema_t& schema) {
        schema_t copy(resource);
        copy.reserve(schema.size());
        for (const auto& record : schema) {
            copy.push_back(record.clone(resource));
        }
        return copy;
    }

    // Stamp the identity the constructors could not: the chunk was built out of types, and
    // both halves of what a column IS live on the record.
    static void stamp_schema(data_chunk_t& chunk, const schema_t& schema) {
        const auto width = std::min<uint64_t>(chunk.column_count(), schema.size());
        for (uint64_t i = 0; i < width; i++) {
            if (chunk.data[i].name() != std::string_view{schema[i].name}) {
                chunk.set_column_name(i, schema[i].name);
            }
            chunk.set_column_attoid(i, schema[i].attoid);
        }
    }

    data_chunk_t make_chunk(std::pmr::memory_resource* resource, const schema_t& schema, uint64_t capacity) {
        data_chunk_t chunk{resource, schema_types(resource, schema), capacity};
        stamp_schema(chunk, schema);
        return chunk;
    }

    data_chunk_t make_chunk(std::pmr::memory_resource* resource,
                            const schema_t& schema,
                            const std::vector<size_t>& projected_cols,
                            uint64_t capacity) {
        data_chunk_t chunk{resource, schema_types(resource, schema), projected_cols, capacity};
        stamp_schema(chunk, schema);
        return chunk;
    }

    const types::complex_logical_type& type_from_path(const schema_t& schema,
                                                      const std::pmr::vector<size_t>& path) {
        assert(!schema.empty() && "vector::type_from_path should not be called with an empty schema");
        assert(!path.empty() && "vector::type_from_path should not be called with an empty path");
        return schema.at(path.front()).type.type_from_path_tail(path);
    }

    // An unprojected placeholder vector has no data buffer AND no auxiliary buffer.
    // (ARRAY/STRUCT/LIST real vectors have auxiliary != nullptr even though data_ is null.)
    // These exist to keep column indices stable when projected_scan skips columns.
    static bool is_unprojected_placeholder(const vector_t& v) noexcept {
        return v.data() == nullptr && v.auxiliary() == nullptr;
    }

    core::result_wrapper_t<uint64_t> data_chunk_t::allocation_size() const {
        uint64_t total_size = 0;
        auto cardinality = size();
        for (auto& vec : data) {
            auto column_size = vec.allocation_size(cardinality);
            if (column_size.has_error()) {
                return column_size;
            }
            total_size += column_size.value();
        }
        return total_size;
    }

    void data_chunk_t::reset() {
        if (data.empty()) {
            return;
        }
        capacity_ = DEFAULT_VECTOR_CAPACITY;
        set_cardinality(0);
    }

    void data_chunk_t::drop_unprojected_placeholders() {
        data.erase(
            std::remove_if(data.begin(), data.end(), [](const vector_t& v) { return is_unprojected_placeholder(v); }),
            data.end());
    }

    void data_chunk_t::destroy() {
        data.clear();
        capacity_ = 0;
        set_cardinality(0);
    }

    types::logical_value_t data_chunk_t::value(uint64_t col_idx, uint64_t index) const {
        assert(index < size());
        return data[col_idx].value(index);
    }

    types::logical_value_t data_chunk_t::value(const std::pmr::vector<size_t>& col_path, uint64_t index) const {
        auto element = data[col_path.front()].resolve_nested_element(index, col_path, 1);
        if (element.is_null) {
            return types::logical_value_t{element.leaf->resource(),
                                          types::complex_logical_type{types::logical_type::NA}};
        }
        return element.leaf->value(element.index);
    }

    core::error_t data_chunk_t::set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val) {
        return data[col_idx].set_value(index, val);
    }

    core::error_t data_chunk_t::set_value(const std::pmr::vector<size_t>& col_path,
                                          uint64_t index,
                                          const types::logical_value_t& val) {
        vector_t* sub_column = &data[col_path.front()];
        for (auto it = std::next(col_path.begin()); it != col_path.end(); ++it) {
            if (std::next(it) == col_path.end()) {
                if (sub_column->type().type() == types::logical_type::ARRAY) {
                    auto stride =
                        static_cast<const types::array_logical_type_extension*>(sub_column->type().extension())->size();
                    return sub_column->entry().set_value(index * stride + *it, val);
                } else if (sub_column->type().type() == types::logical_type::LIST) {
                    // Mutate element *it of row `index` in place through the row's
                    // (offset,length) entry; out-of-range indices are a no-op (LIST
                    // has no fixed width to grow into here).
                    const auto& offlen = sub_column->data<types::list_entry_t>()[index];
                    if (*it >= offlen.length) {
                        return core::error_t::no_error();
                    }
                    return sub_column->entry().set_value(offlen.offset + *it, val);
                } else {
                    return sub_column->entries()[*it]->set_value(index, val);
                }
            } else {
                sub_column = sub_column->entries()[*it].get();
            }
        }
        return sub_column->set_value(index, val);
    }

    bool data_chunk_t::is_null(uint64_t col_idx, uint64_t index) const { return data[col_idx].is_null(index); }
    bool data_chunk_t::is_null(uint64_t col_idx, const std::pmr::vector<size_t>& path) const {
        return data[col_idx].is_null(path);
    }
    void data_chunk_t::set_null(uint64_t col_idx, const std::pmr::vector<size_t>& path, bool value) {
        data[col_idx].set_null(path, value);
    }

    vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_path) {
        // A top-level ordinal past the chunk's width is "column not found" — the same
        // nullptr contract callers already handle for unresolvable nested paths.
        if (col_path.front() >= data.size()) {
            return nullptr;
        }
        vector_t* sub_column = &data[col_path.front()];
        for (auto it = std::next(col_path.begin()); it != col_path.end(); ++it) {
            if (sub_column->type().type() == types::logical_type::ARRAY ||
                sub_column->type().type() == types::logical_type::LIST) {
                sub_column = &sub_column->entry();
            } else {
                sub_column = sub_column->entries()[*it].get();
            }
        }
        return sub_column;
    }

    const vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_path) const {
        // Same "column not found" -> nullptr contract as the non-const overload.
        if (col_path.front() >= data.size()) {
            return nullptr;
        }
        const vector_t* sub_column = &data[col_path.front()];
        for (auto it = std::next(col_path.begin()); it != col_path.end(); ++it) {
            if (sub_column->type().type() == types::logical_type::ARRAY ||
                sub_column->type().type() == types::logical_type::LIST) {
                sub_column = &sub_column->entry();
            } else {
                sub_column = sub_column->entries()[*it].get();
            }
        }
        return sub_column;
    }

    bool data_chunk_t::all_constant() const {
        for (auto& v : data) {
            if (v.get_vector_type() != vector_type::CONSTANT) {
                return false;
            }
        }
        return true;
    }

    void data_chunk_t::reference(data_chunk_t& chunk) {
        assert(chunk.column_count() <= column_count());
        set_capacity(chunk.capacity_);
        set_cardinality(chunk.count_);
        for (uint64_t i = 0; i < chunk.column_count(); i++) {
            data[i].reference(chunk.data[i]);
            data[i].set_attoid(chunk.data[i].attoid());
            data[i].set_name(chunk.data[i].name());
        }
        row_ids.reference(chunk.row_ids);
    }

    // M3-B4. Column i of the destination IS column i of the source — the assert below says
    // so — and the engine's standard way to duplicate a chunk is to build a fresh one from
    // types() and copy into it (operator_insert.cpp's copy_of, manager_disk_impl.hpp's
    // rebuild_chunk, partial_copy here). types() carries neither the identity nor the NAME
    // (M3-B5) — a chunk rebuilt from types() starts nameless. Both halves therefore travel
    // here, from the source COLUMN, or every rebuild would silently strip them.
    void data_chunk_t::copy(data_chunk_t& other, uint64_t offset) const {
        assert(column_count() == other.column_count());
        assert(other.size() == 0);

        for (uint64_t i = 0; i < column_count(); i++) {
            other.data[i].set_attoid(data[i].attoid());
            other.data[i].set_name(data[i].name());
            if (is_unprojected_placeholder(data[i]))
                continue;
            assert(other.data[i].get_vector_type() == vector_type::FLAT);
            vector_ops::copy(data[i], other.data[i], size(), offset, 0);
        }
        assert(other.row_ids.get_vector_type() == vector_type::FLAT);
        vector_ops::copy(row_ids, other.row_ids, size(), offset, 0);
        other.set_cardinality(size() - offset);
    }

    void data_chunk_t::copy(data_chunk_t& other,
                            const indexing_vector_t& indexing,
                            uint64_t source_count,
                            uint64_t offset) const {
        assert(column_count() == other.column_count());
        assert(other.size() == 0);
        assert(source_count <= size());

        for (uint64_t i = 0; i < column_count(); i++) {
            other.data[i].set_attoid(data[i].attoid());
            other.data[i].set_name(data[i].name());
            if (is_unprojected_placeholder(data[i]))
                continue;
            assert(other.data[i].get_vector_type() == vector_type::FLAT);
            vector_ops::copy(data[i], other.data[i], indexing, source_count, offset, 0);
        }
        assert(other.row_ids.get_vector_type() == vector_type::FLAT);
        vector_ops::copy(row_ids, other.row_ids, indexing, source_count, offset, 0);
        other.set_cardinality(source_count - offset);
    }

    void data_chunk_t::split(data_chunk_t& other, uint64_t split_idx) {
        assert(other.size() == 0);
        assert(other.data.empty());
        assert(split_idx < data.size());
        uint64_t num_cols = data.size();
        for (uint64_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
            other.data.push_back(std::move(data[col_idx]));
        }
        for (uint64_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
            data.pop_back();
        }
        vector_ops::copy(row_ids, other.row_ids, size(), 0, 0);
        other.set_capacity(capacity_);
        other.set_cardinality(count_);
    }

    void data_chunk_t::fuse(data_chunk_t&& other) {
        assert(other.size() == size());
        uint64_t num_cols = other.data.size();
        for (uint64_t col_idx = 0; col_idx < num_cols; ++col_idx) {
            data.emplace_back(std::move(other.data[col_idx]));
        }
        other.destroy();
    }

    void data_chunk_t::reference_columns(data_chunk_t& other, const std::vector<uint64_t>& column_ids) {
        assert(column_count() == column_ids.size());
        reset();
        for (uint64_t col_idx = 0; col_idx < column_count(); col_idx++) {
            auto& other_col = other.data[column_ids[col_idx]];
            auto& this_col = data[col_idx];
            assert(other_col.type() == this_col.type());
            this_col.reference(other_col);
            this_col.set_attoid(other_col.attoid());
            this_col.set_name(other_col.name());
        }
        set_cardinality(other.size());
    }

    void data_chunk_t::flatten() {
        for (uint64_t i = 0; i < column_count(); i++) {
            data[i].flatten(size());
        }
    }

    std::pmr::vector<types::complex_logical_type> data_chunk_t::types() const {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        for (uint64_t i = 0; i < column_count(); i++) {
            types.push_back(data[i].type());
        }
        return types;
    }

    std::pmr::memory_resource* data_chunk_t::resource() const { return resource_; }

    void data_chunk_t::slice(const indexing_vector_t& indexing_vector, uint64_t count) {
        count_ = count;
        indexing_cache_t merge_cache;
        for (uint64_t c = 0; c < column_count(); c++) {
            data[c].slice(indexing_vector, count, merge_cache);
        }
    }

    void data_chunk_t::slice(const data_chunk_t& other,
                             const indexing_vector_t& indexing,
                             uint64_t count,
                             uint64_t col_offset) {
        assert(other.column_count() <= col_offset + column_count());
        count_ = count;
        indexing_cache_t merge_cache;
        for (uint64_t c = 0; c < other.column_count(); c++) {
            if (other.data[c].get_vector_type() == vector_type::DICTIONARY) {
                // already a dictionary! merge the dictionaries
                data[col_offset + c].reference(other.data[c]);
                data[col_offset + c].slice(indexing, count, merge_cache);
            } else {
                data[col_offset + c].slice(other.data[c], indexing, count);
            }
        }
    }

    void data_chunk_t::slice(std::pmr::memory_resource* resource, uint64_t offset, uint64_t slice_count) {
        assert(offset + slice_count <= size());
        indexing_vector_t indexing(resource, offset, slice_count);
        slice(indexing, slice_count);
    }

    data_chunk_t
    data_chunk_t::partial_copy(std::pmr::memory_resource* resource, uint64_t offset, uint64_t count) const {
        assert(offset + count <= size());
        data_chunk_t result(resource, std::pmr::vector<types::complex_logical_type>{resource}, 0);
        result.capacity_ = count;
        result.count_ = count;
        result.data.reserve(column_count());
        for (uint64_t c = 0; c < column_count(); c++) {
            if (is_unprojected_placeholder(data[c])) {
                // Preserve placeholder status — slicing nullptr would produce a bogus offset pointer.
                result.data.emplace_back(resource, data[c].type(), false, false, 0);
            } else {
                result.data.emplace_back(data[c], offset, count);
            }
        }
        result.row_ids = vector_t(resource, types::logical_type::BIGINT, count);
        if (count > 0) {
            vector::vector_ops::copy(row_ids, result.row_ids, offset + count, offset, 0);
        }
        return result;
    }

    std::vector<unified_vector_format> data_chunk_t::to_unified_format(std::pmr::memory_resource* resource) {
        std::vector<unified_vector_format> unified_data;
        unified_data.reserve(column_count());
        for (uint64_t col_idx = 0; col_idx < column_count(); col_idx++) {
            unified_data.emplace_back(resource, size());
            data[col_idx].to_unified_format(size(), unified_data[col_idx]);
        }
        return unified_data;
    }

    void data_chunk_t::hash(vector_t& result) {
        assert(result.type().type() == types::logical_type::UBIGINT);
        vector_ops::hash(data[0], result, size());
        for (uint64_t i = 1; i < column_count(); i++) {
            vector_ops::combine_hash(result, data[i], size());
        }
    }

    void data_chunk_t::hash(std::vector<uint64_t>& column_ids, vector_t& result) {
        assert(result.type().type() == types::logical_type::UBIGINT);
        assert(!column_ids.empty());

        vector_ops::hash(data[column_ids[0]], result, size());
        for (uint64_t i = 1; i < column_ids.size(); i++) {
            vector_ops::combine_hash(result, data[column_ids[i]], size());
        }
    }

    void data_chunk_t::resize(uint64_t new_size) {
        if (new_size > count_) {
            new_size = is_power_of_two(new_size) ? new_size * 2 : next_power_of_two(new_size);
        }
        for (auto& column : data) {
            if (is_unprojected_placeholder(column))
                continue;
            column.resize(capacity_, new_size);
        }
        row_ids.resize(capacity_, new_size);
        capacity_ = new_size;
        if (count_ > new_size) {
            count_ = new_size;
        }
    }

    void validate_chunk_capacity(vector::data_chunk_t& chunk, size_t filled_size) {
        if (filled_size >= chunk.capacity()) {
            chunk.resize(filled_size);
        }
    }

    core::result_wrapper_t<types::logical_value_t> compact_to_bool_value(const std::pmr::vector<data_chunk_t>& chunks) {
        // EXISTS: true iff ANY chunk carries a row (a multi-chunk / multi-branch result must not be
        // judged empty from chunk 0 alone).
        bool any = false;
        for (const auto& c : chunks) {
            if (!c.empty()) {
                any = true;
                break;
            }
        }
        return types::logical_value_t{chunks.front().resource(), any};
    }

    core::result_wrapper_t<types::logical_value_t>
    compact_to_single_value(const std::pmr::vector<data_chunk_t>& chunks) {
        // Count rows across ALL chunks — a scalar sub-query returning 2 rows may split across chunks, and
        // that must still error (">1 row"), not silently take chunk 0's single cell.
        size_t total_rows = 0;
        size_t cols = 0;
        for (const auto& c : chunks) {
            total_rows += c.size();
            if (c.column_count() > cols) {
                cols = c.column_count();
            }
        }
        if (cols == 1 && total_rows == 1) {
            for (const auto& c : chunks) {
                if (c.size() == 1) {
                    return c.value(0, 0);
                }
            }
        }
        // No extractable value cell → SQL NULL, not an error. This covers a scalar sub-query that returned
        // zero rows AND the degenerate zero-column result an ungrouped aggregate emits when its input was
        // filtered out (e.g. SELECT MAX(x) ... WHERE <no match> → one row, no column). The untyped NA null
        // yielded here makes `x = (NULL scalar subquery)` compare against NULL and select nothing. Only a
        // genuine shape violation (>1 row, or >1 column) falls through to the error.
        if (total_rows == 0 || cols == 0) {
            return types::logical_value_t{chunks.front().resource(), nullptr};
        }
        return core::error_t(
            core::error_code_t::conversion_failure,
            std::pmr::string{"could not convert data_chunk_t to a single value", chunks.front().resource()});
    }

    core::result_wrapper_t<types::logical_value_t>
    compact_to_array_value(const std::pmr::vector<data_chunk_t>& chunks) {
        // IN / ANY / ALL list: gather EVERY row of EVERY chunk (unbounded — PostgreSQL treats
        // `x IN (SELECT ...)` as a semi-join with no fixed row cap), not just chunk 0's ≤1024.
        size_t total_rows = 0;
        for (const auto& c : chunks) {
            total_rows += c.size();
        }
        if (total_rows == 0) {
            // Empty sub-query (e.g. `x IN (SELECT ... WHERE false)`): PostgreSQL treats
            // this as an empty semi-join — `IN ()` matches nothing, `NOT IN ()` matches
            // everything — NOT a type error. Return the SAME NA-null sentinel a zero-row
            // scalar sub-query returns (compact_to_single_value above); the ANY/ALL
            // evaluator special-cases the null array (no empty-array value built).
            return types::logical_value_t{chunks.front().resource(), nullptr};
        }
        std::vector<types::logical_value_t> array;
        array.reserve(total_rows);
        const data_chunk_t* typed = nullptr; // first non-empty chunk — carries the element type
        for (const auto& c : chunks) {
            if (c.empty()) {
                continue;
            }
            if (c.column_count() != 1) {
                return core::error_t(
                    core::error_code_t::conversion_failure,
                    std::pmr::string{"could not convert data_chunk_t to a array value", chunks.front().resource()});
            }
            if (typed == nullptr) {
                typed = &c;
            }
            for (size_t i = 0; i < c.size(); ++i) {
                array.emplace_back(c.value(0, i));
            }
        }
        return types::logical_value_t::create_array(chunks.front().resource(), typed->data[0].type(), array);
    }

    core::result_wrapper_t<types::logical_value_t> compact_to_row_value(const std::pmr::vector<data_chunk_t>& chunks) {
        size_t total_rows = 0;
        for (const auto& c : chunks) {
            total_rows += c.size();
        }
        if (total_rows == 1) {
            for (const auto& c : chunks) {
                if (c.size() == 1) {
                    std::vector<types::logical_value_t> fields;
                    fields.reserve(c.column_count());
                    for (size_t i = 0; i < c.column_count(); ++i) {
                        fields.emplace_back(c.value(i, 0));
                    }
                    return types::logical_value_t::create_struct(c.resource(), "", fields);
                }
            }
        }
        return core::error_t(
            core::error_code_t::conversion_failure,
            std::pmr::string{"could not convert data_chunk_t to a row value", chunks.front().resource()});
    }

} // namespace components::vector