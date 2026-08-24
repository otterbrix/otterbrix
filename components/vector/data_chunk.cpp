#include "data_chunk.hpp"
#include "vector_operations.hpp"

#include <algorithm>
#include <charconv>
#include <stdexcept>
#include <utility>

namespace {
    using namespace components;
    using namespace components::vector;

    struct resolved_path_t {
        const vector_t* array; // ARRAY/LIST the path ends up indexing; null unless it does
        size_t index;          // which of its elements, meaningful only alongside `array`
        const vector_t* leaf;
    };

    resolved_path_t resolve_path(const std::vector<vector_t>& data, const std::pmr::vector<size_t>& col_path) {
        if (col_path.front() >= data.size()) {
            return {.array = nullptr, .index = 0, .leaf = nullptr};
        }
        const vector_t* sub_column = &data[col_path.front()];
        for (auto it = std::next(col_path.begin()); it != col_path.end(); ++it) {
            auto t = sub_column->type().type();
            if (t == types::logical_type::ARRAY || t == types::logical_type::LIST) {
                // Only a trailing subscript resolves. Anything after it (arr[i].field) would
                // have to index a vector that does not exist: the field lives once per
                // element, not once per row.
                if (std::next(it) != col_path.end()) {
                    return {.array = nullptr, .index = 0, .leaf = nullptr};
                }
                return {.array = sub_column, .index = *it, .leaf = &sub_column->entry()};
            }
            sub_column = sub_column->entries()[*it].get();
        }
        return {.array = nullptr, .index = 0, .leaf = sub_column};
    }

    // Copy element `index` of every row out of an ARRAY/LIST into a contiguous vector, so
    // that index i means row i. Those values are strided in the flat child, hence the gather —
    // typed copies through vector_ops::copy, no per-element logical_value_t round-trip.
    vector_t align_to_rows(const vector_t& array, size_t index, uint64_t count, std::pmr::memory_resource* resource) {
        const vector_t& child = array.entry();
        const bool is_list = array.type().type() == types::logical_type::LIST;
        const size_t stride =
            is_list ? 0 : static_cast<const types::array_logical_type_extension*>(array.type().extension())->size();

        vector_t out(resource, child.type(), count);
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t pos;
            if (is_list) {
                const auto& offlen = array.data<types::list_entry_t>()[i];
                if (index >= offlen.length) {
                    out.validity().set_invalid(i);
                    continue;
                }
                pos = offlen.offset + index;
            } else {
                pos = i * stride + index;
            }
            vector_ops::copy(child, out, pos + 1, pos, i);
        }
        return out;
    }
} // namespace

namespace components::vector {
    data_chunk_t::data_chunk_t(std::pmr::memory_resource* resource,
                               const std::pmr::vector<types::complex_logical_type>& types,
                               uint64_t capacity)
        : resource_(resource)
        , capacity_(capacity)
        , row_ids(resource, types::logical_type::BIGINT, capacity) {
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
        , row_ids(resource, types::logical_type::BIGINT, capacity) {
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

    data_chunk_t::data_chunk_t(data_chunk_t&& other) noexcept
        : resource_(other.resource_)
        , count_(other.count_)
        , capacity_(other.capacity_)
        , row_ids(std::move(other.row_ids))
        , data(std::move(other.data)) {}

    data_chunk_t& data_chunk_t::operator=(data_chunk_t&& other) noexcept {
        resource_ = other.resource_;
        count_ = other.count_;
        capacity_ = other.capacity_;
        row_ids = std::move(other.row_ids);
        data = std::move(other.data);
        return *this;
    }

    // An unprojected placeholder vector has no data buffer AND no auxiliary buffer.
    // (ARRAY/STRUCT/LIST real vectors have auxiliary != nullptr even though data_ is null;
    // an NA column is a real column that allocates nothing at all.)
    // These exist to keep column indices stable when projected_scan skips columns.
    static bool is_unprojected_placeholder(const vector_t& v) noexcept {
        return v.type().type() != types::logical_type::NA && v.data() == nullptr && v.auxiliary() == nullptr;
    }

    uint64_t data_chunk_t::allocation_size() const {
        uint64_t total_size = 0;
        auto cardinality = size();
        for (auto& vec : data) {
            total_size += vec.allocation_size(cardinality);
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

    void data_chunk_t::set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val) {
        data[col_idx].set_value(index, val);
    }

    void data_chunk_t::set_value(const std::pmr::vector<size_t>& col_path,
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
                        return;
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

    const vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_path) const {
        return resolve_path(data, col_path).leaf;
    }

    vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_path) {
        return const_cast<vector_t*>(std::as_const(*this).at(col_path));
    }

    data_chunk_t::at_aligned_t data_chunk_t::at_aligned(const std::pmr::vector<size_t>& col_path,
                                                        std::pmr::memory_resource* resource) const {
        auto resolved = resolve_path(data, col_path);
        if (resolved.array) {
            return at_aligned_t{align_to_rows(*resolved.array, resolved.index, size(), resource)};
        }
        return at_aligned_t{resolved.leaf};
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
        }
        row_ids.reference(chunk.row_ids);
    }

    void data_chunk_t::copy(data_chunk_t& other, uint64_t offset) const {
        assert(column_count() == other.column_count());
        assert(other.size() == 0);

        for (uint64_t i = 0; i < column_count(); i++) {
            if (is_unprojected_placeholder(data[i]))
                continue;
            // There's nothing to copy for vector of NULLs
            if (data[i].type().type() == types::logical_type::NA)
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
            if (is_unprojected_placeholder(data[i]))
                continue;
            // There's nothing to copy for vector of NULLs
            if (data[i].type().type() == types::logical_type::NA)
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

    size_t data_chunk_t::column_index(std::string_view key) const {
        for (uint64_t i = 0; i < column_count(); i++) {
            if (data[i].type().alias() == key) {
                return i;
            }
        }
        assert(false && "data_chunk_t::column_index: no such column");
        return std::numeric_limits<size_t>::max();
    }

    std::pmr::vector<size_t> data_chunk_t::sub_column_indices(const std::pmr::vector<std::pmr::string>& path) const {
        std::pmr::vector<size_t> res(resource_);
        for (uint64_t i = 0; i < column_count(); i++) {
            if (core::pmr::operator==(data[i].type().alias(), path.front())) {
                res.emplace_back(i);
                break;
            }
        }
        if (res.empty()) {
            assert(false && "data_chunk_t::column_index: no such column");
            return {size_t(-1)};
        } else {
            const vector_t* sub_column = &data[res.front()];
            for (auto it = std::next(path.begin()); it != path.end(); ++it) {
                bool field_found = false;
                if (sub_column->type().type() == types::logical_type::ARRAY) {
                    size_t index{};
                    auto [p, ec] = std::from_chars(it->data(), it->data() + it->size(), index);
                    if (ec == std::errc{} &&
                        index < static_cast<const types::array_logical_type_extension*>(sub_column->type().extension())
                                    ->size()) {
                        res.emplace_back(index);
                        sub_column = &sub_column->entry();
                        field_found = true;
                    }
                } else {
                    for (uint64_t i = 0; i < sub_column->type().child_types().size(); i++) {
                        if (core::pmr::operator==(sub_column->type().child_types()[i].alias(), *it)) {
                            res.emplace_back(i);
                            if (std::next(it) != path.end()) {
                                sub_column = sub_column->entries()[i].get();
                            }
                            field_found = true;
                            break;
                        }
                    }
                }
                if (!field_found) {
                    return {size_t(-1)};
                }
            }
        }
        return res;
    }

    std::pmr::memory_resource* data_chunk_t::resource() const { return resource_; }

    void data_chunk_t::slice(const indexing_vector_t& indexing_vector, uint64_t count) {
        count_ = count;
        // Explicit resource: a default-constructed pmr container would allocate on the process
        // default resource, which this project forbids.
        indexing_cache_t merge_cache{resource_};
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
        indexing_cache_t merge_cache{resource_};
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
        // filtered out (e.g. SELECT MAX(x) ... WHERE <no match> → one row, no column). Yielding an untyped
        // NA null matches the value get_parameter() returns for an unbound id, so `x = (NULL scalar
        // subquery)` compares against NULL and selects nothing. Only a genuine shape violation (>1 row, or
        // >1 column) falls through to the error.
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