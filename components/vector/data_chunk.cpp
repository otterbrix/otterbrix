#include "data_chunk.hpp"
#include "vector_builders.hpp"
#include "vector_operations.hpp"

#include <algorithm>
#include <charconv>
#include <stdexcept>
#include <utility>

namespace {
    using namespace components;
    using namespace components::vector;

    void reset_validity_recursive(vector_t& vec) {
        vec.validity().reset();
        auto aux = vec.auxiliary();
        if (!aux) {
            return;
        }
        switch (aux->type()) {
            case vector_buffer_type::LIST:
                reset_validity_recursive(static_cast<list_vector_buffer_t*>(aux.get())->nested_data());
                break;
            case vector_buffer_type::ARRAY:
                reset_validity_recursive(static_cast<array_vector_buffer_t*>(aux.get())->nested_data());
                break;
            case vector_buffer_type::STRUCT:
                for (auto& entry : static_cast<struct_vector_buffer_t*>(aux.get())->entries()) {
                    reset_validity_recursive(*entry);
                }
                break;
            case vector_buffer_type::VECTOR_CHILD:
                reset_validity_recursive(static_cast<child_vector_buffer_t*>(aux.get())->nested_data());
                break;
            default:
                break;
        }
    }

    const vector_t* resolve_path(const std::vector<vector_t>& data, const std::pmr::vector<size_t>& col_path) {
        assert(!col_path.empty());
        assert(col_path.front() < data.size());
        const vector_t* sub_column = &data[col_path.front()];
        for (auto it = std::next(col_path.begin()); it != col_path.end(); ++it) {
            auto t = sub_column->type().type();
            if (t == types::logical_type::ARRAY || t == types::logical_type::LIST) {
                // Only a trailing subscript resolves — arr[i].field would index a vector that
                // doesn't exist, since the field lives once per element, not once per row.
                if (std::next(it) != col_path.end()) {
                    return nullptr;
                }
                return &sub_column->entry();
            }
            sub_column = sub_column->entries()[*it].get();
        }
        return sub_column;
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

    // NA-typed vectors are CONSTANT, not FLAT, so skipping them here keeps the FLAT-destination
    // assert in copy() from aborting on `INSERT ... VALUES (..., NULL)`.
    static bool is_null_typed(const vector_t& v) noexcept { return v.type().type() == types::logical_type::NA; }

    void data_chunk_t::reset() {
        if (data.empty()) {
            return;
        }
        capacity_ = DEFAULT_VECTOR_CAPACITY;
        set_cardinality(0);
        // validity_scan_partial ANDs bits into the mask, so a stale mask surviving reset() would
        // carry the previous fill's NULLs into the next one — nested masks and the heap leak too.
        for (auto& vec : data) {
            reset_validity_recursive(vec);
            vec.reset_string_heap();
        }
    }

    void data_chunk_t::destroy() {
        data.clear();
        capacity_ = 0;
        set_cardinality(0);
    }

    bool data_chunk_t::is_null(uint64_t col_idx, uint64_t index) const { return data[col_idx].is_null(index); }
    bool data_chunk_t::is_null(uint64_t col_idx, const std::pmr::vector<size_t>& path) const {
        return data[col_idx].is_null(path);
    }
    void data_chunk_t::set_null(uint64_t col_idx, const std::pmr::vector<size_t>& path, bool value) {
        data[col_idx].set_null(path, value);
    }

    const vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_path) const {
        return resolve_path(data, col_path);
    }

    vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_path) {
        return const_cast<vector_t*>(std::as_const(*this).at(col_path));
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
            if (is_null_typed(other.data[i]))
                continue;
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
            if (is_null_typed(other.data[i]))
                continue;
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

    core::result_wrapper_t<size_t> data_chunk_t::column_index(std::string_view key) const {
        for (uint64_t i = 0; i < column_count(); i++) {
            if (data[i].type().alias() == key) {
                return static_cast<size_t>(i);
            }
        }
        // Unknown name is user input, not an invariant — return an error, not a SIZE_MAX sentinel
        // that indexes out of bounds under NDEBUG.
        std::pmr::string message{resource_};
        message.append("data_chunk_t::column_index: no column named \"");
        message.append(key);
        message.append("\"");
        return core::error_t{core::error_code_t::field_not_exists, std::move(message)};
    }

    core::result_wrapper_t<std::pmr::vector<size_t>>
    data_chunk_t::sub_column_indices(const std::pmr::vector<std::pmr::string>& path) const {
        auto missing = [&](std::string_view segment) {
            std::pmr::string message{resource_};
            message.append("data_chunk_t::sub_column_indices: no column or field named \"");
            message.append(segment);
            message.append("\"");
            return core::error_t{core::error_code_t::field_not_exists, std::move(message)};
        };
        if (path.empty()) {
            std::pmr::string message{"data_chunk_t::sub_column_indices: empty path", resource_};
            return core::error_t{core::error_code_t::field_not_exists, std::move(message)};
        }
        std::pmr::vector<size_t> res(resource_);
        for (uint64_t i = 0; i < column_count(); i++) {
            if (core::pmr::operator==(data[i].type().alias(), path.front())) {
                res.emplace_back(i);
                break;
            }
        }
        if (res.empty()) {
            return missing(path.front());
        }
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
                return missing(*it);
            }
        }
        return res;
    }

    std::pmr::memory_resource* data_chunk_t::resource() const { return resource_; }

    void data_chunk_t::slice(const indexing_vector_t& indexing_vector, uint64_t count) {
        count_ = count;
        // Passes resource_ explicitly — a default-constructed pmr container would use the
        // process default resource, which this project forbids.
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
            result.data.emplace_back(data[c], offset, count);
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

    data_chunk_t adopted(std::pmr::memory_resource* resource, data_chunk_t&& chunk) {
        if (chunk.resource() == resource) {
            return std::move(chunk);
        }
        data_chunk_t own(resource, chunk.types(), std::clamp<uint64_t>(chunk.size(), 1, DEFAULT_VECTOR_CAPACITY));
        if (chunk.size() > own.capacity()) {
            own.resize(chunk.size());
        }
        chunk.copy(own, 0);
        return own;
    }

    core::result_wrapper_t<vector_t> compact_to_bool_value(std::pmr::memory_resource* resource,
                                                           const std::pmr::vector<data_chunk_t>& chunks,
                                                           const types::complex_logical_type&) {
        bool any = false;
        for (const auto& c : chunks) {
            if (!c.empty()) {
                any = true;
                break;
            }
        }
        return make_scalar(resource, any);
    }

    core::result_wrapper_t<vector_t> compact_to_single_value(std::pmr::memory_resource* resource,
                                                             const std::pmr::vector<data_chunk_t>& chunks,
                                                             const types::complex_logical_type& result_type) {
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
                    return make_constant_copy(resource, c.data[0]);
                }
            }
        }
        // No cell means SQL NULL, not an error -- covers an empty scalar sub-query and the zero-column
        // row an ungrouped aggregate emits when its filtered input matches nothing.
        if (total_rows == 0 || cols == 0) {
            return make_null(resource, result_type);
        }
        return core::error_t(core::error_code_t::conversion_failure,
                             std::pmr::string{"could not convert data_chunk_t to a single value", resource});
    }

    core::result_wrapper_t<vector_t> compact_to_list_value(std::pmr::memory_resource* resource,
                                                           const std::pmr::vector<data_chunk_t>& chunks,
                                                           const types::complex_logical_type& result_type) {
        // Gathers every row of every chunk, unbounded -- PostgreSQL treats `x IN (SELECT ...)` as
        // a semi-join with no row cap, not capped at chunk 0's <=1024.
        size_t total_rows = 0;
        const data_chunk_t* typed = nullptr;
        for (const auto& c : chunks) {
            total_rows += c.size();
            if (c.empty()) {
                continue;
            }
            if (c.column_count() != 1) {
                return core::error_t(core::error_code_t::conversion_failure,
                                     std::pmr::string{"could not convert data_chunk_t to a list value", resource});
            }
            if (typed == nullptr) {
                typed = &c;
            } else if (c.data[0].type() != typed->data[0].type()) {
                return core::error_t(
                    core::error_code_t::conversion_failure,
                    std::pmr::string{"could not convert data_chunk_t to a list value: the column changes its type "
                                     "between chunks",
                                     resource});
            }
        }
        if (total_rows == 0) {
            return make_list(resource, result_type.child_type(), {});
        }
        auto result = make_list(resource, typed->data[0].type(), {});
        auto* buffer = static_cast<list_vector_buffer_t*>(result.auxiliary().get());
        buffer->reserve(total_rows);
        for (const auto& c : chunks) {
            if (!c.empty()) {
                buffer->append(c.data[0], c.size());
            }
        }
        result.data<types::list_entry_t>()[0] = types::list_entry_t{0, total_rows};
        return result;
    }

} // namespace components::vector