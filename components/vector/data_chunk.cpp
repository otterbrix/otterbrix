#include "data_chunk.hpp"
#include "vector_operations.hpp"
#include <components/serialization/deserializer.hpp>

#include <stdexcept>

namespace components::vector {

    data_chunk_t::data_chunk_t(std::pmr::memory_resource* resource,
                               const std::pmr::vector<types::complex_logical_type>& types,
                               uint64_t capacity)
        : resource_(resource)
        , capacity_(capacity)
        , row_ids(resource, types::logical_type::BIGINT, capacity) {
        for (uint64_t i = 0; i < types.size(); i++) {
            data.emplace_back(resource_, types[i], capacity_);
        }
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

    void data_chunk_t::destroy() {
        data.clear();
        capacity_ = 0;
        set_cardinality(0);
    }

    types::logical_value_t data_chunk_t::value(uint64_t col_idx, uint64_t index) const {
        assert(index < size());
        return data[col_idx].value(index);
    }
    types::logical_value_t data_chunk_t::value(const std::pmr::vector<size_t>& col_indices, uint64_t index) const {
        const vector_t* sub_column = &data[col_indices.front()];
        for (auto it = std::next(col_indices.begin()); it != col_indices.end(); ++it) {
            if (std::next(it) == col_indices.end()) {
                return sub_column->entries()[*it]->value(index);
            } else {
                sub_column = sub_column->entries()[*it].get();
            }
        }
        return sub_column->value(index);
    }

    void data_chunk_t::set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val) {
        data[col_idx].set_value(index, val);
    }

    void data_chunk_t::set_value(const std::pmr::vector<size_t>& col_indices,
                                 uint64_t index,
                                 const types::logical_value_t& val) {
        vector_t* sub_column = &data[col_indices.front()];
        for (auto it = std::next(col_indices.begin()); it != col_indices.end(); ++it) {
            if (std::next(it) == col_indices.end()) {
                return sub_column->entries()[*it]->set_value(index, val);
            } else {
                sub_column = sub_column->entries()[*it].get();
            }
        }
        return sub_column->set_value(index, val);
    }

    vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_indices) {
        vector_t* sub_column = &data[col_indices.front()];
        for (auto it = std::next(col_indices.begin()); it != col_indices.end(); ++it) {
            sub_column = sub_column->entries()[*it].get();
        }
        return sub_column;
    }

    const vector_t* data_chunk_t::at(const std::pmr::vector<size_t>& col_indices) const {
        const vector_t* sub_column = &data[col_indices.front()];
        for (auto it = std::next(col_indices.begin()); it != col_indices.end(); ++it) {
            sub_column = sub_column->entries()[*it].get();
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
        set_capacity(chunk);
        set_cardinality(chunk);
        for (uint64_t i = 0; i < chunk.column_count(); i++) {
            data[i].reference(chunk.data[i]);
        }
    }

    void data_chunk_t::copy(data_chunk_t& other, uint64_t offset) const {
        assert(column_count() == other.column_count());
        assert(other.size() == 0);

        for (uint64_t i = 0; i < column_count(); i++) {
            assert(other.data[i].get_vector_type() == vector_type::FLAT);
            vector_ops::copy(data[i], other.data[i], size(), offset, 0);
        }
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
            assert(other.data[i].get_vector_type() == vector_type::FLAT);
            vector_ops::copy(data[i], other.data[i], indexing, source_count, offset, 0);
        }
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
        other.set_capacity(*this);
        other.set_cardinality(*this);
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

    void
    data_chunk_t::append(const data_chunk_t& other, bool resize, indexing_vector_t* indexing, uint64_t indexing_count) {
        uint64_t new_size = indexing ? size() + indexing_count : size() + other.size();
        if (other.size() == 0) {
            return;
        }
        if (column_count() != other.column_count()) {
            throw std::logic_error("Column counts of appending chunk doesn't match!");
        }
        if (new_size > capacity_) {
            if (resize) {
                auto new_capacity = next_power_of_two(new_size);
                for (uint64_t i = 0; i < column_count(); i++) {
                    data[i].resize(size(), new_capacity);
                }
                capacity_ = new_capacity;
            } else {
                throw std::logic_error("Can't append chunk to other chunk without resizing");
            }
        }
        for (uint64_t i = 0; i < column_count(); i++) {
            assert(data[i].get_vector_type() == vector_type::FLAT);
            if (indexing) {
                vector_ops::copy(other.data[i], data[i], *indexing, indexing_count, 0, size());
            } else {
                vector_ops::copy(other.data[i], data[i], other.size(), 0, size());
            }
        }
        set_cardinality(new_size);
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
        return -1;
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
                if (!field_found) {
                    return {size_t(-1)};
                }
            }
        }
        return res;
    }

    std::pmr::memory_resource* data_chunk_t::resource() const { return resource_; }

    // TODO: serialize vector_t buffers instead of logical_values
    void data_chunk_t::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(column_count());
        for (const auto& column : data) {
            serializer->start_array(2);
            column.type().serialize(serializer);
            serializer->start_array(size());
            for (size_t i = 0; i < size(); i++) {
                column.value(i).serialize(serializer);
            }
            serializer->end_array();
            serializer->end_array();
        }
        serializer->end_array();
    }

    data_chunk_t data_chunk_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        std::pmr::vector<types::complex_logical_type> types;
        size_t size = 0;
        types.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < types.capacity(); i++) {
            deserializer->advance_array(i);
            deserializer->advance_array(0);
            types.emplace_back(types::complex_logical_type::deserialize(deserializer));
            deserializer->pop_array();
            deserializer->advance_array(1);
            size = std::max(size, deserializer->current_array_size());
            deserializer->pop_array();
            deserializer->pop_array();
        }
        if (types.empty()) {
            deserializer->pop_array();
            return {deserializer->resource(), types, 0};
        }

        auto res = vector::data_chunk_t(deserializer->resource(), types, size);
        res.set_cardinality(size);
        for (size_t i = 0; i < res.column_count(); i++) {
            deserializer->advance_array(i);
            deserializer->advance_array(1);
            for (size_t j = 0; j < size; j++) {
                deserializer->advance_array(j);
                res.set_value(i, j, types::logical_value_t::deserialize(deserializer));
                deserializer->pop_array();
            }
            deserializer->pop_array();
            deserializer->pop_array();
        }
        return res;
    }

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
        indexing_vector_t indexing(resource, slice_count);
        for (uint64_t i = 0; i < slice_count; i++) {
            indexing.set_index(i, offset + i);
        }
        slice(indexing, slice_count);
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

} // namespace components::vector