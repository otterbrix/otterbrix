#pragma once
#include "vector.hpp"

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

namespace components::vector {
    class data_chunk_t {
    public:
        data_chunk_t(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& types,
                     uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        data_chunk_t(const data_chunk_t&) = delete;
        data_chunk_t& operator=(const data_chunk_t&) = delete;
        data_chunk_t(data_chunk_t&&) noexcept;
        data_chunk_t& operator=(data_chunk_t&&) noexcept;
        ~data_chunk_t() = default;

        bool empty() const { return count_ == 0; }
        uint64_t size() const { return count_; }
        uint64_t capacity() const { return capacity_; }
        uint64_t column_count() const { return data.size(); }
        void set_cardinality(uint64_t count) {
            assert(count <= capacity_);
            count_ = count;
        }
        void set_capacity(uint64_t capacity) { capacity_ = capacity; }

        bool is_null(uint64_t col_idx, uint64_t index) const;
        bool is_null(uint64_t col_idx, const std::pmr::vector<size_t>& path) const;
        void set_null(uint64_t col_idx, const std::pmr::vector<size_t>& path, bool value);
        template<typename T>
        T get_value(uint64_t col_idx, uint64_t index) const {
            return data[col_idx].get_value<T>(index);
        }
        // Forwards plain values, optionals and (optional-of-)struct-tuples to the column vector, which
        // deduces and routes them.
        template<typename Arg>
        void set_value(uint64_t col_idx, uint64_t index, Arg&& value) {
            data[col_idx].set_value(index, std::forward<Arg>(value));
        }

        vector_t* at(const std::pmr::vector<size_t>& col_path);
        const vector_t* at(const std::pmr::vector<size_t>& col_path) const;

        bool all_constant() const;

        void reference(data_chunk_t& chunk);

        void destroy();

        void copy(data_chunk_t& other, uint64_t offset = 0) const;
        void
        copy(data_chunk_t& other, const indexing_vector_t& indexing, uint64_t source_count, uint64_t offset = 0) const;

        void split(data_chunk_t& other, uint64_t split_idx);

        void fuse(data_chunk_t&& other);

        void reference_columns(data_chunk_t& other, const std::vector<uint64_t>& column_ids);

        void flatten();

        std::vector<unified_vector_format> to_unified_format(std::pmr::memory_resource* resource);

        void slice(const indexing_vector_t& indexing_vector, uint64_t count);

        void
        slice(const data_chunk_t& other, const indexing_vector_t& indexing, uint64_t count, uint64_t col_offset = 0);

        void slice(std::pmr::memory_resource* resource, uint64_t offset, uint64_t count);

        data_chunk_t partial_copy(std::pmr::memory_resource* resource, uint64_t offset, uint64_t count) const;

        void reset();

        void hash(vector_t& result);
        void hash(std::vector<uint64_t>& column_ids, vector_t& result);
        void resize(uint64_t new_size);

        [[nodiscard]] std::pmr::vector<types::complex_logical_type> types() const;
        // Index of the column whose alias is `key`. An unknown name is a field_not_exists
        // error, never a SIZE_MAX sentinel the public cursor API could forward as a real index.
        [[nodiscard]] core::result_wrapper_t<size_t> column_index(std::string_view key) const;
        // Column index + nested field/array indices along `path`; same field_not_exists channel
        // as column_index, for the same reason.
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<size_t>>
        sub_column_indices(const std::pmr::vector<std::pmr::string>& path) const;

        std::pmr::memory_resource* resource() const;

    private:
        std::pmr::memory_resource* resource_ = nullptr;
        uint64_t count_ = 0;
        uint64_t capacity_ = DEFAULT_VECTOR_CAPACITY;

    public:
        vector_t row_ids;
        std::vector<vector_t> data;
    };

    void validate_chunk_capacity(vector::data_chunk_t& chunk, size_t filled_size);

    // Sub-query result compacters: fold ALL of a cursor's chunks (not just the first) into one bound
    // parameter value, so a >1024-row or multi-branch result is not silently truncated.
    core::result_wrapper_t<vector_t> compact_to_bool_value(const std::pmr::vector<data_chunk_t>& chunks);
    core::result_wrapper_t<vector_t> compact_to_single_value(const std::pmr::vector<data_chunk_t>& chunks);
    core::result_wrapper_t<vector_t> compact_to_array_value(const std::pmr::vector<data_chunk_t>& chunks);
    core::result_wrapper_t<vector_t> compact_to_list_value(const std::pmr::vector<data_chunk_t>& chunks);

} // namespace components::vector