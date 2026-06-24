#pragma once
#include "vector.hpp"

#include <components/types/logical_value.hpp>
#include <core/result_wrapper.hpp>

namespace components::vector {

    class data_chunk_t {
    public:
        data_chunk_t(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& types,
                     uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        // Projected constructor: allocates buffers only for projected_cols; other columns
        // are placeholders (no buffer) so that column indices stay stable for downstream operators.
        data_chunk_t(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& all_types,
                     const std::vector<size_t>& projected_cols,
                     uint64_t capacity);
        data_chunk_t(const data_chunk_t&) = delete;
        data_chunk_t& operator=(const data_chunk_t&) = delete;
        data_chunk_t(data_chunk_t&&) = default;
        data_chunk_t& operator=(data_chunk_t&&) = default;
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

        types::logical_value_t value(uint64_t col_idx, uint64_t index) const;
        types::logical_value_t value(const std::pmr::vector<size_t>& col_path, uint64_t index) const;
        void set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val);
        void set_value(const std::pmr::vector<size_t>& col_path, uint64_t index, const types::logical_value_t& val);

        template<typename T>
        std::optional<T> get_value(uint64_t col_idx, uint64_t index) const {
            return data[col_idx].get_value<T>(index);
        }
        template<typename... Ts>
        requires(sizeof...(Ts) > 1) std::optional<std::tuple<std::optional<Ts>...>> get_value(uint64_t col_idx,
                                                                                              uint64_t index) const {
            return data[col_idx].get_value<Ts...>(index);
        }
        template<typename T>
        T get_value_not_null(uint64_t col_idx, uint64_t index) const {
            return data[col_idx].get_value_not_null<T>(index);
        }
        template<typename... Ts>
        requires(sizeof...(Ts) > 1) std::tuple<std::optional<Ts>...> get_value_not_null(uint64_t col_idx,
                                                                                        uint64_t index) const {
            return data[col_idx].get_value_not_null<Ts...>(index);
        }
        // Forwards plain values, optionals and (optional-of-)struct-tuples to the column vector, which
        // deduces and routes them. logical_value_t is excluded so it keeps hitting the runtime overload.
        template<typename Arg>
        requires(!std::is_same_v<std::remove_cvref_t<Arg>, types::logical_value_t>) void set_value(uint64_t col_idx,
                                                                                                   uint64_t index,
                                                                                                   Arg&& value) {
            data[col_idx].set_value(index, std::forward<Arg>(value));
        }

        vector_t* at(const std::pmr::vector<size_t>& col_indices);
        const vector_t* at(const std::pmr::vector<size_t>& col_indices) const;

        uint64_t allocation_size() const;

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

        // Drop unprojected placeholder columns in-place (data() == nullptr &&
        // auxiliary() == nullptr) so user-visible iteration sees only real
        // data. Used at the cursor boundary where placeholder stability for
        // downstream operators is no longer needed.
        void drop_unprojected_placeholders();

        void hash(vector_t& result);
        void hash(std::vector<uint64_t>& column_ids, vector_t& result);
        void resize(uint64_t new_size);

        [[nodiscard]] std::pmr::vector<types::complex_logical_type> types() const;
        size_t column_index(std::string_view key) const;
        std::pmr::vector<size_t> sub_column_indices(const std::pmr::vector<std::pmr::string>& path) const;

        std::pmr::memory_resource* resource() const;

    private:
        std::pmr::memory_resource* resource_;
        uint64_t count_ = 0;
        uint64_t capacity_ = DEFAULT_VECTOR_CAPACITY;

    public:
        vector_t row_ids;
        std::vector<vector_t> data;
    };

    void validate_chunk_capacity(vector::data_chunk_t& chunk, size_t filled_size);

    core::result_wrapper_t<types::logical_value_t> compact_to_bool_value(const data_chunk_t& data);
    core::result_wrapper_t<types::logical_value_t> compact_to_single_value(const data_chunk_t& data);
    core::result_wrapper_t<types::logical_value_t> compact_to_array_value(const data_chunk_t& data);
    core::result_wrapper_t<types::logical_value_t> compact_to_row_value(const data_chunk_t& data);

} // namespace components::vector