#include "cursor.hpp"

namespace components::cursor {

    cursor_t::cursor_t(std::pmr::memory_resource* resource)
        : table_data_(resource, {})
        , type_data_(resource)
        , error_(core::error_t::no_error()) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, const core::error_t& error)
        : table_data_(resource, {})
        , type_data_(resource)
        , error_(error) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, core::error_t&& error)
        : table_data_(resource, {})
        , type_data_(resource)
        , error_(std::move(error)) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : size_(chunk.size())
        , table_data_(std::move(chunk))
        , type_data_(resource)
        , error_(core::error_t::no_error()) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource,
                       std::pmr::vector<components::types::complex_logical_type>&& types)
        : size_(types.size())
        , table_data_(resource, {})
        , type_data_(std::move(types))
        , error_(core::error_t::no_error()) {}

    vector::data_chunk_t& cursor_t::chunk_data() { return table_data_; }
    const vector::data_chunk_t& cursor_t::chunk_data() const { return table_data_; }
    std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() { return type_data_; }
    const std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() const { return type_data_; }

    std::size_t cursor_t::size() const { return size_; }
    bool cursor_t::has_next() const { return static_cast<std::size_t>(current_index_ + 1) < size_; }
    void cursor_t::advance() { ++current_index_; }
    index_t cursor_t::current_index() const { return current_index_; }

    types::logical_value_t cursor_t::value(uint64_t col_idx) const {
        return table_data_.value(col_idx, static_cast<uint64_t>(current_index_));
    }

    types::logical_value_t cursor_t::value(uint64_t col_idx, uint64_t row_idx) const {
        return table_data_.value(col_idx, row_idx);
    }

    std::pmr::vector<types::logical_value_t> cursor_t::row() const {
        return row(static_cast<uint64_t>(current_index_));
    }

    std::pmr::vector<types::logical_value_t> cursor_t::row(uint64_t row_idx) const {
        std::pmr::vector<types::logical_value_t> result(table_data_.resource());
        result.reserve(table_data_.column_count());
        for (uint64_t col = 0; col < table_data_.column_count(); ++col) {
            result.push_back(table_data_.value(col, row_idx));
        }
        return result;
    }

    bool cursor_t::is_success() const noexcept { return !error_.contains_error(); }

    bool cursor_t::is_error() const noexcept { return error_.contains_error(); }

    core::error_t cursor_t::get_error() const { return error_; }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource) { return cursor_t_ptr{new cursor_t(resource)}; }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, const core::error_t& error) {
        return cursor_t_ptr{new cursor_t(resource, error)};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, core::error_t&& error) {
        return cursor_t_ptr{new cursor_t(resource, std::move(error))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return cursor_t_ptr{new cursor_t(resource, std::move(chunk))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types) {
        return cursor_t_ptr{new cursor_t(resource, std::move(types))};
    }
} // namespace components::cursor
