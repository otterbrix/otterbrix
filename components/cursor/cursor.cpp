#include "cursor.hpp"

#include <components/vector/vector_operations.hpp>

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

    cursor_t::cursor_t(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks)
        : table_data_(resource, std::pmr::vector<types::complex_logical_type>{resource})
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        std::size_t total = 0;
        for (const auto& c : chunks) {
            total += c.size();
        }
        size_ = total;
        if (chunks.empty()) {
            return;
        }
        if (chunks.size() == 1) {
            table_data_ = std::move(chunks.front());
            return;
        }
        auto types = chunks.front().types();
        vector::data_chunk_t combined(resource, types, total == 0 ? 1 : total);
        uint64_t offset = 0;
        for (auto& c : chunks) {
            if (c.size() == 0) {
                continue;
            }
            for (uint64_t col = 0; col < c.column_count(); ++col) {
                vector::vector_ops::copy(c.data[col], combined.data[col], c.size(), 0, offset);
            }
            vector::vector_ops::copy(c.row_ids, combined.row_ids, c.size(), 0, offset);
            offset += c.size();
        }
        combined.set_cardinality(total);
        table_data_ = std::move(combined);
    }

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

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks) {
        return cursor_t_ptr{new cursor_t(resource, std::move(chunks))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types) {
        return cursor_t_ptr{new cursor_t(resource, std::move(types))};
    }
} // namespace components::cursor
