#include "cursor.hpp"

namespace components::cursor {

    namespace {
        // An empty (0-column, 0-row) chunk so chunk_data() always has a valid front.
        vector::data_chunk_t empty_chunk(std::pmr::memory_resource* resource) {
            return vector::data_chunk_t(resource, std::pmr::vector<types::complex_logical_type>{resource});
        }
    } // namespace

    cursor_t::cursor_t(std::pmr::memory_resource* resource)
        : chunks_(resource)
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, const core::error_t& error)
        : chunks_(resource)
        , type_data_(resource)
        , error_(error) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, core::error_t&& error)
        : chunks_(resource)
        , type_data_(resource)
        , error_(std::move(error)) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : size_(chunk.size())
        , chunks_(resource)
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        // Strip placeholder columns (created by projected_cols scans to keep
        // storage indices stable for downstream operators). User-facing
        // iteration should only see real data.
        chunk.drop_unprojected_placeholders();
        // Mirror final column shape into type_data_ so callers querying the
        // result's typed-column descriptor see one entry per output column.
        const auto& chunk_types = chunk.types();
        type_data_.assign(chunk_types.begin(), chunk_types.end());
        chunks_.emplace_back(std::move(chunk));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks)
        : chunks_(std::move(chunks), resource)
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        // Keep the chunks as-is (each ≤DEFAULT_VECTOR_CAPACITY); never combine into one
        // oversized chunk. Drop placeholder columns per chunk (same shape across all).
        std::size_t total = 0;
        for (auto& c : chunks_) {
            c.drop_unprojected_placeholders();
            total += c.size();
        }
        size_ = total;
        if (chunks_.empty()) {
            chunks_.emplace_back(empty_chunk(resource));
        }
        const auto& chunk_types = chunks_.front().types();
        type_data_.assign(chunk_types.begin(), chunk_types.end());
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource,
                       std::pmr::vector<components::types::complex_logical_type>&& types)
        : size_(types.size())
        , chunks_(resource)
        , type_data_(std::move(types))
        , error_(core::error_t::no_error()) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    // chunk_data() exposes the first chunk — its column shape/types are shared by every
    // chunk, and for a single-chunk result it is the whole result. Row access that may
    // span chunks must go through value()/row(), which locate the owning chunk.
    vector::data_chunk_t& cursor_t::chunk_data() { return chunks_.front(); }
    const vector::data_chunk_t& cursor_t::chunk_data() const { return chunks_.front(); }
    std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() { return type_data_; }
    const std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() const { return type_data_; }

    std::size_t cursor_t::size() const { return size_; }
    bool cursor_t::has_next() const { return static_cast<std::size_t>(current_index_ + 1) < size_; }
    void cursor_t::advance() { ++current_index_; }
    index_t cursor_t::current_index() const { return current_index_; }

    types::logical_value_t cursor_t::value(uint64_t col_idx) const {
        return value(col_idx, static_cast<uint64_t>(current_index_));
    }

    types::logical_value_t cursor_t::value(uint64_t col_idx, uint64_t row_idx) const {
        // Locate the chunk holding the global row_idx (chunks are ≤CAP each).
        uint64_t base = 0;
        for (const auto& chunk : chunks_) {
            const auto rows = chunk.size();
            if (row_idx < base + rows) {
                return chunk.value(col_idx, row_idx - base);
            }
            base += rows;
        }
        return chunks_.front().value(col_idx, row_idx);
    }

    std::pmr::vector<types::logical_value_t> cursor_t::row() const {
        return row(static_cast<uint64_t>(current_index_));
    }

    std::pmr::vector<types::logical_value_t> cursor_t::row(uint64_t row_idx) const {
        const auto cols = chunks_.front().column_count();
        std::pmr::vector<types::logical_value_t> result(chunks_.front().resource());
        result.reserve(cols);
        for (uint64_t col = 0; col < cols; ++col) {
            result.push_back(value(col, row_idx));
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

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks) {
        return cursor_t_ptr{new cursor_t(resource, std::move(chunks))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types) {
        return cursor_t_ptr{new cursor_t(resource, std::move(types))};
    }
} // namespace components::cursor
