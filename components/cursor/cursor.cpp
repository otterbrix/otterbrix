#include "cursor.hpp"

namespace components::cursor {

    namespace {
        // An empty (0-column, 0-row) chunk so chunks().front() is always valid.
        vector::data_chunk_t empty_chunk(std::pmr::memory_resource* resource) {
            return vector::data_chunk_t(resource, std::pmr::vector<types::complex_logical_type>{resource});
        }

        // Deep-copy a chunk's schema into the cursor's own descriptor. column_schema_t is
        // move-only on purpose — a defaulted copy would take the string's allocator from
        // std::pmr's DEFAULT resource — so each record is rebuilt against this cursor's.
        void mirror_schema(std::pmr::memory_resource* resource,
                           const std::pmr::vector<vector::column_schema_t>& schema,
                           std::pmr::vector<vector::column_schema_t>& out) {
            out.clear();
            out.reserve(schema.size());
            for (const auto& record : schema) {
                out.emplace_back(resource);
                out.back().attoid = record.attoid;
                out.back().name.assign(record.name.data(), record.name.size());
                out.back().type = record.type;
            }
        }
    } // namespace

    cursor_t::cursor_t(std::pmr::memory_resource* resource)
        : chunks_(resource)
        , columns_(resource)
        , error_(core::error_t::no_error()) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, const core::error_t& error)
        : chunks_(resource)
        , columns_(resource)
        , error_(error) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, core::error_t&& error)
        : chunks_(resource)
        , columns_(resource)
        , error_(std::move(error)) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : size_(chunk.size())
        , chunks_(resource)
        , columns_(resource)
        , error_(core::error_t::no_error()) {
        // Strip placeholder columns (created by projected_cols scans to keep
        // storage indices stable for downstream operators). User-facing
        // iteration should only see real data.
        chunk.drop_unprojected_placeholders();
        // Mirror the final column shape into the descriptor so callers querying the result's
        // column metadata see one entry per output column — name, type and identity.
        mirror_schema(resource, chunk.schema(), columns_);
        chunks_.emplace_back(std::move(chunk));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks)
        : chunks_(std::move(chunks), resource)
        , columns_(resource)
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
        mirror_schema(resource, chunks_.front().schema(), columns_);
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::column_schema_t>&& columns)
        : size_(columns.size())
        , chunks_(resource)
        , columns_(std::move(columns))
        , error_(core::error_t::no_error()) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    // Column shape/types are shared by every chunk; row access that may span chunks goes
    // through value()/row(), which locate the owning chunk. chunks() is for callers that
    // genuinely need raw per-chunk column vectors (and must iterate chunks themselves).
    std::pmr::vector<vector::data_chunk_t>& cursor_t::chunks() { return chunks_; }
    const std::pmr::vector<vector::data_chunk_t>& cursor_t::chunks() const { return chunks_; }
    const std::pmr::vector<vector::column_schema_t>& cursor_t::columns() const { return columns_; }

    std::size_t cursor_t::size() const { return size_; }
    std::size_t cursor_t::column_count() const { return columns_.size(); }
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
                             std::pmr::vector<vector::column_schema_t>&& columns) {
        return cursor_t_ptr{new cursor_t(resource, std::move(columns))};
    }
} // namespace components::cursor
