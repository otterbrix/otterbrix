#include "cursor.hpp"

#include <algorithm>

namespace components::cursor {

    namespace {
        // An empty (0-column, 0-row) chunk so chunks().front() is always valid.
        vector::data_chunk_t empty_chunk(std::pmr::memory_resource* resource) {
            return vector::data_chunk_t(resource, std::pmr::vector<types::complex_logical_type>{resource});
        }
    } // namespace

    cursor_t::cursor_t(std::pmr::memory_resource* resource)
        : chunks_(resource)
        , chunk_starts_(1, 0, resource)
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    // error_t's own copy/move ctors don't move the message onto `resource` (copy keeps the
    // default resource, move keeps the producer's), so it's rebuilt via error_on().
    cursor_t::cursor_t(std::pmr::memory_resource* resource, const core::error_t& error)
        : chunks_(resource)
        , chunk_starts_(1, 0, resource)
        , type_data_(resource)
        , error_(core::error_on(resource, error)) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, core::error_t&& error)
        // `error` is a named parameter (lvalue), so this delegates to the &-ctor above.
        : cursor_t(resource, error) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : size_(chunk.size())
        , chunks_(resource)
        , chunk_starts_(1, 0, resource)
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        // Mirror final column shape into type_data_ so callers querying the
        // result's typed-column descriptor see one entry per output column.
        const auto& chunk_types = chunk.types();
        type_data_.assign(chunk_types.begin(), chunk_types.end());
        chunks_.emplace_back(std::move(chunk));
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks)
        : chunks_(std::move(chunks), resource)
        , chunk_starts_(resource)
        , type_data_(resource)
        , error_(core::error_t::no_error()) {
        // Keep the chunks as-is (each ≤DEFAULT_VECTOR_CAPACITY); never combine into one
        // oversized chunk.
        std::size_t total = 0;
        for (const auto& c : chunks_) {
            chunk_starts_.push_back(total);
            total += c.size();
        }
        size_ = total;
        if (chunks_.empty()) {
            chunks_.emplace_back(empty_chunk(resource));
            chunk_starts_.push_back(0);
        }
        const auto& chunk_types = chunks_.front().types();
        type_data_.assign(chunk_types.begin(), chunk_types.end());
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource,
                       std::pmr::vector<components::types::complex_logical_type>&& types)
        : size_(types.size())
        , chunks_(resource)
        , chunk_starts_(1, 0, resource)
        , type_data_(std::move(types))
        , error_(core::error_t::no_error()) {
        chunks_.emplace_back(empty_chunk(resource));
    }

    // Column shape/types are shared by every chunk; row access that may span chunks goes
    // through locate(), which finds the owning chunk. chunks() is for callers that
    // genuinely need raw per-chunk column vectors (and must iterate chunks themselves).
    std::pmr::vector<vector::data_chunk_t>& cursor_t::chunks() {
        assert(!is_write_ && "result is a row count, not a data");
        return chunks_;
    }
    const std::pmr::vector<vector::data_chunk_t>& cursor_t::chunks() const {
        assert(!is_write_ && "result is a row count, not a data");
        return chunks_;
    }
    std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() {
        assert(!is_write_ && "result is a row count, not a data");
        return type_data_;
    }
    const std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() const {
        assert(!is_write_ && "result is a row count, not a data");
        return type_data_;
    }

    std::size_t cursor_t::size() const {
        assert(!is_write_ && "result is a row count, not a data");
        return size_;
    }
    std::size_t cursor_t::column_count() const {
        assert(!is_write_ && "result is a row count, not a data");
        return type_data_.size();
    }
    core::result_wrapper_t<std::size_t> cursor_t::column_index(std::string_view key) const {
        assert(!is_write_ && "result is a row count, not a data");
        return chunks_.front().column_index(key);
    }
    bool cursor_t::has_next() const { return static_cast<std::size_t>(current_index_ + 1) < size_; }
    void cursor_t::advance() { ++current_index_; }
    index_t cursor_t::current_index() const { return current_index_; }

    core::error_t
    cursor_t::make_error(core::error_code_t code, std::string_view what, std::source_location location) const {
        return core::error_t{code, std::pmr::string{what, chunks_.get_allocator().resource()}, location};
    }

    core::result_wrapper_t<cell_location_t> cursor_t::locate(uint64_t row_idx) const {
        assert(!is_write_ && "result is a row count, not a data");
        if (row_idx >= chunk_starts_.back() + chunks_.back().size()) {
            return make_error(core::error_code_t::invalid_parameter, "row is out of range");
        }
        const auto next = std::upper_bound(chunk_starts_.begin(), chunk_starts_.end(), row_idx);
        const auto chunk = static_cast<std::size_t>(next - chunk_starts_.begin()) - 1;
        return cell_location_t{chunk, row_idx - chunk_starts_[chunk]};
    }

    core::result_wrapper_t<cursor_t::cell_t>
    cursor_t::resolve_cell(uint64_t col_idx, uint64_t row_idx, std::span<const std::size_t> path) const {
        VALUE_OR_RETURN(const auto location, locate(row_idx));
        const auto& columns = chunks_[location.chunk].data;
        if (col_idx >= columns.size()) {
            return make_error(core::error_code_t::field_not_exists, "column is out of range");
        }
        const auto& column = columns[col_idx];

        const types::complex_logical_type* type = &column.type();
        for (const auto sub : path) {
            switch (type->type()) {
                case types::logical_type::LIST:
                case types::logical_type::MAP:
                    type = &type->child_type();
                    break;
                case types::logical_type::ARRAY:
                    if (sub >= type->extension_as<types::array_logical_type_extension>()->size()) {
                        return make_error(core::error_code_t::invalid_parameter, "array subscript is out of range");
                    }
                    type = &type->child_type();
                    break;
                case types::logical_type::STRUCT:
                    if (sub >= type->child_types().size()) {
                        return make_error(core::error_code_t::invalid_parameter, "struct field is out of range");
                    }
                    type = &type->child_types()[sub];
                    break;
                default:
                    return make_error(core::error_code_t::invalid_parameter,
                                      "path steps through a type with no sub-elements");
            }
        }

        const auto element = column.resolve_nested_element(location.row, path, 0);
        return cell_t{element.leaf,
                      element.index,
                      element.is_null || element.leaf->type().type() == types::logical_type::NA};
    }

    core::result_wrapper_t<bool>
    cursor_t::is_null(uint64_t col_idx, uint64_t row_idx, std::span<const std::size_t> path) const {
        assert(!is_write_ && "result is a row count, not a data");
        VALUE_OR_RETURN(const auto cell, resolve_cell(col_idx, row_idx, path));
        return cell.is_null;
    }

    core::result_wrapper_t<uint64_t> cursor_t::list_size(uint64_t col_idx, uint64_t row_idx) const {
        assert(!is_write_ && "result is a row count, not a data");
        VALUE_OR_RETURN(const auto cell, resolve_cell(col_idx, row_idx, {}));
        if (cell.is_null) {
            return make_error(core::error_code_t::conversion_failure, "cell is NULL");
        }
        switch (cell.vec->type().type()) {
            case types::logical_type::LIST:
            case types::logical_type::MAP:
                return static_cast<uint64_t>(cell.vec->data<types::list_entry_t>()[cell.index].length);
            case types::logical_type::ARRAY:
                return static_cast<uint64_t>(
                    cell.vec->type().extension_as<types::array_logical_type_extension>()->size());
            default:
                return make_error(core::error_code_t::invalid_parameter, "cell is not a LIST, MAP or ARRAY");
        }
    }

    bool cursor_t::is_success() const noexcept { return !error_.contains_error(); }

    bool cursor_t::is_error() const noexcept { return error_.contains_error(); }

    const core::error_t& cursor_t::get_error() const noexcept { return error_; }

    std::uint64_t cursor_t::affected_rows() const noexcept {
        assert(is_write_ && "result is stored as data, not row count");
        return affected_rows_;
    }

    bool cursor_t::is_write() const noexcept { return is_write_; }

    void cursor_t::set_written(std::uint64_t rows) noexcept {
        affected_rows_ = rows;
        is_write_ = true;
    }

    catalog::oid_t cursor_t::created_oid() const noexcept { return created_oid_; }

    void cursor_t::set_created_oid(catalog::oid_t oid) noexcept { created_oid_ = oid; }

    session::transaction_status_t cursor_t::transaction_status() const noexcept { return transaction_status_; }

    void cursor_t::set_transaction_status(session::transaction_status_t status) noexcept {
        transaction_status_ = status;
    }

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
