#include "node_data.hpp"

#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>

#include <algorithm>
#include <sstream>

namespace components::logical_plan {

    node_data_t::node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk)
        : node_t(resource, node_type::data_t)
        , chunks_(resource)
        , deferred_cells_(resource) {
        chunks_.emplace_back(std::move(chunk));
    }

    node_data_t::node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk)
        : node_t(resource, node_type::data_t)
        , chunks_(resource)
        , deferred_cells_(resource) {
        vector::data_chunk_t copy(resource, chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
        chunk.copy(copy, 0);
        chunks_.emplace_back(std::move(copy));
    }

    node_data_t::node_data_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks)
        : node_t(resource, node_type::data_t)
        , chunks_(std::move(chunks))
        , deferred_cells_(resource) {
        // Keep at least one (possibly empty) chunk so data_chunk()/column metadata always
        // have a valid front to return.
        if (chunks_.empty()) {
            chunks_.emplace_back(resource, std::pmr::vector<components::types::complex_logical_type>{resource});
        }
    }

    chunks_vector_t& node_data_t::chunks() { return chunks_; }

    const chunks_vector_t& node_data_t::chunks() const { return chunks_; }

    components::vector::data_chunk_t& node_data_t::data_chunk() { return chunks_.front(); }

    const components::vector::data_chunk_t& node_data_t::data_chunk() const { return chunks_.front(); }

    size_t node_data_t::size() const {
        size_t total = 0;
        for (const auto& chunk : chunks_) {
            total += chunk.size();
        }
        return total;
    }

    core::error_t node_data_t::settle_column(uint64_t column,
                                             const types::complex_logical_type& type,
                                             const casts::cast_t& rows_cast,
                                             const graph_execution_context& context) {
        auto* resource = this->resource();
        if (column >= chunks_.front().data.size()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"VALUES: a column past the end of the VALUES list", resource});
        }
        std::pmr::vector<const deferred_cell_t*> cells(resource);
        for (const auto& cell : deferred_cells_) {
            if (cell.column == column) {
                cells.push_back(&cell);
            }
        }
        std::sort(cells.begin(), cells.end(), [](const deferred_cell_t* left, const deferred_cell_t* right) {
            return left->row < right->row;
        });

        const auto written_type = chunks_.front().data[column].type();
        auto target_type = type;
        target_type.set_alias(std::string(written_type.alias()));
        size_t next_cell = 0;
        uint64_t chunk_start = 0;
        for (auto& chunk : chunks_) {
            const auto& stored = chunk.data[column];
            const uint64_t count = chunk.size();
            if (stored.type() != written_type) {
                return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{"VALUES: a column of differing types spans several chunks", resource});
            }
            vector::vector_t converted_rows(resource, target_type, count == 0 ? uint64_t{1} : count);
            if (rows_cast) {
                if (auto error = rows_cast(casts::cast_kind::cast, stored, &converted_rows, context, count);
                    error.contains_error()) {
                    return error;
                }
            }
            const vector::vector_t& rows = rows_cast ? converted_rows : stored;
            vector::vector_t rebuilt(resource, target_type, chunk.capacity());
            for (uint64_t row = 0; row < count; ++row) {
                if (next_cell < cells.size() && cells[next_cell]->row == chunk_start + row) {
                    const auto* cell = cells[next_cell++];
                    if (!cell->cast) {
                        vector::vector_ops::copy(cell->value, rebuilt, 1, 0, row);
                        continue;
                    }
                    vector::vector_t cell_value(resource, target_type, 1);
                    if (auto error = cell->cast(casts::cast_kind::cast,
                                                vector::make_flat_copy(resource, cell->value, 1),
                                                &cell_value,
                                                context,
                                                1);
                        error.contains_error()) {
                        return error;
                    }
                    vector::vector_ops::copy(cell_value, rebuilt, 1, 0, row);
                } else if (stored.is_null(row)) {
                    rebuilt.set_null(row, true);
                } else {
                    vector::vector_ops::copy(rows, rebuilt, row + 1, row, row);
                }
            }
            chunk.data[column] = std::move(rebuilt);
            chunk_start += count;
        }
        if (next_cell != cells.size()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"VALUES: a deferred cell points outside the VALUES list", resource});
        }
        deferred_cells_.erase(std::remove_if(deferred_cells_.begin(),
                                             deferred_cells_.end(),
                                             [column](const deferred_cell_t& cell) { return cell.column == column; }),
                              deferred_cells_.end());
        return core::error_t::no_error();
    }

    hash_t node_data_t::hash_impl() const { return 0; }

    std::string node_data_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$raw_data: {";
        stream << "$rows: " << size();
        stream << "}";
        return stream.str();
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk) {
        return {new node_data_t{resource, std::move(chunk)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk) {
        return {new node_data_t{resource, chunk}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, chunks_vector_t&& chunks) {
        return {new node_data_t{resource, std::move(chunks)}};
    }

} // namespace components::logical_plan
