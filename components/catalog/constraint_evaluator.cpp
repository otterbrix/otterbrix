#include "constraint_evaluator.hpp"

namespace components::catalog {

    not_null_result_t enforce_not_null(
        const vector::data_chunk_t&                    chunk,
        const std::vector<table::column_definition_t>& columns)
    {
        const std::size_t ncols = std::min(columns.size(), static_cast<std::size_t>(chunk.column_count()));
        for (std::size_t col = 0; col < ncols; ++col) {
            if (!columns[col].is_not_null()) continue;
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                if (!chunk.data[col].validity().row_is_valid(row)) {
                    return {columns[col].name(), col, row};
                }
            }
        }
        return {};
    }

    void apply_defaults(
        vector::data_chunk_t&                          chunk,
        const std::vector<table::column_definition_t>& columns)
    {
        const std::size_t ncols = std::min(columns.size(), static_cast<std::size_t>(chunk.column_count()));
        for (std::size_t col = 0; col < ncols; ++col) {
            if (!columns[col].has_default_value()) continue;
            const auto& def = columns[col].default_value();
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                if (!chunk.data[col].validity().row_is_valid(row)) {
                    chunk.data[col].set_value(row, def);
                }
            }
        }
    }

    std::int64_t evaluate_check(const vector::data_chunk_t& chunk,
                                 const row_predicate_fn&     pred)
    {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            if (!pred(chunk, row)) {
                return static_cast<std::int64_t>(row);
            }
        }
        return -1;
    }

} // namespace components::catalog
