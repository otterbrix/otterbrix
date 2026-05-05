#pragma once

#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace components::catalog {

    // Describes a NOT NULL violation in a chunk.
    struct not_null_result_t {
        bool ok() const noexcept { return column_name.empty(); }
        std::string   column_name;      // empty when no violation
        std::size_t   column_index{0};
        std::uint64_t row{0};
    };

    // Check every row in chunk for NOT NULL violations on marked columns.
    // Returns the first violation found, or ok() if all rows pass.
    not_null_result_t enforce_not_null(
        const vector::data_chunk_t&                         chunk,
        const std::vector<table::column_definition_t>&      columns);

    // Fill null cells in chunk with their column default values where applicable.
    // Columns without a default are left null. Modifies chunk in-place.
    void apply_defaults(
        vector::data_chunk_t&                          chunk,
        const std::vector<table::column_definition_t>& columns);

    // Evaluate a compiled predicate over every row in chunk.
    // pred(chunk, row) must return true if the row passes the CHECK.
    // Returns the row index of the first failing row, or -1 if all pass.
    using row_predicate_fn = std::function<bool(const vector::data_chunk_t&, std::uint64_t)>;
    std::int64_t evaluate_check(const vector::data_chunk_t& chunk,
                                 const row_predicate_fn&     pred);

} // namespace components::catalog
