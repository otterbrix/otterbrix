#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

using namespace components;
using components::operators::operator_match_t;

// The measurable half of "stop boxing every cell".
//
// filter_batch_ used to build one logical_value_t per surviving cell and immediately take it apart
// again (set_value(j, out, chunk.data[j].value(i))), so the cost of a chunk grew with rows TIMES
// columns. It now builds a selection and hands the whole chunk to data_chunk_t::copy, which routes
// each column through vector_ops::copy. What that changes is not "fewer allocations" in the
// abstract -- it is that the count no longer scales with the cell count, and that is what this
// pins: the same operator over ten times the rows must not cost ten times the allocations.

namespace {
    constexpr uint64_t kNarrow = 64;
    constexpr uint64_t kWide = 640;

    // STRING, and well past any small-string buffer: boxing such a cell into a logical_value_t
    // copies the bytes onto the resource, which is exactly what a gather does not do.
    vector::data_chunk_t make_chunk(std::pmr::memory_resource* resource, uint64_t rows) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        vector::data_chunk_t chunk(resource, types, rows);
        const std::string value(64, 'x');
        for (uint64_t row = 0; row < rows; row++) {
            for (uint64_t col = 0; col < 3; col++) {
                chunk.set_value(col, row, std::string_view{value});
            }
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    // No expression: the predicate is `always`, so every row survives and what is measured is the
    // copy itself rather than a condition graph.
    std::size_t allocations_for(uint64_t rows) {
        core::pmr::otterbrix_resource upstream;
        // Counting the operator's OWN arena, not the process default: everything here is placed,
        // so a process-default probe would read zero either way and prove nothing.
        core::pmr::counting_resource_t arena{&upstream};
        logical_plan::storage_parameters parameters{&arena};
        pipeline::context_t ctx{parameters,
                                actor_zeta::address_t::empty_address(),
                                actor_zeta::address_t::empty_address(),
                                actor_zeta::address_t::empty_address()};

        operator_match_t op{&arena, log_t{}, expressions::expression_ptr{}, logical_plan::limit_t::unlimit()};
        auto chunk = make_chunk(&arena, rows);
        operators::chunks_vector_t out{&arena};

        arena.reset(); // the chunk and the operator are built; measure only the filtering
        REQUIRE_FALSE(op.push(&ctx, std::move(chunk), out).contains_error());
        const std::size_t taken = arena.allocations();
        REQUIRE(out.size() == 1);
        REQUIRE(out.front().size() == rows);
        return taken;
    }

    // The shape filter_batch_ used to have: one logical_value_t per surviving cell, taken apart
    // again by set_value. Kept here only as the control this test compares against.
    std::size_t per_cell_allocations_for(uint64_t rows) {
        core::pmr::otterbrix_resource upstream;
        core::pmr::counting_resource_t arena{&upstream};

        auto chunk = make_chunk(&arena, rows);
        std::pmr::vector<types::complex_logical_type> types{&arena};
        for (uint64_t col = 0; col < 3; col++) {
            types.emplace_back(types::logical_type::STRING_LITERAL);
        }
        vector::data_chunk_t out_chunk(&arena, types, rows);

        arena.reset();
        for (uint64_t row = 0; row < rows; row++) {
            for (uint64_t col = 0; col < 3; col++) {
                out_chunk.set_value(col, row, chunk.data[col].value(row));
            }
        }
        out_chunk.set_cardinality(rows);
        return arena.allocations();
    }
} // namespace

// Measures the same work twice on the same input: once the way filter_batch_ does it now (build a
// selection, one typed gather), once the way it used to (a logical_value_t per surviving cell).
// No magic threshold -- the control is measured in the same run, so the test cannot rot into a
// number that stopped meaning anything.
TEST_CASE("components::operators::match::filtering a chunk does not cost one allocation per cell") {
    const std::size_t gathered = allocations_for(kWide);
    const std::size_t per_cell = per_cell_allocations_for(kWide);

    INFO("gather: " << gathered << " allocations, per-cell: " << per_cell << " (rows " << kWide << " x 3 columns)");
    REQUIRE(per_cell > 0);
    CHECK(gathered * 4 < per_cell);

    // And it must not scale with the row count either.
    const std::size_t narrow = allocations_for(kNarrow);
    INFO("gather over " << kNarrow << " rows: " << narrow << ", over " << kWide << " rows: " << gathered);
    CHECK(gathered < narrow * 2);
}
