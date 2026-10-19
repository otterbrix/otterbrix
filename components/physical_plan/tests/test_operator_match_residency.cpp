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

// filter_batch_ builds a selection and hands the whole chunk to data_chunk_t::copy, which routes
// each column through vector_ops::copy, so the allocation count does not scale with the cell count.
// This pins it: the same operator over ten times the rows must not cost ten times the allocations.

namespace {
    constexpr uint64_t kNarrow = 64;
    constexpr uint64_t kWide = 640;

    // STRING, and well past any small-string buffer.
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

    // Under ASAN the string arena takes one upstream block per stored string, so a gather that copies
    // each string once would scale with the rows here for a reason that has nothing to do with boxing.
    // The arena's own blocks are taken out of both measurements; what is left is everything else.
    std::size_t string_arena_allocations(const vector::data_chunk_t& chunk) {
        std::size_t total = 0;
        for (const auto& column : chunk.data) {
            const auto auxiliary = column.auxiliary();
            REQUIRE(auxiliary);
            REQUIRE(auxiliary->type() == vector::vector_buffer_type::STRING);
            total += static_cast<const vector::string_vector_buffer_t*>(auxiliary.get())->upstream_allocations();
        }
        return total;
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

        operator_match_t op{&arena, log_t{}, expressions::expression_ptr{}, logical_plan::row_cap_t::uncapped()};
        auto chunk = make_chunk(&arena, rows);
        operators::chunks_vector_t out{&arena};

        arena.reset(); // the chunk and the operator are built; measure only the filtering
        REQUIRE_FALSE(op.push(&ctx, std::move(chunk), out).contains_error());
        const std::size_t taken = arena.allocations();
        REQUIRE(out.size() == 1);
        REQUIRE(out.front().size() == rows);
        const std::size_t own = string_arena_allocations(out.front());
        REQUIRE(own <= taken);
        return taken - own;
    }
} // namespace

// The allocation count must not scale with the row count: ten times the rows must not cost ten times
// the allocations.
TEST_CASE("components::operators::match::filtering a chunk does not cost one allocation per cell") {
    const std::size_t gathered = allocations_for(kWide);
    const std::size_t narrow = allocations_for(kNarrow);
    INFO("gather over " << kNarrow << " rows: " << narrow << ", over " << kWide << " rows: " << gathered);
    CHECK(gathered < narrow * 2);
}
