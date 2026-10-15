#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>

#include <string>
#include <string_view>

using namespace components;

// DELETE ... USING ... RETURNING pairs every deleted row with the right-side row that matched it.
// The right side of a filtered USING source is a DICTIONARY chunk, and copying one cell out of one
// slices every index up to that row, so pairing cell by cell allocated once per cell and cost rows^2
// index entries. The pairing is one gather per column, so neither the allocations nor the bytes may
// grow with the number of deleted rows. #646.

namespace {
    constexpr uint64_t kNarrow = 64;
    constexpr uint64_t kWide = 640;

    vector::data_chunk_t make_chunk(std::pmr::memory_resource* resource, uint64_t rows) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::BIGINT);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        vector::data_chunk_t chunk(resource, types, rows);
        const std::string value(64, 'x');
        for (uint64_t row = 0; row < rows; row++) {
            chunk.set_value(0, row, static_cast<int64_t>(row));
            chunk.set_value(1, row, std::string_view{value});
            chunk.set_value(2, row, std::string_view{value});
            chunk.row_ids.data<int64_t>()[row] = static_cast<int64_t>(row);
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    // The right side holds one DICTIONARY chunk and every left row matches its first row (no
    // condition), so the number of pairings is exactly the number of deleted rows.
    struct taken_t {
        std::size_t allocations;
        std::size_t bytes;
    };

    taken_t taken_for(uint64_t rows) {
        core::pmr::otterbrix_resource upstream;
        core::pmr::counting_resource_t arena{&upstream};
        logical_plan::storage_parameters parameters{&arena};
        pipeline::context_t ctx{parameters,
                                actor_zeta::address_t::empty_address(),
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox()};

        std::pmr::vector<operators::projected_column_t> returning{&arena};
        returning.emplace_back(&arena,
                               "*",
                               expressions::expression_ptr(expressions::make_scalar_expression(
                                   &arena,
                                   expressions::scalar_type::star_expand,
                                   expressions::key_t{&arena})));
        operators::operator_delete op{&arena, log_t{}, catalog::oid_t{1}, std::move(returning)};

        operators::operator_ptr right(new operators::read_only_operator_t(&arena, log_t{}, operators::operator_type::empty));
        operators::chunks_vector_t right_chunks{&arena};
        right_chunks.emplace_back(make_chunk(&arena, rows));
        vector::indexing_vector_t identity{&arena, uint64_t{0}, rows};
        right_chunks.back().slice(identity, rows);
        REQUIRE(right_chunks.back().data[1].get_vector_type() == vector::vector_type::DICTIONARY);
        right->set_output(operators::make_operator_data(&arena, std::move(right_chunks)));
        op.set_children(nullptr, right);

        auto left = make_chunk(&arena, rows);
        operators::chunks_vector_t out{&arena};

        arena.reset();
        REQUIRE_FALSE(op.push(&ctx, std::move(left), out).contains_error());
        return {arena.allocations(), arena.allocated_bytes()};
    }
} // namespace

TEST_CASE("integration::cpp::delete_using_returning::pairing_the_matched_right_rows_does_not_allocate_per_row") {
    const auto wide = taken_for(kWide);
    const auto narrow = taken_for(kNarrow);
    INFO("rows " << kNarrow << ": " << narrow.allocations << " allocations, " << narrow.bytes << " bytes; rows "
                 << kWide << ": " << wide.allocations << " allocations, " << wide.bytes << " bytes");
    // 10x the rows: linear in the rows is about 10x, per cell about 10x allocations, quadratic about 100x bytes.
    CHECK(wide.allocations < narrow.allocations * 2);
    CHECK(wide.bytes < narrow.bytes * 20);
}
