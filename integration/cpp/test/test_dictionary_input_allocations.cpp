#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>

#include <string>
#include <string_view>

using namespace components;

// A filtered scan hands DISTINCT a DICTIONARY chunk. Copying one cell used to slice every index up to
// that row, so a chunk cost rows^2 index entries (bytes, not the allocation count: the dedup table allocates per row anyway).

namespace {
    vector::data_chunk_t make_dictionary_chunk(std::pmr::memory_resource* resource, uint64_t rows) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::BIGINT);
        types.emplace_back(types::logical_type::TEXT);
        vector::data_chunk_t flat(resource, types, rows);
        const std::string value(32, 'x');
        for (uint64_t row = 0; row < rows; row++) {
            flat.set_value(0, row, static_cast<int64_t>(row));
            flat.set_value(1, row, std::string_view{value});
        }
        flat.set_cardinality(rows);
        vector::indexing_vector_t identity{resource, uint64_t{0}, rows};
        flat.slice(identity, rows);
        REQUIRE(flat.data[0].get_vector_type() == vector::vector_type::DICTIONARY);
        return flat;
    }

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
        operators::operator_distinct_t op{&arena, log_t{}};
        auto chunk = make_dictionary_chunk(&arena, rows);
        operators::chunks_vector_t out{&arena};
        arena.reset();
        REQUIRE_FALSE(op.push(&ctx, std::move(chunk), out).contains_error());
        REQUIRE(out.size() == 1);
        REQUIRE(out.front().size() == rows);
        return {arena.allocations(), arena.allocated_bytes()};
    }
} // namespace

TEST_CASE("integration::cpp::distinct::dictionary_input_costs_a_linear_number_of_bytes") {
    constexpr uint64_t kNarrow = 256;
    constexpr uint64_t kWide = 1024;
    const auto narrow = taken_for(kNarrow);
    const auto wide = taken_for(kWide);
    INFO("rows " << kNarrow << ": " << narrow.allocations << " allocations, " << narrow.bytes << " bytes; rows "
                 << kWide << ": " << wide.allocations << " allocations, " << wide.bytes << " bytes");
    // 4x the rows: linear is about 4x the bytes, quadratic about 16x.
    CHECK(wide.bytes < narrow.bytes * 8);
}
