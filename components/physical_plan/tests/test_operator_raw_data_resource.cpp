#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator_raw_data.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/resource_tracer.hpp>

#include <memory_resource>

using components::operators::operator_raw_data_t;

// The empty-batch case must still resolve to the caller's arena via the vector's own allocator, not
// std::pmr::get_default_resource() (forbidden) — an empty std::pmr::vector still names its arena.

TEST_CASE("components::operators::raw_data_empty_batch_stays_on_the_callers_arena") {
    resource_tracer_t tracer;
    std::pmr::vector<components::vector::data_chunk_t> chunks(&tracer);

    operator_raw_data_t op(chunks);

    CHECK(op.resource() == &tracer);
}

TEST_CASE("components::operators::raw_data_populated_batch_stays_on_the_chunks_arena") {
    resource_tracer_t tracer;
    std::pmr::vector<components::types::complex_logical_type> types(&tracer);
    types.emplace_back(components::types::logical_type::BIGINT);

    std::pmr::vector<components::vector::data_chunk_t> chunks(&tracer);
    chunks.emplace_back(&tracer, types, 1u);

    operator_raw_data_t op(chunks);

    CHECK(op.resource() == &tracer);
}
