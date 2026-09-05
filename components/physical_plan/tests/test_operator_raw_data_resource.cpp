#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator_raw_data.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/resource_tracer.hpp>

#include <memory_resource>

using components::operators::operator_raw_data_t;

// THE LITERAL CARRIER MUST LIVE ON THE CALLER'S ARENA — FOR THE EMPTY BATCH TOO.
//
// The multi-chunk constructor takes a std::pmr::vector of chunks, and that vector names its arena in its
// allocator whether or not it holds a single chunk. An empty-vector branch reaching for
// std::pmr::get_default_resource() instead (rule 14 forbids it in production code) puts the operator's
// output_ — and every chunk a drain of this source later builds — on the global default arena nobody owns
// or traces, while the caller's arena shows nothing. The vector's own allocator is the answer that is
// right in both branches; the non-empty control below pins that the populated case still lands on the
// chunks' arena.

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
