#include <catch2/catch.hpp>

#include <components/vector/arithmetic.hpp>
#include <components/vector/data_chunk.hpp>

using namespace components;

// Regression: SSB q1-1 on database REOPEN segfaulted in
// compute_binary_arithmetic while evaluating
// SUM(lo_extendedprice * lo_discount) over an EMPTY batch chunk (count==0).
//
// On reopen a degenerate group sub-chunk has 0 rows AND 0 columns. The
// arithmetic operand keys resolve via data_chunk_t::at(path) to out-of-bounds
// vector_t* pointers (the chunk's column array is empty). compute_binary_arithmetic
// then dereferenced those operand vectors (left.type()) before checking the row
// count, causing EXC_BAD_ACCESS.
//
// This test reproduces that exact shape at the vector layer: a 0-row, 0-column
// chunk whose operand pointers are resolved with at(path) (the same call the
// arithmetic operator uses). With the count==0 guard in place the call must NOT
// crash and must return an empty (0-row) result vector.
//
// NOTE: the operand pointers are intentionally dangling/out-of-bounds — the test
// itself never dereferences them; only the function-under-test would, which is
// precisely the bug being guarded.
TEST_CASE("compute_binary_arithmetic: empty chunk operands, count==0 does not deref") {
    auto resource = core::pmr::otterbrix_resource();

    // Degenerate batch chunk: no columns, zero rows.
    std::pmr::vector<types::complex_logical_type> no_types(&resource);
    components::vector::data_chunk_t chunk(&resource, no_types, /*capacity=*/1);
    chunk.set_cardinality(0);
    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);

    // Resolve operand vectors the same way the arithmetic operator does: by
    // column path. With an empty column array these yield out-of-bounds pointers.
    std::pmr::vector<size_t> left_path(&resource);
    left_path.push_back(9); // lo_extendedprice index in the real SSB chunk
    std::pmr::vector<size_t> right_path(&resource);
    right_path.push_back(11); // lo_discount index

    components::vector::vector_t* left = chunk.at(left_path);
    components::vector::vector_t* right = chunk.at(right_path);

    // Without the count==0 guard this dereferences left->type() on a dangling
    // vector → SIGSEGV. Reaching the assertions below proves the guard ran.
    auto out = components::vector::compute_binary_arithmetic(&resource,
                                                             components::vector::arithmetic_op::multiply,
                                                             *left,
                                                             *right,
                                                             /*count=*/0);

    // A flat numeric result vector of the promoted arithmetic type, carrying no
    // rows (count==0 was requested).
    REQUIRE(out.get_vector_type() == components::vector::vector_type::FLAT);
    REQUIRE(out.type().type() == components::types::logical_type::DOUBLE);
}
