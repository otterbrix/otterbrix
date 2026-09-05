#include <catch2/catch_test_macros.hpp>

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
// This test pins the two guarantees that shape meets, layer by layer:
// data_chunk_t::at(path) answers nullptr for an ordinal past the chunk's width
// ("column not found", handled as a clean error by the arithmetic operator), and
// compute_binary_arithmetic with count==0 touches no operand rows and returns an
// empty (0-row) result vector of the promoted type.
TEST_CASE("compute_binary_arithmetic: empty chunk operands, count==0 does not deref") {
    auto resource = core::pmr::otterbrix_resource();

    // Degenerate batch chunk: no columns, zero rows.
    std::pmr::vector<types::complex_logical_type> no_types(&resource);
    components::vector::data_chunk_t chunk(&resource, no_types, /*capacity=*/1);
    chunk.set_cardinality(0);
    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);

    // Resolve operand vectors the same way the arithmetic operator does: by
    // column path. An ordinal past the chunk's width is "column not found" —
    // data_chunk_t::at now answers nullptr instead of handing out an
    // out-of-bounds pointer, and the arithmetic operator converts that into a
    // clean error before compute_binary_arithmetic can ever see the operand.
    std::pmr::vector<size_t> left_path(&resource);
    left_path.push_back(9); // lo_extendedprice index in the real SSB chunk
    std::pmr::vector<size_t> right_path(&resource);
    right_path.push_back(11); // lo_discount index

    REQUIRE(chunk.at(left_path) == nullptr);
    REQUIRE(chunk.at(right_path) == nullptr);

    // The count==0 guard of compute_binary_arithmetic itself still must not
    // touch operand ROWS: 0-row operands of the SSB q1-1 types multiply into an
    // empty result. (Element access on an empty vector would be OOB — the ASAN
    // suite keeps this honest.)
    components::vector::vector_t left(&resource, types::logical_type::DOUBLE, /*capacity=*/0);
    components::vector::vector_t right(&resource, types::logical_type::BIGINT, /*capacity=*/0);
    auto out = components::vector::compute_binary_arithmetic(&resource,
                                                             components::vector::arithmetic_op::multiply,
                                                             left,
                                                             right,
                                                             /*count=*/0);

    // A flat numeric result vector of the promoted arithmetic type, carrying no
    // rows (count==0 was requested). The count==0 guard answers BEFORE the operand
    // types are read, so it stays a success even though DOUBLE/BIGINT never reaches
    // the type classification below.
    REQUIRE_FALSE(out.has_error());
    REQUIRE(out.value().get_vector_type() == components::vector::vector_type::FLAT);
    REQUIRE(out.value().type().type() == components::types::logical_type::DOUBLE);
}

// THE arithmetic.cpp COORDINATE — the vector-level twin of the logical_value_t
// mixed-operand refusal.
//
// Measured without the type guard: compute_binary_arithmetic on STRING_LITERAL +
// BIGINT answers a vector of logical_type 0 (== NA) with row0 and row1 both NULL,
// and BOOLEAN + BOOLEAN answers logical_type 0 as well — a success-shaped NULL for
// an operation that has no meaning, indistinguishable in the result from real SQL
// NULLs. compute_unary_neg is worse without one: it dispatches into
// unary_neg_wrapper, whose non-numeric branch THROWS std::logic_error out of a
// compute path.
//
// The four entry points answer core::result_wrapper_t<vector_t> and refuse an
// operand pair arithmetic cannot type. An NA-TYPED operand is NOT such a pair:
// an NA-typed vector is this engine's untyped-NULL column and SQL says NULL + 1
// is NULL, so that case still answers NA — the one shape deliberately preserved.
TEST_CASE("compute arithmetic: an operand pair arithmetic cannot type is a refusal") {
    auto resource = core::pmr::otterbrix_resource();
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;
    using components::vector::arithmetic_op;
    using components::vector::vector_t;

    constexpr uint64_t count = 2;

    auto strings = [&]() {
        vector_t v(&resource, complex_logical_type(logical_type::STRING_LITERAL), count);
        v.set_value(0, logical_value_t{&resource, std::string{"a"}});
        v.set_value(1, logical_value_t{&resource, std::string{"b"}});
        return v;
    };
    auto bigints = [&]() {
        vector_t v(&resource, complex_logical_type(logical_type::BIGINT), count);
        v.set_value(0, logical_value_t{&resource, int64_t{1}});
        v.set_value(1, logical_value_t{&resource, int64_t{2}});
        return v;
    };
    auto bools = [&]() {
        vector_t v(&resource, complex_logical_type(logical_type::BOOLEAN), count);
        v.set_value(0, logical_value_t{&resource, true});
        v.set_value(1, logical_value_t{&resource, true});
        return v;
    };

    SECTION("vector op vector: string + number refuses instead of answering a silent NA") {
        auto s = strings();
        auto n = bigints();
        auto r = components::vector::compute_binary_arithmetic(&resource, arithmetic_op::add, s, n, count);
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::arithmetics_failure);
    }
    SECTION("vector op vector: the mirrored pair refuses too") {
        auto s = strings();
        auto n = bigints();
        auto r = components::vector::compute_binary_arithmetic(&resource, arithmetic_op::add, n, s, count);
        REQUIRE(r.has_error());
    }
    SECTION("vector op vector: BOOLEAN is not an arithmetic numeric either") {
        auto b = bools();
        auto r = components::vector::compute_binary_arithmetic(&resource, arithmetic_op::add, b, b, count);
        REQUIRE(r.has_error());
    }
    SECTION("vector op scalar: a string scalar refuses") {
        auto n = bigints();
        auto r = components::vector::compute_vector_scalar_arithmetic(&resource,
                                                                      arithmetic_op::multiply,
                                                                      n,
                                                                      logical_value_t{&resource, std::string{"x"}},
                                                                      count);
        REQUIRE(r.has_error());
    }
    SECTION("scalar op vector: a string vector refuses") {
        auto s = strings();
        auto r = components::vector::compute_scalar_vector_arithmetic(&resource,
                                                                      arithmetic_op::multiply,
                                                                      logical_value_t{&resource, int64_t{3}},
                                                                      s,
                                                                      count);
        REQUIRE(r.has_error());
    }
    SECTION("unary negation of a string refuses instead of throwing") {
        auto s = strings();
        auto r = components::vector::compute_unary_neg(&resource, s, count);
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::arithmetics_failure);
    }

    SECTION("numeric pairs still answer, promoted") {
        auto n = bigints();
        vector_t d(&resource, complex_logical_type(logical_type::DOUBLE), count);
        d.set_value(0, logical_value_t{&resource, 1.5});
        d.set_value(1, logical_value_t{&resource, 2.5});
        auto r = components::vector::compute_binary_arithmetic(&resource, arithmetic_op::add, n, d, count);
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value().type().type() == logical_type::DOUBLE);
        CHECK(r.value().value(0).value<double>() == 2.5);
        CHECK(r.value().value(1).value<double>() == 4.5);
    }
    SECTION("unary negation of a numeric still answers") {
        auto n = bigints();
        auto r = components::vector::compute_unary_neg(&resource, n, count);
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value().value(0).value<int64_t>() == -1);
    }
    SECTION("an NA-typed operand is untyped NULL, not a mismatch: it still answers NA") {
        auto n = bigints();
        vector_t na(&resource, complex_logical_type(logical_type::NA), count);
        auto r = components::vector::compute_binary_arithmetic(&resource, arithmetic_op::add, na, n, count);
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value().type().type() == logical_type::NA);
        auto u = components::vector::compute_unary_neg(&resource, na, count);
        REQUIRE_FALSE(u.has_error());
        CHECK(u.value().type().type() == logical_type::NA);
    }
}
