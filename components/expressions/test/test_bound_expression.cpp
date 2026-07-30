#include <catch2/catch_test_macros.hpp>

#include <components/expressions/bound/bound_expression.hpp>

#include <memory_resource>

using namespace components;
using namespace components::expressions;
using types::complex_logical_type;
using types::logical_type;
using types::physical_type;

// ============================================================================
// The bound layer's whole point is that a node CARRIES its type instead of the
// rule re-deriving it at execution time. These pin the three things a node must
// answer without touching a row: kind (O(1) dispatch, no RTTI), return type, and
// the physical type CACHED next to it (to_physical_type branches on DECIMAL, so
// recomputing it per row is a branch per cell).
// ============================================================================
TEST_CASE("components::expressions::bound::reference_carries_its_type") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    auto ref = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 3, side_t::left);

    CHECK(ref->kind() == bound_kind::reference);
    CHECK(ref->return_type().type() == logical_type::BIGINT);
    CHECK(ref->physical_type() == physical_type::INT64);
    CHECK(ref->column_index() == 3);
    CHECK(ref->side() == side_t::left);
    CHECK(ref->children().empty());
}

TEST_CASE("components::expressions::bound::physical_type_is_cached_not_recomputed") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    // DECIMAL is the case that matters: to_physical_type() branches on the
    // decimal width, so the answer must be materialised once at bind time.
    auto decimal = types::complex_logical_type::create_decimal(9, 2);
    auto ref = make_bound_reference(&resource, decimal, 0);

    CHECK(ref->physical_type() == decimal.to_physical_type());
    // Same object, asked twice: a cache, not a recomputation with a side effect.
    CHECK(ref->physical_type() == ref->physical_type());
}

// ============================================================================
// A constant OWNS its value. The parsed layer cannot say that: there the value
// lives in the shared parameter map and the expression only holds a slot id.
// ============================================================================
TEST_CASE("components::expressions::bound::constant_owns_its_value") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    auto konst = make_bound_constant(&resource, types::logical_value_t{&resource, int64_t{42}});

    CHECK(konst->kind() == bound_kind::constant);
    CHECK(konst->return_type().type() == logical_type::BIGINT);
    auto stored = konst->value().as<int64_t>();
    REQUIRE_FALSE(stored.has_error());
    CHECK(stored.value() == 42);
}

// ============================================================================
// foldable is a PROPERTY OF THE SUBTREE, not of the node kind: a node folds only
// when every input it reads is fixed at bind time. A reference reads a row and a
// parameter is re-read live, so neither ever folds -- that non-foldability is
// what keeps LATERAL correct.
// ============================================================================
TEST_CASE("components::expressions::bound::foldable_propagates_from_the_leaves") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    auto lit_a = make_bound_constant(&resource, types::logical_value_t{&resource, int64_t{2}});
    auto lit_b = make_bound_constant(&resource, types::logical_value_t{&resource, int64_t{3}});
    CHECK(lit_a->traits().foldable);

    auto folded = make_bound_arithmetic(&resource, vector::arithmetic_op::add, lit_a, lit_b);
    REQUIRE_FALSE(folded.has_error());
    CHECK(folded.value()->traits().foldable);

    auto ref = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 0);
    CHECK_FALSE(ref->traits().foldable);
    auto unfolded = make_bound_arithmetic(&resource, vector::arithmetic_op::add, ref, lit_b);
    REQUIRE_FALSE(unfolded.has_error());
    CHECK_FALSE(unfolded.value()->traits().foldable);

    auto param = make_bound_parameter(&resource, core::parameter_id_t{1}, complex_logical_type{logical_type::BIGINT});
    CHECK_FALSE(param->traits().foldable);
    auto with_param = make_bound_arithmetic(&resource, vector::arithmetic_op::add, ref, param);
    REQUIRE_FALSE(with_param.has_error());
    CHECK_FALSE(with_param.value()->traits().foldable);
}

// ============================================================================
// copy() must rehome the whole subtree, INCLUDING a constant's value: a
// logical_value_t copied by the defaulted copy keeps its source resource, so a
// tree copied onto an arena would keep allocating on the old one.
// ============================================================================
TEST_CASE("components::expressions::bound::copy_rehomes_the_subtree") {
    std::pmr::monotonic_buffer_resource source_arena;
    std::pmr::monotonic_buffer_resource target_arena;

    auto ref = make_bound_reference(&source_arena, complex_logical_type{logical_type::BIGINT}, 1);
    auto lit = make_bound_constant(&source_arena, types::logical_value_t{&source_arena, int64_t{7}});
    auto sum = make_bound_arithmetic(&source_arena, vector::arithmetic_op::add, ref, lit);
    REQUIRE_FALSE(sum.has_error());

    auto copy = sum.value()->copy(&target_arena);
    REQUIRE(copy);
    CHECK(copy->kind() == bound_kind::arithmetic);
    CHECK(copy->resource() == &target_arena);
    CHECK(copy->return_type() == sum.value()->return_type());
    REQUIRE(copy->children().size() == 2);
    CHECK(copy->children()[0]->resource() == &target_arena);
    CHECK(copy->children()[1]->resource() == &target_arena);
    const auto* copied_const = static_cast<const bound_constant_t*>(copy->children()[1].get());
    CHECK(copied_const->value().resource() == &target_arena);
}

// ============================================================================
// The arithmetic node's return_type_ must equal what the kernel will actually
// produce. FLOAT x INT32 is the combination M4 fixed a heap overflow on: the
// kernel used to demote FLOAT to DOUBLE so the 8-byte write fitted. A bound node
// that claimed DOUBLE while the kernel wrote FLOAT would put that overflow back.
// ============================================================================
TEST_CASE("components::expressions::bound::arithmetic_return_type_matches_the_kernel_rule") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    struct pair_t {
        logical_type left;
        logical_type right;
    };
    const pair_t pairs[] = {{logical_type::INTEGER, logical_type::BIGINT},
                            {logical_type::FLOAT, logical_type::INTEGER},
                            {logical_type::INTEGER, logical_type::FLOAT},
                            {logical_type::FLOAT, logical_type::DOUBLE},
                            {logical_type::TINYINT, logical_type::SMALLINT},
                            {logical_type::FLOAT, logical_type::FLOAT}};

    for (const auto& p : pairs) {
        auto l = make_bound_reference(&resource, complex_logical_type{p.left}, 0);
        auto r = make_bound_reference(&resource, complex_logical_type{p.right}, 1);
        auto node = make_bound_arithmetic(&resource, vector::arithmetic_op::add, l, r);
        REQUIRE_FALSE(node.has_error());
        CHECK(node.value()->return_type().type() == types::arithmetic_result_type(p.left, p.right, vector::arithmetic_op::add));
    }

    // FLOAT stays FLOAT: 4-byte result slot, 4-byte writes.
    auto l = make_bound_reference(&resource, complex_logical_type{logical_type::FLOAT}, 0);
    auto r = make_bound_reference(&resource, complex_logical_type{logical_type::INTEGER}, 1);
    auto node = make_bound_arithmetic(&resource, vector::arithmetic_op::add, l, r);
    REQUIRE_FALSE(node.has_error());
    CHECK(node.value()->return_type().type() == logical_type::FLOAT);
    CHECK(node.value()->physical_type() == physical_type::FLOAT);
}

// An operand pair the kernels have no arithmetic for is an ERROR AT BIND TIME,
// answered through result_wrapper_t. Never an exception (rules 2 and 9), and
// never a node that claims a type the kernel will not produce.
TEST_CASE("components::expressions::bound::arithmetic_on_a_non_numeric_pair_is_a_bind_error") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    auto l = make_bound_reference(&resource, complex_logical_type{logical_type::STRING_LITERAL}, 0);
    auto r = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 1);
    auto node = make_bound_arithmetic(&resource, vector::arithmetic_op::multiply, l, r);
    REQUIRE(node.has_error());
    CHECK(node.error().type == core::error_code_t::arithmetics_failure);
}

// A comparison is BOOLEAN whatever its operands are, and a conjunction of
// comparisons is BOOLEAN too -- the executor's select() depends on it.
TEST_CASE("components::expressions::bound::predicates_are_boolean") {
    auto resource = std::pmr::monotonic_buffer_resource{};

    auto l = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 0);
    auto lit = make_bound_constant(&resource, types::logical_value_t{&resource, int64_t{5}});
    auto cmp = make_bound_comparison(&resource, compare_type::gt, l, lit);
    REQUIRE_FALSE(cmp.has_error());
    CHECK(cmp.value()->return_type().type() == logical_type::BOOLEAN);
    CHECK(cmp.value()->physical_type() == physical_type::BOOL);

    std::pmr::vector<bound_expression_ptr> args{&resource};
    args.push_back(cmp.value());
    auto conj = make_bound_conjunction(&resource, compare_type::union_not, std::move(args));
    REQUIRE_FALSE(conj.has_error());
    CHECK(conj.value()->return_type().type() == logical_type::BOOLEAN);
    CHECK(conj.value()->kind() == bound_kind::conjunction);
}
