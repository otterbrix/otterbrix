#include <catch2/catch_test_macros.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

using namespace components::expressions;
using key = components::expressions::key_t;

// ============================================================================
// A key_t must stay on the resource it was built on across a COPY.
//
// The pmr allocator's select_on_container_copy_construction returns a
// default-constructed allocator, so a defaulted copy constructor silently
// re-homes both pmr vector members onto the process-default resource — the copy
// escapes the arena it was supposed to live in. Copy-ASSIGNMENT does not have
// this property (pmr assignment keeps the destination's allocator), so both
// directions are pinned here.
// ============================================================================
TEST_CASE("components::expression::key::copy_construction_keeps_the_resource") {
    std::pmr::monotonic_buffer_resource arena;

    key original{&arena, "field"};
    REQUIRE(original.resource() == &arena);

    key copy{original}; // NOLINT(performance-unnecessary-copy-initialization)
    CHECK(copy.resource() == &arena);
    CHECK(copy.storage() == original.storage());
}

TEST_CASE("components::expression::key::copy_construction_keeps_the_resource_for_the_path") {
    std::pmr::monotonic_buffer_resource arena;

    key original{&arena, "field"};
    std::pmr::vector<size_t> path{&arena};
    path.push_back(3);
    original.set_path(std::move(path));
    REQUIRE(original.path().get_allocator().resource() == &arena);

    key copy{original};
    CHECK(copy.path().get_allocator().resource() == &arena);
    REQUIRE(copy.path().size() == 1);
    CHECK(copy.path()[0] == 3);
}

TEST_CASE("components::expression::key::copy_assignment_keeps_the_destination_resource") {
    std::pmr::monotonic_buffer_resource arena_a;
    std::pmr::monotonic_buffer_resource arena_b;

    key source{&arena_a, "field"};
    key destination{&arena_b, "other"};
    destination = source;
    CHECK(destination.resource() == &arena_b);
    CHECK(destination.storage() == source.storage());
}

// make_sort_expression has NO resource parameter — it copies the key straight
// into sort_expression_t, so the copy constructor is the only thing keeping that
// key on the caller's arena.
TEST_CASE("components::expression::key::sort_expression_keeps_the_key_resource") {
    std::pmr::monotonic_buffer_resource arena;

    key original{&arena, "field"};
    auto expr = make_sort_expression(original, sort_order::asc);
    CHECK(expr->key().resource() == &arena);
}

// ============================================================================
// Identity must include the cast type and the '::?' variant-select flag.
//
// `val`, `val::string` and `val::?string` address DIFFERENT columns/values;
// comparing (and hashing) only storage_ collapses them into one key.
// ============================================================================
TEST_CASE("components::expression::key::cast_type_discriminates_identity") {
    auto resource = core::pmr::otterbrix_resource();

    key plain{&resource, "val"};
    key cast_string{&resource, "val"};
    cast_string.set_cast_type(components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});

    CHECK_FALSE(plain == cast_string);
    CHECK(plain != cast_string);
    CHECK(plain.hash() != cast_string.hash());
}

TEST_CASE("components::expression::key::cast_type_value_discriminates_identity") {
    auto resource = core::pmr::otterbrix_resource();

    key cast_string{&resource, "val"};
    cast_string.set_cast_type(components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    key cast_bigint{&resource, "val"};
    cast_bigint.set_cast_type(components::types::complex_logical_type{components::types::logical_type::BIGINT});

    CHECK_FALSE(cast_string == cast_bigint);
    CHECK(cast_string.hash() != cast_bigint.hash());
}

TEST_CASE("components::expression::key::variant_select_discriminates_identity") {
    auto resource = core::pmr::otterbrix_resource();

    key cast_only{&resource, "val"};
    cast_only.set_cast_type(components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    key variant{&resource, "val"};
    variant.set_cast_type(components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    variant.set_variant_select(true);

    CHECK_FALSE(cast_only == variant);
    CHECK(cast_only.hash() != variant.hash());
}

TEST_CASE("components::expression::key::equal_keys_stay_equal") {
    auto resource = core::pmr::otterbrix_resource();

    key a{&resource, "val"};
    key b{&resource, "val"};
    CHECK(a == b);
    CHECK(a.hash() == b.hash());

    a.set_cast_type(components::types::complex_logical_type{components::types::logical_type::BIGINT});
    b.set_cast_type(components::types::complex_logical_type{components::types::logical_type::BIGINT});
    CHECK(a == b);
    CHECK(a.hash() == b.hash());

    a.set_variant_select(true);
    b.set_variant_select(true);
    CHECK(a == b);
    CHECK(a.hash() == b.hash());

    // A copy is always identity-equal to its original.
    key copy{a};
    CHECK(copy == a);
    CHECK(copy.hash() == a.hash());
}
