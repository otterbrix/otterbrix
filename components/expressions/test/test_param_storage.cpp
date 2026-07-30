// param_storage is a hand-written tagged union (rule 14). It replaced a
// std::variant<parameter_id_t, key_t, expression_ptr> and had to keep that variant's
// observable behaviour byte for byte — including two parts that surprise on first reading:
//
//   * assigning one key over another MOVES NOTHING. key_t declares no move-assignment (its
//     user-declared copy-assign suppresses the implicit one), so an rvalue key binds to the
//     copy: the destination keeps its own memory resource and the source survives intact.
//   * a cross-alternative assignment builds the new value BEFORE destroying the old one, so
//     the object is never left empty. That is what made the variant's valueless_by_exception
//     state unreachable for this alternative set, and losing it would introduce a failure
//     mode that never existed here.
//
// Both are pinned below, along with the layout, the converting constructors the tree relies
// on, and the equality/hash contracts.

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <memory_resource>
#include <sstream>

using namespace components::expressions;
using ekey = components::expressions::key_t;

TEST_CASE("components::expressions::param_storage::layout_and_default") {
    // The union is sized by key_t; the tag lands in its tail padding, so the whole thing is
    // exactly the 112 bytes the std::variant occupied.
    STATIC_REQUIRE(sizeof(param_storage) == 112);
    STATIC_REQUIRE(alignof(param_storage) == 8);
    // std::pmr::vector<param_storage> reallocates by moving only while this holds.
    STATIC_REQUIRE(std::is_nothrow_move_constructible_v<param_storage>);

    // Alternative 0, as the variant defaulted. key_t is not default-constructible, so the
    // order of the alternatives is load-bearing.
    param_storage p{};
    REQUIRE(is_parameter(p));
    REQUIRE_FALSE(is_key(p));
    REQUIRE_FALSE(is_expr(p));
    REQUIRE(as_parameter(p) == core::parameter_id_t{0});
}

TEST_CASE("components::expressions::param_storage::converting_construction") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    SECTION("from a parameter id") {
        param_storage p = core::parameter_id_t{7};
        REQUIRE(is_parameter(p));
        REQUIRE(as_parameter(p) == core::parameter_id_t{7});
    }

    SECTION("from a key, by lvalue and by rvalue") {
        ekey k{res, "a"};
        param_storage from_lvalue = k;
        REQUIRE(is_key(from_lvalue));
        REQUIRE(as_key(from_lvalue).as_string() == "a");
        // The source still holds its own key.
        REQUIRE(k.as_string() == "a");

        param_storage from_rvalue = ekey{res, "b"};
        REQUIRE(is_key(from_rvalue));
        REQUIRE(as_key(from_rvalue).as_string() == "b");
    }

    SECTION("from nullptr — the empty operand of a unary compare") {
        param_storage p = nullptr;
        REQUIRE(is_expr(p));
        REQUIRE(as_expr(p) == nullptr);
    }

    SECTION("from a DERIVED handle, in one user-defined conversion") {
        // This is the shape ~20 call sites use (append_param(scalar_expression_ptr) and
        // friends). A plain param_storage(expression_ptr) would need two user-defined
        // conversions here and would not compile.
        auto scalar = make_scalar_expression(res, scalar_type::get_field, ekey{res, "c"});
        param_storage p = scalar;
        REQUIRE(is_expr(p));
        REQUIRE(as_expr(p).get() == scalar.get());
    }
}

TEST_CASE("components::expressions::param_storage::copy_homes_a_key_on_the_sources_resource") {
    std::pmr::synchronized_pool_resource source_resource;
    std::pmr::synchronized_pool_resource other_resource;

    param_storage original = ekey{&source_resource, "a"};
    param_storage copy = original;

    REQUIRE(is_key(copy));
    REQUIRE(as_key(copy).as_string() == "a");
    // key_t's copy constructor is deliberately not `= default`: a copy stays on the resource
    // the original lives on, instead of silently escaping to the process default.
    REQUIRE(as_key(copy).resource() == &source_resource);
    REQUIRE(as_key(copy).resource() != &other_resource);
}

TEST_CASE("components::expressions::param_storage::move_assignment_between_keys_copies") {
    std::pmr::synchronized_pool_resource destination_resource;
    std::pmr::synchronized_pool_resource source_resource;

    param_storage destination = ekey{&destination_resource, "dst"};
    param_storage source = ekey{&source_resource, "src"};

    destination = std::move(source);

    REQUIRE(is_key(destination));
    REQUIRE(as_key(destination).as_string() == "src");
    // Copy-assignment: the destination keeps ITS resource...
    REQUIRE(as_key(destination).resource() == &destination_resource);
    // ...and the source is not emptied, because key_t has no move-assignment to empty it.
    REQUIRE(is_key(source)); // NOLINT(bugprone-use-after-move): the point of the test
    REQUIRE(as_key(source).as_string() == "src");
}

TEST_CASE("components::expressions::param_storage::cross_alternative_assignment_releases_the_old_one") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    auto scalar = make_scalar_expression(res, scalar_type::get_field, ekey{res, "a"});
    param_storage slot = scalar;
    // `slot` and the local handle share ownership.
    REQUIRE(scalar->use_count() == 2);

    // This is constant_folding's promotion of a folded expression to a parameter id: the
    // assignment destroys the held handle, which is why the id must be read out by value
    // first when it lives inside the expression being dropped.
    slot = core::parameter_id_t{3};

    REQUIRE(is_parameter(slot));
    REQUIRE(as_parameter(slot) == core::parameter_id_t{3});
    REQUIRE(scalar->use_count() == 1);
}

TEST_CASE("components::expressions::param_storage::null_expression_operand_hashes_and_streams") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    // The empty operand of a unary/union compare: kind is expression, handle is null.
    // Every union AND/OR/NOT compare holds exactly this state in left_ and right_, and
    // compare_expression_t::hash_impl folds both unconditionally — so the hash (noexcept)
    // and the stream operator must not dereference the null handle.
    param_storage p = nullptr;
    REQUIRE(is_expr(p));

    const std::hash<param_storage> hasher;
    REQUIRE(hasher(p) == hasher(param_storage{nullptr}));

    std::stringstream stream;
    stream << p;
    REQUIRE(stream.str() == "null");

    // The production shape: hashing a union compare built by the 2-arg factory.
    auto cmp = make_compare_union_expression(res, compare_type::union_and);
    REQUIRE(cmp->hash() == cmp->hash());
}

TEST_CASE("components::expressions::param_storage::self_assignment_is_a_no_op") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    // Both operators carry an explicit this == &other branch; dropping it in a refactor
    // would destroy the held key/handle before reading it — silent UB this suite exists
    // to catch. The reference alias keeps -Wself-assign-overloaded quiet.
    SECTION("copy, key alternative") {
        param_storage p = ekey{res, "a"};
        auto& same = p;
        p = same;
        REQUIRE(is_key(p));
        REQUIRE(as_key(p).as_string() == "a");
    }

    SECTION("move, key alternative") {
        param_storage p = ekey{res, "a"};
        auto& same = p;
        p = std::move(same);
        REQUIRE(is_key(p)); // NOLINT(bugprone-use-after-move): the point of the test
        REQUIRE(as_key(p).as_string() == "a");
    }

    SECTION("move, expression alternative keeps the refcount") {
        auto scalar = make_scalar_expression(res, scalar_type::get_field, ekey{res, "a"});
        param_storage p = scalar;
        REQUIRE(scalar->use_count() == 2);
        auto& same = p;
        p = std::move(same);
        REQUIRE(is_expr(p)); // NOLINT(bugprone-use-after-move): the point of the test
        REQUIRE(as_expr(p).get() == scalar.get());
        REQUIRE(scalar->use_count() == 2);
    }
}

TEST_CASE("components::expressions::param_storage::cross_alternative_move_assignment_with_a_key") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    SECTION("parameter slot becomes a key: the incoming key is MOVED from") {
        param_storage destination = core::parameter_id_t{9};
        param_storage source = ekey{res, "col"};

        destination = std::move(source);

        REQUIRE(is_key(destination));
        REQUIRE(as_key(destination).as_string() == "col");
        // Cross-alternative move really moves (unlike the same-alternative case, where
        // key_t's suppressed move-assignment degrades to a copy): the source key's
        // storage was stolen by key_t's move constructor.
        REQUIRE(is_key(source)); // NOLINT(bugprone-use-after-move): the point of the test
        REQUIRE(as_key(source).is_null());
    }

    SECTION("key slot becomes an expression, the key is destroyed") {
        auto scalar = make_scalar_expression(res, scalar_type::get_field, ekey{res, "a"});
        param_storage slot = ekey{res, "old"};

        slot = param_storage{scalar};

        REQUIRE(is_expr(slot));
        REQUIRE(as_expr(slot).get() == scalar.get());
        REQUIRE(scalar->use_count() == 2);
    }
}

TEST_CASE("components::expressions::param_storage::equality_is_per_alternative") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    param_storage id0 = core::parameter_id_t{0};
    param_storage id1 = core::parameter_id_t{1};
    param_storage key_a = ekey{res, "a"};
    param_storage key_a_again = ekey{res, "a"};
    param_storage key_b = ekey{res, "b"};

    REQUIRE(id0 == param_storage{core::parameter_id_t{0}});
    REQUIRE(id0 != id1);
    REQUIRE(key_a == key_a_again);
    REQUIRE(key_a != key_b);
    // Different alternatives are never equal — a parameter id of 0 is not the key "a".
    REQUIRE(id0 != key_a);

    // A nested expression compares by HANDLE IDENTITY, as the variant's operator== did: two
    // structurally identical expressions are two different operands.
    auto left = make_scalar_expression(res, scalar_type::get_field, ekey{res, "a"});
    auto right = make_scalar_expression(res, scalar_type::get_field, ekey{res, "a"});
    param_storage from_left = left;
    param_storage from_right = right;
    REQUIRE(*left == *right);
    REQUIRE(from_left != from_right);
    REQUIRE(from_left == param_storage{left});
}

TEST_CASE("components::expressions::param_storage::hash_agrees_with_equality") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    const std::hash<param_storage> hasher;

    REQUIRE(hasher(param_storage{core::parameter_id_t{5}}) == hasher(param_storage{core::parameter_id_t{5}}));
    REQUIRE(hasher(param_storage{ekey{res, "a"}}) == hasher(param_storage{ekey{res, "a"}}));

    auto scalar = make_scalar_expression(res, scalar_type::get_field, ekey{res, "a"});
    REQUIRE(hasher(param_storage{scalar}) == hasher(param_storage{scalar}));
    REQUIRE(hasher(param_storage{scalar}) == scalar->hash());
}

TEST_CASE("components::expressions::param_storage::survives_vector_growth") {
    std::pmr::synchronized_pool_resource resource;
    auto* res = &resource;

    // Reallocation moves the elements (the nothrow-move static_assert above is what lets it),
    // so every alternative has to come out the other side intact.
    std::pmr::vector<param_storage> params(res);
    for (uint16_t i = 0; i < 64; ++i) {
        params.emplace_back(core::parameter_id_t{i});
        params.emplace_back(ekey{res, "col"});
        params.emplace_back(nullptr);
    }

    REQUIRE(params.size() == 192);
    for (uint16_t i = 0; i < 64; ++i) {
        REQUIRE(is_parameter(params[3 * i]));
        REQUIRE(as_parameter(params[3 * i]) == core::parameter_id_t{i});
        REQUIRE(is_key(params[3 * i + 1]));
        REQUIRE(as_key(params[3 * i + 1]).as_string() == "col");
        REQUIRE(is_expr(params[3 * i + 2]));
        REQUIRE(as_expr(params[3 * i + 2]) == nullptr);
    }
}
