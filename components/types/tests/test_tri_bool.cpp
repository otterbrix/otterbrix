#include <catch2/catch_test_macros.hpp>
#include <components/types/tri_bool.hpp>

using components::types::permits;
using components::types::selects;
using components::types::tri_and;
using components::types::tri_bool_t;
using components::types::tri_not;
using components::types::tri_of;
using components::types::tri_or;

namespace {
    constexpr auto T = tri_bool_t::yes;
    constexpr auto F = tri_bool_t::no;
    constexpr auto U = tri_bool_t::unknown;

    // The whole point of three-valued logic is that UNKNOWN is not FALSE: NOT UNKNOWN stays
    // UNKNOWN. These truth tables are the contract every 3VL consumer relies on, pinned at
    // compile time so a refactor of the combinators cannot silently change them.

    static_assert(tri_of(true) == T);
    static_assert(tri_of(false) == F);

    static_assert(tri_not(T) == F);
    static_assert(tri_not(F) == T);
    static_assert(tri_not(U) == U); // the load-bearing case

    // AND: FALSE dominates; UNKNOWN otherwise absorbs.
    static_assert(tri_and(T, T) == T);
    static_assert(tri_and(T, F) == F);
    static_assert(tri_and(F, T) == F);
    static_assert(tri_and(F, F) == F);
    static_assert(tri_and(T, U) == U);
    static_assert(tri_and(U, T) == U);
    static_assert(tri_and(F, U) == F); // FALSE wins over UNKNOWN
    static_assert(tri_and(U, F) == F);
    static_assert(tri_and(U, U) == U);

    // OR: TRUE dominates; UNKNOWN otherwise absorbs.
    static_assert(tri_or(T, T) == T);
    static_assert(tri_or(T, F) == T);
    static_assert(tri_or(F, T) == T);
    static_assert(tri_or(F, F) == F);
    static_assert(tri_or(T, U) == T); // TRUE wins over UNKNOWN
    static_assert(tri_or(U, T) == T);
    static_assert(tri_or(F, U) == U);
    static_assert(tri_or(U, F) == U);
    static_assert(tri_or(U, U) == U);

    // A WHERE / join / DML predicate admits a row only on TRUE.
    static_assert(selects(T));
    static_assert(!selects(F));
    static_assert(!selects(U));

    // A CHECK constraint is violated only on FALSE; UNKNOWN passes.
    static_assert(permits(T));
    static_assert(!permits(F));
    static_assert(permits(U));
} // namespace

TEST_CASE("types::tri_bool::truth_tables") {
    // The contract is enforced by the static_asserts above; this run-time case makes the
    // coverage visible to the test harness and guards De Morgan / double-negation identities.
    for (auto a : {T, F, U}) {
        CHECK(tri_not(tri_not(a)) == a);
        for (auto b : {T, F, U}) {
            CHECK(tri_and(a, b) == tri_and(b, a)); // commutative
            CHECK(tri_or(a, b) == tri_or(b, a));
            // De Morgan: NOT(a AND b) == (NOT a) OR (NOT b)
            CHECK(tri_not(tri_and(a, b)) == tri_or(tri_not(a), tri_not(b)));
            CHECK(tri_not(tri_or(a, b)) == tri_and(tri_not(a), tri_not(b)));
        }
    }
}
