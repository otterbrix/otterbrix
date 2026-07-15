#pragma once

#include <cstdint>

namespace components::types {

    // SQL three-valued logic.
    //
    // A predicate evaluated over a NULL operand is UNKNOWN, which is distinct from FALSE: the two
    // diverge under NOT (`NOT UNKNOWN` is UNKNOWN, while `NOT FALSE` is TRUE). A filter tree that
    // collapses UNKNOWN into FALSE therefore lets NOT resurrect a NULL row. This is the single
    // vocabulary for that logic, shared by the storage-scan filter and the in-memory predicate
    // evaluator so the two engines cannot drift.
    //
    // Only `yes` (TRUE) admits a row through a WHERE / join / DML predicate; only `no` (FALSE)
    // violates a CHECK constraint. UNKNOWN excludes from a WHERE but satisfies a CHECK.
    enum class tri_bool_t : uint8_t
    {
        no = 0,
        yes = 1,
        unknown = 2
    };

    constexpr tri_bool_t tri_of(bool b) noexcept { return b ? tri_bool_t::yes : tri_bool_t::no; }

    // NOT: TRUE<->FALSE, UNKNOWN is unchanged.
    constexpr tri_bool_t tri_not(tri_bool_t v) noexcept {
        switch (v) {
            case tri_bool_t::yes:
                return tri_bool_t::no;
            case tri_bool_t::no:
                return tri_bool_t::yes;
            default:
                return tri_bool_t::unknown;
        }
    }

    // AND: FALSE dominates (FALSE AND anything = FALSE); otherwise UNKNOWN if either is UNKNOWN.
    constexpr tri_bool_t tri_and(tri_bool_t a, tri_bool_t b) noexcept {
        if (a == tri_bool_t::no || b == tri_bool_t::no) {
            return tri_bool_t::no;
        }
        if (a == tri_bool_t::unknown || b == tri_bool_t::unknown) {
            return tri_bool_t::unknown;
        }
        return tri_bool_t::yes;
    }

    // OR: TRUE dominates (TRUE OR anything = TRUE); otherwise UNKNOWN if either is UNKNOWN.
    constexpr tri_bool_t tri_or(tri_bool_t a, tri_bool_t b) noexcept {
        if (a == tri_bool_t::yes || b == tri_bool_t::yes) {
            return tri_bool_t::yes;
        }
        if (a == tri_bool_t::unknown || b == tri_bool_t::unknown) {
            return tri_bool_t::unknown;
        }
        return tri_bool_t::no;
    }

    // A row passes a WHERE / join / DML predicate only when the predicate is definitely TRUE.
    constexpr bool selects(tri_bool_t v) noexcept { return v == tri_bool_t::yes; }

    // A CHECK constraint is satisfied unless the predicate is definitely FALSE (UNKNOWN passes).
    constexpr bool permits(tri_bool_t v) noexcept { return v != tri_bool_t::no; }

} // namespace components::types
