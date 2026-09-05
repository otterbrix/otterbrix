#pragma once

#include <cstdint>

namespace components::catalog {

    // What a DROP statement SAID about its dependents. Three forms, because the
    // grammar has three: `RESTRICT`, `CASCADE`, and the empty alternative
    // (components/sql/parser/gram.y, opt_drop_behavior).
    //
    // The empty alternative is NOT the same thing as either written word, and
    // collapsing it onto one of them is what makes a written word unenforceable:
    // with only two forms, a build that wants bare `DROP TABLE t` to keep meaning
    // CASCADE has to hardcode cascade_ everywhere, and then a statement that
    // explicitly says RESTRICT gets cascaded too — an operator that says RESTRICT
    // and cascades is lying about what it did.
    enum class drop_behavior_t : std::uint8_t
    {
        // The statement wrote RESTRICT: refuse if a blocking dependency exists.
        restrict_ = 0,
        // The statement wrote CASCADE: drop the dependents too.
        cascade_ = 1,
        // The statement wrote NEITHER. Owner decision (2026-09-04): this build
        // keeps resolving it to CASCADE, so the tree's ~205 bare DROP statements
        // do not change meaning under this wave; moving the default to
        // PostgreSQL's RESTRICT is tracked as GitHub #638. It is a SEPARATE value
        // rather than an alias so that (a) the resolution lives in exactly one
        // place — refuses_on_dependency below — instead of being re-decided at
        // every call site, and (b) flipping it for #638 is one edit, not a search
        // for which cascade_ meant "the user asked" and which meant "nobody did".
        unspecified = 2,
    };

    // The single place the three forms collapse into the two things a planner can
    // do. A behavior refuses the statement when a blocking dependency exists only
    // if the user WROTE RESTRICT; everything else — CASCADE, and the unwritten
    // form per the owner decision above — drops the dependents.
    inline constexpr bool refuses_on_dependency(drop_behavior_t b) noexcept {
        return b == drop_behavior_t::restrict_;
    }

    enum class ddl_status : std::uint8_t
    {
        ok = 0,
        restrict_blocked = 1,
        cycle_detected = 2, // pg_depend back-edge encountered during cascade DFS
    };

} // namespace components::catalog
