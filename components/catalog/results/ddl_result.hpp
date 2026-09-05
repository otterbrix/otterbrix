#pragma once

#include <cstdint>

namespace components::catalog {

    // What a DROP statement said about its dependents. Two values, PostgreSQL
    // parity (owner decision 2026-09-05, GitHub #638): the grammar's
    // opt_drop_behavior yields DROP_RESTRICT for a written RESTRICT and for the
    // empty alternative alike — in PostgreSQL the two ARE the same thing, so no
    // third "unwritten" value exists to carry. The one implicitly-CASCADE
    // statement form, DROP DATABASE, takes no behavior in the grammar and is
    // stamped cascade_ by its transform instead (transform_database.cpp).
    enum class drop_behavior_t : std::uint8_t
    {
        // RESTRICT, written or defaulted: refuse if a blocking dependency exists.
        restrict_ = 0,
        // The statement wrote CASCADE: drop the dependents too.
        cascade_ = 1,
    };

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
