#pragma once

#include <components/expressions/compare_expression.hpp>

namespace services::planner::impl {

    // Mirror compare_type for key-on-right: swap left/right sense
    inline components::expressions::compare_type mirror_compare(components::expressions::compare_type ct) {
        using components::expressions::compare_type;
        switch (ct) {
            case compare_type::lt:
                return compare_type::gt;
            case compare_type::lte:
                return compare_type::gte;
            case compare_type::gt:
                return compare_type::lt;
            case compare_type::gte:
                return compare_type::lte;
            default:
                return ct; // eq, ne are symmetric
        }
    }

} // namespace services::planner::impl
