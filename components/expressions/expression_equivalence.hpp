#pragma once

#include "expression.hpp"

#include <components/types/parameter_map.hpp>

namespace components::expressions {

    // True when both expressions compute the same value from the same inputs.
    bool
    same_computation(const expression_ptr& lhs, const expression_ptr& rhs, const types::parameter_map_t& parameters);

} // namespace components::expressions
