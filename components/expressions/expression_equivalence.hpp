#pragma once

#include "expression.hpp"

#include <components/vector/storage_parameters.hpp>

namespace components::expressions {

    // True when both expressions compute the same value from the same inputs.
    bool same_computation(const expression_ptr& lhs,
                          const expression_ptr& rhs,
                          const vector::storage_parameters& parameters);

} // namespace components::expressions
