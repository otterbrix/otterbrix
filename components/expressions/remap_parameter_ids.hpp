#pragma once

#include "expression.hpp"

#include <core/parameter_id.hpp>

#include <memory_resource>
#include <unordered_map>

namespace components::expressions {

    // old parameter id -> the id the same value was re-registered under
    using parameter_id_map_t = std::pmr::unordered_map<core::parameter_id_t, core::parameter_id_t>;

    // Rewrites `expr`'s parameter_id_t operands in place via `id_map` (ids absent from the map are
    // untouched); must mutate in place since the tree is already wired into the outer plan. Needed
    // because each plan's parameter counter restarts at 0, so an unmapped merge can let the outer
    // query's constant silently overwrite a view body's own (wrong answer, not a crash).
    void remap_parameter_ids(const expression_ptr& expr, const parameter_id_map_t& id_map);

} // namespace components::expressions
