#pragma once

#include "expression.hpp"

#include <core/parameter_id.hpp>

#include <memory_resource>
#include <unordered_map>

namespace components::expressions {

    // old parameter id -> the id the same value was re-registered under
    using parameter_id_map_t = std::pmr::unordered_map<core::parameter_id_t, core::parameter_id_t>;

    // Rewrite every parameter_id_t operand of `expr` (and of its whole operand
    // subtree) through `id_map`, IN PLACE. An id that is not in the map is left
    // alone.
    //
    // Why in place rather than by cloning: the tree being renumbered was just
    // built by its own transformer and is not shared with anything yet, and the
    // renumbering has to survive as the SAME node objects because the caller has
    // already wired them into the outer plan.
    //
    // Why it is needed at all: parameter_node_t::counter_ starts at 0 in every
    // plan, so a freshly transformed view body numbers its constants #0, #1, ...
    // exactly like the outer query does. Merging the two parameter maps without
    // renumbering lets the outer query's constant overwrite the view body's —
    // `SELECT * FROM v WHERE col_b > 18` over `... WHERE col_b > 10` would run the
    // body's predicate against 18. That is a silently wrong answer, not a crash.
    //
    // The walk mirrors clone_expression's, plus aggregate_expression_t::child()
    // (the resolved call node), which clone_expression does not visit.
    void remap_parameter_ids(const expression_ptr& expr, const parameter_id_map_t& id_map);

} // namespace components::expressions
