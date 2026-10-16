#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Narrows every scan to the columns the query reads from it (if possible),
    // and rebases the plan onto the narrowed shema
    //
    // Must run AFTER validate_schema (paths are resolved to column ordinals) and after every rule that moves
    // keys across scans or joins
    void prune_columns(const logical_plan::node_ptr& root);

} // namespace components::planner::optimizer
