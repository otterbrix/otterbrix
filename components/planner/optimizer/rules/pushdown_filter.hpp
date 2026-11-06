#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    logical_plan::node_ptr pushdown_filter(std::pmr::memory_resource* resource, logical_plan::node_ptr node);

    // Push a consumer's WHERE conjuncts INTO an inlined single-table CTE / FROM-subquery
    // body (a table-scan aggregate) so the filter reaches the base scan. Runs BEFORE
    // pushdown_filter, on the original tree.
    logical_plan::node_ptr pushdown_cte_filter(std::pmr::memory_resource* resource, logical_plan::node_ptr node);

} // namespace components::planner::optimizer
