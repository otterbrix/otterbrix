#pragma once

#include <components/table/column_state.hpp>
#include <memory_resource>

namespace components::operators::predicates {

    // Agent-side glue between a table::expression_filter_t (which crosses the mailbox carrying only
    // the compare + referenced paths + a parameter snapshot) and the predicate machinery that
    // actually evaluates it. Walk a table_filter_t tree and attach a per-row evaluator to every
    // expression_filter_t found (recursing through conjunction_filter_t children). A value_getter
    // closure captures the agent's memory resource + function registry, neither of which can travel
    // through a message, so the evaluator MUST be built here, on the owning agent. No-op (and no
    // function registry built) when the tree holds no expression filter.
    void attach_expression_evaluators(std::pmr::memory_resource* resource, table::table_filter_t* filter);

} // namespace components::operators::predicates
