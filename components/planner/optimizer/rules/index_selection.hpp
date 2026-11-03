#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/index_facts.hpp>
#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // The compare that reads the same with its operands swapped: `$1 < key` is `key > $1`.
    expressions::compare_type mirror_compare(expressions::compare_type compare) noexcept;

    // Stamps every WHERE an index answers (node_match_t::set_index): a WHERE of one compare, not a conjunction,
    // that compares a column of its table with a parameter (either side), over a single-column index of that
    // table; a range needs an index other than a hashed one. A stamped WHERE is never offered to the table's
    // storage: plan generation reads it through the index.
    void select_indexes(const logical_plan::node_ptr& root, const logical_plan::index_facts_t& indexes);

} // namespace components::planner::optimizer
