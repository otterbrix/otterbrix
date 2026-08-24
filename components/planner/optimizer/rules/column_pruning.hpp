#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>

namespace components::planner::optimizer {

    // Walks a logical plan that has been path-resolved by the schema validator and
    // computes, for every node_aggregate_t, the set of column indices it needs to
    // read from its source. Writes the result via node_aggregate_t::set_projected_cols.
    //
    // Must run AFTER validate_schema (paths must be resolved to column indices)
    // and AFTER enrich (so consumer nodes carry table_oid).
    //
    // Schema info comes from `resolves` — the plan's resolved table entries. Null
    // when the plan depends on no tables, in which case no aggregate can be pruned
    // against a known column count.
    //
    // Handles:
    //   * plain SELECT / SELECT ... WHERE — collects from group_t + match_t
    //   * GROUP BY with aggregates         — same code path
    //   * JOIN                              — splits per-side, adds JOIN condition columns
    //   * Nested subquery                  — each level computes its own projection
    //
    // Falls back to "no projection" (empty vector) when:
    //   * wildcard (SELECT *) or unresolved path is seen
    //   * WHERE contains function expressions whose referenced columns cannot be
    //     statically enumerated
    void prune_columns(const logical_plan::node_ptr& root, const logical_plan::catalog_resolves_t* resolves);

} // namespace components::planner::optimizer