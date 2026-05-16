#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Walks a logical plan that has been path-resolved by the schema validator and
    // computes, for every node_aggregate_t, the set of column indices it needs to
    // read from its source. Writes the result via node_aggregate_t::set_projected_cols.
    //
    // Must run AFTER validate_schema (paths must be resolved to column indices)
    // and AFTER stamp_oids_from_resolves (so consumer nodes carry table_oid).
    //
    // Schema info is read from sibling catalog_resolve_table_t nodes inside
    // each sequence_t — the optimizer is self-contained and needs no external
    // catalog handle.
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
    void prune_columns(const logical_plan::node_ptr& root);

} // namespace components::planner::optimizer