#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit);

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit,
                                                          const std::vector<size_t>& projected_cols);

    // Lower a node_having_t to a dedicated operator_having_t filter over the group's output.
    components::operators::operator_ptr create_plan_having(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node);

    // Descend `root`'s left spine to the physical scan that SOURCES the target table's
    // physical row_ids for an enclosing DELETE/UPDATE and mark exactly that ONE scan mutating,
    // so the owning disk agent retains its streaming cursor past drain (has_active_scan_for_oid
    // then keeps deferring compaction across the capture->apply window). Marking a SINGLE scan
    // per statement is load-bearing: release_mutating_scans clears by (oid, session), so two
    // retained mutating cursors on one (oid, session) would clear each other prematurely — hence
    // USING/FROM/subquery source scans (even on the same oid) are left non-mutating. A matched
    // index_scan needs no mark: it is a one-shot fetch with no cursor, and it only runs on INDEXED
    // tables, which are excluded from every compaction site (tables_without_indexes), so nothing
    // can renumber under its captured ids.
    void mark_mutation_target_scan(const components::operators::operator_ptr& root,
                                   components::catalog::oid_t target_oid);

} // namespace services::planner::impl