#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Partial (eager) aggregation pushdown through an INNER equi-join (Yan & Larson).
    //
    // Rewrites  aggregate{ join(A, B) ON A.k=B.k, group(g, MIN/MAX(x)) }  into
    //           aggregate{ join(A', B) ON A'.k=B.k, group(g, MIN/MAX(px)) }
    // where A' = aggregate{ scan A, group(g, k, MIN/MAX(x)->px) } is a PARTIAL
    // aggregate pushed onto the join side that holds every group key and every
    // aggregate argument. The join key is ADDED to the partial grouping so the
    // inner-join drop semantics commute with the partial reduce; the FINAL group
    // above the join re-aggregates the partial columns (MIN(MIN)=MIN, MAX(MAX)=MAX).
    //
    // SOUNDNESS ENVELOPE (only fires when provably correct — false negatives only):
    //   * The source is an INNER join lowered to a single equi-key hash join.
    //   * Both join children carry a validator-stamped output schema (frame widths).
    //   * Every group key is a plain top-level column; every aggregate is a
    //     non-DISTINCT, mergeable MIN or MAX over a single top-level column.
    //   * ALL group keys AND all aggregate arguments live on ONE side of the join
    //     (no cross-side measure / key).
    //   * The pushed side is a bare single-table scan aggregate (optionally with a
    //     WHERE) — never something already grouped / limited / distinct.
    //
    // MIN and MAX are the ONLY aggregates handled here because they are IDEMPOTENT
    // under the row duplication an equi-join can introduce (m matching rows on the
    // other side repeat the same partial extremum, which MIN/MAX absorb). SUM /
    // COUNT / AVG would over-count on duplication and are sound only when the OTHER
    // side is key-unique on the join column — a property the current SELECT-plan
    // metadata does not expose — so they are deliberately EXCLUDED.
    //
    // ANNOTATION-FREE STRUCTURAL rewrite; runs AFTER rewrite_hash_joins (so the
    // equi-key indices are settled and can be re-stamped) and BEFORE
    // pushdown_aggregate + column_pruning (so the partial gets the normal
    // single-table lowering and the join frame-split stays consistent).
    logical_plan::node_ptr eager_aggregation(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer
