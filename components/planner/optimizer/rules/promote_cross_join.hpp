#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Promote a comma-join (SQL-89 `FROM a, b WHERE a.k = b.k`) from a nested-loop
    // CROSS join into an INNER join carrying the equi-key on its ON condition, so
    // rewrite_hash_joins (which runs after) can lower it to a hash join.
    //
    // Shape targeted: an aggregate_t whose children()[0] is a node_join_t{cross}
    // with a sibling match_t. The comma-join transformer lowers the WHERE into that
    // sibling match_t and stamps BOTH equi keys side=left (unqualified columns over
    // the merged join schema, validate_logical_plan.cpp same_schema path), so
    // detect_equi_columns can never accept the cross join as-is. This rule classifies
    // the equi keys by PATH RANGE against the intact stamped scan children
    // (left_width = children()[0]->output_types().size()), moves ONE qualifying
    // eq(key,key) onto a fresh inner join, re-localizes + re-sides the right-range
    // key, and keeps every other conjunct as a residual match.
    //
    // Multi-way: `FROM a, b, c` lowers to a left-deep chain
    // join{cross}(join{cross}(a, b), c). This rule recurses into EVERY cross join in
    // the source subtree — each nested join classifies its two keys against ITS OWN
    // children's output_types() (not the outer merged schema) and claims the one WHERE
    // conjunct that straddles ITS boundary. A key's outer-merged path equals its index
    // within the left child (which spans the merged prefix), so classification and the
    // right-range re-localization (path = mergedIdx - left_width) hold at every depth.
    // If a nested join's children are unstamped, that join is left cross (safe no-op)
    // while the others still promote — partial promotion is correct, never wrong.
    //
    // Star reordering (fact-LAST): `FROM dim0, .., fact WHERE fact=dim0 AND ..` lowers
    // to a left-deep CROSS chain whose inner joins hold only DIMENSIONS, so no equi
    // straddles them and the canonical promotion leaves them CROSS — a dimension
    // cartesian. A pure pre-normalizer (normalize_star_shape) detects the single-fact
    // star, rebuilds the CROSS chain fact-FIRST over the ORIGINAL leaf scans, and
    // block-permutes every frozen column ref (the match, group keys, aggregate
    // arguments, and — without a GROUP BY — sort/select loci) so this same canonical
    // path then claims every boundary. It NEVER promotes anything itself (one
    // promotion engine). All detection/verify is read-only and precedes any mutation, so
    // a non-star / already-claimable / unclassifiable shape leaves the tree pristine and
    // flows through unchanged.
    //
    // Must run BEFORE pushdown_filter (whose join branch wraps the join's children in
    // fresh, unstamped aggregates that would collapse left_width to 0) and BEFORE
    // rewrite_hash_joins (which then accepts the promoted inner join).
    //
    // Any malformed / unclassifiable shape is left unchanged (no-op skip) — optimizer
    // rules have no error channel, so they never throw.
    logical_plan::node_ptr promote_cross_joins(std::pmr::memory_resource* resource, logical_plan::node_ptr node);

} // namespace components::planner::optimizer
