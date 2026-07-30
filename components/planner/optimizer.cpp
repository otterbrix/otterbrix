#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/drop_redundant_distinct.hpp"
#include "optimizer/rules/eager_aggregation.hpp"
#include "optimizer/rules/hash_join.hpp"
#include "optimizer/rules/promote_cross_join.hpp"
#include "optimizer/rules/pushdown_aggregate.hpp"
#include "optimizer/rules/pushdown_filter.hpp"
#include "optimizer/rules/pushdown_limit.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    bool can_push_to_agent,
                                    optimizer_pass_t host_pass) {
        if (!node) {
            return nullptr;
        }

        // Single post-planner pass. Order matters: fold constants on parameter
        // expressions, push filters down, then select hash joins. Hash-join
        // selection reads key.side()/key.path() stamped by validate_schema,
        // which has already run. All rules are safe here — the planner wraps
        // DML on top and lowers DDL to sequences, leaving the
        // match_t/join_t/aggregate_t these rules target intact.
        if (parameters) {
            optimizer::fold_constants(resource, node, parameters);
        }
        // Clear a DISTINCT that a GROUP BY already makes redundant (group keys ⊆
        // projection / DISTINCT ON columns) so no operator_distinct "Unique" pass is
        // built. Runs on the freshly validated tree, BEFORE the pushdown/pruning rules:
        // it only reads the group-output ordinals validate_schema stamped and flips the
        // is_distinct flag, so it neither depends on nor disturbs the later rules — and
        // clearing it early lets column_pruning / pushdown_limit see the simpler plan.
        node = optimizer::drop_redundant_distinct(resource, std::move(node));
        // Promote comma-join CROSS joins to INNER before pushdown_filter: its join
        // branch wraps the join's children in fresh, unstamped aggregates whose
        // output_schema() is empty (validation already ran), which would collapse the
        // promote rule's left_width to 0. fold_constants (above) never restructures
        // joins, so the children stay stamped for the range classification.
        node = optimizer::promote_cross_joins(resource, std::move(node));
        // Push the outer WHERE INTO an inlined single-table CTE / FROM-subquery body so the
        // filter reaches the base scan (disk pushdown + column pruning) instead of a Filter
        // above the body's Project. Runs BEFORE pushdown_filter and on a DISJOINT source
        // shape (a table-scan aggregate) — deliberately not folded into pushdown_filter,
        // whose join branch synthesizes `aggregate{scan, match}` wrappers that this rule must
        // not fuse (that would change the join lowering / EXPLAIN shape). Conservative: never
        // pushes below a LIMIT/GROUP/HAVING/DISTINCT, and only through a leading-prefix
        // identity projection; recursive-CTE and multi-referenced bodies are untouched
        // (each non-recursive reference is inlined as its own copy, so per-copy pushes never
        // cross-contaminate).
        node = optimizer::pushdown_cte_filter(resource, std::move(node));
        node = optimizer::pushdown_filter(resource, std::move(node));
        node = optimizer::rewrite_hash_joins(resource, std::move(node));

        // Eager (partial) aggregation pushdown through an INNER equi-join: push a
        // MIN/MAX partial reduce onto the single join side that owns every group key
        // and aggregate argument, leaving a FINAL merge above the join. Runs AFTER
        // rewrite_hash_joins (needs the settled single equi-key to re-stamp) and
        // BEFORE pushdown_aggregate + column_pruning, so the synthesized partial gets
        // the ordinary single-table lowering (and, when an agent is reachable, the
        // agent-side reduce) and the join frame-split stays consistent. Only fires on
        // the provably-sound MIN/MAX shape (see eager_aggregation.hpp) — a false
        // negative (missed push) is the only failure mode, never a wrong result.
        node = optimizer::eager_aggregation(resource, std::move(node));

        // Stamp a pure COUNT read-cap on the cardinality-preserving source under an
        // effective LIMIT/OFFSET (create_plan_aggregate reads it; the authoritative
        // operator_limit still windows on top). UNGATED — unlike pushdown_aggregate
        // this is a local scan/sort hint with no owning-agent precondition, valid in
        // in-memory mode too. AFTER pushdown_filter/rewrite_hash_joins so it sees the
        // settled match/join shape.
        node = optimizer::pushdown_limit(resource, std::move(node));

        // Annotate pushable single-owned-table aggregates. Runs LAST — it only
        // reads node types + table_oid() + the group child, so ordering vs. the
        // other rules is immaterial. The rule decides purely by shape (single
        // owned table, mergeable kinds, no HAVING/DISTINCT — exactly like
        // hash-join selection). The sole gate here is a hard CAPABILITY
        // precondition, NOT a fallback/rollout flag: `can_push_to_agent` is false
        // in disk-less (in-memory) mode, where there is NO owning agent to push
        // to, so pushable aggregates must stay coordinator-side.
        if (can_push_to_agent) {
            node = optimizer::pushdown_aggregate(resource, std::move(node));
        }

        // Column pruning runs LAST — after pushdown_filter has relocalized any
        // single-table filters below the join and rewrite_hash_joins has settled the
        // join shape (equi-key stamp + localized ON keys). Running it earlier would
        // race those restructurings, which shift merged schemas and the per-side
        // column indices this rule splits on. At this
        // point key.side()/key.path() are final, so process_join's per-side split and
        // ON-key remap read the same localized indices rewrite_hash_joins detected.
        // UNGATED: projected_cols is a scan projection HINT (empty = read all), valid
        // in in-memory mode too — it needs no owning agent, only the resolved paths.
        optimizer::prune_columns(node);

        // Host-injected final pass on the fully-optimized tree (Null Object = no-op).
        node = host_pass(resource, std::move(node));

        return node;
    }

} // namespace components::planner
