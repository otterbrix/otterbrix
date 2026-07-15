#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Annotate the cardinality-preserving source under an effective LIMIT/OFFSET
    // with a pure COUNT read-cap (limit+offset, offset 0), so the physical-plan
    // generator caps that source's read while the authoritative operator_limit
    // (inserted by create_plan_aggregate) still applies the real
    // [offset, offset+limit) window. Post-order walk; at every node_aggregate_t
    // carrying an effective limit_t child, classify its children (mirroring
    // create_plan_aggregate) and stamp read_cap on exactly ONE node:
    //   - the node_sort_t child            when there is an ORDER BY and NO DISTINCT
    //                                        (the sort's OUTPUT is the final set);
    //   - NOTHING                           when DISTINCT is set (dedup is layered
    //                                        above the terminal; capping any source
    //                                        would drop rows), or a GROUP BY / UNION
    //                                        / recursive-CTE / join source sits
    //                                        between (not cardinality-preserving and
    //                                        no output-cap hook);
    //   - the node_match_t child            for a WHERE scan with no sort/group/
    //                                        distinct/non-scan source (disk caps the
    //                                        POST-filter output);
    //   - the node_aggregate_t itself       for a plain scan with NO WHERE and NOT
    //                                        is_distinct() (the terminal
    //                                        transfer_scan reads the aggregate's cap).
    //
    // ANNOTATION only — logical semantics are unchanged; the read-cap is advisory
    // (children may over-read; correctness never depends on them honoring it).
    // Signature mirrors pushdown_aggregate. Returns the root; nodes annotated in
    // place. Registered UNGATED (a local scan/sort hint, valid in in-memory mode).
    logical_plan::node_ptr pushdown_limit(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer
