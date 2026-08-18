#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // The EFFECTIVE base relation behind a plan sub-tree: the node's own resolved
    // table stamp, or — for the filter wrapper pushdown_filter synthesizes around
    // a join input (an oid-less aggregate_t holding [source, match...]) — the
    // wrapped source's stamp, found by descending through match-only wrappers.
    // INVALID_OID for anything else (a join sub-tree, a set operation, raw data):
    // those have no single backing relation whose live row count could bound their
    // size. Both the count-fetch side (collect_inner_hash_join_oids in
    // services/collection/executor.cpp) and the consumer (create_plan_join.cpp's
    // hash-join build-side selection) resolve join inputs through THIS one descent,
    // so every side with a backing relation gets a live count.
    catalog::oid_t effective_table_oid(const node_ptr& node);

} // namespace components::logical_plan