#include "optimizer.hpp"

#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/hash_join.hpp"
#include "optimizer/rules/pushdown_aggregate.hpp"
#include "optimizer/rules/pushdown_filter.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    bool can_push_to_agent) {
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
        node = optimizer::pushdown_filter(resource, node);
        node = optimizer::rewrite_hash_joins(resource, std::move(node));

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

        return node;
    }

} // namespace components::planner
