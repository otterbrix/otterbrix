#include "optimizer.hpp"

#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/hash_join.hpp"
#include "optimizer/rules/pushdown_filter.hpp"
#include "optimizer/rules/spill_strategy.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    bool spill_enabled) {
        if (!node) {
            return nullptr;
        }

        // Single post-planner pass. Order matters: fold constants on parameter
        // expressions, push filters down, then select hash joins. Hash-join
        // selection reads key.side()/key.path() stamped by validate_schema,
        // which has already run. spill_strategy runs last and only stamps the
        // exec_strategy annotation; it does not restructure the tree and is
        // orthogonal to the algo choice, so order vs hash_joins is irrelevant.
        // All rules are safe here — the planner wraps DML on top and lowers DDL
        // to sequences, leaving the match_t/join_t/aggregate_t these rules
        // target intact.
        if (parameters) {
            optimizer::fold_constants(resource, node, parameters);
        }
        node = optimizer::pushdown_filter(resource, node);
        node = optimizer::rewrite_hash_joins(resource, std::move(node));
        node = optimizer::spill_strategy(std::move(node), spill_enabled);

        return node;
    }

} // namespace components::planner
