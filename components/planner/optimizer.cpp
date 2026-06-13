#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/hash_join.hpp"
#include "optimizer/rules/pushdown_filter.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters) {
        if (!node) {
            return nullptr;
        }

        // Constant folding: resolve arithmetic on parameters at plan time.
        if (parameters) {
            optimizer::fold_constants(resource, node, parameters);
        }

        node = optimizer::pushdown_filter(resource, node);

        return node;
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        if (!node) {
            return nullptr;
        }

        // Column pruning is intentionally disabled in the current work; the rule
        // (optimizer::prune_columns) is left in place but not run here.

        // Hash-join selection: rewrite eligible nested-loop joins (single
        // eq(left.key, right.key) condition) into node_hash_join_t so the planner
        // can lower them to operator_hash_join_t. Relies on key.side()/key.path()
        // stamped by validate_schema, which has already run at this point.
        node = optimizer::rewrite_hash_joins(resource, std::move(node));

        return node;
    }

} // namespace components::planner