#include "optimizer.hpp"

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

        return node;
    }

} // namespace components::planner
