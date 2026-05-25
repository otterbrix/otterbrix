#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
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

        // Predicate pushdown: move match_t under aggregate/join/group/select
        // when the rewrite is provably safe. May return a different root.
        node = optimizer::pushdown_filter(resource, node);

        return node;
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* /*resource*/,
                                                  logical_plan::node_ptr node) {
        if (!node) {
            return nullptr;
        }

        // Column pruning: annotate aggregate nodes with the set of columns each
        // one actually needs to read from its source. Reads schema info from
        // sibling catalog_resolve_table_t nodes (no external catalog needed).
        optimizer::prune_columns(node);

        return node;
    }

} // namespace components::planner