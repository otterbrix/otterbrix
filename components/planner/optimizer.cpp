#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/hash_join.hpp"
#include "optimizer/rules/join_predicate_pushdown.hpp"
#include "optimizer/rules/pushdown_filter.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters) {
        if (!node) {
            return nullptr;
        }

        // Single post-planner pass. Order matters: fold constants on parameter
        // expressions, push filters down, duplicate cross-join WHERE predicates,
        // prune aggregate input columns, then select hash joins. Hash-join
        // selection reads key.side()/key.path() stamped by validate_schema,
        // which has already run. All rules are safe here — the planner wraps
        // DML on top and lowers DDL to sequences, leaving the
        // match_t/join_t/aggregate_t these rules target intact.
        if (parameters) {
            optimizer::fold_constants(resource, node, parameters);
        }
        node = optimizer::pushdown_filter(resource, node);

        // Push WHERE predicates that span both join sides onto the synthesized
        // cross join (adds a join filter; WHERE is untouched).
        optimizer::push_down_join_predicates(node);

        // Column pruning: annotate aggregate nodes with the column indices each
        // one needs to read from its source. Must run while joins are still
        // node_join_t (it splits join columns per side), i.e. BEFORE the
        // hash-join rewrite below converts them into node_hash_join_t.
        optimizer::prune_columns(node);

        node = optimizer::rewrite_hash_joins(resource, std::move(node));

        return node;
    }

} // namespace components::planner
