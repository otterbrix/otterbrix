#include <components/logical_plan/node_delete.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node, logical_plan::parameter_node_t* params) {
        if (!node.whereClause) {
            return logical_plan::make_node_delete_many(
                resource_,
                rangevar_to_collection(node.relation),
                logical_plan::make_node_match(resource_,
                                              rangevar_to_collection(node.relation),
                                              make_compare_expression(resource_, compare_type::all_true)));
        }
        collection_full_name_t collection = rangevar_to_collection(node.relation);
        collection_full_name_t collection_from = {};
        if (!node.usingClause->lst.empty()) {
            collection_from = rangevar_to_collection(pg_ptr_cast<RangeVar>(node.usingClause->lst.front().data));
        }
        return logical_plan::make_node_delete_many(
            resource_,
            collection,
            collection_from,
            logical_plan::make_node_match(resource_,
                                          collection,
                                          transform_a_expr(params, pg_ptr_cast<A_Expr>(node.whereClause))));
    }
} // namespace components::sql::transform
