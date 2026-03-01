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
        name_collection_t names;
        names.left_name = rangevar_to_collection(node.relation);
        names.left_alias = construct_alias(node.relation->alias);
        if (!node.usingClause->lst.empty()) {
            auto clause = pg_ptr_cast<RangeVar>(node.usingClause->lst.front().data);
            names.right_name = rangevar_to_collection(clause);
            names.right_alias = construct_alias(clause->alias);
        }
        expression_ptr where_expr;
        if (nodeTag(node.whereClause) == T_NullTest) {
            where_expr = transform_null_test(pg_ptr_cast<NullTest>(node.whereClause), names, params);
        } else {
            where_expr = transform_a_expr(pg_ptr_cast<A_Expr>(node.whereClause), names, params);
        }
        return logical_plan::make_node_delete_many(
            resource_,
            names.left_name,
            names.right_name,
            logical_plan::make_node_match(resource_, names.left_name, where_expr));
    }
} // namespace components::sql::transform
