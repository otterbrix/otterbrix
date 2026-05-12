#include <components/logical_plan/node_delete.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node, logical_plan::parameter_node_t* params) {
        if (!node.whereClause) {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto del = logical_plan::make_node_delete_many(
                resource_,
                qn.dbname,
                qn.relname,
                logical_plan::make_node_match(resource_,
                                              qn.dbname,
                                              qn.relname,
                                              make_compare_expression(resource_, compare_type::all_true)));
            // Phase 13 T13: tag the target table for catalog resolution.
            return maybe_wrap_with_catalog_resolve_table(
                resource_, qn.dbname, qn.relname, std::move(del));
        }
        name_collection_t names;
        names.left_name = rangevar_to_qualified_name(node.relation);
        names.left_alias = construct_alias(node.relation->alias);
        if (!node.usingClause->lst.empty()) {
            auto clause = pg_ptr_cast<RangeVar>(node.usingClause->lst.front().data);
            names.right_name = rangevar_to_qualified_name(clause);
            names.right_alias = construct_alias(clause->alias);
        }
        expression_ptr where_expr;
        if (nodeTag(node.whereClause) == T_NullTest) {
            where_expr = transform_null_test(pg_ptr_cast<NullTest>(node.whereClause), names, params);
        } else {
            where_expr = transform_a_expr(pg_ptr_cast<A_Expr>(node.whereClause), names, params);
        }
        auto del = logical_plan::make_node_delete_many(
            resource_,
            names.left_name.dbname,
            names.left_name.relname,
            names.right_name.dbname,
            names.right_name.relname,
            logical_plan::make_node_match(resource_,
                                          names.left_name.dbname,
                                          names.left_name.relname,
                                          where_expr));
        // Phase 13 T13: wrap with namespace + table resolve nodes for the primary
        // (LEFT) table; USING-side table is captured inside the node itself.
        return maybe_wrap_with_catalog_resolve_table(
            resource_, names.left_name.dbname, names.left_name.relname, std::move(del));
    }
} // namespace components::sql::transform
