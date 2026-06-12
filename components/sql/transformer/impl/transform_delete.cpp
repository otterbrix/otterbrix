#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_delete(DeleteStmt& node, logical_plan::execution_plan_t* plan) {
        if (!node.whereClause) {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto del = logical_plan::make_node_delete_many(
                resource_,
                logical_plan::make_node_match(resource_,
                                              core::dbname_t{qn.dbname},
                                              core::relname_t{qn.relname},
                                              make_compare_expression(resource_, compare_type::all_true)));
            if (node.returningList) {
                name_collection_t rnames;
                rnames.left_name = qn;
                rnames.left_alias = construct_alias(node.relation->alias);
                del->returning() = transform_returning(node.returningList, rnames, plan);
                if (error_.contains_error()) {
                    return nullptr;
                }
            }
            // Tag the target table for catalog resolution and emit
            // resolve_constraint(referencing) so enrich reads descendant FKs
            // are stamped on the plan tree by Pass 1.
            return maybe_wrap_with_catalog_resolve_table(resource_,
                                                         qn.dbname,
                                                         qn.relname,
                                                         std::move(del),
                                                         constraint_resolve_kind::referencing);
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
            where_expr = transform_null_test(pg_ptr_cast<NullTest>(node.whereClause), names, plan->parameters.get());
        } else if (nodeTag(node.whereClause) == T_SubLink) {
            where_expr = transform_sublink_expr(pg_ptr_cast<SubLink>(node.whereClause), names, plan);
        } else {
            where_expr = transform_a_expr(pg_ptr_cast<A_Expr>(node.whereClause), names, plan);
        }
        auto del =
            logical_plan::make_node_delete_many(resource_,
                                                logical_plan::make_node_match(resource_,
                                                                              core::dbname_t{names.left_name.dbname},
                                                                              core::relname_t{names.left_name.relname},
                                                                              where_expr));
        if (node.returningList) {
            del->returning() = transform_returning(node.returningList, names, plan);
            if (error_.contains_error()) {
                return nullptr;
            }
        }
        // Wrap with namespace + table resolve nodes for the primary (LEFT)
        // table and emit resolve_constraint(referencing) for FK cascade enrich.
        auto wrapped = maybe_wrap_with_catalog_resolve_table(resource_,
                                                             names.left_name.dbname,
                                                             names.left_name.relname,
                                                             std::move(del),
                                                             constraint_resolve_kind::referencing);
        // When DELETE ... USING is present, splice a resolve_table for the
        // USING source into the wrapping sequence_t so
        // stamp_drop_oids_from_resolves picks it up as `rt_index` and stamps
        // node->table_oid_from() at enrich time.
        if (!names.right_name.empty() && wrapped->type() == logical_plan::node_type::sequence_t) {
            auto from_resolve =
                logical_plan::make_node_catalog_resolve_table(resource_,
                                                              core::dbname_t{names.right_name.dbname},
                                                              core::relname_t{names.right_name.relname});
            auto& kids = wrapped->children();
            kids.insert(kids.end() - 1, from_resolve);
        }
        return wrapped;
    }
} // namespace components::sql::transform
