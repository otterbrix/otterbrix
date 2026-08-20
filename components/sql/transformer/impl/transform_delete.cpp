#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_delete(DeleteStmt& node,
                                                                                 logical_plan::execution_plan_t* plan) {
        // A leading WITH must be registered before the body so `DELETE ... WHERE id IN (SELECT ... FROM cte)`
        // resolves the CTE instead of falling through to a (wrong / nonexistent) base table.
        if (auto err = register_with_ctes(node.withClause); err.contains_error()) {
            return err;
        }
        // Only the bare `DELETE FROM t` (no WHERE, no USING) short-circuits to a
        // delete-all. A USING clause with no WHERE is a cross-join filter (delete
        // every target row that joins a source row), so it must go through the join
        // path below — otherwise an empty source would wrongly delete all rows.
        if (!node.whereClause && (!node.usingClause || node.usingClause->lst.empty())) {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto del_limit_res =
                build_dml_limit(node.limitCount, core::dbname_t{qn.dbname}, core::relname_t{qn.relname}, plan);
            if (del_limit_res.has_error()) {
                return del_limit_res.error();
            }
            auto del_limit = std::move(del_limit_res.value());
            auto del = logical_plan::make_node_delete(
                resource_,
                logical_plan::make_node_match(resource_,
                                              core::dbname_t{qn.dbname},
                                              core::relname_t{qn.relname},
                                              make_compare_expression(resource_, compare_type::all_true)),
                del_limit);
            // The target identity stays ON the node: enrich binds it to a resolved
            // entry by name and stamps table_oid() + table_metadata() from there.
            del->set_dbname(qn.dbname);
            del->set_relname(qn.relname);
            if (node.returningList) {
                name_collection_t rnames;
                rnames.left_name = qn;
                rnames.left_alias = construct_alias(node.relation->alias);
                auto returning_res = transform_returning(node.returningList, rnames, plan);
                if (returning_res.has_error()) {
                    return returning_res.error();
                }
                del->returning() = std::move(returning_res.value());
            }
            // Tag the target table for catalog resolution with the referencing
            // constraint gather, so enrich reads the descendant FKs.
            register_catalog_resolve_table(resource_,
                                           &catalog_resolves_,
                                           qn.dbname,
                                           qn.relname,
                                           constraint_resolve_kind::referencing);
            return del;
        }
        name_collection_t names;
        names.left_name = rangevar_to_qualified_name(node.relation);
        names.left_alias = construct_alias(node.relation->alias);
        // DELETE ... USING: build the USING clause as a source sub-plan (a table, a
        // join tree, a table function, or a — possibly LATERAL — derived table) that
        // becomes the RIGHT side of the delete join. target = left, source = right for
        // the predicate; validate resolves further source columns against the source
        // schema. The source is a plain child node whose scans self-resolve by name.
        logical_plan::node_ptr source_child = nullptr;
        if (node.usingClause && !node.usingClause->lst.empty()) {
            name_collection_t source_names;
            auto source_res = transform_from_source(node.usingClause, source_names, plan);
            if (source_res.has_error()) {
                return source_res.error();
            }
            source_child = std::move(source_res.value());
            names.right_name = source_names.left_name;
            names.right_alias = source_names.left_alias;
        }
        // No WHERE with a USING clause: the cross join of target and source is the
        // filter, so match every target row (all_true) and let the semi-join keep only
        // targets that join a source row. Mirrors transform_update's FROM path.
        expression_ptr where_expr;
        if (node.whereClause) {
            auto where_res = transform_predicate(node.whereClause, names, plan);
            if (where_res.has_error()) {
                return where_res.error();
            }
            where_expr = std::move(where_res.value());
        } else {
            where_expr = make_compare_expression(resource_, compare_type::all_true);
        }
        auto del_limit_res = build_dml_limit(node.limitCount,
                                             core::dbname_t{names.left_name.dbname},
                                             core::relname_t{names.left_name.relname},
                                             plan);
        if (del_limit_res.has_error()) {
            return del_limit_res.error();
        }
        auto del_limit = std::move(del_limit_res.value());
        auto del =
            logical_plan::make_node_delete(resource_,
                                           logical_plan::make_node_match(resource_,
                                                                         core::dbname_t{names.left_name.dbname},
                                                                         core::relname_t{names.left_name.relname},
                                                                         where_expr),
                                           del_limit);
        // The target identity stays ON the node: enrich binds it to a resolved
        // entry by name and stamps table_oid() + table_metadata() from there.
        del->set_dbname(names.left_name.dbname);
        del->set_relname(names.left_name.relname);
        // The USING source is a child sub-plan (the RIGHT side of the delete join);
        // its scans self-resolve by name, so no table_oid_from splice is needed.
        if (source_child) {
            del->append_child(source_child);
        }
        if (node.returningList) {
            auto returning_res = transform_returning(node.returningList, names, plan);
            if (returning_res.has_error()) {
                return returning_res.error();
            }
            del->returning() = std::move(returning_res.value());
        }
        // Resolve the primary (LEFT) table and gather its referencing
        // constraints for FK cascade enrich.
        register_catalog_resolve_table(resource_,
                                       &catalog_resolves_,
                                       names.left_name.dbname,
                                       names.left_name.relname,
                                       constraint_resolve_kind::referencing);
        return del;
    }
} // namespace components::sql::transform
