#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_rename(RenameStmt& node) {
        if (node.renameType != OBJECT_COLUMN) {
            return logical_plan::make_node_alter_table_drop_column(resource_, std::string{});
        }
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string db_for_resolve = qn.dbname;
        const std::string rel_for_resolve = qn.relname;
        std::string old_name = node.subname ? node.subname : "";
        std::string new_name = node.newname ? node.newname : "";
        auto n = logical_plan::make_node_alter_table_rename_column(resource_, std::move(old_name), std::move(new_name));
        // The altered table's identity stays ON the node: enrich binds it to a
        // resolved entry by name and stamps table_oid() + relkind from there.
        n->set_dbname(db_for_resolve);
        n->set_relname(rel_for_resolve);
        register_catalog_resolve_table(resource_, &catalog_resolves_, db_for_resolve, rel_for_resolve);
        return n;
    }

    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_alter_table(AlterTableStmt& node, logical_plan::execution_plan_t* plan) {
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string& db = qn.dbname;
        const std::string& rel = qn.relname;
        // Helper: every return path below targets (db, rel) — name the node and
        // register the lookup once.
        auto wrap_primary = [&](logical_plan::node_ptr n) {
            if (n && n->type() == logical_plan::node_type::alter_table_t) {
                auto* alter = static_cast<logical_plan::node_alter_table_t*>(n.get());
                alter->set_dbname(db);
                alter->set_relname(rel);
            }
            register_catalog_resolve_table(resource_, &catalog_resolves_, db, rel);
            return n;
        };
        if (!node.cmds || node.cmds->lst.empty()) {
            return wrap_primary(logical_plan::make_node_alter_table_drop_column(resource_, std::string{}));
        }
        std::vector<logical_plan::alter_table_subcommand_t> subs;
        subs.reserve(node.cmds->lst.size());
        for (const auto& raw_cell : node.cmds->lst) {
            auto* cmd = pg_ptr_cast<AlterTableCmd>(raw_cell.data);
            switch (cmd->subtype) {
                case AT_AddColumn: {
                    if (!cmd->def || nodeTag(cmd->def) != T_ColumnDef) {
                        continue;
                    }
                    List tmp(resource_);
                    PGListCell cell;
                    cell.data = cmd->def;
                    tmp.lst.push_back(cell);
                    VALUE_OR_RETURN(auto cols, get_column_definitions(resource_, tmp));
                    if (cols.empty()) {
                        continue;
                    }
                    logical_plan::alter_table_subcommand_t sub;
                    sub.kind = logical_plan::alter_table_kind::add_column;
                    sub.column_name = cols.front().name();
                    sub.column = std::move(cols.front());
                    subs.push_back(std::move(sub));
                    break;
                }
                case AT_DropColumn: {
                    logical_plan::alter_table_subcommand_t sub;
                    sub.kind = logical_plan::alter_table_kind::drop_column;
                    sub.column_name = cmd->name ? cmd->name : "";
                    subs.push_back(std::move(sub));
                    break;
                }
                case AT_AddConstraint: {
                    if (!cmd->def || nodeTag(cmd->def) != T_Constraint) {
                        break;
                    }
                    auto* constr = pg_ptr_cast<Constraint>(cmd->def);
                    if (constr->contype == CONSTR_FOREIGN && constr->pktable) {
                        std::string con_name = constr->conname ? constr->conname : "";
                        std::string ref_db;
                        if (constr->pktable->catalogname) {
                            ref_db = constr->pktable->catalogname;
                        } else if (constr->pktable->schemaname) {
                            ref_db = constr->pktable->schemaname;
                        } else {
                            ref_db = db;
                        }
                        std::string ref_rel = constr->pktable->relname ? constr->pktable->relname : "";
                        auto fk_node =
                            logical_plan::make_node_create_constraint(resource_,
                                                                      db,
                                                                      rel,
                                                                      core::constraint_name_t{std::move(con_name)},
                                                                      logical_plan::constraint_kind::foreign_key,
                                                                      ref_db);
                        if (constr->fk_attrs) {
                            std::vector<std::string> fk_cols;
                            fk_cols.reserve(constr->fk_attrs->lst.size());
                            for (auto& col : constr->fk_attrs->lst) {
                                fk_cols.emplace_back(strVal(col.data));
                            }
                            fk_node->set_local_col_names(std::move(fk_cols));
                        }
                        if (constr->pk_attrs) {
                            std::vector<std::string> ref_cols;
                            ref_cols.reserve(constr->pk_attrs->lst.size());
                            for (auto& col : constr->pk_attrs->lst) {
                                ref_cols.emplace_back(strVal(col.data));
                            }
                            fk_node->set_ref_col_names(std::move(ref_cols));
                        }
                        const char mt = constr->fk_matchtype;
                        fk_node->set_match_type((mt == 'f' || mt == 'p' || mt == 's') ? mt : 's');
                        const char da = constr->fk_del_action;
                        fk_node->set_del_action((da == 'a' || da == 'r' || da == 'c' || da == 'n' || da == 'd') ? da
                                                                                                                : 'a');
                        const char ua = constr->fk_upd_action;
                        fk_node->set_upd_action((ua == 'a' || ua == 'r' || ua == 'c' || ua == 'n' || ua == 'd') ? ua
                                                                                                                : 'a');
                        // FK requires BOTH the constrained table and the
                        // referenced table to be resolved at Pass 1 time.
                        const std::string fk_ref_db = fk_node->ref_dbname();
                        std::vector<std::pair<std::string, std::string>> targets;
                        targets.emplace_back(db, rel);
                        if (!ref_rel.empty()) {
                            const std::string& effective_ref_db = fk_ref_db.empty() ? db : fk_ref_db;
                            targets.emplace_back(effective_ref_db, ref_rel);
                        }
                        // Both identities stay ON the node — enrich looks each up by
                        // name, so neither depends on registration order.
                        fk_node->set_ref_relname(ref_rel);
                        register_catalog_resolve_tables(resource_, &catalog_resolves_, targets);
                        return logical_plan::node_ptr{std::move(fk_node)};
                    }
                    if (constr->contype == CONSTR_CHECK && constr->raw_expr) {
                        const name_collection_t names;
                        const std::size_t sub_queries_before = plan->sub_queries.size();
                        VALUE_OR_RETURN(auto expr, transform_predicate(constr->raw_expr, names, plan));
                        // CHECK expr should not contain subqueries, and this is an easy way to enforce that
                        if (plan->sub_queries.size() != sub_queries_before) {
                            return core::error_t(
                                core::error_code_t::invalid_constraint,
                                std::pmr::string{"CHECK constraint contains a sub-query; a CHECK may only read "
                                                 "the row it judges",
                                                 resource_});
                        }
                        VALUE_OR_RETURN(auto expr_text, slice_check_expression(resource_, raw_sql_, constr->location));
                        if (expr_text.empty()) {
                            return core::error_t(
                                core::error_code_t::invalid_constraint,
                                std::pmr::string{"CHECK constraint expression cannot be stored: it contains a "
                                                 "construct that cannot be written back to SQL",
                                                 resource_});
                        }
                        std::string con_name = constr->conname ? constr->conname : "";
                        auto check_node =
                            logical_plan::make_node_create_constraint(resource_,
                                                                      db,
                                                                      rel,
                                                                      core::constraint_name_t{std::move(con_name)},
                                                                      logical_plan::constraint_kind::check);
                        check_node->set_check_expression(std::move(expr));
                        check_node->set_check_expression_sql(std::move(expr_text));
                        return wrap_primary(logical_plan::node_ptr{std::move(check_node)});
                    }
                    if (constr->contype == CONSTR_UNIQUE || constr->contype == CONSTR_PRIMARY) {
                        // UNIQUE / PRIMARY KEY. The enforced columns live in constr->keys
                        // (identical to CREATE TABLE table-level constraints). The kind is
                        // lowered to pg_constraint.contype 'u'/'p' by rewrite_create_constraint;
                        // operator_resolve_constraint reads it back on INSERT/UPDATE and stamps
                        // the DML node's unique_groups (enrich → planner → unique operator).
                        std::string con_name = constr->conname ? constr->conname : "";
                        const auto kind = (constr->contype == CONSTR_PRIMARY)
                                              ? logical_plan::constraint_kind::primary_key
                                              : logical_plan::constraint_kind::unique;
                        auto uq_node =
                            logical_plan::make_node_create_constraint(resource_,
                                                                      db,
                                                                      rel,
                                                                      core::constraint_name_t{std::move(con_name)},
                                                                      kind);
                        if (constr->keys) {
                            std::vector<std::string> cols;
                            cols.reserve(constr->keys->lst.size());
                            for (auto& col : constr->keys->lst) {
                                cols.emplace_back(strVal(col.data));
                            }
                            uq_node->set_local_col_names(std::move(cols));
                        }
                        return wrap_primary(logical_plan::node_ptr{std::move(uq_node)});
                    }
                    break;
                }
                default:
                    break;
            }
        }
        if (subs.empty()) {
            return wrap_primary(logical_plan::make_node_alter_table_drop_column(resource_, std::string{}));
        }
        if (subs.size() == 1) {
            auto& s = subs.front();
            switch (s.kind) {
                case logical_plan::alter_table_kind::add_column:
                    return wrap_primary(logical_plan::make_node_alter_table_add_column(resource_, std::move(s.column)));
                case logical_plan::alter_table_kind::drop_column:
                    return wrap_primary(
                        logical_plan::make_node_alter_table_drop_column(resource_, std::move(s.column_name)));
                case logical_plan::alter_table_kind::rename_column:
                    return wrap_primary(
                        logical_plan::make_node_alter_table_rename_column(resource_,
                                                                          std::move(s.column_name),
                                                                          std::move(s.new_column_name)));
            }
        }
        return wrap_primary(logical_plan::make_node_alter_table_multi(resource_, std::move(subs)));
    }

} // namespace components::sql::transform
