#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_rename(RenameStmt& node) {
        if (node.renameType != OBJECT_COLUMN) {
            return logical_plan::make_node_alter_table_drop_column(resource_, std::string{}, std::string{}, std::string{});
        }
        auto qn = rangevar_to_qualified_name(node.relation);
        std::string old_name = node.subname ? node.subname : "";
        std::string new_name = node.newname ? node.newname : "";
        return logical_plan::make_node_alter_table_rename_column(resource_,
                                                                  std::move(qn.dbname),
                                                                  std::move(qn.relname),
                                                                  std::move(old_name),
                                                                  std::move(new_name));
    }

    logical_plan::node_ptr transformer::transform_alter_table(AlterTableStmt& node) {
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string& db = qn.dbname;
        const std::string& rel = qn.relname;
        if (!node.cmds || node.cmds->lst.empty()) {
            return logical_plan::make_node_alter_table_drop_column(resource_, db, rel, std::string{});
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
                    std::vector<components::table::column_definition_t> cols;
                    fill_column_definitions(cols, resource_, tmp);
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
                        auto fk_node = logical_plan::make_node_create_constraint(
                            resource_, db, rel, std::move(con_name),
                            logical_plan::constraint_kind::foreign_key,
                            std::move(ref_db), std::move(ref_rel));
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
                        fk_node->set_del_action((da == 'a' || da == 'r' || da == 'c' || da == 'n' || da == 'd') ? da : 'a');
                        const char ua = constr->fk_upd_action;
                        fk_node->set_upd_action((ua == 'a' || ua == 'r' || ua == 'c' || ua == 'n' || ua == 'd') ? ua : 'a');
                        return fk_node;
                    }
                    if (constr->contype == CONSTR_CHECK && constr->raw_expr) {
                        std::string expr_text = deparse_check_expr(constr->raw_expr);
                        if (!expr_text.empty()) {
                            std::string con_name = constr->conname ? constr->conname : "";
                            auto check_node = logical_plan::make_node_create_constraint(
                                resource_, db, rel, std::move(con_name),
                                logical_plan::constraint_kind::check);
                            check_node->set_check_expr(std::move(expr_text));
                            return check_node;
                        }
                        throw parser_exception_t{
                            "CHECK constraint expression contains unsupported constructs; "
                            "allowed: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                            "column references, and constants",
                            ""};
                    }
                    break;
                }
                default:
                    break;
            }
        }
        if (subs.empty()) {
            return logical_plan::make_node_alter_table_drop_column(resource_, db, rel, std::string{});
        }
        if (subs.size() == 1) {
            auto& s = subs.front();
            switch (s.kind) {
                case logical_plan::alter_table_kind::add_column:
                    return logical_plan::make_node_alter_table_add_column(resource_, db, rel, std::move(s.column));
                case logical_plan::alter_table_kind::drop_column:
                    return logical_plan::make_node_alter_table_drop_column(resource_, db, rel, std::move(s.column_name));
                case logical_plan::alter_table_kind::rename_column:
                    return logical_plan::make_node_alter_table_rename_column(resource_,
                                                                              db,
                                                                              rel,
                                                                              std::move(s.column_name),
                                                                              std::move(s.new_column_name));
            }
        }
        return logical_plan::make_node_alter_table_multi(resource_, db, rel, std::move(subs));
    }

} // namespace components::sql::transform
