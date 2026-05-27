#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_rename(RenameStmt& node) {
        if (node.renameType != OBJECT_COLUMN) {
            return logical_plan::make_node_alter_table_drop_column(resource_, std::string{});
        }
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string db_for_resolve = qn.dbname;
        const std::string rel_for_resolve = qn.relname;
        std::string old_name = node.subname ? node.subname : "";
        std::string new_name = node.newname ? node.newname : "";
        auto n = logical_plan::make_node_alter_table_rename_column(resource_, std::move(old_name), std::move(new_name));
        return maybe_wrap_with_catalog_resolve_table(resource_, db_for_resolve, rel_for_resolve, std::move(n));
    }

    logical_plan::node_ptr transformer::transform_alter_table(AlterTableStmt& node) {
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string& db = qn.dbname;
        const std::string& rel = qn.relname;
        // Helper: every return path below targets (db, rel) — wrap once.
        auto wrap_primary = [&](logical_plan::node_ptr n) {
            return maybe_wrap_with_catalog_resolve_table(resource_, db, rel, std::move(n));
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
                    if (auto cols_res = get_column_definitions(resource_, tmp); cols_res.has_error()) {
                        error_ = cols_res.error();
                        return nullptr;
                    } else {
                        if (cols_res.value().empty()) {
                            continue;
                        }
                        logical_plan::alter_table_subcommand_t sub;
                        sub.kind = logical_plan::alter_table_kind::add_column;
                        sub.column_name = cols_res.value().front().name();
                        sub.column = std::move(cols_res.value().front());
                        subs.push_back(std::move(sub));
                    }
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
                        return maybe_wrap_with_catalog_resolve_tables(resource_,
                                                                      std::move(targets),
                                                                      logical_plan::node_ptr{std::move(fk_node)});
                    }
                    if (constr->contype == CONSTR_CHECK && constr->raw_expr) {
                        if (auto expr_text = deparse_check_expr(resource_, constr->raw_expr); expr_text.has_error()) {
                            error_ = expr_text.error();
                            return nullptr;
                        } else if (!expr_text.value().empty()) {
                            std::string con_name = constr->conname ? constr->conname : "";
                            auto check_node =
                                logical_plan::make_node_create_constraint(resource_,
                                                                          db,
                                                                          rel,
                                                                          core::constraint_name_t{std::move(con_name)},
                                                                          logical_plan::constraint_kind::check);
                            check_node->set_check_expr(std::move(expr_text.value()));
                            return wrap_primary(logical_plan::node_ptr{std::move(check_node)});
                        }
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"CHECK constraint expression contains unsupported constructs; "
                                             "allowed: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                                             "column references, and constants",
                                             resource_});
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
