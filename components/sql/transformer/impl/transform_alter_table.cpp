#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_rename(RenameStmt& node) {
        if (node.renameType != OBJECT_COLUMN) {
            // Other rename targets (TABLE, etc.) not yet supported — return a no-op so
            // the parser doesn't reject SQL that hits unrelated rename forms.
            collection_full_name_t empty_coll;
            return logical_plan::make_node_alter_table_drop_column(resource_, empty_coll, std::string{});
        }
        auto coll = rangevar_to_collection(node.relation);
        std::string old_name = node.subname ? node.subname : "";
        std::string new_name = node.newname ? node.newname : "";
        return logical_plan::make_node_alter_table_rename_column(resource_, coll,
                                                                   std::move(old_name),
                                                                   std::move(new_name));
    }

    logical_plan::node_ptr transformer::transform_alter_table(AlterTableStmt& node) {
        auto coll = rangevar_to_collection(node.relation);
        if (!node.cmds || node.cmds->lst.empty()) {
            // Empty ALTER — emit no-op alter (drop_column with empty name) so caller doesn't crash.
            return logical_plan::make_node_alter_table_drop_column(resource_, coll, std::string{});
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
                    if (constr->contype == CONSTR_CHECK && constr->raw_expr) {
                        std::string expr_text = deparse_check_expr(constr->raw_expr);
                        if (!expr_text.empty()) {
                            std::string con_name = constr->conname ? constr->conname : "";
                            auto node = logical_plan::make_node_create_constraint(
                                resource_, coll, std::move(con_name),
                                logical_plan::constraint_kind::check);
                            node->set_check_expr(std::move(expr_text));
                            return node;
                        }
                        // CHECK expression contains unsupported constructs (e.g. function calls,
                        // subqueries, CASE). Reject early rather than silently creating a no-op.
                        throw parser_exception_t{
                            "CHECK constraint expression contains unsupported constructs; "
                            "allowed: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                            "column references, and constants",
                            ""};
                    }
                    break;
                }
                default:
                    // Unsupported ALTER subtypes (AT_AlterColumnType, AT_SetNotNull, etc.) skipped
                    // silently — preserves prior no-op behavior for unsupported forms.
                    break;
            }
        }
        if (subs.empty()) {
            return logical_plan::make_node_alter_table_drop_column(resource_, coll, std::string{});
        }
        if (subs.size() == 1) {
            // Preserve single-cmd factory shape (some downstream paths assume kind() reflects intent).
            auto& s = subs.front();
            switch (s.kind) {
                case logical_plan::alter_table_kind::add_column:
                    return logical_plan::make_node_alter_table_add_column(resource_, coll, std::move(s.column));
                case logical_plan::alter_table_kind::drop_column:
                    return logical_plan::make_node_alter_table_drop_column(resource_, coll, std::move(s.column_name));
                case logical_plan::alter_table_kind::rename_column:
                    return logical_plan::make_node_alter_table_rename_column(resource_, coll,
                                                                              std::move(s.column_name),
                                                                              std::move(s.new_column_name));
            }
        }
        return logical_plan::make_node_alter_table_multi(resource_, coll, std::move(subs));
    }

} // namespace components::sql::transform
