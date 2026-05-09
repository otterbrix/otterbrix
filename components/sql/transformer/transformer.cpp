#include "transformer.hpp"
#include "utils.hpp"

#include <components/sql/parser/parser.h>

namespace components::sql::transform {

    transform_result transformer::transform(Node& node) {
        auto params = logical_plan::make_parameter_node(resource_);
        logical_plan::node_ptr log_node;

        // TODO: Error handling
        switch (node.type) {
            case T_CreatedbStmt:
                log_node = transform_create_database(pg_cast<CreatedbStmt>(node));
                break;
            case T_DropdbStmt:
                log_node = transform_drop_database(pg_cast<DropdbStmt>(node));
                break;
            case T_CreateStmt:
                log_node = transform_create_table(pg_cast<CreateStmt>(node));
                break;
            case T_DropStmt:
                log_node = transform_drop(pg_cast<DropStmt>(node));
                break;
            case T_CompositeTypeStmt:
                log_node = transform_create_type(pg_cast<CompositeTypeStmt>(node));
                break;
            case T_CreateEnumStmt:
                log_node = transform_create_enum_type(pg_cast<CreateEnumStmt>(node));
                break;
            case T_SelectStmt:
                log_node = transform_select(pg_cast<SelectStmt>(node), params.get());
                break;
            case T_UpdateStmt:
                log_node = transform_update(pg_cast<UpdateStmt>(node), params.get());
                break;
            case T_InsertStmt:
                log_node = transform_insert(pg_cast<InsertStmt>(node), params.get());
                break;
            case T_DeleteStmt:
                log_node = transform_delete(pg_cast<DeleteStmt>(node), params.get());
                break;
            case T_IndexStmt:
                log_node = transform_create_index(pg_cast<IndexStmt>(node));
                break;
            case T_CheckPointStmt:
                log_node = transform_checkpoint(pg_cast<CheckPointStmt>(node));
                break;
            case T_VacuumStmt:
                log_node = transform_vacuum(pg_cast<VacuumStmt>(node));
                break;
            case T_CreateSeqStmt:
                log_node = transform_create_sequence(pg_cast<CreateSeqStmt>(node));
                break;
            case T_ViewStmt:
                log_node = transform_create_view(pg_cast<ViewStmt>(node));
                break;
            case T_CreateFunctionStmt:
                log_node = transform_create_function(pg_cast<CreateFunctionStmt>(node));
                break;
            case T_AlterTableStmt:
                log_node = transform_alter_table(pg_cast<AlterTableStmt>(node));
                break;
            case T_RenameStmt:
                log_node = transform_rename(pg_cast<RenameStmt>(node));
                break;
            case T_TransactionStmt:
                log_node = transform_transaction(pg_cast<TransactionStmt>(node));
                break;
            default:
                throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node.type));
        }

        return {std::move(log_node),
                std::move(params),
                std::move(parameter_map_),
                std::move(parameter_insert_map_),
                std::move(parameter_insert_rows_)};
    }

    transformer::check_expr_result transformer::parse_where_expr(const std::string& expr_text) {
        std::string wrapped = "SELECT 1 WHERE " + expr_text;
        std::pmr::monotonic_buffer_resource arena(resource_);
        auto* raw_list = raw_parser(&arena, wrapped.c_str());
        if (!raw_list || raw_list->lst.empty()) {
            return {};
        }
        auto* raw = linitial(raw_list);
        if (!raw || nodeTag(raw) != T_SelectStmt) {
            return {};
        }
        auto* sel = pg_ptr_cast<SelectStmt>(raw);
        if (!sel->whereClause) {
            return {};
        }
        auto params = logical_plan::make_parameter_node(resource_);
        name_collection_t empty_names;
        try {
            expressions::expression_ptr expr;
            if (nodeTag(sel->whereClause) == T_NullTest) {
                expr = transform_null_test(pg_ptr_cast<NullTest>(sel->whereClause),
                                           empty_names, params.get());
            } else if (nodeTag(sel->whereClause) == T_FuncCall) {
                expr = transform_a_expr_func(pg_ptr_cast<FuncCall>(sel->whereClause),
                                              empty_names, params.get());
            } else {
                expr = transform_a_expr(pg_ptr_cast<A_Expr>(sel->whereClause),
                                        empty_names, params.get());
            }
            if (!expr) return {};
            return {std::move(expr), std::move(params)};
        } catch (...) {
            return {};
        }
    }

} // namespace components::sql::transform
