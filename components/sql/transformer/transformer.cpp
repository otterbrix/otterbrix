#include "transformer.hpp"
#include "utils.hpp"

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
            default:
                throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node.type));
        }

        return {std::move(log_node),
                std::move(params),
                std::move(parameter_map_),
                std::move(parameter_insert_map_),
                std::move(parameter_insert_docs_)};
    }
} // namespace components::sql::transform
