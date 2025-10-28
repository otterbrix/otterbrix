#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_create_database(CreatedbStmt& node) {
        return logical_plan::make_node_create_database(resource_, {node.dbname, collection_name_t()});
    }

    logical_plan::node_ptr transformer::transform_drop_database(DropdbStmt& node) {
        return logical_plan::make_node_drop_database(resource_, {node.dbname, collection_name_t()});
    }


} // namespace components::sql::transform
