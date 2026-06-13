#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_create_database(CreatedbStmt& node) {
        return logical_plan::make_node_create_database(
            resource_,
            core::dbname_t{node.dbname ? std::string(node.dbname) : std::string{}},
            node.if_not_exists);
    }

    logical_plan::node_ptr transformer::transform_drop_database(DropdbStmt&) {
        // dbname is captured by the resolve-namespace wrap in transformer::transform
        return logical_plan::make_node_drop_database(resource_);
    }

} // namespace components::sql::transform
