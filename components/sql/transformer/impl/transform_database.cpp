#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_database(CreatedbStmt& node) {
        return logical_plan::make_node_create_database(
            resource_,
            core::dbname_t{node.dbname ? std::string(node.dbname) : std::string{}},
            node.if_not_exists);
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_drop_database(DropdbStmt& node) {
        // dbname is captured by the resolve-namespace wrap in transformer::transform
        auto drop = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::database);
        // DropdbStmt has its own missing_ok, separate from DropStmt's.
        drop->set_missing_ok(node.missing_ok);
        // Grammar has no CASCADE/RESTRICT for DROP DATABASE, implicitly CASCADE (gram.y:11047);
        // restrict_ default would make a populated database undroppable.
        drop->set_behavior(components::catalog::drop_behavior_t::cascade_);
        return drop;
    }

} // namespace components::sql::transform
