#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_database(CreatedbStmt& node) {
        const core::dbname_t database{construct(node.dbname)};
        auto created =
            logical_plan::make_node_create_database(resource_,
                                                    qualified_name_t{database, core::schema_t{}, core::relname_t{}},
                                                    node.if_not_exists);
        register_database(database.t);
        return created;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_drop_database(DropdbStmt& node) {
        auto drop = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::database);
        // DropdbStmt has its own missing_ok, separate from DropStmt's.
        drop->set_if_exists(node.missing_ok);
        // Grammar has no CASCADE/RESTRICT for DROP DATABASE, implicitly CASCADE (gram.y:11047);
        // restrict_ default would make a populated database undroppable.
        drop->set_behavior(components::catalog::drop_behavior_t::cascade_);
        drop->set_target(qualified_name_t{core::dbname_t{construct(node.dbname)}, core::schema_t{}, core::relname_t{}});
        register_database(drop->target().database.t);
        return drop;
    }

    // PostgreSQL 18 creates the schema in the current database; its elements and its owner are not implemented.
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_schema(CreateSchemaStmt& node) {
        const std::string schema = construct(node.schemaname);
        if (node.schemaElts != nullptr && !node.schemaElts->lst.empty()) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE SCHEMA ... CREATE: schema elements are not implemented; "
                                                  "create the schema, then its objects; nothing was created",
                                                  resource_});
        }
        if (node.authid != nullptr) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE SCHEMA ... AUTHORIZATION is not implemented: there are no "
                                                  "roles; nothing was created",
                                                  resource_});
        }
        auto created =
            logical_plan::make_node_create_schema(resource_,
                                                  qualified_name_t{core::schema_t{schema}, core::relname_t{}},
                                                  node.if_not_exists);
        register_namespace(created->target());
        return created;
    }

} // namespace components::sql::transform
