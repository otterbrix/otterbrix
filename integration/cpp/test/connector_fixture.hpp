#pragma once

#include "session_sql_fixture.hpp"
#include "test_connector.hpp"

#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <memory>
#include <string>

namespace connector_fixture {

    // An engine opened with the test connector factory; the factory outlives it.
    struct host_t {
        explicit host_t(const std::string& leaf,
                        test_connector::storage_maker_fn maker = &test_connector::make_read_storage,
                        std::span<const components::planner::optimizer_rule_t> rules = {},
                        void (*configure)(configuration::config&) = nullptr)
            : factory(std::make_unique<test_connector::factory_t>(maker))
            , db(leaf, components::planner::primitives_t{rules, factory.get()}, configure) {}

        std::pmr::memory_resource* resource() { return db.dispatcher()->resource(); }

        components::cursor::cursor_t_ptr send(const otterbrix::session_id_t& session,
                                              components::logical_plan::execution_plan_t plan) {
            return db.dispatcher()->execute_plan(session, std::move(plan));
        }

        components::cursor::cursor_t_ptr send(components::logical_plan::execution_plan_t plan) {
            return send(otterbrix::session_id_t(), std::move(plan));
        }

        void ok(components::logical_plan::execution_plan_t plan) {
            auto cursor = send(std::move(plan));
            INFO(session_sql::error_of(cursor));
            REQUIRE(cursor->is_success());
        }

        core::error_t refused(components::logical_plan::execution_plan_t plan) {
            auto cursor = send(std::move(plan));
            REQUIRE(cursor->is_error());
            return cursor->get_error();
        }

        components::logical_plan::execution_plan_t plan_of(components::logical_plan::node_ptr node) {
            return components::logical_plan::execution_plan_t{
                resource(),
                std::move(node),
                components::logical_plan::make_parameter_node(resource())};
        }

        // The host's plan that makes `name` a database of `connection` (pg_database.datconnection).
        components::logical_plan::execution_plan_t remote_database(const std::string& connection,
                                                                   const std::string& name) {
            return plan_of(components::logical_plan::make_node_create_database(
                resource(),
                qualified_name_t{core::connection_name_t{connection}, core::dbname_t{name}, {}, {}}));
        }

        // The host's plan that registers the schema `schema` of the database `database` of `connection`.
        components::logical_plan::execution_plan_t
        remote_schema(const std::string& connection, const std::string& database, const std::string& schema) {
            return plan_of(
                components::logical_plan::make_node_create_schema(resource(),
                                                                  qualified_name_t{core::connection_name_t{connection},
                                                                                   core::dbname_t{database},
                                                                                   core::schema_t{schema},
                                                                                   {}}));
        }

        // connection.database with its schema public, as a host registers what the backend has.
        void register_database(const std::string& connection, const std::string& database) {
            ok(remote_database(connection, database));
            ok(remote_schema(connection, database, "public"));
        }

        void register_shop() { register_database("m2", "shop"); }

        // The host's plan that takes the database `name` of `connection` away with what is in it, as DROP DATABASE
        // does: the connector's DROP of each table and schema, then the catalog's rows.
        components::logical_plan::execution_plan_t drop_remote_database(const std::string& connection,
                                                                        const std::string& name) {
            auto drop = components::logical_plan::make_node_drop(resource(),
                                                                 components::logical_plan::drop_target_kind::database);
            drop->set_target(qualified_name_t{core::connection_name_t{connection},
                                              core::dbname_t{name},
                                              core::schema_t{},
                                              core::relname_t{}});
            return plan_of(drop);
        }

        // m2.shop.public.orders(id, amount), created through the connector.
        void register_orders() {
            register_shop();
            db.ok(otterbrix::session_id_t(), "CREATE TABLE m2.shop.public.orders (id BIGINT, amount BIGINT);");
            factory->clear();
        }

        std::unique_ptr<test_connector::factory_t> factory;
        session_sql::fixture_t db;
    };

} // namespace connector_fixture
