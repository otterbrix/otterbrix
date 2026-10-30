#pragma once

#include "session_sql_fixture.hpp"
#include "test_host_actor.hpp"

#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/host_hooks.hpp>

#include <memory>
#include <memory_resource>
#include <string>
#include <vector>

namespace host_fixture {

    using strings = std::vector<std::string>;

    // The most rows one write message to a connection carries, by connection (absent: no limit).
    using caps_t = std::vector<std::pair<std::string, std::uint64_t>>;

    // The backend actors of a host's connections, one per connection, and the map open_engine reads. They outlive the
    // engine: they are declared before it. "b=a" makes the connection b another connection of a's actor.
    struct backends_t {
        explicit backends_t(const strings& connections, caps_t write_caps = {})
            : caps(std::move(write_caps)) {
            for (const auto& spec : connections) {
                const auto shared = spec.find('=');
                if (shared == std::string::npos) {
                    names.push_back(spec);
                    actor_of.push_back(actors.size());
                    actors.push_back(actor_zeta::spawn<host_actor::backend_t>(std::pmr::new_delete_resource(), spec));
                    continue;
                }
                names.push_back(spec.substr(0, shared));
                actor_of.push_back(index_of(spec.substr(shared + 1)));
            }
            map_all();
        }

        // The map holds only `present` of the connections (for the next open).
        void map_only(const strings& present) {
            map.clear();
            for (std::size_t i = 0; i < names.size(); ++i) {
                if (std::find(present.begin(), present.end(), names[i]) != present.end()) {
                    std::uint64_t cap = 0;
                    for (const auto& [connection, rows] : caps) {
                        if (connection == names[i]) {
                            cap = rows;
                        }
                    }
                    map.push_back({names[i], actors[actor_of[i]]->address(), cap});
                }
            }
        }
        void map_all() { map_only(names); }

        host_actor::backend_t& of(const std::string& connection) { return *actors[actor_of[index_of(connection)]]; }

        components::planner::primitives_t primitives() const { return components::planner::primitives_t{map}; }

        strings names;
        caps_t caps;
        std::vector<std::size_t> actor_of;
        std::vector<host_actor::backend_ptr> actors;
        std::vector<components::planner::backend_address_t> map;

    private:
        std::size_t index_of(const std::string& connection) const {
            for (std::size_t i = 0; i < names.size(); ++i) {
                if (names[i] == connection) {
                    return i;
                }
            }
            FAIL("the host has no backend of connection " << connection);
            return 0;
        }
    };

    // An engine of a host whose connections (m2 by default) have test backends.
    struct host_t {
        explicit host_t(const std::string& leaf,
                        void (*configure)(configuration::config&) = nullptr,
                        strings connections = {"m2"},
                        caps_t write_caps = {})
            : backends(connections, std::move(write_caps))
            , db(leaf, backends.primitives(), configure) {}

        host_actor::backend_t& backend(const std::string& connection = "m2") { return backends.of(connection); }

        // Reopens the engine with a map of only `present` connections.
        void restart_with(const strings& present) {
            backends.map_only(present);
            db.primitives = backends.primitives();
            db.restart();
        }

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

        // The host's plan that registers the schema `schema` of the database `database` of `connection`: a row of
        // otterbrix's catalog only.
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
        // does: the backend's DROP of its tables, then the catalog's rows.
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

        // m2.shop.public.orders(id, amount), created in the backend.
        void register_orders() {
            register_shop();
            db.ok(otterbrix::session_id_t(), "CREATE TABLE m2.shop.public.orders (id BIGINT, amount BIGINT);");
            backend().clear_journal();
        }

        backends_t backends;
        session_sql::fixture_t db;
    };

} // namespace host_fixture
