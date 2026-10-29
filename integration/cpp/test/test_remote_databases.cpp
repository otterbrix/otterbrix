// A database of a host's connection is a pg_database row whose datconnection names the connection: a host's plan
// creates it, connection.database.schema.table resolves through it, and its tables keep their rows on the backend.

#include "host_fixture.hpp"

#include <components/logical_plan/node_create_collection.hpp>

#include <filesystem>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

using host_fixture::host_t;
using code = core::error_code_t;
using strings = std::vector<std::string>;
using ints = std::vector<std::int64_t>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    std::int64_t count_of(host_t& host, const std::string& table) {
        return host.db.ints(any(), "SELECT count(*) FROM " + table + ";").front();
    }

    std::uint32_t oid_of(host_t& host, const std::string& relname) {
        const auto found =
            host.db.oids(any(), "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(found.size() == 1);
        return found.front();
    }

    std::vector<std::uint32_t> attoids_of(host_t& host, std::uint32_t table) {
        return host.db.oids(any(),
                            "SELECT attoid FROM pg_catalog.pg_attribute WHERE attrelid = " + std::to_string(table) +
                                " ORDER BY attnum;");
    }

    std::uint32_t namespace_of(host_t& host, const std::string& relname) {
        const auto found =
            host.db.oids(any(), "SELECT relnamespace FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(found.size() == 1);
        return found.front();
    }

} // namespace

TEST_CASE("integration::cpp::remote_databases::a_database_of_a_connection_is_a_pg_database_row") {
    host_t host("test_remote_databases/row");
    host.ok(host.remote_database("m2", "shop"));
    CHECK(host.db.strings(any(), "SELECT datname FROM pg_catalog.pg_database ORDER BY datname;") ==
          strings{"main", "shop"});
    CHECK(host.db.strings(any(), "SELECT datconnection FROM pg_catalog.pg_database ORDER BY datname;") ==
          strings{"", "m2"});
    const std::string schemas = "SELECT n.nspname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_database d "
                                "ON n.nspdatabase = d.oid WHERE d.datconnection = 'm2' ORDER BY n.nspname;";
    // Created empty: the host registers the schemas its backend has, in otterbrix's catalog only.
    CHECK(host.db.strings(any(), schemas).empty());
    host.ok(host.remote_schema("m2", "shop", "public"));
    CHECK(host.db.strings(any(), schemas) == strings{"public"});
    CHECK(host.backend().changes().empty());
}

TEST_CASE("integration::cpp::remote_databases::a_remote_and_a_local_database_of_one_name") {
    host_t host("test_remote_databases/one_name");
    host.register_orders();
    host.db.ok(any(), "CREATE DATABASE shop;");
    host.db.ok(any(), "CREATE TABLE shop.public.t (a BIGINT);");
    host.db.ok(any(), "INSERT INTO shop.public.t (a) VALUES (5);");
    host.backend().set_rows("m2.shop.public.orders", {{1, 100}});
    CHECK(host.db.ints(any(), "SELECT a FROM shop.public.t;") == ints{5});
    CHECK(host.db.ints(any(), "SELECT amount FROM m2.shop.public.orders;") == ints{100});
}

TEST_CASE("integration::cpp::remote_databases::a_database_of_a_connection_twice") {
    host_t host("test_remote_databases/twice", nullptr, {"m2", "m3"});
    host.ok(host.remote_database("m2", "shop"));
    const auto error = host.refused(host.remote_database("m2", "shop"));
    CHECK(error.type == code::database_already_exists);
    host.ok(host.remote_database("m3", "shop"));
}

TEST_CASE("integration::cpp::remote_databases::a_table_of_a_connection_has_no_storage") {
    host_t host("test_remote_databases/no_storage");
    host.register_orders();
    const auto orders = oid_of(host, "orders");
    CHECK(host.db.strings(any(), "SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'orders';") == strings{"r"});
    CHECK(attoids_of(host, orders).size() == 2);
    const auto storage =
        host.db.config.main_path / std::to_string(namespace_of(host, "orders")) / std::to_string(orders);
    CHECK_FALSE(std::filesystem::exists(storage));
    host.db.restart();
    CHECK(oid_of(host, "orders") == orders);
    CHECK_FALSE(std::filesystem::exists(storage));
    host.backend().set_rows("m2.shop.public.orders", {{7, 70}});
    CHECK(host.db.ints(any(), "SELECT id FROM m2.shop.public.orders;") == ints{7});
}

TEST_CASE("integration::cpp::remote_databases::the_backend_gets_the_oids_the_catalog_holds") {
    host_t host("test_remote_databases/oids");
    host.register_shop();
    host.db.ok(any(), "CREATE TABLE m2.shop.public.orders (id BIGINT, amount BIGINT);");
    const auto orders = oid_of(host, "orders");
    const std::string name = "m2.shop.public.orders";
    CHECK(host.backend().oid_of(name) == orders);
    CHECK(host.backend().attoids_of(name) == attoids_of(host, orders));

    using oid_call_t = host_actor::backend_t::oid_call_t;
    constexpr auto none = components::catalog::INVALID_OID;

    host.backend().clear_journal();
    REQUIRE(host.db.ints(any(), "SELECT count(*) FROM m2.shop.public.orders;") == ints{0});
    CHECK(host.backend().journal("scan") == strings{"scan " + name});
    CHECK(host.backend().oid_calls() == std::vector<oid_call_t>{{"scan", orders, none}});

    const auto before = attoids_of(host, orders);
    host.backend().clear_journal();
    host.db.ok(any(), "ALTER TABLE m2.shop.public.orders DROP COLUMN amount;");
    host.db.ok(any(), "ALTER TABLE m2.shop.public.orders ADD COLUMN amount BIGINT;");
    const auto after = attoids_of(host, orders);
    CHECK(host.backend().changes() == strings{"alter drop " + name + " amount", "alter add " + name + " amount"});
    CHECK(host.backend().attoids_of(name) == after);
    REQUIRE(before.size() == 2);
    REQUIRE(after.size() == 2);
    CHECK(before[1] != after[1]);
    CHECK(host.backend().oid_calls() ==
          std::vector<oid_call_t>{{"alter drop", orders, before[1]}, {"alter add", orders, after[1]}});

    host.backend().clear_journal();
    host.db.ok(any(), "DROP TABLE m2.shop.public.orders;");
    CHECK(host.backend().changes() == strings{"drop " + name});
    CHECK(host.backend().oid_calls() == std::vector<oid_call_t>{{"drop", orders, none}});
    CHECK_FALSE(host.backend().has_table(name));
}

// As DROP DATABASE, with everything in it: the backend's DROP of each table it reaches, and then the catalog's rows.
TEST_CASE("integration::cpp::remote_databases::a_database_of_a_connection_goes_with_what_is_in_it") {
    host_t host("test_remote_databases/drop");
    host.register_orders();
    host.ok(host.drop_remote_database("m2", "shop"));
    CHECK(host.backend().changes() == strings{"drop m2.shop.public.orders"});
    CHECK(host.db.strings(any(), "SELECT datname FROM pg_catalog.pg_database;") == strings{"main"});
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'orders';") == ints{0});
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.public.orders;",
                          code::database_not_exists,
                          "database \"m2.shop\" does not exist");
}

TEST_CASE("integration::cpp::remote_databases::a_database_of_a_connection_is_created_in_a_transaction") {
    host_t host("test_remote_databases/transaction");
    const auto a = otterbrix::session_id_t();
    const auto b = otterbrix::session_id_t();
    host.db.ok(a, "BEGIN;");
    REQUIRE(host.send(a, host.remote_database("m2", "shop"))->is_success());
    CHECK(host.db.ints(b, "SELECT count(*) FROM pg_catalog.pg_database WHERE datconnection = 'm2';") == ints{0});
    auto schema = host.send(a, host.remote_schema("m2", "shop", "public"));
    REQUIRE(schema->is_error());
    CHECK(schema->get_error().type == code::unimplemented_yet);
    CHECK(std::string{schema->get_error().what} == "connection \"m2\" only supports DDL in autocommit");
    host.db.ok(a, "ROLLBACK;");
    CHECK(host.backend().changes().empty());
    CHECK(count_of(host, "pg_catalog.pg_database") == 1);
}

TEST_CASE("integration::cpp::remote_databases::a_place_in_a_connection") {
    host_t host("test_remote_databases/place");
    const auto session = otterbrix::session_id_t();
    host.db.check_refused(session,
                          "SET search_path = 'm2.shop.public';",
                          code::database_not_exists,
                          "database \"m2.shop\" does not exist");
    host.register_orders();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    auto cursor = host.db.ok(session, "SELECT current_database(), current_schema();");
    CHECK(cursor->chunks().front().get_value<std::string_view>(0, 0) == "shop");
    CHECK(cursor->chunks().front().get_value<std::string_view>(1, 0) == "public");
    host.db.check_refused(session,
                          "SET search_path = 'm2.nodb.public';",
                          code::database_not_exists,
                          "database \"m2.nodb\" does not exist");
    host.backend().set_rows("m2.shop.public.orders", {{3, 30}});
    CHECK(host.db.ints(session, "SELECT id FROM orders;") == ints{3});
}

// CREATE / DROP SCHEMA in a connection's database: otterbrix's checks and the catalog's rows, never the backend; only
// in autocommit.
TEST_CASE("integration::cpp::remote_databases::a_schema_of_a_database_of_a_connection_is_the_catalogs") {
    host_t host("test_remote_databases/schema");
    host.register_orders();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    host.db.ok(session, "CREATE SCHEMA s;");
    host.db.ok(session, "DROP SCHEMA s;");
    host.db.ok(session, "CREATE SCHEMA bad1;");
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'bad1';") == ints{1});
    CHECK(host.backend().changes().empty());
    host.db.ok(session, "CREATE SCHEMA s;");
    const auto restricted = host.db.refused(session, "DROP SCHEMA public;");
    CHECK(restricted.type == code::dependent_objects_still_exist);
    CHECK(std::string{restricted.what}.starts_with(
        "cannot drop schema m2.shop.public because other objects depend on it"));
    for (const std::string& sql : {"CREATE SCHEMA s2;", "DROP SCHEMA s;"}) {
        INFO(sql);
        host.db.ok(session, "BEGIN;");
        host.db.check_refused(session,
                              sql,
                              code::unimplemented_yet,
                              "connection \"m2\" only supports DDL in autocommit");
        host.db.ok(session, "ROLLBACK;");
    }
    CHECK(host.backend().changes().empty());
}

TEST_CASE("integration::cpp::remote_databases::pg_catalog_from_a_connections_place") {
    host_t host("test_remote_databases/pg_catalog_place");
    host.register_orders();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    CHECK(host.db.strings(session, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'pg_class';") ==
          strings{"pg_class"});
    CHECK(host.db.ints(session, "SELECT count(*) FROM pg_class WHERE relname = 'pg_class';") == ints{1});
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.pg_catalog.pg_class;",
                          code::table_not_exists,
                          "relation \"m2.shop.pg_catalog.pg_class\" does not exist");
}

TEST_CASE("integration::cpp::remote_databases::three_parts_are_a_local_database") {
    host_t host("test_remote_databases/three_parts");
    host.register_orders();
    host.db.ok(any(), "CREATE DATABASE shop;");
    host.db.ok(any(), "CREATE TABLE shop.public.t (a BIGINT);");
    host.db.ok(any(), "INSERT INTO shop.public.t (a) VALUES (4);");
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    CHECK(host.db.ints(session, "SELECT a FROM shop.public.t;") == ints{4});
    host.db.check_refused(session,
                          "SELECT * FROM m2.shop.orders;",
                          code::table_not_exists,
                          "relation \"m2.shop.orders\" does not exist");
}

TEST_CASE("integration::cpp::remote_databases::nothing_is_created_in_pg_catalog") {
    host_t host("test_remote_databases/pg_catalog");
    host.db.ok(any(), "CREATE DATABASE d2;");
    for (const std::string& sql :
         {"CREATE TABLE pg_catalog.x (a BIGINT);", "CREATE TABLE d2.pg_catalog.y (a BIGINT);"}) {
        const auto error = host.db.refused(any(), sql);
        INFO(sql);
        CHECK(std::string{error.what}.find("system catalog") != std::string::npos);
    }
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname IN ('x', 'y');") == ints{0});
}

// otterbrix knows databases, not connections: a missing database of a connection answers alike everywhere, and IF
// EXISTS finds nothing to do.
TEST_CASE("integration::cpp::remote_databases::a_missing_database_of_a_connection") {
    host_t host("test_remote_databases/unknown");
    host.register_orders();
    for (const auto& [sql, what] : std::initializer_list<std::pair<std::string, std::string>>{
             {"CREATE TABLE u.d.s.t3 (id BIGINT);", "database \"u.d\" does not exist"},
             {"DROP TABLE u.d.s.t3;", "database \"u.d\" does not exist"},
             {"ALTER TABLE u.d.s.t3 ADD COLUMN x BIGINT;", "database \"u.d\" does not exist"},
             {"INSERT INTO u.d.s.t3 (id) VALUES (1);", "database \"u.d\" does not exist"},
             {"UPDATE u.d.s.t3 SET id = 2;", "database \"u.d\" does not exist"},
             {"DELETE FROM u.d.s.t3;", "database \"u.d\" does not exist"},
             {"SELECT * FROM u.d.s.t3;", "database \"u.d\" does not exist"},
             {"SET search_path = 'u.d.s';", "database \"u.d\" does not exist"},
             {"SELECT * FROM m2.nodb.public.orders;", "database \"m2.nodb\" does not exist"},
             {"CREATE TABLE m2.nodb.public.t3 (id BIGINT);", "database \"m2.nodb\" does not exist"}}) {
        host.db.check_refused(any(), sql, code::database_not_exists, what);
    }
    for (const std::string& sql : {"DROP TABLE IF EXISTS u.d.s.t3;",
                                   "DROP TABLE IF EXISTS m2.nodb.public.t3;",
                                   "ALTER TABLE IF EXISTS u.d.s.t3 ADD COLUMN x BIGINT;"}) {
        host.db.ok(any(), sql);
    }
    // The checks of a name come before the refusals of what a connection's database takes.
    host.db.check_refused(any(),
                          "CREATE VIEW m2.nodb.public.v AS SELECT 1 AS a;",
                          code::database_not_exists,
                          "database \"m2.nodb\" does not exist");
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.check_refused(session,
                          "CREATE TABLE m2.nodb.public.t3 (id BIGINT);",
                          code::database_not_exists,
                          "database \"m2.nodb\" does not exist");
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 't3';") == ints{0});
    CHECK(host.backend().changes().empty());
}

// pg_rewrite_ref keeps a body name's connection apart from its database: a local database named "m2.shop" is not the
// database shop of the connection m2.
TEST_CASE("integration::cpp::remote_databases::a_local_database_named_like_a_connections_one") {
    host_t host("test_remote_databases/dotted_name");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", {{1, 100}, {2, 200}});
    host.db.ok(any(), "CREATE DATABASE \"m2.shop\";");
    host.db.ok(any(), "CREATE TABLE \"m2.shop\".public.orders (id BIGINT, amount BIGINT);");
    host.db.ok(any(), "INSERT INTO \"m2.shop\".public.orders (id, amount) VALUES (9, 900);");
    host.db.ok(any(),
               "CREATE VIEW public.both AS SELECT l.id AS local_id, r.id AS remote_id FROM "
               "\"m2.shop\".public.orders l, m2.shop.public.orders r;");
    CHECK(host.db.ints(any(), "SELECT local_id FROM public.both ORDER BY remote_id;") == ints{9, 9});
    CHECK(host.db.ints(any(), "SELECT remote_id FROM public.both ORDER BY remote_id;") == ints{1, 2});
    CHECK(host.db.ints(any(),
                       "SELECT count(*) FROM pg_catalog.pg_rewrite_ref WHERE connection = 'm2' AND dbname = 'shop';") ==
          ints{1});
}

// open_engine reads the host's map once: every executor keeps its own copy, so the host's array may change or go.
TEST_CASE("integration::cpp::remote_databases::every_executor_has_its_own_copy_of_the_map") {
    host_fixture::backends_t backends({"m2"});
    auto config = test_create_config(integration_fixture_path("test_remote_databases/executors"));
    config.log.level = log_t::level::off;
    test_clear_directory(config);
    std::string connection = "m2";
    std::vector<components::planner::backend_address_t> map{{connection, backends.actors.front()->address()}};
    test_spaces space(config, components::planner::primitives_t{map});
    connection = "xx";
    map.clear();
    map.shrink_to_fit();
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();
    const auto plan_of = [resource](components::logical_plan::node_ptr node) {
        return components::logical_plan::execution_plan_t{resource,
                                                          std::move(node),
                                                          components::logical_plan::make_parameter_node(resource)};
    };
    REQUIRE(dispatcher
                ->execute_plan(any(),
                               plan_of(components::logical_plan::make_node_create_database(
                                   resource,
                                   qualified_name_t{core::connection_name_t{"m2"}, core::dbname_t{"shop"}, {}, {}})))
                ->is_success());
    REQUIRE(dispatcher
                ->execute_plan(any(),
                               plan_of(components::logical_plan::make_node_create_schema(
                                   resource,
                                   qualified_name_t{core::connection_name_t{"m2"},
                                                    core::dbname_t{"shop"},
                                                    core::schema_t{"public"},
                                                    {}})))
                ->is_success());
    const auto statements = 2 * config.execution.executor_pool_size;
    for (std::size_t i = 0; i < statements; ++i) {
        const std::string table = "t" + std::to_string(i);
        INFO(table);
        REQUIRE(dispatcher->execute_sql(any(), "CREATE TABLE m2.shop.public." + table + " (id BIGINT);")->is_success());
        REQUIRE(dispatcher->execute_sql(any(), "SELECT id FROM m2.shop.public." + table + ";")->is_success());
    }
    CHECK(backends.of("m2").journal("create").size() == statements);
    CHECK(backends.of("m2").journal("scan").size() == statements);
}

TEST_CASE("integration::cpp::remote_databases::only_tables_in_a_database_of_a_connection") {
    host_t host("test_remote_databases/only_tables");
    host.register_orders();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    const std::string refusal = "only tables can be created in a schema of connection \"m2\"";
    for (const std::string& sql : {"CREATE VIEW v AS SELECT id FROM orders;",
                                   "CREATE SEQUENCE q;",
                                   "CREATE FUNCTION f(x INT) RETURNS INT AS 'x -> x';",
                                   "CREATE TYPE pair_t AS (x BIGINT);"}) {
        host.db.check_refused(session, sql, code::invalid_parameter, refusal);
    }
    host.db.check_refused(any(),
                          "CREATE VIEW m2.shop.public.v AS SELECT id FROM m2.shop.public.orders;",
                          code::invalid_parameter,
                          refusal);
    host.db.ok(any(), "CREATE VIEW public.v AS SELECT id FROM m2.shop.public.orders;");
}

// pg_catalog.x is main's from any place, a connection's included: CREATE there is refused as in a system catalog.
TEST_CASE("integration::cpp::remote_databases::create_in_pg_catalog_from_a_connections_place") {
    host_t host("test_remote_databases/create_pg_catalog");
    host.register_orders();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    const auto error = host.db.refused(session, "CREATE TABLE pg_catalog.x (a BIGINT);");
    CHECK(std::string{error.what}.find("system catalog") != std::string::npos);
    CHECK(host.backend().changes().empty());
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'x';") == ints{0});
}

// A schema of a connection's database is otterbrix's catalog alone; a database of a connection the host's map does
// not hold is refused.
TEST_CASE("integration::cpp::remote_databases::a_schema_needs_no_backend") {
    host_t host("test_remote_databases/no_connector");
    host.ok(host.remote_database("m2", "shop"));
    host.ok(host.remote_schema("m2", "shop", "s"));
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 's';") == ints{1});
    CHECK(host.backend().changes().empty());
    const auto refused = host.refused(host.remote_database("m3", "shop"));
    CHECK(refused.type == code::do_not_exists);
    CHECK(std::string{refused.what} == "connection \"m3\" does not exist");
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_database WHERE datconnection = 'm3';") == ints{0});
}

// A host's CREATE plan for a table of a connection's database sends the backend create_storage_disk, then the prepare
// and the release of the statement's transaction, in this order, and nothing else.
TEST_CASE("integration::cpp::remote_databases::a_host_create_sends_the_backend_create_prepare_and_release") {
    host_t host("test_remote_databases/host_create_messages");
    host.register_shop();
    host.backend().clear_journal();
    const auto before = host.backend().messages();

    std::vector<components::table::column_definition_t> columns;
    columns.emplace_back("id", components::types::complex_logical_type(components::types::logical_type::BIGINT));
    columns.emplace_back("amount", components::types::complex_logical_type(components::types::logical_type::BIGINT));
    auto create = components::logical_plan::make_node_create_collection(host.resource(),
                                                                        core::relname_t{"orders"},
                                                                        std::move(columns),
                                                                        {});
    create->set_target(qualified_name_t{core::connection_name_t{"m2"},
                                        core::dbname_t{"shop"},
                                        core::schema_t{"public"},
                                        core::relname_t{"orders"}});
    host.ok(host.plan_of(create));

    CHECK(host.backend().messages() - before == 3);
    CHECK(host.backend().journal() == strings{"create m2.shop.public.orders id amount", "prepare 1", "release"});
    CHECK(host.db.strings(any(), "SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'orders';") == strings{"r"});
}
