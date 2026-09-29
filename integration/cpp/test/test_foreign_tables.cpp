// Remote servers are an in-memory registry "server name -> connector type" the host hands to spawn_engine and
// changes at runtime; the catalog keeps nothing for them. A name whose first part is a registered server is remote;
// every other name resolves as before.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <array>

using namespace components;

namespace {

    components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    std::string error_of(const components::cursor::cursor_t_ptr& cur) {
        return cur->is_error() ? std::string(cur->get_error().what) : std::string{"<ok>"};
    }

    std::size_t rows_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_success());
        return cur->size();
    }

    std::string what_of(const core::error_t& err) { return std::string(err.what); }

    void add_m2(test_spaces& space) {
        auto err = space.add_server("m2", "mysql");
        INFO(what_of(err));
        REQUIRE_FALSE(err.contains_error());
    }

    // server.name leaves the schema to be guessed: refused before any connector call.
    void require_full_path_demanded(const components::cursor::cursor_t_ptr& cur, const std::string& sql) {
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::invalid_parameter);
        REQUIRE(error_of(cur).find("write the full path as server.schema.table") != std::string::npos);
    }

    void require_no_connector(const components::cursor::cursor_t_ptr& cur, const std::string& sql) {
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::connector_not_exists);
        REQUIRE(error_of(cur).find("no connector for server type 'mysql'") != std::string::npos);
    }

    void require_database_missing(const components::cursor::cursor_t_ptr& cur, const std::string& sql) {
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::database_not_exists);
    }

} // namespace

TEST_CASE("integration::cpp::foreign_tables::spawn_time_servers_make_names_remote") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/spawn"));
    test_clear_directory(config);
    const std::array servers{services::remote_server_t{"m2", "mysql"}, services::remote_server_t{"pg1", "postgres"}};
    services::engine::primitives_t primitives;
    primitives.servers = servers;
    test_spaces space(config, primitives);
    auto* dispatcher = space.dispatcher();

    for (const std::string sql : {"SELECT * FROM m2.shop.orders;",
                                  "SELECT * FROM m2.crm.shop.orders;",
                                  "SELECT count(*) FROM m2.shop.orders;"}) {
        require_no_connector(exec(dispatcher, sql), sql);
    }
    auto postgres = exec(dispatcher, "SELECT * FROM pg1.public.t;");
    INFO(error_of(postgres));
    REQUIRE(postgres->get_error().type == core::error_code_t::connector_not_exists);
    REQUIRE(error_of(postgres).find("no connector for server type 'postgres'") != std::string::npos);
    // Nothing reaches the catalog for a remote name.
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'orders';") == 0);
}

TEST_CASE("integration::cpp::foreign_tables::runtime_add_and_remove") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/runtime"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    const std::string sql = "SELECT * FROM m2.shop.orders;";

    require_database_missing(exec(dispatcher, sql), sql);
    add_m2(space);
    require_no_connector(exec(dispatcher, sql), sql);

    auto duplicate = space.add_server("m2", "postgres");
    REQUIRE(duplicate.type == core::error_code_t::server_already_exists);
    for (const auto& [name, type] : std::array{std::pair{"", "mysql"}, std::pair{"m3", ""}}) {
        auto empty = space.add_server(name, type);
        INFO(name << "/" << type);
        REQUIRE(empty.type == core::error_code_t::invalid_parameter);
    }

    REQUIRE_FALSE(space.remove_server("m2").contains_error());
    require_database_missing(exec(dispatcher, sql), sql);
    REQUIRE(space.remove_server("m2").type == core::error_code_t::server_not_exists);

    add_m2(space);
    require_no_connector(exec(dispatcher, sql), sql);
}

// The registry is memory only: a restart without the server knows nothing of it.
TEST_CASE("integration::cpp::foreign_tables::servers_do_not_survive_restart") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/restart"));
    test_clear_directory(config);
    {
        test_spaces space(config);
        add_m2(space);
    }
    test_spaces space(config);
    const std::string sql = "SELECT * FROM m2.shop.orders;";
    require_database_missing(exec(space.dispatcher(), sql), sql);
    for (const std::string table :
         {"pg_foreign_server", "pg_foreign_option", "pg_foreign_namespace", "pg_foreign_table"}) {
        auto cur = exec(space.dispatcher(), "SELECT * FROM pg_catalog." + table + ";");
        INFO(table << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::table_not_exists);
    }
}

TEST_CASE("integration::cpp::foreign_tables::server_sql_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/sql_refused"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE fdb;")->is_success());

    for (const std::string sql : {"CREATE SERVER m2 FOREIGN DATA WRAPPER mysql_fdw;",
                                  "CREATE SERVER m2 TYPE 'mysql' FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host 'h');",
                                  "DROP SERVER m2;",
                                  "DROP SERVER IF EXISTS m2;",
                                  "CREATE FOREIGN TABLE fdb.ft (id BIGINT) SERVER m2;",
                                  "DROP FOREIGN TABLE fdb.ft;"}) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::sql_parse_error);
    }
}

TEST_CASE("integration::cpp::foreign_tables::server_and_database_names_do_not_collide") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/collisions"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE fdb;")->is_success());
    add_m2(space);

    for (const std::string name : {"fdb", "public", "pg_catalog"}) {
        auto err = space.add_server(name, "mysql");
        INFO(name << ": " << what_of(err));
        REQUIRE(err.type == core::error_code_t::already_exists);
    }
    auto db = exec(dispatcher, "CREATE DATABASE m2;");
    INFO(error_of(db));
    REQUIRE(db->is_error());
    REQUIRE(db->get_error().type == core::error_code_t::already_exists);
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'm2';") == 0);

    auto pg_prefix = space.add_server("pg_remote", "mysql");
    INFO(what_of(pg_prefix));
    REQUIRE_FALSE(pg_prefix.contains_error());

    // Once the server is gone the name is free for a database again.
    REQUIRE_FALSE(space.remove_server("m2").contains_error());
    REQUIRE(exec(dispatcher, "CREATE DATABASE m2;")->is_success());
}

TEST_CASE("integration::cpp::foreign_tables::spawn_time_collisions_refuse_the_start") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/spawn_collisions"));
    test_clear_directory(config);
    {
        test_spaces space(config);
        REQUIRE(exec(space.dispatcher(), "CREATE DATABASE fdb;")->is_success());
    }
    {
        const std::array servers{services::remote_server_t{"fdb", "mysql"}};
        services::engine::primitives_t primitives;
        primitives.servers = servers;
        auto refused = otterbrix::base_otterbrix_t::open(config, primitives);
        REQUIRE(refused.has_error());
        INFO(what_of(refused.error()));
        REQUIRE(refused.error().type == core::error_code_t::already_exists);
    }
    {
        const std::array servers{services::remote_server_t{"m2", "mysql"}, services::remote_server_t{"m2", "pg"}};
        services::engine::primitives_t primitives;
        primitives.servers = servers;
        auto refused = otterbrix::base_otterbrix_t::open(config, primitives);
        REQUIRE(refused.has_error());
        REQUIRE(refused.error().type == core::error_code_t::server_already_exists);
    }
    {
        const std::array servers{services::remote_server_t{"m2", ""}};
        services::engine::primitives_t primitives;
        primitives.servers = servers;
        auto refused = otterbrix::base_otterbrix_t::open(config, primitives);
        REQUIRE(refused.has_error());
        REQUIRE(refused.error().type == core::error_code_t::invalid_parameter);
    }
}

TEST_CASE("integration::cpp::foreign_tables::server_and_one_part_is_not_a_remote_path") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/one_part"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);

    for (const std::string sql : {"SELECT * FROM m2.orders;",
                                  "SELECT count(*) FROM m2.orders;",
                                  "INSERT INTO m2.orders (id, name) VALUES (1, 'a');",
                                  "UPDATE m2.orders SET name = 'b' WHERE id = 1;",
                                  "DELETE FROM m2.orders WHERE id = 1;",
                                  "CREATE TABLE m2.t (id BIGINT);",
                                  "CREATE VIEW m2.v AS SELECT 1 AS a;",
                                  "CREATE SEQUENCE m2.s;",
                                  "CREATE TYPE m2.pair AS (a BIGINT, b BIGINT);",
                                  "DROP TABLE m2.orders;",
                                  "DROP VIEW m2.v;",
                                  "CREATE INDEX ft_id ON m2.orders (id);"}) {
        require_full_path_demanded(exec(dispatcher, sql), sql);
    }
    auto cur = exec(dispatcher, "SELECT * FROM m2.orders;");
    REQUIRE(error_of(cur).find("remote table \"m2.orders\"") != std::string::npos);
}

// Until a connector for the server type exists, a remote read is refused — never a disk scan or a disk reduce.
TEST_CASE("integration::cpp::foreign_tables::read_without_connector_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/no_connector"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);

    for (const std::string sql : {"SELECT id, name FROM m2.shop.orders;",
                                  "SELECT count(*) FROM m2.shop.orders;",
                                  "SELECT id FROM m2.shop.orders WHERE id = 1;",
                                  "SELECT name, count(*) FROM m2.shop.orders GROUP BY name;",
                                  "SELECT id FROM m2.shop.orders ORDER BY id LIMIT 1;",
                                  "SELECT a.id FROM m2.sales.orders AS a JOIN m2.archive.orders AS b "
                                  "ON a.id = b.id;"}) {
        auto cur = exec(dispatcher, sql);
        require_no_connector(cur, sql);
        REQUIRE(error_of(cur).find("orders") != std::string::npos);
    }
}

TEST_CASE("integration::cpp::foreign_tables::writes_through_a_server_need_a_connector") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/dml"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);

    for (const std::string target : {"m2.shop.orders", "m2.crm.shop.orders"}) {
        for (const auto& sql : {"INSERT INTO " + target + " (id, name) VALUES (1, 'a');",
                                "UPDATE " + target + " SET name = 'b' WHERE id = 1;",
                                "DELETE FROM " + target + " WHERE id = 1;"}) {
            require_no_connector(exec(dispatcher, sql), sql);
        }
    }
}

TEST_CASE("integration::cpp::foreign_tables::ddl_through_a_server_needs_a_connector") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/ddl"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);

    for (const std::string sql : {"CREATE TABLE m2.shop.t (id BIGINT);",
                                  "CREATE TABLE m2.crm.shop.t (id BIGINT);",
                                  "ALTER TABLE m2.shop.orders ADD COLUMN z BIGINT;",
                                  "CREATE VIEW m2.shop.v AS SELECT 1 AS a;",
                                  "CREATE SEQUENCE m2.shop.s;",
                                  "CREATE TYPE m2.shop.pair AS (a BIGINT, b BIGINT);",
                                  "DROP TABLE m2.shop.orders;",
                                  "DROP VIEW m2.shop.v;"}) {
        require_no_connector(exec(dispatcher, sql), sql);
    }
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 't';") == 0);
}

TEST_CASE("integration::cpp::foreign_tables::indexes_on_remote_tables_are_refused_by_otterbrix") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/no_index"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);

    for (const std::string sql : {"CREATE INDEX ft_id ON m2.shop.orders (id);",
                                  "CREATE INDEX ft_id ON m2.crm.shop.orders (id);",
                                  "DROP INDEX m2.shop.orders.ft_id;"}) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::invalid_parameter);
        REQUIRE(error_of(cur).find("remote") != std::string::npos);
    }
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'ft_id';") == 0);
}

TEST_CASE("integration::cpp::foreign_tables::functions_of_a_server_are_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/functions"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);

    for (const std::string sql : {"SELECT m2.f(1);", "SELECT * FROM m2.f(1);", "SELECT m2.shop.f(1);"}) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::unimplemented_yet);
        REQUIRE(error_of(cur).find("functions of a remote server are not supported") != std::string::npos);
    }
}

TEST_CASE("integration::cpp::foreign_tables::names_without_a_server_resolve_as_before") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/unchanged"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);
    REQUIRE(exec(dispatcher, "CREATE DATABASE d;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE d.t (id BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO d.t (id) VALUES (1), (2);")->is_success());

    REQUIRE(rows_of(dispatcher, "SELECT id FROM d.t;") == 2);
    auto read_schema = exec(dispatcher, "SELECT id FROM d.s.t;");
    REQUIRE(read_schema->is_error());
    REQUIRE(read_schema->get_error().type == core::error_code_t::invalid_parameter);
    REQUIRE(rows_of(dispatcher, "WITH m2 AS (SELECT 1 AS a) SELECT a FROM m2;") == 1);

    auto one_part = exec(dispatcher, "SELECT * FROM m2;");
    REQUIRE(one_part->is_error());
    REQUIRE(one_part->get_error().type == core::error_code_t::table_not_exists);
    require_database_missing(exec(dispatcher, "SELECT * FROM nodb.t;"), "SELECT * FROM nodb.t;");
    auto write_schema = exec(dispatcher, "INSERT INTO d.s.t (id) VALUES (3);");
    REQUIRE(write_schema->is_error());
    REQUIRE(write_schema->get_error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("integration::cpp::foreign_tables::server_names_fold_like_identifiers") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/case"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE_FALSE(space.add_server("M2", "mysql").contains_error());

    const std::string quoted = "SELECT * FROM \"M2\".shop.orders;";
    require_no_connector(exec(dispatcher, quoted), quoted);
    require_database_missing(exec(dispatcher, "SELECT * FROM M2.shop.orders;"), "M2.shop.orders");
}

// A foreign key cannot point at a remote table, whatever the connector (PostgreSQL 18 refuses a foreign table as a
// referenced relation).
TEST_CASE("integration::cpp::foreign_tables::a_foreign_key_to_a_remote_table_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/fk_remote"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    add_m2(space);
    REQUIRE(exec(dispatcher, "CREATE DATABASE d;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE d.o (cid BIGINT);")->is_success());

    const std::vector<std::pair<std::string, std::string>> cases{
        {"CREATE TABLE d.c1 (cid BIGINT REFERENCES m2.shop.customers (id));", "m2.shop.customers"},
        {"CREATE TABLE d.c2 (cid BIGINT, FOREIGN KEY (cid) REFERENCES m2.crm.shop.customers (id));",
         "m2.crm.shop.customers"},
        {"CREATE TABLE d.c3 (cid BIGINT REFERENCES m2.customers (id));", "m2.customers"},
        {"ALTER TABLE d.o ADD CONSTRAINT fk FOREIGN KEY (cid) REFERENCES m2.shop.customers (id);",
         "m2.shop.customers"}};
    for (const auto& [sql, target] : cases) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::invalid_constraint);
        REQUIRE(error_of(cur).find("referenced relation \"" + target + "\" is not a table") != std::string::npos);
    }
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'c1';") == 0);
}
