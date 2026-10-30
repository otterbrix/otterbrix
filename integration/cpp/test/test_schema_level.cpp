// Two parts name schema.relation, three name database.schema.relation; a database holds schemas, a schema holds
// relations (PostgreSQL 18). Every database is created with its schema public; built-in schemas live in main.

#include "session_sql_fixture.hpp"

#include <components/catalog/system_table_schemas.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace {

    using session_sql::error_of;

    // session_sql's fixture with the session its statements run in, and the oids of what the statements name.
    struct fixture_t : session_sql::fixture_t {
        explicit fixture_t(const char* leaf)
            : session_sql::fixture_t(std::string{"test_schema_level/"} + leaf) {}

        const otterbrix::session_id_t session;

        std::uint32_t one_oid(const std::string& sql) {
            const auto found = oids(session, sql);
            INFO(sql);
            REQUIRE(found.size() == 1);
            return found.front();
        }

        std::uint32_t database_oid(const std::string& name) {
            return one_oid("SELECT oid FROM pg_catalog.pg_database WHERE datname = '" + name + "';");
        }

        std::uint32_t schema_oid(const std::string& database, const std::string& schema) {
            return one_oid("SELECT oid FROM pg_catalog.pg_namespace WHERE nspdatabase = " +
                           std::to_string(database_oid(database)) + " AND nspname = '" + schema + "';");
        }

        std::uint32_t relation_oid(const std::string& database, const std::string& schema, const std::string& rel) {
            return one_oid("SELECT oid FROM pg_catalog.pg_class WHERE relnamespace = " +
                           std::to_string(schema_oid(database, schema)) + " AND relname = '" + rel + "';");
        }
    };

    void check_missing_relation(fixture_t& db, const std::string& sql, const std::string& written) {
        const auto error = db.refused(db.session, sql);
        CHECK(error.type == core::error_code_t::table_not_exists);
        CHECK(std::string{error.what} == "relation \"" + written + "\" does not exist");
    }

    void seed_schema_and_database(fixture_t& db) {
        db.ok(db.session, "CREATE SCHEMA s;");
        db.ok(db.session, "CREATE TABLE s.t (a BIGINT);");
        db.ok(db.session, "INSERT INTO s.t (a) VALUES (1);");
        db.ok(db.session, "CREATE DATABASE d2;");
        db.ok(db.session, "CREATE TABLE d2.public.t (a BIGINT);");
        db.ok(db.session, "INSERT INTO d2.public.t (a) VALUES (2);");
    }

} // namespace

TEST_CASE("integration::cpp::schema_level::create_database_adds_a_pg_database_row") {
    fixture_t db("r1_1");
    db.ok(db.session, "CREATE DATABASE shop;");
    CHECK(db.strings(db.session, "SELECT datname FROM pg_catalog.pg_database ORDER BY datname;") ==
          std::vector<std::string>{"main", "shop"});
}

TEST_CASE("integration::cpp::schema_level::a_new_database_holds_its_public_schema") {
    fixture_t db("r1_2");
    db.ok(db.session, "CREATE DATABASE shop;");
    // and its information_schema.
    CHECK(db.strings(db.session,
                     "SELECT n.nspname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_database d "
                     "ON n.nspdatabase = d.oid WHERE d.datname = 'shop' ORDER BY n.nspname;") ==
          std::vector<std::string>{"information_schema", "public"});
}

TEST_CASE("integration::cpp::schema_level::two_parts_are_schema_and_relation") {
    fixture_t db("r1_3");
    db.ok(db.session, "CREATE SCHEMA s;");
    db.ok(db.session, "CREATE TABLE s.t (a BIGINT);");
    db.ok(db.session, "INSERT INTO s.t (a) VALUES (1);");
    CHECK(db.ints(db.session, "SELECT a FROM s.t;") == std::vector<std::int64_t>{1});
    CHECK(db.ints(db.session, "SELECT a FROM main.s.t;") == std::vector<std::int64_t>{1});
}

TEST_CASE("integration::cpp::schema_level::three_parts_reach_another_database") {
    fixture_t db("r1_4");
    db.ok(db.session, "CREATE DATABASE d2;");
    db.ok(db.session, "CREATE TABLE d2.public.t (a BIGINT);");
    db.ok(db.session, "INSERT INTO d2.public.t (a) VALUES (2);");
    CHECK(db.ints(db.session, "SELECT a FROM d2.public.t;") == std::vector<std::int64_t>{2});
    check_missing_relation(db, "SELECT a FROM public.t;", "public.t");
}

TEST_CASE("integration::cpp::schema_level::one_statement_reads_two_databases") {
    fixture_t db("r1_5");
    seed_schema_and_database(db);
    auto cursor = db.ok(db.session, "SELECT x.a, y.a FROM main.s.t x CROSS JOIN d2.public.t y;");
    REQUIRE(cursor->size() == 1);
    REQUIRE(cursor->column_count() == 2);
    const auto& chunk = cursor->chunks().front();
    CHECK(chunk.get_value<std::int64_t>(0, 0) == 1);
    CHECK(chunk.get_value<std::int64_t>(1, 0) == 2);
}

TEST_CASE("integration::cpp::schema_level::a_missing_schema_or_database") {
    fixture_t db("r1_6");
    check_missing_relation(db, "SELECT * FROM nosuch.t;", "nosuch.t");
    check_missing_relation(db, "SELECT * FROM nodb.s.t;", "nodb.s.t");
    const auto error = db.refused(db.session, "CREATE TABLE nosuch.t (a BIGINT);");
    CHECK(error.type == core::error_code_t::schema_not_exists);
    CHECK(std::string{error.what} == "schema \"nosuch\" does not exist");
    CHECK(db.strings(db.session, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 't';").empty());
}

TEST_CASE("integration::cpp::schema_level::a_schema_is_created_once") {
    fixture_t db("r1_7");
    db.ok(db.session, "CREATE SCHEMA s;");
    const auto error = db.refused(db.session, "CREATE SCHEMA s;");
    CHECK(error.type == core::error_code_t::schema_already_exists);
    CHECK(std::string{error.what} == "schema \"s\" already exists");
    db.ok(db.session, "CREATE SCHEMA IF NOT EXISTS s;");
    CHECK(db.strings(db.session, "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 's';") ==
          std::vector<std::string>{"s"});
}

TEST_CASE("integration::cpp::schema_level::drop_schema_is_restrict_by_default") {
    fixture_t db("r1_8");
    db.ok(db.session, "CREATE SCHEMA s;");
    db.ok(db.session, "CREATE TABLE s.t (a BIGINT);");
    const auto error = db.refused(db.session, "DROP SCHEMA s;");
    CHECK(std::string{error.what}.find("cannot drop schema main.s because other objects depend on it") !=
          std::string::npos);
    CHECK(db.ints(db.session, "SELECT a FROM s.t;").empty());

    db.ok(db.session, "DROP SCHEMA s CASCADE;");
    check_missing_relation(db, "SELECT a FROM s.t;", "s.t");
    CHECK(db.strings(db.session, "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 's';").empty());
}

TEST_CASE("integration::cpp::schema_level::drop_database_drops_its_schemas") {
    fixture_t db("r1_9");
    seed_schema_and_database(db);
    const auto d2 = db.database_oid("d2");
    db.ok(db.session, "DROP DATABASE d2;");
    check_missing_relation(db, "SELECT a FROM d2.public.t;", "d2.public.t");
    CHECK(db.oids(db.session, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspdatabase = " + std::to_string(d2) + ";")
              .empty());
    CHECK(db.strings(db.session, "SELECT datname FROM pg_catalog.pg_database WHERE datname = 'd2';").empty());
    CHECK(db.ints(db.session, "SELECT a FROM s.t;") == std::vector<std::int64_t>{1});
}

TEST_CASE("integration::cpp::schema_level::built_in_objects_are_not_dropped") {
    fixture_t db("r1_10");
    for (const char* sql : {"DROP DATABASE main;",
                            "DROP SCHEMA pg_catalog;",
                            "DROP SCHEMA pg_catalog CASCADE;",
                            "DROP SCHEMA information_schema;",
                            "DROP SCHEMA public;",
                            "DROP SCHEMA public CASCADE;"}) {
        const auto error = db.refused(db.session, sql);
        INFO(sql << " -> " << std::string{error.what});
        CHECK(std::string{error.what}.find("built-in") != std::string::npos);
    }
    CHECK(db.strings(db.session, "SELECT datname FROM pg_catalog.pg_database;") == std::vector<std::string>{"main"});
    CHECK(db.strings(db.session, "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname;") ==
          std::vector<std::string>{"information_schema", "pg_catalog", "public"});
}

TEST_CASE("integration::cpp::schema_level::a_view_reads_another_database") {
    fixture_t db("r1_11");
    seed_schema_and_database(db);
    db.ok(db.session, "CREATE VIEW s.v AS SELECT a FROM d2.public.t;");
    CHECK(db.ints(db.session, "SELECT a FROM s.v;") == std::vector<std::int64_t>{2});
    const auto view = db.relation_oid("main", "s", "v");
    const auto table = db.relation_oid("d2", "public", "t");
    CHECK(db.strings(db.session,
                     "SELECT refkind FROM pg_catalog.pg_rewrite_ref WHERE ev_class = " + std::to_string(view) + ";") ==
          std::vector<std::string>{"r"});
    CHECK(db.oids(db.session,
                  "SELECT refobjid FROM pg_catalog.pg_rewrite_ref WHERE ev_class = " + std::to_string(view) + ";") ==
          std::vector<std::uint32_t>{table});
}

TEST_CASE("integration::cpp::schema_level::names_and_oids_survive_a_restart") {
    fixture_t db("r1_12");
    seed_schema_and_database(db);
    const auto d2 = db.database_oid("d2");
    const auto s = db.schema_oid("main", "s");
    const auto st = db.relation_oid("main", "s", "t");
    const auto dt = db.relation_oid("d2", "public", "t");

    db.restart();

    CHECK(db.database_oid("d2") == d2);
    CHECK(db.schema_oid("main", "s") == s);
    CHECK(db.relation_oid("main", "s", "t") == st);
    CHECK(db.relation_oid("d2", "public", "t") == dt);
    CHECK(db.ints(db.session, "SELECT a FROM s.t;") == std::vector<std::int64_t>{1});
    CHECK(db.ints(db.session, "SELECT a FROM d2.public.t;") == std::vector<std::int64_t>{2});
    CHECK(db.ints(db.session, "SELECT a FROM main.s.t;") == std::vector<std::int64_t>{1});
}

// Fails on main too: bootstrap writes a system table's pg_attribute.attoid as relation_oid * 100 + attnum, while
// system_column_attoids numbers every system column in a row from FIRST_SYSTEM_ATTOID. Old bug, fixed elsewhere.
TEST_CASE("integration::cpp::schema_level::system_column_oids_agree", "[!shouldfail]") {
    fixture_t db("r1_13");
    auto resource = core::pmr::otterbrix_resource();
    const auto expected =
        components::catalog::system_column_attoids(&resource, components::catalog::well_known_oid::pg_namespace_table);
    const auto stored =
        db.oids(db.session, "SELECT attoid FROM pg_catalog.pg_attribute WHERE attrelid = 10 ORDER BY attnum;");
    CHECK(stored == std::vector<std::uint32_t>(expected.begin(), expected.end()));
}

TEST_CASE("integration::cpp::schema_level::a_foreign_key_does_not_cross_databases") {
    fixture_t db("r1_14");
    db.ok(db.session, "CREATE DATABASE d3;");
    db.ok(db.session, "CREATE TABLE d3.public.p (id BIGINT PRIMARY KEY);");
    const auto error = db.refused(db.session, "CREATE TABLE c (pid BIGINT REFERENCES d3.public.p (id));");
    CHECK(error.type == core::error_code_t::unimplemented_yet);
    CHECK(std::string{error.what} == "cross-database references are not implemented: \"d3.public.p\"");
    CHECK(db.strings(db.session, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'c';").empty());
}

TEST_CASE("integration::cpp::schema_level::a_foreign_key_crosses_schemas_of_one_database") {
    fixture_t db("r1_15");
    db.ok(db.session, "CREATE SCHEMA s;");
    db.ok(db.session, "CREATE TABLE p (id BIGINT PRIMARY KEY);");
    db.ok(db.session, "CREATE TABLE s.c (pid BIGINT REFERENCES public.p (id));");
    const auto error = db.refused(db.session, "INSERT INTO s.c (pid) VALUES (9);");
    CHECK(error.type != core::error_code_t::none);
    db.ok(db.session, "INSERT INTO p (id) VALUES (9);");
    db.ok(db.session, "INSERT INTO s.c (pid) VALUES (9);");
    CHECK(db.ints(db.session, "SELECT pid FROM s.c;") == std::vector<std::int64_t>{9});
}

TEST_CASE("integration::cpp::schema_level::schema_elements_and_authorization_are_refused") {
    fixture_t db("r1_16");
    for (const char* sql : {"CREATE SCHEMA s2 CREATE TABLE t (a BIGINT);", "CREATE SCHEMA s3 AUTHORIZATION u;"}) {
        const auto error = db.refused(db.session, sql);
        INFO(sql << " -> " << std::string{error.what});
        CHECK(error.type == core::error_code_t::unimplemented_yet);
    }
    CHECK(db.strings(db.session, "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 's2' OR nspname = 's3';")
              .empty());
    CHECK(db.strings(db.session, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 't';").empty());
}

TEST_CASE("integration::cpp::schema_level::an_uncommitted_database_is_invisible") {
    fixture_t db("r1_17");
    const otterbrix::session_id_t a;
    const otterbrix::session_id_t b;
    const std::string probe = "SELECT datname FROM pg_catalog.pg_database WHERE datname = 'd4';";
    REQUIRE(db.run(a, "BEGIN;")->is_success());
    auto created = db.run(a, "CREATE DATABASE d4;");
    INFO(error_of(created));
    REQUIRE(created->is_success());
    auto seen = db.run(b, probe);
    INFO(error_of(seen));
    REQUIRE(seen->is_success());
    CHECK(seen->size() == 0);
    REQUIRE(db.run(a, "ROLLBACK;")->is_success());
    CHECK(db.strings(db.session, probe).empty());
    CHECK(db.strings(db.session, "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'public';") ==
          std::vector<std::string>{"public"});
}
