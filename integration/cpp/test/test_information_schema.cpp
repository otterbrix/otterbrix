#include "session_sql_fixture.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>

#include <string>
#include <vector>

namespace {

    using strings = std::vector<std::string>;
    using ints = std::vector<std::int64_t>;

    const std::string views_of_main = "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON "
                                      "n.oid = c.relnamespace WHERE n.nspname = 'information_schema' AND "
                                      "n.nspdatabase = 5 AND c.relkind = 'v' ORDER BY c.relname;";

} // namespace

TEST_CASE("integration::cpp::information_schema::views_are_in_pg_class") {
    session_sql::fixture_t db("test_information_schema/pg_class");
    const otterbrix::session_id_t session;
    CHECK(db.strings(session, views_of_main) == strings{"columns", "schemata"});
    CHECK(db.strings(session,
                     "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = "
                     "a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspdatabase = 5 AND "
                     "n.nspname = 'information_schema' AND c.relname = 'schemata' ORDER BY a.attnum;") ==
          strings{"catalog_name",
                  "schema_name",
                  "schema_owner",
                  "default_character_set_catalog",
                  "default_character_set_schema",
                  "default_character_set_name",
                  "sql_path"});
}

TEST_CASE("integration::cpp::information_schema::columns_reads_pg_attribute") {
    session_sql::fixture_t db("test_information_schema/columns");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session,
          "CREATE TABLE s.items (id bigint NOT NULL, name text, price numeric(10, 2), qty integer, active boolean, "
          "created_at timestamp);");
    db.ok(session, "ALTER TABLE s.items DROP COLUMN qty;");
    const std::string of_items = " FROM information_schema.columns WHERE table_schema = 's' AND table_name = 'items' "
                                 "ORDER BY ordinal_position;";
    CHECK(db.strings(session, "SELECT column_name" + of_items) ==
          db.strings(session,
                     "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = (SELECT oid FROM "
                     "pg_catalog.pg_class WHERE relname = 'items') AND attnum > 0 AND attisdropped = false ORDER BY "
                     "attnum;"));
    CHECK(db.strings(session, "SELECT column_name" + of_items) ==
          strings{"id", "name", "price", "active", "created_at"});
    CHECK(db.strings(session, "SELECT is_nullable" + of_items) == strings{"NO", "YES", "YES", "YES", "YES"});
    CHECK(db.strings(session, "SELECT data_type" + of_items) ==
          strings{"bigint", "text", "numeric", "boolean", "timestamp without time zone"});
    CHECK(db.strings(session, "SELECT udt_name" + of_items) == strings{"int8", "text", "numeric", "bool", "timestamp"});
    CHECK(db.ints(session,
                  "SELECT CAST(numeric_precision AS bigint) FROM information_schema.columns WHERE table_name = "
                  "'items' AND column_name = 'price';") == ints{10});
    CHECK(db.ints(session,
                  "SELECT CAST(numeric_scale AS bigint) FROM information_schema.columns WHERE table_name = 'items' "
                  "AND column_name = 'price';") == ints{2});
}

TEST_CASE("integration::cpp::information_schema::a_database_has_its_own") {
    session_sql::fixture_t db("test_information_schema/database");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.in_main (a bigint);");
    db.ok(session, "CREATE DATABASE d2;");
    db.ok(session, "CREATE TABLE d2.public.in_d2 (b bigint);");

    CHECK(db.strings(session, "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;") ==
          strings{"information_schema", "pg_catalog", "public", "s"});
    CHECK(db.strings(session, "SELECT schema_name FROM d2.information_schema.schemata ORDER BY schema_name;") ==
          strings{"information_schema", "pg_catalog", "public"});
    CHECK(db.strings(session,
                     "SELECT table_name FROM information_schema.columns WHERE table_schema <> 'pg_catalog' AND "
                     "table_schema <> 'information_schema';") == strings{"in_main"});
    CHECK(db.strings(session,
                     "SELECT table_name FROM d2.information_schema.columns WHERE table_schema = 'public';") ==
          strings{"in_d2"});
    CHECK(db.strings(session, "SELECT DISTINCT table_catalog FROM d2.information_schema.columns;") == strings{"d2"});
}

TEST_CASE("integration::cpp::information_schema::go_with_a_rolled_back_database") {
    session_sql::fixture_t db("test_information_schema/rollback");
    const otterbrix::session_id_t session;
    db.ok(session, "BEGIN;");
    db.ok(session, "CREATE DATABASE d2;");
    db.ok(session, "ROLLBACK;");
    CHECK(db.ints(session,
                  "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = "
                  "c.relnamespace WHERE n.nspname = 'information_schema';") == ints{2});
}

TEST_CASE("integration::cpp::information_schema::survive_a_restart") {
    session_sql::fixture_t db("test_information_schema/restart");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE DATABASE d2;");
    const std::string views = "SELECT CAST(c.oid AS text) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON "
                              "n.oid = c.relnamespace WHERE n.nspname = 'information_schema' ORDER BY c.oid;";
    const auto before = db.strings(session, views);
    REQUIRE(before.size() == 4);
    db.restart();
    CHECK(db.strings(session, views) == before);
    CHECK(db.strings(session, "SELECT schema_name FROM d2.information_schema.schemata WHERE schema_name = 'public';") ==
          strings{"public"});
}

// The declared columns are written by hand next to each body: what the body computes must be what pg_attribute says.
TEST_CASE("integration::cpp::information_schema::declared_types_are_the_bodies") {
    session_sql::fixture_t db("test_information_schema/types");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint);");
    db.ok(session, "CREATE DATABASE d2;");
    for (const std::string database : {"main", "d2"}) {
        for (const std::string view : {"schemata", "columns"}) {
            INFO(database << ".information_schema." << view);
            auto rows = db.ok(session, "SELECT * FROM " + database + ".information_schema." + view + ";");
            REQUIRE(rows->size() > 0);
            const auto& computed = rows->chunks().front();
            auto declared = db.ok(session,
                                  "SELECT a.attname, a.atttypid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class "
                                  "c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                                  "JOIN pg_catalog.pg_database d ON d.oid = n.nspdatabase WHERE d.datname = '" +
                                      database + "' AND n.nspname = 'information_schema' AND c.relname = '" + view +
                                      "' ORDER BY a.attnum;");
            REQUIRE(declared->size() == computed.column_count());
            const auto& attributes = declared->chunks().front();
            for (std::uint64_t column = 0; column < computed.column_count(); ++column) {
                INFO("column " << attributes.get_value<std::string_view>(0, column));
                CHECK(std::string{computed.data[column].type().alias()} ==
                      std::string{attributes.get_value<std::string_view>(0, column)});
                CHECK(computed.data[column].type().type() ==
                      components::catalog::oid_to_builtin_type(
                          attributes.get_value<components::catalog::oid_t>(1, column)));
            }
        }
    }
}
