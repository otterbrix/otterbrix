#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/connection.hpp>
#include <unistd.h>

#include <string>

namespace {

    void require_ok(otterbrix::connection_t& connection, const std::string& sql) {
        auto cursor = connection.execute(sql);
        INFO("statement: " << sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
    }

    std::size_t visible_rows(otterbrix::connection_t& connection) {
        auto cursor = connection.execute("SELECT id FROM leakdb.t;");
        INFO("SELECT error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        return cursor->size();
    }

    void require_next_statement_sees_a_fresh_snapshot(otterbrix::connection_t& reader,
                                                      otterbrix::connection_t& writer,
                                                      const std::string& empty_statement) {
        const auto before = visible_rows(reader);
        require_ok(reader, empty_statement);
        require_ok(writer, "INSERT INTO leakdb.t (id) VALUES (1);");
        REQUIRE(visible_rows(reader) == before + 1);
    }

} // namespace

TEST_CASE("integration::cpp::empty_statement_transaction::each_empty_success_ends_its_transaction") {
    auto config =
        test_create_config(integration_fixture_path("empty_statement_transaction") / std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    otterbrix::connection_t writer(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id bigint);");

    SECTION("CREATE DATABASE IF NOT EXISTS on an existing database") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "CREATE DATABASE IF NOT EXISTS leakdb;");
    }
    SECTION("CREATE TABLE IF NOT EXISTS on an existing table") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "CREATE TABLE IF NOT EXISTS leakdb.t (id bigint);");
    }
    SECTION("DROP DATABASE IF EXISTS on a missing database") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP DATABASE IF EXISTS nosuchdb;");
    }
    SECTION("DROP TABLE IF EXISTS on a missing table") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP TABLE IF EXISTS leakdb.nosuch;");
    }
    SECTION("DROP TABLE IF EXISTS in a missing database") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP TABLE IF EXISTS nosuchdb.nosuch;");
    }
    SECTION("DROP TYPE IF EXISTS on a missing type") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP TYPE IF EXISTS nosuch_type;");
    }
    SECTION("DROP SEQUENCE IF EXISTS on a missing sequence") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP SEQUENCE IF EXISTS leakdb.nosuch;");
    }
    SECTION("DROP FUNCTION (macro) IF EXISTS on a missing macro") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP FUNCTION IF EXISTS leakdb.nosuch();");
    }
    SECTION("DROP VIEW IF EXISTS on a missing view") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP VIEW IF EXISTS leakdb.nosuch;");
    }
    SECTION("DROP MATERIALIZED VIEW IF EXISTS on a missing materialized view") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "DROP MATERIALIZED VIEW IF EXISTS leakdb.nosuch;");
    }
    SECTION("DROP INDEX IF EXISTS on a missing index") {
        require_next_statement_sees_a_fresh_snapshot(connection, writer, "DROP INDEX IF EXISTS leakdb.t.nosuch;");
    }
    SECTION("ALTER TABLE IF EXISTS on a missing table ADD CONSTRAINT") {
        require_next_statement_sees_a_fresh_snapshot(
            connection,
            writer,
            "ALTER TABLE IF EXISTS leakdb.nosuch ADD CONSTRAINT c UNIQUE (id);");
    }
    SECTION("ALTER TABLE DROP COLUMN IF EXISTS on a missing column") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "ALTER TABLE leakdb.t DROP COLUMN IF EXISTS nosuch;");
    }
    SECTION("ALTER TABLE DROP CONSTRAINT IF EXISTS on a missing constraint") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "ALTER TABLE leakdb.t DROP CONSTRAINT IF EXISTS nosuch;");
    }
    SECTION("ALTER TABLE IF EXISTS on a missing table ADD COLUMN") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "ALTER TABLE IF EXISTS leakdb.nosuch ADD COLUMN x bigint;");
    }
    SECTION("ALTER TABLE IF EXISTS on a missing table RENAME COLUMN") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "ALTER TABLE IF EXISTS leakdb.nosuch RENAME COLUMN id TO x;");
    }
    SECTION("ALTER TABLE IF EXISTS on a missing table DROP COLUMN") {
        require_next_statement_sees_a_fresh_snapshot(connection,
                                                     writer,
                                                     "ALTER TABLE IF EXISTS leakdb.nosuch DROP COLUMN id;");
    }
}

TEST_CASE("integration::cpp::empty_statement_transaction::without_if_exists_each_statement_errors") {
    auto config =
        test_create_config(integration_fixture_path("empty_statement_transaction_errors") / std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id bigint);");

    struct refusal_t {
        std::string sql;
        core::error_code_t code;
        std::string what;
    };
    const refusal_t refusals[] = {
        {"CREATE DATABASE leakdb;", core::error_code_t::database_already_exists, "database already exists"},
        {"CREATE TABLE leakdb.t (id bigint);", core::error_code_t::table_already_exists, "collection already exists"},
        {"DROP DATABASE nosuchdb;", core::error_code_t::database_not_exists, "database \"nosuchdb\" does not exist"},
        {"DROP TABLE leakdb.nosuch;",
         core::error_code_t::table_not_exists,
         "relation \"leakdb.nosuch\" does not exist"},
        {"DROP TABLE nosuchdb.nosuch;", core::error_code_t::database_not_exists, "database does not exist"},
        {"DROP TYPE nosuch_type;", core::error_code_t::type_not_exists, "type \"nosuch_type\" does not exist"},
        {"DROP SEQUENCE leakdb.nosuch;",
         core::error_code_t::table_not_exists,
         "relation \"leakdb.nosuch\" does not exist"},
        {"DROP FUNCTION leakdb.nosuch();",
         core::error_code_t::table_not_exists,
         "relation \"leakdb.nosuch\" does not exist"},
        {"DROP VIEW leakdb.nosuch;", core::error_code_t::table_not_exists, "relation \"leakdb.nosuch\" does not exist"},
        {"DROP MATERIALIZED VIEW leakdb.nosuch;",
         core::error_code_t::table_not_exists,
         "relation \"leakdb.nosuch\" does not exist"},
        {"DROP INDEX leakdb.t.nosuch;",
         core::error_code_t::index_not_exists,
         "DROP INDEX: index leakdb.t.nosuch does not exist"},
        {"ALTER TABLE leakdb.nosuch ADD CONSTRAINT c UNIQUE (id);",
         core::error_code_t::table_not_exists,
         "relation \"leakdb.nosuch\" does not exist"},
        {"ALTER TABLE leakdb.t DROP COLUMN nosuch;",
         core::error_code_t::schema_error,
         "column \"nosuch\" of relation \"t\" does not exist; use DROP COLUMN IF EXISTS to ignore it"},
        {"ALTER TABLE leakdb.t DROP CONSTRAINT nosuch;",
         core::error_code_t::invalid_constraint,
         "constraint \"nosuch\" of relation \"t\" does not exist"},
        {"ALTER TABLE leakdb.nosuch ADD COLUMN x bigint;",
         core::error_code_t::table_not_exists,
         "ALTER TABLE: relation \"leakdb.nosuch\" does not exist"},
        {"ALTER TABLE leakdb.nosuch RENAME COLUMN id TO x;",
         core::error_code_t::table_not_exists,
         "ALTER TABLE: relation \"leakdb.nosuch\" does not exist"},
        {"ALTER TABLE leakdb.nosuch DROP COLUMN id;",
         core::error_code_t::table_not_exists,
         "ALTER TABLE: relation \"leakdb.nosuch\" does not exist"},
    };
    for (const auto& refusal : refusals) {
        auto cursor = connection.execute(refusal.sql);
        INFO("statement: " << refusal.sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == refusal.code);
        CHECK(std::string(cursor->get_error().what) == refusal.what);
    }
}

TEST_CASE("integration::cpp::empty_statement_transaction::a_missing_table_is_decided_before_its_columns") {
    auto config = test_create_config(integration_fixture_path("empty_statement_transaction_alter_order") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");

    require_ok(connection, "ALTER TABLE IF EXISTS leakdb.nosuch ADD COLUMN c nosuchtype;");
    auto refused = connection.execute("ALTER TABLE leakdb.nosuch ADD COLUMN c nosuchtype;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::table_not_exists);
    CHECK(std::string(refused->get_error().what) == "ALTER TABLE: relation \"leakdb.nosuch\" does not exist");
}

TEST_CASE("integration::cpp::empty_statement_transaction::an_existing_table_is_decided_before_its_columns") {
    auto config = test_create_config(integration_fixture_path("empty_statement_transaction_create_order") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id bigint);");

    require_ok(connection, "CREATE TABLE IF NOT EXISTS leakdb.t (a int, a int);");
    auto refused = connection.execute("CREATE TABLE leakdb.t (a int, a int);");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::duplicate_field);
    CHECK(std::string(refused->get_error().what) == "column \"a\" specified more than once");
}

TEST_CASE("integration::cpp::empty_statement_transaction::a_name_the_catalog_has_no_place_for_is_refused") {
    auto config = test_create_config(integration_fixture_path("empty_statement_transaction_external") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id bigint);");

    for (const std::string sql : {"DROP VIEW leakdb.s.v;",
                                  "DROP VIEW IF EXISTS leakdb.s.v;",
                                  "DROP MATERIALIZED VIEW leakdb.s.mv;",
                                  "DROP MATERIALIZED VIEW IF EXISTS leakdb.s.mv;",
                                  "DROP TABLE IF EXISTS leakdb.s.t;",
                                  "DROP TABLE IF EXISTS u.leakdb.s.t;",
                                  "ALTER TABLE IF EXISTS leakdb.s.t ADD COLUMN x BIGINT;",
                                  "CREATE TABLE IF NOT EXISTS leakdb.s.t (a BIGINT);"}) {
        auto refused = connection.execute(sql);
        INFO("statement: " << sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string(refused->get_error().what).find("names a uid or schema segment") != std::string::npos);
    }
    require_ok(connection, "SELECT id FROM leakdb.t;");
}

TEST_CASE("integration::cpp::empty_statement_transaction::a_built_in_type_cannot_be_dropped") {
    auto config = test_create_config(integration_fixture_path("empty_statement_transaction_builtin_type") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);

    for (const std::string sql : {"DROP TYPE integer;", "DROP TYPE IF EXISTS integer;"}) {
        auto refused = connection.execute(sql);
        INFO("statement: " << sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::dependent_objects_still_exist);
        CHECK(std::string(refused->get_error().what) ==
              "cannot drop type integer because it is required by the database system");
    }
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id integer);");
}

TEST_CASE("integration::cpp::empty_statement_transaction::restrict_refuses_a_drop_with_dependents") {
    auto config = test_create_config(integration_fixture_path("empty_statement_transaction_restrict") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id bigint);");
    require_ok(connection, "CREATE VIEW leakdb.v AS SELECT id FROM leakdb.t;");

    auto refused = connection.execute("DROP TABLE leakdb.t;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::dependent_objects_still_exist);
    CHECK(
        std::string(refused->get_error().what).find("cannot drop table leakdb.t because other objects depend on it") !=
        std::string::npos);
}

TEST_CASE("integration::cpp::empty_statement_transaction::next_statement_sees_a_fresh_snapshot") {
    auto config = test_create_config(integration_fixture_path("empty_statement_transaction_snapshot") /
                                     std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t reader(instance);
    otterbrix::connection_t writer(instance);
    require_ok(writer, "CREATE DATABASE leakdb;");
    require_ok(writer, "CREATE TABLE leakdb.t (id bigint);");

    require_ok(reader, "CREATE TABLE IF NOT EXISTS leakdb.t (id bigint);");
    require_ok(writer, "INSERT INTO leakdb.t (id) VALUES (1);");
    REQUIRE(visible_rows(reader) == 1);
}

TEST_CASE("integration::cpp::empty_statement_transaction::explicit_block_keeps_its_work") {
    auto config =
        test_create_config(integration_fixture_path("empty_statement_transaction_block") / std::to_string(::getpid()));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    auto instance = test_make_otterbrix(config);
    otterbrix::connection_t connection(instance);
    otterbrix::connection_t observer(instance);
    require_ok(connection, "CREATE DATABASE leakdb;");
    require_ok(connection, "CREATE TABLE leakdb.t (id bigint);");

    SECTION("COMMIT keeps the block's work") {
        require_ok(connection, "BEGIN;");
        require_ok(connection, "CREATE TABLE IF NOT EXISTS leakdb.t (id bigint);");
        require_ok(connection, "INSERT INTO leakdb.t (id) VALUES (1);");
        require_ok(connection, "DROP TABLE IF EXISTS leakdb.nosuch;");
        REQUIRE(visible_rows(observer) == 0);
        require_ok(connection, "COMMIT;");
        REQUIRE(visible_rows(observer) == 1);
        REQUIRE(visible_rows(connection) == 1);
    }
    SECTION("ROLLBACK discards the block's work") {
        require_ok(connection, "BEGIN;");
        require_ok(connection, "DROP TABLE IF EXISTS leakdb.nosuch;");
        require_ok(connection, "INSERT INTO leakdb.t (id) VALUES (1);");
        require_ok(connection, "ROLLBACK;");
        REQUIRE(visible_rows(observer) == 0);
        REQUIRE(visible_rows(connection) == 0);
        require_ok(connection, "SELECT 1;");
    }
}
