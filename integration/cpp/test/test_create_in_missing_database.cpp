// A CREATE into a database that does not exist is refused and writes nothing: no pg_class row without a namespace
// is left behind to collide with the same name in another database.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>

namespace {

    struct fixture_t {
        explicit fixture_t(const char* dir)
            : config(test_create_config(integration_fixture_path(dir))) {
            test_clear_directory(config);
            space.emplace(config);
        }

        components::cursor::cursor_t_ptr run(const std::string& sql) {
            return space->dispatcher()->execute_sql(otterbrix::session_id_t(), sql);
        }

        void ok(const std::string& sql) {
            auto cursor = run(sql);
            INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
            REQUIRE(cursor->is_success());
        }

        std::size_t rows(const std::string& sql) {
            auto cursor = run(sql);
            INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
            REQUIRE(cursor->is_success());
            return cursor->size();
        }

        configuration::config config;
        std::optional<test_spaces> space;
    };

    void check_refused(fixture_t& db, const std::string& sql) {
        auto cursor = db.run(sql);
        INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
        CHECK(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::database_not_exists);
        CHECK(std::string{cursor->get_error().what} == "database \"nodb\" does not exist");
    }

    const char* orphans = "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 0;";

} // namespace

TEST_CASE("integration::cpp::create_in_missing_database::a_table_is_refused_and_writes_nothing") {
    fixture_t db("test_create_in_missing_database/table");
    for (const char* sql : {"CREATE TABLE nodb.t (id INT);",
                            "CREATE TABLE nodb.t ();",
                            "CREATE TABLE IF NOT EXISTS nodb.t (id INT);",
                            "CREATE TABLE IF NOT EXISTS nodb.t ();"}) {
        check_refused(db, sql);
    }
    CHECK(db.rows(orphans) == 0);
    CHECK(db.rows("SELECT relname FROM pg_catalog.pg_class WHERE relname = 't';") == 0);

    db.ok("CREATE DATABASE otherdb;");
    db.ok("CREATE TABLE otherdb.t (id INT);");
    db.ok("INSERT INTO otherdb.t (id) VALUES (1);");
    CHECK(db.rows("SELECT id FROM otherdb.t;") == 1);
}

TEST_CASE("integration::cpp::create_in_missing_database::other_objects_are_refused_and_write_nothing") {
    fixture_t db("test_create_in_missing_database/other");
    for (const char* sql : {"CREATE SEQUENCE nodb.s;",
                            "CREATE VIEW nodb.v AS SELECT 1 AS a;",
                            "CREATE FUNCTION nodb.f(x INT) RETURNS INT AS 'x -> x * 2';",
                            "CREATE MATERIALIZED VIEW nodb.mv AS SELECT 1 AS a WITH NO DATA;",
                            "CREATE INDEX i ON nodb.t (id);"}) {
        check_refused(db, sql);
    }
    CHECK(db.rows(orphans) == 0);
}
