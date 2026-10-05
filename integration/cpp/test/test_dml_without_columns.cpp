// UPDATE / DELETE over rows that have no columns: a table whose last column was dropped still has its rows, and
// a scan hands them over as batches without columns. The write operators count those rows by the batch, never by
// a column.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>

namespace {

    struct fixture_t {
        explicit fixture_t(const char* dir)
            : config(test_create_config(integration_fixture_path(dir))) {
            test_clear_directory(config);
            space.emplace(config);
            ok("CREATE DATABASE zdb;");
        }

        components::cursor::cursor_t_ptr run(const std::string& sql) {
            return space->dispatcher()->execute_sql(session, sql);
        }

        components::cursor::cursor_t_ptr ok(const std::string& sql) {
            auto cursor = run(sql);
            INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
            REQUIRE(cursor->is_success());
            return cursor;
        }

        configuration::config config;
        std::optional<test_spaces> space;
        otterbrix::session_id_t session;
    };

} // namespace

TEST_CASE("integration::cpp::dml_without_columns::delete_rows_whose_columns_are_gone") {
    fixture_t f("test_dml_without_columns/dropped_last_column");
    f.ok("CREATE TABLE zdb.t (x BIGINT);");
    f.ok("INSERT INTO zdb.t (x) VALUES (1), (2), (3);");
    f.ok("ALTER TABLE zdb.t DROP COLUMN x;");

    auto counted = f.ok("SELECT count(*) AS c FROM zdb.t;");
    REQUIRE(counted->size() == 1);
    CHECK(counted->value(0, 0).value<int64_t>() == 3);

    auto deleted = f.ok("DELETE FROM zdb.t;");
    CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{3});

    auto left = f.ok("SELECT count(*) AS c FROM zdb.t;");
    REQUIRE(left->size() == 1);
    CHECK(left->value(0, 0).value<int64_t>() == 0);
}

// A table created without columns has no rows before its first INSERT: DELETE removes nothing, and UPDATE has no
// column to set.
TEST_CASE("integration::cpp::dml_without_columns::a_table_created_without_columns_before_its_first_insert") {
    fixture_t f("test_dml_without_columns/created_without_columns");
    f.ok("CREATE TABLE zdb.t ();");

    auto deleted = f.ok("DELETE FROM zdb.t;");
    CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{0});

    auto updated = f.run("UPDATE zdb.t SET a = 1;");
    CHECK(updated->is_error());
}
