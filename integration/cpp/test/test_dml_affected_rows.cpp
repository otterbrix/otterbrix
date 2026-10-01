// A write reports the rows it changed as the cursor's affected_rows(), not as rows of the result: without
// RETURNING the result has no rows at all; with RETURNING it has the returned rows and the count.

#define CATCH_CONFIG_ENABLE_OPTIONAL_STRINGMAKER
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
        }

        components::cursor::cursor_t_ptr run(const std::string& sql) {
            auto cursor = space->dispatcher()->execute_sql(session, sql);
            INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
            REQUIRE(cursor->is_success());
            return cursor;
        }

        configuration::config config;
        std::optional<test_spaces> space;
        otterbrix::session_id_t session;
    };

    void check_written(const components::cursor::cursor_t_ptr& cursor, std::uint64_t rows) {
        CHECK(cursor->affected_rows() == std::optional<std::uint64_t>{rows});
        CHECK(cursor->size() == 0);
        CHECK(cursor->column_count() == 0);
    }

} // namespace

TEST_CASE("integration::cpp::dml_affected_rows::plain_writes") {
    fixture_t db("test_dml_affected_rows/plain");
    db.run("CREATE DATABASE ar;");
    db.run("CREATE TABLE ar.t (id BIGINT, v BIGINT);");

    check_written(db.run("INSERT INTO ar.t (id, v) VALUES (1, 10), (2, 20), (3, 30);"), 3);
    check_written(db.run("UPDATE ar.t SET v = v + 1 WHERE id >= 2;"), 2);
    check_written(db.run("DELETE FROM ar.t WHERE id = 1;"), 1);

    SECTION("a write that matched nothing reports 0, not nothing") {
        check_written(db.run("UPDATE ar.t SET v = 0 WHERE id = 99;"), 0);
        check_written(db.run("DELETE FROM ar.t WHERE id = 99;"), 0);
    }
    SECTION("INSERT ... SELECT") {
        db.run("CREATE TABLE ar.copy (id BIGINT, v BIGINT);");
        check_written(db.run("INSERT INTO ar.copy (id, v) SELECT id, v FROM ar.t;"), 2);
    }
    SECTION("more rows than one batch") {
        std::string values;
        for (int i = 0; i < 2500; ++i) {
            values += (i ? ", (" : "(") + std::to_string(100 + i) + ", 0)";
        }
        check_written(db.run("INSERT INTO ar.t (id, v) VALUES " + values + ";"), 2500);
        check_written(db.run("DELETE FROM ar.t WHERE id >= 100;"), 2500);
    }
}

TEST_CASE("integration::cpp::dml_affected_rows::returning_gives_rows_and_count") {
    fixture_t db("test_dml_affected_rows/returning");
    db.run("CREATE DATABASE ar;");
    db.run("CREATE TABLE ar.t (id BIGINT, v BIGINT);");

    auto inserted = db.run("INSERT INTO ar.t (id, v) VALUES (1, 10), (2, 20) RETURNING id;");
    CHECK(inserted->affected_rows() == std::optional<std::uint64_t>{2});
    CHECK(inserted->size() == 2);

    auto updated = db.run("UPDATE ar.t SET v = 0 WHERE id = 2 RETURNING v;");
    CHECK(updated->affected_rows() == std::optional<std::uint64_t>{1});
    REQUIRE(updated->size() == 1);
    CHECK(updated->value(0, 0).value<int64_t>() == 0);

    auto deleted = db.run("DELETE FROM ar.t WHERE id = 99 RETURNING id;");
    CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{0});
    CHECK(deleted->size() == 0);
}

TEST_CASE("integration::cpp::dml_affected_rows::writes_under_constraints") {
    fixture_t db("test_dml_affected_rows/constraints");
    db.run("CREATE DATABASE ar;");
    db.run("CREATE TABLE ar.parent (id BIGINT PRIMARY KEY, v BIGINT);");
    db.run("CREATE TABLE ar.child (id BIGINT, parent_id BIGINT);");
    db.run("ALTER TABLE ar.parent ADD CONSTRAINT v_positive CHECK (v > 0);");
    db.run("ALTER TABLE ar.child ADD CONSTRAINT fk_parent "
           "FOREIGN KEY (parent_id) REFERENCES ar.parent (id) ON DELETE CASCADE;");

    check_written(db.run("INSERT INTO ar.parent (id, v) VALUES (1, 1), (2, 2), (3, 3);"), 3);
    check_written(db.run("INSERT INTO ar.child (id, parent_id) VALUES (10, 1), (11, 1), (12, 2);"), 3);
    check_written(db.run("UPDATE ar.parent SET v = 5 WHERE id <= 2;"), 2);
    check_written(db.run("DELETE FROM ar.parent WHERE id = 1;"), 1);

    auto children = db.run("SELECT id FROM ar.child;");
    CHECK(children->size() == 1);

    SECTION("RETURNING under a cascading FK returns the projection, not the matched parent rows") {
        auto deleted = db.run("DELETE FROM ar.parent WHERE id = 2 RETURNING id;");
        CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{1});
        REQUIRE(deleted->size() == 1);
        CHECK(deleted->column_count() == 1);
        CHECK(deleted->value(0, 0).value<int64_t>() == 2);
    }
}

TEST_CASE("integration::cpp::dml_affected_rows::dynamic_schema_insert") {
    fixture_t db("test_dml_affected_rows/dynamic");
    db.run("CREATE DATABASE ar;");
    db.run("CREATE TABLE ar.d ();");
    check_written(db.run("INSERT INTO ar.d (id, name) VALUES (1, 'a'), (2, 'b');"), 2);
}

TEST_CASE("integration::cpp::dml_affected_rows::statements_that_write_no_rows") {
    fixture_t db("test_dml_affected_rows/no_rows");
    CHECK_FALSE(db.run("CREATE DATABASE ar;")->affected_rows().has_value());
    CHECK_FALSE(db.run("CREATE TABLE ar.t (id BIGINT);")->affected_rows().has_value());
    db.run("INSERT INTO ar.t (id) VALUES (1), (2);");
    auto selected = db.run("SELECT id FROM ar.t;");
    CHECK_FALSE(selected->affected_rows().has_value());
    CHECK(selected->size() == 2);
    CHECK_FALSE(db.run("BEGIN;")->affected_rows().has_value());
    check_written(db.run("DELETE FROM ar.t WHERE id = 1;"), 1);
    CHECK_FALSE(db.run("COMMIT;")->affected_rows().has_value());
}
