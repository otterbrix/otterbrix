#include "session_sql_fixture.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/vector/vector_builders.hpp>

#include <string>
#include <utility>
#include <vector>

namespace {

    using ints = std::vector<std::int64_t>;
    using strings = std::vector<std::string>;
    using code = core::error_code_t;

} // namespace

TEST_CASE("integration::cpp::insert_default_cells::values") {
    session_sql::fixture_t db("test_insert_default_cells/values");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint, b bigint DEFAULT 7, c text DEFAULT 'x', d bigint);");

    SECTION("a declared default") {
        db.ok(session, "INSERT INTO s.t (a, b, c) VALUES (1, DEFAULT, DEFAULT);");
        CHECK(db.ints(session, "SELECT b FROM s.t;") == ints{7});
        CHECK(db.strings(session, "SELECT c FROM s.t;") == strings{"x"});
    }

    SECTION("no declared default") {
        db.ok(session, "INSERT INTO s.t (a, d) VALUES (1, DEFAULT);");
        CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE d IS NULL;") == ints{1});
    }

    SECTION("among written values") {
        db.ok(session, "INSERT INTO s.t (a, b, c) VALUES (1, DEFAULT, 'y'), (2, 20, DEFAULT), (3, DEFAULT, 'z');");
        CHECK(db.ints(session, "SELECT b FROM s.t ORDER BY a;") == ints{7, 20, 7});
        CHECK(db.strings(session, "SELECT c FROM s.t ORDER BY a;") == strings{"y", "x", "z"});
    }

    SECTION("without a column list") {
        db.ok(session, "INSERT INTO s.t VALUES (1, DEFAULT, DEFAULT, 4);");
        CHECK(db.ints(session, "SELECT b FROM s.t;") == ints{7});
        CHECK(db.strings(session, "SELECT c FROM s.t;") == strings{"x"});
    }

    SECTION("returning") {
        CHECK(db.ints(session, "INSERT INTO s.t (a, b) VALUES (1, DEFAULT) RETURNING b;") == ints{7});
    }

    SECTION("with parameters") {
        std::vector<std::pair<size_t, components::vector::vector_t>> values;
        values.emplace_back(1, components::vector::make_scalar(db.dispatcher()->resource(), std::int64_t{1}));
        values.emplace_back(2, components::vector::make_scalar(db.dispatcher()->resource(), std::int64_t{5}));
        auto inserted =
            db.dispatcher()->execute_sql_with_params(session,
                                                     "INSERT INTO s.t (a, b) VALUES ($1, DEFAULT), (2, $2);",
                                                     values);
        INFO(session_sql::error_of(inserted));
        REQUIRE(inserted->is_success());
        CHECK(db.ints(session, "SELECT b FROM s.t ORDER BY a;") == ints{7, 5});
    }

    SECTION("outside an INSERT") {
        CHECK(db.refused(session, "VALUES (1, DEFAULT);").type == code::sql_parse_error);
        CHECK(db.refused(session, "SELECT * FROM (VALUES (1, DEFAULT)) AS v (a, b);").type == code::sql_parse_error);
    }
}

TEST_CASE("integration::cpp::insert_default_cells::not_null") {
    session_sql::fixture_t db("test_insert_default_cells/not_null");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint, b bigint NOT NULL DEFAULT 7, c bigint NOT NULL);");
    db.ok(session, "INSERT INTO s.t (a, b, c) VALUES (1, DEFAULT, 1);");
    CHECK(db.ints(session, "SELECT b FROM s.t;") == ints{7});
    CHECK(db.refused(session, "INSERT INTO s.t (a, b, c) VALUES (2, 2, DEFAULT);").type == code::not_null_violation);
}

TEST_CASE("integration::cpp::insert_default_cells::default_values") {
    session_sql::fixture_t db("test_insert_default_cells/default_values");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint, b bigint DEFAULT 7, c text DEFAULT 'x');");
    db.ok(session, "CREATE TABLE s.n (a bigint NOT NULL);");

    db.ok(session, "INSERT INTO s.t DEFAULT VALUES;");
    CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE a IS NULL AND b = 7 AND c = 'x';") == ints{1});
    CHECK(db.ints(session, "INSERT INTO s.t DEFAULT VALUES RETURNING b;") == ints{7});
    CHECK(db.ints(session, "SELECT count(*) FROM s.t;") == ints{2});
    CHECK(db.refused(session, "INSERT INTO s.n DEFAULT VALUES;").type == code::not_null_violation);
}

// More rows than a chunk holds: a DEFAULT cell past the first chunk lands in its own row.
TEST_CASE("integration::cpp::insert_default_cells::across_chunks") {
    session_sql::fixture_t db("test_insert_default_cells/across_chunks");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint, b bigint DEFAULT 7);");
    constexpr std::int64_t rows = 1500;
    std::string insert = "INSERT INTO s.t (a, b) VALUES ";
    for (std::int64_t row = 0; row < rows; ++row) {
        insert += row == 0 ? "" : ", ";
        insert += "(" + std::to_string(row) + ", " +
                  (row % 3 == 0 ? std::string{"DEFAULT"} : std::to_string(row + 100)) + ")";
    }
    db.ok(session, insert + ";");
    CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE b = 7;") == ints{rows / 3});
    CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE b = 7 AND a % 3 <> 0;") == ints{0});
    CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE b = a + 100;") == ints{rows - rows / 3});
}
