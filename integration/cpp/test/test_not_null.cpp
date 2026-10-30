// NOT NULL is the storage's: the disk refuses a NULL in a NOT NULL column when it writes the row (PostgreSQL 18's
// 23502, "null value in column ... violates not-null constraint"), whatever statement made the row. A PRIMARY KEY
// column is NOT NULL, as in PostgreSQL.

#include "session_sql_fixture.hpp"

#include <string>
#include <vector>

using code = core::error_code_t;
using ints = std::vector<std::int64_t>;
using strings = std::vector<std::string>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    std::string null_in(const std::string& column, const std::string& table) {
        return "null value in column \"" + column + "\" of relation \"" + table + "\" violates not-null constraint";
    }

    strings not_null_columns(session_sql::fixture_t& db, const std::string& table) {
        return db.strings(any(),
                          "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c "
                          "ON a.attrelid = c.oid WHERE c.relname = '" +
                              table + "' AND a.attnotnull = true ORDER BY a.attnum;");
    }

} // namespace

TEST_CASE("integration::cpp::not_null::every_write_of_a_null_is_refused_by_the_storage") {
    session_sql::fixture_t db("test_not_null/writes");
    db.ok(any(), "CREATE SCHEMA s;");
    db.ok(any(), "CREATE TABLE s.t (id BIGINT NOT NULL, v BIGINT);");
    db.ok(any(), "CREATE TABLE s.src (id BIGINT, v BIGINT);");
    db.ok(any(), "INSERT INTO s.t (id, v) VALUES (1, 10);");
    db.ok(any(), "INSERT INTO s.src (id, v) VALUES (NULL, 20);");
    for (const std::string& sql : {"INSERT INTO s.t (id, v) VALUES (NULL, 2);",
                                   "INSERT INTO s.t (v) VALUES (3);",
                                   "INSERT INTO s.t (id, v) SELECT id, v FROM s.src;",
                                   "UPDATE s.t SET id = NULL WHERE v = 10;"}) {
        db.check_refused(any(), sql, code::not_null_violation, null_in("id", "t"));
    }
    CHECK(db.ints(any(), "SELECT id FROM s.t;") == ints{1});
}

TEST_CASE("integration::cpp::not_null::a_primary_key_column_is_not_null") {
    session_sql::fixture_t db("test_not_null/primary_key");
    db.ok(any(), "CREATE SCHEMA s;");
    db.ok(any(), "CREATE TABLE s.p (id BIGINT PRIMARY KEY, v BIGINT);");
    CHECK(not_null_columns(db, "p") == strings{"id"});
    db.check_refused(any(), "INSERT INTO s.p (id, v) VALUES (NULL, 1);", code::not_null_violation, null_in("id", "p"));
    db.check_refused(any(), "INSERT INTO s.p (v) VALUES (1);", code::not_null_violation, null_in("id", "p"));

    db.ok(any(), "CREATE TABLE s.q (id BIGINT, v BIGINT);");
    db.ok(any(), "INSERT INTO s.q (id, v) VALUES (1, 1);");
    db.ok(any(), "ALTER TABLE s.q ADD CONSTRAINT q_pk PRIMARY KEY (id);");
    CHECK(not_null_columns(db, "q") == strings{"id"});
    db.check_refused(any(), "INSERT INTO s.q (id, v) VALUES (NULL, 2);", code::not_null_violation, null_in("id", "q"));
    db.check_refused(any(), "UPDATE s.q SET id = NULL;", code::not_null_violation, null_in("id", "q"));

    db.restart();
    CHECK(not_null_columns(db, "q") == strings{"id"});
    db.check_refused(any(), "INSERT INTO s.q (id, v) VALUES (NULL, 3);", code::not_null_violation, null_in("id", "q"));
}

// PostgreSQL 18 refuses a PRIMARY KEY over a column that holds a NULL, and a ROLLBACK takes the NOT NULL back.
TEST_CASE("integration::cpp::not_null::a_primary_key_over_a_null_and_its_rollback") {
    session_sql::fixture_t db("test_not_null/primary_key_null");
    db.ok(any(), "CREATE SCHEMA s;");
    db.ok(any(), "CREATE TABLE s.r (id BIGINT, v BIGINT);");
    db.ok(any(), "INSERT INTO s.r (id, v) VALUES (NULL, 1);");
    db.check_refused(any(),
                     "ALTER TABLE s.r ADD CONSTRAINT r_pk PRIMARY KEY (id);",
                     code::not_null_violation,
                     "column \"id\" of relation \"r\" contains null values");
    CHECK(not_null_columns(db, "r").empty());

    db.ok(any(), "DELETE FROM s.r;");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "BEGIN;");
    db.ok(session, "ALTER TABLE s.r ADD CONSTRAINT r_pk PRIMARY KEY (id);");
    db.check_refused(session,
                     "INSERT INTO s.r (id, v) VALUES (NULL, 2);",
                     code::not_null_violation,
                     null_in("id", "r"));
    db.ok(session, "ROLLBACK;");
    CHECK(not_null_columns(db, "r").empty());
    db.ok(any(), "INSERT INTO s.r (id, v) VALUES (NULL, 3);");
}

// A crash before the first checkpoint after the DDL: the storage takes NOT NULL from pg_attribute when it comes up,
// for a table created NOT NULL and for a PRIMARY KEY added to a checkpointed table.
TEST_CASE("integration::cpp::not_null::the_storage_learns_not_null_from_the_catalog_after_a_crash") {
    auto config = test_create_config(integration_fixture_path("test_not_null/crash_src"));
    test_clear_directory(config);
    const auto crash_dir = integration_fixture_path("test_not_null/crash_image");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(test_helpers::ok(d, "CREATE SCHEMA s;"));
        REQUIRE(test_helpers::ok(d, "CREATE TABLE s.q (id BIGINT, v BIGINT);"));
        REQUIRE(test_helpers::ok(d, "INSERT INTO s.q (id, v) VALUES (1, 1);"));
        REQUIRE(test_helpers::ok(d, "CHECKPOINT;"));
        REQUIRE(test_helpers::ok(d, "CREATE TABLE s.t (id BIGINT NOT NULL, v BIGINT);"));
        REQUIRE(test_helpers::ok(d, "ALTER TABLE s.q ADD CONSTRAINT q_pk PRIMARY KEY (id);"));
        test_helpers::copy_crash_image(config.main_path, crash_dir);
    }
    auto crash_config = test_create_config(crash_dir);
    test_spaces space(crash_config);
    auto* d = space.dispatcher();
    const std::pair<std::string, std::string> writes[] = {{"INSERT INTO s.t (id, v) VALUES (NULL, 2);", "t"},
                                                          {"INSERT INTO s.q (id, v) VALUES (NULL, 2);", "q"}};
    for (const auto& [sql, table] : writes) {
        INFO(table);
        INFO(sql);
        auto refused = test_helpers::exec(d, sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == code::not_null_violation);
        // The relation's name a reload of a never-checkpointed file has is "temp" (an old bug, reported apart).
        CHECK(std::string{refused->get_error().what}.rfind("null value in column \"id\" of relation ", 0) == 0);
    }
}

// A refused write message leaves none of its rows: ROLLBACK TO a savepoint before it finds the table as it was, for
// an INSERT and for an UPDATE whose NULL is in the last of several chunks.
TEST_CASE("integration::cpp::not_null::a_refused_write_of_several_chunks_leaves_no_row") {
    session_sql::fixture_t db("test_not_null/several_chunks");
    constexpr int rows = 5000;
    db.ok(any(), "CREATE SCHEMA s;");
    db.ok(any(), "CREATE TABLE s.src (id BIGINT, v BIGINT);");
    db.ok(any(), "CREATE TABLE s.t (id BIGINT NOT NULL, v BIGINT);");
    std::string values;
    for (int row = 1; row <= rows; ++row) {
        values += (row == 1 ? "" : ", ");
        values += "(" + (row == rows ? std::string{"NULL"} : std::to_string(row)) + ", " + std::to_string(row) + ")";
    }
    db.ok(any(), "INSERT INTO s.src (id, v) VALUES " + values + ";");

    const auto session = otterbrix::session_id_t();
    db.ok(session, "BEGIN;");
    db.ok(session, "SAVEPOINT a;");
    db.check_refused(session,
                     "INSERT INTO s.t (id, v) SELECT id, v FROM s.src;",
                     code::not_null_violation,
                     null_in("id", "t"));
    db.ok(session, "ROLLBACK TO SAVEPOINT a;");
    CHECK(db.ints(session, "SELECT count(*) FROM s.t;") == ints{0});
    db.ok(session, "ROLLBACK;");

    db.ok(any(), "INSERT INTO s.t (id, v) SELECT v, v FROM s.src;");
    db.ok(session, "BEGIN;");
    db.ok(session, "SAVEPOINT a;");
    db.check_refused(session,
                     "UPDATE s.t SET id = CASE WHEN v = " + std::to_string(rows) + " THEN NULL ELSE id + 1 END;",
                     code::not_null_violation,
                     null_in("id", "t"));
    db.ok(session, "ROLLBACK TO SAVEPOINT a;");
    CHECK(db.ints(session, "SELECT count(*) FROM s.t;") == ints{rows});
    CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE id = v;") == ints{rows});
    db.ok(session, "COMMIT;");
    CHECK(db.ints(any(), "SELECT count(*) FROM s.t;") == ints{rows});
}
