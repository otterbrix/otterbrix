#include "test_config.hpp"
#include <catch2/catch.hpp>

// Tests for computed_schema (dynamic per-type columnar storage)
// A computed-schema table is created with CREATE TABLE db.t() — no fixed columns.
// Each INSERT can bring new (field_name, type) pairs; each becomes a separate physical column.
// Subsequent INSERTs with the same field_name but different type create additional columns.

static const database_name_t cs_db = "cs_testdb";

TEST_CASE("integration::cpp::test_computed_schema::basic_insert_and_select") {
    auto config = test_create_config("/tmp/test_computed_schema/basic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t1 ();");
        REQUIRE(cur->is_success());
    }

    // First INSERT: introduces id (bigint) and name (string)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // SELECT * should return 2 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }

    // Second INSERT: same schema
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t1 (id, name) VALUES (3, 'Charlie');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // SELECT * should now return 3 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::evolving_schema") {
    auto config = test_create_config("/tmp/test_computed_schema/evolving");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t2 ();");
        REQUIRE(cur->is_success());
    }

    // INSERT with only 'id' column
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t2 (id) VALUES (1), (2), (3);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    // INSERT with 'id' and 'value' — 'value' is a new column
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t2 (id, value) VALUES (4, 100);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // All 4 rows should be returned; rows 1-3 have NULL for 'value'
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }

    // WHERE on 'value' should find only row 4
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t2 WHERE value = 100;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::delete_rows") {
    auto config = test_create_config("/tmp/test_computed_schema/delete");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t3 ();");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t3 (id, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM cs_testdb.t3 WHERE id <= 2;");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::multi_type_field") {
    auto config = test_create_config("/tmp/test_computed_schema/multi_type");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t4 ();");
        REQUIRE(cur->is_success());
    }

    // Insert 'id' as bigint
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t4 (id, val) VALUES (1, 1), (2, 2);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // Insert 'id' as string — creates a second physical column id__STRING_LITERAL
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t4 (id, val) VALUES (3, 'hello');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // SELECT * must fail: 'id' has two physical types (bigint and string)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t4;");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::schema_error);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT t4.* FROM cs_testdb.t4;");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::schema_error);
    }

    // Select val as string: rows 1-2 have NULL for val (no string was inserted), row 3 has "hello"
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT id, val::string FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).is_null());
        REQUIRE(cur->chunk_data().value(1, 1).is_null());
        { auto v = cur->chunk_data().value(1, 2); REQUIRE(v.value<const std::string&>() == "hello"); }
    }

    // Select val as bigint: rows 1-2 have (1,2), row 3 has NULL for val::bigint (no bigint was inserted)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT id, val::bigint FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 2);
        REQUIRE(cur->chunk_data().value(1, 2).is_null());
    }

    // WHERE val::bigint
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT id, val::bigint FROM cs_testdb.t4 WHERE val::bigint > 0 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 2);
    }

    // TODO do correct error
    // SELECT val (no cast) — val has 2 types, which is error
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT val FROM cs_testdb.t4;");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::field_not_exists);
    }

}
