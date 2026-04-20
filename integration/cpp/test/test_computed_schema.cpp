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
        REQUIRE(cur->get_error().what == "column 'val' has multiple types; use explicit column selection");
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
}

// ---- Sparse schema tests ----
// Tables created with WITH (sparse_threshold=N): columns start in separate sparse tables.
// SELECT returns main columns + sparse columns merged via _id lookup.

TEST_CASE("integration::cpp::test_computed_schema::sparse_basic_insert_select") {
    auto config = test_create_config("/tmp/test_computed_schema/sparse_basic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    // Create table with high threshold so all columns stay sparse
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "CREATE TABLE cs_testdb.sp1 () WITH (sparse_threshold=1000);");
        REQUIRE(cur->is_success());
    }

    // INSERT 3 rows: each gets an auto-assigned _id
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.sp1 (name) VALUES ('Alice'), ('Bob'), ('Charlie');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    // SELECT * returns _id (main) + name (sparse, post-processed)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.sp1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        // Result has _id column (from main table) + name (from sparse table)
        REQUIRE(cur->chunk_data().column_count() == 2);
        // All name values are non-null
        REQUIRE_FALSE(cur->chunk_data().value(1, 0).is_null());
        REQUIRE_FALSE(cur->chunk_data().value(1, 1).is_null());
        REQUIRE_FALSE(cur->chunk_data().value(1, 2).is_null());
    }

    // Second INSERT adds 2 more rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.sp1 (name) VALUES ('Dave'), ('Eve');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // SELECT * now returns 5 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.sp1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::sparse_mixed_nulls") {
    auto config = test_create_config("/tmp/test_computed_schema/sparse_mixed");
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
        auto cur = dispatcher->execute_sql(
            session, "CREATE TABLE cs_testdb.sp2 () WITH (sparse_threshold=1000);");
        REQUIRE(cur->is_success());
    }

    // INSERT rows: only first batch has 'score', second batch only has 'name'
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.sp2 (name, score) VALUES ('Alice', 95), ('Bob', 80);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.sp2 (name) VALUES ('Charlie'), ('Dave');");
    }

    // SELECT * returns 4 rows; columns: _id, name, score
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.sp2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        // _id + name + score = 3 columns
        REQUIRE(cur->chunk_data().column_count() == 3);

        // Charlie and Dave (rows 2,3) have NULL score
        REQUIRE(cur->chunk_data().value(2, 2).is_null());
        REQUIRE(cur->chunk_data().value(2, 3).is_null());
    }
}

TEST_CASE("integration::cpp::test_computed_schema::sparse_zero_threshold_no_effect") {
    // sparse_threshold=0 (default) means no sparse optimization — same as regular computed schema
    auto config = test_create_config("/tmp/test_computed_schema/sparse_zero");
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
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.sp3 ();");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.sp3 (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.sp3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2); // id + name (no _id auto-column)
    }
}

TEST_CASE("integration::cpp::test_computed_schema::sparse_promotion_basic") {
    // sparse_threshold=3: column stays sparse until 3 non-null values, then promoted to main table
    auto config = test_create_config("/tmp/test_computed_schema/sparse_promotion_basic");
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
        auto cur = dispatcher->execute_sql(
            session, "CREATE TABLE cs_testdb.promo1 () WITH (sparse_threshold=3);");
        REQUIRE(cur->is_success());
    }

    // INSERT 2 rows: non_null_count = 2 < threshold=3, stays sparse
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo1 (name) VALUES ('Alice'), ('Bob');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // INSERT 2 more rows: non_null_count becomes 4 >= threshold=3 → promotion triggered
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo1 (name) VALUES ('Charlie'), ('Dave');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // After promotion, SELECT * should return all 4 rows with 'name' in main table
    // Columns: _id (BIGINT) + name (STRING) = 2 columns; name comes from main table (not sparse)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.promo1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->chunk_data().column_count() == 2);
        // All 4 name values should be non-null
        REQUIRE_FALSE(cur->chunk_data().value(1, 0).is_null());
        REQUIRE_FALSE(cur->chunk_data().value(1, 1).is_null());
        REQUIRE_FALSE(cur->chunk_data().value(1, 2).is_null());
        REQUIRE_FALSE(cur->chunk_data().value(1, 3).is_null());
    }

    // INSERT more rows after promotion: should go directly to main table
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo1 (name) VALUES ('Eve'), ('Frank');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.promo1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
        REQUIRE(cur->chunk_data().column_count() == 2);
        // All 6 rows non-null
        for (uint64_t r = 0; r < 6; r++) {
            REQUIRE_FALSE(cur->chunk_data().value(1, r).is_null());
        }
    }
}

TEST_CASE("integration::cpp::test_computed_schema::sparse_promotion_with_nulls") {
    // Threshold=2: after promotion, rows without the column stay NULL in main table
    auto config = test_create_config("/tmp/test_computed_schema/sparse_promotion_nulls");
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
        auto cur = dispatcher->execute_sql(
            session, "CREATE TABLE cs_testdb.promo2 () WITH (sparse_threshold=2);");
        REQUIRE(cur->is_success());
    }

    // Row 0: only 'score'
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo2 (score) VALUES (100);");
        REQUIRE(cur->is_success());
    }

    // Row 1: only 'name'
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo2 (name) VALUES ('Alice');");
        REQUIRE(cur->is_success());
    }

    // Row 2: both 'name' and 'score' → name reaches threshold=2, promotion triggered
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo2 (name, score) VALUES ('Bob', 90);");
        REQUIRE(cur->is_success());
    }

    // SELECT *: 3 rows, 3 columns (_id, name, score)
    // row 0: _id=0, name=NULL, score=100
    // row 1: _id=1, name='Alice', score=NULL
    // row 2: _id=2, name='Bob', score=90
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.promo2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        // _id + name (promoted) + score (still sparse or promoted depending on count)
        // name has 2 non-nulls (Alice + Bob) >= threshold=2 → promoted
        // score has 2 non-nulls (100 + 90) >= threshold=2 → promoted
        REQUIRE(cur->chunk_data().column_count() == 3);

        // Find column indices by checking values
        // Row 0: name should be NULL, score non-null
        // Row 1: name non-null, score NULL
        // Row 2: both non-null
        bool found_null_name_row0 = false;
        bool found_null_score_row1 = false;
        bool found_both_row2 = false;
        for (uint64_t r = 0; r < 3; r++) {
            bool name_null = cur->chunk_data().value(1, r).is_null();
            bool score_null = cur->chunk_data().value(2, r).is_null();
            if (name_null && !score_null) found_null_name_row0 = true;
            if (!name_null && score_null) found_null_score_row1 = true;
            if (!name_null && !score_null) found_both_row2 = true;
        }
        REQUIRE(found_null_name_row0);
        REQUIRE(found_null_score_row1);
        REQUIRE(found_both_row2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::sparse_promotion_then_insert") {
    // After promotion, subsequent INSERTs go directly to main table (no sparse intermediate)
    auto config = test_create_config("/tmp/test_computed_schema/sparse_promotion_then_insert");
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
        auto cur = dispatcher->execute_sql(
            session, "CREATE TABLE cs_testdb.promo3 () WITH (sparse_threshold=2);");
        REQUIRE(cur->is_success());
    }

    // 3 batches: first 2 go to sparse (1 non-null each), 3rd triggers promotion
    for (int i = 1; i <= 3; i++) {
        auto session = otterbrix::session_id_t();
        auto name = std::string("'User") + std::to_string(i) + "'";
        auto sql = std::string("INSERT INTO cs_testdb.promo3 (val) VALUES (") + name + ");";
        auto cur = dispatcher->execute_sql(session, sql);
        REQUIRE(cur->is_success());
    }

    // Now 'val' is promoted. Insert 2 more rows post-promotion.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "INSERT INTO cs_testdb.promo3 (val) VALUES ('User4'), ('User5');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // SELECT: 5 rows, 2 columns (_id, val), all non-null
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.promo3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        REQUIRE(cur->chunk_data().column_count() == 2);
        for (uint64_t r = 0; r < 5; r++) {
            REQUIRE_FALSE(cur->chunk_data().value(1, r).is_null());
        }
    }
}

// ---------------------------------------------------------------------------
// INSERT INTO <computed-schema> VALUES (json('...')), (json('...')), ...
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_computed_schema::json_insert_basic") {
    auto config = test_create_config("/tmp/test_computed_schema/json_basic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.jt ();");
        REQUIRE(cur->is_success());
    }

    // Two rows with overlapping + disjoint fields; exercises column union + NULL padding.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "INSERT INTO cs_testdb.jt VALUES "
            "(json('{\"id\": 1, \"name\": \"alice\"}')), "
            "(json('{\"id\": 2, \"city\": \"nyc\"}'));");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // Each field from the union lands as its own physical column.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT * FROM cs_testdb.jt;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // id + name + city = 3 columns (non-sparse path does not add _id)
        REQUIRE(cur->chunk_data().column_count() == 3);
    }

    // Verify concrete values via targeted SELECTs.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT id FROM cs_testdb.jt ORDER BY id ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[1] == 2);
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT name FROM cs_testdb.jt ORDER BY id ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // Row 0: alice; row 1: NULL (absent)
        REQUIRE_FALSE(cur->chunk_data().data[0].validity().row_is_valid(1));
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT city FROM cs_testdb.jt ORDER BY id ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // Row 0: NULL; row 1: nyc
        REQUIRE_FALSE(cur->chunk_data().data[0].validity().row_is_valid(0));
    }
}

TEST_CASE("integration::cpp::test_computed_schema::json_insert_nested") {
    auto config = test_create_config("/tmp/test_computed_schema/json_nested");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.nt ();");
    }
    // Nested object flattens to dotted path as its column name.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "INSERT INTO cs_testdb.nt VALUES (json('{\"user\": {\"profile\": {\"age\": 30}}}'));");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT * FROM cs_testdb.nt;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1); // "user.profile.age"
    }
}

TEST_CASE("integration::cpp::test_computed_schema::json_insert_evolving_across_batches") {
    auto config = test_create_config("/tmp/test_computed_schema/json_evolving");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.ev ();");
    }

    // Batch 1: introduces a, b
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "INSERT INTO cs_testdb.ev VALUES (json('{\"a\": 1, \"b\": 2}'));");
        REQUIRE(cur->is_success());
    }
    // Batch 2: adds c, reuses a
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "INSERT INTO cs_testdb.ev VALUES (json('{\"a\": 10, \"c\": 100}'));");
        REQUIRE(cur->is_success());
    }
    // Batch 3: all three fields at once, batch of 2 rows
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "INSERT INTO cs_testdb.ev VALUES "
            "(json('{\"a\": 20, \"b\": 200, \"c\": 2000}')), "
            "(json('{\"a\": 30, \"b\": 300, \"c\": 3000}'));");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT * FROM cs_testdb.ev;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->chunk_data().column_count() == 3); // a + b + c
    }
    // Sanity: aggregate across all rows
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT SUM(a) FROM cs_testdb.ev;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == (1 + 10 + 20 + 30));
    }
}

TEST_CASE("integration::cpp::test_computed_schema::json_insert_type_mix_types") {
    auto config = test_create_config("/tmp/test_computed_schema/json_types");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.tt ();");
    }
    // Booleans, doubles, strings, ints — each becomes its own column type.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "INSERT INTO cs_testdb.tt VALUES "
            "(json('{\"i\": 7, \"d\": 0.5, \"b\": true, \"s\": \"hi\"}'));");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT * FROM cs_testdb.tt;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 4); // i + d + b + s
    }
}

TEST_CASE("integration::cpp::test_computed_schema::json_arrow_select") {
    auto config = test_create_config("/tmp/test_computed_schema/json_arrow");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.ar ();");
    }
    // Insert nested json so the flattened column name is "hello.word".
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "INSERT INTO cs_testdb.ar VALUES (json('{\"hello\": {\"word\": 42}}'));");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    // Bare -> chain resolves to the flattened column.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT ar -> 'hello' -> 'word' FROM cs_testdb.ar;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 42);
    }
    // -> chain wrapped in ::int cast.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT (ar -> 'hello' -> 'word')::int FROM cs_testdb.ar;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int32_t>()[0] == 42);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::json_arrow_column_base") {
    auto config = test_create_config("/tmp/test_computed_schema/json_arrow_col");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.at ();");
    }
    // Flattened column name is "a.x.y"; leftmost in the -> chain is "a" — a column, not the table.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "INSERT INTO cs_testdb.at VALUES (json('{\"a\": {\"x\": {\"y\": 99}}}'));");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT a -> 'x' -> 'y' FROM cs_testdb.at;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 99);
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT (a -> 'x' -> 'y')::int FROM cs_testdb.at;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int32_t>()[0] == 99);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::json_insert_error_propagation") {
    auto config = test_create_config("/tmp/test_computed_schema/json_errors");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE cs_testdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE TABLE cs_testdb.err ();");
    }
    // Invalid JSON — the transformer throws parser_exception_t, surfacing as an
    // unhandled exception out of execute_sql (existing convention, same as bad SQL).
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_THROWS(
            dispatcher->execute_sql(s, "INSERT INTO cs_testdb.err VALUES (json('{bad json'));"));
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_THROWS(
            dispatcher->execute_sql(s, "INSERT INTO cs_testdb.err VALUES (json('42'));"));
    }
}
