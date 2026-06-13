#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <set>
#include <string>

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
        auto cur =
            dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
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
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t1 (id, name) VALUES (3, 'Charlie');");
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

// Multi-type field: a computing table keeps several columns with the same name
// but different types. SELECT * returns ALL of them; an explicit reference to the
// ambiguous name errors (type selection is needed — see ::? in a later step).
TEST_CASE("integration::cpp::test_computed_schema::multitype_select_star") {
    auto config = test_create_config("/tmp/test_computed_schema/multitype_star");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.mt ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mt (id, val) VALUES (1, 1), (2, 2);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mt (id, val) VALUES (3, 'hello');")->is_success());

    SECTION("SELECT * returns both 'val' variants with their own values") {
        auto cur = exec("SELECT * FROM cs_testdb.mt ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 3);

        // Locate the two 'val' columns by physical type.
        const auto& chunk = cur->chunk_data();
        int bigint_val = -1, string_val = -1;
        for (size_t c = 0; c < chunk.column_count(); ++c) {
            if (std::string(chunk.data[c].type().alias()) != "val") {
                continue;
            }
            if (chunk.data[c].type().type() == components::types::logical_type::BIGINT) {
                bigint_val = static_cast<int>(c);
            } else {
                string_val = static_cast<int>(c);
            }
        }
        REQUIRE(bigint_val >= 0);
        REQUIRE(string_val >= 0);

        // bigint variant: rows id=1,2 have values; id=3 (string row) is NULL.
        REQUIRE(chunk.value(static_cast<size_t>(bigint_val), 0).value<int64_t>() == 1);
        REQUIRE(chunk.value(static_cast<size_t>(bigint_val), 1).value<int64_t>() == 2);
        REQUIRE(chunk.value(static_cast<size_t>(bigint_val), 2).is_null());
        // string variant: only id=3 has a value.
        REQUIRE(chunk.value(static_cast<size_t>(string_val), 0).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(string_val), 1).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(string_val), 2).value<std::string_view>() == "hello");
    }

    SECTION("an explicit reference to the multi-type name is ambiguous") {
        REQUIRE_FALSE(exec("SELECT val FROM cs_testdb.mt;")->is_success());
    }

    SECTION("an unambiguous column still works") {
        auto cur = exec("SELECT id FROM cs_testdb.mt ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
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

// JSONB scalar navigation (->> and #>>) over a computing table.
// Nested fields are flattened: INSERT (a.b, a.c) creates columns "a/b","a/c".
// A scalar jsonb chain (terminated by ->>/#>>) addresses one flattened column.
TEST_CASE("integration::cpp::test_computed_schema::jsonb_scalar_navigation") {
    auto config = test_create_config("/tmp/test_computed_schema/jsonb_scalar");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.j ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.j (a.b, a.c, x) VALUES (1, 2, 9), (10, 20, 90);")->is_success());

    SECTION("-> ... ->> resolves a nested leaf to its native value") {
        auto cur = exec("SELECT x, j -> 'a' ->> 'b' AS b FROM cs_testdb.j ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 10);
    }

    SECTION("->> on a top-level field") {
        auto cur = exec("SELECT j ->> 'x' AS xx FROM cs_testdb.j ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 9);
        REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 90);
        REQUIRE(std::string(cur->chunk_data().data[0].type().alias()) == "xx");
    }

    SECTION("#>> with dotted path and PG-array path are equivalent") {
        auto dotted = exec("SELECT j #>> 'a.c' AS c FROM cs_testdb.j ORDER BY x;");
        REQUIRE(dotted->is_success());
        REQUIRE(dotted->chunk_data().value(0, 0).value<int64_t>() == 2);
        REQUIRE(dotted->chunk_data().value(0, 1).value<int64_t>() == 20);

        auto arr = exec("SELECT j #>> '{a,c}' AS c FROM cs_testdb.j ORDER BY x;");
        REQUIRE(arr->is_success());
        REQUIRE(arr->chunk_data().value(0, 0).value<int64_t>() == 2);
        REQUIRE(arr->chunk_data().value(0, 1).value<int64_t>() == 20);
    }

    SECTION("jsonb scalar usable in WHERE") {
        auto cur = exec("SELECT x FROM cs_testdb.j WHERE j -> 'a' ->> 'b' = 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 90);

        auto cur2 = exec("SELECT x FROM cs_testdb.j WHERE j #>> '{a,c}' = 2;");
        REQUIRE(cur2->is_success());
        REQUIRE(cur2->size() == 1);
        REQUIRE(cur2->chunk_data().value(0, 0).value<int64_t>() == 9);
    }

    SECTION("a chain still ending in -> resolves to a single leaf column") {
        // j -> 'a' -> 'b' : prefix a/b is a leaf -> one column named 'b'
        auto cur = exec("SELECT j -> 'a' -> 'b' FROM cs_testdb.j ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(std::string(cur->chunk_data().data[0].type().alias()) == "b");
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 10);
    }
}

// JSONB key existence '?' / '?|' / '?&'. Per row, a key "exists" iff its
// flattened column is present and non-null (IS NOT NULL on the joined path).
// Note: a key absent from the table schema currently errors (see plan §0.1),
// so tests use keys that exist in the schema.
TEST_CASE("integration::cpp::test_computed_schema::jsonb_exists") {
    auto config = test_create_config("/tmp/test_computed_schema/jsonb_exists");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.qe ();")->is_success());
    // row0: a/b=1, x=9 ; row1: a/b=5, x=NULL (x absent from this INSERT)
    REQUIRE(exec("INSERT INTO cs_testdb.qe (a.b, x) VALUES (1, 9);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.qe (a.b) VALUES (5);")->is_success());

    SECTION("? matches rows where the key is present and non-null") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe ? 'x';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
    }

    SECTION("?| any-of") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe ?| '{x}';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
    }

    SECTION("?& all-of") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe ?& '{x}';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
    }

    SECTION("existence on a nested path prefix") {
        // both rows have a/b present -> 2 rows
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe -> 'a' ? 'b';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

// JSONB delete '-' / '#-' over a computing table. Returns a table = all columns
// except those under the deleted prefix (column-set exclusion, expanded in SELECT).
TEST_CASE("integration::cpp::test_computed_schema::jsonb_delete") {
    auto config = test_create_config("/tmp/test_computed_schema/jsonb_delete");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto alias_set = [](const auto& cur) {
        std::set<std::string> s;
        for (size_t c = 0; c < cur->chunk_data().column_count(); ++c) {
            s.insert(std::string(cur->chunk_data().data[c].type().alias()));
        }
        return s;
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.jd ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.jd (a.b, a.c, x) VALUES (1, 2, 9), (10, 20, 90);")->is_success());

    SECTION("- 'x' drops a top-level key") {
        auto cur = exec("SELECT jd - 'x' FROM cs_testdb.jd;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(alias_set(cur) == std::set<std::string>{"a/b", "a/c"});
    }

    SECTION("- 'a' drops a whole subtree") {
        auto cur = exec("SELECT jd - 'a' FROM cs_testdb.jd;");
        REQUIRE(cur->is_success());
        REQUIRE(alias_set(cur) == std::set<std::string>{"x"});
    }

    SECTION("#- 'a.b' drops one nested leaf, keeps the sibling") {
        auto cur = exec("SELECT jd #- 'a.b' FROM cs_testdb.jd;");
        REQUIRE(cur->is_success());
        REQUIRE(alias_set(cur) == std::set<std::string>{"a/c", "x"});
    }
}

// JSONB table-valued navigation '->' / '#>' in the SELECT list expands the
// subtree under the prefix into its (rerooted) columns. NB: this is the
// SELECT-list form; `SELECT * FROM t -> 'a'` is intentionally not supported
// (it would require a parser/grammar change).
TEST_CASE("integration::cpp::test_computed_schema::jsonb_expand") {
    auto config = test_create_config("/tmp/test_computed_schema/jsonb_expand");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto alias_set = [](const auto& cur) {
        std::set<std::string> s;
        for (size_t c = 0; c < cur->chunk_data().column_count(); ++c) {
            s.insert(std::string(cur->chunk_data().data[c].type().alias()));
        }
        return s;
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.je ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.je (a.b, a.c, x) VALUES (1, 2, 9);")->is_success());

    SECTION("-> 'a' expands the subtree, rerooted (strips the 'a/' prefix)") {
        auto cur = exec("SELECT je -> 'a' FROM cs_testdb.je;");
        REQUIRE(cur->is_success());
        REQUIRE(alias_set(cur) == std::set<std::string>{"b", "c"}); // a/b -> b, a/c -> c
    }

    SECTION("#> 'a' (single-element path) expands the same way") {
        auto cur = exec("SELECT je #> 'a' FROM cs_testdb.je;");
        REQUIRE(cur->is_success());
        REQUIRE(alias_set(cur) == std::set<std::string>{"b", "c"});
    }

    SECTION("-> to a leaf yields a single rerooted column with the value") {
        auto cur = exec("SELECT je -> 'a' -> 'c' FROM cs_testdb.je;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(std::string(cur->chunk_data().data[0].type().alias()) == "c");
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 2);
    }
}

// Multi-type field: '::?type' selects the same-named column whose physical type
// matches (the '::' cast is left for value conversion). 'val' is inserted as
// bigint then string, producing two physical 'val' columns.
TEST_CASE("integration::cpp::test_computed_schema::multitype_variant_select") {
    auto config = test_create_config("/tmp/test_computed_schema/multitype_variant");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.t4 ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.t4 (id, val) VALUES (1, 1), (2, 2);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.t4 (id, val) VALUES (3, 'hello');")->is_success());

    SECTION("::?string selects the string variant (bigint rows are NULL)") {
        auto cur = exec("SELECT id, val::?string FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).is_null());
        REQUIRE(cur->chunk_data().value(1, 1).is_null());
        REQUIRE(cur->chunk_data().value(1, 2).value<std::string_view>() == "hello");
    }

    SECTION("::?bigint selects the bigint variant (string row is NULL)") {
        auto cur = exec("SELECT id, val::?bigint FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 2);
        REQUIRE(cur->chunk_data().value(1, 2).is_null());
    }

    SECTION("::?type works in WHERE") {
        auto cur = exec("SELECT id, val::?bigint FROM cs_testdb.t4 WHERE val::?bigint > 0 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 2);
    }

    SECTION("an unselected multi-type reference is an error") {
        REQUIRE_FALSE(exec("SELECT val FROM cs_testdb.t4;")->is_success());
    }
}

// The jsonb operators coexist with a multi-type field in the same table: they
// resolve single-type fields normally, drop/expand around the multi-type one,
// and an unselected reference to the multi-type name stays ambiguous.
TEST_CASE("integration::cpp::test_computed_schema::jsonb_operators_with_multitype") {
    auto config = test_create_config("/tmp/test_computed_schema/jsonb_multitype");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto alias_set = [](const auto& cur) {
        std::set<std::string> s;
        for (size_t c = 0; c < cur->chunk_data().column_count(); ++c) {
            s.insert(std::string(cur->chunk_data().data[c].type().alias()));
        }
        return s;
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.m ();")->is_success());
    // x: single-type; v: multi-type (bigint then string); a/b: single-type (only row 1).
    REQUIRE(exec("INSERT INTO cs_testdb.m (x, v, a.b) VALUES (1, 10, 100);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.m (x, v) VALUES (2, 'str');")->is_success());

    SECTION("->> resolves a single-type field next to a multi-type one") {
        auto cur = exec("SELECT m ->> 'x' FROM cs_testdb.m ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 2);
    }

    SECTION("-> ... ->> resolves a nested single-type leaf") {
        auto cur = exec("SELECT m -> 'a' ->> 'b' FROM cs_testdb.m ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 100);
        REQUIRE(cur->chunk_data().value(0, 1).is_null());
    }

    SECTION("'-' drops every variant of the multi-type field") {
        auto cur = exec("SELECT m - 'v' FROM cs_testdb.m;");
        REQUIRE(cur->is_success());
        REQUIRE(alias_set(cur) == std::set<std::string>{"x", "a/b"});
    }

    SECTION("'?' on a present field") {
        auto cur = exec("SELECT x FROM cs_testdb.m WHERE m ? 'x' ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("scalar nav to the multi-type name is ambiguous") {
        REQUIRE_FALSE(exec("SELECT m ->> 'v' FROM cs_testdb.m;")->is_success());
    }

    SECTION("::? still selects the multi-type variant") {
        auto cur = exec("SELECT v::?bigint FROM cs_testdb.m WHERE x = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 10);
    }
}

// Per-operator multi-type semantics: '?' exists if ANY variant is non-null;
// '::?' composes onto a jsonb-nav chain to pick the variant of a nested
// multi-type leaf.
TEST_CASE("integration::cpp::test_computed_schema::jsonb_multitype_semantics") {
    auto config = test_create_config("/tmp/test_computed_schema/jsonb_mt_sem");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.mm ();")->is_success());
    // v and a/b are multi-type (bigint then string); row 3 has neither.
    REQUIRE(exec("INSERT INTO cs_testdb.mm (id, v, a.b) VALUES (1, 10, 100);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mm (id, v, a.b) VALUES (2, 'sv', 'sb');")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mm (id) VALUES (3);")->is_success());

    SECTION("'?' is true if ANY variant is non-null (false only if all null)") {
        auto cur = exec("SELECT id FROM cs_testdb.mm WHERE mm ? 'v' ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2); // rows 1 (bigint) and 2 (string); row 3 excluded
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 2);
    }

    SECTION("nested '?' over a multi-type leaf") {
        auto cur = exec("SELECT id FROM cs_testdb.mm WHERE mm -> 'a' ? 'b' ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("'::?' composes onto a jsonb-nav chain (bigint variant)") {
        auto cur = exec("SELECT id, mm -> 'a' ->> 'b' ::? bigint AS b FROM cs_testdb.mm ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 100); // row1: bigint a/b
        REQUIRE(cur->chunk_data().value(1, 1).is_null());               // row2: string variant
        REQUIRE(cur->chunk_data().value(1, 2).is_null());               // row3: absent
    }

    SECTION("'::?' composes onto a jsonb-nav chain (string variant)") {
        auto cur = exec("SELECT id, mm -> 'a' ->> 'b' ::? string AS b FROM cs_testdb.mm ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(1, 0).is_null());
        REQUIRE(cur->chunk_data().value(1, 1).value<std::string_view>() == "sb");
        REQUIRE(cur->chunk_data().value(1, 2).is_null());
    }

    SECTION("scalar nav to a multi-type leaf without ::? is ambiguous") {
        REQUIRE_FALSE(exec("SELECT mm -> 'a' ->> 'b' FROM cs_testdb.mm;")->is_success());
    }

    SECTION("'::?' over a nav chain works in WHERE") {
        auto cur = exec("SELECT id FROM cs_testdb.mm WHERE mm -> 'a' ->> 'b' ::? bigint > 50 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1); // only row 1 (a/b bigint = 100); row 2 is string, row 3 absent
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
    }
}

// Regression: unary minus ('-x') has a null left operand. The '-' jsonb-delete
// detection must not treat it as a delete (or it dereferences null). See the
// crash that 'INSERT ... ; SELECT -x' would otherwise cause.
TEST_CASE("integration::cpp::test_computed_schema::unary_minus_not_jsonb_delete") {
    auto config = test_create_config("/tmp/test_computed_schema/unary_minus");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.u ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.u (x) VALUES (5), (7);")->is_success());

    auto cur = exec("SELECT -x FROM cs_testdb.u ORDER BY x;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == -5);
    REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == -7);
}
