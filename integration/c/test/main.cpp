#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>

#include "../otterbrix.h"

#include <string>
#include <unistd.h>
#include <vector>

// ---------------------------------------------------------------------------
// C-API boundary tests.
//
// These guard the same contracts the Rust integration tests exercise
// (otterbrix/tests/ddl.rs, cursor.rs, values.rs) but at the raw C-API level
// declared in integration/c/otterbrix.h. The Rust side currently is the only
// consumer that asserts on DDL cursor sizes, the OOB cursor/column contracts
// and the value dispatch; these tests reproduce that coverage in C++.
//
// NOTE: otterbrix.h does NOT expose the LOGICAL_TYPE_* constants (they live in
// the Rust crate, integration/rust/otterbrix/src/cursor.rs). cursor_column_
// logical_type() returns the raw components::types::logical_type enum value
// (see components/types/types.hpp). We mirror the relevant enum values here so
// the assertions match both the C++ enum and the Rust constants 1:1.
// ---------------------------------------------------------------------------

namespace {

    // Mirrors components::types::logical_type (components/types/types.hpp) and
    // the LOGICAL_TYPE_* constants in integration/rust/otterbrix/src/cursor.rs.
    constexpr int32_t LT_BOOLEAN = 10;
    constexpr int32_t LT_INTEGER = 13;
    constexpr int32_t LT_BIGINT = 14;
    constexpr int32_t LT_DOUBLE = 24;
    constexpr int32_t LT_STRING_LITERAL = 35;

    string_view_t sv(const std::string& s) { return string_view_t{s.data(), s.size()}; }

    // RAII setup mirroring integration/rust/otterbrix-sys/tests/smoke.rs:
    // hold the path strings in scope for the lifetime of the config_t, build a
    // unique /tmp dir per test, and keep wal/disk off so nothing touches disk.
    struct test_db_t {
        std::string base;
        std::string log_path;
        std::string wal_path;
        std::string disk_path;
        std::string main_path;
        otterbrix_ptr ptr{nullptr};

        explicit test_db_t(const std::string& tag) {
            base = test_temp_path("otterbrix_c_test_" + tag);
            log_path = base + "/log";
            wal_path = base + "/wal";
            disk_path = base + "/disk";
            main_path = base + "/main";

            config_t cfg{};
            cfg.level = 0;
            cfg.log_path = sv(log_path);
            cfg.wal_path = sv(wal_path);
            cfg.disk_path = sv(disk_path);
            cfg.main_path = sv(main_path);
            cfg.wal_on = false;
            cfg.disk_on = false;
            cfg.sync_to_disk = false;

            ptr = otterbrix_create(cfg);
        }

        ~test_db_t() {
            if (ptr != nullptr) {
                otterbrix_destroy(ptr);
            }
        }

        test_db_t(const test_db_t&) = delete;
        test_db_t& operator=(const test_db_t&) = delete;
    };

    // Run a SQL statement and assert it succeeded; release the cursor.
    void run_ok(otterbrix_ptr db, const std::string& query) {
        cursor_ptr cur = execute_sql(db, sv(query));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        release_cursor(cur);
    }

} // namespace

// --------------------------------------------------------------------------
// DDL cursor-size contract: every DDL statement yields a successful, empty
// cursor (size 0). Mirrors ddl.rs create_database_returns_cursor /
// create_collection_returns_cursor and the drop_* path that is currently
// untested everywhere on the C++ side.
// --------------------------------------------------------------------------

TEST_CASE("c-api: create_database returns successful empty cursor", "[c-api][ddl]") {
    test_db_t t("create_database");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr cur = create_database(t.ptr, sv(std::string("mydb")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE_FALSE(cursor_is_error(cur));
    REQUIRE(cursor_size(cur) == 0);
    release_cursor(cur);
}

TEST_CASE("c-api: create_collection returns successful empty cursor", "[c-api][ddl]") {
    test_db_t t("create_collection");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr db_cur = create_database(t.ptr, sv(std::string("mydb")));
    REQUIRE(db_cur != nullptr);
    REQUIRE(cursor_is_success(db_cur));
    release_cursor(db_cur);

    cursor_ptr cur = create_collection(t.ptr, sv(std::string("mydb")), sv(std::string("users")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 0);
    release_cursor(cur);
}

TEST_CASE("c-api: drop_collection then drop_database succeed with empty cursors", "[c-api][ddl]") {
    test_db_t t("drop");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr db_cur = create_database(t.ptr, sv(std::string("dropme")));
    REQUIRE(db_cur != nullptr);
    REQUIRE(cursor_is_success(db_cur));
    release_cursor(db_cur);

    cursor_ptr coll_cur = create_collection(t.ptr, sv(std::string("dropme")), sv(std::string("t")));
    REQUIRE(coll_cur != nullptr);
    REQUIRE(cursor_is_success(coll_cur));
    release_cursor(coll_cur);

    cursor_ptr drop_coll = drop_collection(t.ptr, sv(std::string("dropme")), sv(std::string("t")));
    REQUIRE(drop_coll != nullptr);
    REQUIRE(cursor_is_success(drop_coll));
    REQUIRE_FALSE(cursor_is_error(drop_coll));
    REQUIRE(cursor_size(drop_coll) == 0);
    release_cursor(drop_coll);

    cursor_ptr drop_db = drop_database(t.ptr, sv(std::string("dropme")));
    REQUIRE(drop_db != nullptr);
    REQUIRE(cursor_is_success(drop_db));
    REQUIRE_FALSE(cursor_is_error(drop_db));
    REQUIRE(cursor_size(drop_db) == 0);
    release_cursor(drop_db);
}

TEST_CASE("c-api: DDL via execute_sql returns successful empty cursors", "[c-api][ddl]") {
    test_db_t t("ddl_sql");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr db_cur = execute_sql(t.ptr, sv(std::string("CREATE DATABASE testdb;")));
    REQUIRE(db_cur != nullptr);
    REQUIRE(cursor_is_success(db_cur));
    REQUIRE(cursor_size(db_cur) == 0);
    release_cursor(db_cur);

    cursor_ptr tbl_cur = execute_sql(t.ptr, sv(std::string("CREATE TABLE testdb.items (name string, price bigint);")));
    REQUIRE(tbl_cur != nullptr);
    REQUIRE(cursor_is_success(tbl_cur));
    REQUIRE(cursor_size(tbl_cur) == 0);
    release_cursor(tbl_cur);
}

// --------------------------------------------------------------------------
// SELECT after INSERT: cursor_size reflects the row count.
// Mirrors otterbrix-sys/tests/smoke.rs::test_execute_sql.
// --------------------------------------------------------------------------

TEST_CASE("c-api: cursor_size matches inserted row count", "[c-api][cursor]") {
    test_db_t t("select_size");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE test_db;");
    run_ok(t.ptr, "CREATE TABLE test_db.users (name string, age bigint);");
    run_ok(t.ptr, "INSERT INTO test_db.users (name, age) VALUES ('Alice', 30), ('Bob', 25);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT * FROM test_db.users;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 2);
    release_cursor(cur);
}

// --------------------------------------------------------------------------
// Out-of-bounds / negative column index contracts.
// Mirrors cursor.rs column_logical_type_returns_none_for_negative_index,
// ..._out_of_bounds_index, and column_name_returns_none_for_out_of_bounds_index.
// cursor_column_logical_type returns -1 on OOB; cursor_column_name returns
// nullptr on OOB.
// --------------------------------------------------------------------------

TEST_CASE("c-api: cursor_column_logical_type rejects negative and OOB indices", "[c-api][cursor]") {
    test_db_t t("col_type_oob");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE typedb;");
    run_ok(t.ptr, "CREATE TABLE typedb.t (n integer);");
    run_ok(t.ptr, "INSERT INTO typedb.t (n) VALUES (1);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT n FROM typedb.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_column_count(cur) == 1);

    REQUIRE(cursor_column_logical_type(cur, -1) == -1);
    REQUIRE(cursor_column_logical_type(cur, 100) == -1);

    release_cursor(cur);
}

TEST_CASE("c-api: cursor_column_name returns nullptr for OOB index", "[c-api][cursor]") {
    test_db_t t("col_name_oob");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE namedb;");
    run_ok(t.ptr, "CREATE TABLE namedb.t (n integer);");
    run_ok(t.ptr, "INSERT INTO namedb.t (n) VALUES (1);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT n FROM namedb.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_column_count(cur) == 1);

    REQUIRE(cursor_column_name(cur, 100) == nullptr);

    // In-bounds name is heap-allocated and must be freed via otterbrix_free_string.
    char* name = cursor_column_name(cur, 0);
    REQUIRE(name != nullptr);
    REQUIRE(std::string(name) == "n");
    otterbrix_free_string(name);

    release_cursor(cur);
}

// --------------------------------------------------------------------------
// M3-B2 characterization: the column NAMES the C ABI publishes, across the
// shapes a name can come from. C# and Rust reach the engine through exactly
// these entry points, so this is the whole binding surface for column identity.
//
// cursor_column_name and cursor_get_value_by_name read the cursor's own
// descriptor (cursor_t::columns()), NOT the result chunk. The two sources are
// not interchangeable (see components/cursor/tests/test_cursor.cpp,
// two_sources_of_column_identity), so these pin what the descriptor answers and
// will fail loudly if a reader is ever repointed at the chunk.
// --------------------------------------------------------------------------

namespace {

    // The column names a cursor publishes, in order, freeing each as it goes.
    std::vector<std::string> c_api_column_names(cursor_ptr cur) {
        std::vector<std::string> names;
        for (int32_t col = 0; col < cursor_column_count(cur); ++col) {
            char* name = cursor_column_name(cur, col);
            REQUIRE(name != nullptr);
            names.emplace_back(name);
            otterbrix_free_string(name);
        }
        return names;
    }

} // namespace

TEST_CASE("c-api: cursor_column_name publishes the query's column names", "[c-api][cursor]") {
    test_db_t t("col_name_shapes");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE namesdb;");
    run_ok(t.ptr, "CREATE TABLE namesdb.t (id bigint, label string, price double);");
    run_ok(t.ptr, "INSERT INTO namesdb.t (id, label, price) VALUES (1, 'a', 1.5), (2, 'b', 2.5);");

    // No SECTIONs here on purpose: Catch2 re-runs the whole body once per section, and
    // test_db_t's paths are keyed by tag, so four sections would stand four engines up on
    // one directory and the second would inherit the first's catalog.

    INFO("explicit projection keeps the source column names, in order");
    {
        cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT price, id FROM namesdb.t ORDER BY id;")));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        REQUIRE(c_api_column_names(cur) == std::vector<std::string>{"price", "id"});
        release_cursor(cur);
    }

    INFO("AS renames the published column");
    {
        cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT id AS ident, label AS text FROM namesdb.t;")));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        REQUIRE(c_api_column_names(cur) == std::vector<std::string>{"ident", "text"});

        // The renamed column resolves by its NEW name and not by its old one.
        value_ptr by_new = cursor_get_value_by_name(cur, 0, sv(std::string("ident")));
        REQUIRE(by_new != nullptr);
        release_value(by_new);
        REQUIRE(cursor_get_value_by_name(cur, 0, sv(std::string("id"))) == nullptr);
        release_cursor(cur);
    }

    INFO("SELECT * publishes every table column");
    {
        cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT * FROM namesdb.t ORDER BY id;")));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        REQUIRE(c_api_column_names(cur) == std::vector<std::string>{"id", "label", "price"});
        release_cursor(cur);
    }

    INFO("EXPLAIN publishes a single QUERY PLAN column");
    {
        // EXPLAIN's column name is WRITTEN once, in renderer_postgres.cpp, and read
        // back only here — the renderer itself reads no column name at all.
        cursor_ptr cur = execute_sql(t.ptr, sv(std::string("EXPLAIN SELECT id FROM namesdb.t;")));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        REQUIRE(cursor_column_count(cur) == 1);
        REQUIRE(c_api_column_names(cur) == std::vector<std::string>{"QUERY PLAN"});
        release_cursor(cur);
    }
}

// --------------------------------------------------------------------------
// Logical type reporting for a known SELECT. Mirrors
// ddl.rs::cursor_reports_logical_type_for_basic_types.
// --------------------------------------------------------------------------

TEST_CASE("c-api: cursor_column_logical_type reports basic types", "[c-api][cursor]") {
    test_db_t t("col_types");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE typedb;");
    run_ok(t.ptr, "CREATE TABLE typedb.mix (i integer, big bigint, flag boolean, val double, label string);");
    run_ok(t.ptr, "INSERT INTO typedb.mix (i, big, flag, val, label) VALUES (1, 2, true, 3.5, 'x');");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT i, big, flag, val, label FROM typedb.mix;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_column_count(cur) == 5);

    REQUIRE(cursor_column_logical_type(cur, 0) == LT_INTEGER);
    REQUIRE(cursor_column_logical_type(cur, 1) == LT_BIGINT);
    REQUIRE(cursor_column_logical_type(cur, 2) == LT_BOOLEAN);
    REQUIRE(cursor_column_logical_type(cur, 3) == LT_DOUBLE);
    REQUIRE(cursor_column_logical_type(cur, 4) == LT_STRING_LITERAL);

    release_cursor(cur);
}

// --------------------------------------------------------------------------
// cursor_has_next is true over a non-empty result. Mirrors
// cursor.rs::has_next_is_true_when_select_returns_rows.
// --------------------------------------------------------------------------

TEST_CASE("c-api: cursor_has_next is true on non-empty SELECT", "[c-api][cursor]") {
    test_db_t t("has_next");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr db_cur = create_database(t.ptr, sv(std::string("db")));
    REQUIRE(db_cur != nullptr);
    REQUIRE(cursor_is_success(db_cur));
    release_cursor(db_cur);

    cursor_ptr coll_cur = create_collection(t.ptr, sv(std::string("db")), sv(std::string("t")));
    REQUIRE(coll_cur != nullptr);
    REQUIRE(cursor_is_success(coll_cur));
    release_cursor(coll_cur);

    run_ok(t.ptr, "INSERT INTO db.t (x) VALUES (1), (2), (3);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT x FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_has_next(cur));
    REQUIRE(cursor_size(cur) == 3);
    release_cursor(cur);
}

// --------------------------------------------------------------------------
// Value dispatch: bigint column extracts as int. Mirrors
// values.rs::extract_integer_value. Exercises cursor_get_value, the value_is_*
// predicates and value_get_int, plus the release_value memory contract.
// --------------------------------------------------------------------------

TEST_CASE("c-api: value dispatch for an integer column", "[c-api][value]") {
    test_db_t t("value_int");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t (num bigint);");
    run_ok(t.ptr, "INSERT INTO db.t (num) VALUES (42);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT num FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 1);

    value_ptr val = cursor_get_value(cur, 0, 0);
    REQUIRE(val != nullptr);
    REQUIRE(value_is_int(val));
    REQUIRE_FALSE(value_is_string(val));
    REQUIRE_FALSE(value_is_null(val));
    REQUIRE(value_get_int(val) == 42);
    release_value(val);

    release_cursor(cur);
}

// --------------------------------------------------------------------------
// Value dispatch: string column extracts as string. Mirrors
// values.rs::extract_string_value. Exercises value_get_string + the
// otterbrix_free_string memory contract.
// --------------------------------------------------------------------------

TEST_CASE("c-api: value dispatch for a string column", "[c-api][value]") {
    test_db_t t("value_string");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t (name string);");
    run_ok(t.ptr, "INSERT INTO db.t (name) VALUES ('hello');");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT name FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 1);

    value_ptr val = cursor_get_value(cur, 0, 0);
    REQUIRE(val != nullptr);
    REQUIRE(value_is_string(val));
    REQUIRE_FALSE(value_is_int(val));
    REQUIRE_FALSE(value_is_null(val));

    char* str = value_get_string(val);
    REQUIRE(str != nullptr);
    REQUIRE(std::string(str) == "hello");
    otterbrix_free_string(str);

    release_value(val);
    release_cursor(cur);
}

// --------------------------------------------------------------------------
// cursor_get_value out-of-bounds row/column yields nullptr (see main.cpp:
// the bounds check returns nullptr before allocating a value).
// --------------------------------------------------------------------------

TEST_CASE("c-api: cursor_get_value returns nullptr for OOB row/column", "[c-api][value]") {
    test_db_t t("value_oob");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t (num bigint);");
    run_ok(t.ptr, "INSERT INTO db.t (num) VALUES (7);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT num FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 1);

    REQUIRE(cursor_get_value(cur, 100, 0) == nullptr);
    REQUIRE(cursor_get_value(cur, 0, 100) == nullptr);

    release_cursor(cur);
}

// --------------------------------------------------------------------------
// cursor_get_value_by_name: the name->value contract every binding rides on.
// It walks the cursor's columns() and answers the FIRST column whose name
// matches, or nullptr when no column carries the name. The python binding
// (integration/python/sql/wrapper_cursor.cpp, wrapper_cursor::get_) runs the
// same loop and returns py::none() in the same "no such name" case, pinned by
// integration/python/tests/test_dynamic_schema.py.
// --------------------------------------------------------------------------

TEST_CASE("c-api: cursor_get_value_by_name resolves a present name and rejects an absent one",
          "[c-api][value]") {
    test_db_t t("value_by_name");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE namedb;");
    run_ok(t.ptr, "CREATE TABLE namedb.t (num bigint, label string);");
    run_ok(t.ptr, "INSERT INTO namedb.t (num, label) VALUES (7, 'seven');");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT num, label FROM namedb.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_column_count(cur) == 2);

    value_ptr num = cursor_get_value_by_name(cur, 0, sv(std::string("num")));
    REQUIRE(num != nullptr);
    REQUIRE(value_is_int(num));
    REQUIRE(value_get_int(num) == 7);
    release_value(num);

    value_ptr label = cursor_get_value_by_name(cur, 0, sv(std::string("label")));
    REQUIRE(label != nullptr);
    REQUIRE(value_is_string(label));
    char* text = value_get_string(label);
    REQUIRE(text != nullptr);
    REQUIRE(std::string(text) == "seven");
    otterbrix_free_string(text);
    release_value(label);

    // No column carries this name -> nullptr, never a fabricated or out-of-range read.
    REQUIRE(cursor_get_value_by_name(cur, 0, sv(std::string("no_such_column"))) == nullptr);
    // Casing is not folded: the lookup is an exact alias match.
    REQUIRE(cursor_get_value_by_name(cur, 0, sv(std::string("NUM"))) == nullptr);

    release_cursor(cur);
}

// A computing table can carry two columns of one name and different physical
// type (mirrors integration/cpp/test/test_computed_schema.cpp). Both reach the
// result, so the name has two right answers; cursor_get_value_by_name commits
// to the first one. This pin records that commitment.
TEST_CASE("c-api: cursor_get_value_by_name answers the FIRST of two same-named columns", "[c-api][value]") {
    test_db_t t("value_by_name_dup");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE dupdb;");
    run_ok(t.ptr, "CREATE TABLE dupdb.mt ();");
    run_ok(t.ptr, "INSERT INTO dupdb.mt (id, val) VALUES (1, 1), (2, 2);");
    run_ok(t.ptr, "INSERT INTO dupdb.mt (id, val) VALUES (3, 'hello');");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT * FROM dupdb.mt ORDER BY id;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 3);
    REQUIRE(cursor_column_count(cur) == 3);

    // Locate both 'val' columns by position.
    int32_t first_val = -1;
    int32_t second_val = -1;
    for (int32_t col = 0; col < cursor_column_count(cur); ++col) {
        char* name = cursor_column_name(cur, col);
        REQUIRE(name != nullptr);
        const bool is_val = std::string(name) == "val";
        otterbrix_free_string(name);
        if (!is_val) {
            continue;
        }
        if (first_val < 0) {
            first_val = col;
        } else if (second_val < 0) {
            second_val = col;
        }
    }
    REQUIRE(first_val >= 0);
    REQUIRE(second_val > first_val);
    // The two answers are genuinely different columns, not a repeated one.
    REQUIRE(cursor_column_logical_type(cur, first_val) != cursor_column_logical_type(cur, second_val));

    // Row 0 (id = 1) has a value in exactly one of the two 'val' columns.
    value_ptr by_name = cursor_get_value_by_name(cur, 0, sv(std::string("val")));
    value_ptr at_first = cursor_get_value(cur, 0, first_val);
    value_ptr at_second = cursor_get_value(cur, 0, second_val);
    REQUIRE(by_name != nullptr);
    REQUIRE(at_first != nullptr);
    REQUIRE(at_second != nullptr);

    REQUIRE(value_is_null(at_first) != value_is_null(at_second));
    // by_name matched the FIRST of the two.
    REQUIRE(value_is_null(by_name) == value_is_null(at_first));
    REQUIRE(value_is_int(by_name) == value_is_int(at_first));
    REQUIRE(value_is_string(by_name) == value_is_string(at_first));

    release_value(at_second);
    release_value(at_first);
    release_value(by_name);
    release_cursor(cur);
}
