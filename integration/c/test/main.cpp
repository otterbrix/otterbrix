#include <catch2/catch_test_macros.hpp>

#include "../otterbrix.h"

#include <cstddef>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>

namespace {

    // otterbrix.h doesn't expose LOGICAL_TYPE_*; these mirror types.hpp's logical_type and src/cursor.rs 1:1.
    constexpr int32_t LT_BOOLEAN = 10;
    constexpr int32_t LT_INTEGER = 13;
    constexpr int32_t LT_BIGINT = 14;
    constexpr int32_t LT_DOUBLE = 24;
    constexpr int32_t LT_STRING_LITERAL = 35;

    string_view_t sv(const std::string& s) { return string_view_t{s.data(), s.size()}; }

    // Mirrors otterbrix-sys/tests/smoke.rs; path strings are members so they outlive config_t's use here.
    struct test_db_t {
        std::string base;
        std::string log_path;
        std::string wal_path;
        std::string disk_path;
        std::string main_path;
        otterbrix_ptr ptr{nullptr};

        explicit test_db_t(const std::string& tag) {
            base = "/tmp/otterbrix_c_test_" + tag + "_" + std::to_string(::getpid());
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

            error_message refusal{};
            ptr = otterbrix_create(cfg, &refusal);
            REQUIRE(refusal.message == nullptr);
        }

        ~test_db_t() {
            if (ptr != nullptr) {
                otterbrix_destroy(ptr);
            }
        }

        test_db_t(const test_db_t&) = delete;
        test_db_t& operator=(const test_db_t&) = delete;
    };

    void run_ok(otterbrix_ptr db, const std::string& query) {
        cursor_ptr cur = execute_sql(db, sv(query));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        release_cursor(cur);
    }

} // namespace

// Mirrors ddl.rs create_database_returns_cursor / create_collection_returns_cursor; the DROP path
// is otherwise untested on the C++ side.

TEST_CASE("c-api: CREATE DATABASE returns successful empty cursor", "[c-api][ddl]") {
    test_db_t t("create_database");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("CREATE DATABASE mydb;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE_FALSE(cursor_is_error(cur));
    REQUIRE(cursor_size(cur) == 0);
    release_cursor(cur);
}

TEST_CASE("c-api: CREATE TABLE without columns returns successful empty cursor", "[c-api][ddl]") {
    test_db_t t("create_collection");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE mydb;");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("CREATE TABLE mydb.users();")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_size(cur) == 0);
    release_cursor(cur);
}

TEST_CASE("c-api: CREATE TABLE in a database that does not exist is refused", "[c-api][ddl]") {
    test_db_t t("create_collection_no_db");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("CREATE TABLE nodb.t();")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_error(cur));
    error_message refusal = cursor_get_error(cur);
    REQUIRE(refusal.message != nullptr);
    CHECK(std::string(refusal.message) == "database \"nodb\" does not exist");
    otterbrix_free_string(refusal.message);
    release_cursor(cur);

    run_ok(t.ptr, "CREATE DATABASE otherdb;");
    cursor_ptr other = execute_sql(t.ptr, sv(std::string("CREATE TABLE otherdb.t();")));
    REQUIRE(other != nullptr);
    CHECK(cursor_is_success(other));
    release_cursor(other);
}

TEST_CASE("c-api: CREATE DATABASE folds an unquoted name to lower case", "[c-api][ddl]") {
    test_db_t t("create_database_case");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE SqlDatabase;");
    cursor_ptr folded = execute_sql(t.ptr,
                                    sv(std::string("SELECT nspname FROM pg_catalog.pg_namespace "
                                                   "WHERE nspname = 'sqldatabase';")));
    REQUIRE(cursor_is_success(folded));
    CHECK(cursor_size(folded) == 1);
    release_cursor(folded);
}

// Document mode through the C API: a lower-case database, a table without columns, fields registered by INSERT.
TEST_CASE("c-api: document flow in a database created through the C API", "[c-api][ddl]") {
    test_db_t t("document_flow");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE docdb;");
    run_ok(t.ptr, "CREATE TABLE docdb.docs();");

    run_ok(t.ptr, "INSERT INTO docdb.docs (id, name) VALUES (1, 'a'), (2, 'b');");
    run_ok(t.ptr, "INSERT INTO docdb.docs (id, score) VALUES (3, 30);");
    cursor_ptr read = execute_sql(t.ptr, sv(std::string("SELECT id FROM docdb.docs;")));
    REQUIRE(cursor_is_success(read));
    CHECK(cursor_size(read) == 3);
    release_cursor(read);
}

// SQL folds an unquoted table name to lower case, so a mixed-case INSERT reaches the lower-case table.
TEST_CASE("c-api: an unquoted table name folds to lower case", "[c-api][ddl]") {
    test_db_t t("create_collection_case");
    REQUIRE(t.ptr != nullptr);
    run_ok(t.ptr, "CREATE DATABASE db;");

    run_ok(t.ptr, "CREATE TABLE db.testcollection();");
    run_ok(t.ptr, "INSERT INTO db.TestCollection (id) VALUES (1);");
    cursor_ptr read = execute_sql(t.ptr, sv(std::string("SELECT id FROM db.testcollection;")));
    REQUIRE(cursor_is_success(read));
    CHECK(cursor_size(read) == 1);
    release_cursor(read);
}

TEST_CASE("c-api: DROP TABLE then DROP DATABASE succeed with empty cursors", "[c-api][ddl]") {
    test_db_t t("drop");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE dropme;");
    run_ok(t.ptr, "CREATE TABLE dropme.t();");

    cursor_ptr drop_coll = execute_sql(t.ptr, sv(std::string("DROP TABLE dropme.t;")));
    REQUIRE(drop_coll != nullptr);
    REQUIRE(cursor_is_success(drop_coll));
    REQUIRE_FALSE(cursor_is_error(drop_coll));
    REQUIRE(cursor_size(drop_coll) == 0);
    release_cursor(drop_coll);

    cursor_ptr drop_db = execute_sql(t.ptr, sv(std::string("DROP DATABASE dropme;")));
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

// Mirrors otterbrix-sys/tests/smoke.rs::test_execute_sql.

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

TEST_CASE("c-api: cursor_affected_rows reports the rows a write changed", "[c-api][cursor]") {
    test_db_t t("affected_rows");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE test_db;");
    run_ok(t.ptr, "CREATE TABLE test_db.users (name string, age bigint);");

    auto affected = [&](const std::string& sql, uint64_t* rows) {
        cursor_ptr cur = execute_sql(t.ptr, sv(sql));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        const bool wrote = cursor_affected_rows(cur, rows);
        CHECK(cursor_size(cur) == 0);
        release_cursor(cur);
        return wrote;
    };

    uint64_t rows = 99;
    REQUIRE(affected("INSERT INTO test_db.users (name, age) VALUES ('Alice', 30), ('Bob', 25);", &rows));
    CHECK(rows == 2);
    REQUIRE(affected("UPDATE test_db.users SET age = 31 WHERE name = 'Alice';", &rows));
    CHECK(rows == 1);
    REQUIRE(affected("DELETE FROM test_db.users WHERE age > 100;", &rows));
    CHECK(rows == 0);

    cursor_ptr select = execute_sql(t.ptr, sv(std::string("SELECT * FROM test_db.users;")));
    REQUIRE(cursor_is_success(select));
    rows = 99;
    CHECK_FALSE(cursor_affected_rows(select, &rows));
    CHECK(rows == 99);
    release_cursor(select);
}

// Mirrors cursor.rs column_logical_type_returns_none_for_negative_index, ..._out_of_bounds_index,
// and column_name_returns_none_for_out_of_bounds_index.

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

// Mirrors ddl.rs::cursor_reports_logical_type_for_basic_types.

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

// Mirrors cursor.rs::has_next_is_true_when_select_returns_rows.

TEST_CASE("c-api: cursor_has_next is true on non-empty SELECT", "[c-api][cursor]") {
    test_db_t t("has_next");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t();");

    run_ok(t.ptr, "INSERT INTO db.t (x) VALUES (1), (2), (3);");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT x FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    REQUIRE(cursor_has_next(cur));
    REQUIRE(cursor_size(cur) == 3);
    release_cursor(cur);
}

// Mirrors values.rs::extract_integer_value.

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

// Mirrors values.rs::extract_string_value.

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

// See integration/c/main.cpp: cursor_get_value's bounds check returns nullptr before allocating a value.

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

namespace {
    template<typename T, typename = void>
    struct has_sync_to_disk : std::false_type {};
    template<typename T>
    struct has_sync_to_disk<T, std::void_t<decltype(std::declval<const T&>().sync_to_disk)>> : std::true_type {};

    template<typename T, typename = void>
    struct has_wal_on : std::false_type {};
    template<typename T>
    struct has_wal_on<T, std::void_t<decltype(std::declval<const T&>().wal_on)>> : std::true_type {};
} // namespace

// The C struct is the contract the hand-written C# mirror copies field for field, and nothing
// there is checked by a compiler -- so the shape is pinned here instead.
TEST_CASE("c-api: config_t carries no boolean switches", "[c-api][abi]") {
    CHECK_FALSE(has_sync_to_disk<config_t>::value);
    CHECK_FALSE(has_wal_on<config_t>::value);
    CHECK(std::is_standard_layout_v<config_t>);
    // Now that both bools are gone, main_path really is the last field, and sizeof says so --
    // it could not while a trailing bool hid inside the tail padding.
    CHECK(sizeof(config_t) == offsetof(config_t, main_path) + sizeof(string_view_t));
}

TEST_CASE("c-api: a second engine on the same main_path is refused", "[c-api][lifecycle]") {
    test_db_t first("same_main_path");
    REQUIRE(first.ptr != nullptr);

    config_t cfg{};
    cfg.level = 0;
    cfg.log_path = sv(first.log_path);
    cfg.wal_path = sv(first.wal_path);
    cfg.disk_path = sv(first.disk_path);
    cfg.main_path = sv(first.main_path);

    error_message refusal{};
    REQUIRE(otterbrix_create(cfg, &refusal) == nullptr);
    REQUIRE(refusal.code != 0);
    REQUIRE(refusal.message != nullptr);
    const std::string reason{refusal.message};
    otterbrix_free_string(refusal.message);
    INFO("refusal: " << reason);
    CHECK(reason.find("unique directory") != std::string::npos);

    otterbrix_destroy(first.ptr);
    error_message none{};
    first.ptr = otterbrix_create(cfg, &none);
    REQUIRE(first.ptr != nullptr);
    CHECK(none.code == 0);
    CHECK(none.message == nullptr);
}

TEST_CASE("c-api: a cursor and a value outlive otterbrix_destroy", "[c-api][lifecycle]") {
    test_db_t t("outlive_destroy");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t (name string);");
    run_ok(t.ptr, "INSERT INTO db.t (name) VALUES ('kept');");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT name FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));
    value_ptr val = cursor_get_value(cur, 0, 0);
    REQUIRE(val != nullptr);

    otterbrix_destroy(t.ptr);
    t.ptr = nullptr;

    config_t cfg{};
    cfg.level = 0;
    cfg.log_path = sv(t.log_path);
    cfg.wal_path = sv(t.wal_path);
    cfg.disk_path = sv(t.disk_path);
    cfg.main_path = sv(t.main_path);

    CHECK(cursor_size(cur) == 1);
    char* name = cursor_column_name(cur, 0);
    REQUIRE(name != nullptr);
    CHECK(std::string(name) == "name");
    otterbrix_free_string(name);
    release_cursor(cur);

    char* text = value_get_string(val);
    REQUIRE(text != nullptr);
    CHECK(std::string(text) == "kept");
    otterbrix_free_string(text);

    // the value still holds the engine, and with it main_path
    error_message refusal{};
    CHECK(otterbrix_create(cfg, &refusal) == nullptr);
    otterbrix_free_string(refusal.message);

    std::thread([val] { release_value(val); }).join();

    error_message none{};
    t.ptr = otterbrix_create(cfg, &none);
    CHECK(t.ptr != nullptr);
    CHECK(none.message == nullptr);
}
