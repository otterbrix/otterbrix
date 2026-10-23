#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include "../otterbrix.h"
#include "mirrored_error_codes.hpp"

#include <climits>
#include <cstddef>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/stat.h>
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

            error_message refusal{};
            ptr = otterbrix_create(config(), &refusal);
            REQUIRE(refusal.message == nullptr);
        }

        config_t config() const {
            config_t cfg{};
            cfg.level = 0;
            cfg.log_path = sv(log_path);
            cfg.wal_path = sv(wal_path);
            cfg.disk_path = sv(disk_path);
            cfg.main_path = sv(main_path);
            return cfg;
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
    const char* reason = cursor_error_message(cur);
    REQUIRE(reason != nullptr);
    CHECK(std::string(reason) == "database \"nodb\" does not exist");
    release_cursor(cur);

    run_ok(t.ptr, "CREATE DATABASE otherdb;");
    cursor_ptr other = execute_sql(t.ptr, sv(std::string("CREATE TABLE otherdb.t();")));
    REQUIRE(other != nullptr);
    CHECK(cursor_is_success(other));
    release_cursor(other);
}

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
        CHECK(cursor_column_count(cur) == 0);
        CHECK(cursor_get_value(cur, 0, 0) == nullptr);
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

TEST_CASE("c-api: cursor_created_oid reports the table a CREATE TABLE created", "[c-api][cursor]") {
    test_db_t t("created_oid");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr database = execute_sql(t.ptr, sv(std::string("CREATE DATABASE test_db;")));
    REQUIRE(cursor_is_success(database));
    uint32_t oid = 7;
    CHECK_FALSE(cursor_created_oid(database, &oid));
    CHECK(oid == 7);
    release_cursor(database);

    cursor_ptr table = execute_sql(t.ptr, sv(std::string("CREATE TABLE test_db.users (name string, age bigint);")));
    REQUIRE(cursor_is_success(table));
    REQUIRE(cursor_created_oid(table, &oid));
    release_cursor(table);

    cursor_ptr catalog =
        execute_sql(t.ptr, sv(std::string("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'users';")));
    REQUIRE(cursor_is_success(catalog));
    REQUIRE(cursor_size(catalog) == 1);
    value_ptr stored = cursor_get_value(catalog, 0, 0);
    REQUIRE(stored != nullptr);
    REQUIRE(value_is_uint(stored));
    CHECK(value_get_uint(stored) == oid);
    release_value(stored);
    uint32_t untouched = 7;
    CHECK_FALSE(cursor_created_oid(catalog, &untouched));
    CHECK(untouched == 7);
    release_cursor(catalog);
}

namespace {
    void require_error_cursor(cursor_ptr cur, int32_t code, const std::string& reason) {
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_error(cur));
        CHECK(cursor_error_code(cur) == code);
        const char* message = cursor_error_message(cur);
        REQUIRE(message != nullptr);
        CHECK(std::string(message) == reason);
        release_cursor(cur);
    }

    void require_parameter_refusal(otterbrix_ptr db, const sql_param_t& param, const std::string& reason) {
        const std::string query = "SELECT $1;";
        require_error_cursor(execute_sql_params(db, sv(query), &param, 1), EC_INVALID_PARAMETER, reason);
    }
} // namespace

TEST_CASE("c-api: a parse error answers an error cursor and the engine goes on", "[c-api][errors]") {
    test_db_t t("parse_error");
    REQUIRE(t.ptr != nullptr);

    const std::string broken = "SELECT FROM";
    require_error_cursor(execute_sql(t.ptr, sv(broken)), EC_SQL_PARSE_ERROR, "syntax error at end of input");

    sql_param_t param{};
    param.index = 1;
    param.kind = SQL_PARAM_INT64;
    param.int64_value = 1;
    const std::string broken_with_param = "SELECT FROM $1";
    require_error_cursor(execute_sql_params(t.ptr, sv(broken_with_param), &param, 1),
                         EC_SQL_PARSE_ERROR,
                         "syntax error at or near \"$1\"");

    run_ok(t.ptr, "SELECT 1;");
}

TEST_CASE("c-api: cursor_error_code and cursor_error_message on an error and a success cursor", "[c-api][errors]") {
    test_db_t t("error_accessors");
    REQUIRE(t.ptr != nullptr);

    cursor_ptr refused = execute_sql(t.ptr, sv(std::string("SELECT FROM")));
    REQUIRE(refused != nullptr);
    REQUIRE(cursor_is_error(refused));
    CHECK(cursor_error_code(refused) == EC_SQL_PARSE_ERROR);
    const char* reason = cursor_error_message(refused);
    REQUIRE(reason != nullptr);
    CHECK(cursor_error_message(refused) == reason);
    CHECK(std::string(reason) == "syntax error at end of input");
    release_cursor(refused);

    cursor_ptr fine = execute_sql(t.ptr, sv(std::string("SELECT 1;")));
    REQUIRE(fine != nullptr);
    REQUIRE(cursor_is_success(fine));
    CHECK(cursor_error_code(fine) == 0);
    const char* none = cursor_error_message(fine);
    REQUIRE(none != nullptr);
    CHECK(std::string(none).empty());
    release_cursor(fine);
}

TEST_CASE("c-api: execute_sql_params answers an error cursor for an index below 1", "[c-api][params]") {
    test_db_t t("params_index");
    REQUIRE(t.ptr != nullptr);

    sql_param_t param{};
    param.index = 0;
    param.kind = SQL_PARAM_INT64;
    param.int64_value = 1;
    require_parameter_refusal(t.ptr, param, "sql_param_t: index must be >= 1 (e.g. $1 -> 1)");
}

TEST_CASE("c-api: execute_sql_params answers an error cursor for an unknown parameter kind", "[c-api][params]") {
    test_db_t t("params_kind");
    REQUIRE(t.ptr != nullptr);

    sql_param_t param{};
    param.index = 1;
    // the first value past SQL_PARAM_STRING still lies in the enumeration's range, so the cast is defined
    param.kind = static_cast<sql_param_kind_t>(SQL_PARAM_STRING + 1);
    require_parameter_refusal(t.ptr, param, "sql_param_t: unknown kind");
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

    const char* name = cursor_column_name(cur, 0);
    REQUIRE(name != nullptr);
    REQUIRE(std::string(name) == "n");

    release_cursor(cur);
}

TEST_CASE("c-api: cursor_column_name and value_get_string lend the handle's own string", "[c-api][cursor]") {
    test_db_t t("borrowed_strings");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t (name string);");
    run_ok(t.ptr, "INSERT INTO db.t (name) VALUES ('lent');");

    cursor_ptr cur = execute_sql(t.ptr, sv(std::string("SELECT name FROM db.t;")));
    REQUIRE(cur != nullptr);
    REQUIRE(cursor_is_success(cur));

    const char* name = cursor_column_name(cur, 0);
    REQUIRE(name != nullptr);
    CHECK(cursor_has_next(cur));
    value_ptr by_index = cursor_get_value(cur, 0, 0);
    REQUIRE(by_index != nullptr);
    release_value(by_index);
    value_ptr by_name = cursor_get_value_by_name(cur, 0, sv(std::string("name")));
    REQUIRE(by_name != nullptr);
    release_value(by_name);
    CHECK(cursor_column_name(cur, 0) == name);

    value_ptr val = cursor_get_value(cur, 0, 0);
    REQUIRE(val != nullptr);
    const char* text = value_get_string(val);
    REQUIRE(text != nullptr);
    CHECK(value_is_string(val));
    CHECK(value_get_string(val) == text);

    CHECK(cursor_column_name(cur, 0) == name);
    CHECK(std::string(name) == "name");
    CHECK(std::string(text) == "lent");

    release_value(val);
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

    const char* str = value_get_string(val);
    REQUIRE(str != nullptr);
    REQUIRE(std::string(str) == "hello");

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

namespace {
    template<typename R, typename... A>
    constexpr bool is_noexcept(R (*)(A...) noexcept) {
        return true;
    }
    template<typename R, typename... A>
    constexpr bool is_noexcept(R (*)(A...)) {
        return false;
    }
} // namespace

#define OTTERBRIX_ENTRY_POINTS(X)                                                                                      \
    X(otterbrix_create)                                                                                                \
    X(otterbrix_destroy)                                                                                               \
    X(execute_sql)                                                                                                     \
    X(execute_sql_params)                                                                                              \
    X(release_cursor)                                                                                                  \
    X(cursor_size)                                                                                                     \
    X(cursor_affected_rows)                                                                                            \
    X(cursor_created_oid)                                                                                              \
    X(cursor_column_count)                                                                                             \
    X(cursor_column_logical_type)                                                                                      \
    X(cursor_has_next)                                                                                                 \
    X(cursor_is_success)                                                                                               \
    X(cursor_is_error)                                                                                                 \
    X(cursor_error_code)                                                                                               \
    X(cursor_error_message)                                                                                            \
    X(cursor_error_position)                                                                                           \
    X(cursor_transaction_status)                                                                                       \
    X(cursor_column_name)                                                                                              \
    X(cursor_get_value)                                                                                                \
    X(cursor_get_value_by_name)                                                                                        \
    X(release_value)                                                                                                   \
    X(value_is_null)                                                                                                   \
    X(value_is_bool)                                                                                                   \
    X(value_is_int)                                                                                                    \
    X(value_is_uint)                                                                                                   \
    X(value_is_double)                                                                                                 \
    X(value_is_string)                                                                                                 \
    X(value_get_bool)                                                                                                  \
    X(value_get_int)                                                                                                   \
    X(value_get_uint)                                                                                                  \
    X(value_get_double)                                                                                                \
    X(value_get_string)                                                                                                \
    X(otterbrix_free_string)

#define OTTERBRIX_ASSERT_NOEXCEPT(f) static_assert(is_noexcept(&f));
OTTERBRIX_ENTRY_POINTS(OTTERBRIX_ASSERT_NOEXCEPT)
#undef OTTERBRIX_ASSERT_NOEXCEPT

#define OTTERBRIX_COUNT_ONE(f) +1
constexpr std::size_t entry_point_count = 0 OTTERBRIX_ENTRY_POINTS(OTTERBRIX_COUNT_ONE);
#undef OTTERBRIX_COUNT_ONE
#undef OTTERBRIX_ENTRY_POINTS

TEST_CASE("c-api: every function declared in otterbrix.h is OTTERBRIX_NOEXCEPT", "[c-api][abi]") {
    constexpr std::string_view otterbrix_h_text =
#include "otterbrix_h_text.inc"
        ;
    std::istringstream header{std::string{otterbrix_h_text}};
    std::string code;
    std::string line;
    while (std::getline(header, line)) {
        const auto first = line.find_first_not_of(" \t");
        if (first != std::string::npos && line[first] == '#') {
            continue;
        }
        code += line.substr(0, line.find("//"));
        code += ' ';
    }
    std::size_t declarations = 0;
    std::istringstream statements(code);
    std::string statement;
    while (std::getline(statements, statement, ';')) {
        const auto first = statement.find_first_not_of(" \t");
        const auto last = statement.find_last_not_of(" \t");
        if (first == std::string::npos || statement.find('(') == std::string::npos ||
            statement.compare(first, 7, "typedef") == 0) {
            continue;
        }
        ++declarations;
        INFO(statement.substr(first, last - first + 1));
        CHECK((last + 1 >= 18 && statement.compare(last + 1 - 18, 18, "OTTERBRIX_NOEXCEPT") == 0));
    }
    CHECK(declarations == entry_point_count);
}

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

    const config_t cfg = first.config();

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

namespace {
    class working_directory_guard_t {
    public:
        working_directory_guard_t() { REQUIRE(::getcwd(saved_, sizeof saved_) != nullptr); }
        ~working_directory_guard_t() { CHECK(::chdir(saved_) == 0); }
        working_directory_guard_t(const working_directory_guard_t&) = delete;
        working_directory_guard_t& operator=(const working_directory_guard_t&) = delete;

    private:
        char saved_[PATH_MAX];
    };

    void enter_deleted_directory(const std::string& tag) {
        const std::string gone = "/tmp/otterbrix_c_test_" + tag + "_" + std::to_string(::getpid());
        REQUIRE(::mkdir(gone.c_str(), 0755) == 0);
        REQUIRE(::chdir(gone.c_str()) == 0);
        REQUIRE(::rmdir(gone.c_str()) == 0);
    }
} // namespace

TEST_CASE("c-api: an engine on absolute paths starts in a deleted working directory", "[c-api][lifecycle]") {
    const working_directory_guard_t guard;
    enter_deleted_directory("deleted_cwd_dir");

    test_db_t t("deleted_cwd");
    REQUIRE(t.ptr != nullptr);
    run_ok(t.ptr, "SELECT 1;");
}

TEST_CASE("c-api: an engine on relative paths in a deleted working directory is refused", "[c-api][lifecycle]") {
    const working_directory_guard_t guard;
    enter_deleted_directory("deleted_cwd_relative");

    config_t cfg{};
    const std::string log_path = "rel/log";
    const std::string wal_path = "rel/wal";
    const std::string disk_path = "rel/disk";
    const std::string main_path = "rel/main";
    cfg.log_path = sv(log_path);
    cfg.wal_path = sv(wal_path);
    cfg.disk_path = sv(disk_path);
    cfg.main_path = sv(main_path);

    error_message refusal{};
    REQUIRE(otterbrix_create(cfg, &refusal) == nullptr);
    REQUIRE(refusal.message != nullptr);
    const std::string reason{refusal.message};
    otterbrix_free_string(refusal.message);
    INFO("refusal: " << reason);
    CHECK(refusal.code == EC_IO_ERROR);
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

    const config_t cfg = t.config();

    CHECK(cursor_size(cur) == 1);
    const char* name = cursor_column_name(cur, 0);
    REQUIRE(name != nullptr);
    CHECK(std::string(name) == "name");
    release_cursor(cur);

    const char* text = value_get_string(val);
    REQUIRE(text != nullptr);
    CHECK(std::string(text) == "kept");

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

namespace {
    // Reads (row 0, column col) of `query` and hands the value to check; the cursor and value are released after it.
    template<typename Check>
    void with_value(otterbrix_ptr db, const std::string& query, int32_t col, Check check) {
        cursor_ptr cur = execute_sql(db, sv(query));
        REQUIRE(cur != nullptr);
        REQUIRE(cursor_is_success(cur));
        REQUIRE(cursor_size(cur) == 1);
        value_ptr val = cursor_get_value(cur, 0, col);
        REQUIRE(val != nullptr);
        check(val);
        release_value(val);
        release_cursor(cur);
    }

    bool no_kind(value_ptr val) {
        return !value_is_null(val) && !value_is_bool(val) && !value_is_int(val) && !value_is_uint(val) &&
               !value_is_double(val) && !value_is_string(val);
    }
} // namespace

TEST_CASE("c-api: value kinds of every scalar column type", "[c-api][value]") {
    test_db_t t("value_kinds");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr,
           "CREATE TABLE db.t (b boolean, i bigint, u ubigint, d double, s string, dt date, dec decimal(10,2), "
           "l bigint[], n bigint);");
    run_ok(t.ptr,
           "INSERT INTO db.t (b, i, u, d, s, dt, dec, l, n) VALUES "
           "(true, -5, 7, 2.5, 'txt', DATE '2024-01-01', 1.50, ARRAY[1, 2], NULL);");
    const std::string q = "SELECT b, i, u, d, s, dt, dec, l, n FROM db.t;";

    with_value(t.ptr, q, 0, [](value_ptr v) {
        CHECK(value_is_bool(v));
        CHECK_FALSE(value_is_null(v));
        CHECK(value_get_bool(v));
    });
    with_value(t.ptr, q, 1, [](value_ptr v) {
        CHECK(value_is_int(v));
        CHECK(value_get_int(v) == -5);
    });
    with_value(t.ptr, q, 2, [](value_ptr v) {
        CHECK(value_is_uint(v));
        CHECK_FALSE(value_is_int(v));
        CHECK(value_get_uint(v) == 7);
    });
    with_value(t.ptr, q, 3, [](value_ptr v) {
        CHECK(value_is_double(v));
        CHECK_THAT(value_get_double(v), Catch::Matchers::WithinULP(2.5, 0));
    });
    with_value(t.ptr, q, 4, [](value_ptr v) {
        CHECK(value_is_string(v));
        const char* text = value_get_string(v);
        REQUIRE(text != nullptr);
        CHECK(std::string(text) == "txt");
    });
    // DATE is the engine's day count (2024-01-01 is 8766 days after 2000-01-01), not the Unix one
    with_value(t.ptr, q, 5, [](value_ptr v) {
        CHECK(value_is_int(v));
        CHECK(value_get_int(v) == 8766);
    });
    // DECIMAL(10,2) is the unscaled integer
    with_value(t.ptr, q, 6, [](value_ptr v) {
        CHECK(value_is_int(v));
        CHECK(value_get_int(v) == 150);
    });
    with_value(t.ptr, q, 7, [](value_ptr v) { CHECK(no_kind(v)); });
    with_value(t.ptr, q, 8, [](value_ptr v) {
        CHECK(value_is_null(v));
        CHECK_FALSE(value_is_int(v));
        CHECK_FALSE(value_is_string(v));
    });
}

TEST_CASE("c-api: a 128-bit and a NULL UUID cell", "[c-api][value]") {
    test_db_t t("value_wide");
    REQUIRE(t.ptr != nullptr);

    run_ok(t.ptr, "CREATE DATABASE db;");
    run_ok(t.ptr, "CREATE TABLE db.t (h hugeint, u uuid);");
    run_ok(t.ptr, "INSERT INTO db.t (h, u) VALUES (5, NULL);");

    // No SQL reaches a non-NULL UUID, so the C API has no way to read one.
    with_value(t.ptr, "SELECT h, u FROM db.t;", 0, [](value_ptr v) { CHECK(no_kind(v)); });
    with_value(t.ptr, "SELECT h, u FROM db.t;", 1, [](value_ptr v) { CHECK(value_is_null(v)); });
}

TEST_CASE("c-api: SELECT NULL answers a null value", "[c-api][value]") {
    test_db_t t("value_select_null");
    REQUIRE(t.ptr != nullptr);

    with_value(t.ptr, "SELECT NULL;", 0, [](value_ptr v) {
        CHECK(value_is_null(v));
        CHECK_FALSE(value_is_bool(v));
        CHECK_FALSE(value_is_int(v));
        CHECK_FALSE(value_is_uint(v));
        CHECK_FALSE(value_is_double(v));
        CHECK_FALSE(value_is_string(v));
    });
}

TEST_CASE("c-api: cursor_transaction_status reports where the session stands", "[c-api][cursor]") {
    test_db_t t("transaction_status");
    REQUIRE(t.ptr != nullptr);

    const std::string select = "SELECT 1;";
    cursor_ptr idle = execute_sql(t.ptr, sv(select));
    REQUIRE(cursor_is_success(idle));
    CHECK(cursor_transaction_status(idle) == TRANSACTION_STATUS_IDLE);
    release_cursor(idle);

    const std::string failing = "SELECT no_such_column FROM no_such_db.no_such_table;";
    cursor_ptr autocommit_error = execute_sql(t.ptr, sv(failing));
    REQUIRE(cursor_is_error(autocommit_error));
    CHECK(cursor_transaction_status(autocommit_error) == TRANSACTION_STATUS_IDLE);
    release_cursor(autocommit_error);

    const std::string begin = "BEGIN;";
    cursor_ptr block = execute_sql(t.ptr, sv(begin));
    REQUIRE(cursor_is_success(block));
    CHECK(cursor_transaction_status(block) == TRANSACTION_STATUS_IN_TRANSACTION);
    release_cursor(block);
}

TEST_CASE("c-api: cursor_error_position points at the character a parse error is at", "[c-api][cursor]") {
    test_db_t t("error_position");
    REQUIRE(t.ptr != nullptr);

    const std::string misspelled = "SELEC 1;";
    cursor_ptr at_start = execute_sql(t.ptr, sv(misspelled));
    REQUIRE(cursor_is_error(at_start));
    CHECK(cursor_error_code(at_start) == EC_SQL_PARSE_ERROR);
    CHECK(std::string(cursor_error_message(at_start)) == "syntax error at or near \"SELEC\"");
    CHECK(cursor_error_position(at_start) == 1);
    release_cursor(at_start);

    const std::string unterminated = "SELECT 'жж";
    cursor_ptr in_literal = execute_sql(t.ptr, sv(unterminated));
    REQUIRE(cursor_is_error(in_literal));
    CHECK(cursor_error_position(in_literal) == 8);
    release_cursor(in_literal);

    const std::string missing = "SELECT * FROM no_such_db.no_such_table;";
    cursor_ptr unpositioned = execute_sql(t.ptr, sv(missing));
    REQUIRE(cursor_is_error(unpositioned));
    CHECK(cursor_error_position(unpositioned) == -1);
    release_cursor(unpositioned);

    const std::string fine = "SELECT 1;";
    cursor_ptr success = execute_sql(t.ptr, sv(fine));
    REQUIRE(cursor_is_success(success));
    CHECK(cursor_error_position(success) == -1);
    release_cursor(success);
}
