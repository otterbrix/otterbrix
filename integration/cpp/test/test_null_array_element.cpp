#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <string>

// Storage-scan pushdown predicates over an ARRAY/LIST element (v[i]) must handle a NULL element
// in three-valued logic and must never treat the array column as a struct.
//
// A value comparison over a NULL element is UNKNOWN (the row is excluded, and NOT does not
// resurrect it). IS NULL / IS NOT NULL over the element are always TRUE/FALSE. An element is NULL
// when the whole array cell is NULL, when the subscript is out of range, or when that element is
// itself NULL. Before the fix, IS NULL over v[i] cast the array column to a struct and read a
// bogus sub-column pointer -- a segfault.

namespace {
    template<typename D>
    bool ok(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        return c && c->is_success();
    }
    template<typename D>
    size_t rows(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        REQUIRE(c);
        REQUIRE(c->is_success());
        return c->size();
    }
} // namespace

TEST_CASE("integration::cpp::null_arr_elem::fixed_array_null_cell") {
    auto config = test_create_config(test_temp_path("test_null_arr_elem/fixed"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE ae;"));
    REQUIRE(ok(d, "CREATE TABLE ae.t (id bigint, v int[3]);"));
    REQUIRE(ok(d, "INSERT INTO ae.t (id, v) VALUES (1, ARRAY[10, 20, 30]);"));
    REQUIRE(ok(d, "INSERT INTO ae.t (id, v) VALUES (2, NULL);")); // whole array cell NULL
    REQUIRE(ok(d, "INSERT INTO ae.t (id, v) VALUES (3, ARRAY[40, 50, 60]);"));

    // Value comparison over the element: the NULL-cell row is UNKNOWN, hence excluded.
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[1] = 10;") == 1); // id=1
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[1] > 5;") == 2);  // id=1,3 (id=2 excluded)
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[2] = 20;") == 1); // id=1

    // IS NULL / IS NOT NULL over the element (previously a segfault).
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[1] IS NULL;") == 1);     // id=2
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[1] IS NOT NULL;") == 2); // id=1,3
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[3] IS NULL;") == 1);     // id=2
}

TEST_CASE("integration::cpp::null_arr_elem::fixed_array_null_padded_element") {
    auto config = test_create_config(test_temp_path("test_null_arr_elem/pad"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE ae;"));
    REQUIRE(ok(d, "CREATE TABLE ae.t (id bigint, v int[3]);"));
    REQUIRE(ok(d, "INSERT INTO ae.t (id, v) VALUES (1, ARRAY[10, 20, 30]);"));
    // A short array pads the missing tail elements with NULL (the column has no default).
    REQUIRE(ok(d, "INSERT INTO ae.t (id, v) VALUES (2, ARRAY[40]);"));

    // v[3] is a real NULL element in row 2: comparison is UNKNOWN, IS NULL is TRUE.
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[3] = 30;") == 1);        // id=1 only
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[3] IS NULL;") == 1);     // id=2
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[3] IS NOT NULL;") == 1); // id=1
    // v[1] is present in both rows.
    CHECK(rows(d, "SELECT id FROM ae.t WHERE v[1] IS NOT NULL;") == 2);
}

TEST_CASE("integration::cpp::null_arr_elem::variadic_list") {
    auto config = test_create_config(test_temp_path("test_null_arr_elem/list"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE ae;"));
    REQUIRE(ok(d, "CREATE TABLE ae.l (id bigint, v int[]);"));
    REQUIRE(ok(d, "INSERT INTO ae.l (id, v) VALUES (1, ARRAY[10, 20]);"));
    REQUIRE(ok(d, "INSERT INTO ae.l (id, v) VALUES (2, NULL);"));      // whole list NULL
    REQUIRE(ok(d, "INSERT INTO ae.l (id, v) VALUES (3, ARRAY[30]);")); // v[2] out of range

    // v[2]: present in row 1, out of range in row 3, NULL cell in row 2 -> only row 1 has it.
    CHECK(rows(d, "SELECT id FROM ae.l WHERE v[2] = 20;") == 1);        // id=1
    CHECK(rows(d, "SELECT id FROM ae.l WHERE v[2] IS NULL;") == 2);     // id=2 (null cell), id=3 (out of range)
    CHECK(rows(d, "SELECT id FROM ae.l WHERE v[2] IS NOT NULL;") == 1); // id=1
    CHECK(rows(d, "SELECT id FROM ae.l WHERE v[1] IS NULL;") == 1);     // id=2 only
}
