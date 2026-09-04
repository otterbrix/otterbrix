#include "test_config.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <string>

using namespace components;

namespace {
    components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }
} // namespace

TEST_CASE("integration::list_array::fixed_array_crud") {
    auto config = test_create_config("/tmp/test_list_array/fixed_array_crud");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("DDL: a fixed-size int[3] ARRAY column is accepted");
    { REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success()); }

    INFO("INSERT ARRAY[...] literal materializes the array column");
    {
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[10,20,30]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (2, ARRAY[40,50,60]);")->is_success());
    }

    INFO("SELECT round-trips the whole array (3 children per row)");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 1);

        auto row0 = cur->value(0, 0);
        REQUIRE(row0.children().size() == 3);
        REQUIRE(row0.children()[0].value<int32_t>() == 10);
        REQUIRE(row0.children()[1].value<int32_t>() == 20);
        REQUIRE(row0.children()[2].value<int32_t>() == 30);

        auto row1 = cur->value(0, 1);
        REQUIRE(row1.children().size() == 3);
        REQUIRE(row1.children()[0].value<int32_t>() == 40);
        REQUIRE(row1.children()[2].value<int32_t>() == 60);
    }

    INFO("subscript READ v[i] is 1-based and projects one scalar column per index");
    {
        auto cur = exec(dispatcher, "SELECT v[1], v[2], v[3] FROM TestDatabase.arr;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 3);
        REQUIRE(cur->size() == 2);
        // row 0 == {10,20,30}
        REQUIRE(cur->value(0, 0).value<int32_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int32_t>() == 20);
        REQUIRE(cur->value(2, 0).value<int32_t>() == 30);
        // row 1 == {40,50,60}
        REQUIRE(cur->value(0, 1).value<int32_t>() == 40);
        REQUIRE(cur->value(2, 1).value<int32_t>() == 60);
    }

    INFO("subscript UPDATE v[i] = x mutates a single element in place");
    {
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.arr SET v[1] = 99 WHERE id = 1;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 99); // changed
        REQUIRE(v.children()[1].value<int32_t>() == 20); // untouched
        REQUIRE(v.children()[2].value<int32_t>() == 30); // untouched
    }
}

TEST_CASE("integration::list_array::fixed_array_element_types") {
    auto config = test_create_config("/tmp/test_list_array/element_types");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("boolean[] and double[] fixed arrays parse and accept literals");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.t (id bigint, flags boolean[3], vals double[2]);")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "INSERT INTO TestDatabase.t (id, flags, vals) VALUES (1, ARRAY[true,false,true], ARRAY[1.5,2.5]);")
                    ->is_success());
        auto cur = exec(dispatcher, "SELECT flags, vals FROM TestDatabase.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).children().size() == 3);
        REQUIRE(cur->value(1, 0).children().size() == 2);
        REQUIRE(cur->value(1, 0).children()[0].value<double>() == Catch::Approx(1.5));
    }
}

TEST_CASE("integration::list_array::variadic_list_crud") {
    auto config = test_create_config("/tmp/test_list_array/variadic_list_crud");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("DDL: a variadic int[] LIST column is accepted");
    { REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.l (id bigint, v int[]);")->is_success()); }

    INFO("INSERT accepts ARRAY[...] literals of different lengths into the same column");
    {
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (2, ARRAY[30,40,50]);")->is_success());
    }

    INFO("SELECT round-trips each row's list with its own length");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 1);

        auto row0 = cur->value(0, 0);
        REQUIRE(row0.children().size() == 2);
        REQUIRE(row0.children()[0].value<int32_t>() == 10);
        REQUIRE(row0.children()[1].value<int32_t>() == 20);

        auto row1 = cur->value(0, 1);
        REQUIRE(row1.children().size() == 3);
        REQUIRE(row1.children()[0].value<int32_t>() == 30);
        REQUIRE(row1.children()[2].value<int32_t>() == 50);
    }

    INFO("explicit projection of the list column round-trips identically");
    {
        auto cur = exec(dispatcher, "SELECT id, v FROM TestDatabase.l;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(1, 0).children().size() == 2);
        REQUIRE(cur->value(1, 1).children().size() == 3);
    }

    INFO("subscript READ v[i] on a LIST is 1-based, per-row element access");
    {
        auto cur = exec(dispatcher, "SELECT v[1], v[2] FROM TestDatabase.l;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->size() == 2);
        // row 0 == {10,20}
        REQUIRE(cur->value(0, 0).value<int32_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int32_t>() == 20);
        // row 1 == {30,40,50}
        REQUIRE(cur->value(0, 1).value<int32_t>() == 30);
        REQUIRE(cur->value(1, 1).value<int32_t>() == 40);
    }

    INFO("subscript UPDATE v[i] = x mutates a single LIST element in place");
    {
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.l SET v[3] = 99 WHERE id = 2;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 30); // untouched
        REQUIRE(v.children()[1].value<int32_t>() == 40); // untouched
        REQUIRE(v.children()[2].value<int32_t>() == 99); // changed
    }
}

TEST_CASE("integration::list_array::list_array_conversion") {
    auto config = test_create_config("/tmp/test_list_array/conversion");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.src_arr (id bigint, v int[3]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src_arr (id, v) VALUES (1, ARRAY[5,6,7]);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.src_list (id bigint, v int[]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src_list (id, v) VALUES (1, ARRAY[1,2,3]);")->is_success());

    INFO("ARRAY column value inserted into a variadic LIST column");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.dst_list (id bigint, v int[]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.dst_list (id, v) SELECT id, v FROM TestDatabase.src_arr;")
                    ->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst_list;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 5);
        REQUIRE(v.children()[1].value<int32_t>() == 6);
        REQUIRE(v.children()[2].value<int32_t>() == 7);
    }

    INFO("LIST column value inserted into a fixed ARRAY column");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.dst_arr (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.dst_arr (id, v) SELECT id, v FROM TestDatabase.src_list;")
                    ->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst_arr;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 1);
        REQUIRE(v.children()[2].value<int32_t>() == 3);
    }
}

TEST_CASE("integration::list_array::list_to_array_length") {
    auto config = test_create_config("/tmp/test_list_array/length_reconcile");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.src (id bigint, v int[]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src (id, v) VALUES (2, ARRAY[30,40,50]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src (id, v) VALUES (3, ARRAY[60,70,80,90]);")->is_success());

    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.dst (id bigint, v int[3]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.dst (id, v) SELECT id, v FROM TestDatabase.src;")->is_success());

    INFO("short list is padded to the array size; a nullable column with no default pads NULL");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 10);
        REQUIRE(v.children()[1].value<int32_t>() == 20);
        REQUIRE(v.children()[2].is_null()); // padded with NULL (no column default)
    }

    INFO("exact-length list maps element-for-element");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst WHERE id = 2;");
        REQUIRE(cur->is_success());
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 30);
        REQUIRE(v.children()[2].value<int32_t>() == 50);
    }

    INFO("over-long list is truncated to the array size");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst WHERE id = 3;");
        REQUIRE(cur->is_success());
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 60);
        REQUIRE(v.children()[1].value<int32_t>() == 70);
        REQUIRE(v.children()[2].value<int32_t>() == 80); // 90 dropped
    }
}

TEST_CASE("integration::list_array::empty_array_literal") {
    auto config = test_create_config("/tmp/test_list_array/empty_array");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("empty ARRAY[] into a variadic LIST column stores an empty list");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.l (id bigint, v int[]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (1, ARRAY[]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).children().size() == 0);
    }

    INFO("empty ARRAY[] into a nullable fixed int[3] column pads NULL to the array size");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.a (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.a (id, v) VALUES (1, ARRAY[]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.a WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].is_null());
        REQUIRE(v.children()[2].is_null());
    }
}

TEST_CASE("integration::list_array::array_default_padding") {
    auto config = test_create_config("/tmp/test_list_array/default_padding");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("VALUES short ARRAY into a nullable no-default column pads NULL");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.n (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.n (id, v) VALUES (1, ARRAY[7,8]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.n WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 7);
        REQUIRE(v.children()[1].value<int32_t>() == 8);
        REQUIRE(v.children()[2].is_null());
    }

    INFO("a column DEFAULT does not pad a short ARRAY: the missing slots are NULL");
    {
        REQUIRE(
            exec(dispatcher, "CREATE TABLE TestDatabase.d (id bigint, v int[3] DEFAULT ARRAY[1,2,3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.d (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.d WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 10); // provided
        REQUIRE(v.children()[1].value<int32_t>() == 20); // provided
        // A DEFAULT fills an ABSENT column, never the missing tail of a value that WAS supplied.
        // The array reconciles to the declared length by padding NULL either way.
        REQUIRE(v.children()[2].is_null());
    }

    INFO("NOT NULL column with no default: a too-short value is a clean error, not a silent drop");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.nn (id bigint, v int[3] NOT NULL);")->is_success());
        // The short value would reconcile by padding NULL, which the column does not allow. This
        // is rejected before the append (per-column validation in operator_check_constraint),
        // not silently dropped.
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.nn (id, v) VALUES (1, ARRAY[10,20]);");
        REQUIRE_FALSE(cur->is_success());
        auto sel = exec(dispatcher, "SELECT v FROM TestDatabase.nn;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 0); // nothing stored
    }

    INFO("a long-enough value into the NOT NULL column is stored (truncated)");
    {
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.nn (id, v) VALUES (2, ARRAY[1,2,3,4]);")->is_success());
        auto sel = exec(dispatcher, "SELECT v FROM TestDatabase.nn WHERE id = 2;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 1);
        auto v = sel->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[2].value<int32_t>() == 3); // 4 truncated
    }

    INFO("a NOT NULL column is not exempted by having a DEFAULT: the pad would still be NULL");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.nnd (id bigint, v int[3] NOT NULL DEFAULT ARRAY[1,2,3]);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.nnd (id) VALUES (1);")->is_success());
        {
            auto sel = exec(dispatcher, "SELECT v FROM TestDatabase.nnd WHERE id = 1;");
            REQUIRE(sel->is_success());
            auto v = sel->value(0, 0);
            REQUIRE(v.children().size() == 3);
            REQUIRE(v.children()[2].value<int32_t>() == 3);
        }
        REQUIRE_FALSE(exec(dispatcher, "INSERT INTO TestDatabase.nnd (id, v) VALUES (2, ARRAY[10,20]);")->is_success());
        auto sel = exec(dispatcher, "SELECT id FROM TestDatabase.nnd WHERE id = 2;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 0); // nothing stored
    }

    INFO("INSERT..SELECT of a too-short value into the NOT NULL column also errors (runtime path)");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.src2 (id bigint, v int[]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src2 (id, v) VALUES (5, ARRAY[1,2]);")->is_success());
        REQUIRE_FALSE(
            exec(dispatcher, "INSERT INTO TestDatabase.nn (id, v) SELECT id, v FROM TestDatabase.src2;")->is_success());
        auto sel = exec(dispatcher, "SELECT v FROM TestDatabase.nn WHERE id = 5;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 0); // nothing stored
    }
}

TEST_CASE("integration::list_array::subscript_in_where") {
    auto config = test_create_config("/tmp/test_list_array/subscript_where");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("fixed ARRAY: WHERE v[i] = x filters on a single element");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[10,20,30]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (2, ARRAY[40,20,60]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (3, ARRAY[70,80,90]);")->is_success());

        // v[2] == 20 matches rows 1 and 2, not row 3.
        auto cur = exec(dispatcher, "SELECT id FROM TestDatabase.arr WHERE v[2] = 20;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);

        // v[1] == 70 matches only row 3.
        auto cur2 = exec(dispatcher, "SELECT id FROM TestDatabase.arr WHERE v[1] = 70;");
        REQUIRE(cur2->is_success());
        REQUIRE(cur2->size() == 1);
        REQUIRE(cur2->value(0, 0).value<int64_t>() == 3);
    }

    INFO("variadic LIST: WHERE v[i] = x filters per-row by element");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.l (id bigint, v int[]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (2, ARRAY[30,40,50]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (3, ARRAY[20,99]);")->is_success());

        // v[1] == 10 matches only row 1.
        auto cur = exec(dispatcher, "SELECT id FROM TestDatabase.l WHERE v[1] = 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);

        // v[2] == 40 matches only row 2 (row 1's v[2]=20, row 3's v[2]=99).
        auto cur2 = exec(dispatcher, "SELECT id FROM TestDatabase.l WHERE v[2] = 40;");
        REQUIRE(cur2->is_success());
        REQUIRE(cur2->size() == 1);
        REQUIRE(cur2->value(0, 0).value<int64_t>() == 2);
    }
}

TEST_CASE("integration::list_array::unsupported_clean_failures") {
    auto config = test_create_config("/tmp/test_list_array/unsupported");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("ARRAY[...] with mixed element types is rejected at parse time");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[1, 'a']);");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(std::string(cur->get_error().what).find("inconsistent element types") != std::string::npos);
    }

    INFO("ARRAY value into a dynamic-schema (empty CREATE TABLE) table is rejected by design");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.dyn ();")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.dyn (id, tags) VALUES (1, ARRAY['a','b','c']);");
        REQUIRE_FALSE(cur->is_success());
        // error mentions complex types being unsupported on the dynamic path.
        REQUIRE(std::string(cur->get_error().what).find("complex types") != std::string::npos);
    }

    INFO("list/array scalar & aggregate functions are not registered");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.f (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.f (id, v) VALUES (1, ARRAY[1,2,3]);")->is_success());
        for (const char* fn : {"SELECT array_length(v) FROM TestDatabase.f;",
                               "SELECT len(v) FROM TestDatabase.f;",
                               "SELECT cardinality(v) FROM TestDatabase.f;",
                               "SELECT array_contains(v, 2) FROM TestDatabase.f;",
                               "SELECT unnest(v) FROM TestDatabase.f;",
                               "SELECT array_agg(id) FROM TestDatabase.f;"}) {
            auto cur = exec(dispatcher, fn);
            INFO("function call: " << fn);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(std::string(cur->get_error().what).find("unrecognized function") != std::string::npos);
        }
    }
}

TEST_CASE("integration::list_array::full_array_update") {
    auto config = test_create_config("/tmp/test_list_array/full_array_update");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("fixed ARRAY: UPDATE v = ARRAY[...] replaces the whole array, casting widths");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[10,20,30]);")->is_success());

        // ARRAY[7,8,9] are BIGINT literals written into an INTEGER[3] column.
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.arr SET v = ARRAY[7,8,9] WHERE id = 1;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 7);
        REQUIRE(v.children()[1].value<int32_t>() == 8);
        REQUIRE(v.children()[2].value<int32_t>() == 9);
    }

    INFO("fixed ARRAY: UPDATE v = ARRAY[...] with a same-width literal round-trips");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr2 (id bigint, v int[2]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr2 (id, v) VALUES (1, ARRAY[1,2]);")->is_success());
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.arr2 SET v = ARRAY[5,6] WHERE id = 1;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr2 WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 2);
        REQUIRE(v.children()[0].value<int32_t>() == 5);
        REQUIRE(v.children()[1].value<int32_t>() == 6);
    }
}

TEST_CASE("integration::list_array::full_list_update") {
    auto config = test_create_config("/tmp/test_list_array/full_list_update");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.l (id bigint, v int[]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (2, ARRAY[30,40,50]);")->is_success());

    INFO("grow: a len-2 list is replaced by a len-3 list, casting wider element literals");
    {
        // ARRAY[100,200,300] are BIGINT literals into an INTEGER[] list column.
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.l SET v = ARRAY[100,200,300] WHERE id = 1;")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 100);
        REQUIRE(v.children()[1].value<int32_t>() == 200);
        REQUIRE(v.children()[2].value<int32_t>() == 300);
    }

    INFO("shrink: a len-3 list is replaced by a len-1 list");
    {
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.l SET v = ARRAY[7] WHERE id = 2;")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 1);
        REQUIRE(v.children()[0].value<int32_t>() == 7);
    }

    INFO("the untouched row keeps its original list; the grown row is unchanged by the second update");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 100);
        REQUIRE(v.children()[2].value<int32_t>() == 300);
    }
}
TEST_CASE("integration::list_array::null_array_value_reads_safely") {
    auto config = test_create_config("/tmp/test_list_array/null_array_value");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[7,8,9]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (2, NULL);")->is_success());

    INFO("a NULL array row is readable through the children() idiom");
    {
        auto cur = exec(dispatcher, "SELECT id, v FROM TestDatabase.arr ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto full = cur->value(1, 0);
        REQUIRE_FALSE(full.is_null());
        REQUIRE(full.children().size() == 3);
        REQUIRE(full.children()[0].value<int32_t>() == 7);
        // Regression: children() on the NULL value dereferenced the null
        // payload pointer and crashed the process.
        auto null_value = cur->value(1, 1);
        REQUIRE(null_value.is_null());
        CHECK(null_value.children().empty());
    }
}

// Automates the SQL battery from the logical_value_t::children() fix: with a
// NULL array row present, every one of these read/scan/aggregate/DML statements
// must complete cleanly (the engine's own call sites already guard is_null()),
// and a NULL row's value read through the children() idiom must be empty rather
// than crashing the process.
TEST_CASE("integration::list_array::null_array_sql_operations_clean") {
    auto config = test_create_config("/tmp/test_list_array/null_array_sql_ops");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.a (id bigint, v int[3]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.a (id, v) VALUES (1, ARRAY[7,8,9]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.a (id, v) VALUES (2, NULL);")->is_success());

    auto ok = [&](const std::string& sql) {
        INFO(sql);
        return exec(dispatcher, sql);
    };

    SECTION("reads and scans over the NULL array row") {
        {
            auto cur = ok("SELECT v FROM db.a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2); // both rows returned, incl. the NULL one
        }
        {
            auto cur = ok("SELECT v[1] FROM db.a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto cur = ok("SELECT id FROM db.a WHERE v = ARRAY[7,8,9];");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1); // only the non-NULL row matches
        }
        // NB: `WHERE v = v` (whole-array self-comparison) is intentionally not
        // covered here — array = array is not a supported filter shape and is a
        // separate concern from the children() NULL fix.
        REQUIRE(ok("SELECT id FROM db.a ORDER BY v;")->is_success());
        REQUIRE(ok("SELECT v, COUNT(*) FROM db.a GROUP BY v;")->is_success());
        REQUIRE(ok("SELECT DISTINCT v FROM db.a;")->is_success());
    }

    SECTION("DML touching the NULL array row") {
        REQUIRE(ok("UPDATE db.a SET v[1] = 5 WHERE id = 1;")->is_success());
        REQUIRE(ok("UPDATE db.a SET v = ARRAY[1,2,3] WHERE id = 2;")->is_success());
        {
            auto cur = ok("INSERT INTO db.a (id, v) VALUES (3, NULL) RETURNING v;");
            REQUIRE(cur->is_success());
            // RETURNING a NULL array must read back safely through children().
            REQUIRE(cur->size() == 1);
            auto returned = cur->value(0, 0);
            REQUIRE(returned.is_null());
            CHECK(returned.children().empty());
        }
    }
}

// #563/#559: `col = ARRAY(SELECT ...)` is real, length-aware array equality — the
// sub-query rows are compacted into an array and compared to the column. A different-length result is
// simply unequal (never truncated/padded), and an empty sub-query yields a real empty array {} (unequal
// to a non-empty column), not the NA-null sentinel.
TEST_CASE("integration::list_array::array_equality_subquery") {
    auto config = test_create_config("/tmp/test_list_array/array_eq_subquery");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.a (id bigint, v int[3]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.a (id, v) VALUES (1, ARRAY[7,8,9]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.a (id, v) VALUES (2, ARRAY[1,2,3]);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.src (x int);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.src (x) VALUES (7), (8), (9);")->is_success());

    auto ok = [&](const std::string& sql) {
        INFO(sql);
        return exec(dispatcher, sql);
    };

    SECTION("matching length and values") {
        auto cur = ok("SELECT id FROM db.a WHERE v = ARRAY(SELECT x FROM db.src);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1); // only row 1 ([7,8,9]) equals the sub-query array [7,8,9]
    }
    SECTION("different length is unequal (no truncate/pad)") {
        auto cur = ok("SELECT id FROM db.a WHERE v = ARRAY(SELECT x FROM db.src WHERE x < 9);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0); // sub-query yields [7,8] (length 2) != any int[3]
    }
    SECTION("empty sub-query is a real empty array, unequal to a non-empty column") {
        auto cur = ok("SELECT id FROM db.a WHERE v = ARRAY(SELECT x FROM db.src WHERE x > 100);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0); // {} != [7,8,9] and != [1,2,3]
    }
}

// A NULL element inside an ARRAY/LIST literal must be storable and must survive the round trip as a
// NULL element (validity mask honored), for both fixed ARRAY[n] and variable-length LIST columns.
TEST_CASE("integration::list_array::null_elements") {
    auto config = test_create_config("/tmp/test_list_array/null_elements");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };

    ok("CREATE DATABASE db;");
    ok("CREATE TABLE db.a (id int, arr int[3], lst int[]);");
    ok("INSERT INTO db.a (id, arr, lst) VALUES (1, ARRAY[10,20,30], ARRAY[10,20,30]);");
    // A row whose ARRAY and LIST both carry a NULL middle element.
    ok("INSERT INTO db.a (id, arr, lst) VALUES (2, ARRAY[10,NULL,30], ARRAY[10,NULL,30]);");

    SECTION("whole ARRAY column round-trips the NULL element") {
        auto cur = ok("SELECT arr FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 10);
        REQUIRE(v.children()[1].is_null());
        REQUIRE(v.children()[2].value<int32_t>() == 30);
    }
    SECTION("whole LIST column round-trips the NULL element") {
        auto cur = ok("SELECT lst FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        REQUIRE(v.children().size() == 3);
        REQUIRE(v.children()[0].value<int32_t>() == 10);
        REQUIRE(v.children()[1].is_null());
        REQUIRE(v.children()[2].value<int32_t>() == 30);
    }
    SECTION("subscript of a NULL element yields NULL (ARRAY and LIST)") {
        auto cur = ok("SELECT arr[2], lst[2] FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).is_null());
        REQUIRE(cur->value(1, 0).is_null());
    }
    SECTION("subscript of a non-NULL element is unaffected") {
        auto cur = ok("SELECT arr[1], lst[3] FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int32_t>() == 30);
    }
    SECTION("a NULL element does not match an equality filter on that position") {
        // row 2's arr[2]/lst[2] are NULL, so only row 1 (value 20) matches.
        auto cur = ok("SELECT id FROM db.a WHERE arr[2] = 20;");
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
        auto cur2 = ok("SELECT id FROM db.a WHERE lst[2] = 20;");
        REQUIRE(cur2->size() == 1);
        REQUIRE(cur2->value(0, 0).value<int32_t>() == 1);
    }
    SECTION("IS NULL / IS NOT NULL on a subscript") {
        // row 1's arr[2]=20 (not null), row 2's arr[2]=NULL.
        auto isnull = ok("SELECT id FROM db.a WHERE arr[2] IS NULL;");
        REQUIRE(isnull->size() == 1);
        REQUIRE(isnull->value(0, 0).value<int32_t>() == 2);
        auto notnull = ok("SELECT id FROM db.a WHERE lst[2] IS NOT NULL;");
        REQUIRE(notnull->size() == 1);
        REQUIRE(notnull->value(0, 0).value<int32_t>() == 1);
    }
    SECTION("comparing whole arrays with a NULL element still answers true or false") {
        // A NULL ELEMENT is not a NULL array. Whole containers order totally: a NULL element
        // sorts after every value and two NULLs in the same place are equal, so the row gets a
        // definite answer. Matches PostgreSQL, where `arr = ARRAY[10,NULL,30]` selects the row
        // and `arr > ARRAY[10,20,30]` selects it too.
        auto same = ok("SELECT id FROM db.a WHERE lst = ARRAY[10,NULL,30];");
        REQUIRE(same->size() == 1);
        REQUIRE(same->value(0, 0).value<int32_t>() == 2);
        auto after = ok("SELECT id FROM db.a WHERE lst > ARRAY[10,20,30];");
        REQUIRE(after->size() == 1);
        REQUIRE(after->value(0, 0).value<int32_t>() == 2);
    }
    SECTION("ORDER BY a subscript orders by the element, NULLs last") {
        // arr[2] is 20 (id 1) and NULL (id 2): ascending places 20 first, the NULL last.
        auto cur = ok("SELECT id FROM db.a ORDER BY arr[2];");
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
        REQUIRE(cur->value(0, 1).value<int32_t>() == 2);
        // Explicit NULLS FIRST places the NULL row (id 2) first.
        auto nf = ok("SELECT id FROM db.a ORDER BY lst[2] NULLS FIRST;");
        REQUIRE(nf->size() == 2);
        REQUIRE(nf->value(0, 0).value<int32_t>() == 2);
        REQUIRE(nf->value(0, 1).value<int32_t>() == 1);
    }
}

// MIN/MAX over a whole array is the ordinary reduction over the vector's rows — it picks one
// winning row out of many, exactly as over a number. What differs is only the comparison:
// lexicographic over the shared prefix, then by length, with a NULL element sorting last.
TEST_CASE("integration::list_array::min_max_over_whole_array") {
    auto config = test_create_config("/tmp/test_list_array/min_max_over_whole_array");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };

    ok("CREATE DATABASE db;");
    ok("CREATE TABLE db.l (id int, grp int, v int[]);");
    ok("INSERT INTO db.l (id, grp, v) VALUES (1, 1, ARRAY[1,2]);");
    ok("INSERT INTO db.l (id, grp, v) VALUES (2, 1, ARRAY[1,2,3]);");
    ok("INSERT INTO db.l (id, grp, v) VALUES (3, 2, ARRAY[0,9]);");

    SECTION("the winner is a whole list, not a scalar drawn from inside one") {
        auto cur = ok("SELECT max(v) FROM db.l;");
        REQUIRE(cur->size() == 1);
        auto winner = cur->value(0, 0);
        REQUIRE(winner.children().size() == 3);
        REQUIRE(winner.children()[0].value<int32_t>() == 1);
        REQUIRE(winner.children()[1].value<int32_t>() == 2);
        REQUIRE(winner.children()[2].value<int32_t>() == 3);
    }
    SECTION("min picks the row that is smallest at the first element that differs") {
        auto cur = ok("SELECT min(v) FROM db.l;");
        REQUIRE(cur->size() == 1);
        auto winner = cur->value(0, 0);
        REQUIRE(winner.children().size() == 2);
        REQUIRE(winner.children()[0].value<int32_t>() == 0);
        REQUIRE(winner.children()[1].value<int32_t>() == 9);
    }
    SECTION("a shared prefix is broken by length: the longer list is the greater one") {
        auto cur = ok("SELECT max(v) FROM db.l WHERE grp = 1;");
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).children().size() == 3);
        auto shorter = ok("SELECT min(v) FROM db.l WHERE grp = 1;");
        REQUIRE(shorter->value(0, 0).children().size() == 2);
    }
    SECTION("each group reduces to its own winner") {
        auto cur = ok("SELECT grp, max(v) FROM db.l GROUP BY grp ORDER BY grp;");
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
        REQUIRE(cur->value(1, 0).children()[2].value<int32_t>() == 3); // {1,2,3}
        REQUIRE(cur->value(0, 1).value<int32_t>() == 2);
        REQUIRE(cur->value(1, 1).children()[0].value<int32_t>() == 0); // {0,9}
    }
    SECTION("a constant array argument reduces to that array") {
        auto cur = ok("SELECT max(ARRAY[7,8,9]) FROM db.l;");
        REQUIRE(cur->size() == 1);
        auto winner = cur->value(0, 0);
        REQUIRE(winner.children().size() == 3);
        REQUIRE(winner.children()[0].value<int32_t>() == 7);
        REQUIRE(winner.children()[2].value<int32_t>() == 9);
    }
    SECTION("NULL rows are skipped, and only NULL rows give a NULL result") {
        ok("INSERT INTO db.l (id, grp, v) VALUES (4, 3, NULL);");
        auto skipped = ok("SELECT max(v) FROM db.l WHERE grp = 2 OR grp = 3;");
        REQUIRE(skipped->size() == 1);
        REQUIRE(skipped->value(0, 0).children().size() == 2); // {0,9}, the NULL row ignored
        auto empty = ok("SELECT max(v) FROM db.l WHERE grp = 3;");
        REQUIRE(empty->size() == 1);
        REQUIRE(empty->value(0, 0).is_null());
    }
    SECTION("a NULL element sorts after every value, so it wins max and loses min") {
        ok("CREATE TABLE db.n (id int, v int[]);");
        ok("INSERT INTO db.n (id, v) VALUES (1, ARRAY[1,NULL]);");
        ok("INSERT INTO db.n (id, v) VALUES (2, ARRAY[1,2]);");

        auto largest = ok("SELECT max(v) FROM db.n;");
        REQUIRE(largest->value(0, 0).children()[1].is_null());
        auto smallest = ok("SELECT min(v) FROM db.n;");
        REQUIRE(smallest->value(0, 0).children()[1].value<int32_t>() == 2);
    }
}

// The same reduction over a fixed-width ARRAY column, where every row is the same length and
// only the elements can break the tie.
TEST_CASE("integration::list_array::min_max_over_fixed_array") {
    auto config = test_create_config("/tmp/test_list_array/min_max_over_fixed_array");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };

    ok("CREATE DATABASE db;");
    ok("CREATE TABLE db.a (id int, v int[3]);");
    ok("INSERT INTO db.a (id, v) VALUES (1, ARRAY[1,2,3]);");
    ok("INSERT INTO db.a (id, v) VALUES (2, ARRAY[1,5,0]);");
    ok("INSERT INTO db.a (id, v) VALUES (3, ARRAY[1,2,9]);");

    SECTION("max compares element by element") {
        auto cur = ok("SELECT max(v) FROM db.a;");
        REQUIRE(cur->size() == 1);
        auto winner = cur->value(0, 0);
        REQUIRE(winner.children().size() == 3);
        REQUIRE(winner.children()[1].value<int32_t>() == 5); // {1,5,0} beats both {1,2,*}
        REQUIRE(winner.children()[2].value<int32_t>() == 0);
    }
    SECTION("min falls through to the last element when the prefix ties") {
        auto cur = ok("SELECT min(v) FROM db.a;");
        REQUIRE(cur->size() == 1);
        auto winner = cur->value(0, 0);
        REQUIRE(winner.children()[1].value<int32_t>() == 2);
        REQUIRE(winner.children()[2].value<int32_t>() == 3); // {1,2,3} < {1,2,9}
    }
}

// Casting a whole array to text renders it the way an array literal reads back: the elements
// comma-separated inside braces, a NULL element as the bare word NULL, and an element quoted
// only when leaving it bare would change what it says.
TEST_CASE("integration::list_array::cast_array_to_text") {
    auto config = test_create_config("/tmp/test_list_array/cast_array_to_text");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };
    // Copied out: the view points into the cursor, which dies with this call.
    auto text_of = [&](const std::string& expression) -> std::string {
        auto cur = ok("SELECT CAST(" + expression + " AS TEXT) FROM db.one;");
        REQUIRE(cur->size() == 1);
        return std::string{cur->value(0, 0).value<std::string_view>()};
    };

    ok("CREATE DATABASE db;");
    ok("CREATE TABLE db.one (id int);");
    ok("INSERT INTO db.one (id) VALUES (1);");

    SECTION("an array literal renders braced and comma-separated") { REQUIRE(text_of("ARRAY[1,2,3]") == "{1,2,3}"); }
    SECTION("a one-element array keeps its braces") { REQUIRE(text_of("ARRAY[7]") == "{7}"); }
    SECTION("a NULL element is the bare word NULL, not an empty slot") {
        REQUIRE(text_of("ARRAY[1,NULL,3]") == "{1,NULL,3}");
    }
    SECTION("a nested array renders as nested braces, unquoted") {
        REQUIRE(text_of("ARRAY[ARRAY[1,2],ARRAY[3,4]]") == "{{1,2},{3,4}}");
    }
    SECTION("plain text elements need no quoting") { REQUIRE(text_of("ARRAY['a','b']") == "{a,b}"); }
    SECTION("an element is quoted when a bare one would read as something else") {
        // A space or a comma would break the element apart, a quote or backslash is escaped,
        // an empty element would vanish, and a bare NULL would read as the null marker.
        REQUIRE(text_of("ARRAY['a b']") == "{\"a b\"}");
        REQUIRE(text_of("ARRAY['c,d']") == "{\"c,d\"}");
        REQUIRE(text_of("ARRAY['e\"f']") == "{\"e\\\"f\"}");
        REQUIRE(text_of("ARRAY['']") == "{\"\"}");
        REQUIRE(text_of("ARRAY['NULL']") == "{\"NULL\"}");
        REQUIRE(text_of("ARRAY['{a}']") == "{\"{a}\"}");
    }

    SECTION("a stored column casts the same way, row by row") {
        ok("CREATE TABLE db.l (id int, v int[]);");
        ok("INSERT INTO db.l (id, v) VALUES (1, ARRAY[10,20]);");
        ok("INSERT INTO db.l (id, v) VALUES (2, ARRAY[30,40,50]);");
        ok("INSERT INTO db.l (id, v) VALUES (3, NULL);");

        auto cur = ok("SELECT CAST(v AS TEXT) FROM db.l ORDER BY id;");
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "{10,20}");
        REQUIRE(cur->value(0, 1).value<std::string_view>() == "{30,40,50}");
        REQUIRE(cur->value(0, 2).is_null()); // a NULL array is a NULL text, not the string "{}"
    }
    SECTION("a fixed-width ARRAY column renders its full width") {
        ok("CREATE TABLE db.a (id int, v int[3]);");
        ok("INSERT INTO db.a (id, v) VALUES (1, ARRAY[1,2,3]);");
        auto cur = ok("SELECT CAST(v AS TEXT) FROM db.a;");
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "{1,2,3}");
    }
    SECTION("the rendered text is what lands in a TEXT column assigned from an array") {
        ok("CREATE TABLE db.t (id int, name text);");
        ok("INSERT INTO db.t (id, name) VALUES (1, 'before');");
        ok("UPDATE db.t SET name = CAST(ARRAY[1,2,3] AS TEXT) WHERE id = 1;");
        auto cur = ok("SELECT name FROM db.t WHERE id = 1;");
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "{1,2,3}");
    }
}
