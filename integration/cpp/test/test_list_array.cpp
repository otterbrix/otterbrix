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

    INFO("short ARRAY into a column with a DEFAULT pads from the default at that position");
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
        REQUIRE(v.children()[2].value<int32_t>() == 3);  // from DEFAULT[2]
    }

    INFO("NOT NULL column with no default: a too-short value is a clean error, not a silent drop");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.nn (id bigint, v int[3] NOT NULL);")->is_success());
        // The short value cannot fill the fixed array and there is no column default to pad
        // from. This is rejected with an error before the append (per-column validation in
        // operator_check_constraint), not silently dropped.
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.nn (id, v) VALUES (1, ARRAY[10,20]);");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(std::string(cur->get_error().what).find("array column 'v'") != std::string::npos);
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

    INFO("INSERT..SELECT of a too-short value into the NOT NULL column also errors (runtime path)");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.src2 (id bigint, v int[]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src2 (id, v) VALUES (5, ARRAY[1,2]);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.nn (id, v) SELECT id, v FROM TestDatabase.src2;");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(std::string(cur->get_error().what).find("array column 'v'") != std::string::npos);
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
            REQUIRE(std::string(cur->get_error().what).find("was not found") != std::string::npos);
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
