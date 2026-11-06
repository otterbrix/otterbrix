#include "integration_fixture_path.hpp"
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
    auto config = test_create_config(integration_fixture_path("test_list_array/fixed_array_crud"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

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

        auto cell0 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell0.has_error());
        REQUIRE(cell0.value() == 3);
        auto cell1 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value() == 10);
        auto cell2 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value() == 20);
        auto cell3 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == 30);

        auto cell4 = cur->list_size(0, 1);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == 3);
        auto cell5 = cur->get<int32_t>(0, 1, {0});
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == 40);
        auto cell6 = cur->get<int32_t>(0, 1, {2});
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == 60);
    }

    INFO("subscript READ v[i] is 1-based and projects one scalar column per index");
    {
        auto cur = exec(dispatcher, "SELECT v[1], v[2], v[3] FROM TestDatabase.arr;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 3);
        REQUIRE(cur->size() == 2);
        // row 0 == {10,20,30}
        auto cell7 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell7.has_error());
        REQUIRE(cell7.value() == 10);
        auto cell8 = cur->get<int32_t>(1, 0);
        REQUIRE_FALSE(cell8.has_error());
        REQUIRE(cell8.value() == 20);
        auto cell9 = cur->get<int32_t>(2, 0);
        REQUIRE_FALSE(cell9.has_error());
        REQUIRE(cell9.value() == 30);
        // row 1 == {40,50,60}
        auto cell10 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell10.has_error());
        REQUIRE(cell10.value() == 40);
        auto cell11 = cur->get<int32_t>(2, 1);
        REQUIRE_FALSE(cell11.has_error());
        REQUIRE(cell11.value() == 60);
    }

    INFO("subscript UPDATE v[i] = x mutates a single element in place");
    {
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.arr SET v[1] = 99 WHERE id = 1;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell12 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell12.has_error());
        REQUIRE(cell12.value() == 3);
        auto cell13 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell13.has_error());
        REQUIRE(cell13.value() == 99); // changed
        auto cell14 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell14.has_error());
        REQUIRE(cell14.value() == 20); // untouched
        auto cell15 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell15.has_error());
        REQUIRE(cell15.value() == 30); // untouched
    }
}

TEST_CASE("integration::list_array::fixed_array_element_types") {
    auto config = test_create_config(integration_fixture_path("test_list_array/element_types"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

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
        auto cell16 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell16.has_error());
        REQUIRE(cell16.value() == 3);
        auto cell17 = cur->list_size(1, 0);
        REQUIRE_FALSE(cell17.has_error());
        REQUIRE(cell17.value() == 2);
        auto cell18 = cur->get<double>(1, 0, {0});
        REQUIRE_FALSE(cell18.has_error());
        REQUIRE(cell18.value() == Catch::Approx(1.5));
    }
}

TEST_CASE("integration::list_array::variadic_list_crud") {
    auto config = test_create_config(integration_fixture_path("test_list_array/variadic_list_crud"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

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

        auto cell19 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell19.has_error());
        REQUIRE(cell19.value() == 2);
        auto cell20 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell20.has_error());
        REQUIRE(cell20.value() == 10);
        auto cell21 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell21.has_error());
        REQUIRE(cell21.value() == 20);

        auto cell22 = cur->list_size(0, 1);
        REQUIRE_FALSE(cell22.has_error());
        REQUIRE(cell22.value() == 3);
        auto cell23 = cur->get<int32_t>(0, 1, {0});
        REQUIRE_FALSE(cell23.has_error());
        REQUIRE(cell23.value() == 30);
        auto cell24 = cur->get<int32_t>(0, 1, {2});
        REQUIRE_FALSE(cell24.has_error());
        REQUIRE(cell24.value() == 50);
    }

    INFO("explicit projection of the list column round-trips identically");
    {
        auto cur = exec(dispatcher, "SELECT id, v FROM TestDatabase.l;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->size() == 2);
        auto cell25 = cur->list_size(1, 0);
        REQUIRE_FALSE(cell25.has_error());
        REQUIRE(cell25.value() == 2);
        auto cell26 = cur->list_size(1, 1);
        REQUIRE_FALSE(cell26.has_error());
        REQUIRE(cell26.value() == 3);
    }

    INFO("subscript READ v[i] on a LIST is 1-based, per-row element access");
    {
        auto cur = exec(dispatcher, "SELECT v[1], v[2] FROM TestDatabase.l;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->size() == 2);
        // row 0 == {10,20}
        auto cell27 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell27.has_error());
        REQUIRE(cell27.value() == 10);
        auto cell28 = cur->get<int32_t>(1, 0);
        REQUIRE_FALSE(cell28.has_error());
        REQUIRE(cell28.value() == 20);
        // row 1 == {30,40,50}
        auto cell29 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell29.has_error());
        REQUIRE(cell29.value() == 30);
        auto cell30 = cur->get<int32_t>(1, 1);
        REQUIRE_FALSE(cell30.has_error());
        REQUIRE(cell30.value() == 40);
    }

    INFO("subscript UPDATE v[i] = x mutates a single LIST element in place");
    {
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.l SET v[3] = 99 WHERE id = 2;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell31 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell31.has_error());
        REQUIRE(cell31.value() == 3);
        auto cell32 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell32.has_error());
        REQUIRE(cell32.value() == 30); // untouched
        auto cell33 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell33.has_error());
        REQUIRE(cell33.value() == 40); // untouched
        auto cell34 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell34.has_error());
        REQUIRE(cell34.value() == 99); // changed
    }
}

TEST_CASE("integration::list_array::list_array_conversion") {
    auto config = test_create_config(integration_fixture_path("test_list_array/conversion"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());
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
        auto cell35 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell35.has_error());
        REQUIRE(cell35.value() == 3);
        auto cell36 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell36.has_error());
        REQUIRE(cell36.value() == 5);
        auto cell37 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell37.has_error());
        REQUIRE(cell37.value() == 6);
        auto cell38 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell38.has_error());
        REQUIRE(cell38.value() == 7);
    }

    INFO("LIST column value inserted into a fixed ARRAY column");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.dst_arr (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.dst_arr (id, v) SELECT id, v FROM TestDatabase.src_list;")
                    ->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst_arr;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell39 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell39.has_error());
        REQUIRE(cell39.value() == 3);
        auto cell40 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell40.has_error());
        REQUIRE(cell40.value() == 1);
        auto cell41 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell41.has_error());
        REQUIRE(cell41.value() == 3);
    }
}

TEST_CASE("integration::list_array::list_to_array_length") {
    auto config = test_create_config(integration_fixture_path("test_list_array/length_reconcile"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());
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
        auto cell42 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell42.has_error());
        REQUIRE(cell42.value() == 3);
        auto cell43 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell43.has_error());
        REQUIRE(cell43.value() == 10);
        auto cell44 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell44.has_error());
        REQUIRE(cell44.value() == 20);
        auto cell45 = cur->is_null(0, 0, {2});
        REQUIRE_FALSE(cell45.has_error());
        REQUIRE(cell45.value()); // padded with NULL (no column default)
    }

    INFO("exact-length list maps element-for-element");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst WHERE id = 2;");
        REQUIRE(cur->is_success());
        auto cell46 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell46.has_error());
        REQUIRE(cell46.value() == 3);
        auto cell47 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell47.has_error());
        REQUIRE(cell47.value() == 30);
        auto cell48 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell48.has_error());
        REQUIRE(cell48.value() == 50);
    }

    INFO("over-long list is truncated to the array size");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.dst WHERE id = 3;");
        REQUIRE(cur->is_success());
        auto cell49 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell49.has_error());
        REQUIRE(cell49.value() == 3);
        auto cell50 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell50.has_error());
        REQUIRE(cell50.value() == 60);
        auto cell51 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell51.has_error());
        REQUIRE(cell51.value() == 70);
        auto cell52 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell52.has_error());
        REQUIRE(cell52.value() == 80); // 90 dropped
    }
}

TEST_CASE("integration::list_array::empty_array_literal") {
    auto config = test_create_config(integration_fixture_path("test_list_array/empty_array"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

    INFO("empty ARRAY[] into a variadic LIST column stores an empty list");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.l (id bigint, v int[]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.l (id, v) VALUES (1, ARRAY[]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell53 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell53.has_error());
        REQUIRE(cell53.value() == 0);
    }

    INFO("empty ARRAY[] into a nullable fixed int[3] column pads NULL to the array size");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.a (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.a (id, v) VALUES (1, ARRAY[]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.a WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell54 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell54.has_error());
        REQUIRE(cell54.value() == 3);
        auto cell55 = cur->is_null(0, 0, {0});
        REQUIRE_FALSE(cell55.has_error());
        REQUIRE(cell55.value());
        auto cell56 = cur->is_null(0, 0, {2});
        REQUIRE_FALSE(cell56.has_error());
        REQUIRE(cell56.value());
    }
}

TEST_CASE("integration::list_array::array_default_padding") {
    auto config = test_create_config(integration_fixture_path("test_list_array/default_padding"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

    INFO("VALUES short ARRAY into a nullable no-default column pads NULL");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.n (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.n (id, v) VALUES (1, ARRAY[7,8]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.n WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto cell57 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell57.has_error());
        REQUIRE(cell57.value() == 3);
        auto cell58 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell58.has_error());
        REQUIRE(cell58.value() == 7);
        auto cell59 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell59.has_error());
        REQUIRE(cell59.value() == 8);
        auto cell60 = cur->is_null(0, 0, {2});
        REQUIRE_FALSE(cell60.has_error());
        REQUIRE(cell60.value());
    }

    INFO("a column DEFAULT does not pad a short ARRAY: the missing slots are NULL");
    {
        REQUIRE(
            exec(dispatcher, "CREATE TABLE TestDatabase.d (id bigint, v int[3] DEFAULT ARRAY[1,2,3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.d (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.d WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto cell61 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell61.has_error());
        REQUIRE(cell61.value() == 3);
        auto cell62 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell62.has_error());
        REQUIRE(cell62.value() == 10); // provided
        auto cell63 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell63.has_error());
        REQUIRE(cell63.value() == 20); // provided
        // A DEFAULT fills an ABSENT column, never the missing tail of a value that WAS supplied.
        // The array reconciles to the declared length by padding NULL either way.
        auto cell64 = cur->is_null(0, 0, {2});
        REQUIRE_FALSE(cell64.has_error());
        REQUIRE(cell64.value());
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
        auto cell65 = sel->list_size(0, 0);
        REQUIRE_FALSE(cell65.has_error());
        REQUIRE(cell65.value() == 3);
        auto cell66 = sel->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell66.has_error());
        REQUIRE(cell66.value() == 3); // 4 truncated
    }

    INFO("a NOT NULL column is not exempted by having a DEFAULT: the pad would still be NULL");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.nnd (id bigint, v int[3] NOT NULL DEFAULT ARRAY[1,2,3]);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.nnd (id) VALUES (1);")->is_success());
        {
            auto sel = exec(dispatcher, "SELECT v FROM TestDatabase.nnd WHERE id = 1;");
            REQUIRE(sel->is_success());
            auto cell67 = sel->list_size(0, 0);
            REQUIRE_FALSE(cell67.has_error());
            REQUIRE(cell67.value() == 3);
            auto cell68 = sel->get<int32_t>(0, 0, {2});
            REQUIRE_FALSE(cell68.has_error());
            REQUIRE(cell68.value() == 3);
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
    auto config = test_create_config(integration_fixture_path("test_list_array/subscript_where"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

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
        auto cell69 = cur2->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell69.has_error());
        REQUIRE(cell69.value() == 3);
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
        auto cell70 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell70.has_error());
        REQUIRE(cell70.value() == 1);

        // v[2] == 40 matches only row 2 (row 1's v[2]=20, row 3's v[2]=99).
        auto cur2 = exec(dispatcher, "SELECT id FROM TestDatabase.l WHERE v[2] = 40;");
        REQUIRE(cur2->is_success());
        REQUIRE(cur2->size() == 1);
        auto cell71 = cur2->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell71.has_error());
        REQUIRE(cell71.value() == 2);
    }
}

TEST_CASE("integration::list_array::unsupported_clean_failures") {
    auto config = test_create_config(integration_fixture_path("test_list_array/unsupported"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

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
    auto config = test_create_config(integration_fixture_path("test_list_array/full_array_update"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());

    INFO("fixed ARRAY: UPDATE v = ARRAY[...] replaces the whole array, casting widths");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[10,20,30]);")->is_success());

        // ARRAY[7,8,9] are BIGINT literals written into an INTEGER[3] column.
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.arr SET v = ARRAY[7,8,9] WHERE id = 1;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell72 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell72.has_error());
        REQUIRE(cell72.value() == 3);
        auto cell73 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell73.has_error());
        REQUIRE(cell73.value() == 7);
        auto cell74 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell74.has_error());
        REQUIRE(cell74.value() == 8);
        auto cell75 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell75.has_error());
        REQUIRE(cell75.value() == 9);
    }

    INFO("fixed ARRAY: UPDATE v = ARRAY[...] with a same-width literal round-trips");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr2 (id bigint, v int[2]);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr2 (id, v) VALUES (1, ARRAY[1,2]);")->is_success());
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.arr2 SET v = ARRAY[5,6] WHERE id = 1;")->is_success());

        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.arr2 WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto cell76 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell76.has_error());
        REQUIRE(cell76.value() == 2);
        auto cell77 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell77.has_error());
        REQUIRE(cell77.value() == 5);
        auto cell78 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell78.has_error());
        REQUIRE(cell78.value() == 6);
    }
}

TEST_CASE("integration::list_array::full_list_update") {
    auto config = test_create_config(integration_fixture_path("test_list_array/full_list_update"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());
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
        auto cell79 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell79.has_error());
        REQUIRE(cell79.value() == 3);
        auto cell80 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell80.has_error());
        REQUIRE(cell80.value() == 100);
        auto cell81 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell81.has_error());
        REQUIRE(cell81.value() == 200);
        auto cell82 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell82.has_error());
        REQUIRE(cell82.value() == 300);
    }

    INFO("shrink: a len-3 list is replaced by a len-1 list");
    {
        REQUIRE(exec(dispatcher, "UPDATE TestDatabase.l SET v = ARRAY[7] WHERE id = 2;")->is_success());
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell83 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell83.has_error());
        REQUIRE(cell83.value() == 1);
        auto cell84 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell84.has_error());
        REQUIRE(cell84.value() == 7);
    }

    INFO("the untouched row keeps its original list; the grown row is unchanged by the second update");
    {
        auto cur = exec(dispatcher, "SELECT v FROM TestDatabase.l WHERE id = 1;");
        REQUIRE(cur->is_success());
        auto cell85 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell85.has_error());
        REQUIRE(cell85.value() == 3);
        auto cell86 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell86.has_error());
        REQUIRE(cell86.value() == 100);
        auto cell87 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell87.has_error());
        REQUIRE(cell87.value() == 300);
    }
}
TEST_CASE("integration::list_array::null_array_value_reads_safely") {
    auto config = test_create_config(integration_fixture_path("test_list_array/null_array_value"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.arr (id bigint, v int[3]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (1, ARRAY[7,8,9]);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.arr (id, v) VALUES (2, NULL);")->is_success());

    INFO("a NULL array row reads back as NULL");
    {
        auto cur = exec(dispatcher, "SELECT id, v FROM TestDatabase.arr ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell88 = cur->is_null(1, 0);
        REQUIRE_FALSE(cell88.has_error());
        REQUIRE_FALSE(cell88.value());
        auto cell89 = cur->list_size(1, 0);
        REQUIRE_FALSE(cell89.has_error());
        REQUIRE(cell89.value() == 3);
        auto cell90 = cur->get<int32_t>(1, 0, {0});
        REQUIRE_FALSE(cell90.has_error());
        REQUIRE(cell90.value() == 7);
        auto null_row = cur->is_null(1, 1);
        REQUIRE_FALSE(null_row.has_error());
        REQUIRE(null_row.value());
    }
}

// With a NULL array row present, every one of these read/scan/aggregate/DML statements
// must complete cleanly (the engine's own call sites already guard is_null()),
// rather than crashing the process.
TEST_CASE("integration::list_array::null_array_sql_operations_clean") {
    auto config = test_create_config(integration_fixture_path("test_list_array/null_array_sql_ops"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA db;")->is_success());
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
            // RETURNING a NULL array must read back as NULL.
            REQUIRE(cur->size() == 1);
            auto returned = cur->is_null(0, 0);
            REQUIRE_FALSE(returned.has_error());
            REQUIRE(returned.value());
        }
    }
}

// #563/#559: `col = ARRAY(SELECT ...)` is real, length-aware array equality — the
// sub-query rows are compacted into an array and compared to the column. A different-length result is
// simply unequal (never truncated/padded), and an empty sub-query yields a real empty array {} (unequal
// to a non-empty column), not the NA-null sentinel.
TEST_CASE("integration::list_array::array_equality_subquery") {
    auto config = test_create_config(integration_fixture_path("test_list_array/array_eq_subquery"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE SCHEMA db;")->is_success());
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
    auto config = test_create_config(integration_fixture_path("test_list_array/null_elements"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };

    ok("CREATE SCHEMA db;");
    ok("CREATE TABLE db.a (id int, arr int[3], lst int[]);");
    ok("INSERT INTO db.a (id, arr, lst) VALUES (1, ARRAY[10,20,30], ARRAY[10,20,30]);");
    // A row whose ARRAY and LIST both carry a NULL middle element.
    ok("INSERT INTO db.a (id, arr, lst) VALUES (2, ARRAY[10,NULL,30], ARRAY[10,NULL,30]);");

    SECTION("whole ARRAY column round-trips the NULL element") {
        auto cur = ok("SELECT arr FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        auto cell91 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell91.has_error());
        REQUIRE(cell91.value() == 3);
        auto cell92 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell92.has_error());
        REQUIRE(cell92.value() == 10);
        auto cell93 = cur->is_null(0, 0, {1});
        REQUIRE_FALSE(cell93.has_error());
        REQUIRE(cell93.value());
        auto cell94 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell94.has_error());
        REQUIRE(cell94.value() == 30);
    }
    SECTION("whole LIST column round-trips the NULL element") {
        auto cur = ok("SELECT lst FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        auto cell95 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell95.has_error());
        REQUIRE(cell95.value() == 3);
        auto cell96 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell96.has_error());
        REQUIRE(cell96.value() == 10);
        auto cell97 = cur->is_null(0, 0, {1});
        REQUIRE_FALSE(cell97.has_error());
        REQUIRE(cell97.value());
        auto cell98 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell98.has_error());
        REQUIRE(cell98.value() == 30);
    }
    SECTION("subscript of a NULL element yields NULL (ARRAY and LIST)") {
        auto cur = ok("SELECT arr[2], lst[2] FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        auto cell99 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell99.has_error());
        REQUIRE(cell99.value());
        auto cell100 = cur->is_null(1, 0);
        REQUIRE_FALSE(cell100.has_error());
        REQUIRE(cell100.value());
    }
    SECTION("subscript of a non-NULL element is unaffected") {
        auto cur = ok("SELECT arr[1], lst[3] FROM db.a WHERE id = 2;");
        REQUIRE(cur->size() == 1);
        auto cell101 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell101.has_error());
        REQUIRE(cell101.value() == 10);
        auto cell102 = cur->get<int32_t>(1, 0);
        REQUIRE_FALSE(cell102.has_error());
        REQUIRE(cell102.value() == 30);
    }
    SECTION("a NULL element does not match an equality filter on that position") {
        // row 2's arr[2]/lst[2] are NULL, so only row 1 (value 20) matches.
        auto cur = ok("SELECT id FROM db.a WHERE arr[2] = 20;");
        REQUIRE(cur->size() == 1);
        auto cell103 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell103.has_error());
        REQUIRE(cell103.value() == 1);
        auto cur2 = ok("SELECT id FROM db.a WHERE lst[2] = 20;");
        REQUIRE(cur2->size() == 1);
        auto cell104 = cur2->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell104.has_error());
        REQUIRE(cell104.value() == 1);
    }
    SECTION("IS NULL / IS NOT NULL on a subscript") {
        // row 1's arr[2]=20 (not null), row 2's arr[2]=NULL.
        auto isnull = ok("SELECT id FROM db.a WHERE arr[2] IS NULL;");
        REQUIRE(isnull->size() == 1);
        auto cell105 = isnull->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell105.has_error());
        REQUIRE(cell105.value() == 2);
        auto notnull = ok("SELECT id FROM db.a WHERE lst[2] IS NOT NULL;");
        REQUIRE(notnull->size() == 1);
        auto cell106 = notnull->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell106.has_error());
        REQUIRE(cell106.value() == 1);
    }
    SECTION("comparing whole arrays with a NULL element still answers true or false") {
        // A NULL ELEMENT is not a NULL array. Whole containers order totally: a NULL element
        // sorts after every value and two NULLs in the same place are equal, so the row gets a
        // definite answer. Matches PostgreSQL, where `arr = ARRAY[10,NULL,30]` selects the row
        // and `arr > ARRAY[10,20,30]` selects it too.
        auto same = ok("SELECT id FROM db.a WHERE lst = ARRAY[10,NULL,30];");
        REQUIRE(same->size() == 1);
        auto cell107 = same->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell107.has_error());
        REQUIRE(cell107.value() == 2);
        auto after = ok("SELECT id FROM db.a WHERE lst > ARRAY[10,20,30];");
        REQUIRE(after->size() == 1);
        auto cell108 = after->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell108.has_error());
        REQUIRE(cell108.value() == 2);
    }
    SECTION("ORDER BY a subscript orders by the element, NULLs last") {
        // arr[2] is 20 (id 1) and NULL (id 2): ascending places 20 first, the NULL last.
        auto cur = ok("SELECT id FROM db.a ORDER BY arr[2];");
        REQUIRE(cur->size() == 2);
        auto cell109 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell109.has_error());
        REQUIRE(cell109.value() == 1);
        auto cell110 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell110.has_error());
        REQUIRE(cell110.value() == 2);
        // Explicit NULLS FIRST places the NULL row (id 2) first.
        auto nf = ok("SELECT id FROM db.a ORDER BY lst[2] NULLS FIRST;");
        REQUIRE(nf->size() == 2);
        auto cell111 = nf->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell111.has_error());
        REQUIRE(cell111.value() == 2);
        auto cell112 = nf->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell112.has_error());
        REQUIRE(cell112.value() == 1);
    }
}

// MIN/MAX over a whole array is the ordinary reduction over the vector's rows — it picks one
// winning row out of many, exactly as over a number. What differs is only the comparison:
// lexicographic over the shared prefix, then by length, with a NULL element sorting last.
TEST_CASE("integration::list_array::min_max_over_whole_array") {
    auto config = test_create_config(integration_fixture_path("test_list_array/min_max_over_whole_array"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };

    ok("CREATE SCHEMA db;");
    ok("CREATE TABLE db.l (id int, grp int, v int[]);");
    ok("INSERT INTO db.l (id, grp, v) VALUES (1, 1, ARRAY[1,2]);");
    ok("INSERT INTO db.l (id, grp, v) VALUES (2, 1, ARRAY[1,2,3]);");
    ok("INSERT INTO db.l (id, grp, v) VALUES (3, 2, ARRAY[0,9]);");

    SECTION("the winner is a whole list, not a scalar drawn from inside one") {
        auto cur = ok("SELECT max(v) FROM db.l;");
        REQUIRE(cur->size() == 1);
        auto cell113 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell113.has_error());
        REQUIRE(cell113.value() == 3);
        auto cell114 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell114.has_error());
        REQUIRE(cell114.value() == 1);
        auto cell115 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell115.has_error());
        REQUIRE(cell115.value() == 2);
        auto cell116 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell116.has_error());
        REQUIRE(cell116.value() == 3);
    }
    SECTION("min picks the row that is smallest at the first element that differs") {
        auto cur = ok("SELECT min(v) FROM db.l;");
        REQUIRE(cur->size() == 1);
        auto cell117 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell117.has_error());
        REQUIRE(cell117.value() == 2);
        auto cell118 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell118.has_error());
        REQUIRE(cell118.value() == 0);
        auto cell119 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell119.has_error());
        REQUIRE(cell119.value() == 9);
    }
    SECTION("a shared prefix is broken by length: the longer list is the greater one") {
        auto cur = ok("SELECT max(v) FROM db.l WHERE grp = 1;");
        REQUIRE(cur->size() == 1);
        auto cell120 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell120.has_error());
        REQUIRE(cell120.value() == 3);
        auto shorter = ok("SELECT min(v) FROM db.l WHERE grp = 1;");
        auto cell121 = shorter->list_size(0, 0);
        REQUIRE_FALSE(cell121.has_error());
        REQUIRE(cell121.value() == 2);
    }
    SECTION("each group reduces to its own winner") {
        auto cur = ok("SELECT grp, max(v) FROM db.l GROUP BY grp ORDER BY grp;");
        REQUIRE(cur->size() == 2);
        auto cell122 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell122.has_error());
        REQUIRE(cell122.value() == 1);
        auto cell123 = cur->get<int32_t>(1, 0, {2});
        REQUIRE_FALSE(cell123.has_error());
        REQUIRE(cell123.value() == 3); // {1,2,3}
        auto cell124 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell124.has_error());
        REQUIRE(cell124.value() == 2);
        auto cell125 = cur->get<int32_t>(1, 1, {0});
        REQUIRE_FALSE(cell125.has_error());
        REQUIRE(cell125.value() == 0); // {0,9}
    }
    SECTION("a constant array argument reduces to that array") {
        auto cur = ok("SELECT max(ARRAY[7,8,9]) FROM db.l;");
        REQUIRE(cur->size() == 1);
        auto cell126 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell126.has_error());
        REQUIRE(cell126.value() == 3);
        auto cell127 = cur->get<int32_t>(0, 0, {0});
        REQUIRE_FALSE(cell127.has_error());
        REQUIRE(cell127.value() == 7);
        auto cell128 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell128.has_error());
        REQUIRE(cell128.value() == 9);
    }
    SECTION("NULL rows are skipped, and only NULL rows give a NULL result") {
        ok("INSERT INTO db.l (id, grp, v) VALUES (4, 3, NULL);");
        auto skipped = ok("SELECT max(v) FROM db.l WHERE grp = 2 OR grp = 3;");
        REQUIRE(skipped->size() == 1);
        auto cell129 = skipped->list_size(0, 0);
        REQUIRE_FALSE(cell129.has_error());
        REQUIRE(cell129.value() == 2); // {0,9}, the NULL row ignored
        auto empty = ok("SELECT max(v) FROM db.l WHERE grp = 3;");
        REQUIRE(empty->size() == 1);
        auto cell130 = empty->is_null(0, 0);
        REQUIRE_FALSE(cell130.has_error());
        REQUIRE(cell130.value());
    }
    SECTION("a NULL element sorts after every value, so it wins max and loses min") {
        ok("CREATE TABLE db.n (id int, v int[]);");
        ok("INSERT INTO db.n (id, v) VALUES (1, ARRAY[1,NULL]);");
        ok("INSERT INTO db.n (id, v) VALUES (2, ARRAY[1,2]);");

        auto largest = ok("SELECT max(v) FROM db.n;");
        auto cell131 = largest->is_null(0, 0, {1});
        REQUIRE_FALSE(cell131.has_error());
        REQUIRE(cell131.value());
        auto smallest = ok("SELECT min(v) FROM db.n;");
        auto cell132 = smallest->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell132.has_error());
        REQUIRE(cell132.value() == 2);
    }
}

// The same reduction over a fixed-width ARRAY column, where every row is the same length and
// only the elements can break the tie.
TEST_CASE("integration::list_array::min_max_over_fixed_array") {
    auto config = test_create_config(integration_fixture_path("test_list_array/min_max_over_fixed_array"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto ok = [&](const std::string& sql) {
        INFO(sql);
        auto cur = exec(dispatcher, sql);
        REQUIRE(cur->is_success());
        return cur;
    };

    ok("CREATE SCHEMA db;");
    ok("CREATE TABLE db.a (id int, v int[3]);");
    ok("INSERT INTO db.a (id, v) VALUES (1, ARRAY[1,2,3]);");
    ok("INSERT INTO db.a (id, v) VALUES (2, ARRAY[1,5,0]);");
    ok("INSERT INTO db.a (id, v) VALUES (3, ARRAY[1,2,9]);");

    SECTION("max compares element by element") {
        auto cur = ok("SELECT max(v) FROM db.a;");
        REQUIRE(cur->size() == 1);
        auto cell133 = cur->list_size(0, 0);
        REQUIRE_FALSE(cell133.has_error());
        REQUIRE(cell133.value() == 3);
        auto cell134 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell134.has_error());
        REQUIRE(cell134.value() == 5); // {1,5,0} beats both {1,2,*}
        auto cell135 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell135.has_error());
        REQUIRE(cell135.value() == 0);
    }
    SECTION("min falls through to the last element when the prefix ties") {
        auto cur = ok("SELECT min(v) FROM db.a;");
        REQUIRE(cur->size() == 1);
        auto cell136 = cur->get<int32_t>(0, 0, {1});
        REQUIRE_FALSE(cell136.has_error());
        REQUIRE(cell136.value() == 2);
        auto cell137 = cur->get<int32_t>(0, 0, {2});
        REQUIRE_FALSE(cell137.has_error());
        REQUIRE(cell137.value() == 3); // {1,2,3} < {1,2,9}
    }
}

// Casting a whole array to text renders it the way an array literal reads back: the elements
// comma-separated inside braces, a NULL element as the bare word NULL, and an element quoted
// only when leaving it bare would change what it says.
TEST_CASE("integration::list_array::cast_array_to_text") {
    auto config = test_create_config(integration_fixture_path("test_list_array/cast_array_to_text"));
    test_clear_directory(config);
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
        auto cell138 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell138.has_error());
        return std::string{cell138.value()};
    };

    ok("CREATE SCHEMA db;");
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
        auto cell139 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell139.has_error());
        REQUIRE(cell139.value() == "{10,20}");
        auto cell140 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell140.has_error());
        REQUIRE(cell140.value() == "{30,40,50}");
        auto cell141 = cur->is_null(0, 2);
        REQUIRE_FALSE(cell141.has_error());
        REQUIRE(cell141.value()); // a NULL array is a NULL text, not the string "{}"
    }
    SECTION("a fixed-width ARRAY column renders its full width") {
        ok("CREATE TABLE db.a (id int, v int[3]);");
        ok("INSERT INTO db.a (id, v) VALUES (1, ARRAY[1,2,3]);");
        auto cur = ok("SELECT CAST(v AS TEXT) FROM db.a;");
        REQUIRE(cur->size() == 1);
        auto cell142 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell142.has_error());
        REQUIRE(cell142.value() == "{1,2,3}");
    }
    SECTION("the rendered text is what lands in a TEXT column assigned from an array") {
        ok("CREATE TABLE db.t (id int, name text);");
        ok("INSERT INTO db.t (id, name) VALUES (1, 'before');");
        ok("UPDATE db.t SET name = CAST(ARRAY[1,2,3] AS TEXT) WHERE id = 1;");
        auto cur = ok("SELECT name FROM db.t WHERE id = 1;");
        REQUIRE(cur->size() == 1);
        auto cell143 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell143.has_error());
        REQUIRE(cell143.value() == "{1,2,3}");
    }
}
