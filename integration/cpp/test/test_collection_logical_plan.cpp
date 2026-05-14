#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/tests/generaty.hpp>
#include <core/operations_helper.hpp>
#include <variant>

static const database_name_t table_database_name = "table_testdatabase";
static const collection_name_t table_collection_name = "table_testcollection";
static const collection_name_t table_other_collection_name = "table_othertestcollection";
static const collection_name_t table_collection_left = "table_testcollection_left_join";
static const collection_name_t table_collection_right = "table_testcollection_right_join";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::logical_plan") {
    auto config = test_create_config("/tmp/test_collection_logical_plan");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, table_database_name);
        }
        const char* kGenColsSql =
            "count bigint, count_str string, count_double double, count_bool bool, "
            "count_array ubigint[5], count_decimal decimal(15,7)";
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                fmt::format("CREATE TABLE {}.{}({});", table_database_name, table_collection_name, kGenColsSql));
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                fmt::format("CREATE TABLE {}.{}({});", table_database_name, table_other_collection_name, kGenColsSql));
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                fmt::format("CREATE TABLE {}.{}(name string, key_1 bigint, key_2 bigint);",
                            table_database_name, table_collection_left));
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                fmt::format("CREATE TABLE {}.{}(value bigint, key bigint);",
                            table_database_name, table_collection_right));
        }
    }

    INFO("insert") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            gen_data_chunk_insert_sql(table_database_name, table_collection_name, kNumInserts));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{};", table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            // Floating-point compare against int column — coerce to double.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90.0;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("group by boolean") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("SELECT count_bool, count(count) AS cnt, sum(count) AS sum_val, "
                        "avg(count) AS avg_val FROM {}.{} GROUP BY count_bool ORDER BY count_bool ASC;",
                        table_database_name, table_collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);

        // row 0: false (even indices 0,2,4,...98 → count values 1,3,5,...,99) → cnt=50, sum=2500, avg=50.0
        // row 1: true  (odd indices 1,3,5,...99 → count values 2,4,6,...,100) → cnt=50, sum=2550, avg=51.0
        // Note: gen_data_chunk produces count = i+1, count_bool = (i%2==0)
        // Even indices (0,2,...,98): count_bool=true, count=1,3,...,99 → sum=2500, avg=50.0
        // Odd indices (1,3,...,99): count_bool=false, count=2,4,...,100 → sum=2550, avg=51.0
        // After sort asc: false first (row 0), true second (row 1)
        REQUIRE(cur->chunk_data().value(0, 0).value<bool>() == false);
        REQUIRE(cur->chunk_data().value(0, 1).value<bool>() == true);
        REQUIRE(cur->chunk_data().value(1, 0).value<uint64_t>() == 50);
        REQUIRE(cur->chunk_data().value(1, 1).value<uint64_t>() == 50);
        REQUIRE(cur->chunk_data().value(2, 0).value<int64_t>() == 2550);
        REQUIRE(cur->chunk_data().value(2, 1).value<int64_t>() == 2500);
        REQUIRE(cur->chunk_data().value(3, 0).value<int64_t>() == 51);
        REQUIRE(cur->chunk_data().value(3, 1).value<int64_t>() == 50);
    }

    INFO("insert from select") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("INSERT INTO {}.{} SELECT * FROM {}.{};",
                        table_database_name, table_other_collection_name,
                        table_database_name, table_collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("DELETE FROM {}.{} WHERE count > 90;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count > 90;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("delete using") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("DELETE FROM {}.{} USING {}.{} WHERE {}.count = {}.count;",
                        table_database_name, table_other_collection_name,
                        table_database_name, table_collection_name,
                        table_other_collection_name, table_collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 90);
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count < 20;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("UPDATE {}.{} SET count = 1000 WHERE count < 20;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count < 20;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count = 1000;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
    }

    INFO("update array element") {
        {
            // Path "count_array.1" sets element at index 1 (zero-indexed).
            // SQL syntax: UPDATE … SET count_array[1] = 9999 …
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("UPDATE {}.{} SET count_array[1] = 9999 WHERE count = 1000;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count = 1000;",
                            table_database_name, table_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
            for (size_t i = 0; i < cur->size(); ++i) {
                auto arr = cur->chunk_data().value(4, i);
                REQUIRE(arr.children()[0].value<uint64_t>() == 9999);
            }
        }
    }

    INFO("update from") {
        // UPDATE ... FROM ... pattern: double each row in other_collection
        // where its count matches a row in collection.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("UPDATE {0}.{1} AS initial_table SET count = initial_table.count * 2 "
                            "FROM {0}.{2} AS from_table WHERE initial_table.count = from_table.count;",
                            table_database_name, table_other_collection_name, table_other_collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{};",
                            table_database_name, table_other_collection_name));
            REQUIRE(cur->size() == 10);
            for (size_t num = 0; num < cur->size(); ++num) {
                REQUIRE(cur->chunk_data().value(0, num).value<int64_t>() == static_cast<int64_t>(91 + num) * 2);
            }
        }
    }

    INFO("delete with limit 1") {
        // 19 rows have count==1000. DELETE LIMIT 1 removes one.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("DELETE FROM {}.{} WHERE count = 1000 LIMIT 1;",
                        table_database_name, table_collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("delete with limit") {
        // 18 rows have count==1000. DELETE LIMIT 5 removes 5.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("DELETE FROM {}.{} WHERE count = 1000 LIMIT 5;",
                        table_database_name, table_collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("update with limit 1") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("UPDATE {}.{} SET count = 2000 WHERE count = 1000 LIMIT 1;",
                        table_database_name, table_collection_name));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("update with limit N") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            fmt::format("UPDATE {}.{} SET count = 3000 WHERE count = 1000 LIMIT 5;",
                        table_database_name, table_collection_name));
        REQUIRE(cur->is_success());
        // There were 18 rows with count==1000 after delete limit 1 removed 1, delete limit 5 removed 5, and update limit 1 changed 1
        // So 12 remain with count==1000, limit 5 should update 5
        REQUIRE(cur->size() == 5);
    }

    INFO("join with outside data") {
        // M4: `raw_data` is a logical-plan-only node (no SQL counterpart).
        // The full raw_data-based JOIN sub-tests below test the
        // wrapper_dispatcher::execute_plan(raw_data) path which is being
        // removed. JOIN / aggregate / sort semantics are covered by
        // test_join.cpp and test_arithmetic.cpp via SQL. INSERT the data
        // into real tables and run a JOIN over them — preserves the
        // join-result expectations without exercising the raw_data path.
        std::vector<std::string> left_rows;
        left_rows.reserve(101);
        for (int64_t num = 0, reversed = 100; num < 101; ++num, --reversed) {
            left_rows.push_back(fmt::format("('Name {}', {}, {})", num, num, reversed));
        }
        std::vector<std::string> right_rows;
        right_rows.reserve(100);
        for (int64_t num = 0; num < 100; ++num) {
            right_rows.push_back(fmt::format("({}, {})", (num + 25) * 2 * 10, (num + 25) * 2));
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (name, key_1, key_2) VALUES {};",
                            table_database_name, table_collection_left, fmt::join(left_rows, ", ")));
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (value, key) VALUES {};",
                            table_database_name, table_collection_right, fmt::join(right_rows, ", ")));
            REQUIRE(cur->is_success());
        }
        INFO("inner join over inserted data") {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {0}.{1} JOIN {0}.{2} ON {1}.key_1 = {2}.key;",
                            table_database_name, table_collection_left, table_collection_right));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);
        }
        INFO("inner join with extra WHERE") {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {0}.{1} JOIN {0}.{2} ON {1}.key_1 = {2}.key "
                            "WHERE {2}.key > 75;",
                            table_database_name, table_collection_left, table_collection_right));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);
        }
        INFO("join with aggregate") {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("SELECT key_1, count(name) AS count, sum(value) AS sum, "
                            "avg(key) AS avg, min(value) AS min, max(value) AS max "
                            "FROM {0}.{1} JOIN {0}.{2} ON {1}.key_1 = {2}.key "
                            "WHERE {1}.key_1 < 75 "
                            "GROUP BY key_1 ORDER BY avg DESC;",
                            table_database_name, table_collection_left, table_collection_right));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);
        }
    }
}
