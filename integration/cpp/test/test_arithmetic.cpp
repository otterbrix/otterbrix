#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <core/date/date_parse.hpp>
#include <core/operations_helper.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using namespace components;
using namespace components::cursor;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_arithmetic") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert test data");
    {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
    }

    INFO("A1. binary operator +");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count + 10 AS plus )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == static_cast<int64_t>(i + 1 + 10));
        }
    }

    INFO("A1. binary operator -");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count - 5 AS minus )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == static_cast<int64_t>(i + 1 - 5));
        }
    }

    INFO("A1. binary operator *");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count * 2 AS doubled )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == static_cast<int64_t>((i + 1) * 2));
        }
    }

    INFO("A1. binary operator /");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count / 3 AS divided )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == static_cast<int64_t>((i + 1) / 3));
        }
    }

    INFO("A1. binary operator %");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count % 7 AS remainder )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == static_cast<int64_t>((i + 1) % 7));
        }
    }

    INFO("A2. column * constant (DOUBLE result)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_double, count_double * 0.13 AS tax )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            double expected_double = static_cast<double>(i + 1) + 0.1;
            double tax = expected_double * 0.13;
            REQUIRE(core::is_equals(cur->chunks().front().data[0].data<double>()[i], expected_double));
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], tax));
        }
    }

    INFO("A3. column * column");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count_double, count * count_double AS product )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto count_val = static_cast<int64_t>(i + 1);
            double count_double_val = static_cast<double>(i + 1) + 0.1;
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == count_val);
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], count_double_val));
            REQUIRE(core::is_equals(cur->chunks().front().data[2].data<double>()[i],
                                    static_cast<double>(count_val) * count_double_val));
        }
    }

    INFO("A4. chained arithmetic");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count * 2 + 10 AS chained )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == v * 2 + 10);
        }
    }

    INFO("A4. nested parenthesized arithmetic");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, (count + 5) * (count - 5) AS expr )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == (v + 5) * (v - 5));
        }
    }

    INFO("A5. tax scenario (multiple computed columns)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count * 0.13 AS tax, count - count * 0.13 AS net )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            double tax = static_cast<double>(v) * 0.13;
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], tax));
            REQUIRE(core::is_equals(cur->chunks().front().data[2].data<double>()[i], static_cast<double>(v) - tax));
        }
    }

    INFO("A6. unary minus");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, -count AS negated )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == -v);
        }
    }

    INFO("A7. constants only");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT 2 + 3 AS five, 10 * 5 AS fifty;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 5);
        REQUIRE(cur->chunks().front().data[1].data<int64_t>()[0] == 50);
    }

    INFO("A8. type promotion int * double");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count * 1.5 AS promoted )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC LIMIT 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(core::is_equals(cur->chunks().front().data[0].data<double>()[i], static_cast<double>(i + 1) * 1.5));
        }
    }

    INFO("B1. arithmetic expression vs constant");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 2 > 150 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 25);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(76 + i));
        }
    }

    INFO("B2. column * column in WHERE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * count_double > 5000.0 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        size_t expected = 0;
        for (int i = 1; i <= 100; i++) {
            if (static_cast<double>(i) * (static_cast<double>(i) + 0.1) > 5000.0) {
                expected++;
            }
        }
        REQUIRE(cur->size() == expected);
    }

    INFO("B3. arithmetic with AND");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 2 > 100 AND count * 2 < 150 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 24);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(51 + i));
        }
    }

    INFO("B4. arithmetic on BOTH sides");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 3 > count_double * 2 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("B5. arithmetic with OR");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count + 10 < 15 OR count - 5 > 90 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 9);
    }

    INFO("B6. nested arithmetic in WHERE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE (count + 1) * (count - 1) > 9000 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(95 + i));
        }
    }

    INFO("C1. SUM of expression");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * 2) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 10100);
    }

    INFO("C2. SUM of column * column");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * count_double) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        double expected = 0.0;
        for (int i = 1; i <= 100; i++) {
            expected += static_cast<double>(i) * (static_cast<double>(i) + 0.1);
        }
        REQUIRE(core::is_equals(cur->chunks().front().data[0].data<double>()[0], expected));
    }

    INFO("C3. AVG of expression");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT AVG(count * 10) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // AVG might return int or double depending on implementation
        auto val = cur->chunks().front().data[0].data<int64_t>()[0];
        REQUIRE(val == 505);
    }

    INFO("C4. MIN/MAX of expression");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT MIN(count * 2) AS min_val, MAX(count * 2) AS max_val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 2);
        REQUIRE(cur->chunks().front().data[1].data<int64_t>()[0] == 200);
    }

    INFO("C5pre. COUNT(*) without WHERE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(*) AS cnt )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<uint64_t>()[0] == kNumInserts);
    }

    INFO("C5. COUNT with arithmetic WHERE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(*) AS cnt )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 3 > 200;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<uint64_t>()[0] == 34);
    }

    INFO("D1. GROUP BY with arithmetic in aggregate arg");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count * 2) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("D2. GROUP BY + arithmetic in WHERE + aggregate on expression");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count * count_double) AS revenue )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count > 10 )_"
                                           R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("E1. arithmetic on single aggregate");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count) * 2 AS doubled )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 10100);
    }

    INFO("E2. arithmetic on multiple aggregates");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count) / COUNT(*) AS manual_avg )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // This engine's SUM(...)/COUNT(...) truncates to integer rather than promoting to double.
        auto val = cur->chunks().front().data[0].data<int64_t>()[0];
        REQUIRE(val == 50);
    }

    INFO("E3. complex: aggregate * constant");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * count_double) * 0.3 AS margin )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        double sum_val = 0.0;
        for (int i = 1; i <= 100; i++) {
            sum_val += static_cast<double>(i) * (static_cast<double>(i) + 0.1);
        }
        auto actual_val = cur->chunks().front().data[0].data<double>()[0];
        auto expected_val = sum_val * 0.3;
        REQUIRE(std::abs(actual_val - expected_val) < 1.0);
    }

    INFO("E4. GROUP BY + post-aggregate arithmetic");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    R"_(SELECT count_bool, SUM(count) AS total, SUM(count) * 2 AS doubled_total )_"
                                    R"_(FROM TestDatabase.TestCollection )_"
                                    R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        for (size_t i = 0; i < cur->size(); i++) {
            auto total = cur->chunks().front().data[1].data<int64_t>()[i];
            auto doubled = cur->chunks().front().data[2].data<int64_t>()[i];
            REQUIRE(doubled == total * 2);
        }
    }

    INFO("E5. interleaved post-aggregate arithmetic columns");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) * 2 AS doubled, )_"
                                           R"_(COUNT(*) AS cnt )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 3);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == static_cast<int64_t>((50 + i) * 100));
            REQUIRE(cur->chunks().front().data[2].data<int64_t>()[i] == 50);
        }
    }

    INFO("F1. ORDER BY computed expression");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count * -1 ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(kNumInserts - i));
        }
    }

    INFO("F2. ORDER BY column not in SELECT (ASC)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_double, count_double * 0.13 AS tax )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            double expected_double = static_cast<double>(i + 1) + 0.1;
            double tax = expected_double * 0.13;
            REQUIRE(core::is_equals(cur->chunks().front().data[0].data<double>()[i], expected_double));
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], tax));
        }
    }

    INFO("F3. ORDER BY column not in SELECT (DESC) — regression");
    {
        // Regression: an unresolved sort key (column dropped by GROUP) left rows in insertion order instead of DESC.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_double, count_double * 0.13 AS tax )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count DESC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            double expected_double = static_cast<double>(kNumInserts - i) + 0.1;
            double tax = expected_double * 0.13;
            REQUIRE(core::is_equals(cur->chunks().front().data[0].data<double>()[i], expected_double));
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], tax));
        }
    }

    INFO("F4. ORDER BY arithmetic expression DESC");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count_double )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count + count_double DESC LIMIT 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(kNumInserts - i));
        }
    }

    INFO("G1. UPDATE SET with arithmetic");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE count <= 10 ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(UPDATE TestDatabase.TestCollection )_"
                                               R"_(SET count = count * 2 WHERE count <= 10;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE count <= 20 ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            bool found_even = false;
            for (size_t i = 0; i < cur->size(); i++) {
                auto v = cur->chunks().front().data[0].data<int64_t>()[i];
                if (v == 2)
                    found_even = true;
            }
            REQUIRE(found_even);
        }
    }

    INFO("H1. DELETE with arithmetic WHERE");
    {
        size_t count_before;
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT COUNT(*) AS cnt )_"
                                               R"_(FROM TestDatabase.TestCollection;)_");
            REQUIRE(cur->is_success());
            count_before = cur->chunks().front().data[0].data<uint64_t>()[0];
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(DELETE FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE count * 3 > 270;)_");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT COUNT(*) AS cnt )_"
                                               R"_(FROM TestDatabase.TestCollection;)_");
            REQUIRE(cur->is_success());
            auto count_after = cur->chunks().front().data[0].data<uint64_t>()[0];
            REQUIRE(count_after < count_before);
        }
    }

    INFO("I1. INSERT with computed values");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(INSERT INTO TestDatabase.TestCollection )_"
                                           R"_(  (count, count_str, count_double, count_bool) )_"
                                           R"_(VALUES (10 * 5, '50', 50.5, true);)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("I2. INSERT with expressions in multiple VALUES");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(INSERT INTO TestDatabase.TestCollection )_"
                                           R"_(  (count, count_str, count_double, count_bool) )_"
                                           R"_(VALUES (100 + 1, '101', 101.1, false), )_"
                                           R"_(       (100 + 2, '102', 102.1, true);)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::test_arithmetic::join") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_join"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, R"_(CREATE TABLE TestDatabase.TestCollection2();)_");
        }
    }

    INFO("insert test data");
    {
        {
            auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
            auto ins = components::sql::transform::name_catalog_target(
                database_name,
                collection_name,
                logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection2 (price, quantity) VALUES ";
            for (int i = 1; i <= 10; i++) {
                query << "(" << i * 10 << ", " << i << ")";
                if (i < 10)
                    query << ", ";
            }
            query << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("J1. JOIN with arithmetic in ON");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                    R"_(JOIN TestDatabase.TestCollection2 )_"
                                    R"_(ON TestCollection.count = TestCollection2.price * TestCollection2.quantity )_"
                                    R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("J2. JOIN with arithmetic on one side");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(JOIN TestDatabase.TestCollection2 )_"
                                           R"_(ON TestCollection.count * 10 = TestCollection2.price )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }
}

TEST_CASE("integration::cpp::test_arithmetic::having") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_having"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert test data");
    {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_plan(session,
                                     components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("K1. basic HAVING");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool )_"
                                           R"_(HAVING SUM(count) > 2000;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("K2. HAVING with arithmetic");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool )_"
                                           R"_(HAVING SUM(count) * 2 > 5000;)_");
        REQUIRE(cur->is_success());
        // odd sum * 2 = 5000 (not > 5000), even sum * 2 = 5100 (> 5000): only the even group qualifies.
        REQUIRE(cur->size() == 1);
    }

    INFO("K3. HAVING with unary minus");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool )_"
                                           R"_(HAVING -SUM(count) > -2520;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::test_arithmetic::case_when") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_case"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert test data");
    {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_plan(session,
                                     components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("L1. CASE in SELECT with arithmetic in THEN");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    R"_(SELECT count, )_"
                                    R"_(  CASE WHEN count > 50 THEN count * 0.9 ELSE count * 1.0 END AS adjusted )_"
                                    R"_(FROM TestDatabase.TestCollection )_"
                                    R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            double expected = v > 50 ? static_cast<double>(v) * 0.9 : static_cast<double>(v) * 1.0;
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], expected));
        }
    }

    INFO("L2. CASE with arithmetic in WHEN condition");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, )_"
                                           R"_(  CASE WHEN count * 2 > 100 THEN 'high' ELSE 'low' END AS label )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            std::string_view expected = v * 2 > 100 ? "high" : "low";
            REQUIRE(cur->chunks().front().data[1].data<std::string_view>()[i] == expected);
        }
    }

    INFO("L3. CASE with multiple WHEN + arithmetic");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, )_"
                                           R"_(  CASE )_"
                                           R"_(    WHEN count * 10 > 500 THEN 'tier3' )_"
                                           R"_(    WHEN count * 10 > 200 THEN 'tier2' )_"
                                           R"_(    ELSE 'tier1' )_"
                                           R"_(  END AS tier )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == v);
            std::string_view expected;
            if (v * 10 > 500)
                expected = "tier3";
            else if (v * 10 > 200)
                expected = "tier2";
            else
                expected = "tier1";
            REQUIRE(cur->chunks().front().data[1].data<std::string_view>()[i] == expected);
        }
    }
}

TEST_CASE("integration::cpp::test_arithmetic::edge_cases") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_edge"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert test data");
    {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_plan(session,
                                     components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("M1. division by zero returns error (PostgreSQL behavior)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count / 0 AS bad )_"
                                           R"_(FROM TestDatabase.TestCollection LIMIT 1;)_");
        REQUIRE(cur->is_error());
    }

    INFO("M2. very large multiplication");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count * count * count * count AS big )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count = 100;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 100000000);
    }

    INFO("M3. mixed nested: arithmetic inside aggregate inside arithmetic");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * 2) + MAX(count) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 10200);
    }
}

TEST_CASE("integration::cpp::test_arithmetic::interleaved_group_by") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_interleaved_gb"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, R"_(CREATE TABLE TestDatabase.TestCollection3();)_");
        }
    }

    INFO("insert test data");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    R"_(INSERT INTO TestDatabase.TestCollection3 (region, category, amount) VALUES )_"
                                    R"_(('east','A',10), ('east','A',20), ('east','B',30), )_"
                                    R"_(('west','A',40), ('west','B',50), ('west','B',60);)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
    }

    INFO("E6a. SELECT region, SUM(amount)*2, category, COUNT(*)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT region, SUM(amount) * 2 AS doubled, category, COUNT(*) AS cnt )_"
                                           R"_(FROM TestDatabase.TestCollection3 )_"
                                           R"_(GROUP BY region, category;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->column_count() == 4);

        std::map<std::pair<std::string, std::string>, std::pair<int64_t, int64_t>> rows;
        for (size_t i = 0; i < cur->size(); i++) {
            auto region = std::string(cur->chunks().front().data[0].data<std::string_view>()[i]);
            auto doubled = cur->chunks().front().data[1].data<int64_t>()[i];
            auto category = std::string(cur->chunks().front().data[2].data<std::string_view>()[i]);
            auto cnt = static_cast<int64_t>(cur->chunks().front().data[3].data<uint64_t>()[i]);
            rows[{region, category}] = {doubled, cnt};
        }
        REQUIRE(rows.size() == 4);
        REQUIRE(rows[{"east", "A"}].first == 60);
        REQUIRE(rows[{"east", "A"}].second == 2);
        REQUIRE(rows[{"east", "B"}].first == 60);
        REQUIRE(rows[{"east", "B"}].second == 1);
        REQUIRE(rows[{"west", "A"}].first == 80);
        REQUIRE(rows[{"west", "A"}].second == 1);
        REQUIRE(rows[{"west", "B"}].first == 220);
        REQUIRE(rows[{"west", "B"}].second == 2);
    }

    INFO("E6b. SELECT region, COUNT(*), SUM(amount)+100, category");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    R"_(SELECT region, COUNT(*) AS cnt, SUM(amount) + 100 AS shifted, category )_"
                                    R"_(FROM TestDatabase.TestCollection3 )_"
                                    R"_(GROUP BY region, category;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->column_count() == 4);

        std::map<std::pair<std::string, std::string>, std::pair<int64_t, int64_t>> rows;
        for (size_t i = 0; i < cur->size(); i++) {
            auto region = std::string(cur->chunks().front().data[0].data<std::string_view>()[i]);
            auto cnt = static_cast<int64_t>(cur->chunks().front().data[1].data<uint64_t>()[i]);
            auto shifted = cur->chunks().front().data[2].data<int64_t>()[i];
            auto category = std::string(cur->chunks().front().data[3].data<std::string_view>()[i]);
            rows[{region, category}] = {cnt, shifted};
        }
        REQUIRE(rows.size() == 4);
        REQUIRE(rows[{"east", "A"}].first == 2);
        REQUIRE(rows[{"east", "A"}].second == 130);
        REQUIRE(rows[{"east", "B"}].first == 1);
        REQUIRE(rows[{"east", "B"}].second == 130);
        REQUIRE(rows[{"west", "A"}].first == 1);
        REQUIRE(rows[{"west", "A"}].second == 140);
        REQUIRE(rows[{"west", "B"}].first == 2);
        REQUIRE(rows[{"west", "B"}].second == 210);
    }
}

TEST_CASE("integration::cpp::test_optimizer_constant_folding") {
    auto config = test_create_config(integration_fixture_path("test_optimizer_folding"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert test data");
    {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_plan(session,
                                     components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("I1. WHERE with constant true: 5 = 5");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE 5 = 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("I2. WHERE with constant false: 5 = 7");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE 5 = 7;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("I3a. sanity: WHERE count > 5 (no arithmetic)");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE count > 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 95);
    }

    INFO("I3. WHERE count > 2 + 3");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE count > 2 + 3;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 95);
    }

    INFO("I4. WHERE count < 5 * 2");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE count < 5 * 2;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 9);
    }

    INFO("I5. WHERE 10 > 5 (constant true)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE 10 > 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("I6. WHERE 3 > 10 (constant false)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE 3 > 10;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("I7. WHERE count = 10 + 40");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count = 10 + 40;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 50);
    }

    INFO("I8. SELECT count + 10 (projection not folded)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count + 10 AS plus FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC LIMIT 3;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 11);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[1] == 12);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[2] == 13);
    }

    INFO("I9. WHERE 5 = 5 AND count > 95");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE 5 = 5 AND count > 95;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("I10. WHERE 5 = 7 OR count = 50");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE 5 = 7 OR count = 50;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 50);
    }

    INFO("I11. WHERE count = (2 + 3) * 10");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count = (2 + 3) * 10;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 50);
    }

    INFO("I12. WHERE count > 99.5");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, R"_(SELECT count FROM TestDatabase.TestCollection WHERE count > 99.5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 100);
    }

    INFO("I13. WHERE count = 100 - 1");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count = 100 - 1;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 99);
    }

    INFO("I14. WHERE count = 103 % 10");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    R"_(SELECT count FROM TestDatabase.TestCollection WHERE count = 103 % 10;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 3);
    }
}

TEST_CASE("integration::cpp::test_arithmetic::datetime") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_datetime"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection ("
                                               "  d DATE,"
                                               "  ts TIMESTAMP,"
                                               "  t TIME,"
                                               "  iv INTERVAL,"
                                               "  tstz TIMESTAMP WITH TIME ZONE"
                                               ");");
            REQUIRE(cur->is_success());
        }
    }

    INFO("insert test data");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (d, ts, t, iv, tstz) VALUES ("
                                               "  DATE '2024-01-01',"
                                               "  TIMESTAMP '2024-01-01 00:00:00',"
                                               "  TIME '08:00:00',"
                                               "  INTERVAL '1 day',"
                                               "  TIMESTAMPTZ '2024-01-01 00:00:00+00:00'"
                                               ");");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (d, ts, t, iv, tstz) VALUES ("
                                               "  DATE '2024-03-15',"
                                               "  TIMESTAMP '2024-03-15 12:30:00',"
                                               "  TIME '12:30:00',"
                                               "  INTERVAL '7 days',"
                                               "  TIMESTAMPTZ '2024-03-15 12:30:00+00:00'"
                                               ");");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (d, ts, t, iv, tstz) VALUES ("
                                               "  DATE '2024-12-31',"
                                               "  TIMESTAMP '2024-12-31 23:59:00',"
                                               "  TIME '23:59:00',"
                                               "  INTERVAL '30 days',"
                                               "  TIMESTAMPTZ '2024-12-31 23:59:00+00:00'"
                                               ");");
            REQUIRE(cur->is_success());
        }
    }

    INFO("N1. DATE + INTERVAL '1 day' = DATE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d + INTERVAL '1 day' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-01-02"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-03-16"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2025-01-01"));
    }

    INFO("N2. DATE - INTERVAL '7 days' = DATE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d - INTERVAL '7 days' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2023-12-25"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-03-08"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2024-12-24"));
    }

    INFO("N3. DATE column + INTERVAL column = DATE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d + iv AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-01-02"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-03-22"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2025-01-30"));
    }

    INFO("N4. TIMESTAMP + INTERVAL '1 day' = TIMESTAMP");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ts + INTERVAL '1 day' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY ts ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-01-02 00:00:00"));
        REQUIRE(cur->value(0, 1).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-03-16 12:30:00"));
        REQUIRE(cur->value(0, 2).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2025-01-01 23:59:00"));
    }

    INFO("N5. TIMESTAMP - INTERVAL '1 day' = TIMESTAMP");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ts - INTERVAL '1 day' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY ts ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2023-12-31 00:00:00"));
        REQUIRE(cur->value(0, 1).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-03-14 12:30:00"));
        REQUIRE(cur->value(0, 2).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-12-30 23:59:00"));
    }

    INFO("N6. TIMESTAMP column + INTERVAL column = TIMESTAMP");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ts + iv AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY ts ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-01-02 00:00:00"));
        REQUIRE(cur->value(0, 1).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-03-22 12:30:00"));
        REQUIRE(cur->value(0, 2).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2025-01-30 23:59:00"));
    }

    INFO("N7. DATE - DATE = INTERVAL (days difference)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d - DATE '2024-01-01' AS diff "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE d = DATE '2024-03-15';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto iv = cur->value(0, 0).value<core::date::interval_t>();
        REQUIRE(iv.day.count() == 74);
        REQUIRE(iv.time.count() == 0);
        REQUIRE(iv.month.count() == 0);
    }

    INFO("N8. INTERVAL + INTERVAL = INTERVAL");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT iv + INTERVAL '3 days' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::interval_t>().day.count() == 4);
        REQUIRE(cur->value(0, 1).value<core::date::interval_t>().day.count() == 10);
        REQUIRE(cur->value(0, 2).value<core::date::interval_t>().day.count() == 33);
    }

    INFO("N9. INTERVAL - INTERVAL = INTERVAL");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT iv - INTERVAL '1 day' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::interval_t>().day.count() == 0);
        REQUIRE(cur->value(0, 1).value<core::date::interval_t>().day.count() == 6);
        REQUIRE(cur->value(0, 2).value<core::date::interval_t>().day.count() == 29);
    }

    INFO("N10. WHERE d + INTERVAL '1 day' > DATE '2024-03-15'");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d FROM TestDatabase.TestCollection "
                                           "WHERE d + INTERVAL '1 day' > DATE '2024-03-15';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("N11. TIMESTAMP - TIMESTAMP = INTERVAL (microseconds difference)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ts - TIMESTAMP '2024-01-01 00:00:00' AS diff "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE ts = TIMESTAMP '2024-03-15 12:30:00';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto iv = cur->value(0, 0).value<core::date::interval_t>();
        // A TIMESTAMP-TIMESTAMP result stores the whole span in .time; .day stays 0.
        auto expected_us =
            int64_t{74} * 86400LL * 1'000'000LL + 12LL * 3600LL * 1'000'000LL + 30LL * 60LL * 1'000'000LL;
        REQUIRE(iv.time.count() == expected_us);
        REQUIRE(iv.day.count() == 0);
        REQUIRE(iv.month.count() == 0);
    }

    INFO("N12. INTERVAL column * 3 = INTERVAL");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT iv * 3 AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::interval_t>().day.count() == 3);
        REQUIRE(cur->value(0, 1).value<core::date::interval_t>().day.count() == 21);
        REQUIRE(cur->value(0, 2).value<core::date::interval_t>().day.count() == 90);
    }

    INFO("N13. INTERVAL column / 2 = INTERVAL");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT iv / 2 AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        // Fractional days round via llround (half away from zero), not truncation.
        REQUIRE(cur->value(0, 0).value<core::date::interval_t>().day.count() == 1);
        REQUIRE(cur->value(0, 1).value<core::date::interval_t>().day.count() == 4);
        REQUIRE(cur->value(0, 2).value<core::date::interval_t>().day.count() == 15);
    }

    INFO("N14. 2 * INTERVAL column = INTERVAL");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT 2 * iv AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::interval_t>().day.count() == 2);
        REQUIRE(cur->value(0, 1).value<core::date::interval_t>().day.count() == 14);
        REQUIRE(cur->value(0, 2).value<core::date::interval_t>().day.count() == 60);
    }

    INFO("N15. INTERVAL '10 days' * 1.5 = INTERVAL '15 days'");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT INTERVAL '10 days' * 1.5 AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE d = DATE '2024-01-01';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<core::date::interval_t>().day.count() == 15);
    }

    INFO("N16. TIME + INTERVAL '1 hour' = TIME");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT t + INTERVAL '1 hour' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY t ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::time_t>() == *core::date::parse_time("09:00:00"));
        REQUIRE(cur->value(0, 1).value<core::date::time_t>() == *core::date::parse_time("13:30:00"));
        REQUIRE(cur->value(0, 2).value<core::date::time_t>() == *core::date::parse_time("00:59:00"));
    }

    INFO("N17. TIME - INTERVAL '30 minutes' = TIME");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT t - INTERVAL '30 minutes' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY t ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::time_t>() == *core::date::parse_time("07:30:00"));
        REQUIRE(cur->value(0, 1).value<core::date::time_t>() == *core::date::parse_time("12:00:00"));
        REQUIRE(cur->value(0, 2).value<core::date::time_t>() == *core::date::parse_time("23:29:00"));
    }

    INFO("N18. TIME '08:00:00' - INTERVAL '10 hours' wraps to 22:00:00");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT t - INTERVAL '10 hours' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE t = TIME '08:00:00';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<core::date::time_t>() == *core::date::parse_time("22:00:00"));
    }

    INFO("N19. TIME column - TIME '06:00:00' = INTERVAL");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT t - TIME '06:00:00' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE t = TIME '08:00:00';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto iv = cur->value(0, 0).value<core::date::interval_t>();
        REQUIRE(iv.time.count() == 2LL * 3600LL * 1'000'000LL);
        REQUIRE(iv.day.count() == 0);
        REQUIRE(iv.month.count() == 0);
    }

    INFO("N20. DATE + INTERVAL '1 month' = DATE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d + INTERVAL '1 month' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-02-01"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-04-15"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2025-01-31"));
    }

    INFO("N21. DATE '2024-01-31' + INTERVAL '1 month' clamps to 2024-02-29");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT DATE '2024-01-31' + INTERVAL '1 month' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE d = DATE '2024-01-01';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-02-29"));
    }

    INFO("N22. TIMESTAMP + INTERVAL '2 months' = TIMESTAMP");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ts + INTERVAL '2 months' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY ts ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-03-01 00:00:00"));
        REQUIRE(cur->value(0, 1).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2024-05-15 12:30:00"));
        REQUIRE(cur->value(0, 2).value<core::date::timestamp_t>() ==
                *core::date::parse_timestamp("2025-02-28 23:59:00"));
    }

    INFO("N23. iv + d = DATE (INTERVAL column + DATE column)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT iv + d AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-01-02"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-03-22"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2025-01-30"));
    }

    INFO("N24. WHERE ts - INTERVAL '1 day' > TIMESTAMP '2024-01-01 00:00:00'");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d FROM TestDatabase.TestCollection "
                                           "WHERE ts - INTERVAL '1 day' > TIMESTAMP '2024-01-01 00:00:00';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("N25. TIMESTAMPTZ + INTERVAL '1 day' = TIMESTAMPTZ");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT tstz + INTERVAL '1 day' AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY tstz ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::timestamptz_t>() ==
                *core::date::parse_timestamptz("2024-01-02 00:00:00+00:00"));
        REQUIRE(cur->value(0, 1).value<core::date::timestamptz_t>() ==
                *core::date::parse_timestamptz("2024-03-16 12:30:00+00:00"));
        REQUIRE(cur->value(0, 2).value<core::date::timestamptz_t>() ==
                *core::date::parse_timestamptz("2025-01-01 23:59:00+00:00"));
    }

    INFO("N26. TIMESTAMPTZ - TIMESTAMPTZ = INTERVAL (µs difference)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT tstz - TIMESTAMPTZ '2024-01-01 00:00:00+00:00' AS diff "
                                           "FROM TestDatabase.TestCollection "
                                           "WHERE tstz = TIMESTAMPTZ '2024-03-15 12:30:00+00:00';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto iv = cur->value(0, 0).value<core::date::interval_t>();
        auto expected_us =
            int64_t{74} * 86400LL * 1'000'000LL + 12LL * 3600LL * 1'000'000LL + 30LL * 60LL * 1'000'000LL;
        REQUIRE(iv.time.count() == expected_us);
        REQUIRE(iv.day.count() == 0);
        REQUIRE(iv.month.count() == 0);
    }

    INFO("N27. CASE WHEN with DATE + INTERVAL exercises arithmetic_eval.cpp");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT CASE WHEN d = DATE '2024-01-01' "
                                           "THEN d + INTERVAL '1 day' ELSE d END AS result "
                                           "FROM TestDatabase.TestCollection "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-01-02"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-03-15"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2024-12-31"));
    }

    INFO("N28. SELECT d + INTERVAL '1 month' ... GROUP BY d exercises operator_group.cpp");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d + INTERVAL '1 month' AS bucket, COUNT(*) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY d "
                                           "ORDER BY d ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-02-01"));
        REQUIRE(cur->value(0, 1).value<core::date::date_t>() == *core::date::parse_date("2024-04-15"));
        REQUIRE(cur->value(0, 2).value<core::date::date_t>() == *core::date::parse_date("2025-01-31"));
    }

    INFO("N29. UPDATE SET d = d + INTERVAL '7 days' exercises update_expression.cpp");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection "
                                               "SET d = d + INTERVAL '7 days' "
                                               "WHERE d = DATE '2024-01-01';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT d FROM TestDatabase.TestCollection "
                                               "WHERE d = DATE '2024-01-08';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<core::date::date_t>() == *core::date::parse_date("2024-01-08"));
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT d FROM TestDatabase.TestCollection "
                                               "WHERE d = DATE '2024-01-01';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }
}

// UPDATE SET <col> = <col> << 1 on a DOUBLE/STRING column used to report success while writing
// 0 (resp. '') — the shift kernel's non-integer branch was an assert-only stub, erased in
// Release, leaving the destination unwritten. Such updates must fail, not corrupt silently.
TEST_CASE("integration::cpp::test_arithmetic::update_bitshift_non_integer_rejected") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic/update_bitshift_non_integer"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.b (x bigint, f double, s string);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.b (x, f, s) VALUES (3, 1.5, 'ab');")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "UPDATE t.b SET f = f << 1 WHERE x > 0;");
        REQUIRE_FALSE(cur->is_success()); // was: reported success
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "UPDATE t.b SET s = s << 1 WHERE x > 0;");
        REQUIRE_FALSE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT f, s FROM t.b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const double f = cur->value(0, 0).value<double>();
        CHECK(f > 1.4999); // was: silently overwritten with 0
        CHECK(f < 1.5001);
        CHECK(cur->value(1, 0).value<std::string_view>() == "ab"); // was: ''
    }
    // Bitshift on a genuinely integer column keeps working.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "UPDATE t.b SET x = x << 1 WHERE x > 0;")->is_success());
        auto cur = dispatcher->execute_sql(session, "SELECT x FROM t.b;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 6);
    }
}

TEST_CASE("integration::cpp::test_arithmetic::constant_division_by_zero_is_an_error") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_const_div_zero"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE z;")->is_success());
    REQUIRE(exec("CREATE TABLE z.t (id BIGINT, x BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO z.t (id, x) VALUES (1, 9);")->is_success());

    // The reference spellings, already correct.
    CHECK(exec("SELECT 1 / 0 AS v;")->is_error());
    CHECK(exec("SELECT x / 0 AS v FROM z.t;")->is_error());
    CHECK(exec("SELECT id FROM z.t WHERE x / 0 = 1;")->is_error());
    CHECK(exec("UPDATE z.t SET x = x / 0;")->is_error());

    // The INSERT VALUES fold: '/' stored a zero, '%' trapped with SIGFPE.
    CHECK(exec("INSERT INTO z.t (id, x) VALUES (2, 1 / 0);")->is_error());
    CHECK(exec("INSERT INTO z.t (id, x) VALUES (3, 1 % 0);")->is_error());

    // The optimizer fold: a constant zero divisor answered NULL, so the predicate
    // succeeded and matched nothing rather than reporting the division.
    CHECK(exec("SELECT id FROM z.t WHERE 1 / 0 = 1;")->is_error());
    CHECK(exec("SELECT id FROM z.t WHERE 1 % 0 = 1;")->is_error());
    CHECK(exec("SELECT id FROM z.t WHERE x = 1 / 0;")->is_error());

    // Nothing above may have been written.
    auto rows = exec("SELECT id FROM z.t;");
    REQUIRE(rows->is_success());
    CHECK(rows->size() == 1);
}

TEST_CASE("integration::cpp::test_arithmetic::constant_folding_still_folds") {
    auto config = test_create_config(integration_fixture_path("test_arithmetic_const_fold_ok"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE z;")->is_success());
    REQUIRE(exec("CREATE TABLE z.t (id BIGINT, x BIGINT, d DATE);")->is_success());

    REQUIRE(exec("INSERT INTO z.t (id, x) VALUES (1, 2 + 3);")->is_success());
    REQUIRE(exec("INSERT INTO z.t (id, x) VALUES (2, 10 / 2);")->is_success());
    REQUIRE(exec("INSERT INTO z.t (id, x) VALUES (3, 11 % 3);")->is_success());
    {
        auto cur = exec("SELECT x FROM z.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        CHECK(cur->value(0, 0).value<int64_t>() == 5);
        CHECK(cur->value(0, 1).value<int64_t>() == 5);
        CHECK(cur->value(0, 2).value<int64_t>() == 2);
    }

    // Temporal constant arithmetic in a VALUES list goes through the same fold.
    REQUIRE(exec("INSERT INTO z.t (id, d) VALUES (4, DATE '2020-01-01' + INTERVAL '1 day');")->is_success());

    // A folded constant predicate still decides the scan.
    {
        auto cur = exec("SELECT id FROM z.t WHERE 10 / 2 = 5;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 4);
    }
    {
        auto cur = exec("SELECT id FROM z.t WHERE 10 / 2 = 4;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0);
    }

    // A NULL divisor is NULL, not a division by zero.
    {
        auto cur = exec("SELECT x / NULL AS v FROM z.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->chunks().front().data[0].is_null(0));
    }
}
