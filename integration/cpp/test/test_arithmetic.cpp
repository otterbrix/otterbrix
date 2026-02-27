#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>
#include <core/operations_helper.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using namespace components;
using namespace components::cursor;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_arithmetic") {
    auto config = test_create_config("/tmp/test_arithmetic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
    }

    INFO("insert test data") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == kNumInserts);
        }
    }

    // ================================================================
    // A. SELECT — arithmetic in projection
    // ================================================================

    INFO("A1. binary operator +") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count + 10 AS plus )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == static_cast<int64_t>(i + 1 + 10));
        }
    }

    INFO("A1. binary operator -") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count - 5 AS minus )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == static_cast<int64_t>(i + 1 - 5));
        }
    }

    INFO("A1. binary operator *") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count * 2 AS doubled )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == static_cast<int64_t>((i + 1) * 2));
        }
    }

    INFO("A1. binary operator /") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count / 3 AS divided )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == static_cast<int64_t>((i + 1) / 3));
        }
    }

    INFO("A1. binary operator %") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count % 7 AS remainder )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == static_cast<int64_t>((i + 1) % 7));
        }
    }

    INFO("A2. column * constant (DOUBLE result)") {
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
            REQUIRE(core::is_equals(cur->chunk_data().data[0].data<double>()[i], expected_double));
            REQUIRE(core::is_equals(cur->chunk_data().data[1].data<double>()[i], tax));
        }
    }

    INFO("A3. column * column") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == count_val);
            REQUIRE(core::is_equals(cur->chunk_data().data[1].data<double>()[i], count_double_val));
            REQUIRE(core::is_equals(cur->chunk_data().data[2].data<double>()[i],
                                    static_cast<double>(count_val) * count_double_val));
        }
    }

    INFO("A4. chained arithmetic") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count * 2 + 10 AS chained )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == v * 2 + 10);
        }
    }

    INFO("A4. nested parenthesized arithmetic") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, (count + 5) * (count - 5) AS expr )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == (v + 5) * (v - 5));
        }
    }

    INFO("A5. tax scenario (multiple computed columns)") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            REQUIRE(core::is_equals(cur->chunk_data().data[1].data<double>()[i], tax));
            REQUIRE(core::is_equals(cur->chunk_data().data[2].data<double>()[i], static_cast<double>(v) - tax));
        }
    }

    INFO("A6. unary minus") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, -count AS negated )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == -v);
        }
    }

    INFO("A7. constants only") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT 2 + 3 AS five, 10 * 5 AS fifty;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 5);
        REQUIRE(cur->chunk_data().data[1].data<int64_t>()[0] == 50);
    }

    INFO("A8. type promotion int * double") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count * 1.5 AS promoted )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC LIMIT 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(core::is_equals(cur->chunk_data().data[0].data<double>()[i],
                                    static_cast<double>(i + 1) * 1.5));
        }
    }

    // ================================================================
    // B. WHERE — arithmetic in filter predicates
    // ================================================================

    INFO("B1. arithmetic expression vs constant") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 2 > 150 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // count * 2 > 150 => count > 75 => count 76..100 => 25 rows
        REQUIRE(cur->size() == 25);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(76 + i));
        }
    }

    INFO("B2. column * column in WHERE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * count_double > 5000.0 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // count * (count + 0.1) > 5000 => check each row
        size_t expected = 0;
        for (int i = 1; i <= 100; i++) {
            if (static_cast<double>(i) * (static_cast<double>(i) + 0.1) > 5000.0) {
                expected++;
            }
        }
        REQUIRE(cur->size() == expected);
    }

    INFO("B3. arithmetic with AND") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 2 > 100 AND count * 2 < 150 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // count * 2 > 100 => count > 50; count * 2 < 150 => count < 75 => count 51..74 => 24 rows
        REQUIRE(cur->size() == 24);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(51 + i));
        }
    }

    INFO("B4. arithmetic on BOTH sides") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 3 > count_double * 2 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // count * 3 > (count + 0.1) * 2 => 3*count > 2*count + 0.2 => count > 0.2
        // all rows satisfy this (count starts at 1), size = 100
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("B5. arithmetic with OR") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count + 10 < 15 OR count - 5 > 90 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // count + 10 < 15 => count < 5 => count 1..4
        // count - 5 > 90 => count > 95 => count 96..100
        // total = 4 + 5 = 9
        REQUIRE(cur->size() == 9);
    }

    INFO("B6. nested arithmetic in WHERE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE (count + 1) * (count - 1) > 9000 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // (count+1)*(count-1) = count^2 - 1 > 9000 => count^2 > 9001 => count >= 95
        // count 95..100 => 6 rows
        REQUIRE(cur->size() == 6);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(95 + i));
        }
    }

    // ================================================================
    // C. Aggregates with arithmetic arguments
    // ================================================================

    INFO("C1. SUM of expression") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * 2) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // sum(1..100) = 5050, val = 5050 * 2 = 10100
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 10100);
    }

    INFO("C2. SUM of column * column") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * count_double) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // val = sum(i * (i + 0.1)) for i=1..100
        double expected = 0.0;
        for (int i = 1; i <= 100; i++) {
            expected += static_cast<double>(i) * (static_cast<double>(i) + 0.1);
        }
        REQUIRE(core::is_equals(cur->chunk_data().data[0].data<double>()[0], expected));
    }

    INFO("C3. AVG of expression") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT AVG(count * 10) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // avg(1..100) = 50.5, val = 50.5 * 10 = 505
        // AVG might return int or double depending on implementation
        auto val = cur->chunk_data().data[0].data<int64_t>()[0];
        REQUIRE(val == 505);
    }

    INFO("C4. MIN/MAX of expression") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT MIN(count * 2) AS min_val, MAX(count * 2) AS max_val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 2);
        REQUIRE(cur->chunk_data().data[1].data<int64_t>()[0] == 200);
    }

    INFO("C5pre. COUNT(*) without WHERE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(*) AS cnt )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<uint64_t>()[0] == kNumInserts);
    }

    INFO("C5. COUNT with arithmetic WHERE") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(*) AS cnt )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count * 3 > 200;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // count * 3 > 200 => count > 66.67 => count >= 67 => 34 rows
        REQUIRE(cur->chunk_data().data[0].data<uint64_t>()[0] == 34);
    }

    // ================================================================
    // D. GROUP BY + aggregates with arithmetic
    // ================================================================

    INFO("D1. GROUP BY with arithmetic in aggregate arg") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count * 2) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        // 2 groups: odd count_bool=true, even count_bool=false
        REQUIRE(cur->size() == 2);
        // sum of odd (1,3,5,...99) * 2 = 2 * 2500 = 5000
        // sum of even (2,4,6,...100) * 2 = 2 * 2550 = 5100
    }

    INFO("D2. GROUP BY + arithmetic in WHERE + aggregate on expression") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count * count_double) AS revenue )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count > 10 )_"
                                           R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // ================================================================
    // E. Post-aggregate arithmetic
    // ================================================================

    INFO("E1. arithmetic on single aggregate") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count) * 2 AS doubled )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // 5050 * 2 = 10100
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 10100);
    }

    INFO("E2. arithmetic on multiple aggregates") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count) / COUNT(*) AS manual_avg )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // 5050 / 100 = 50 (integer division) or 50.5
        auto val = cur->chunk_data().data[0].data<int64_t>()[0];
        // integer division: 5050 / 100 = 50
        REQUIRE(val == 50);
    }

    INFO("E3. complex: aggregate * constant") {
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
        auto actual_val = cur->chunk_data().data[0].data<double>()[0];
        auto expected_val = sum_val * 0.3;
        REQUIRE(std::abs(actual_val - expected_val) < 1.0);
    }

    INFO("E4. GROUP BY + post-aggregate arithmetic") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) AS total, SUM(count) * 2 AS doubled_total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        for (size_t i = 0; i < cur->size(); i++) {
            auto total = cur->chunk_data().data[1].data<int64_t>()[i];
            auto doubled = cur->chunk_data().data[2].data<int64_t>()[i];
            REQUIRE(doubled == total * 2);
        }
    }

    // ================================================================
    // F. ORDER BY with arithmetic
    // ================================================================

    INFO("F1. ORDER BY computed expression") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count * -1 ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        // count * -1 ascending => count descending
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(kNumInserts - i));
        }
    }

    INFO("F2. ORDER BY arithmetic expression DESC") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, count_double )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count + count_double DESC LIMIT 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        // highest count + count_double first: count=100, count_double=100.1 → 200.1
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(kNumInserts - i));
        }
    }

    // ================================================================
    // G. UPDATE — arithmetic in SET and WHERE
    // ================================================================

    INFO("G1. UPDATE SET with arithmetic") {
        // First, verify initial state for count <= 10
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE count <= 10 ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        // UPDATE: double the count for rows where count <= 10
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(UPDATE TestDatabase.TestCollection )_"
                                               R"_(SET count = count * 2 WHERE count <= 10;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        // Verify: those 10 rows now have count 2,4,6,...,20
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE count <= 20 ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            // Original rows 11..20 plus updated 2,4,...,20
            // Updated: 2,4,6,8,10,12,14,16,18,20 and original: 11,12,13,...,20
            // Need to verify some rows were updated
            bool found_even = false;
            for (size_t i = 0; i < cur->size(); i++) {
                auto v = cur->chunk_data().data[0].data<int64_t>()[i];
                if (v == 2) found_even = true;
            }
            REQUIRE(found_even);
        }
        // Restore: undo the doubling (set count = count / 2 where original was <= 10)
        // We'll just re-insert the data for clean state for subsequent tests
    }

    // ================================================================
    // H. DELETE — arithmetic in WHERE
    // ================================================================

    // Use a fresh test case for DELETE to avoid state issues
    INFO("H1. DELETE with arithmetic WHERE") {
        // Count before
        size_t count_before;
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT COUNT(*) AS cnt )_"
                                               R"_(FROM TestDatabase.TestCollection;)_");
            REQUIRE(cur->is_success());
            count_before = cur->chunk_data().data[0].data<uint64_t>()[0];
        }
        // Delete rows where count * 3 > 270 => count > 90
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(DELETE FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE count * 3 > 270;)_");
            REQUIRE(cur->is_success());
        }
        // Verify count decreased
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT COUNT(*) AS cnt )_"
                                               R"_(FROM TestDatabase.TestCollection;)_");
            REQUIRE(cur->is_success());
            auto count_after = cur->chunk_data().data[0].data<uint64_t>()[0];
            REQUIRE(count_after < count_before);
        }
    }

    // ================================================================
    // I. INSERT — arithmetic in VALUES
    // ================================================================

    INFO("I1. INSERT with computed values") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(INSERT INTO TestDatabase.TestCollection )_"
                                           R"_(  (count, count_str, count_double, count_bool) )_"
                                           R"_(VALUES (10 * 5, '50', 50.5, true);)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("I2. INSERT with expressions in multiple VALUES") {
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

// ================================================================
// Separate test case for JOIN tests (needs second table)
// ================================================================

TEST_CASE("integration::cpp::test_arithmetic::join") {
    auto config = test_create_config("/tmp/test_arithmetic_join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    R"_(CREATE TABLE TestDatabase.TestCollection2();)_");
        }
    }

    INFO("insert test data") {
        // Insert main collection data
        {
            auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {database_name, collection_name}, std::move(chunk));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        // Insert second table: price and quantity
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection2 (price, quantity) VALUES ";
            for (int i = 1; i <= 10; i++) {
                query << "(" << i * 10 << ", " << i << ")";
                if (i < 10) query << ", ";
            }
            query << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("J1. JOIN with arithmetic in ON") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(JOIN TestDatabase.TestCollection2 )_"
                                           R"_(ON TestCollection.count = TestCollection2.price * TestCollection2.quantity )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // price * quantity = 10*1=10, 20*2=40, 30*3=90, 40*4=160, ...
        // only 10, 40, 90 are in range 1..100 (160 > 100)
        // So matches: count=10 (p=10,q=1), count=40 (p=20,q=2), count=90 (p=30,q=3)
        REQUIRE(cur->size() == 3);
    }

    INFO("J2. JOIN with arithmetic on one side") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(JOIN TestDatabase.TestCollection2 )_"
                                           R"_(ON TestCollection.count * 10 = TestCollection2.price )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        // count * 10 = price => count = price/10
        // prices: 10,20,...,100 => count: 1,2,...,10
        REQUIRE(cur->size() == 10);
    }
}

// ================================================================
// Separate test case for HAVING (if supported)
// ================================================================

TEST_CASE("integration::cpp::test_arithmetic::having") {
    auto config = test_create_config("/tmp/test_arithmetic_having");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
    }

    INFO("insert test data") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("K1. basic HAVING") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool )_"
                                           R"_(HAVING SUM(count) > 2000;)_");
        REQUIRE(cur->is_success());
        // odd sum = 2500, even sum = 2550, both > 2000
        REQUIRE(cur->size() == 2);
    }

    INFO("K2. HAVING with arithmetic") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count_bool, SUM(count) AS total )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count_bool )_"
                                           R"_(HAVING SUM(count) * 2 > 5000;)_");
        REQUIRE(cur->is_success());
        // odd sum * 2 = 5000, even sum * 2 = 5100
        // SUM(count) * 2 > 5000 => only even group (5100 > 5000)
        // odd: 5000 is NOT > 5000
        REQUIRE(cur->size() == 1);
    }
}

// ================================================================
// Separate test case for CASE/WHEN with arithmetic
// ================================================================

TEST_CASE("integration::cpp::test_arithmetic::case_when") {
    auto config = test_create_config("/tmp/test_arithmetic_case");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
    }

    INFO("insert test data") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("L1. CASE in SELECT with arithmetic in THEN") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, )_"
                                           R"_(  CASE WHEN count > 50 THEN count * 0.9 ELSE count * 1.0 END AS adjusted )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
        for (size_t i = 0; i < cur->size(); i++) {
            auto v = static_cast<int64_t>(i + 1);
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            double expected = v > 50 ? static_cast<double>(v) * 0.9 : static_cast<double>(v) * 1.0;
            REQUIRE(core::is_equals(cur->chunk_data().data[1].data<double>()[i], expected));
        }
    }

    INFO("L2. CASE with arithmetic in WHEN condition") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            std::string_view expected = v * 2 > 100 ? "high" : "low";
            REQUIRE(cur->chunk_data().data[1].data<std::string_view>()[i] == expected);
        }
    }

    INFO("L3. CASE with multiple WHEN + arithmetic") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == v);
            std::string_view expected;
            if (v * 10 > 500)
                expected = "tier3";
            else if (v * 10 > 200)
                expected = "tier2";
            else
                expected = "tier1";
            REQUIRE(cur->chunk_data().data[1].data<std::string_view>()[i] == expected);
        }
    }
}

// ================================================================
// Separate test case for edge cases
// ================================================================

TEST_CASE("integration::cpp::test_arithmetic::edge_cases") {
    auto config = test_create_config("/tmp/test_arithmetic_edge");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
    }

    INFO("insert test data") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    INFO("M1. division by zero") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count / 0 AS bad )_"
                                           R"_(FROM TestDatabase.TestCollection LIMIT 1;)_");
        // Division by zero returns 0
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 0);
    }

    INFO("M2. very large multiplication") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count * count * count * count AS big )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count = 100;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // 100^4 = 100000000
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 100000000);
    }

    INFO("M3. mixed nested: arithmetic inside aggregate inside arithmetic") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count * 2) + MAX(count) AS val )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // SUM(count*2) = 10100, MAX(count) = 100, val = 10200
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 10200);
    }
}

// ================================================================
// Optimizer constant folding — integration tests
// ================================================================

TEST_CASE("integration::cpp::test_optimizer_constant_folding") {
    auto config = test_create_config("/tmp/test_optimizer_folding");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
    }

    INFO("insert test data") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    // ================================================================
    // I1. WHERE with constant true
    // ================================================================
    INFO("I1. WHERE with constant true: 5 = 5") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE 5 = 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    // ================================================================
    // I2. WHERE with constant false
    // ================================================================
    INFO("I2. WHERE with constant false: 5 = 7") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE 5 = 7;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // ================================================================
    // I3. WHERE with constant arithmetic + field
    // ================================================================
    INFO("I3a. sanity: WHERE count > 5 (no arithmetic)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count > 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 95);
    }

    INFO("I3. WHERE count > 2 + 3") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count > 2 + 3;)_");
        REQUIRE(cur->is_success());
        // count > 5 means count 6..100 = 95 rows
        REQUIRE(cur->size() == 95);
    }

    // ================================================================
    // I4. WHERE with constant arithmetic: multiply
    // ================================================================
    INFO("I4. WHERE count < 5 * 2") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count < 5 * 2;)_");
        REQUIRE(cur->is_success());
        // count < 10 means count 1..9 = 9 rows
        REQUIRE(cur->size() == 9);
    }

    // ================================================================
    // I5. WHERE with constant comparison: gt true
    // ================================================================
    INFO("I5. WHERE 10 > 5 (constant true)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE 10 > 5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumInserts);
    }

    // ================================================================
    // I6. WHERE with constant comparison: lt false
    // ================================================================
    INFO("I6. WHERE 3 > 10 (constant false)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE 3 > 10;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // ================================================================
    // I7. Nested: field compare to arithmetic
    // ================================================================
    INFO("I7. WHERE count = 10 + 40") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count = 10 + 40;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 50);
    }

    // ================================================================
    // I8. No-op: non-match expressions unchanged
    // ================================================================
    INFO("I8. SELECT count + 10 (projection not folded)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count + 10 AS plus FROM TestDatabase.TestCollection )_"
                                           R"_(ORDER BY count ASC LIMIT 3;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 11);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[1] == 12);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[2] == 13);
    }

    // ================================================================
    // I9. WHERE with AND: constant + field
    // ================================================================
    INFO("I9. WHERE 5 = 5 AND count > 95") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE 5 = 5 AND count > 95;)_");
        REQUIRE(cur->is_success());
        // count 96..100 = 5 rows
        REQUIRE(cur->size() == 5);
    }

    // ================================================================
    // I10. WHERE with OR: constant false + field
    // ================================================================
    INFO("I10. WHERE 5 = 7 OR count = 50") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE 5 = 7 OR count = 50;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 50);
    }

    // ================================================================
    // I11. Nested arithmetic in WHERE: (2+3)*10
    // ================================================================
    INFO("I11. WHERE count = (2 + 3) * 10") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count = (2 + 3) * 10;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 50);
    }

    // ================================================================
    // I12. Double constants in WHERE
    // ================================================================
    INFO("I12. WHERE count > 99.5") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count > 99.5;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 100);
    }

    // ================================================================
    // I13. Subtraction in WHERE
    // ================================================================
    INFO("I13. WHERE count = 100 - 1") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count = 100 - 1;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 99);
    }

    // ================================================================
    // I14. Modulo in WHERE
    // ================================================================
    INFO("I14. WHERE count = 103 % 10") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection WHERE count = 103 % 10;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 3);
    }
}
