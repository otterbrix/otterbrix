#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>

// database_name used indirectly via SQL strings
// static const database_name_t database_name = "testdatabase";

#define CHECK_SQL(QUERY, COUNT)                                                                                        \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == COUNT);                                                                                 \
    } while (false)

// ---------------------------------------------------------------------------
// Test 1: Scale test — INSERT 100K rows, GROUP BY, aggregates
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::scale_100k_group_by") {
    auto config = test_create_config("/tmp/otterbrix/production/scale_100k");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (id bigint, group_name string, value double);");
        }
    }

    INFO("insert 100K rows in batches of 1000") {
        for (int batch = 0; batch < 100; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, group_name, value) VALUES ";
            for (int i = 0; i < 1000; ++i) {
                int id = batch * 1000 + i;
                if (i > 0)
                    ss << ",";
                ss << "(" << id << ", 'group_" << (id % 50) << "', " << (id * 1.5) << ")";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1000);
        }
    }

    INFO("verify total count") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 100000);
    }

    INFO("GROUP BY with COUNT") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT group_name, COUNT(id) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY group_name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50);
        // Each group should have exactly 2000 rows
        for (size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->chunk_data().value(1, i).value<uint64_t>() == 2000);
        }
    }
}

// ---------------------------------------------------------------------------
// Test 2: Multi-table JOIN + aggregates (2 JOINs)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::multi_table_join") {
    auto config = test_create_config("/tmp/otterbrix/production/multi_join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        // Create 3 tables
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.orders (order_id bigint, customer_id bigint, amount bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.customers (id bigint, name string, city string);");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.cities (city string, country string);");
        }
    }

    INFO("insert cities") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.cities (city, country) VALUES "
                                           "('NYC', 'USA'), ('London', 'UK'), ('Paris', 'France'), "
                                           "('Berlin', 'Germany'), ('Tokyo', 'Japan');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("insert customers") {
        std::string cities[] = {"NYC", "London", "Paris", "Berlin", "Tokyo"};
        std::stringstream ss;
        ss << "INSERT INTO TestDatabase.customers (id, name, city) VALUES ";
        for (int i = 0; i < 20; ++i) {
            if (i > 0)
                ss << ", ";
            ss << "(" << i << ", 'Customer_" << i << "', '" << cities[i % 5] << "')";
        }
        ss << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, ss.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20);
    }

    INFO("insert orders") {
        std::stringstream ss;
        ss << "INSERT INTO TestDatabase.orders (order_id, customer_id, amount) VALUES ";
        for (int i = 0; i < 200; ++i) {
            if (i > 0)
                ss << ", ";
            ss << "(" << i << ", " << (i % 20) << ", " << ((i % 10 + 1) * 100) << ")";
        }
        ss << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, ss.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 200);
    }

    INFO("2-table JOIN: orders + customers") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT c.name, COUNT(o.order_id) AS order_count, SUM(o.amount) AS total "
                                           "FROM TestDatabase.orders o "
                                           "INNER JOIN TestDatabase.customers c ON o.customer_id = c.id "
                                           "GROUP BY c.name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20); // 20 customers, each with 10 orders
        for (size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->chunk_data().value(1, i).value<uint64_t>() == 10);
        }
    }

    INFO("2-table JOIN: customers + cities") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ci.country, COUNT(c.id) AS customer_count "
                                           "FROM TestDatabase.customers c "
                                           "INNER JOIN TestDatabase.cities ci ON c.city = ci.city "
                                           "GROUP BY ci.country;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5); // 5 countries, each with 4 customers
        for (size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->chunk_data().value(1, i).value<uint64_t>() == 4);
        }
    }
}

// ---------------------------------------------------------------------------
// Test 3: NULL in JOIN keys — SQL standard: NULL = NULL → UNKNOWN (false)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::null_join_keys") {
    auto config = test_create_config("/tmp/otterbrix/production/null_join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.table_a (id bigint, label string);");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.table_b (id bigint, tag string);");
        }
    }

    INFO("insert data with NULLs") {
        // Table A: rows with id = 1, 2, 4 and two rows with NULL id
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.table_a (id, label) VALUES "
                                               "(1, 'a1'), (2, 'a2'), (4, 'a4');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            // Insert rows with NULL id
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.table_a (label) VALUES "
                                               "('a_null_1'), ('a_null_2');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        // Table B: rows with id = 2, 4, 5 and one row with NULL id
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.table_b (id, tag) VALUES "
                                               "(2, 'b2'), (4, 'b4'), (5, 'b5');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.table_b (tag) VALUES ('b_null');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("INNER JOIN: NULL keys excluded") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.label, b.tag "
                                           "FROM TestDatabase.table_a a "
                                           "INNER JOIN TestDatabase.table_b b ON a.id = b.id;");
        REQUIRE(cur->is_success());
        // SQL standard: NULL = NULL → UNKNOWN → false
        // Matches: (2,b2), (4,b4) only — NULL keys excluded
        REQUIRE(cur->size() == 2);
    }

    INFO("LEFT JOIN with NULL keys") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.label, b.tag "
                                           "FROM TestDatabase.table_a a "
                                           "LEFT JOIN TestDatabase.table_b b ON a.id = b.id;");
        REQUIRE(cur->is_success());
        // table_a has 5 rows: a1(id=1), a2(id=2), a4(id=4), a_null_1(id=NULL), a_null_2(id=NULL)
        // LEFT JOIN: all rows from table_a preserved; NULL keys find no match (NULL=NULL is false)
        // Expected: (a1,NULL), (a2,b2), (a4,b4), (a_null_1,NULL), (a_null_2,NULL) = 5 rows
        REQUIRE(cur->size() == 5);
    }

    INFO("verify NULL rows exist in both tables") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.table_a WHERE id IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.table_b WHERE id IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("INNER JOIN with filter on non-NULL rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.label, b.tag "
                                           "FROM TestDatabase.table_a a "
                                           "INNER JOIN TestDatabase.table_b b ON a.id = b.id "
                                           "AND a.id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

// ---------------------------------------------------------------------------
// Test 4: Unicode strings
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::unicode_strings") {
    auto config = test_create_config("/tmp/otterbrix/production/unicode");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, name string);");
        }
    }

    INFO("insert unicode data") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                           "(1, 'Hello World'), "
                                           "(2, 'Привет мир'), "
                                           "(3, 'emoji_test_fire');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("exact match on ASCII") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name = 'Hello World';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("exact match on Cyrillic") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name = 'Привет мир';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("LIKE with ASCII pattern") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE '%emoji%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("LIKE with Cyrillic pattern") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE '%Привет%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("select all returns correct count") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

// ---------------------------------------------------------------------------
// Test 5: Concurrent INSERT (2 threads)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::concurrent_insert") {
    auto config = test_create_config("/tmp/otterbrix/production/concurrent_insert");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, thread_id bigint);");
        }
    }

    INFO("concurrent inserts from 2 threads") {
        auto insert_batch = [&](int start, int end, int thread_num) {
            for (int batch_start = start; batch_start < end; batch_start += 50) {
                int batch_end = std::min(batch_start + 50, end);
                std::stringstream ss;
                ss << "INSERT INTO TestDatabase.TestCollection (id, thread_id) VALUES ";
                for (int i = batch_start; i < batch_end; ++i) {
                    if (i > batch_start)
                        ss << ", ";
                    ss << "(" << i << ", " << thread_num << ")";
                }
                ss << ";";
                auto session = otterbrix::session_id_t();
                dispatcher->execute_sql(session, ss.str());
            }
        };

        std::thread t1([&]() { insert_batch(0, 500, 1); });
        std::thread t2([&]() { insert_batch(500, 1000, 2); });
        t1.join();
        t2.join();
    }

    INFO("verify all rows inserted") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 1000);
    }

    INFO("verify thread 1 rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection "
                                           "WHERE thread_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 500);
    }

    INFO("verify thread 2 rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection "
                                           "WHERE thread_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 500);
    }
}

// ---------------------------------------------------------------------------
// Test 6: Concurrent read + write
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::concurrent_read_write") {
    auto config = test_create_config("/tmp/otterbrix/production/concurrent_rw");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, value bigint);");
        }
    }

    INFO("concurrent writer + reader") {
        std::atomic<bool> writer_done{false};
        std::atomic<uint64_t> max_count_seen{0};
        std::atomic<bool> reader_saw_decrease{false};

        std::thread writer([&]() {
            for (int batch_start = 0; batch_start < 500; batch_start += 50) {
                std::stringstream ss;
                ss << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";
                for (int i = batch_start; i < batch_start + 50; ++i) {
                    if (i > batch_start)
                        ss << ", ";
                    ss << "(" << i << ", " << (i * 10) << ")";
                }
                ss << ";";
                auto session = otterbrix::session_id_t();
                dispatcher->execute_sql(session, ss.str());
            }
            writer_done = true;
        });

        std::thread reader([&]() {
            while (!writer_done.load()) {
                auto session = otterbrix::session_id_t();
                auto cur =
                    dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
                if (cur->is_success() && cur->size() == 1) {
                    auto count = cur->chunk_data().value(0, 0).value<uint64_t>();
                    auto prev_max = max_count_seen.load();
                    if (count > prev_max) {
                        max_count_seen.store(count);
                    } else if (count < prev_max) {
                        reader_saw_decrease.store(true);
                    }
                }
            }
        });

        writer.join();
        reader.join();

        // Count should never decrease (monotonic growth)
        REQUIRE_FALSE(reader_saw_decrease.load());
    }

    INFO("verify final count") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 500);
    }
}

// ---------------------------------------------------------------------------
// Test 7: Large batch checkpoint (100K rows)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::large_checkpoint_100k") {
    auto config = test_create_config("/tmp/otterbrix/production/large_checkpoint");
    test_clear_directory(config);

    // Compute expected sum: sum of i*1.5 for i=0..99999 = 1.5 * (99999*100000/2) = 1.5 * 4999950000 = 7499925000
    constexpr int64_t expected_count = 100000;

    INFO("phase 1: insert 100K rows and checkpoint") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection (id bigint, value bigint) "
                                    "WITH (storage = 'disk');");
        }

        // Insert 100K rows in batches of 1000
        for (int batch = 0; batch < 100; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";
            for (int i = 0; i < 1000; ++i) {
                int id = batch * 1000 + i;
                if (i > 0)
                    ss << ",";
                ss << "(" << id << ", " << (id * 2) << ")";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1000);
        }

        // Verify before checkpoint
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == uint64_t(expected_count));
        }

        // Checkpoint
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify all 100K rows survived") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == uint64_t(expected_count));
        }

        // Spot-check specific values
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 0;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 50000;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 99999;", 1);
    }
}

// ---------------------------------------------------------------------------
// Test 8: Complex WHERE with nested AND/OR
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::complex_where") {
    auto config = test_create_config("/tmp/otterbrix/production/complex_where");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection "
                                    "(id bigint, category string, value bigint, status string);");
        }
    }

    INFO("insert 100 rows") {
        // id: 1..100, category: A/B/C (cycle), value: 1..100, status: active/inactive (cycle)
        std::stringstream ss;
        ss << "INSERT INTO TestDatabase.TestCollection (id, category, value, status) VALUES ";
        std::string cats[] = {"A", "B", "C"};
        std::string stats[] = {"active", "inactive"};
        for (int i = 1; i <= 100; ++i) {
            if (i > 1)
                ss << ", ";
            ss << "(" << i << ", '" << cats[(i - 1) % 3] << "', " << i << ", '" << stats[(i - 1) % 2] << "')";
        }
        ss << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, ss.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100);
    }

    INFO("complex WHERE: (category = 'A' AND value > 50) OR (category = 'B' AND status = 'inactive')") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE (category = 'A' AND value > 50) "
                                           "OR (category = 'B' AND status = 'inactive');");
        REQUIRE(cur->is_success());
        // Category A, value > 50: ids where (id-1)%3==0 and id>50
        // ids: 52,55,58,61,64,67,70,73,76,79,82,85,88,91,94,97,100 = 17
        // Category B, status inactive: ids where (id-1)%3==1 and (id-1)%2==1
        //   meaning id%3==2 and id%2==0 → id in {2,8,14,20,26,32,38,44,50,56,62,68,74,80,86,92,98} = 17
        REQUIRE(cur->size() == 34);
    }

    INFO("complex WHERE: value > 20 AND value <= 40 AND category IN ('A', 'C')") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE value > 20 AND value <= 40 "
                                           "AND category IN ('A', 'C');");
        REQUIRE(cur->is_success());
        // value 21..40 = 20 values. Among them, category A or C: (id-1)%3==0 or (id-1)%3==2
        // That's 2/3 of values. In 20 values: ids 21-40
        // A: (id-1)%3==0 → id=22,25,28,31,34,37,40 = 7
        // C: (id-1)%3==2 → id=21,24,27,30,33,36,39 = 7
        // Total: 14
        REQUIRE(cur->size() == 14);
    }

    INFO("nested AND/OR: status != 'inactive' AND (value < 10 OR value > 90)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE status != 'inactive' AND (value < 10 OR value > 90);");
        REQUIRE(cur->is_success());
        // status active: (id-1)%2==0 → odd ids: 1,3,5,7,9,11,...,99
        // value < 10: ids 1..9, active among them: 1,3,5,7,9 = 5
        // value > 90: ids 91..100, active among them: 91,93,95,97,99 = 5
        REQUIRE(cur->size() == 10);
    }
}

// ---------------------------------------------------------------------------
// Test 9: Corrupted .otbx recovery
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::corrupted_otbx_recovery") {
    auto config = test_create_config("/tmp/otterbrix/production/corrupted_otbx");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert, checkpoint") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection (id bigint, name string) "
                                    "WITH (storage = 'disk');");
        }

        // Insert 50 rows
        {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, name) VALUES ";
            for (int i = 0; i < 50; ++i) {
                if (i > 0)
                    ss << ", ";
                ss << "(" << i << ", 'row_" << i << "')";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        // Checkpoint to flush to .otbx
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("corrupt the .otbx file") {
        // On-disk layout is oid-keyed: ${path}/${db_oid}/${tbl_oid}/table.otbx.
        // The test creates exactly one user table (TestDatabase.TestCollection); find
        // its .otbx by walking for the first DB dir whose numeric name is >=
        // FIRST_USER_OID and contains a table dir with table.otbx.
        std::filesystem::path otbx_path;
        for (const auto& db_dir : std::filesystem::directory_iterator(config.disk.path)) {
            if (!db_dir.is_directory())
                continue;
            std::uint64_t db_oid = 0;
            try {
                db_oid = std::stoull(db_dir.path().filename().string());
            } catch (...) {
                continue;
            }
            if (db_oid < components::catalog::FIRST_USER_OID)
                continue;
            for (const auto& tbl_dir : std::filesystem::directory_iterator(db_dir.path())) {
                if (!tbl_dir.is_directory())
                    continue;
                auto candidate = tbl_dir.path() / "table.otbx";
                if (std::filesystem::exists(candidate)) {
                    otbx_path = candidate;
                    break;
                }
            }
            if (!otbx_path.empty())
                break;
        }
        REQUIRE(std::filesystem::exists(otbx_path));

        auto file_size = std::filesystem::file_size(otbx_path);
        REQUIRE(file_size > 1024);

        // Write garbage in the middle of the file
        {
            std::fstream f(otbx_path, std::ios::binary | std::ios::in | std::ios::out);
            REQUIRE(f.is_open());
            f.seekp(1024);
            char garbage[64];
            std::fill(std::begin(garbage), std::end(garbage), static_cast<char>(0xDE));
            f.write(garbage, sizeof(garbage));
        }
    }

    INFO("restart after corruption: must not crash") {
        // The key assertion: the process does not SIGSEGV or abort.
        // It's acceptable if load fails gracefully (empty table, exception caught, etc.)
        bool crashed = false;
        try {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();

            // Try to query — may return 0 rows or throw, both are acceptable
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            // If we get here, the engine handled corruption gracefully
            REQUIRE(cur != nullptr);
        } catch (const std::exception& /*e*/) {
            // Exception is acceptable — corruption was detected
            crashed = false;
        } catch (...) {
            // Unknown exception is also acceptable
            crashed = false;
        }
        REQUIRE_FALSE(crashed);
    }
}

// ---------------------------------------------------------------------------
// Test 10: WAL segment rotation under load
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::wal_segment_rotation") {
    auto config = test_create_config("/tmp/otterbrix/production/wal_rotation");
    test_clear_directory(config);
    // disk.on = true for catalog persistence (needed for restart recovery)
    // Table uses in-memory storage (no WITH storage='disk') so data comes from WAL replay
    config.wal.max_segment_size = 4 * 1024; // 4 KB — force small segments

    INFO("phase 1: insert 500 rows (one by one to force many WAL records)") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, data string);");
        }

        // Insert in small batches to generate many WAL records
        for (int batch = 0; batch < 50; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, data) VALUES ";
            for (int i = 0; i < 10; ++i) {
                int id = batch * 10 + i;
                if (i > 0)
                    ss << ", ";
                ss << "(" << id << ", 'data_value_" << id << "_padding_for_size')";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }

        // Verify before restart
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 500);
        }
    }

    INFO("check WAL segment files exist") {
        // With 4KB segments and 500 rows of data, there should be multiple WAL files
        // New WAL puts files in per-database subdirectories
        int wal_file_count = 0;
        if (std::filesystem::exists(config.wal.path)) {
            for (const auto& database_entry : std::filesystem::directory_iterator(config.wal.path)) {
                if (database_entry.is_directory()) {
                    for (const auto& segment_entry : std::filesystem::directory_iterator(database_entry.path())) {
                        if (segment_entry.is_regular_file()) {
                            auto filename = segment_entry.path().filename().string();
                            if (filename.find("wal_") == 0) {
                                ++wal_file_count;
                            }
                        }
                    }
                }
            }
        }
        // Should have at least 2 WAL files (rotation happened)
        REQUIRE(wal_file_count >= 2);
    }

    INFO("phase 2: restart and verify all data recovered from segmented WAL") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 500);
        }

        // Spot-check
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 0;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 250;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 499;", 1);
    }
}

// ---------------------------------------------------------------------------
// Test 11: Compaction + checkpoint cycle (VACUUM + CHECKPOINT + restart)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::production::compaction_checkpoint_cycle") {
    auto config = test_create_config("/tmp/otterbrix/production/compaction_cycle");
    test_clear_directory(config);

    INFO("phase 1: insert 1000, delete 80%, vacuum, checkpoint") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection (id bigint, value bigint) "
                                    "WITH (storage = 'disk');");
        }

        // Insert 1000 rows
        for (int batch = 0; batch < 10; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";
            for (int i = 0; i < 100; ++i) {
                int id = batch * 100 + i + 1; // id 1..1000
                if (i > 0)
                    ss << ", ";
                ss << "(" << id << ", " << (id * 10) << ")";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }

        // Verify 1000 rows
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection;", 1000);

        // Delete rows where id > 200 (removes 800 rows)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id > 200;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 800);
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);

        // VACUUM to compact
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "VACUUM;");
            REQUIRE(cur->is_success());
        }

        // Verify after vacuum
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify compacted data") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 200);
        }

        // Verify boundary values
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 1;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 200;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 201;", 0);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 1000;", 0);
    }
}

// Large-table-scan segfault repro (SSB q1-1 shape), DISK-backed e2e guard.
//
// On pre-fix code a q1-1-style aggregate over a WIDE table (mirroring SSB
// lineorder, 17 columns) SIGSEGVs mid-scan (EXC_BAD_ACCESS, null buffer in
// standard_buffer_manager_t::pin). The fix has two parts the disk-backed path
// here exercises: an eviction guard (managed, non-reloadable blocks can never
// be unloaded, so re-pinning an evicted segment never derefs a null buffer) and
// disk-backed write-through (a filled segment is flushed to the data file and
// gets a real reloadable block_id, so the pool can evict + reload it and large
// inserts stay BOUNDED instead of pinning the whole table resident). Post-fix
// this must (a) accept every INSERT batch without OOM and (b) complete the large
// scan with the CORRECT aggregate — prove COMPLETION, not just no-crash.
//
// DISK-backed (config.disk.on, storage = 'disk') so write-through is actually
// exercised — an in-memory table would pin the whole working set and clean-OOM
// by design, which cannot validate the write-through bound.
//
// Working-set / pool note: there is NO buffer-pool / memory-limit knob in
// configuration::config — the pool size is hardcoded (4 GiB) inside the disk
// service (services/disk/manager_disk.cpp) and is only overridable
// programmatically (set_memory_limit), which production never calls and a test
// at this e2e layer cannot reach. So we cannot shrink the pool to force
// eviction at a small row count from here; that path is covered directly at the
// unit layer (components/table/test/test_disk_backed_scan.cpp, which uses a tiny
// pool). Here the row count is chosen as the largest WIDE-table scan that keeps
// this Debug-build case to a few seconds while still meaningfully driving the
// disk-backed append + full-scan path end to end.
TEST_CASE("integration::cpp::production::large_scan_segfault_red", "[step1]") {
    auto config = test_create_config("/tmp/otterbrix/production/large_scan_segfault");
    test_clear_directory(config);
    // DISK-backed so write-through evicts filled segments and large inserts stay
    // bounded.
    config.disk.on = true;
    config.wal.on = true;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr int batches = 50;
    constexpr int rows_per_batch = 1000;
    constexpr int64_t total_rows = int64_t(batches) * rows_per_batch;

    // Mirror the row-generation formulas below to compute the EXPECTED q1-1
    // aggregate and matching-row count up front, so the assertions prove the
    // scan returned the right value (not merely "no crash").
    int64_t expected_revenue = 0;
    int64_t expected_match_rows = 0;
    for (int64_t id = 0; id < total_rows; ++id) {
        const int64_t year = 1992 + (id % 7);
        const int64_t discount = id % 11;            // 0..10
        const int64_t quantity = 1 + (id % 50);      // 1..50
        const int64_t extprice = 1000 + (id % 9000); // 1000..9999
        if (year == 1993 && discount >= 1 && discount <= 3 && quantity < 25) {
            expected_revenue += extprice * discount;
            ++expected_match_rows;
        }
    }
    REQUIRE(expected_match_rows > 0); // the filter must select a non-trivial subset

    INFO("initialization: WIDE DISK table mirroring SSB lineorder (17 columns)") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            // Mirror the SSB lineorder shape: many wide bigint columns plus a few
            // text columns, so a 1024-row row-group is a large working set.
            // storage = 'disk' enables write-through for this table.
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.Lineorder ("
                                    "lo_orderkey bigint, "
                                    "lo_linenumber bigint, "
                                    "lo_custkey bigint, "
                                    "lo_partkey bigint, "
                                    "lo_suppkey bigint, "
                                    "lo_orderdate bigint, "
                                    "lo_orderpriority string, "
                                    "lo_shippriority string, "
                                    "lo_quantity bigint, "
                                    "lo_extendedprice bigint, "
                                    "lo_ordtotalprice bigint, "
                                    "lo_discount bigint, "
                                    "lo_revenue bigint, "
                                    "lo_supplycost bigint, "
                                    "lo_tax bigint, "
                                    "lo_commitdate bigint, "
                                    "lo_shipmode string) "
                                    "WITH (storage = 'disk');");
        }
    }

    INFO("insert wide rows in batches of 1000") {
        // Derive every column from the row index so the data is varied and the
        // q1-1 filters select a non-trivial subset:
        //   lo_orderdate  -> year, 1992..1998 (so d_year = 1993 selects ~1/7)
        //   lo_discount   -> 0..10            (BETWEEN 1 AND 3 selects 3/11)
        //   lo_quantity   -> 1..50            (< 25 selects ~half)
        for (int batch = 0; batch < batches; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.Lineorder ("
                  "lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, "
                  "lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, "
                  "lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, "
                  "lo_supplycost, lo_tax, lo_commitdate, lo_shipmode) VALUES ";
            for (int i = 0; i < rows_per_batch; ++i) {
                const int64_t id = int64_t(batch) * rows_per_batch + i;
                const int64_t year = 1992 + (id % 7);
                const int64_t discount = id % 11;            // 0..10
                const int64_t quantity = 1 + (id % 50);      // 1..50
                const int64_t extprice = 1000 + (id % 9000); // 1000..9999
                if (i > 0)
                    ss << ",";
                ss << "(" << id                     // lo_orderkey
                   << ", " << (1 + (id % 7))        // lo_linenumber
                   << ", " << (id % 30000)          // lo_custkey
                   << ", " << (id % 200000)         // lo_partkey
                   << ", " << (id % 2000)           // lo_suppkey
                   << ", " << year                  // lo_orderdate (== year here)
                   << ", '1-URGENT'"                // lo_orderpriority
                   << ", '0'"                       // lo_shippriority
                   << ", " << quantity              // lo_quantity
                   << ", " << extprice              // lo_extendedprice
                   << ", " << (extprice * quantity) // lo_ordtotalprice
                   << ", " << discount              // lo_discount
                   << ", " << (extprice * discount) // lo_revenue
                   << ", " << (extprice / 2)        // lo_supplycost
                   << ", " << (id % 8)              // lo_tax
                   << ", " << year                  // lo_commitdate
                   << ", 'MAIL')";                  // lo_shipmode
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            // Write-through must keep each batch BOUNDED: a clean success is
            // required. An OOM here means the bound is not working — do NOT
            // weaken this; investigate instead.
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == rows_per_batch);
        }
    }

    INFO("q1-1-style aggregate over the full WIDE table (the large scan)") {
        // On pre-fix code the process SIGSEGVs during this scan. On the fixed
        // code it must COMPLETE with the correct aggregate.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT SUM(lo_extendedprice * lo_discount) AS revenue "
                                           "FROM TestDatabase.Lineorder "
                                           "WHERE lo_orderdate = 1993 "
                                           "AND lo_discount BETWEEN 1 AND 3 "
                                           "AND lo_quantity < 25;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // SUM over bigint expressions comes back as a signed scalar. Match the
        // value computed from the same generation formulas above: proves the
        // large scan COMPLETED with the correct result, not just no-crash.
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == expected_revenue);
    }

    INFO("sanity: full-table count scans cleanly and returns every row") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(lo_orderkey) AS cnt FROM TestDatabase.Lineorder;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == uint64_t(total_rows));
    }
}

// REGRESSION: a disk-backed table that is CHECKPOINTed and then reopened must
// still resolve its columns and run a q1-1-style aggregate. This exercises the
// full bootstrap → resolve_table path after reopen, including the MVCC
// commit-clock re-seed. Without that seed, a reopened instance whose persisted
// pg_attribute columns carry a non-zero added_at_commit_id from the prior
// session would judge every column "added after my snapshot" (start_time reset
// to 1) and resolution would fail "<col> not found".
//
// WAL stays on so the user rows survive the reopen and the post-reopen
// aggregate can be value-checked end to end.
TEST_CASE("integration::cpp::production::reopen_resolves_columns_after_checkpoint") {
    auto config = test_create_config("/tmp/otterbrix/production/reopen_resolve_columns");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;

    INFO("phase 1: disk-backed CREATE TABLE, INSERT, CHECKPOINT") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            // Disk-backed so the rows survive reopen and the aggregate below is
            // value-checkable; the SSB lineorder column shape.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.Lineorder ("
                                               "lo_orderkey bigint, "
                                               "lo_orderdate bigint, "
                                               "lo_quantity bigint, "
                                               "lo_extendedprice bigint, "
                                               "lo_discount bigint) "
                                               "WITH (storage = 'disk');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.Lineorder "
                "(lo_orderkey, lo_orderdate, lo_quantity, lo_extendedprice, lo_discount) VALUES "
                "(1, 1993, 10, 1000, 2), (2, 1994, 30, 2000, 5), (3, 1993, 20, 1500, 1);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            // CHECKPOINT folds the durable frontier into pg_attribute and the
            // user table's row-groups into its .otbx.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: REOPEN and resolve a column — must NOT fail 'not found'") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // The pre-fix failure was column resolution: SELECT <col> reported the
        // column as not found. Assert the resolve path succeeds.
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT lo_orderdate FROM TestDatabase.Lineorder WHERE lo_orderdate = 1993;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        // And a q1-1-style aggregate over the reopened table resolves every
        // referenced column and returns the correct value.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT SUM(lo_extendedprice * lo_discount) AS revenue "
                                               "FROM TestDatabase.Lineorder "
                                               "WHERE lo_orderdate = 1993 "
                                               "AND lo_discount BETWEEN 1 AND 3 "
                                               "AND lo_quantity < 25;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            // row 1: 1000*2=2000 (orderdate 1993, disc 2, qty 10<25) — matches
            // row 3: 1500*1=1500 (orderdate 1993, disc 1, qty 20<25) — matches
            // row 2: orderdate 1994 — excluded
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3500);
        }
    }
}
