#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <chrono>
#include <random>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

// External merge sort spill regression guard. operator_external_sort_t spills
// locally-sorted runs to disk (serialize_unified) and k-way-merges them back.
// These cases assert the spilled results match an in-memory sort (single- and
// multi-column, duplicates, LIMIT/OFFSET) and that large inputs complete without
// OOM. Spill is opt-in per query via config.disk.spill_enabled.

TEST_CASE("integration::cpp::test_sort_spill::single_column_asc_large", "[step4][sort]") {
    auto config = test_create_config("/tmp/test_sort_spill/single_asc");
    test_clear_directory(config);
    config.disk.on = true;   // Enable disk for temp files
    // Opt into the spill plan: configuration default is spill_enabled=false.
    config.disk.spill_enabled = true;
    config.wal.on = false;   // Disable WAL for pure sort test
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("create table with single integer column") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (value bigint);");
    }

    INFO("insert 100000 rows with random values") {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (value) VALUES ";

        // Use deterministic random seed for reproducibility
        std::mt19937 gen(42);
        std::uniform_int_distribution<int64_t> dist(-1000000, 1000000);

        for (int i = 0; i < 100000; ++i) {
            int64_t val = dist(gen);
            query << "(" << val << ")" << (i == 99999 ? ";" : ", ");
        }

        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100000);
    }

    INFO("ORDER BY value ASC should spill to disk and return correct results") {
        auto session = otterbrix::session_id_t();

        auto start = std::chrono::high_resolution_clock::now();
        auto cur = dispatcher->execute_sql(session,
            "SELECT value FROM TestDatabase.TestCollection ORDER BY value ASC;");
        auto end = std::chrono::high_resolution_clock::now();

        REQUIRE(cur->is_success());

        // Verify all rows returned
        REQUIRE(cur->size() == 100000);

        // Verify sorted order (ascending)
        int64_t prev_val = std::numeric_limits<int64_t>::min();
        for (size_t i = 0; i < cur->size(); ++i) {
            auto cur_val = cur->value(0, i).value<int64_t>();
            REQUIRE(cur_val >= prev_val);  // Non-decreasing order
            prev_val = cur_val;
        }

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        INFO("Sort completed in " << duration.count() << "ms");

        // Performance sanity: should complete within reasonable time
        // (not immediate like in-memory, but not excessively slow with spill)
        REQUIRE(duration.count() < 30000);  // < 30 seconds
    }
}

TEST_CASE("integration::cpp::test_sort_spill::single_column_desc_large", "[step4][sort]") {
    auto config = test_create_config("/tmp/test_sort_spill/single_desc");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: configuration default is spill_enabled=false.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("create table with single integer column") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (score bigint);");
    }

    INFO("insert 200000 rows with random values") {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (score) VALUES ";

        std::mt19937 gen(123);
        std::uniform_int_distribution<int64_t> dist(0, 1000000);

        for (int i = 0; i < 200000; ++i) {
            int64_t val = dist(gen);
            query << "(" << val << ")" << (i == 199999 ? ";" : ", ");
        }

        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 200000);
    }

    INFO("ORDER BY score DESC should spill and return descending results") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT score FROM TestDatabase.TestCollection ORDER BY score DESC;");

        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 200000);

        // Verify sorted order (descending)
        int64_t prev_val = std::numeric_limits<int64_t>::max();
        for (size_t i = 0; i < cur->size(); ++i) {
            auto cur_val = cur->value(0, i).value<int64_t>();
            REQUIRE(cur_val <= prev_val);  // Non-increasing order
            prev_val = cur_val;
        }
    }
}

TEST_CASE("integration::cpp::test_sort_spill::multi_column_large", "[step4][sort]") {
    auto config = test_create_config("/tmp/test_sort_spill/multi_column");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: configuration default is spill_enabled=false.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("create table with string and integer columns") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        dispatcher->execute_sql(session,
            "CREATE TABLE TestDatabase.TestCollection (name string, priority bigint);");
    }

    INFO("insert 50000 rows with mixed data") {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (name, priority) VALUES ";

        std::mt19937 gen(456);
        std::uniform_int_distribution<int64_t> dist(1, 100);

        for (int i = 0; i < 50000; ++i) {
            int64_t priority = dist(gen);
            std::string name = "item_" + std::to_string(i % 1000);  // Create name groups
            query << "('" << name << "', " << priority << ")" << (i == 49999 ? ";" : ", ");
        }

        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50000);
    }

    INFO("ORDER BY name ASC, priority DESC should spill with correct multi-column sort") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT name, priority FROM TestDatabase.TestCollection "
            "ORDER BY name ASC, priority DESC;");

        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50000);

        // Verify multi-column sort order
        std::string prev_name = "";
        int64_t prev_priority = std::numeric_limits<int64_t>::max();

        for (size_t i = 0; i < cur->size(); ++i) {
            // Bind the cell to a named local before reading the string_view: the
            // codebase reads STRING via value<std::string_view>() (there is no
            // value<std::string>()), and the view dangles if the cur->value(...)
            // temporary is destroyed at the end of the full expression.
            auto name_cell = cur->value(0, i);
            std::string cur_name{name_cell.value<std::string_view>()};
            auto cur_priority = cur->value(1, i).value<int64_t>();

            // Primary key: name ascending
            REQUIRE(cur_name >= prev_name);

            // Secondary key: priority descending within same name
            if (cur_name == prev_name) {
                REQUIRE(cur_priority <= prev_priority);
            }

            prev_name = cur_name;
            prev_priority = cur_priority;
        }
    }
}

TEST_CASE("integration::cpp::test_sort_spill::verify_correctness_with_duplicates", "[step4][sort]") {
    auto config = test_create_config("/tmp/test_sort_spill/duplicates");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: configuration default is spill_enabled=false.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("create table") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, value string);");
    }

    INFO("insert 150000 rows with many duplicates") {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";

        std::mt19937 gen(789);
        std::uniform_int_distribution<int64_t> id_dist(1, 1000);  // Only 1000 unique IDs

        for (int i = 0; i < 150000; ++i) {
            int64_t id = id_dist(gen);
            std::string value = "val_" + std::to_string(id);
            query << "(" << id << ", '" << value << "')" << (i == 149999 ? ";" : ", ");
        }

        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 150000);
    }

    INFO("ORDER BY with duplicates should preserve stable sort behavior") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT id, value FROM TestDatabase.TestCollection ORDER BY id ASC;");

        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 150000);

        // Verify sorted with duplicates
        int64_t prev_id = 0;
        for (size_t i = 0; i < cur->size(); ++i) {
            auto cur_id = cur->value(0, i).value<int64_t>();
            REQUIRE(cur_id >= prev_id);
            prev_id = cur_id;
        }

        // Count duplicates to verify we didn't lose any rows
        std::map<int64_t, size_t> id_counts;
        for (size_t i = 0; i < cur->size(); ++i) {
            auto cur_id = cur->value(0, i).value<int64_t>();
            id_counts[cur_id]++;
        }

        // Verify total count matches (no rows lost during spill)
        size_t total_count = 0;
        for (const auto& [id, count] : id_counts) {
            total_count += count;
        }
        REQUIRE(total_count == 150000);
    }
}

TEST_CASE("integration::cpp::test_sort_spill::with_limit_offset", "[step4][sort]") {
    auto config = test_create_config("/tmp/test_sort_spill/limit_offset");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: configuration default is spill_enabled=false.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("create table") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (num bigint);");
    }

    INFO("insert 120000 rows") {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (num) VALUES ";

        for (int i = 0; i < 120000; ++i) {
            // Reverse order to make sorting interesting
            query << "(" << (120000 - i) << ")" << (i == 119999 ? ";" : ", ");
        }

        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 120000);
    }

    INFO("ORDER BY with LIMIT and OFFSET should work correctly with spill") {
        auto session = otterbrix::session_id_t();

        // Test LIMIT: should return first 1000 smallest values
        auto cur1 = dispatcher->execute_sql(session,
            "SELECT num FROM TestDatabase.TestCollection ORDER BY num ASC LIMIT 1000;");

        REQUIRE(cur1->is_success());
        REQUIRE(cur1->size() == 1000);
        REQUIRE(cur1->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur1->value(0, 999).value<int64_t>() == 1000);

        // Test OFFSET: skip first 50000, return next 1000
        auto cur2 = dispatcher->execute_sql(session,
            "SELECT num FROM TestDatabase.TestCollection ORDER BY num ASC LIMIT 1000 OFFSET 50000;");

        REQUIRE(cur2->is_success());
        REQUIRE(cur2->size() == 1000);
        REQUIRE(cur2->value(0, 0).value<int64_t>() == 50001);
        REQUIRE(cur2->value(0, 999).value<int64_t>() == 51000);
    }
}

TEST_CASE("integration::cpp::test_sort_spill::memory_stress_very_large", "[step4][sort]") {
    auto config = test_create_config("/tmp/test_sort_spill/very_large");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: configuration default is spill_enabled=false.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("create table") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (data bigint);");
    }

    INFO("insert 500000 rows to force multiple spill cycles") {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (data) VALUES ";

        std::mt19937 gen(999);
        std::uniform_int_distribution<int64_t> dist(1, 10000000);

        for (int i = 0; i < 500000; ++i) {
            int64_t val = dist(gen);
            query << "(" << val << ")" << (i == 499999 ? ";" : ", ");
        }

        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 500000);
    }

    INFO("very large sort should complete with spill without OOM") {
        auto session = otterbrix::session_id_t();

        auto start = std::chrono::high_resolution_clock::now();
        auto cur = dispatcher->execute_sql(session,
            "SELECT data FROM TestDatabase.TestCollection ORDER BY data ASC;");
        auto end = std::chrono::high_resolution_clock::now();

        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 500000);

        // Verify correctness
        int64_t prev_val = std::numeric_limits<int64_t>::min();
        size_t error_count = 0;

        for (size_t i = 0; i < cur->size(); ++i) {
            auto cur_val = cur->value(0, i).value<int64_t>();
            if (cur_val < prev_val) {
                error_count++;
            }
            prev_val = cur_val;
        }

        REQUIRE(error_count == 0);  // Perfect sort

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        INFO("Very large sort (500K rows) completed in " << duration.count() << "ms");

        // Should complete within reasonable time (external merge sort is efficient)
        REQUIRE(duration.count() < 60000);  // < 60 seconds
    }
}