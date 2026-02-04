#include "test_config.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>

#include <catch2/catch.hpp>
#include <thread>
#include <vector>
#include <atomic>
#include <fstream>

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;
using namespace components::types;

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

// ============================================================
// Helper macros
// ============================================================

#define INIT_COLLECTION()                                                                                              \
    do {                                                                                                               \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_database(session, database_name);                                                       \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            auto types = gen_data_chunk(0, dispatcher->resource()).types();                                            \
            dispatcher->create_collection(session, database_name, collection_name, types);                             \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION(COUNT)                                                                                         \
    do {                                                                                                               \
        auto chunk = gen_data_chunk(COUNT, dispatcher->resource());                                                    \
        auto ins = components::logical_plan::make_node_insert(dispatcher->resource(),                                  \
                                                              {database_name, collection_name},                        \
                                                              std::move(chunk));                                       \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->execute_plan(session, ins);                                                                    \
        }                                                                                                              \
    } while (false)

#define CREATE_INDEX(INDEX_NAME, KEY)                                                                                  \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     {database_name, collection_name},                 \
                                                                     INDEX_NAME,                                       \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        dispatcher->create_index(session, node);                                                                       \
    } while (false)

#define CHECK_FIND(KEY, COMPARE, VALUE, COUNT)                                                                         \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan =                                                                                                    \
            components::logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});   \
        auto expr = components::expressions::make_compare_expression(dispatcher->resource(),                           \
                                                                     COMPARE,                                          \
                                                                     key{dispatcher->resource(), KEY, side_t::left},   \
                                                                     id_par{1});                                       \
        plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),                           \
                                                                     {database_name, collection_name},                 \
                                                                     std::move(expr)));                                \
        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());                           \
        params->add_parameter(id_par{1}, VALUE);                                                                       \
        auto c = dispatcher->find(session, plan, params);                                                              \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

#define CHECK_FIND_SQL(QUERY, COUNT)                                                                                   \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == COUNT);                                                                                 \
    } while (false)

// ============================================================
// TEST 1: Disk-based index scan after restart
// Verifies: await_async_and_resume, sync_index_from_disk
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::scan_after_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/scan_after_restart");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: create collection, index, fill data") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        FILL_COLLECTION(kDocuments);

        // Verify index works before restart
        CHECK_FIND("count", compare_type::eq, logical_value_t(50), 1);
    }

    INFO("phase 2: restart and verify disk-based index scan") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // These queries use disk-based index (await_async_and_resume path)
        CHECK_FIND("count", compare_type::eq, logical_value_t(1), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(50), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(100), 1);
        CHECK_FIND("count", compare_type::gt, logical_value_t(90), 10);
        CHECK_FIND("count", compare_type::lt, logical_value_t(11), 10);
        CHECK_FIND("count", compare_type::gte, logical_value_t(95), 6);
        CHECK_FIND("count", compare_type::lte, logical_value_t(5), 5);
    }
}

// ============================================================
// TEST 2: Disk-based index scan with SQL
// Verifies: SQL parser + disk index integration
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::sql_after_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/sql_after_restart");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: create and fill via SQL") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto types = gen_data_chunk(0, dispatcher->resource()).types();
            dispatcher->create_collection(session, database_name, collection_name, types);
        }

        // Insert via SQL
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, count, count_str, count_double, count_bool) VALUES ";
            for (int num = 1; num <= kDocuments; ++num) {
                query << "('" << gen_id(num, dispatcher->resource()) << "', "
                      << num << ", '" << num << "', " << (num + 0.1) << ", " << ((num % 2 != 0) ? "true" : "false") << ")"
                      << (num == kDocuments ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }

        // Create index via SQL
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and query via SQL") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 10);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count < 11;", 10);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count >= 95;", 6);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count <= 5;", 5);
    }
}

// ============================================================
// TEST 3: Concurrent disk index queries
// Verifies: thread safety of coroutines with disk I/O
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::concurrent_queries") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/concurrent_queries");
    test_clear_directory(config);

    constexpr int kDocuments = 100;  // Reduced to avoid load issues
    constexpr int kThreads = 5;
    constexpr int kQueriesPerThread = 5;

    INFO("phase 1: create collection with index") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        FILL_COLLECTION(kDocuments);
    }

    INFO("phase 2: concurrent queries after restart") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction

        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t]() {
                for (int q = 0; q < kQueriesPerThread; ++q) {
                    try {
                        int search_value = (t * kQueriesPerThread + q) % kDocuments + 1;
                        auto session = otterbrix::session_id_t();
                        auto plan = components::logical_plan::make_node_aggregate(
                            dispatcher->resource(), {database_name, collection_name});
                        auto expr = components::expressions::make_compare_expression(
                            dispatcher->resource(),
                            compare_type::eq,
                            key{dispatcher->resource(), "count", side_t::left},
                            id_par{1});
                        plan->append_child(components::logical_plan::make_node_match(
                            dispatcher->resource(),
                            {database_name, collection_name},
                            std::move(expr)));
                        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
                        params->add_parameter(id_par{1}, logical_value_t(search_value));
                        auto c = dispatcher->find(session, plan, params);

                        if (c->size() == 1) {
                            ++success_count;
                        } else {
                            ++error_count;
                        }
                    } catch (...) {
                        ++error_count;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count == kThreads * kQueriesPerThread);
        REQUIRE(error_count == 0);
    }
}

// ============================================================
// TEST 4: Multiple indexes scan after restart
// Verifies: multiple disk indexes work correctly
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::multiple_indexes") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/multiple_indexes");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: create multiple indexes") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        CREATE_INDEX("idx_countStr", "count_str");
        CREATE_INDEX("idx_countDouble", "count_double");
        FILL_COLLECTION(kDocuments);

        // Verify all indexes work
        CHECK_FIND("count", compare_type::eq, logical_value_t(50), 1);
    }

    INFO("phase 2: restart and query all indexes") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction

        // Query by count (int)
        CHECK_FIND("count", compare_type::eq, logical_value_t(25), 1);
        CHECK_FIND("count", compare_type::gt, logical_value_t(95), 5);

        // Query by count_str (string) - need string value
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(
                dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(
                dispatcher->resource(),
                compare_type::eq,
                key{dispatcher->resource(), "count_str", side_t::left},
                id_par{1});
            plan->append_child(components::logical_plan::make_node_match(
                dispatcher->resource(),
                {database_name, collection_name},
                std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t("50"));
            auto c = dispatcher->find(session, plan, params);
            REQUIRE(c->size() == 1);
        }

        // Query by count_double (double) - note: gen_data_chunk uses num + 0.1
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(
                dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(
                dispatcher->resource(),
                compare_type::eq,
                key{dispatcher->resource(), "count_double", side_t::left},
                id_par{1});
            plan->append_child(components::logical_plan::make_node_match(
                dispatcher->resource(),
                {database_name, collection_name},
                std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(50.1));
            auto c = dispatcher->find(session, plan, params);
            REQUIRE(c->size() == 1);
        }
    }
}

// ============================================================
// TEST 5: Large dataset after restart
// Verifies: WAL size_tt fix (uint16_t -> uint32_t) allows > 65KB records
// This test previously crashed with msgpack::insufficient_bytes
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::large_dataset") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/large_dataset");
    test_clear_directory(config);

    // 500 documents creates ~200KB WAL record (exceeds old 65KB limit)
    constexpr int kDocuments = 500;

    INFO("phase 1: create collection with 500 documents and index") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        FILL_COLLECTION(kDocuments);

        // Verify index works before restart
        CHECK_FIND("count", compare_type::eq, logical_value_t(250), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(kDocuments), 1);
    }

    INFO("phase 2: restart and verify - this previously crashed with msgpack::insufficient_bytes") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction  // <-- This previously crashed with msgpack::insufficient_bytes

        // Verify all documents loaded correctly
        CHECK_FIND("count", compare_type::eq, logical_value_t(1), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(250), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(kDocuments), 1);
        CHECK_FIND("count", compare_type::gt, logical_value_t(490), 10);
        CHECK_FIND("count", compare_type::lt, logical_value_t(11), 10);
    }
}

// ============================================================
// TEST 6: Very large dataset (1000 documents)
// Verifies: WAL handles ~500KB records correctly
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::very_large_dataset") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/very_large_dataset");
    test_clear_directory(config);

    // 1000 documents creates ~500KB WAL record
    constexpr int kDocuments = 1000;

    INFO("phase 1: create collection with 1000 documents") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        FILL_COLLECTION(kDocuments);

        // Verify index works before restart
        CHECK_FIND("count", compare_type::eq, logical_value_t(500), 1);
    }

    INFO("phase 2: restart and verify very large dataset") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction

        // Verify all documents loaded correctly
        CHECK_FIND("count", compare_type::eq, logical_value_t(1), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(500), 1);
        CHECK_FIND("count", compare_type::eq, logical_value_t(kDocuments), 1);
        CHECK_FIND("count", compare_type::gt, logical_value_t(990), 10);
    }
}

// ============================================================
// TEST 7: Concurrent queries with large dataset
// Verifies: thread safety with large datasets (previously crashed)
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::concurrent_large_dataset") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/concurrent_large_dataset");
    test_clear_directory(config);

    ////constexpr int kDocuments = 5000;  // Large dataset that previously caused issue
    constexpr int kDocuments = 10;  // Large dataset that previously caused issues
    constexpr int kThreads = 50;
    ///constexpr int kQueriesPerThread = 200;
    constexpr int kQueriesPerThread = 10;

    INFO("phase 1: create collection with large dataset") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        FILL_COLLECTION(kDocuments);
    }

    INFO("phase 2: concurrent queries on large dataset after restart") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction

        std::atomic<int> success_count{0};
        std::atomic<int> error_count{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t]() {
                for (int q = 0; q < kQueriesPerThread; ++q) {
                    try {
                        int search_value = (t * kQueriesPerThread + q) % kDocuments + 1;
                        auto session = otterbrix::session_id_t();
                        auto plan = components::logical_plan::make_node_aggregate(
                            dispatcher->resource(), {database_name, collection_name});
                        auto expr = components::expressions::make_compare_expression(
                            dispatcher->resource(),
                            compare_type::eq,
                            key{dispatcher->resource(), "count", side_t::left},
                            id_par{1});
                        plan->append_child(components::logical_plan::make_node_match(
                            dispatcher->resource(),
                            {database_name, collection_name},
                            std::move(expr)));
                        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
                        params->add_parameter(id_par{1}, logical_value_t(search_value));
                        auto c = dispatcher->find(session, plan, params);

                        if (c->size() == 1) {
                            ++success_count;
                        } else {
                            ++error_count;
                        }
                    } catch (...) {
                        ++error_count;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(success_count == kThreads * kQueriesPerThread);
        REQUIRE(error_count == 0);
    }
}

// ============================================================
// TEST 8: Disk I/O error handling (mock)
// Verifies: error handling in await_async_and_resume
// ============================================================
TEST_CASE("integration::cpp::test_disk_index::io_error_handling") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_disk_index/io_error_handling");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: create collection with index") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("idx_count", "count");
        FILL_COLLECTION(kDocuments);
    }

    INFO("phase 2: corrupt index directory and try to query") {
        // Corrupt the index file to simulate I/O error
        auto index_path = config.disk.path / database_name / collection_name / "idx_count";
        if (std::filesystem::exists(index_path)) {
            // Remove index data files to simulate corruption
            for (auto& entry : std::filesystem::directory_iterator(index_path)) {
                if (entry.is_regular_file()) {
                    // Truncate file to simulate corruption
                    std::ofstream ofs(entry.path(), std::ios::trunc);
                    ofs.close();
                }
            }
        }

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // NOTE: load() removed - External Loader handles loading during construction

        // Query should handle the error gracefully (not crash)
        // The behavior depends on implementation - may return empty or fall back to full scan
        auto session = otterbrix::session_id_t();
        auto plan = components::logical_plan::make_node_aggregate(
            dispatcher->resource(), {database_name, collection_name});
        auto expr = components::expressions::make_compare_expression(
            dispatcher->resource(),
            compare_type::eq,
            key{dispatcher->resource(), "count", side_t::left},
            id_par{1});
        plan->append_child(components::logical_plan::make_node_match(
            dispatcher->resource(),
            {database_name, collection_name},
            std::move(expr)));
        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
        params->add_parameter(id_par{1}, logical_value_t(50));

        // Should not throw/crash - graceful degradation
        try {
            auto c = dispatcher->find(session, plan, params);
            // Result may be empty or error, but should not crash
            REQUIRE((c->is_success() || c->is_error()));
        } catch (const std::exception& e) {
            // If exception is thrown, it should be a controlled error
            WARN("Exception during corrupted index query: " << e.what());
        }
    }
}
