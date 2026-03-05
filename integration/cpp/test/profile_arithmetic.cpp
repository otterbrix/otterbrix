#include "test_config.hpp"

#include <chrono>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>
#include <iostream>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using namespace components;

int main() {
    auto config = configuration::config::create_config("/tmp/profile_arithmetic");
    std::filesystem::remove_all(config.main_path);
    std::filesystem::create_directories(config.main_path);
    config.disk.on = false;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    // Setup
    {
        auto s = otterbrix::session_id_t();
        dispatcher->create_database(s, database_name);
    }
    {
        auto s = otterbrix::session_id_t();
        dispatcher->create_collection(s, database_name, collection_name, types);
    }

    // Insert 1000 rows
    constexpr int kRows = 1000;
    {
        auto chunk = gen_data_chunk(kRows, dispatcher->resource());
        auto ins = logical_plan::make_node_insert(
            dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(s, ins);
        if (!cur->is_success()) {
            std::cerr << "Insert failed\n";
            return 1;
        }
        std::cerr << "Inserted " << cur->size() << " rows\n";
    }

    // Arithmetic + GROUP BY queries to profile
    const char* queries[] = {
        "SELECT count, count + 10 AS plus FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, count - 5 AS minus FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, count * 3 AS times FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, count / 2 AS div FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, count % 7 AS modulo FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, count * 2 + 10 AS expr FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, (count + 5) * (count - 1) AS expr FROM TestDatabase.TestCollection ORDER BY count ASC;",
        "SELECT count, count * 0.15 AS tax, count - count * 0.15 AS net FROM TestDatabase.TestCollection ORDER BY count ASC;",
        // GROUP BY queries — 2 groups (count_bool = true/false)
        "SELECT count_bool, SUM(count) AS total FROM TestDatabase.TestCollection GROUP BY count_bool;",
        "SELECT count_bool, AVG(count_double) AS avg_d FROM TestDatabase.TestCollection GROUP BY count_bool;",
        "SELECT count_bool, COUNT(*) AS cnt FROM TestDatabase.TestCollection GROUP BY count_bool;",
        "SELECT count_bool, SUM(count) AS total, AVG(count_double) AS avg_d FROM TestDatabase.TestCollection GROUP BY count_bool;",
        // GROUP BY queries — 1000 groups (count is unique → worst case for linear scan)
        "SELECT count, COUNT(*) AS cnt FROM TestDatabase.TestCollection GROUP BY count;",
        "SELECT count, SUM(count_double) AS sd FROM TestDatabase.TestCollection GROUP BY count;",
        // GROUP BY queries — string keys, 1000 groups
        "SELECT count_str, SUM(count) AS total FROM TestDatabase.TestCollection GROUP BY count_str;",
        "SELECT count_str, COUNT(*) AS cnt FROM TestDatabase.TestCollection GROUP BY count_str;",
    };
    constexpr int kQueries = sizeof(queries) / sizeof(queries[0]);
    constexpr int kIterations = 1000;

    std::cerr << "Running " << kQueries << " queries x " << kIterations << " iterations...\n";

    auto start = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < kIterations; iter++) {
        for (int q = 0; q < kQueries; q++) {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, queries[q]);
            if (!cur->is_success()) {
                std::cerr << "Query failed: " << queries[q] << "\n";
                return 1;
            }
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cerr << "Done: " << kIterations * kQueries << " queries in " << ms << " ms\n";
    std::cerr << "Avg: " << double(ms) / (kIterations * kQueries) << " ms/query\n";

    return 0;
}
