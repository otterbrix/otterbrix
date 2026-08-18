#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/index/index_engine.hpp>
#include <string>

// Matching an index key to its column is a property of the CHUNK, not of the row.
//
// It used to be done per row, twice: once to check the key column is present and once to read
// the value — and each check walked the chunk's columns comparing a column alias against a
// freshly constructed std::string built from the key. Inserting N rows into a table with one
// index therefore inspected columns O(N) times and allocated a string for every comparison,
// to answer a question whose answer is identical for every row of the chunk.
//
// This is a counter test rather than a timing one: the defect is a growth rate, and the fix
// changes the growth rate. The bound is expressed against the ROW COUNT, so it fails if the
// work is per-row and passes only if it is per-chunk.
TEST_CASE("integration::cpp::test_index_key_binding::key_lookup_is_per_chunk_not_per_row") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_key_binding/per_chunk");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE b;")->is_success());
    REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint, v bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX t_k ON b.t (k);")->is_success());

    constexpr int kRows = 500;
    std::string sql = "INSERT INTO b.t (id, k, v) VALUES ";
    for (int i = 0; i < kRows; ++i) {
        if (i != 0) {
            sql += ", ";
        }
        sql += "(" + std::to_string(i) + ", " + std::to_string(i) + ", 1)";
    }
    sql += ";";

    components::index::reset_index_key_column_probes();
    REQUIRE(exec(sql)->is_success());
    const auto probes = components::index::index_key_column_probes();

    INFO("column inspections while indexing " << kRows << " rows: " << probes);
    // One chunk of 500 rows: a per-chunk resolution inspects a handful of columns, a per-row
    // one inspects hundreds. The bound sits far below the row count and far above the columns.
    CHECK(probes < static_cast<uint64_t>(kRows));

    // The index must still answer correctly — read back THROUGH it.
    auto cur = exec("SELECT id FROM b.t WHERE k = 321;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}
