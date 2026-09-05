#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <string>

// DELETE reads whole rows it has no use for.
//
// A DELETE needs the row ids (which travel in chunk.row_ids, not in a data column),
// plus the columns something downstream actually consumes: the key columns of every
// index on the table (the index mirror removes the old entries by value), RETURNING
// columns, and the key columns an FK cascade probes the child table with. Every other
// column is read from storage, materialized into the scan batch and thrown away.
//
// Measured on a 100k-row table, deleting a quarter of it: 22 ms at 4 payload columns
// against 70 ms at 40 — the cost tracks table width, not the work.
//
// The dangerous failure mode when fixing this is NOT a wrong table: it is a table that
// stays correct while the INDEX diverges, because an index-key column got pruned and
// the mirror deleted nothing. The second test here is the guard for exactly that.

namespace {
    std::string wide_table_ddl(const std::string& name, int payload_columns) {
        std::string ddl = "CREATE TABLE " + name + " (id bigint, k bigint";
        for (int c = 0; c < payload_columns; ++c) {
            ddl += ", v" + std::to_string(c) + " bigint";
        }
        return ddl + ");";
    }

    std::string wide_insert(const std::string& name, int first, int count, int payload_columns) {
        std::string sql = "INSERT INTO " + name + " (id, k";
        for (int c = 0; c < payload_columns; ++c) {
            sql += ", v" + std::to_string(c);
        }
        sql += ") VALUES ";
        for (int i = 0; i < count; ++i) {
            const int v = first + i;
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(v) + ", " + std::to_string(v);
            for (int c = 0; c < payload_columns; ++c) {
                sql += ", " + std::to_string(v);
            }
            sql += ")";
        }
        return sql + ";";
    }
} // namespace

TEST_CASE("integration::cpp::test_delete_column_pruning::reads_only_needed_columns") {
    auto config = test_create_config(integration_fixture_path("test_delete_pruning/needed_columns"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    constexpr int kPayload = 40;
    constexpr int kRows = 3000;

    REQUIRE(exec("CREATE DATABASE d;")->is_success());
    REQUIRE(exec(wide_table_ddl("d.wide", kPayload))->is_success());
    for (int base = 0; base < kRows; base += 1000) {
        REQUIRE(exec(wide_insert("d.wide", base, 1000, kPayload))->is_success());
    }

    // No index, no RETURNING, no foreign key: nothing downstream reads a value, so a
    // pruned scan should materialize no data column at all.
    const auto columns_before = components::operators::delete_scanned_columns();
    REQUIRE(exec("DELETE FROM d.wide WHERE id < 2000;")->is_success());
    const auto columns_after = components::operators::delete_scanned_columns();

    const auto materialized = columns_after - columns_before;
    INFO("materialized columns: " << materialized);
    CHECK(materialized <= 8); // unpruned: 42 columns x 2 scan batches = 84

    {
        auto cur = exec("SELECT id FROM d.wide WHERE id = 2500;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1); // survivors intact
    }
    {
        auto cur = exec("SELECT id FROM d.wide WHERE id = 10;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0); // and the delete really happened
    }
}

TEST_CASE("integration::cpp::test_delete_column_pruning::index_stays_consistent_after_delete") {
    auto config = test_create_config(integration_fixture_path("test_delete_pruning/index_consistent"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    constexpr int kPayload = 8;
    constexpr int kRows = 2000;

    REQUIRE(exec("CREATE DATABASE d;")->is_success());
    REQUIRE(exec(wide_table_ddl("d.indexed", kPayload))->is_success());
    REQUIRE(exec("CREATE INDEX indexed_k_idx ON d.indexed (k);")->is_success());
    for (int base = 0; base < kRows; base += 1000) {
        REQUIRE(exec(wide_insert("d.indexed", base, 1000, kPayload))->is_success());
    }

    REQUIRE(exec("DELETE FROM d.indexed WHERE id < 500;")->is_success());

    // Read back through the INDEX (equality on the indexed column picks an index scan),
    // not through a table scan: a pruned index-key column would leave the mirror unable
    // to remove the old entry, and only this shape would notice.
    INFO("a deleted key must not be reachable through the index");
    {
        auto cur = exec("SELECT id FROM d.indexed WHERE k = 100;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0);
    }
    INFO("a surviving key must still be reachable through the index");
    {
        auto cur = exec("SELECT id FROM d.indexed WHERE k = 1500;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }
    INFO("and a key deleted at the boundary is gone too");
    {
        auto cur = exec("SELECT id FROM d.indexed WHERE k = 499;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0);
    }
}
