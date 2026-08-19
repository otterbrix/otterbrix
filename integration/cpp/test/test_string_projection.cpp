#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/row_group.hpp>
#include <chrono>
#include <string>

// Two defects on one site: the gather-by-row-id path behind storage_fetch.
//
// table_storage_adapter_t::fetch built its column list as 0..column_count-1 — every column of the
// table, whatever the statement actually names — and storage_fetch carried no projection at all,
// unlike its sibling storage_fetch_next_batch. An index scan therefore materialized every text
// column of every row it matched, then threw the strings away.
//
// The second defect is what made the first one dangerous. The chunk that call fills is returned to
// the caller and moved across a mailbox, while the pins taken to fill it die with the local
// column_fetch_state when the call returns. Without result_outlives_pins the string leg writes views
// BORROWED from those blocks, so the caller reads bytes the pool is free to evict — or, now that the
// pool can spill, to write to the scratch file and reload at a different address.
//
// Hidden ([.]). Run them with [strproj].

namespace {
    constexpr int kRows = 50000;

    void fill_tagged(otterbrix::wrapper_dispatcher_t* d) {
        constexpr int kBatch = 1000;
        for (int base = 0; base < kRows; base += kBatch) {
            std::string sql = "INSERT INTO p.t (id, tag, note, v) VALUES ";
            for (int i = 0; i < kBatch; ++i) {
                const int n = base + i;
                if (i != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(n) + ", 'tag_value_" + std::to_string(n) + "', 'note_" +
                       std::to_string(n) + "', " + std::to_string(n) + ")";
            }
            sql += ";";
            auto session = otterbrix::session_id_t();
            REQUIRE(d->execute_sql(session, sql)->is_success());
        }
    }
} // namespace

#define STRPROJ_ENV(dirname)                                                                         \
    auto config = test_create_config("/tmp/otterbrix/integration/test_string_projection/" dirname);   \
    test_clear_directory(config);                                                                     \
    config.disk.on = true;                                                                            \
    config.wal.on = false;                                                                            \
    config.log.level = log_t::level::off;                                                             \
    test_spaces space(config);                                                                        \
    auto* d = space.dispatcher();                                                                     \
    auto exec = [&](const std::string& sql) {                                                         \
        auto session = otterbrix::session_id_t();                                                     \
        return d->execute_sql(session, sql);                                                          \
    };                                                                                                \
    REQUIRE(exec("CREATE DATABASE p;")->is_success());                                                \
    REQUIRE(exec("CREATE TABLE p.t (id bigint, tag text, note text, v bigint);")->is_success());      \
    fill_tagged(d);                                                                                   \
    REQUIRE(exec("CREATE INDEX t_id_idx ON p.t (id);")->is_success())

TEST_CASE("integration::cpp::test_string_projection::gather_never_borrows_beyond_its_pins",
          "[.][strproj]") {
    STRPROJ_ENV("borrow");

    components::table::reset_gathered_borrowed_strings();
    {
        auto cur = exec("SELECT SUM(v) FROM p.t WHERE id > 45000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() > 0);
    }
    const auto escaping = components::table::escaping_borrowed_cells();
    const auto gathered = components::table::gather_rows_fetched();

    // Positive control: with no rows through the gather, a zero above would mean the path never ran.
    INFO("rows fetched by row-id: " << gathered);
    REQUIRE(gathered > 0);

    INFO("string cells handed back as views into blocks this path stopped pinning: " << escaping);
    CHECK(escaping == 0);
}

TEST_CASE("integration::cpp::test_string_projection::unprojected_columns_are_not_fetched",
          "[.][strproj]") {
    STRPROJ_ENV("projection");

    // Control first: a statement that DOES name a text column must materialize strings, so a low
    // count below cannot be mistaken for a counter that is simply not wired up.
    components::table::reset_gathered_borrowed_strings();
    {
        auto cur = exec("SELECT tag FROM p.t WHERE id > 49900;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() > 0);
    }
    const auto projected = components::table::string_materializations();
    INFO("control: a statement projecting tag materialized " << projected << " strings");
    REQUIRE(projected > 0);

    // The measurement: this statement names neither text column, so it should copy no strings at
    // all. The bound is expressed against the rows the gather touched, so the test states the shape
    // of the defect — cost per gathered row — rather than a number that drifts with the fixture.
    components::table::reset_gathered_borrowed_strings();
    {
        auto cur = exec("SELECT SUM(v) FROM p.t WHERE id > 45000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() > 0);
    }
    const auto wasted = components::table::string_materializations();
    const auto gathered = components::table::gather_rows_fetched();

    uint64_t elapsed_us = 0;
    {
        const auto t0 = std::chrono::steady_clock::now();
        for (int rep = 0; rep < 5; ++rep) {
            auto cur = exec("SELECT SUM(v) FROM p.t WHERE id > 45000;");
            REQUIRE(cur->is_success());
        }
        elapsed_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0)
                .count()) / 5;
    }
    WARN("indexed selective query: " << elapsed_us << " us per run");

    REQUIRE(gathered > 0);
    WARN("SELECT SUM(v) materialized " << wasted << " strings from two columns it never names, over "
                                       << gathered << " gathered rows");
    CHECK(wasted < gathered / 8);
}
