#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <core/b_plus_tree/segment_tree.hpp>
#include <services/index/manager_index.hpp>
#include <string>
#include <thread>

// One changed row must not rewrite the whole index: force_flush() used to fsync every leaf on
// every INSERT/UPDATE/DELETE, costing a one-row DELETE 1.6s on a million-row indexed table vs
// 21ms on 10k rows. This test pins the flush to the leaves that actually changed.
//
// Hidden by default ([.], builds a large index) — run with [indexflush].

namespace {
    void fill(otterbrix::wrapper_dispatcher_t* d, const std::string& table, int rows) {
        constexpr int kBatch = 1000;
        for (int base = 0; base < rows; base += kBatch) {
            std::string sql = "INSERT INTO f." + table + " (id, v) VALUES ";
            for (int i = 0; i < kBatch; ++i) {
                if (i != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(base + i) + ", 1)";
            }
            sql += ";";
            auto session = otterbrix::session_id_t();
            REQUIRE(d->execute_sql(session, sql)->is_success());
        }
    }

    // A committed DELETE queues its erase until the snapshot floor reaches the commit id
    // (manager_index.hpp: deferred_deletes_), so the leaf flush lands in the horizon sweep
    // instead of the statement; index_deferred_deletes() == 0 marks that sweep done. Spun on
    // yield(), not slept: the elapsed time below is measured INSIDE this wait, and a
    // millisecond of sleep would swamp the reported microseconds.
    void await_deferred_index_deletes() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (services::index::index_deferred_deletes() != 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        REQUIRE(services::index::index_deferred_deletes() == 0);
    }
} // namespace

TEST_CASE("integration::cpp::test_index_flush_scope::one_row_does_not_rewrite_every_leaf", "[.][indexflush]") {
    auto config = test_create_config(integration_fixture_path("test_index_flush/scope"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    constexpr int kRows = 100000;

    REQUIRE(exec("CREATE DATABASE f;")->is_success());
    REQUIRE(exec("CREATE TABLE f.t (id bigint, v bigint);")->is_success());
    fill(d, "t", kRows);
    REQUIRE(exec("CREATE INDEX t_id ON f.t (id);")->is_success());

    // Warm up: the first statement after CREATE INDEX may still be settling the tree.
    REQUIRE(exec("DELETE FROM f.t WHERE id = 10;")->is_success());
    await_deferred_index_deletes();

    core::b_plus_tree::reset_leaf_flushes();
    const auto start = std::chrono::steady_clock::now();
    REQUIRE(exec("DELETE FROM f.t WHERE id = 20;")->is_success());
    await_deferred_index_deletes();
    const auto elapsed_us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count();
    const auto flushes = core::b_plus_tree::leaf_flushes();
    const auto wasted = core::b_plus_tree::leaf_flushes_without_changes();
    INFO("one-row DELETE took " << elapsed_us << " us");

    INFO("ONE-row DELETE on a " << kRows << "-row indexed table: " << flushes << " leaf flushes, of which " << wasted
                                << " wrote no block at all");

    // Positive control first: a counter reading zero proves nothing about the code, it usually
    // proves the instrument is not wired to the path.
    REQUIRE(flushes > 0);

    // A one-row DELETE removes one index entry, which lives in exactly one leaf. Allow a small
    // constant for the leaf itself plus any structural neighbour a rebalance could touch.
    CHECK(flushes <= 4);

    // `wasted` is REPORTED, not asserted: a header-only flush (e.g. removing a block's last
    // item) is legitimate, and asserting it away would invite skipping it — silent data loss.
    INFO("leaf flushes that wrote no block (reported, not a requirement): " << wasted);
}
