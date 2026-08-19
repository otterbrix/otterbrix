#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <core/b_plus_tree/segment_tree.hpp>
#include <string>

// One changed row must not rewrite the whole index.
//
// A disk B+tree index keeps one file per leaf. btree_index_disk_t::force_flush() — which every
// INSERT/UPDATE/DELETE reaches through index_agent_disk_t::insert_many / remove_many — calls
// btree_t::flush(), and that walks EVERY leaf calling segment_tree_t::flush() on each. Inside,
// only the per-block writes are guarded by a `modified` flag: the header write, the truncate and
// the fsync are unconditional. So a statement touching one row pays one fsync per leaf of the
// entire index.
//
// This is what makes a one-row DELETE cost 1.6 s on a million-row indexed table while the same
// DELETE on a 10k-row table costs 21 ms — a profile taken over a sustained delete loop put
// essentially the whole index agent thread in remove_many -> force_flush -> btree_t::flush.
// b_plus_tree.hpp:200 carries the author's own note: "TODO: flush and load only modified leaves".
//
// The counter reports both totals: how many leaves were flushed, and how many of those flushes
// wrote no block at all. The second number is pure waste and is what the fix has to remove.
//
// Hidden by default ([.]) because it builds an index large enough to span many leaves.
// Run it with [indexflush].

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
} // namespace

TEST_CASE("integration::cpp::test_index_flush_scope::one_row_does_not_rewrite_every_leaf", "[.][indexflush]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_flush/scope");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
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

    core::b_plus_tree::reset_leaf_flushes();
    const auto start = std::chrono::steady_clock::now();
    REQUIRE(exec("DELETE FROM f.t WHERE id = 20;")->is_success());
    const auto elapsed_us =
        std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count();
    const auto flushes = core::b_plus_tree::leaf_flushes();
    const auto wasted = core::b_plus_tree::leaf_flushes_without_changes();
    INFO("one-row DELETE took " << elapsed_us << " us");

    INFO("ONE-row DELETE on a " << kRows << "-row indexed table: " << flushes << " leaf flushes, of which "
                                << wasted << " wrote no block at all");

    // Positive control first: a counter reading zero proves nothing about the code, it usually
    // proves the instrument is not wired to the path.
    REQUIRE(flushes > 0);

    // A one-row DELETE removes one index entry, which lives in exactly one leaf. Allow a small
    // constant for the leaf itself plus any structural neighbour a rebalance could touch.
    CHECK(flushes <= 4);

    // `wasted` is REPORTED, not asserted. A leaf can legitimately need a flush without any block
    // changing — removing the last item from a block rewrites the header, the metadata array and
    // the file length while leaving no modified block behind. Turning "wrote no block" into a
    // requirement would pressure the next reader into skipping those header-only flushes, which is
    // precisely the silent-data-loss bug this change has to avoid.
    INFO("leaf flushes that wrote no block (reported, not a requirement): " << wasted);
}
