#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/row_version_manager.hpp>
#include <string>
#include <vector>

// Does the per-commit cleanup cost grow as a table piles up tombstones?
//
// The S3 plan item assumed commit walks the whole table. Measured on a nearly clean table that is
// false: committing a one-row DELETE visits 1024 version slots — one vector — whatever the table
// size. But that measurement only covers the COMMIT walk.
//
// The cleanup side is a different walk. agent_disk_t::maybe_cleanup_inner asks
// collection_t::committed_row_count() on every commit to decide whether to compact, and that
// reaches chunk_vector_info::committed_deleted_count, which re-scans all slots of EVERY vector
// still carrying a committed tombstone. On a fresh table there are almost none. UPDATE here is
// tombstone+append, so a table under a long UPDATE workload accumulates them — and then every
// later commit, however small, would pay for all of them.
//
// So this measures the one thing the earlier probe did not: reset the cleanup counter, run ONE
// trivial committing statement, and read how many slots that commit had to walk — after 1 pass,
// after 5, after 10. If the number climbs with the number of accumulated tombstones, S3 is alive
// and this is its RED test. If it stays flat, S3 is dead and the plan can drop it.
//
// Hidden by default ([.]): it does repeated 200k-row update passes. Run it with [s3cleanup].

namespace {
    constexpr int kRows = 200000;
    constexpr int kBatch = 1000;
    constexpr int kUpdatedPerPass = 50000;

    void fill(otterbrix::wrapper_dispatcher_t* d, const std::string& table, int rows) {
        for (int base = 0; base < rows; base += kBatch) {
            std::string sql = "INSERT INTO tomb." + table + " (id, v) VALUES ";
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

TEST_CASE("integration::cpp::test_s3_cleanup_scaling::cleanup_cost_vs_accumulated_tombstones",
          "[.][s3cleanup]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/cleanup");
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

    REQUIRE(exec("CREATE DATABASE tomb;")->is_success());
    REQUIRE(exec("CREATE TABLE tomb.t (id bigint, v bigint);")->is_success());
    fill(d, "t", kRows);

    // The probe statement must be a DELETE, not an INSERT. The cleanup fan-out is gated on the
    // commit carrying base deletes (operator_commit_transaction.cpp: "an append-only commit ...
    // produces ZERO dead rows ... the entire fan-out is provably a no-op worth skipping"), so an
    // append-only probe measures nothing and reads zero for the wrong reason.
    //
    // It deletes one row from the UNTOUCHED tail of the table, so the probe never overlaps the
    // rows the update passes tombstone — what it pays for is other statements' leftovers.
    int probe_id = kRows - 1;
    auto cleanup_slots_for_one_commit = [&]() {
        components::table::reset_cleanup_slots_visited();
        components::table::reset_version_slots_visited();
        REQUIRE(exec("DELETE FROM tomb.t WHERE id = " + std::to_string(probe_id--) + ";")->is_success());
        return components::table::cleanup_slots_visited();
    };

    const auto before_any_update = cleanup_slots_for_one_commit();
    INFO("cleanup slots for one commit, before any update pass: " << before_any_update);

    std::vector<uint64_t> series;
    for (int pass = 1; pass <= 10; ++pass) {
        // UPDATE is tombstone+append: this leaves kUpdatedPerPass committed tombstones behind.
        REQUIRE(exec("UPDATE tomb.t SET v = v + 1 WHERE id < " + std::to_string(kUpdatedPerPass) + ";")
                    ->is_success());
        series.push_back(cleanup_slots_for_one_commit());
    }

    INFO("cleanup slots walked by ONE trivial commit, after each update pass:");
    for (size_t i = 0; i < series.size(); ++i) {
        INFO("  pass " << (i + 1) << ": " << series[i]);
    }
    INFO("pass 1 = " << series.front() << ", pass 10 = " << series.back());

    // The acceptance claim S3 would need: a small commit must not pay for tombstones it did not
    // create. Flat (or bounded) means S3 is dead; growth means S3 is alive and this is its RED
    // test. The bound is stated against pass 1 so the test reports the SHAPE, not an absolute.
    CHECK(series.back() <= series.front() * 2);

    // Positive control. A counter that reads zero everywhere proves nothing about the code — it
    // usually means the instrument is not wired to the path, which is exactly how the first
    // version of this test failed (it probed with an INSERT, which the cleanup fan-out skips by
    // design). Deleting a quarter of the table must make the cleanup walk SOMETHING; if this
    // fails, every number above is meaningless and must not be reported.
    REQUIRE(exec("DELETE FROM tomb.t WHERE id >= 100000 AND id < 150000;")->is_success());
    components::table::reset_cleanup_slots_visited();
    REQUIRE(exec("DELETE FROM tomb.t WHERE id = 60000;")->is_success());
    const auto control = components::table::cleanup_slots_visited();
    INFO("positive control — cleanup slots after 50k rows were deleted: " << control);
    REQUIRE(control > 0);
}

// The case the contiguous test above cannot reach.
//
// Deleting a contiguous range takes whole vectors out at once, and a fully deleted vector is
// recorded as a chunk_constant_info whose committed_deleted_count is O(1) — which is why the
// contiguous probe reads a flat 1024 (just the one partially-hit boundary vector).
//
// A SCATTERED update leaves every vector partially tombstoned, so every vector keeps a
// chunk_vector_info with any_deleted set, and the cleanup re-walks all 1024 slots of each on every
// later commit. That is the shape S3 actually describes, and it is what an OLTP workload updating
// rows by primary key produces.
TEST_CASE("integration::cpp::test_s3_cleanup_scaling::scattered_tombstones_cost_per_commit",
          "[.][s3cleanup]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/cleanup_scattered");
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

    REQUIRE(exec("CREATE DATABASE tomb;")->is_success());
    REQUIRE(exec("CREATE TABLE tomb.t (id bigint, v bigint);")->is_success());
    fill(d, "t", kRows);

    int probe_id = kRows - 1;
    auto cleanup_slots_for_one_commit = [&]() {
        components::table::reset_cleanup_slots_visited();
        REQUIRE(exec("DELETE FROM tomb.t WHERE id = " + std::to_string(probe_id--) + ";")->is_success());
        return components::table::cleanup_slots_visited();
    };

    const auto clean = cleanup_slots_for_one_commit();
    INFO("scattered: cleanup slots before any update pass: " << clean);

    std::vector<uint64_t> series;
    for (int pass = 1; pass <= 5; ++pass) {
        // Every 4th row: each 1024-row vector keeps ~256 live and ~256 tombstoned rows, so no
        // vector can collapse into the O(1) constant form.
        REQUIRE(exec("UPDATE tomb.t SET v = v + 1 WHERE id % 4 = 0;")->is_success());
        series.push_back(cleanup_slots_for_one_commit());
    }

    INFO("scattered: cleanup slots walked by ONE trivial commit, per pass:");
    for (size_t i = 0; i < series.size(); ++i) {
        INFO("  pass " << (i + 1) << ": " << series[i]);
    }
    INFO("scattered: pass 1 = " << series.front() << ", pass 5 = " << series.back());

    // Same instrument discipline as above: a flat zero would mean the probe missed the path.
    REQUIRE(series.back() > 0);

    // S3's claim, stated where it can actually be tested: one small commit must not pay for
    // tombstones other statements left behind.
    CHECK(series.back() <= 4 * components::vector::DEFAULT_VECTOR_CAPACITY);
}

// The last corner, and the one that matters most in practice: a table WITH an index.
//
// compact() shifts row positions and the index engines hold positional row refs, so
// operator_commit_transaction filters the compact set through manager_index_t::tables_without_indexes
// before sending maybe_cleanup_many. An indexed table is therefore dropped from safe_oids — which
// means either the cleanup never runs for it (cost zero) or, if it did, its tombstones could never
// be compacted away and the walk would grow without bound. Those are opposite outcomes and only a
// measurement separates them.
TEST_CASE("integration::cpp::test_s3_cleanup_scaling::indexed_table_cleanup_cost", "[.][s3cleanup]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/cleanup_indexed");
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

    constexpr int kIndexedRows = 100000;

    REQUIRE(exec("CREATE DATABASE tomb;")->is_success());
    REQUIRE(exec("CREATE TABLE tomb.t (id bigint, v bigint);")->is_success());
    fill(d, "t", kIndexedRows);
    REQUIRE(exec("CREATE INDEX t_id ON tomb.t (id);")->is_success());

    int probe_id = kIndexedRows - 1;
    auto cleanup_slots_for_one_commit = [&]() {
        components::table::reset_cleanup_slots_visited();
        REQUIRE(exec("DELETE FROM tomb.t WHERE id = " + std::to_string(probe_id--) + ";")->is_success());
        return components::table::cleanup_slots_visited();
    };

    std::vector<uint64_t> series;
    series.push_back(cleanup_slots_for_one_commit());
    for (int pass = 1; pass <= 3; ++pass) {
        REQUIRE(exec("UPDATE tomb.t SET v = v + 1 WHERE id % 4 = 0;")->is_success());
        series.push_back(cleanup_slots_for_one_commit());
    }

    INFO("indexed: cleanup slots walked by ONE trivial commit, clean then after each pass:");
    for (size_t i = 0; i < series.size(); ++i) {
        INFO("  step " << i << ": " << series[i]);
    }
    INFO("indexed: first = " << series.front() << ", last = " << series.back());

    CHECK(series.back() <= 4 * components::vector::DEFAULT_VECTOR_CAPACITY);
}
