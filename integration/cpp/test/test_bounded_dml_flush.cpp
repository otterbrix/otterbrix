// Bounded DML sinks flush incrementally once buffered_rows() >= dml_flush_row_threshold; the default (0) disables
// this, so no other test exercises the incremental path -- these do, via the DEV_MODE dml_flush_count() counter.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/collection/executor.hpp>
#include <sstream>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024) so the scan emits several batches, each tripping a flush at the threshold.
    constexpr unsigned kRowCount = 3000;

    // Small enough that any 1024-row batch trips the mid-pump gate at least once.
    constexpr uint64_t kFlushThreshold = 512;

    // Seeds kRowCount rows via one multi-row VALUES insert: id=i, grp=i%8, val=i*2.
    void seed_source(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& fq_table) {
        auto cur = seed_rows(dispatcher, fq_table, "id, grp, val", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % 8) << ", " << (i * 2) << ")";
            return s.str();
        });
        INFO("seed error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
} // namespace

TEST_CASE("integration::cpp::bounded_dml_flush::insert_select_mid_flushes") {
    auto config = make_test_config(integration_fixture_path("test_bounded_dml_flush/insert_select"));
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.src (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.dst (id bigint, grp int, val bigint);")->is_success());
    seed_source(dispatcher, "FlushDb.src");

    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "INSERT INTO FlushDb.dst (id, grp, val) SELECT id, grp, val FROM FlushDb.src;");
        INFO("INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();

    // With threshold==0 this delta would be 0 (a single post-pump flush); >1 here proves the incremental path ran.
    REQUIRE(flushes_after - flushes_before > 1);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }
    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM FlushDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
}

TEST_CASE("integration::cpp::bounded_dml_flush::update_mid_flushes") {
    auto config = make_test_config(integration_fixture_path("test_bounded_dml_flush/update"));
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.t (id bigint, grp int, val bigint);")->is_success());
    seed_source(dispatcher, "FlushDb.t");

    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "UPDATE FlushDb.t SET val = val + 1;");
        INFO("UPDATE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();
    REQUIRE(flushes_after - flushes_before > 1);

    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2 + 1;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM FlushDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }

    const auto flushes_before2 = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "UPDATE FlushDb.t SET val = val + 10 RETURNING id;");
        INFO("UPDATE RETURNING error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        // One returned row per updated row, accumulated across every flush.
        REQUIRE(cur->size() == kRowCount);
    }
    const auto flushes_after2 = services::collection::executor::dml_flush_count();
    REQUIRE(flushes_after2 - flushes_before2 > 1);

    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2 + 11;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM FlushDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
}

// A UNIQUE violation on a row scanned AFTER an already-flushed batch must revert every mid-flushed append.
TEST_CASE("integration::cpp::bounded_dml_flush::error_after_mid_flush_reverts_all") {
    // disk ON: constraint enforcement + revert path exercised on disk.
    auto config = make_test_config(integration_fixture_path("test_bounded_dml_flush/atomicity"));
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.src (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.acc (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE FlushDb.acc ADD CONSTRAINT uq_acc_id UNIQUE (id);")->is_success());
    seed_source(dispatcher, "FlushDb.src");

    // The collision id sits mid-range so several batches flush cleanly before the scan reaches it and fails.
    const int64_t collide_id = static_cast<int64_t>(kRowCount / 2);
    {
        std::stringstream q;
        q << "INSERT INTO FlushDb.acc (id, grp, val) VALUES (" << collide_id << ", 0, 0);";
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.acc;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1u);
    }

    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "INSERT INTO FlushDb.acc (id, grp, val) SELECT id, grp, val FROM FlushDb.src;");
        REQUIRE(cur->is_error());
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();
    REQUIRE(flushes_after - flushes_before >= 1);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.acc;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1u);
    }
    {
        std::stringstream q;
        q << "SELECT val FROM FlushDb.acc WHERE id = " << collide_id << ";";
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 0);
    }
}

// A recursive-CTE fixpoint (a sourceless bottom) feeding INSERT must trip the same mid-flush gate as a scan source,
// or the sink buffers the whole produced set and the configured memory bound becomes a no-op.
TEST_CASE("integration::cpp::bounded_dml_flush::insert_from_recursive_cte_mid_flushes") {
    auto config = make_test_config(integration_fixture_path("test_bounded_dml_flush/recursive_cte_insert"));
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kCteRows = 1500; // > kFlushThreshold (512): the gate must trip mid-pump

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.dst (n bigint);")->is_success());

    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher,
                        "INSERT INTO FlushDb.dst (n) "
                        "WITH RECURSIVE seq AS ("
                        "  SELECT 1 AS n "
                        "  UNION ALL "
                        "  SELECT n + 1 FROM seq WHERE n < 1500"
                        ") "
                        "SELECT n FROM seq;");
        INFO("INSERT from recursive CTE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kCteRows);
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();

    REQUIRE(flushes_after > flushes_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(n) AS c FROM FlushDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == kCteRows);
    }
}

// DELETE...USING...LIMIT n must stop at exactly n across mid-pump flushes: the bound is the persistent matched_total_
// counter, not the per-flush-cleared modified_ buffer (which would under-count and over-delete).
TEST_CASE("integration::cpp::bounded_dml_flush::delete_using_limit_spans_flushes") {
    auto config = make_test_config(integration_fixture_path("test_bounded_dml_flush/delete_using_limit"));
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.tgt (id bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.src (k bigint);")->is_success());
    // Every target row joins a src row (k in 0..9), so all rows are eligible; only the LIMIT stops the delete.
    {
        auto cur = seed_rows(dispatcher, "FlushDb.tgt", "id, k", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % 10) << ")";
            return s.str();
        });
        INFO("seed tgt error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
    REQUIRE(
        exec(dispatcher, "INSERT INTO FlushDb.src (k) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);")->is_success());

    // kBound sits between the flush threshold and the total, so at least one mid-flush must survive before it lands.
    constexpr uint64_t kBound = 1500;
    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur =
            exec(dispatcher,
                 "DELETE FROM FlushDb.tgt USING FlushDb.src WHERE tgt.k = src.k LIMIT " + std::to_string(kBound) + ";");
        INFO("bounded USING delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.tgt;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount) - kBound);
    }
    // USING/join runs the non-streaming path (a single flush expected), unlike the streaming sinks in the other tests.
    REQUIRE(flushes_after - flushes_before >= 1);
}
