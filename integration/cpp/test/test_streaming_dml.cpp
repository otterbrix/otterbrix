// ============================================================================
// Streaming DML — STEP 3b verification.
//
// STEP 3b makes DML participate in the push-based streaming executor so DML
// over a large scan runs BOUNDED: instead of materializing the WHOLE scan input
// up front, the DML sink folds one batch at a time via push() and drives its
// async WAL->storage->index commit once, after the pump, from the executor's
// coroutine (which owns the cross-actor await — lost-wakeup-safe). Atomicity is
// held by the MVCC TRANSACTION, not the operator, so streaming is correctness-
// safe.
//
// The streaming sink is a SECOND entry point sharing the SAME core as the legacy
// on_execute path (R6: single implementation, two entry points), so results are
// identical regardless of which path runs.
//
// WHAT THESE TESTS ASSERT:
//   (a) CORRECTNESS — every row that the materialize path produced still lands
//       (and RETURNING / index consistency for DELETE/UPDATE).
//   (b) PATH — the statement actually routed through the push-based streaming
//       pipeline (execute_pipeline), proven by streaming_pipeline_runs(): the
//       INSERT...SELECT / predicate DELETE / predicate UPDATE over a MULTI-BATCH
//       scan (>> DEFAULT_VECTOR_CAPACITY rows) bumps the counter, whereas the
//       legacy raw_data/VALUES path does not. (Stubbing role() back to none makes
//       the PATH assertion RED.)
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024): forces the scan source to emit MANY
    // batches so the DML sink must fold across batch boundaries — the bounded
    // streaming property under test.
    constexpr unsigned kRowCount = 5000;

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_dml::insert_select_streams_and_lands") {
    auto config = test_create_config("/tmp/test_streaming_dml_insert");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.dst (id bigint, grp int, val bigint);")->is_success());

    // Seed the source table with a multi-batch row set.
    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.src (id, grp, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << (i % 8) << ", " << (i * 2) << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // INSERT...SELECT over a multi-batch scan source: this is the streaming DML
    // shape. Record the streaming-run counter across just this statement.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.dst (id, grp, val) SELECT id, grp, val FROM StreamDb.src;");
        INFO("INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();

    // PATH: the INSERT...SELECT routed through the push-based streaming pipeline.
    // With role()==sink the whole insert->...->scan chain is a sourced streaming
    // pipeline; the counter bumps. Stub role() back to none and this is RED.
    REQUIRE(runs_after > runs_before);

    // CORRECTNESS: every source row landed in dst, with values intact.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }
    {
        // Analytic spot-check of the streamed payload: SUM(val) == 2 * sum(0..N-1).
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM StreamDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == expected_sum);
    }
}

TEST_CASE("integration::cpp::streaming_dml::insert_values_streams") {
    // operator_raw_data_t is now a streaming SOURCE (role()==source), so the VALUES
    // form is a sourced INSERT...VALUES pipeline: is_streaming_pipeline routes it
    // through execute_pipeline, where the INSERT sink folds the VALUES batches via
    // push() one chunk at a time. The streaming counter MUST bump, and the rows MUST
    // still land identically (R6: the streaming sink and the legacy on_execute path
    // share the same append core).
    auto config = test_create_config("/tmp/test_streaming_dml_values");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.t (id, val) VALUES (1, 10), (2, 20), (3, 30);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    // PATH: routed through the push-based streaming pipeline (VALUES source).
    REQUIRE(runs_after > runs_before);

    // CORRECTNESS: every VALUES row landed with values intact.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 3);
    }
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 20);
    }
}

TEST_CASE("integration::cpp::streaming_dml::insert_values_returning_streams") {
    // INSERT...VALUES...RETURNING over the raw_data SOURCE: the streaming sink folds
    // the VALUES batches via push(), commits in await_async_and_resume, then reads the
    // appended segment back for the RETURNING projection. Proves the source -> sink ->
    // readback path lands the right RETURNING rows when VALUES is a streaming source.
    auto config = test_create_config("/tmp/test_streaming_dml_values_returning");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "INSERT INTO StreamDb.t (id, val) VALUES (1, 10), (2, 20) RETURNING id, val * 2 AS d;");
        INFO("INSERT VALUES RETURNING error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // PATH: streamed through the VALUES source.

    // The two rows landed (RETURNING readback is not order-guaranteed, spot-check via SELECT).
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 10);
    }
}

TEST_CASE("integration::cpp::streaming_dml::delete_predicate_streams_and_lands") {
    auto config = test_create_config("/tmp/test_streaming_dml_delete");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());
    // Index on id so the post-delete point lookup goes through the index — an
    // index-consistency check that the DELETE's index mirror ran for the streamed
    // batches (a deleted key must no longer be found via the index).
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_id ON StreamDb.t (id);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // Selective predicate DELETE over a multi-batch scan. `val < threshold` is a
    // pure compare on a non-indexed column, so the planner pushes it into the
    // full_scan (role()==source); with the DELETE operator a SINK the whole
    // delete->full_scan chain is a sourced streaming pipeline.
    constexpr unsigned kThreshold = 3000; // deletes rows val in [0, 3000) -> 3000 rows
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "DELETE FROM StreamDb.t WHERE val < " + std::to_string(kThreshold) + " RETURNING id, val;");
        INFO("DELETE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        // RETURNING: one row per deleted row.
        REQUIRE(cur->size() == kThreshold);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // PATH: routed through the streaming pipeline

    // CORRECTNESS: only rows with val >= threshold remain.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() ==
                static_cast<uint64_t>(kRowCount - kThreshold));
    }
    // INDEX CONSISTENCY: a deleted key (id == 5, val 5 < threshold) is gone via the
    // index point lookup; a surviving key (id == 4000) is still found.
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE id = 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE id = 4000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 4000);
    }
}

TEST_CASE("integration::cpp::streaming_dml::update_predicate_streams_and_lands") {
    auto config = test_create_config("/tmp/test_streaming_dml_update");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());
    // Index on val so the post-update point lookup goes through the index — an
    // index-consistency check that the UPDATE's index mirror (old delete + new
    // insert) ran for the streamed batches.
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_val ON StreamDb.t (val);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // Selective predicate UPDATE over a multi-batch scan. `id < threshold` is a
    // pure compare on a non-indexed column, so the planner pushes it into the
    // full_scan (role()==source); with the UPDATE operator a SINK the whole
    // update->full_scan chain is a sourced streaming pipeline. SET val = val + K
    // shifts the indexed column so the index-consistency check is meaningful.
    constexpr unsigned kThreshold = 2500;  // updates id in [0, 2500) -> 2500 rows
    constexpr int64_t kBump = 1000000;     // pushes updated val out of the [0, kRowCount) range
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "UPDATE StreamDb.t SET val = val + " + std::to_string(kBump) + " WHERE id < " +
                            std::to_string(kThreshold) + " RETURNING id, val;");
        INFO("UPDATE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kThreshold);
        // RETURNING reflects the NEW value: row with id==5 now has val 5 + kBump.
        // (RETURNING rows are not order-guaranteed; spot-check via a later SELECT.)
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // PATH: routed through the streaming pipeline

    // CORRECTNESS: an updated row carries the bumped value.
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 5 + kBump);
    }
    // A non-updated row keeps its original value.
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 3000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3000);
    }
    // INDEX CONSISTENCY: the OLD val (5) is gone from the index; the NEW val
    // (5 + kBump) is found. This proves the index mirror's delete-old + insert-new
    // ran for the streamed batches.
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE val = 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE val = " + std::to_string(5 + kBump) + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 5);
    }
}

// ============================================================================
// Constrained DML streams end-to-end (constraint-operator migration).
//
// fk_check / fk_cascade / check_constraint are role()==sink + needs_async_finalize:
// each is the PARENT of a DML sink in the plan chain. Marking them sinks lets the
// WHOLE chain (constraint -> DML -> scan/raw_data) route through execute_pipeline
// instead of falling back to the legacy materialize path at the first role()==none.
// The executor pumps the source into the DML's push(), then drives
// await_async_and_resume BOTTOM-UP: the DML commits first (snapshotting the written
// rows into constraint_input_), then the constraint validates / cascades them.
//
// These tests assert BOTH (a) the constrained statement still enforces / cascades
// identically, and (b) it routes through the streaming pipeline (the run counter
// bumps). An INSERT...SELECT over a MULTI-BATCH scan is used so the constraint reads
// the DML's snapshot, not the scan SOURCE's output_ (empty when streaming).
// ============================================================================

TEST_CASE("integration::cpp::streaming_dml::fk_check_streams_insert_select") {
    auto config = test_create_config("/tmp/test_streaming_dml_fk_check");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.parent (id bigint, name text);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.child (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_ok (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_bad (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "ALTER TABLE StreamDb.child ADD CONSTRAINT fk_p "
                 "FOREIGN KEY (parent_id) REFERENCES StreamDb.parent (id);")
                ->is_success());

    // One parent row (id == 1). A multi-batch source that all references id == 1 is
    // valid; a source referencing a missing parent (id == 99) must be rejected.
    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.parent (id, name) VALUES (1, 'p1');")->is_success());
    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.src_ok (id, parent_id) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", 1)" << (i + 1 == kRowCount ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }
    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.src_bad (id, parent_id) VALUES (1, 99);")->is_success());

    // Valid constrained INSERT...SELECT over a multi-batch scan: streams and lands.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur =
            exec(dispatcher, "INSERT INTO StreamDb.child (id, parent_id) SELECT id, parent_id FROM StreamDb.src_ok;");
        INFO("constrained INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    // PATH: the fk_check -> insert -> scan chain routed through execute_pipeline.
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.child;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }

    // FK VIOLATION through the streaming path: referencing a missing parent surfaces
    // as an error cursor (Rule 2/9: never thrown). This is the constraint enforcement
    // the migration must preserve through the streaming chain.
    {
        auto cur =
            exec(dispatcher, "INSERT INTO StreamDb.child (id, parent_id) SELECT id, parent_id FROM StreamDb.src_bad;");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::streaming_dml::check_constraint_streams_insert_select") {
    auto config = test_create_config("/tmp/test_streaming_dml_check");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    // age is bigint so the CHECK constant (parsed as bigint) compares same-type
    // (mirrors test_sql_features::check_constraint; an int32 column would hit an
    // unrelated logical_value_t type-coercion gap in the predicate, not the path).
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.items (id bigint, age bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_ok (id bigint, age bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_bad (id bigint, age bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "ALTER TABLE StreamDb.items ADD CONSTRAINT chk_age CHECK (age > 0);")
                ->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.src_ok (id, age) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << (i + 1) << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }
    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.src_bad (id, age) VALUES (1, -5);")->is_success());

    // Valid CHECK over a multi-batch INSERT...SELECT: streams and lands.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.items (id, age) SELECT id, age FROM StreamDb.src_ok;");
        INFO("CHECK INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    // PATH: the check_constraint -> insert -> scan chain routed through execute_pipeline.
    REQUIRE(runs_after > runs_before);
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.items;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }

    // CHECK VIOLATION through the streaming path: age <= 0 surfaces as an error
    // cursor (Rule 2/9: never thrown). Enforcement preserved through the chain.
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.items (id, age) SELECT id, age FROM StreamDb.src_bad;");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::streaming_dml::fk_cascade_streams_delete") {
    auto config = test_create_config("/tmp/test_streaming_dml_cascade");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.parent (id bigint, val text);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.child (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "ALTER TABLE StreamDb.child ADD CONSTRAINT fk_c "
                 "FOREIGN KEY (parent_id) REFERENCES StreamDb.parent (id) ON DELETE CASCADE;")
                ->is_success());

    // Two parents; many children reference parent 1 (multi-batch child set).
    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.parent (id, val) VALUES (1, 'p1'), (2, 'p2');")->is_success());
    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.child (id, parent_id) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << ((i % 2) + 1) << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }

    // DELETE parent 1: the cascade removes every child referencing it. The
    // fk_cascade -> delete -> scan chain streams (the delete sink snapshots the
    // matched parent row, the cascade reads it to find referencing children).
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "DELETE FROM StreamDb.parent WHERE id = 1;");
        INFO("cascade delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // PATH: routed through the streaming pipeline.

    // CORRECTNESS: children of parent 1 are gone; children of parent 2 remain.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.child WHERE parent_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.child WHERE parent_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / 2));
    }
}
