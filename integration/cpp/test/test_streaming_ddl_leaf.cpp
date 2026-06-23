// ============================================================================
// Streaming DDL/TXN LEAF operators — sourceless SINK-ROOT on the push-based path.
//
// Leaf DDL/txn operators (CREATE TABLE/INDEX/MATVIEW, ALTER COLUMN, DROP INDEX,
// SET TIMEZONE, VACUUM, CHECKPOINT, BEGIN) have no data pipeline: their entire
// effect is an async cross-actor commit inside await_async_and_resume. They lower
// to a SINGLE operator with no left child — a SOURCELESS SINK-ROOT.
//
// is_streaming_pipeline() now admits that shape (root->role()==sink &&
// root->left()==nullptr), and execute_pipeline() skips the source pump and drives
// the leaf through the bottom-up needs_async_finalize pass (the same drive the DML
// insert/delete/update sinks use). This retires the legacy on_execute +
// find_waiting_operator drive for these leaves.
//
// WHAT THESE TESTS ASSERT:
//   (a) PATH — each leaf DDL/txn statement bumps streaming_pipeline_runs(), proving
//       it routed through execute_pipeline as a sourceless sink-root (not legacy).
//       Reverting a leaf to role()==none makes the matching REQUIRE RED.
//   (b) CORRECTNESS — the statement still has its full effect (table created and
//       queryable, column added/renamed/dropped, index dropped, timezone set,
//       vacuum/checkpoint succeed, explicit BEGIN commits) — identical to before.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    // Run `sql`, assert success, and assert it routed through the streaming
    // pipeline (sourceless sink-root) by the streaming_pipeline_runs() bump.
    void exec_streamed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        const auto before = services::collection::executor::streaming_pipeline_runs();
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        const auto after = services::collection::executor::streaming_pipeline_runs();
        REQUIRE(after > before); // routed through execute_pipeline as a sink-root
    }
} // namespace

TEST_CASE("integration::cpp::streaming_ddl_leaf::create_table_streams_and_lands") {
    auto config = test_create_config("/tmp/test_streaming_ddl_create");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());

    // CREATE TABLE -> operator_create_collection_t, a sourceless sink-root.
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");

    // CORRECTNESS: the table exists and accepts inserts / reads.
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name) VALUES (1, 'a'), (2, 'b');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, name FROM D.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::alter_column_add_drop_rename_stream") {
    auto config = test_create_config("/tmp/test_streaming_ddl_alter");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");

    // Single-clause ALTER ADD -> operator_alter_column_add_t sourceless sink-root.
    exec_streamed(dispatcher, "ALTER TABLE D.t ADD COLUMN extra bigint;");
    // RENAME -> operator_alter_column_rename_t sink-root.
    exec_streamed(dispatcher, "ALTER TABLE D.t RENAME COLUMN extra TO renamed;");
    // DROP -> operator_alter_column_drop_t sink-root.
    exec_streamed(dispatcher, "ALTER TABLE D.t DROP COLUMN renamed;");

    // CORRECTNESS: after add+rename+drop the original columns still work.
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name) VALUES (7, 'z');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, name FROM D.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::drop_index_streams") {
    auto config = test_create_config("/tmp/test_streaming_ddl_dropidx");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");
    // CREATE INDEX is a 2-node chain (metadata -> backfill) and stays on the
    // legacy path; it is exercised here only to set up the DROP.
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_t_id ON D.t (id);")->is_success());

    // DROP INDEX -> operator_drop_index_t sourceless sink-root.
    exec_streamed(dispatcher, "DROP INDEX D.t.idx_t_id;");

    // CORRECTNESS: the table is still queryable after the index is gone.
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name) VALUES (3, 'c');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t WHERE id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::set_timezone_streams_and_validates") {
    auto config = test_create_config("/tmp/test_streaming_ddl_tz");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // SET TIMEZONE -> operator_set_timezone_t sourceless sink-root. A valid name
    // streams + succeeds.
    exec_streamed(dispatcher, "SET TIMEZONE TO 'UTC';");

    // CORRECTNESS: an invalid timezone name is still rejected (validation moved to
    // the top of await_async_and_resume — same single point for both entry paths).
    {
        auto cur = exec(dispatcher, "SET TIMEZONE TO 'Not/A_Real_Zone_XYZ';");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::vacuum_and_checkpoint_stream") {
    auto config = test_create_config("/tmp/test_streaming_ddl_maint");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint);");
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id) VALUES (1), (2), (3);")->is_success());

    // VACUUM -> operator_vacuum_t sourceless sink-root.
    exec_streamed(dispatcher, "VACUUM;");
    // CHECKPOINT -> operator_checkpoint_t sourceless sink-root.
    exec_streamed(dispatcher, "CHECKPOINT;");

    // CORRECTNESS: data survives maintenance.
    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::explicit_begin_commit_roundtrip") {
    auto config = test_create_config("/tmp/test_streaming_ddl_txn");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint);");

    // Explicit BEGIN -> operator_begin_transaction_t sourceless sink-root, then DML,
    // then COMMIT (commit operator stays on its dedicated drive). The whole txn must
    // commit so the row is visible afterwards.
    auto session = otterbrix::session_id_t();
    {
        const auto before = services::collection::executor::streaming_pipeline_runs();
        auto cur = dispatcher->execute_sql(session, "BEGIN;");
        REQUIRE(cur->is_success());
        const auto after = services::collection::executor::streaming_pipeline_runs();
        REQUIRE(after > before); // BEGIN routed through the sink-root path
    }
    REQUIRE(dispatcher->execute_sql(session, "INSERT INTO D.t (id) VALUES (42);")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "COMMIT;")->is_success());

    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t WHERE id = 42;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}
