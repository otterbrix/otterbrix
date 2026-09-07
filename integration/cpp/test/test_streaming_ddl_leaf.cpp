// Leaf DDL/txn operators (CREATE TABLE/INDEX/MATVIEW, ALTER COLUMN, DROP INDEX, SET TIMEZONE,
// VACUUM, CHECKPOINT, BEGIN) have no data pipeline -- their effect is an async cross-actor commit
// inside await_async_and_resume, so they lower to a single sourceless sink-root operator.
// is_streaming_pipeline() admits that shape (root->role()==sink && root->left()==nullptr) and
// execute_pipeline() drives it via the same bottom-up needs_async_finalize pass the DML sinks use,
// retiring the legacy on_execute + find_waiting_operator drive for these leaves.
//
// Each case bumps streaming_pipeline_runs() to prove it routed through execute_pipeline, not the
// legacy path -- reverting a leaf to role()==none makes the matching REQUIRE red.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    void exec_streamed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        const auto before = services::collection::executor::streaming_pipeline_runs();
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        const auto after = services::collection::executor::streaming_pipeline_runs();
        REQUIRE(after > before);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_ddl_leaf::create_table_streams_and_lands") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_create"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());

    // CREATE TABLE -> operator_create_collection_t.
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");

    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name) VALUES (1, 'a'), (2, 'b');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, name FROM D.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::alter_column_add_drop_rename_stream") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_alter"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");

    // ALTER ADD -> operator_alter_column_add_t.
    exec_streamed(dispatcher, "ALTER TABLE D.t ADD COLUMN extra bigint;");
    // RENAME -> operator_alter_column_rename_t.
    exec_streamed(dispatcher, "ALTER TABLE D.t RENAME COLUMN extra TO renamed;");
    // DROP -> operator_alter_column_drop_t.
    exec_streamed(dispatcher, "ALTER TABLE D.t DROP COLUMN renamed;");

    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name) VALUES (7, 'z');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, name FROM D.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::multi_clause_alter_streams") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_multi_alter"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");

    // A 2-clause ALTER lowers to an all-sink chain (alter_column_add at the bottom, a second
    // alter_column sink above); the gate admits multi-node all-sink chains, and the bottom-up
    // drive runs the deepest clause first.
    exec_streamed(dispatcher, "ALTER TABLE D.t ADD COLUMN a bigint, ADD COLUMN b text;");

    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name, a, b) VALUES (1, 'x', 10, 'y');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, a, b FROM D.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::create_index_chain_streams") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_createidx"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, count bigint);");
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, count) VALUES (1, 10), (2, 20), (3, 30);")->is_success());

    // CREATE INDEX lowers to a 2-node all-sink chain: backfill (root) over metadata (leaf). The
    // bottom-up drive commits metadata (pg_catalog rows) before the backfill scans and flips
    // indisvalid=true.
    exec_streamed(dispatcher, "CREATE INDEX idx_t_count ON D.t (count);");

    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t WHERE count = 20;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, count) VALUES (4, 40);")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t WHERE count = 40;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::drop_index_streams") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_dropidx"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint, name text);");
    // Plain exec, not exec_streamed: its own streaming assertion lives in
    // create_index_chain_streams; this just sets up the DROP.
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_t_id ON D.t (id);")->is_success());

    // DROP INDEX -> operator_drop_index_t.
    exec_streamed(dispatcher, "DROP INDEX D.t.idx_t_id;");

    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id, name) VALUES (3, 'c');")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t WHERE id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::set_timezone_streams_and_validates") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_tz"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // SET TIMEZONE -> operator_set_timezone_t.
    exec_streamed(dispatcher, "SET TIMEZONE TO 'UTC';");

    // An invalid timezone must still be rejected: validation lives at the top of
    // await_async_and_resume, the single point for both entry paths.
    {
        auto cur = exec(dispatcher, "SET TIMEZONE TO 'Not/A_Real_Zone_XYZ';");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::vacuum_and_checkpoint_stream") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_maint"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint);");
    REQUIRE(exec(dispatcher, "INSERT INTO D.t (id) VALUES (1), (2), (3);")->is_success());

    // VACUUM -> operator_vacuum_t.
    exec_streamed(dispatcher, "VACUUM;");
    // CHECKPOINT -> operator_checkpoint_t.
    exec_streamed(dispatcher, "CHECKPOINT;");

    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::streaming_ddl_leaf::explicit_begin_commit_roundtrip") {
    auto config = test_create_config(integration_fixture_path("test_streaming_ddl_txn"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE D;")->is_success());
    exec_streamed(dispatcher, "CREATE TABLE D.t (id bigint);");

    // BEGIN -> operator_begin_transaction_t; COMMIT stays on its own dedicated drive, not
    // this sink-root path.
    auto session = otterbrix::session_id_t();
    {
        const auto before = services::collection::executor::streaming_pipeline_runs();
        auto cur = dispatcher->execute_sql(session, "BEGIN;");
        REQUIRE(cur->is_success());
        const auto after = services::collection::executor::streaming_pipeline_runs();
        REQUIRE(after > before);
    }
    REQUIRE(dispatcher->execute_sql(session, "INSERT INTO D.t (id) VALUES (42);")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "COMMIT;")->is_success());

    {
        auto cur = exec(dispatcher, "SELECT id FROM D.t WHERE id = 42;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}
