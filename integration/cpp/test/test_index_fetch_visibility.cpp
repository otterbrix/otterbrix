// ============================================================================
// C4b — VISIBILITY ON THE POINT FETCH (index -> storage_fetch by row_id).
//
// The index answer is deliberately a SUPERSET filter, not a visibility one:
// manager_index_t::search_with_preferred_type says so in as many words — "which
// committed rows a reader may SEE is the table's decision, and storage_fetch
// applies it". storage_fetch did NOT apply it: the point-fetch path resolved the
// row group and gathered the cells without ever asking
// row_version_manager_t::fetch(txn, row). So the whole "index -> fetch by row_id"
// route handed a reader rows its snapshot must not contain.
//
// The two SELECTs below differ ONLY in which column the equality names:
//   WHERE id = ...   -> INDEXED  -> index_scan  -> storage_fetch (the broken leg)
//   WHERE val = ...  -> UNINDEXED-> full_scan   -> storage_fetch_next_batch (MVCC-correct)
// Same session, same snapshot, same row. The scan leg has always hidden the row;
// only the point-fetch leg leaked it, so a disagreement between the two is the
// defect itself and not a claim about snapshot semantics in general.
//
// THE ROW IS PAST 1024 ON PURPOSE. Version slots are addressed per row group
// (A6) while the point fetch names collection-ABSOLUTE row ids, so a row inside
// the FIRST row group cannot tell a correct rebase from a missing one — every
// MVCC test that stops at ten rows is blind to that whole class. The table is
// seeded past one row group so the row under test lives in the second.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // > row_group_size (1024): the row appended after the reader's snapshot lands
    // in the SECOND row group, so its version slot is only found through the
    // absolute->group-local rebase.
    constexpr unsigned kSeedRows = 2000;
    constexpr int64_t kLateId = 500000;

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher,
                      otterbrix::session_id_t& session,
                      const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        std::stringstream q;
        q << "INSERT INTO VisDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kSeedRows; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kSeedRows ? ";" : ", ");
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kSeedRows);
    }
} // namespace

TEST_CASE("integration::cpp::index_fetch_visibility::point_fetch_honours_the_readers_snapshot") {
    auto config = test_create_config(integration_fixture_path("test_index_fetch_visibility/snapshot"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE DATABASE VisDb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE TABLE VisDb.t (id bigint, val bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE INDEX idx_id ON VisDb.t (id);")->is_success());
    }
    seed(dispatcher);

    // READER: opens a transaction and takes its snapshot BEFORE the writer commits.
    auto reader = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, reader, "BEGIN;")->is_success());

    INFO("the reader's snapshot is live and the INDEX route works on it");
    {
        // A seeded row past 1024, read through the same index leg the assertions
        // below use — proves the route is the index point fetch, not a scan.
        auto cur = exec(dispatcher, reader, "SELECT id, val FROM VisDb.t WHERE id = 1500;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // WRITER: a separate session commits ONE new row AFTER the reader's snapshot.
    // It lands at absolute row 2000 — the second row group.
    {
        auto writer = otterbrix::session_id_t();
        std::stringstream ins;
        ins << "INSERT INTO VisDb.t (id, val) VALUES (" << kLateId << ", " << kLateId << ");";
        auto cur = exec(dispatcher, writer, ins.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("UNINDEXED equality -> full scan: the reader's snapshot already hides the late row");
    {
        std::stringstream q;
        q << "SELECT id, val FROM VisDb.t WHERE val = " << kLateId << ";";
        auto cur = exec(dispatcher, reader, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("INDEXED equality -> index point fetch: MUST hide the same row for the same reader");
    {
        std::stringstream q;
        q << "SELECT id, val FROM VisDb.t WHERE id = " << kLateId << ";";
        auto cur = exec(dispatcher, reader, q.str());
        REQUIRE(cur->is_success());
        // RED before C4b: the index returns the row_id (a superset answer) and
        // storage_fetch gathered it without consulting the row version manager,
        // so the reader saw a row committed after its own snapshot — while the
        // scan leg above hid it.
        REQUIRE(cur->size() == 0);
    }

    REQUIRE(exec(dispatcher, reader, "COMMIT;")->is_success());

    INFO("a session that starts AFTER the commit sees the row through the same index route");
    {
        auto fresh = otterbrix::session_id_t();
        std::stringstream q;
        q << "SELECT id, val FROM VisDb.t WHERE id = " << kLateId << ";";
        auto cur = exec(dispatcher, fresh, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

// The reader must not be over-filtered either: rows committed BEFORE its snapshot
// stay visible through the point fetch for the whole transaction, including after
// another session has deleted and committed them. This is the guard against
// "fix visibility by hiding everything".
TEST_CASE("integration::cpp::index_fetch_visibility::point_fetch_keeps_rows_the_snapshot_owns") {
    auto config = test_create_config(integration_fixture_path("test_index_fetch_visibility/retain"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE DATABASE VisDb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE TABLE VisDb.t (id bigint, val bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE INDEX idx_id ON VisDb.t (id);")->is_success());
    }
    seed(dispatcher);

    auto reader = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, reader, "BEGIN;")->is_success());
    {
        auto cur = exec(dispatcher, reader, "SELECT id, val FROM VisDb.t WHERE id = 1700;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // Another session deletes that row inside an OPEN transaction and never commits.
    auto deleter = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, deleter, "BEGIN;")->is_success());
    {
        auto cur = exec(dispatcher, deleter, "DELETE FROM VisDb.t WHERE id = 1700;");
        REQUIRE(cur->is_success());
    }

    INFO("the deleting transaction does not see its own uncommitted delete");
    {
        auto cur = exec(dispatcher, deleter, "SELECT id, val FROM VisDb.t WHERE id = 1700;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("every OTHER reader still sees the row: an uncommitted delete hides nothing from them");
    {
        auto cur = exec(dispatcher, reader, "SELECT id, val FROM VisDb.t WHERE id = 1700;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    REQUIRE(exec(dispatcher, deleter, "ROLLBACK;")->is_success());
    REQUIRE(exec(dispatcher, reader, "COMMIT;")->is_success());

    INFO("after the ROLLBACK the row is there for everyone");
    {
        auto fresh = otterbrix::session_id_t();
        auto cur = exec(dispatcher, fresh, "SELECT id, val FROM VisDb.t WHERE id = 1700;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}
