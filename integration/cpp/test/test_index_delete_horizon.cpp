// ============================================================================
// C5c — A COMMITTED DELETE MAY NOT ERASE THE INDEX ENTRY UNDER AN OLDER READER.
//
// The index answer is a SUPERSET and nothing else: manager_index_t says so in as
// many words, and since C4b the point fetch drops the ids the reader's snapshot
// must not see (test_index_fetch_visibility). That contract has ONE direction that
// is safe to be wrong in. Too MANY ids costs a fetch the table then discards; too
// FEW is a silently wrong answer, because a row whose id the index never names is
// never fetched and never filtered — the visibility check downstream cannot put
// back what the index dropped.
//
// commit_deletes used to send the PHYSICAL erase to the index agents at commit
// time. A reader whose snapshot is OLDER than that commit must still see the row —
// its delete_id is above the reader's snapshot horizon, so the table keeps it
// alive — but the index had already forgotten the id, so storage_fetch was never
// even asked for it. Two overlapping transactions are the whole reproduction; no
// checkpoint, no restart, no crash.
//
// The two SELECTs below differ ONLY in which column the equality names:
//   WHERE id = ...   -> INDEXED   -> Index Scan -> storage_fetch (the broken leg)
//   WHERE val = ...  -> UNINDEXED -> Seq Scan   -> the MVCC-correct control
// Same session, same snapshot, same row. A disagreement between the two IS the
// defect; agreement is the fix.
//
// THE EXPLAIN ASSERTION IS LOAD-BEARING. Without it this file is a full-scan test
// wearing an index's name: if the planner stops routing `WHERE id = ...` to the
// index for any reason, every assertion below still passes while the defect is
// live. The plan is checked with the SAME query text the assertions use.
//
// THE ROW IS PAST 1024 ON PURPOSE, for the reason test_index_fetch_visibility
// gives: version slots are addressed per row group while the point fetch names
// collection-ABSOLUTE ids, so a row inside the first row group cannot tell a
// correct rebase from a missing one.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <sstream>
#include <string>
#include <string_view>

using namespace components;
using namespace components::cursor;

namespace {
    // > row_group_size (1024): the row under test lives in the SECOND row group.
    constexpr unsigned kSeedRows = 2000;
    constexpr int64_t kDoomedId = 1500;

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher,
                      otterbrix::session_id_t& session,
                      const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        std::stringstream q;
        q << "INSERT INTO HorizonDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kSeedRows; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kSeedRows ? ";" : ", ");
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kSeedRows);
    }

    std::string indexed_query() {
        std::stringstream q;
        q << "SELECT id, val FROM HorizonDb.t WHERE id = " << kDoomedId << ";";
        return q.str();
    }

    std::string unindexed_query() {
        std::stringstream q;
        q << "SELECT id, val FROM HorizonDb.t WHERE val = " << kDoomedId << ";";
        return q.str();
    }
} // namespace

TEST_CASE("integration::cpp::index_delete_horizon::committed_delete_keeps_the_older_snapshots_row") {
    auto config = test_create_config(integration_fixture_path("test_index_delete_horizon/older_snapshot"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE DATABASE HorizonDb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE TABLE HorizonDb.t (id bigint, val bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE INDEX idx_id ON HorizonDb.t (id);")->is_success());
    }
    seed(dispatcher);

    INFO("the indexed query must actually be an Index Scan, or this whole file is a full-scan test");
    {
        auto s = otterbrix::session_id_t();
        auto plan = exec(dispatcher, s, "EXPLAIN " + indexed_query());
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed query:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }
    INFO("and the control query must NOT be, or the control proves nothing");
    {
        auto s = otterbrix::session_id_t();
        auto plan = exec(dispatcher, s, "EXPLAIN " + unindexed_query());
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the unindexed control query:\n" << text);
        REQUIRE(text.find("Index Scan") == std::string::npos);
    }

    // READER: takes its snapshot BEFORE the delete commits.
    auto reader = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, reader, "BEGIN;")->is_success());

    INFO("the reader's snapshot owns the row, and the index route finds it");
    {
        auto cur = exec(dispatcher, reader, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // WRITER: a separate session DELETES that row and COMMITS, after the reader's
    // snapshot was taken.
    {
        auto writer = otterbrix::session_id_t();
        std::stringstream del;
        del << "DELETE FROM HorizonDb.t WHERE id = " << kDoomedId << ";";
        auto cur = exec(dispatcher, writer, del.str());
        REQUIRE(cur->is_success());
    }

    INFO("CONTROL — UNINDEXED equality -> Seq Scan: the reader's snapshot still owns the row");
    {
        auto cur = exec(dispatcher, reader, unindexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("INDEXED equality -> Index Scan: it MUST agree with the control");
    {
        auto cur = exec(dispatcher, reader, indexed_query());
        REQUIRE(cur->is_success());
        // RED before C5c: commit_deletes physically erased the entry at commit
        // time, so the index named no id, storage_fetch was never asked, and the
        // reader lost a row its own snapshot still owns — while the scan leg above
        // kept it. A SUBSET answer, which no downstream filter can undo.
        REQUIRE(cur->size() == 1);
    }

    REQUIRE(exec(dispatcher, reader, "COMMIT;")->is_success());

    INFO("a session that starts AFTER the delete commits must not see the row through either route");
    {
        auto fresh = otterbrix::session_id_t();
        auto cur = exec(dispatcher, fresh, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    {
        auto fresh = otterbrix::session_id_t();
        auto cur = exec(dispatcher, fresh, unindexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// The mirror guard: holding the entry back must not turn the index into a store
// that never forgets. Once no snapshot can want the row any more, a fresh reader
// must still get the RIGHT answer through the index — and an UPDATE, which is a
// delete of the old key plus an insert of the new one, must move the row from one
// key to the other rather than answer under both.
TEST_CASE("integration::cpp::index_delete_horizon::the_index_still_forgets_once_nobody_is_looking") {
    auto config = test_create_config(integration_fixture_path("test_index_delete_horizon/forgets"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE DATABASE HorizonDb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE TABLE HorizonDb.t (id bigint, val bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE INDEX idx_id ON HorizonDb.t (id);")->is_success());
    }
    seed(dispatcher);

    {
        auto s = otterbrix::session_id_t();
        std::stringstream del;
        del << "DELETE FROM HorizonDb.t WHERE id = " << kDoomedId << ";";
        REQUIRE(exec(dispatcher, s, del.str())->is_success());
    }
    INFO("nothing held a snapshot across the delete: every later reader must miss the row");
    {
        auto s = otterbrix::session_id_t();
        auto cur = exec(dispatcher, s, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // UPDATE = delete of the old key + insert of the new one, in ONE transaction.
    // The insert half publishes at commit; the delete half is held back. If the
    // hold-back leaked into the answer the row would be findable under BOTH keys.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "UPDATE HorizonDb.t SET id = 90001 WHERE id = 1700;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = exec(dispatcher, s, "SELECT id, val FROM HorizonDb.t WHERE id = 90001;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    INFO("the OLD key must answer with nothing: the row moved, it was not copied");
    {
        auto s = otterbrix::session_id_t();
        auto cur = exec(dispatcher, s, "SELECT id, val FROM HorizonDb.t WHERE id = 1700;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}
