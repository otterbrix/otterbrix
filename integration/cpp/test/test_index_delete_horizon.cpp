// Index answers must be a SUPERSET (same contract as test_index_fetch_visibility): too few
// ids is silently wrong, since a dropped id is never fetched or filtered downstream.
// BEFORE: commit_deletes erased the index entry at commit time, so a reader whose snapshot
// predates the commit lost the row through the index while the table still held it.
// WHERE id=... (indexed) vs WHERE val=... (unindexed control) must agree; the EXPLAIN
// assertions below are load-bearing. The row sits past row_group_size (1024) — see
// test_index_fetch_visibility for why a first-row-group row can't tell a rebase from a miss.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
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

    cursor_t_ptr
    exec(otterbrix::wrapper_dispatcher_t* dispatcher, otterbrix::session_id_t& session, const std::string& sql) {
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
        // The broken leg: the physical erase already dropped the id, so storage_fetch never
        // sees it — a SUBSET answer no downstream filter can fix.
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

// The mirror guard: once no snapshot can want the row, a fresh reader must get the RIGHT
// answer, and an UPDATE (delete of the old key + insert of the new one) must move the row
// rather than answer under both keys.
TEST_CASE("integration::cpp::index_delete_horizon::the_index_still_forgets_once_nobody_is_looking") {
    auto config = test_create_config(integration_fixture_path("test_index_delete_horizon/forgets"));
    test_clear_directory(config);
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
