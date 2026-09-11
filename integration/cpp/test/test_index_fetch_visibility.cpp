// The index answers a SUPERSET filter, not a visibility one; the table decides what a reader may see.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // > row_group_size (1024): forces the absolute->group-local rebase for this row's slot.
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

    INFO("the reader's snapshot is live and the INDEX route works on it");
    {
        auto cur = exec(dispatcher, reader, "SELECT id, val FROM VisDb.t WHERE id = 1500;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

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
        // storage_fetch gathered cells without calling row_version_manager_t::fetch, leaking rows past the snapshot.
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

// Guards against over-fixing: a row committed before the snapshot stays visible even after a later delete.
TEST_CASE("integration::cpp::index_fetch_visibility::point_fetch_keeps_rows_the_snapshot_owns") {
    auto config = test_create_config(integration_fixture_path("test_index_fetch_visibility/retain"));
    test_clear_directory(config);
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
