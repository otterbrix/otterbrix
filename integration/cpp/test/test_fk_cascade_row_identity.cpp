// operator_fk_cascade's SET NULL / SET DEFAULT branch used to slice a flat child-id list positionally against
// the fetched chunks, so a row invisible to the acting transaction shifted every later id onto the wrong
// child; it now slices by chunk.row_ids instead. These tests assert row content, not count, since a shifted
// id set still reports the same count.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    // Sentinel: no seeded parent has id 0, so 0 in the expectation tables means NULL.
    constexpr int64_t kNullParent = 0;

    cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher,
                     otterbrix::session_id_t& session,
                     const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    std::vector<int64_t> column_i64(const cursor_t_ptr& cur, uint64_t col) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(col, row).value<int64_t>());
        }
        return out;
    }

    // Children of the deleted parent are interleaved with an untouched parent's, and one is deleted by this
    // transaction before the cascade runs, so a positional slip lands on a row that must not move.
    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& del_action) {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "CREATE DATABASE FkDb;")->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "CREATE TABLE FkDb.parent (id bigint, val text);")->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "CREATE TABLE FkDb.child (id bigint, parent_id bigint, tag bigint);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher,
                        s,
                        "ALTER TABLE FkDb.child ADD CONSTRAINT fk_c "
                        "FOREIGN KEY (parent_id) REFERENCES FkDb.parent (id) ON DELETE " +
                            del_action + ";")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher,
                        s,
                        "INSERT INTO FkDb.parent (id, val) VALUES (1, 'p1'), (2, 'p2'), (3, 'p3');")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher,
                        s,
                        "INSERT INTO FkDb.child (id, parent_id, tag) VALUES "
                        "(10, 1, 10), (20, 2, 20), (30, 3, 30), (21, 2, 21), "
                        "(31, 3, 31), (22, 2, 22), (32, 3, 32);")
                        ->is_success());
        }
    }

    void require_children(otterbrix::wrapper_dispatcher_t* dispatcher,
                          otterbrix::session_id_t& session,
                          const std::vector<int64_t>& ids,
                          const std::vector<int64_t>& parent_ids) {
        auto cur = run(dispatcher, session, "SELECT id, parent_id, tag FROM FkDb.child ORDER BY id;");
        INFO("child read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == ids.size());
        REQUIRE(column_i64(cur, 0) == ids);
        // tag was set equal to id at insert time and never changed, so tag != id means identity moved.
        REQUIRE(column_i64(cur, 2) == ids);
        for (std::size_t row = 0; row < ids.size(); ++row) {
            INFO("child id " << ids[row]);
            const bool want_null = (parent_ids[row] == kNullParent);
            REQUIRE(cur->value(1, row).is_null() == want_null);
            if (!want_null) {
                REQUIRE(cur->value(1, row).value<int64_t>() == parent_ids[row]);
            }
        }
    }

} // namespace

// The cascade deletes by row id via storage_delete_rows, so a drifted id set would remove 30 or 31 instead
// of 21/22.
TEST_CASE("integration::cpp::fk_cascade_row_identity::cascade_deletes_only_the_children_it_owns") {
    auto config = test_create_config(integration_fixture_path("test_fk_cascade_row_identity/cascade"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed(dispatcher, "CASCADE");

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());

    INFO("the same transaction deletes ONE of parent 2's children before the cascade");
    {
        auto cur = run(dispatcher, session, "DELETE FROM FkDb.child WHERE id = 20;");
        INFO("pre-delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    INFO("child 20 is invisible to this transaction from here on");
    require_children(dispatcher, session, {10, 21, 22, 30, 31, 32}, {1, 2, 2, 3, 3, 3});

    INFO("DELETE the parent — the cascade acts on a child set that no longer includes 20");
    {
        auto cur = run(dispatcher, session, "DELETE FROM FkDb.parent WHERE id = 2;");
        INFO("cascade error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    INFO("mid-transaction: exactly parent 2's children are gone, the interleaved ones intact");
    require_children(dispatcher, session, {10, 30, 31, 32}, {1, 3, 3, 3});

    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

    INFO("after COMMIT a fresh session sees the same rows");
    {
        auto fresh = otterbrix::session_id_t();
        require_children(dispatcher, fresh, {10, 30, 31, 32}, {1, 3, 3, 3});
        auto cur = run(dispatcher, fresh, "SELECT id FROM FkDb.parent ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == std::vector<int64_t>{1, 3});
    }
}

// With every child already gone, the cascade takes its empty-set exit; the parent delete must still succeed
// without moving other parents' children.
TEST_CASE("integration::cpp::fk_cascade_row_identity::cascade_over_an_already_emptied_child_set") {
    auto config = test_create_config(integration_fixture_path("test_fk_cascade_row_identity/cascade_empty"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed(dispatcher, "CASCADE");

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());

    {
        auto cur = run(dispatcher, session, "DELETE FROM FkDb.child WHERE parent_id = 2;");
        INFO("pre-delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = run(dispatcher, session, "DELETE FROM FkDb.parent WHERE id = 2;");
        INFO("cascade error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    require_children(dispatcher, session, {10, 30, 31, 32}, {1, 3, 3, 3});
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

    auto fresh = otterbrix::session_id_t();
    require_children(dispatcher, fresh, {10, 30, 31, 32}, {1, 3, 3, 3});
}

// SET NULL fetches child rows and writes them back, so a short reply from an invisible row can pair with
// the wrong ids and NULL parent 3's children instead of parent 2's.
TEST_CASE("integration::cpp::fk_cascade_row_identity::set_null_writes_only_the_children_it_owns") {
    auto config = test_create_config(integration_fixture_path("test_fk_cascade_row_identity/set_null"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed(dispatcher, "SET NULL");

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());

    {
        auto cur = run(dispatcher, session, "DELETE FROM FkDb.child WHERE id = 20;");
        INFO("pre-delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = run(dispatcher, session, "DELETE FROM FkDb.parent WHERE id = 2;");
        INFO("set null error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    INFO("mid-transaction: 21/22 NULLed, parent 3's children untouched");
    require_children(dispatcher, session, {10, 21, 22, 30, 31, 32}, {1, kNullParent, kNullParent, 3, 3, 3});

    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());

    INFO("after COMMIT a fresh session sees the same rows");
    {
        auto fresh = otterbrix::session_id_t();
        require_children(dispatcher, fresh, {10, 21, 22, 30, 31, 32}, {1, kNullParent, kNullParent, 3, 3, 3});
    }
}
