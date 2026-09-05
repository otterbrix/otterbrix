// ============================================================================
// WHICH CHILD ROWS AN FK REFERENTIAL ACTION IS ALLOWED TO TOUCH.
//
// The point fetch is a PRODUCER: collection_t::fetch gathers only rows the asking
// transaction may see and stamps result.row_ids with exactly those, so a reply can
// be SHORTER than the request. operator_fk_cascade's SET NULL / SET DEFAULT branch
// had been slicing a flat child-id list positionally against the fetched chunks; one
// dropped row would have shifted every later id and written the transform into
// somebody else's child rows. It slices by chunk.row_ids instead.
//
// These tests pin the OBSERVABLE half of that contract for every referential action,
// at the level the defect would show: WHICH ROWS SURVIVED and WHAT THEY HOLD, not
// how many there are. A count-only assertion cannot tell "deleted the two rows the
// cascade owned" from "deleted two rows one position over".
//
// THE ARRANGEMENT IS THE TEST. In every case below one child of the deleted parent
// is deleted BY THE SAME TRANSACTION before the cascade runs — the reachable way a
// child row is invisible to the very statement about to act on it — and the children
// of an UNTOUCHED parent are INTERLEAVED in insert order with the children of the
// deleted one. A positional slip of one therefore lands on a row that must not move,
// and the per-row content assertions see it.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    // parent_id sentinel for the expectation tables below: no seeded parent has
    // id 0, so 0 can only mean "this cell must be NULL".
    constexpr int64_t kNullParent = 0;

    cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher,
                     otterbrix::session_id_t& session,
                     const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    // Read one BIGINT column out of a result cursor, in the order the cursor
    // returned it. The callers always ORDER BY, so the comparison is on the set
    // AND on the order.
    std::vector<int64_t> column_i64(const cursor_t_ptr& cur, uint64_t col) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(col, row).value<int64_t>());
        }
        return out;
    }

    // Parent 1 / 2 / 3, and children whose insert order INTERLEAVES parent 2's
    // rows with parent 3's, so any positional slip crosses the parent boundary.
    //
    //   row 0: child 10 -> parent 1
    //   row 1: child 20 -> parent 2      (deleted by the same txn, pre-cascade)
    //   row 2: child 30 -> parent 3
    //   row 3: child 21 -> parent 2      (the referential action owns this one)
    //   row 4: child 31 -> parent 3
    //   row 5: child 22 -> parent 2      (and this one)
    //   row 6: child 32 -> parent 3
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
            // tag == id: a second, independent witness that a surviving row is the
            // row it claims to be and not a neighbour that got moved onto its id.
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher,
                        s,
                        "INSERT INTO FkDb.child (id, parent_id, tag) VALUES "
                        "(10, 1, 10), (20, 2, 20), (30, 3, 30), (21, 2, 21), "
                        "(31, 3, 31), (22, 2, 22), (32, 3, 32);")
                        ->is_success());
        }
    }

    // Every surviving child row, ordered by id, with its parent_id and tag.
    void require_children(otterbrix::wrapper_dispatcher_t* dispatcher,
                          otterbrix::session_id_t& session,
                          const std::vector<int64_t>& ids,
                          const std::vector<int64_t>& parent_ids) {
        auto cur = run(dispatcher, session, "SELECT id, parent_id, tag FROM FkDb.child ORDER BY id;");
        INFO("child read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == ids.size());
        REQUIRE(column_i64(cur, 0) == ids);
        // tag was written equal to id at insert time and no statement here ever
        // changes it, so tag != id means the row identity moved under us.
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

// ON DELETE CASCADE. The cascade must remove children 21 and 22 (parent 2's
// remaining rows) and nothing else — child 20 is already gone by this
// transaction's own hand, and 30/31/32 belong to a parent that was never
// touched. The cascade reaches storage_delete_rows by ROW ID, so the ids it
// deletes are the ids scan_by_keys reported; a set that drifted by one position
// would take 30 or 31 with it, and the surviving-rows assertion sees that.
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

// The same shape with the cascade set emptied entirely: the transaction deletes
// ALL of parent 2's children itself, so scan_by_keys reports nothing and the
// cascade has no ids at all. The branch takes its empty-set exit; the point is
// that the parent delete still succeeds and no OTHER parent's children move.
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

// ON DELETE SET NULL — the branch the row-identity contract actually bites on. It is the one that
// FETCHES the child rows and writes them back, so it is the one where a reply
// shorter than the request can be paired with the wrong ids. Children 21 and 22
// must end up NULL; 30/31/32, interleaved between them in row order, must still
// point at parent 3. Under positional slicing a short reply writes
// the NULL one row over — onto parent 3's children.
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
