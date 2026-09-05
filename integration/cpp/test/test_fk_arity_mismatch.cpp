// ============================================================================
// AN FK WHOSE TWO COLUMN LISTS DISAGREE IN LENGTH IS REFUSED BY THE DDL.
//
// `FOREIGN KEY (a, b) REFERENCES parent (x)` names two referencing columns and
// one referenced column. conkey and confkey are read POSITIONALLY by every
// reader (operator_resolve_constraint pairs child_col_names[i] with
// parent_col_names[i]), so a length disagreement has no pairing to make and both
// operators inherit it:
//
//   * operator_fk_cascade builds its keys-chunk from parent_col_indices (1
//     column) and sends child_col_names (2 names) as the key columns;
//   * operator_fk_check builds it from child_col_indices (2 columns) and sends
//     parent_col_names (1 name).
//
// The disk-side semi-join used to answer that mismatch with ONE EMPTY BUCKET PER
// KEY. For ON DELETE CASCADE / RESTRICT that empty answer reads as "no child row
// references this parent": the parent row is deleted and its children stay
// behind, referencing a row that no longer exists — silently.
//
// The operator floor that refuses this at DML time is still there
// (operator_fk_check.cpp), but a constraint that can never be evaluated must not
// be reported as ACCEPTED in the first place: today's engine answered
// `ADD CONSTRAINT` with SUCCESS and then refused EVERY INSERT into the child and
// EVERY DELETE from the parent, forever, with no way to take the constraint back
// (`ALTER TABLE ... DROP CONSTRAINT` is not implemented). So the refusal moved up
// to the DDL, where PostgreSQL puts it, and these cases now assert two things:
// the ALTER is refused and names the arity, and the constraint DID NOT HALF-LAND
// — the DML that a landed constraint would have blocked runs unimpeded, and
// specifically ON DELETE CASCADE does not cascade.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

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

    // Two tables and the rows that reference each other, then the lopsided
    // constraint — which is NOT a seed any more but the first assertion of every
    // case: the ALTER must be refused and must name the arity. `del_action` is
    // spliced into ON DELETE, because the refusal has to be independent of it.
    void seed_and_require_ddl_refusal(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& del_action) {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "CREATE DATABASE FkArity;")->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "CREATE TABLE FkArity.parent (id bigint, id2 bigint, val text);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "CREATE TABLE FkArity.child (id bigint, pid bigint, pid2 bigint);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "INSERT INTO FkArity.parent (id, id2, val) VALUES (1, 100, 'p1');")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, s, "INSERT INTO FkArity.child (id, pid, pid2) VALUES (10, 1, 100);")
                        ->is_success());
        }
        {
            // TWO referencing columns, ONE referenced column.
            auto s = otterbrix::session_id_t();
            auto cur = run(dispatcher,
                           s,
                           "ALTER TABLE FkArity.child ADD CONSTRAINT fk_lopsided "
                           "FOREIGN KEY (pid, pid2) REFERENCES FkArity.parent (id) ON DELETE " +
                               del_action + ";");
            INFO("ADD CONSTRAINT result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
            REQUIRE(cur->is_error());
            const std::string what{cur->get_error().what};
            REQUIRE(what.find("column count") != std::string::npos);
        }
    }

    void require_parent_ids(otterbrix::wrapper_dispatcher_t* dispatcher, const std::vector<int64_t>& ids) {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "SELECT id FROM FkArity.parent ORDER BY id;");
        INFO("parent read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == ids);
    }

    void require_child_ids(otterbrix::wrapper_dispatcher_t* dispatcher, const std::vector<int64_t>& ids) {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "SELECT id FROM FkArity.child ORDER BY id;");
        INFO("child read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == ids);
    }

} // namespace

// ON DELETE CASCADE over a lopsided FK. The cascade cannot be evaluated, so the
// ALTER that declares it is refused — and because it is refused, nothing about
// the two tables changes: the parent DELETE runs as an ordinary DELETE and, the
// half that matters, DOES NOT CASCADE. A half-landed constraint would show up
// here as child 10 disappearing along with its parent.
TEST_CASE("integration::cpp::fk_arity_mismatch::cascade_refuses_instead_of_orphaning") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/cascade"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed_and_require_ddl_refusal(dispatcher, "CASCADE");

    INFO("the parent DELETE runs: the refused constraint left nothing behind to block it");
    {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "DELETE FROM FkArity.parent WHERE id = 1;");
        // CHECK, not REQUIRE: the row assertions below carry the other half of the
        // statement — that the refused CASCADE did not fire — and must run either way.
        CHECK(cur->is_success());
    }

    INFO("nothing moved on the child side: the refused CASCADE did not cascade");
    require_parent_ids(dispatcher, {});
    require_child_ids(dispatcher, {10});
}

// ON DELETE RESTRICT over the same lopsided FK. The refusal must not depend on
// the referential action: RESTRICT reads the very same per-parent buckets, so it
// is the DECLARATION that is unevaluable, not the action written on it.
TEST_CASE("integration::cpp::fk_arity_mismatch::restrict_refuses_instead_of_orphaning") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/restrict"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed_and_require_ddl_refusal(dispatcher, "RESTRICT");

    INFO("the parent DELETE runs: the refused constraint left nothing behind to block it");
    {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "DELETE FROM FkArity.parent WHERE id = 1;");
        // CHECK, not REQUIRE: the row assertions below carry the other half of the
        // statement — that no child row was touched — and must run either way.
        CHECK(cur->is_success());
    }

    INFO("nothing moved on the child side");
    require_parent_ids(dispatcher, {});
    require_child_ids(dispatcher, {10});
}

// The INSERT side of the same constraint, and the reason the refusal had to move
// to the DDL. While the lopsided ALTER was ACCEPTED, operator_fk_check refused
// every INSERT into the child forever — a table taken permanently out of service
// by a statement the engine had reported as successful, with no DROP CONSTRAINT
// to undo it. With the ALTER refused there is no constraint, so the INSERT lands.
TEST_CASE("integration::cpp::fk_arity_mismatch::insert_names_the_real_defect") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/insert"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed_and_require_ddl_refusal(dispatcher, "NO ACTION");

    auto s = otterbrix::session_id_t();
    auto cur = run(dispatcher, s, "INSERT INTO FkArity.child (id, pid, pid2) VALUES (11, 1, 100);");
    INFO("insert result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
    REQUIRE(cur->is_success());

    INFO("and the row is really there — the child table was not left out of service");
    require_child_ids(dispatcher, {10, 11});
}
