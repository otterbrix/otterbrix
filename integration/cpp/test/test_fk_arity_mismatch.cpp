// ============================================================================
// AN FK WHOSE TWO COLUMN LISTS DISAGREE IN LENGTH MUST NOT ENFORCE ITSELF QUIETLY.
//
// `FOREIGN KEY (a, b) REFERENCES parent (x)` names two referencing columns and
// one referenced column. Nothing on the DDL path rejects that (the transformer
// copies fk_attrs and pk_attrs verbatim, and enrich resolves each list on its
// own), so the constraint lands in pg_constraint with conkey of length 2 and
// confkey of length 1, and both operators inherit the disagreement:
//
//   * operator_fk_cascade builds its keys-chunk from parent_col_indices (1
//     column) and sends child_col_names (2 names) as the key columns;
//   * operator_fk_check builds it from child_col_indices (2 columns) and sends
//     parent_col_names (1 name).
//
// The disk-side semi-join used to answer that mismatch with ONE EMPTY BUCKET PER
// KEY. For ON DELETE CASCADE / RESTRICT that empty answer reads as "no child row
// references this parent": the parent row is deleted and its children stay
// behind, referencing a row that no longer exists — silently. A constraint that
// cannot be evaluated must refuse the statement, never report "nothing matched".
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

    // Two tables and the rows that reference each other, seeded BEFORE the
    // lopsided constraint exists so the seeding INSERTs never run through the
    // FK check themselves. `del_action` is spliced into ON DELETE.
    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& del_action) {
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
            REQUIRE(run(dispatcher,
                        s,
                        "ALTER TABLE FkArity.child ADD CONSTRAINT fk_lopsided "
                        "FOREIGN KEY (pid, pid2) REFERENCES FkArity.parent (id) ON DELETE " +
                            del_action + ";")
                        ->is_success());
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
// DELETE must fail and BOTH tables must be exactly as they were. The failure the
// test is written against is the opposite: the DELETE reports success, the
// parent row is gone, and child 10 is left pointing at nothing.
TEST_CASE("integration::cpp::fk_arity_mismatch::cascade_refuses_instead_of_orphaning") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/cascade"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed(dispatcher, "CASCADE");

    INFO("the parent DELETE must refuse: its cascade cannot be evaluated");
    {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "DELETE FROM FkArity.parent WHERE id = 1;");
        // CHECK, not REQUIRE: the row assertions below carry the other half of the
        // statement — that no orphan was produced — and must run either way.
        CHECK(cur->is_error());
    }

    INFO("nothing moved on either side");
    require_parent_ids(dispatcher, {1});
    require_child_ids(dispatcher, {10});
}

// ON DELETE RESTRICT over the same lopsided FK. RESTRICT reads the very same
// per-parent buckets and blocks on any non-empty one, so an all-empty answer
// lets the parent go exactly as CASCADE does — with the child rows still there.
TEST_CASE("integration::cpp::fk_arity_mismatch::restrict_refuses_instead_of_orphaning") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/restrict"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed(dispatcher, "RESTRICT");

    INFO("the parent DELETE must refuse");
    {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "DELETE FROM FkArity.parent WHERE id = 1;");
        // CHECK, not REQUIRE: the row assertions below carry the other half of the
        // statement — that no orphan was produced — and must run either way.
        CHECK(cur->is_error());
    }

    INFO("nothing moved on either side");
    require_parent_ids(dispatcher, {1});
    require_child_ids(dispatcher, {10});
}

// The INSERT side of the same constraint. operator_fk_check already FAILS the
// statement here, but for the wrong reason: an all-empty answer reads as "the
// referenced row is not in the parent table", which sends the user looking for a
// missing row that is right there. The check must name the real defect.
TEST_CASE("integration::cpp::fk_arity_mismatch::insert_names_the_real_defect") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/insert"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed(dispatcher, "NO ACTION");

    auto s = otterbrix::session_id_t();
    auto cur = run(dispatcher, s, "INSERT INTO FkArity.child (id, pid, pid2) VALUES (11, 1, 100);");
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what};
    INFO("insert error: " << what);
    REQUIRE(what.find("column count") != std::string::npos);
}
