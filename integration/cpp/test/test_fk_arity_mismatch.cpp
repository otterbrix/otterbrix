// conkey/confkey are read positionally (operator_resolve_constraint pairs
// child_col_names[i] with parent_col_names[i]), so a length mismatch between the two column
// lists has no pairing to make; the disk-side semi-join used to answer it with one empty
// bucket per key, which for ON DELETE CASCADE/RESTRICT reads as "no child references this
// parent" -- the parent row is deleted and orphaned children stay behind silently.
// The DML-time floor still refuses it (operator_fk_check.cpp), but ADD CONSTRAINT used to
// answer SUCCESS and then refuse every INSERT/DELETE on the tables until a DROP CONSTRAINT.
// Refusal moved to the DDL (as PostgreSQL does it); these cases assert the ALTER is refused
// AND that DML on the tables runs unimpeded afterwards (constraint did not half-land).

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

    // `del_action` is spliced into ON DELETE: the refusal must be independent of it.
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

// A half-landed constraint would show up here as child 10 disappearing with its parent.
TEST_CASE("integration::cpp::fk_arity_mismatch::cascade_refuses_instead_of_orphaning") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/cascade"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed_and_require_ddl_refusal(dispatcher, "CASCADE");

    INFO("the parent DELETE runs: the refused constraint left nothing behind to block it");
    {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "DELETE FROM FkArity.parent WHERE id = 1;");
        // CHECK, not REQUIRE: the row assertions below must still run either way.
        CHECK(cur->is_success());
    }

    INFO("nothing moved on the child side: the refused CASCADE did not cascade");
    require_parent_ids(dispatcher, {});
    require_child_ids(dispatcher, {10});
}

TEST_CASE("integration::cpp::fk_arity_mismatch::restrict_refuses_instead_of_orphaning") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/restrict"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    seed_and_require_ddl_refusal(dispatcher, "RESTRICT");

    INFO("the parent DELETE runs: the refused constraint left nothing behind to block it");
    {
        auto s = otterbrix::session_id_t();
        auto cur = run(dispatcher, s, "DELETE FROM FkArity.parent WHERE id = 1;");
        // CHECK, not REQUIRE: the row assertions below must still run either way.
        CHECK(cur->is_success());
    }

    INFO("nothing moved on the child side");
    require_parent_ids(dispatcher, {});
    require_child_ids(dispatcher, {10});
}

TEST_CASE("integration::cpp::fk_arity_mismatch::insert_names_the_real_defect") {
    auto config = test_create_config(integration_fixture_path("test_fk_arity_mismatch/insert"));
    test_clear_directory(config);
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
