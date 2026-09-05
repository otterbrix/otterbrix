// ============================================================================
// A REPEATED `DROP INDEX` MUST GIVE THE SAME ANSWER, NOT A SILENT SUCCESS.
//
// The shape below is no longer in the tree, and this file is the end-to-end pin
// that keeps it out. The mechanism was: `rewrite_drop_index` receives index_oid ==
// INVALID_OID, emits NO catalog delete specification, appends the drop_index_t
// marker anyway, and operator_drop_index_t — whose `if (!specs.empty())` guard
// skipped its own no-identity-row-deleted verdict when there were no specs — ran
// off the end into mark_executed(). The statement removed nothing and said it had.
//
// Both halves now refuse:
//   components/planner/planner.cpp, rewrite_drop_index — an unresolved oid is
//     index_not_exists, with `DROP INDEX IF EXISTS` the single carve-out (an
//     honest empty sequence).
//   components/physical_plan/operators/operator_drop_index.cpp:37-47 — empty specs
//     (or no disk actor) is a refusal before any work; :115-129 refuses "specs ran,
//     zero identity rows deleted".
//
// There is no planner catalog cache to go stale: planner_t is stateless and
// catalog_snapshot_t was removed — every statement re-resolves live through
// operator_resolve_table under its own transaction. So the residual risk is only
// that the two refusals above regress.
//
// The unit-level refusals are pinned already (components/planner/test/
// test_ddl_unresolved_refusal.cpp, test_maintenance_wiring_refusal.cpp). What is
// NOT pinned anywhere else is the SEQUENCE — a DROP INDEX that fails, followed by
// the same DROP INDEX again — at the SQL level. The one repeat-drop site in the
// suite, test_index.cpp, fires DROP_INDEX five times on an already-dropped index
// and discards every cursor, so a regression to silent success there is invisible.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>

namespace {

    using namespace test_helpers;

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_drop_index_repeat/") + leaf).string();
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string error_text(const components::cursor::cursor_t_ptr& cur) {
        if (!cur->is_error()) {
            return {};
        }
        return std::string{cur->get_error().what.begin(), cur->get_error().what.end()};
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        run_ok(d, "CREATE DATABASE dix;");
        run_ok(d, "CREATE TABLE dix.t (id bigint, v bigint);");
        run_ok(d, "INSERT INTO dix.t (id, v) VALUES (1, 10), (2, 20);");
        run_ok(d, "CREATE INDEX idx_id ON dix.t (id);");
    }

} // namespace

// The sequence: the first DROP INDEX succeeds, so the second one has no index
// left to drop and must say so. The defect this guards is a second call that
// reports SUCCESS having emitted not one delete spec.
TEST_CASE("integration::cpp::drop_index_repeat::dropping_twice_refuses_the_second_time") {
    auto config = make_test_config(fixture_path("twice"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    run_ok(d, "DROP INDEX dix.t.idx_id;");

    auto second = exec(d, "DROP INDEX dix.t.idx_id;");
    REQUIRE_FALSE(second->is_success());
    CHECK(error_text(second).find("does not exist") != std::string::npos);

    // A third one answers the same way — the verdict must be a function of the
    // catalog, not of how many times it has been asked.
    auto third = exec(d, "DROP INDEX dix.t.idx_id;");
    CHECK_FALSE(third->is_success());

    // The table and its rows are untouched by the refused statements.
    CHECK(run_ok(d, "SELECT id FROM dix.t;")->size() == 2);
}

// A DROP INDEX that FAILS must not leave the next one better off. The failure
// here is the plainest one available — the index never existed — and the point
// is that repeating it is still a refusal rather than the "already handled"
// success a surviving stamp would produce.
TEST_CASE("integration::cpp::drop_index_repeat::a_failed_drop_does_not_license_the_next_one") {
    auto config = make_test_config(fixture_path("failed"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto first = exec(d, "DROP INDEX dix.t.no_such_index;");
    REQUIRE_FALSE(first->is_success());
    const auto first_text = error_text(first);
    CHECK(first_text.find("does not exist") != std::string::npos);

    auto second = exec(d, "DROP INDEX dix.t.no_such_index;");
    REQUIRE_FALSE(second->is_success());
    CHECK(error_text(second) == first_text);

    // The real index is still there and still droppable: the failed statements
    // neither removed it nor poisoned the path to it.
    run_ok(d, "DROP INDEX dix.t.idx_id;");
}

// The one form that is allowed to succeed over a missing index, and the reason
// the refusal above cannot simply be unconditional.
TEST_CASE("integration::cpp::drop_index_repeat::if_exists_is_a_noop_success") {
    auto config = make_test_config(fixture_path("if_exists"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    run_ok(d, "DROP INDEX dix.t.idx_id;");
    run_ok(d, "DROP INDEX IF EXISTS dix.t.idx_id;");
    run_ok(d, "DROP INDEX IF EXISTS dix.t.never_existed;");

    CHECK(run_ok(d, "SELECT id FROM dix.t;")->size() == 2);
}
