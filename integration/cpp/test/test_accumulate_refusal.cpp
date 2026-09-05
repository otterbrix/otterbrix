// ============================================================================
// A STATEMENT MUST NOT REPORT SUCCESS OVER WORK THAT WAS NEVER PARKED.
//
// manager_dispatcher_t::txn_accumulate_msg is the ONE door through which a
// statement's finished work reaches the dispatcher-owned transaction_t: base
// insert and delete ranges, the storage oids a CREATE brought up and a DROP
// retired, pg_catalog row ranges, pg_attribute commit-id backfills. It answers
// core::error_t and refuses with transaction_inactive when the session has no
// transaction_t to park any of that on (dispatcher.cpp,
// manager_dispatcher_t::txn_accumulate_msg).
//
// Its two callers in the executor — the DML tail and the DDL tail of
// execute_plan_full — used to `co_await std::move(acf);` without binding the
// answer. A refusal was therefore invisible to them: they went straight on to
// run_commit_pipeline_, which drained a transaction_t holding NOTHING, took the
// empty-COMMIT leg, allocated no commit id and published nothing — while the
// cursor still said success. Rows physically appended to the heap were never
// made visible and the user was told the statement worked.
//
// WHY THIS IS A SENTINEL AND NOT A DIRECT REPRODUCTION. The path is not named:
// execute_plan_full opens every statement with txn_begin_session_msg, whose
// begin_transaction is idempotent and always leaves an active txn behind, and
// nothing on the INSERT / UPDATE / DELETE / SET TIMEZONE / VACUUM / DDL routes
// ends that txn before the accumulate — the abort legs (txn_abort_msg, the
// empty-COMMIT abort inside txn_commit_drain_msg, the dispatcher's failure
// release) all run strictly after it. There is no SQL that puts the session in
// the refused state, so no test can drive the refusal through the front door.
//
// What this file pins instead is the INVARIANT the swallowed answer broke:
//
//     a statement reports success  <=>  its work is visible afterwards
//
// stated as an equality so that BOTH directions fail loudly. The defect breaks
// exactly one of them — success reported, work gone — so with the answer
// swallowed and txn_accumulate_msg forced to refuse, the equality goes red;
// with the answer read, the statement reports the refusal and the equality
// holds (false <=> false). That sensitivity was proven by injection: making
// txn_accumulate_msg refuse every payload carrying base appends turns the DML
// case below red on the un-fixed tail and leaves the equality intact on the
// fixed one.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

using namespace components::cursor;

namespace {

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher,
                      otterbrix::session_id_t& session,
                      const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    // Rows a fresh session (fresh snapshot) can see — i.e. rows that were really
    // published, not merely appended to the heap by an unparked statement.
    std::size_t visible_rows(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table) {
        auto cur = test_helpers::exec(dispatcher, "SELECT * FROM " + table + ";");
        REQUIRE(cur->is_success());
        return cur->size();
    }

} // namespace

TEST_CASE("integration::cpp::accumulate_refusal::a_successful_dml_statement_has_published_its_rows") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_accumulate_refusal/dml"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE AccDb;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE AccDb.t (id bigint, val bigint);")->is_success());

    // Autocommit INSERT: the accumulate parks the append range and the implicit
    // COMMIT right behind it publishes it. A refusal read by nobody leaves the
    // rows on the heap and the cursor claiming three rows were inserted.
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO AccDb.t (id, val) VALUES (1, 10), (2, 20), (3, 30);");
        const bool reported_success = ins->is_success();
        const bool work_is_visible = visible_rows(dispatcher, "AccDb.t") == 3;
        INFO("INSERT reported success but its rows were never published (or the reverse)");
        REQUIRE(reported_success == work_is_visible);
        REQUIRE(reported_success);
    }

    // Autocommit UPDATE: parks an append range (the new version) AND a delete
    // range (the old one).
    {
        auto upd = test_helpers::exec(dispatcher, "UPDATE AccDb.t SET val = 99 WHERE id = 2;");
        auto cur = test_helpers::exec(dispatcher, "SELECT * FROM AccDb.t WHERE val = 99;");
        REQUIRE(cur->is_success());
        const bool reported_success = upd->is_success();
        const bool work_is_visible = cur->size() == 1;
        INFO("UPDATE reported success but its new version was never published (or the reverse)");
        REQUIRE(reported_success == work_is_visible);
        REQUIRE(reported_success);
    }

    // Autocommit DELETE: parks a delete range only.
    {
        auto del = test_helpers::exec(dispatcher, "DELETE FROM AccDb.t WHERE id = 3;");
        const bool reported_success = del->is_success();
        const bool work_is_visible = visible_rows(dispatcher, "AccDb.t") == 2;
        INFO("DELETE reported success but the row is still visible (or the reverse)");
        REQUIRE(reported_success == work_is_visible);
        REQUIRE(reported_success);
    }
}

TEST_CASE("integration::cpp::accumulate_refusal::an_explicit_transaction_publishes_what_its_statements_reported") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_accumulate_refusal/explicit"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE AccDb;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE AccDb.t (id bigint, val bigint);")->is_success());

    // Inside BEGIN..COMMIT the accumulate is the ONLY channel: no implicit
    // commit follows the statement, so a refused park means the COMMIT two
    // statements later has nothing to publish and takes the empty-COMMIT leg.
    auto txn = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, txn, "BEGIN;")->is_success());
    auto ins = exec(dispatcher, txn, "INSERT INTO AccDb.t (id, val) VALUES (1, 10), (2, 20);");
    const bool insert_reported_success = ins->is_success();
    auto commit = exec(dispatcher, txn, "COMMIT;");

    const bool work_is_visible = visible_rows(dispatcher, "AccDb.t") == 2;
    INFO("an INSERT that reported success inside BEGIN..COMMIT did not survive the COMMIT (or the reverse)");
    REQUIRE((insert_reported_success && commit->is_success()) == work_is_visible);
    REQUIRE(insert_reported_success);
    REQUIRE(commit->is_success());
}

TEST_CASE("integration::cpp::accumulate_refusal::a_successful_ddl_statement_has_published_its_catalog_rows") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_accumulate_refusal/ddl"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE AccDb;")->is_success());

    // The DDL tail ships pg_class / pg_attribute / pg_depend row ranges and the
    // created storage oid through the same door. A refusal read by nobody leaves
    // a CREATE TABLE claiming success over a catalog that does not describe the
    // table, so the table is not there to be written.
    {
        auto ddl = test_helpers::exec(dispatcher, "CREATE TABLE AccDb.t (id bigint, val bigint);");
        auto use = test_helpers::exec(dispatcher, "INSERT INTO AccDb.t (id, val) VALUES (1, 10);");
        const bool reported_success = ddl->is_success();
        INFO("CREATE TABLE reported success but the table it claims to have created is not usable (or the reverse)");
        REQUIRE(reported_success == use->is_success());
        REQUIRE(reported_success);
    }

    // CREATE INDEX rides the same tail and additionally parks a created_index.
    // Here the invariant is not an equality but an unconditional one, because a
    // table without an index still answers a point lookup by scanning: WHATEVER
    // the CREATE INDEX ends up reporting, the row must remain findable. A
    // CREATE INDEX whose ranges were never parked leaves its engine registered
    // with manager_index_t holding entries that were never committed; that
    // engine then captures the lookup and answers it EMPTY, so the statement
    // does not merely fail to build an index — it takes rows away from later
    // readers of a table it never touched.
    {
        auto ddl = test_helpers::exec(dispatcher, "CREATE INDEX idx_acc ON AccDb.t (id);");
        auto use = test_helpers::exec(dispatcher, "SELECT * FROM AccDb.t WHERE id = 1;");
        REQUIRE(use->is_success());
        INFO("a lookup on the indexed column lost the row to a half-built index");
        REQUIRE(use->size() == 1);
        REQUIRE(ddl->is_success());
    }
}
