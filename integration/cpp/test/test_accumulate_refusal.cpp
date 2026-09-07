// txn_accumulate_msg is the only path for a statement's finished work to reach the transaction;
// an unchecked refusal there would let commit report success over an empty transaction. No SQL
// route reaches it without an active txn, so these cases pin success <=> visible as an equality.

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

    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO AccDb.t (id, val) VALUES (1, 10), (2, 20), (3, 30);");
        const bool reported_success = ins->is_success();
        const bool work_is_visible = visible_rows(dispatcher, "AccDb.t") == 3;
        INFO("INSERT reported success but its rows were never published (or the reverse)");
        REQUIRE(reported_success == work_is_visible);
        REQUIRE(reported_success);
    }

    // UPDATE parks both an append range (new version) and a delete range (old one).
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

    // No implicit commit follows a statement here, so a refused park would leave COMMIT nothing to publish.
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

    // The DDL tail ships pg_class/pg_attribute/pg_depend rows and the created oid through the same door.
    {
        auto ddl = test_helpers::exec(dispatcher, "CREATE TABLE AccDb.t (id bigint, val bigint);");
        auto use = test_helpers::exec(dispatcher, "INSERT INTO AccDb.t (id, val) VALUES (1, 10);");
        const bool reported_success = ddl->is_success();
        INFO("CREATE TABLE reported success but the table it claims to have created is not usable (or the reverse)");
        REQUIRE(reported_success == use->is_success());
        REQUIRE(reported_success);
    }

    // Unconditional, not an equality: an unindexed table still scans, so the row stays findable.
    {
        auto ddl = test_helpers::exec(dispatcher, "CREATE INDEX idx_acc ON AccDb.t (id);");
        auto use = test_helpers::exec(dispatcher, "SELECT * FROM AccDb.t WHERE id = 1;");
        REQUIRE(use->is_success());
        INFO("a lookup on the indexed column lost the row to a half-built index");
        REQUIRE(use->size() == 1);
        REQUIRE(ddl->is_success());
    }
}
