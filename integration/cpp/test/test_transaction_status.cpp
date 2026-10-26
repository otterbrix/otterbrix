#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/session/transaction_status.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <string>

using namespace components;
using namespace components::cursor;
using components::session::transaction_status_t;

namespace {
    cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        auto cursor = dispatcher->execute_sql(session, sql);
        INFO("statement: " << sql);
        return cursor;
    }

    void create_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto setup = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, setup, "CREATE SCHEMA TestDatabase;")->is_success());
        REQUIRE(run(dispatcher, setup, "CREATE TABLE TestDatabase.marks (id bigint);")->is_success());
    }
} // namespace

TEST_CASE("integration::cpp::transaction_status::autocommit_statements_are_idle") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/autocommit_idle"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    auto select = run(dispatcher, session, "SELECT 1;");
    REQUIRE(select->is_success());
    CHECK(select->transaction_status() == transaction_status_t::idle);

    auto insert = run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (1);");
    REQUIRE(insert->is_success());
    CHECK(insert->transaction_status() == transaction_status_t::idle);
}

TEST_CASE("integration::cpp::transaction_status::begin_enters_the_block") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/begin"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    auto begin = run(dispatcher, session, "BEGIN;");
    REQUIRE(begin->is_success());
    CHECK(begin->transaction_status() == transaction_status_t::in_transaction);

    auto select = run(dispatcher, session, "SELECT 1;");
    REQUIRE(select->is_success());
    CHECK(select->transaction_status() == transaction_status_t::in_transaction);

    auto insert = run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (1);");
    REQUIRE(insert->is_success());
    CHECK(insert->transaction_status() == transaction_status_t::in_transaction);

    auto commit = run(dispatcher, session, "COMMIT;");
    REQUIRE(commit->is_success());
    CHECK(commit->transaction_status() == transaction_status_t::idle);
}

TEST_CASE("integration::cpp::transaction_status::an_error_inside_the_block_fails_it") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/error_in_block"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());

    auto failing = run(dispatcher, session, "SELECT no_such_column FROM TestDatabase.marks;");
    REQUIRE(failing->is_error());
    CHECK(failing->transaction_status() == transaction_status_t::failed);

    auto refused = run(dispatcher, session, "SELECT 1;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::transaction_finalized);
    CHECK(refused->transaction_status() == transaction_status_t::failed);

    SECTION("rollback returns to idle") {
        auto rollback = run(dispatcher, session, "ROLLBACK;");
        REQUIRE(rollback->is_success());
        CHECK(rollback->transaction_status() == transaction_status_t::idle);
    }

    SECTION("commit ends it as a rollback and returns to idle") {
        auto commit = run(dispatcher, session, "COMMIT;");
        REQUIRE(commit->is_success());
        CHECK(commit->transaction_status() == transaction_status_t::idle);
    }
}

TEST_CASE("integration::cpp::transaction_status::rollback_to_savepoint_recovers_the_block") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/rollback_to"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    auto savepoint = run(dispatcher, session, "SAVEPOINT s;");
    REQUIRE(savepoint->is_success());
    CHECK(savepoint->transaction_status() == transaction_status_t::in_transaction);
    REQUIRE(run(dispatcher, session, "SELECT no_such_column FROM TestDatabase.marks;")->is_error());

    auto unknown = run(dispatcher, session, "ROLLBACK TO SAVEPOINT unknown;");
    REQUIRE(unknown->is_error());
    CHECK(unknown->transaction_status() == transaction_status_t::failed);

    auto recovered = run(dispatcher, session, "ROLLBACK TO SAVEPOINT s;");
    REQUIRE(recovered->is_success());
    CHECK(recovered->transaction_status() == transaction_status_t::in_transaction);

    REQUIRE(run(dispatcher, session, "ROLLBACK;")->transaction_status() == transaction_status_t::idle);
}

TEST_CASE("integration::cpp::transaction_status::an_autocommit_error_is_idle") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/autocommit_error"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    auto failing = run(dispatcher, session, "SELECT no_such_column FROM TestDatabase.marks;");
    REQUIRE(failing->is_error());
    CHECK(failing->transaction_status() == transaction_status_t::idle);

    auto parse_error = run(dispatcher, session, "SELEC 1;");
    REQUIRE(parse_error->is_error());
    CHECK(parse_error->transaction_status() == transaction_status_t::idle);
}

TEST_CASE("integration::cpp::transaction_status::a_parse_error_inside_the_block_fails_it") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/parse_error_in_block"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());

    auto parse_error = run(dispatcher, session, "SELEC 1;");
    REQUIRE(parse_error->is_error());
    CHECK(parse_error->transaction_status() == transaction_status_t::failed);

    REQUIRE(run(dispatcher, session, "ROLLBACK;")->transaction_status() == transaction_status_t::idle);
}

TEST_CASE("integration::cpp::transaction_status::autocommit_off_opens_a_block") {
    auto config = test_create_config(integration_fixture_path("test_transaction_status/autocommit_off"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    auto set = run(dispatcher, session, "SET autocommit = off;");
    REQUIRE(set->is_success());
    CHECK(set->transaction_status() == transaction_status_t::idle);

    auto insert = run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (1);");
    REQUIRE(insert->is_success());
    CHECK(insert->transaction_status() == transaction_status_t::in_transaction);

    auto commit = run(dispatcher, session, "COMMIT;");
    REQUIRE(commit->is_success());
    CHECK(commit->transaction_status() == transaction_status_t::idle);
}
