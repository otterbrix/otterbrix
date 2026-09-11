#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher,
                     const otterbrix::session_id_t& session,
                     const std::string& sql) {
        auto cursor = dispatcher->execute_sql(session, sql);
        INFO("statement: " << sql);
        return cursor;
    }

    std::size_t committed_rows(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.marks;");
        REQUIRE(cursor->is_success());
        return cursor->size();
    }

    void create_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.marks (id bigint);")->is_success());
        }
    }
} // namespace

TEST_CASE("integration::cpp::autocommit::on_commits_each_statement") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/on_commits_each_statement"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    // The default: each statement is committed before the next one runs, on one shared session.
    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 2);
}

TEST_CASE("integration::cpp::autocommit::off_defers_until_commit") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/off_defers_until_commit"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    // Nothing committed yet, though both statements reported success.
    REQUIRE(committed_rows(dispatcher) == 0);

    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 2);
}

TEST_CASE("integration::cpp::autocommit::off_rolls_back_everything_uncommitted") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/off_rolls_back"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());

    REQUIRE(committed_rows(dispatcher) == 0);
}

TEST_CASE("integration::cpp::autocommit::commit_starts_next_transaction") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/commit_starts_fresh"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::the_setting_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/survives_reopen"));
    test_clear_directory(config);
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        create_table(dispatcher);
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "SET autocommit = off;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    }
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
        REQUIRE(committed_rows(dispatcher) == 1);
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        REQUIRE(committed_rows(dispatcher) == 2);
    }
}
