#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <unistd.h>

namespace {

    using namespace test_helpers;

    std::string fixture_path(const char* leaf) {
        return (integration_fixture_path(std::string("test_alter_rename_column_collisions/") + leaf) /
                std::to_string(::getpid()))
            .string();
    }

    components::cursor::cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const otterbrix::session_id_t& session,
                                            const std::string& sql) {
        auto cursor = run(dispatcher, session, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cursor->is_error()
                               ? std::string{cursor->get_error().what.begin(), cursor->get_error().what.end()}
                               : std::string{"none"}));
        REQUIRE(cursor->is_success());
        return cursor;
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session) {
        run_ok(dispatcher, session, "CREATE DATABASE db;");
        run_ok(dispatcher, session, "CREATE TABLE db.t (a int, b int);");
        run_ok(dispatcher, session, "INSERT INTO db.t (a, b) VALUES (1, 2);");
    }

} // namespace

TEST_CASE("integration::cpp::alter_rename_column_collisions") {
    auto config = make_test_config(fixture_path("live"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    const auto session = otterbrix::session_id_t();
    seed(dispatcher, session);

    SECTION("a live name cannot be taken") {
        REQUIRE(run(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO b;")->is_error());
        INFO("both columns are untouched");
        CHECK(run_ok(dispatcher, session, "SELECT a, b FROM db.t;")->size() == 1);
    }

    SECTION("a column cannot be renamed to its own name") {
        REQUIRE(run(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO a;")->is_error());
    }

    SECTION("unquoted spellings fold to one name") {
        REQUIRE(run(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO A;")->is_error());
    }

    SECTION("quoted names of different case are different columns") {
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN \"b\" TO \"B\";");
        CHECK(run_ok(dispatcher, session, "SELECT * FROM db.t;")->column_count() == 2);
    }

    SECTION("a dropped column's name is free") {
        run_ok(dispatcher, session, "ALTER TABLE db.t DROP COLUMN b;");
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO b;");

        auto renamed = run_ok(dispatcher, session, "SELECT b FROM db.t;");
        REQUIRE(renamed->size() == 1);
        INFO("b reads what a held, not what the dropped b held");
        CHECK(renamed->value(0, 0).value<int32_t>() == 1);
        REQUIRE(run(dispatcher, session, "SELECT a FROM db.t;")->is_error());

        INFO("and the reused name takes writes");
        run_ok(dispatcher, session, "INSERT INTO db.t (b) VALUES (9);");
        CHECK(run_ok(dispatcher, session, "SELECT b FROM db.t;")->size() == 2);
    }

    SECTION("two columns cannot be renamed to one name in a single transaction") {
        run_ok(dispatcher, session, "BEGIN;");
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO x;");
        REQUIRE(run(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN b TO x;")->is_error());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_error());
        INFO("the failed transaction took the first rename with it");
        CHECK(run_ok(dispatcher, session, "SELECT a, b FROM db.t;")->size() == 1);
    }

    SECTION("a name freed earlier in the same transaction can be taken") {
        run_ok(dispatcher, session, "BEGIN;");
        run_ok(dispatcher, session, "ALTER TABLE db.t DROP COLUMN b;");
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO b;");
        run_ok(dispatcher, session, "COMMIT;");
        CHECK(run_ok(dispatcher, session, "SELECT b FROM db.t;")->size() == 1);
        REQUIRE(run(dispatcher, session, "SELECT a FROM db.t;")->is_error());
    }

    SECTION("a rename chain within one transaction still works") {
        run_ok(dispatcher, session, "BEGIN;");
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO tmp;");
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN b TO a;");
        run_ok(dispatcher, session, "COMMIT;");
        CHECK(run_ok(dispatcher, session, "SELECT tmp, a FROM db.t;")->size() == 1);
    }
}

TEST_CASE("integration::cpp::alter_rename_column_collisions::reused_name_survives_restart") {
    auto config = make_test_config(fixture_path("restart"));
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        const auto session = otterbrix::session_id_t();
        seed(dispatcher, session);
        run_ok(dispatcher, session, "ALTER TABLE db.t DROP COLUMN b;");
        run_ok(dispatcher, session, "ALTER TABLE db.t RENAME COLUMN a TO b;");
        run_ok(dispatcher, session, "CHECKPOINT;");
    }
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        const auto session = otterbrix::session_id_t();

        auto reloaded = run_ok(dispatcher, session, "SELECT b FROM db.t;");
        REQUIRE(reloaded->size() == 1);
        CHECK(reloaded->value(0, 0).value<int32_t>() == 1);

        run_ok(dispatcher, session, "INSERT INTO db.t (b) VALUES (9);");
        CHECK(run_ok(dispatcher, session, "SELECT b FROM db.t;")->size() == 2);
    }
}
