// Declaration legs accept a second 'p' row in pg_constraint (unlike PostgreSQL); refusal happens
// only at first USE (DML gather or FK bind), naming both constraints, and the doubled state stays repairable.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>

namespace {

    components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_multiple_primary_keys/") + leaf).string();
    }

    std::string error_text(const components::cursor::cursor_t_ptr& cur) {
        return cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                               : std::string{"none"};
    }

} // namespace

TEST_CASE("integration::cpp::multiple_pk::doubled_by_alter_is_refused_at_use_and_repairable") {
    auto config = test_create_config(fixture_path("alter"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE mpk;");
    run_ok(d, "CREATE TABLE mpk.t (a bigint, b bigint);");
    run_ok(d, "ALTER TABLE mpk.t ADD CONSTRAINT pk_a PRIMARY KEY (a);");
    run_ok(d, "ALTER TABLE mpk.t ADD CONSTRAINT pk_b PRIMARY KEY (b);");

    {
        auto cur = exec(d, "INSERT INTO mpk.t (a, b) VALUES (1, 2);");
        const auto what = error_text(cur);
        INFO("error: " << what);
        INFO("a table with two 'p' rows must refuse the DML that would enforce them");
        REQUIRE(cur->is_error());
        REQUIRE(what.find("multiple primary keys") != std::string::npos);
        REQUIRE(what.find("pk_a") != std::string::npos);
        REQUIRE(what.find("pk_b") != std::string::npos);
    }

    // DROP COLUMN scrubs the constraint via its pg_depend edge and registers no constraint gather
    // for the target, so repair must not trip the refusal (the other exit: test_alter_drop_constraint.cpp).
    run_ok(d, "ALTER TABLE mpk.t DROP COLUMN b;");
    {
        auto cur = run_ok(d, "INSERT INTO mpk.t (a) VALUES (1);");
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec(d, "INSERT INTO mpk.t (a) VALUES (1);");
        INFO("error: " << error_text(cur));
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::multiple_pk::doubled_inline_is_refused_at_use") {
    auto config = test_create_config(fixture_path("inline"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE mpk;");
    {
        // If a later change makes this declaration refuse the second PRIMARY KEY (the PostgreSQL
        // answer), this setup fails loudly and the case should move to that declaration error.
        auto cur = exec(d, "CREATE TABLE mpk.t2 (a bigint PRIMARY KEY, b bigint PRIMARY KEY);");
        INFO("error: " << error_text(cur));
        REQUIRE(cur->is_success());
    }

    auto cur = exec(d, "INSERT INTO mpk.t2 (a, b) VALUES (1, 2);");
    const auto what = error_text(cur);
    INFO("error: " << what);
    INFO("two inline PRIMARY KEYs are the same illegal state as two ALTER-added ones");
    REQUIRE(cur->is_error());
    REQUIRE(what.find("multiple primary keys") != std::string::npos);
}
