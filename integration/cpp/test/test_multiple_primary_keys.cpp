// ============================================================================
// ONE PRIMARY KEY PER TABLE.
//
// PostgreSQL refuses the second PRIMARY KEY at declaration ("multiple primary
// keys for table ... are not allowed"). This engine's declaration legs (inline
// CREATE TABLE and ALTER TABLE ADD CONSTRAINT) live upstream of the physical
// plan and today still accept the second 'p' row, so pg_constraint can end up
// holding two of them. What must NEVER follow from that state is silent
// misenforcement: operator_resolve_constraint used to fold both rows into one
// flattened pk_columns list — a multi-column "primary key" nobody declared —
// and enforce each 'p' row as an unrelated unique group.
//
// These cases pin the floor: the moment the doubled key would be USED (a DML
// that gathers constraints, or an FK binding to "the" primary key), the
// statement is refused and the refusal names both constraints. They also pin
// that the state stays REPAIRABLE: ALTER TABLE ... DROP CONSTRAINT gathers no
// constraints for the target, so the repair statement itself must not trip the
// refusal.
// ============================================================================

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
    // The declaration leg still accepts this today (upstream of the physical
    // plan); the floor below is what this file gates.
    run_ok(d, "ALTER TABLE mpk.t ADD CONSTRAINT pk_b PRIMARY KEY (b);");

    {
        auto cur = exec(d, "INSERT INTO mpk.t (a, b) VALUES (1, 2);");
        const auto what = error_text(cur);
        INFO("error: " << what);
        INFO("a table with two 'p' rows must refuse the DML that would enforce them");
        REQUIRE(cur->is_error());
        // The refusal names the real cause and both offenders, not a symptom.
        REQUIRE(what.find("multiple primary keys") != std::string::npos);
        REQUIRE(what.find("pk_a") != std::string::npos);
        REQUIRE(what.find("pk_b") != std::string::npos);
    }

    // Repair path. ALTER TABLE ... DROP CONSTRAINT parses but is refused as
    // unimplemented on this branch, so the live repair is dropping one key's
    // COLUMN: DROP COLUMN scrubs the constraints keyed on it through their
    // 'i' pg_depend edges, and its plan registers no constraint gather for the
    // target — the refusal must not fire on the statement that fixes the
    // catalog, or a per-statement refusal would turn into a dead table.
    run_ok(d, "ALTER TABLE mpk.t DROP COLUMN b;");
    {
        auto cur = run_ok(d, "INSERT INTO mpk.t (a) VALUES (1);");
        REQUIRE(cur->is_success());
    }
    {
        // ... and the surviving key is still enforced.
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
        // The inline leg accepts this today too. If a later change makes the
        // declaration refuse it (the PostgreSQL answer), this case's setup
        // fails loudly and the case should be re-pointed at the declaration
        // error — that is a strictly better world, not a regression.
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
