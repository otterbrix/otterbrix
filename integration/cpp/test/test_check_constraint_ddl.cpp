#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// A CHECK is accepted into the catalog only if it holds up as an expression: its columns and
// functions resolve, it reads as a condition, and it can be written back to SQL for storage.
//
// This is the gate that makes enforcement trustworthy later. A constraint that names a column
// which does not exist, or compares values with no type in common, used to be stored happily and
// then passed every row for the rest of the table's life — enforcement that reports success
// because it never managed to ask a question.

namespace {

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    void accepted(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& name, const std::string& predicate) {
        auto cur = run(dispatcher, "ALTER TABLE c.t ADD CONSTRAINT " + name + " CHECK (" + predicate + ");");
        INFO("must be accepted: CHECK (" << predicate << ")");
        CHECK(cur->is_success());
    }

    void refused(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& name, const std::string& predicate) {
        auto cur = run(dispatcher, "ALTER TABLE c.t ADD CONSTRAINT " + name + " CHECK (" + predicate + ");");
        INFO("must be refused: CHECK (" << predicate << ")");
        CHECK(cur->is_error());
    }

    configuration::config config_for(const std::string& name) {
        auto config = test_create_config(integration_fixture_path("test_check_constraint_ddl/" + name));
        test_clear_directory(config);
        config.wal.on = false;
        config.log.level = log_t::level::off;
        return config;
    }

} // namespace

TEST_CASE("integration::cpp::test_check_constraint_ddl::an_expression_is_accepted", "[checkddl]") {
    auto config = config_for("accepted");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint, m bigint, s text, a bigint[3]);")->is_success());

    // A CHECK is a row predicate, so it admits what a WHERE admits.
    accepted(d, "k01", "n > 0");
    accepted(d, "k02", "0 < n");
    accepted(d, "k03", "n * -1 > 0");
    accepted(d, "k04", "n + m > 10");
    accepted(d, "k05", "n < m");
    accepted(d, "k06", "n > 0 AND n < 100");
    accepted(d, "k07", "n < 0 OR n > 100");
    accepted(d, "k08", "NOT (n = 0)");
    accepted(d, "k09", "s IS NOT NULL");
    accepted(d, "k10", "n BETWEEN 1 AND 10");
    accepted(d, "k11", "n IN (1, 2, 3)");
    accepted(d, "k12", "n NOT IN (1, 2, 3)");
    accepted(d, "k13", "s LIKE 'a%'");
    accepted(d, "k14", "s NOT LIKE 'a%'");
    accepted(d, "k15", "abs(n) > 0");
    accepted(d, "k16", "abs(n - m) < 100");
    accepted(d, "k18", "CAST(n AS bigint) > 0");
    accepted(d, "k19", "a[1] > 0");
    accepted(d, "k20", "s <> 'it''s'");
    // A predicate is whatever yields a condition per row; CASE is not a special form here.
    accepted(d, "k21", "(CASE WHEN n > 0 THEN 1 ELSE 0 END) = 1");
    accepted(d, "k22", "n > 0 AND (CASE WHEN m > 0 THEN 1 ELSE 0 END) = 1");
    // A predicate is whatever yields a condition per row; CASE is not a special form here.
    accepted(d, "k21", "(CASE WHEN n > 0 THEN 1 ELSE 0 END) = 1");
    accepted(d, "k22", "n > 0 AND (CASE WHEN m > 0 THEN 1 ELSE 0 END) = 1");
}

TEST_CASE("integration::cpp::test_check_constraint_ddl::an_unsound_expression_is_refused", "[checkddl]") {
    auto config = config_for("refused");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint, s text);")->is_success());

    // Names a column the table does not have.
    refused(d, "b01", "nosuch > 0");
    // No type is common to the two sides.
    refused(d, "b02", "s > 0");
    // Folds many rows into one, so there is no row for it to answer for.
    refused(d, "b03", "count(id) > 0");
    // Not a condition — a CHECK has to say yes or no.
    refused(d, "b04", "n");
    refused(d, "b05", "n + 1");
    // Cannot be written back to SQL, so it could not be stored and re-read faithfully.
    refused(d, "b06", "n = ANY(ARRAY[1, 2])");
    // Not an expression at all.
    refused(d, "b07", "");
    // Asks about other rows, and a CHECK judges the one row in front of it.
    refused(d, "b09", "n > (SELECT 1)");
    // A CHECK reaches the same function registry a WHERE does, so a name that is not a function
    // there is not one here either.
    refused(d, "b08", "nosuchfn(n) > 0");

    // None of them reached the catalog, so the table still takes any row.
    REQUIRE(run(d, "INSERT INTO c.t (id, n, s) VALUES (1, -5, 'x');")->is_success());
}
