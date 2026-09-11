// target_names_of() left create_collection_t's relname empty, so register_plan_targets
// (enrich_logical_plan.cpp) skipped it and check_collection_exists always answered "does not
// exist", letting a second CREATE TABLE append a duplicate pg_class row under the same name.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>

namespace {

    using namespace test_helpers;

    // pid-qualified: a literal path here is shared by every binary running this file, so
    // unqualified runs truncate each other's segments.
    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_create_table_duplicate/") + leaf).string();
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

} // namespace

TEST_CASE("integration::cpp::create_table_duplicate::second_create_is_refused") {
    auto config = make_test_config(fixture_path("same_session"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE dup.t (id bigint);");

    auto again = exec(d, "CREATE TABLE dup.t (id bigint);");
    REQUIRE_FALSE(again->is_success());

    run_ok(d, "INSERT INTO dup.t (id) VALUES (1);");
    CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);
}

TEST_CASE("integration::cpp::create_table_duplicate::second_create_with_other_columns_is_refused") {
    auto config = make_test_config(fixture_path("other_columns"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE dup.t (id bigint);");

    auto again = exec(d, "CREATE TABLE dup.t (other text, third bigint);");
    REQUIRE_FALSE(again->is_success());

    run_ok(d, "INSERT INTO dup.t (id) VALUES (7);");
    auto cur = run_ok(d, "SELECT * FROM dup.t;");
    CHECK(cur->size() == 1);
}

// Zero columns is the shape the python scratch-table factory creates.
TEST_CASE("integration::cpp::create_table_duplicate::second_create_of_a_computing_table_is_refused") {
    auto config = make_test_config(fixture_path("computing"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE dup.g();");

    auto again = exec(d, "CREATE TABLE dup.g();");
    CHECK_FALSE(again->is_success());
}

// An inline PRIMARY KEY hangs a create_constraint_t child off the create node, and that
// child's target_names_of does name the table.
TEST_CASE("integration::cpp::create_table_duplicate::inline_constraint_form_is_refused_too") {
    auto config = make_test_config(fixture_path("inline_constraint"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE dup.k (id bigint PRIMARY KEY);");

    auto again = exec(d, "CREATE TABLE dup.k (id bigint PRIMARY KEY);");
    CHECK_FALSE(again->is_success());
}

TEST_CASE("integration::cpp::create_table_duplicate::if_not_exists_is_still_a_noop_success") {
    auto config = make_test_config(fixture_path("if_not_exists"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE IF NOT EXISTS dup.t (id bigint);");
    run_ok(d, "INSERT INTO dup.t (id) VALUES (1);");

    run_ok(d, "CREATE TABLE IF NOT EXISTS dup.t (id bigint);");
    CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);
}

// Duplicate detection re-resolves the catalog from disk on every statement, so a restart
// must not reopen the hole.
TEST_CASE("integration::cpp::create_table_duplicate::second_create_after_restart_is_refused") {
    auto config = test_create_config(fixture_path("restart"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        run_ok(d, "CREATE DATABASE dup;");
        run_ok(d, "CREATE TABLE dup.t (id bigint);");
        run_ok(d, "INSERT INTO dup.t (id) VALUES (1);");
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);

        auto again = exec(d, "CREATE TABLE dup.t (id bigint);");
        CHECK_FALSE(again->is_success());

        CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);
    }
}
