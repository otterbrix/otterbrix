// ============================================================================
// A SECOND `CREATE TABLE t` MUST NOT REPORT SUCCESS.
//
// The refusal itself was not the missing part: the create_collection_t arm of
// services/collection/executor.cpp already calls check_collection_exists and
// answers either the IF NOT EXISTS no-op or
// core::error_code_t::table_already_exists. What was missing is its INPUT.
//
// check_collection_exists is a pure read of the plan's resolved catalog entries
// (services/dispatcher/validate_logical_plan.cpp) — it does not query the catalog.
// The entries come from register_plan_targets, which walks the tree asking
// target_names_of for each node's {dbname, relname} and skips any node whose
// relname is empty (enrich_logical_plan.cpp, `if (relname.empty()) continue`). The
// create_collection_t arm of target_names_of returned an EMPTY relname, so CREATE
// TABLE registered its namespace and nothing else; the existence check ran against a
// plan that had never asked about the name, always answered "does not exist", and
// the duplicate was written.
//
// The consequence is not a cosmetic wrong verdict. pg_class has no unique index on
// (relname, relnamespace) — system_table_schemas.cpp declares the columns not-null,
// not unique — so the second CREATE appends a SECOND row for the same name, under a
// NEW oid, with NEW storage. From then on operator_resolve_table binds the name to
// whichever of the two rows its scan reaches first, which is not necessarily the one
// the storage was made for.
//
// `inline_constraint_form_was_already_refused` below localises the defect: an inline
// constraint hangs a create_constraint_t child off the create node, and THAT node's
// target_names_of does name the table — so the identical statement was refused or
// accepted depending on whether a column happened to carry a PRIMARY KEY.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>

namespace {

    using namespace test_helpers;

    // pid-qualified: a literal /tmp path is shared by every binary that runs
    // this file, and two of them truncate each other's segments.
    std::string fixture_path(const char* leaf) {
        std::string p = "/tmp/test_create_table_duplicate_";
        p += std::to_string(static_cast<long>(::getpid()));
        p += '_';
        p += leaf;
        return p;
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

// The plain form — no constraint, no IF NOT EXISTS. This is the one that was
// accepted twice.
TEST_CASE("integration::cpp::create_table_duplicate::second_create_is_refused") {
    auto config = make_test_config(fixture_path("same_session"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE dup.t (id bigint);");

    auto again = exec(d, "CREATE TABLE dup.t (id bigint);");
    REQUIRE_FALSE(again->is_success());

    // The name still resolves, and to a table with the shape the FIRST
    // statement gave it: the refusal must leave the original alone rather than
    // half-replace it.
    run_ok(d, "INSERT INTO dup.t (id) VALUES (1);");
    CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);
}

// A different column list is still the same name, and the name is what
// collides. This is the shape that silently produced two pg_class rows with
// DIFFERENT schemas under one name.
TEST_CASE("integration::cpp::create_table_duplicate::second_create_with_other_columns_is_refused") {
    auto config = make_test_config(fixture_path("other_columns"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE dup.t (id bigint);");

    auto again = exec(d, "CREATE TABLE dup.t (other text, third bigint);");
    REQUIRE_FALSE(again->is_success());

    // The surviving table is the first one: `other` was never added.
    run_ok(d, "INSERT INTO dup.t (id) VALUES (7);");
    auto cur = run_ok(d, "SELECT * FROM dup.t;");
    CHECK(cur->size() == 1);
}

// Zero-column (computing) tables take the same path and were the shape the
// python scratch-table factory produces, so they get their own case.
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

// Localisation. This form was ALREADY refused before the fix, because its
// inline PRIMARY KEY hangs a create_constraint_t child off the create node and
// that child names the table. Two statements that differ only by a constraint
// keyword must not differ in whether duplicates are caught.
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

// IF NOT EXISTS keeps its no-op success. This is the carve-out the refusal is
// built around, and the reason the fix belongs at the demand-registration site
// rather than in a new unconditional guard.
TEST_CASE("integration::cpp::create_table_duplicate::if_not_exists_is_still_a_noop_success") {
    auto config = make_test_config(fixture_path("if_not_exists"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dup;");
    run_ok(d, "CREATE TABLE IF NOT EXISTS dup.t (id bigint);");
    run_ok(d, "INSERT INTO dup.t (id) VALUES (1);");

    // Second time: succeeds, creates nothing, and does not disturb the rows.
    run_ok(d, "CREATE TABLE IF NOT EXISTS dup.t (id bigint);");
    CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);
}

// The half the defect report calls out separately: a NEW PROCESS over the SAME
// catalog directory. The duplicate check reads the plan's resolved entries, and
// those are resolved from disk on every statement, so a restart must not
// re-open the hole.
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

        // The table came back.
        CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);

        auto again = exec(d, "CREATE TABLE dup.t (id bigint);");
        CHECK_FALSE(again->is_success());

        // and the row survived the refusal.
        CHECK(run_ok(d, "SELECT id FROM dup.t;")->size() == 1);
    }
}
