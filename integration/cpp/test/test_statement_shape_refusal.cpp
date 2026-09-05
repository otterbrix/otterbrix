#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/planner/view_expansion.hpp>
#include <core/pmr.hpp>

#include <string>

// parser.h's contract: raw_parser's list may be EMPTY (grammar accepted the text, found no
// statement) or hold MULTIPLE statements. No caller checked list_length() before linitial():
// on an empty list linitial() read past the end of the pmr::list, and on a multi-statement
// query it silently ran only the first and reported success either way.

using namespace components;

namespace {
    std::string error_text(const cursor::cursor_t_ptr& cursor) {
        return std::string{cursor->get_error().what.c_str()};
    }
} // namespace

TEST_CASE("integration::cpp::statement_shape::no_statement_is_a_named_refusal") {
    auto config = test_create_config(integration_fixture_path("test_statement_shape_w3/no_statement"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Each of these parses into no statement at all (empty, comment-only, bare `;`).
    for (const char* text : {";", "", "   ", "-- only a comment", "/* only a comment */"}) {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session, text);
        INFO("query: '" << text << "'");
        REQUIRE_FALSE(cursor->is_success());
        CHECK(error_text(cursor).find("no statement") != std::string::npos);
    }

    // The parameterized overload walks the same seam.
    {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql_with_params(session, ";", {});
        REQUIRE_FALSE(cursor->is_success());
        CHECK(error_text(cursor).find("no statement") != std::string::npos);
    }
}

TEST_CASE("integration::cpp::statement_shape::multi_statement_is_refused_whole") {
    auto config = test_create_config(integration_fixture_path("test_statement_shape_w3/multi"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE shapedb;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE shapedb.t (id INT);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session,
                                              "INSERT INTO shapedb.t (id) VALUES (1); "
                                              "INSERT INTO shapedb.t (id) VALUES (2);");
        // BEFORE: success — the FIRST insert ran, the second was silently dropped.
        REQUIRE_FALSE(cursor->is_success());
        CHECK(error_text(cursor).find("2 statements") != std::string::npos);
    }

    // The refusal happened before execution: NEITHER statement ran.
    {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session, "SELECT id FROM shapedb.t;");
        REQUIRE(cursor->is_success());
        CHECK(cursor->size() == 0);
    }

    // Same seam, parameterized overload.
    {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql_with_params(session,
                                                          "INSERT INTO shapedb.t (id) VALUES (1); "
                                                          "INSERT INTO shapedb.t (id) VALUES (2);",
                                                          {});
        REQUIRE_FALSE(cursor->is_success());
        CHECK(error_text(cursor).find("2 statements") != std::string::npos);
    }
}

// The third violator of the same contract, reached without an engine: the
// view-body re-parse in the planner.
TEST_CASE("integration::cpp::statement_shape::view_body_reparse_checks_statement_count") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("a body with no statement in it is refused by name") {
        // A bare `;` re-parses into no statement — same linitial() off-end bug as above.
        auto body = planner::expand_view_body(&resource, ";");
        REQUIRE(body.error.type != core::error_code_t::none);
        CHECK(std::string{body.error.what}.find("no statement") != std::string::npos);
    }

    SECTION("a body with two statements is refused, not silently halved") {
        // BEFORE: the second statement was dropped and the first came back as
        // the whole body — success.
        auto body = planner::expand_view_body(&resource, "SELECT id FROM vdb.vt; SELECT id FROM vdb.vt");
        REQUIRE(body.error.type != core::error_code_t::none);
        CHECK(std::string{body.error.what}.find("2 statements") != std::string::npos);
    }
}
