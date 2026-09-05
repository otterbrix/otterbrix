#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/planner/view_expansion.hpp>
#include <core/pmr.hpp>

#include <string>

// THE SEAM. parser.h's contract is explicit: raw_parser's returned list may be
// EMPTY — that is success, "the grammar accepted the text and found no statement
// in it" — and callers must test list_length() before reaching for linitial().
// Not one caller did. Both execute_sql overloads and the view-body re-parse
// applied linitial() to the raw result, which on an empty list reads past the
// end of a std::pmr::list; whatever that read happens to produce is what the
// user got. And when the list held MORE than one statement, linitial() took the
// first and dropped the rest on the floor: `INSERT ...; INSERT ...;` executed
// half of what was written and reported success — the exact silent narrowing
// this seam exists to refuse.

using namespace components;

namespace {
    std::string error_text(const cursor::cursor_t_ptr& cursor) {
        return std::string{cursor->get_error().what.c_str()};
    }
} // namespace

TEST_CASE("integration::cpp::statement_shape::no_statement_is_a_named_refusal") {
    auto config = test_create_config("/tmp/test_statement_shape_w3/no_statement");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Every one of these parses successfully into no statement at all
    // (parser.h: empty input, a lone comment, a bare `;`).
    for (const char* text : {";", "", "   ", "-- only a comment", "/* only a comment */"}) {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session, text);
        INFO("query: '" << text << "'");
        REQUIRE_FALSE(cursor->is_success());
        // BEFORE: "unknown parser error" — the parser did not err; there was
        // nothing to execute, and the refusal must say so.
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
    auto config = test_create_config("/tmp/test_statement_shape_w3/multi");
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
        // A stored view body should be one SELECT; a bare `;` re-parses into no
        // statement at all, which used to walk linitial() off the end of the list.
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
