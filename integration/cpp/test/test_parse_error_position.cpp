#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace components::cursor;

TEST_CASE("integration::cpp::parse_error_position::the_cursor_carries_the_parser_position") {
    auto config = test_create_config(integration_fixture_path("test_parse_error_position/cursor"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    SECTION("execute_sql") {
        auto cursor = dispatcher->execute_sql(session, "SELECT * FROM t WHERE a = = 1;");
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::sql_parse_error);
        CHECK(std::string_view{cursor->get_error().what} == R"_(syntax error at or near "=")_");
        CHECK(cursor->get_error().position == 27);
    }

    SECTION("execute_sql_with_params") {
        const std::vector<std::pair<size_t, components::vector::vector_t>> no_params;
        auto cursor = dispatcher->execute_sql_with_params(session, "SELECT 'жж", no_params);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::sql_parse_error);
        CHECK(std::string_view{cursor->get_error().what} == R"_(unterminated quoted string at or near "'жж")_");
        CHECK(cursor->get_error().position == 8);
    }

    SECTION("an error past the parser has none, and the session goes on") {
        auto refused = dispatcher->execute_sql(session, "SELECT * FROM no_such_db.no_such_table;");
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().position == -1);

        auto after = dispatcher->execute_sql(session, "SELECT 1;");
        REQUIRE(after->is_success());
        CHECK(after->get_error().position == -1);
    }
}
