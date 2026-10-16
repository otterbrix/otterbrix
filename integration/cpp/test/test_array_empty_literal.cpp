#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

// An empty ARRAY[] literal has no element type; carrying it through a constant parameter and a
// LATERAL outer column must keep working.
TEST_CASE("integration::cpp::array_empty_literal::select_empty_array") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_array_empty_literal/select"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher, "SELECT ARRAY[];");
    REQUIRE_FALSE(cur->is_error());
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    const auto& column = cur->chunks().front().data[0];
    REQUIRE(column.type().type() == components::types::logical_type::ARRAY);
    REQUIRE_FALSE(column.is_null(0));
}

TEST_CASE("integration::cpp::array_empty_literal::empty_array_through_lateral") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_array_empty_literal/lateral"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur =
        test_helpers::exec(dispatcher, "SELECT * FROM (SELECT ARRAY[] AS a) t, LATERAL (SELECT t.a AS b LIMIT 1) s;");
    REQUIRE_FALSE(cur->is_error());
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    const auto& chunk = cur->chunks().front();
    REQUIRE(chunk.column_count() == 2);
    for (size_t column = 0; column < 2; ++column) {
        REQUIRE(chunk.data[column].type().type() == components::types::logical_type::ARRAY);
        REQUIRE_FALSE(chunk.data[column].is_null(0));
    }
}
