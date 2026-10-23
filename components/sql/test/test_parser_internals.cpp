#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/test/parse_ok.hpp>
#include <core/pmr.hpp>

TEST_CASE("components::sql::parser::ordered_set_aggregate_variadic_types") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    SECTION("same VARIADIC type") {
        auto* statements = parse_ok(&arena_resource, R"_(DROP AGGREGATE f(VARIADIC "any" ORDER BY VARIADIC "any"))_");
        REQUIRE(list_length(statements) == 1);
    }

    SECTION("different VARIADIC types") {
        auto parsed = raw_parser(&arena_resource, R"_(DROP AGGREGATE f(VARIADIC "any" ORDER BY VARIADIC int))_");
        REQUIRE(parsed.has_error());
        CHECK(std::string_view{parsed.error().what} ==
              "an ordered-set aggregate with a VARIADIC direct argument must have one VARIADIC "
              "aggregated argument of the same data type");
    }
}

TEST_CASE("components::sql::parser::errmsg_internal_formats_message") {
    REQUIRE(std::string{errmsg_internal("%s", "scanner jammed")} == "scanner jammed");
}

TEST_CASE("components::sql::parser::negated_long_numeric_literal") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    const std::string literal = std::string(200, '7') + ".5";
    const std::string query = "SELECT -" + literal;

    auto* statements = parse_ok(&arena_resource, query.c_str());
    REQUIRE(list_length(statements) == 1);
    auto* select = reinterpret_cast<SelectStmt*>(linitial(statements));
    auto* target = reinterpret_cast<ResTarget*>(linitial(select->targetList));
    auto* constant = reinterpret_cast<A_Const*>(target->val);
    REQUIRE(std::string{constant->val.val.str} == "-" + literal);
}
