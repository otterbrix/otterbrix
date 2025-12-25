#include <catch2/catch.hpp>
#include <components/expressions/sort_expression.hpp>

using namespace components::expressions;
using key = components::expressions::key_t;

TEST_CASE("components::expression::sort::equals") {
    auto resource = std::pmr::synchronized_pool_resource();

    auto expr1 = make_sort_expression(key(&resource, "name"), sort_order::asc);
    auto expr2 = make_sort_expression(key(&resource, "name"), sort_order::asc);
    auto expr3 = make_sort_expression(key(&resource, "count"), sort_order::asc);
    auto expr4 = make_sort_expression(key(&resource, "name"), sort_order::desc);
    REQUIRE(expression_equal()(expr1, expr2));
    REQUIRE_FALSE(expression_equal()(expr1, expr3));
    REQUIRE_FALSE(expression_equal()(expr1, expr4));
}

TEST_CASE("components::expression::sort::to_string") {
    auto resource = std::pmr::synchronized_pool_resource();

    auto expr1 = make_sort_expression(key(&resource, "count"), sort_order::asc);
    auto expr2 = make_sort_expression(key(&resource, "count"), sort_order::desc);
    REQUIRE(expr1->to_string() == R"(count: 1)");
    REQUIRE(expr2->to_string() == R"(count: -1)");
}
