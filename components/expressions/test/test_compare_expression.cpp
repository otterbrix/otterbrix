#include <actor-zeta.hpp>
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>

using namespace components::expressions;
using key = components::expressions::key_t;

TEST_CASE("expression::compare::equals") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto expr1 =
        make_compare_expression(&resource, compare_type::eq, side_t::left, key("name"), core::parameter_id_t(1));
    auto expr2 =
        make_compare_expression(&resource, compare_type::eq, side_t::left, key("name"), core::parameter_id_t(1));
    auto expr3 =
        make_compare_expression(&resource, compare_type::ne, side_t::left, key("name"), core::parameter_id_t(1));
    auto expr4 =
        make_compare_expression(&resource, compare_type::eq, side_t::left, key("count"), core::parameter_id_t(1));
    auto expr5 =
        make_compare_expression(&resource, compare_type::eq, side_t::left, key("name"), core::parameter_id_t(2));
    auto expr_union1 = make_compare_union_expression(&resource, compare_type::union_and);
    expr_union1->append_child(expr1);
    expr_union1->append_child(expr3);
    auto expr_union2 = make_compare_union_expression(&resource, compare_type::union_and);
    expr_union2->append_child(expr1);
    expr_union2->append_child(expr3);
    auto expr_union3 = make_compare_union_expression(&resource, compare_type::union_and);
    expr_union3->append_child(expr1);
    expr_union3->append_child(expr4);
    auto expr_union4 = make_compare_union_expression(&resource, compare_type::union_or);
    expr_union4->append_child(expr1);
    expr_union4->append_child(expr3);
    REQUIRE(expression_equal()(expr1, expr2));
    REQUIRE_FALSE(expression_equal()(expr1, expr3));
    REQUIRE_FALSE(expression_equal()(expr1, expr4));
    REQUIRE_FALSE(expression_equal()(expr1, expr5));
    REQUIRE(expression_equal()(expr_union1, expr_union2));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union3));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union4));
}

TEST_CASE("expression::compare::to_string") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto expr =
        make_compare_expression(&resource, compare_type::eq, side_t::left, key("count"), core::parameter_id_t(1));
    REQUIRE(expr->to_string() == R"("count": {$eq: #1})");

    expr = make_compare_union_expression(&resource, compare_type::union_and);
    expr->append_child(
        make_compare_expression(&resource, compare_type::eq, side_t::left, key("key1"), core::parameter_id_t(1)));
    expr->append_child(
        make_compare_expression(&resource, compare_type::lt, side_t::left, key("key2"), core::parameter_id_t(2)));
    REQUIRE(expr->to_string() == R"($and: ["key1": {$eq: #1}, "key2": {$lt: #2}])");
}
