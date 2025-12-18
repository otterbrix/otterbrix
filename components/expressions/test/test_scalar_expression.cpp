#include <actor-zeta.hpp>
#include <catch2/catch.hpp>
#include <components/expressions/scalar_expression.hpp>

using namespace components::expressions;
using key = components::expressions::key_t;

TEST_CASE("expression::scalar::equals") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto expr1 = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "name"));
    auto expr2 = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "name"));
    auto expr3 = make_scalar_expression(&resource, scalar_type::abs, key(&resource, "name"));
    auto expr4 = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "count"));
    auto expr_union1 = make_scalar_expression(&resource, scalar_type::multiply);
    expr_union1->append_param(core::parameter_id_t(0));
    expr_union1->append_param(key(&resource, "name"));
    expr_union1->append_param(expr1);
    auto expr_union2 = make_scalar_expression(&resource, scalar_type::multiply);
    expr_union2->append_param(core::parameter_id_t(0));
    expr_union2->append_param(key(&resource, "name"));
    expr_union2->append_param(expr1);
    auto expr_union3 = make_scalar_expression(&resource, scalar_type::multiply);
    expr_union3->append_param(core::parameter_id_t(1));
    expr_union3->append_param(key(&resource, "name"));
    expr_union3->append_param(expr1);
    auto expr_union4 = make_scalar_expression(&resource, scalar_type::multiply);
    expr_union4->append_param(core::parameter_id_t(0));
    expr_union4->append_param(key(&resource, "count"));
    expr_union4->append_param(expr1);
    auto expr_union5 = make_scalar_expression(&resource, scalar_type::multiply);
    expr_union5->append_param(core::parameter_id_t(0));
    expr_union5->append_param(key(&resource, "name"));
    expr_union5->append_param(expr3);
    REQUIRE(expression_equal()(expr1, expr2));
    REQUIRE_FALSE(expression_equal()(expr1, expr3));
    REQUIRE_FALSE(expression_equal()(expr1, expr4));
    REQUIRE(expression_equal()(expr_union1, expr_union2));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union3));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union4));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union5));
}

TEST_CASE("expression::scalar::to_string") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto expr =
        make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "count"), key(&resource, "count"));
    REQUIRE(expr->to_string() == R"(count: "$count")");
    expr = make_scalar_expression(&resource, scalar_type::floor, key(&resource, "count"), key(&resource, "count"));
    REQUIRE(expr->to_string() == R"(count: {$floor: "$count"})");

    expr = make_scalar_expression(&resource, scalar_type::multiply, key(&resource, "multi"));
    expr->append_param(core::parameter_id_t(1));
    expr->append_param(key(&resource, "key"));
    expr->append_param(
        make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "value"), key(&resource, "count")));
    REQUIRE(expr->to_string() == R"(multi: {$multiply: [#1, "$key", value: "$count"]})");
}
