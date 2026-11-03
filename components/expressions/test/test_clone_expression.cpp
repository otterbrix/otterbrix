#include <catch2/catch_test_macros.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/clone_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

using namespace components::expressions;
using key = components::expressions::key_t;
using components::types::complex_logical_type;
using components::types::logical_type;

// A clone is the whole expression: what names it, what it computes and what validation resolved on it.

// The implicit cast an UPDATE wraps an assigned value in carries the target column as its key: the copy a backend is
// offered (detached_block_t) must still say which column it assigns.
TEST_CASE("components::expression::clone::a_cast_keeps_its_key") {
    auto resource = core::pmr::otterbrix_resource();

    key column{&resource, "qty"};
    column.set_path({std::pmr::vector<size_t>{1}, &resource});
    auto cast = make_cast_expression(&resource,
                                     param_storage{core::parameter_id_t{0}},
                                     complex_logical_type{logical_type::INTEGER},
                                     components::casts::cast_t{},
                                     components::casts::cast_kind::cast);
    cast->key() = column;

    auto target = core::pmr::otterbrix_resource();
    const auto copy = clone_expression(&target, expression_ptr{cast.get()});
    REQUIRE(copy->group() == expression_group::cast);
    CHECK(copy->key() == column);
    CHECK(copy->key().as_string() == "qty");
    CHECK(copy->key().path() == std::pmr::vector<size_t>{1});
    CHECK(static_cast<const cast_expression_t&>(*copy).kind() == components::casts::cast_kind::cast);
}

// `count(*)` and `count(DISTINCT x)` outside an aggregate node are calls: a copy (the transformer copies an operand a
// row comparison reads twice) keeps both flags.
TEST_CASE("components::expression::clone::a_call_keeps_distinct_and_star") {
    auto resource = core::pmr::otterbrix_resource();

    auto call = make_function_expression(&resource,
                                         function_qualified_name_t{core::function_name_t{"count"}},
                                         std::pmr::vector<param_storage>{&resource});
    call->set_key(key{&resource, "c"});
    call->set_star_argument(true);
    call->set_distinct(true);

    const auto copy = clone_expression(&resource, expression_ptr{call.get()});
    REQUIRE(copy->group() == expression_group::function);
    const auto& cloned = static_cast<const function_expression_t&>(*copy);
    CHECK(cloned.key().as_string() == "c");
    CHECK(cloned.has_star_argument());
    CHECK(cloned.is_distinct());
}

// What validation resolved on any expression comes along: result type, alias and cardinality.
TEST_CASE("components::expression::clone::the_resolved_fields_come_along") {
    auto resource = core::pmr::otterbrix_resource();

    auto scalar = make_scalar_expression(&resource, scalar_type::get_field, key{&resource, "x"});
    scalar->append_param(param_storage{key{&resource, "x"}});
    scalar->set_result_type(complex_logical_type{logical_type::BIGINT});
    scalar->set_result_alias("x_alias");
    scalar->set_cardinality(cardinality_t::constant);

    const auto copy = clone_expression(&resource, expression_ptr{scalar.get()});
    CHECK(copy->result_type().type() == logical_type::BIGINT);
    CHECK(copy->result_alias() == "x_alias");
    CHECK(copy->cardinality() == cardinality_t::constant);
}
