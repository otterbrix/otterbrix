#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/vector_builders.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>
#include <services/dispatcher/validation/resolve_expression.hpp>

using namespace services::dispatcher;
using components::expressions::make_scalar_expression;
using components::expressions::param_storage;
using components::expressions::scalar_expression_ptr;
using components::expressions::scalar_type;
using components::types::complex_logical_type;
using components::types::logical_type;

using expr_key_t = components::expressions::key_t;

namespace {

    std::pmr::memory_resource* resource() { return std::pmr::get_default_resource(); }

    const components::casts::cast_registry_t& casts() {
        static components::casts::cast_registry_t registry{std::pmr::new_delete_resource()};
        static const bool loaded = [] {
            components::casts::register_default_casts(registry);
            return true;
        }();
        (void) loaded;
        return registry;
    }

    std::string column_name(size_t index) { return "c" + std::to_string(index); }

    // Columns are looked up by name, which find_types reads from the type's alias.
    named_schema schema_of(std::initializer_list<logical_type> types) {
        named_schema schema(resource());
        size_t index = 0;
        for (auto type : types) {
            schema.push_back(type_from_t{"", complex_logical_type(type, column_name(index++))});
        }
        return schema;
    }

    expr_key_t column(size_t index) { return expr_key_t{resource(), column_name(index)}; }

    scalar_expression_ptr binary(scalar_type op,
                                 const components::expressions::param_storage& left,
                                 const components::expressions::param_storage& right) {
        auto expr = make_scalar_expression(resource(), op, expr_key_t{resource()});
        expr->append_param(left);
        expr->append_param(right);
        return expr;
    }

    // A conversion is recorded by splicing a cast expression OVER the operand, so what an operand
    // becomes -- not a stamp on its parent -- is what these tests read.
    const components::expressions::cast_expression_t* cast_over(const components::expressions::param_storage& param) {
        if (!std::holds_alternative<components::expressions::expression_ptr>(param)) {
            return nullptr;
        }
        const auto& nested = std::get<components::expressions::expression_ptr>(param);
        if (!nested || nested->group() != components::expressions::expression_group::cast) {
            return nullptr;
        }
        return static_cast<const components::expressions::cast_expression_t*>(nested.get());
    }

    const components::compute::function_registry_t& functions() {
        static components::compute::function_registry_t registry{std::pmr::new_delete_resource()};
        static const bool loaded = [] {
            components::compute::register_default_functions(registry);
            return true;
        }();
        (void) loaded;
        return registry;
    }

    core::error_t resolve(components::expressions::scalar_expression_t* expr,
                          const named_schema& schema,
                          const components::logical_plan::storage_parameters& parameters) {
        static const components::graph_execution_context execution_context{};
        const validation::expression_context_t context{
            resource(),
            schema,
            parameters,
            casts(),
            functions(),
            execution_context,
            components::compute::create_mask(components::compute::function_type_t::vector),
            nullptr};
        components::expressions::expression_ptr expression{expr};
        return validation::resolve_expression(expression, context);
    }

} // namespace

TEST_CASE("dispatcher::stamp_scalar_types::same_type_operands_need_no_cast") {
    auto schema = schema_of({logical_type::BIGINT, logical_type::BIGINT});
    components::logical_plan::storage_parameters parameters{resource()};

    auto expr = binary(scalar_type::add, column(0), column(1));
    REQUIRE_FALSE(resolve(expr.get(), schema, parameters).contains_error());

    REQUIRE(expr->result_type().type() == logical_type::BIGINT);
    // Nothing is converted, so neither operand is wrapped -- the tree stays as it was.
    REQUIRE(cast_over(expr->params()[0]) == nullptr);
    REQUIRE(cast_over(expr->params()[1]) == nullptr);
}

TEST_CASE("dispatcher::stamp_scalar_types::mixed_operands_stamp_the_common_target") {
    auto schema = schema_of({logical_type::INTEGER, logical_type::DOUBLE});
    components::logical_plan::storage_parameters parameters{resource()};

    auto expr = binary(scalar_type::multiply, column(0), column(1));
    REQUIRE_FALSE(resolve(expr.get(), schema, parameters).contains_error());

    // The INTEGER side has to reach DOUBLE, so a cast expression is spliced over it; the DOUBLE
    // side already fits and is left alone.
    REQUIRE(expr->result_type().type() == logical_type::DOUBLE);
    const auto* converted = cast_over(expr->params()[0]);
    REQUIRE(converted != nullptr);
    REQUIRE(converted->result_type().type() == logical_type::DOUBLE);
    REQUIRE(converted->cast());
    REQUIRE(cast_over(expr->params()[1]) == nullptr);
}

TEST_CASE("dispatcher::stamp_scalar_types::interior_nodes_are_stamped_too") {
    auto schema = schema_of({logical_type::BIGINT, logical_type::INTEGER, logical_type::DOUBLE});
    components::logical_plan::storage_parameters parameters{resource()};

    // c0 + (c1 * c2): the inner node is what a slot has to be sized from, and it is
    // reachable only through the stamp -- the outer node's type does not describe it.
    auto inner = binary(scalar_type::multiply, column(1), column(2));
    components::expressions::expression_ptr inner_param{inner.get()};
    auto outer = binary(scalar_type::add, column(0), param_storage{inner_param});
    REQUIRE_FALSE(resolve(outer.get(), schema, parameters).contains_error());

    REQUIRE(inner->result_type().type() == logical_type::DOUBLE);
    // c1 is INTEGER and has to reach DOUBLE; c2 already is DOUBLE.
    REQUIRE(cast_over(inner->params()[0]) != nullptr);
    REQUIRE(cast_over(inner->params()[1]) == nullptr);

    // The outer node is stamped independently, and its BIGINT operand is converted in turn.
    REQUIRE(outer->result_type().type() == logical_type::DOUBLE);
    REQUIRE(cast_over(outer->params()[0]) != nullptr);
}

TEST_CASE("dispatcher::stamp_scalar_types::unary_minus_has_one_operand") {
    auto schema = schema_of({logical_type::INTEGER});
    components::logical_plan::storage_parameters parameters{resource()};

    auto expr = make_scalar_expression(resource(), scalar_type::unary_minus, expr_key_t{resource()});
    expr->append_param(column(0));
    REQUIRE_FALSE(resolve(expr.get(), schema, parameters).contains_error());

    REQUIRE(expr->result_type().type() == logical_type::INTEGER);
    // The operand already is the operator's type, so it is not wrapped.
    REQUIRE(expr->params().size() == 1);
    REQUIRE(cast_over(expr->params()[0]) == nullptr);
}

TEST_CASE("dispatcher::stamp_scalar_types::scaffolding_gets_a_result_type") {
    auto schema = schema_of({logical_type::BIGINT, logical_type::BIGINT});
    components::logical_plan::storage_parameters parameters{resource()};

    // COALESCE is not an operator, so it resolves a result type but has no operand
    // types to resolve.
    auto expr = binary(scalar_type::coalesce, column(0), column(1));
    REQUIRE_FALSE(resolve(expr.get(), schema, parameters).contains_error());

    REQUIRE(expr->result_type().type() == logical_type::BIGINT);
    // Both arms already are the common type, so neither is wrapped.
    REQUIRE(cast_over(expr->params()[0]) == nullptr);
    REQUIRE(cast_over(expr->params()[1]) == nullptr);
}

TEST_CASE("dispatcher::stamp_scalar_types::an_unresolvable_operator_stamps_nothing") {
    auto schema = schema_of({logical_type::STRING_LITERAL, logical_type::BIGINT});
    components::logical_plan::storage_parameters parameters{resource()};

    auto expr = binary(scalar_type::multiply, column(0), column(1));
    REQUIRE(resolve(expr.get(), schema, parameters).contains_error());

    REQUIRE(expr->result_type().type() == logical_type::INVALID);
}
namespace {

    // A statement whose $1 is slot 0, bound to nothing yet.
    struct untyped_parameter_t {
        components::logical_plan::storage_parameters parameters{resource()};
        std::pmr::vector<components::logical_plan::external_parameter_t> external{resource()};
        core::parameter_id_t slot;

        untyped_parameter_t()
            : slot(parameters.add(components::vector::make_null(resource(), complex_logical_type{logical_type::NA}),
                                  components::vector::parameter_kind::external)) {
            external.emplace_back(resource());
            external.back().number = 1;
            external.back().slot = slot;
        }

        scalar_expression_ptr constant() const {
            auto expr = make_scalar_expression(resource(), scalar_type::constant, expr_key_t{resource()});
            expr->append_param(slot);
            return expr;
        }

        // The type the condition gives $1.
        complex_logical_type typed_by(components::expressions::expression_ptr condition) {
            static const components::graph_execution_context execution_context{};
            validation::parameter_types_t types{resource(), external, parameters, nullptr};
            const auto schema = schema_of({logical_type::BIGINT, logical_type::BIGINT});
            validation::expression_context_t context{
                resource(),
                schema,
                parameters,
                casts(),
                functions(),
                execution_context,
                components::compute::create_mask(components::compute::function_type_t::vector),
                nullptr,
                &types};
            context.required_type = complex_logical_type{logical_type::BOOLEAN};
            auto resolved = validation::resolve_expression(condition, context);
            INFO((resolved.contains_error() ? std::string{resolved.what} : std::string{"ok"}));
            REQUIRE_FALSE(resolved.contains_error());
            REQUIRE_FALSE(types.check_all_typed().contains_error());
            // The statement's own slots, as the executor holds them; the typed one is retyped.
            components::logical_plan::parameter_node_t node{resource()};
            const auto own =
                node.add_parameter(components::vector::make_null(resource(), complex_logical_type{logical_type::NA}),
                                   components::vector::parameter_kind::external);
            REQUIRE(own == slot);
            types.type_slots(node);
            return node.parameter(slot).type();
        }
    };

} // namespace

TEST_CASE("dispatcher::stamp_scalar_types::an_untyped_parameter_a_condition_reads_is_boolean") {
    using components::expressions::compare_type;
    using components::expressions::expression_ptr;
    untyped_parameter_t statement;

    SECTION("the condition alone") {
        CHECK(statement.typed_by(expression_ptr{statement.constant()}).type() == logical_type::BOOLEAN);
    }
    SECTION("an operand of AND") {
        auto both = components::expressions::make_compare_union_expression(resource(), compare_type::union_and);
        both->append_child(components::expressions::make_compare_expression(resource(),
                                                                            compare_type::gt,
                                                                            param_storage{column(0)},
                                                                            param_storage{column(1)}));
        both->append_child(statement.constant());
        CHECK(statement.typed_by(expression_ptr{both}).type() == logical_type::BOOLEAN);
    }
    SECTION("the operand of NOT") {
        auto negated = components::expressions::make_compare_union_expression(resource(), compare_type::union_not);
        negated->append_child(statement.constant());
        CHECK(statement.typed_by(expression_ptr{negated}).type() == logical_type::BOOLEAN);
    }
    SECTION("a WHEN of CASE") {
        auto choice = make_scalar_expression(resource(), scalar_type::case_expr, expr_key_t{resource()});
        choice->append_param(param_storage{expression_ptr{statement.constant()}});
        choice->append_param(param_storage{column(0)});
        choice->append_param(param_storage{column(1)});
        auto is_positive = components::expressions::make_compare_expression(resource(),
                                                                            compare_type::gt,
                                                                            param_storage{expression_ptr{choice}},
                                                                            param_storage{column(1)});
        CHECK(statement.typed_by(expression_ptr{is_positive}).type() == logical_type::BOOLEAN);
    }
}
