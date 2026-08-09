#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <components/expressions/execution_graph_builder.hpp>

using namespace components::expressions;

using components::graph_execution_context;
using components::execution_graph::execution_graph_t;
using components::execution_graph::slot_id_t;
using components::execution_graph::slot_list_t;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::types::parameter_map_t;
using components::vector::data_chunk_t;

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

    std::pmr::vector<complex_logical_type> input_types(std::initializer_list<logical_type> types) {
        std::pmr::vector<complex_logical_type> column_types(resource());
        for (auto type : types) {
            column_types.push_back(complex_logical_type(type));
        }
        return column_types;
    }

    // A column reference already resolved to ordinal `column`, as validation leaves it.
    expr_key_t column(size_t index) {
        expr_key_t key{resource(), "c" + std::to_string(index)};
        key.set_path(std::pmr::vector<size_t>({index}, resource()));
        return key;
    }

    scalar_expression_ptr binary(scalar_type type,
                                 const param_storage& left,
                                 const param_storage& right,
                                 logical_type left_target,
                                 logical_type right_target,
                                 logical_type result,
                                 components::casts::cast_t left_cast = {},
                                 components::casts::cast_t right_cast = {}) {
        auto expr = make_scalar_expression(resource(), type, expr_key_t{resource()});
        // A conversion is a cast expression spliced OVER the operand, which is what validation
        // produces; an operand that already fits is appended untouched.
        auto operand = [](const param_storage& param, logical_type target, components::casts::cast_t cast) {
            if (!cast) {
                return param;
            }
            return param_storage{expression_ptr{make_cast_expression(resource(),
                                                                     param,
                                                                     complex_logical_type(target),
                                                                     std::move(cast),
                                                                     components::casts::cast_kind::cast)}};
        };
        expr->append_param(operand(left, left_target, std::move(left_cast)));
        expr->append_param(operand(right, right_target, std::move(right_cast)));
        expr->set_result_type(complex_logical_type(result));
        return expr;
    }

    // What validation resolves and stamps beside the operand type.
    components::casts::cast_t cast_between(logical_type source, logical_type target) {
        auto cast = casts().resolve(complex_logical_type(source),
                                    complex_logical_type(target),
                                    components::casts::cast_type::implicit);
        REQUIRE(cast.has_value());
        return *cast;
    }

    slot_list_t only(slot_id_t slot) { return slot_list_t(std::initializer_list<slot_id_t>{slot}, resource()); }

    data_chunk_t chunk(const std::pmr::vector<complex_logical_type>& types, uint64_t rows) {
        data_chunk_t input(resource(), types);
        input.set_cardinality(rows);
        return input;
    }

} // namespace

TEST_CASE("expressions::graph_builder::same-typed operands need no cast") {
    auto types = input_types({logical_type::BIGINT, logical_type::BIGINT});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    auto expr = binary(scalar_type::add,
                       column(0),
                       column(1),
                       logical_type::BIGINT,
                       logical_type::BIGINT,
                       logical_type::BIGINT);
    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());

    REQUIRE(graph.node_count() == 1);
    REQUIRE(graph.slot_count() == 3);
    REQUIRE(graph.slot_type(result.value()).type() == logical_type::BIGINT);

    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk(types, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(4));
    input.data[1].set_null(0, false);
    input.data[1].set_value(0, static_cast<int64_t>(38));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    REQUIRE(produced.value().data[0].get_value<int64_t>(0) == 42);
}

TEST_CASE("expressions::graph_builder::a stamped operand type becomes a cast node") {
    auto types = input_types({logical_type::INTEGER, logical_type::DOUBLE});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    // validation unified INTEGER with DOUBLE, so the left operand carries a cast
    auto expr = binary(scalar_type::multiply,
                       column(0),
                       column(1),
                       logical_type::DOUBLE,
                       logical_type::DOUBLE,
                       logical_type::DOUBLE,
                       cast_between(logical_type::INTEGER, logical_type::DOUBLE));
    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());

    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.slot_count() == 4);
    REQUIRE(graph.slot_type(result.value()).type() == logical_type::DOUBLE);

    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk(types, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int32_t>(3));
    input.data[1].set_null(0, false);
    input.data[1].set_value(0, 1.5);

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    REQUIRE(produced.value().data[0].get_value<double>(0) == Catch::Approx(4.5));
}

TEST_CASE("expressions::graph_builder::a column named twice binds one slot") {
    auto types = input_types({logical_type::BIGINT});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    auto expr = binary(scalar_type::add,
                       column(0),
                       column(0),
                       logical_type::BIGINT,
                       logical_type::BIGINT,
                       logical_type::BIGINT);
    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());

    // one input slot, one result slot -- the column is not bound twice
    REQUIRE(graph.slot_count() == 2);
    REQUIRE(graph.input_bindings().size() == 1);

    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk(types, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(21));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    REQUIRE(produced.value().data[0].get_value<int64_t>(0) == 42);
}

TEST_CASE("expressions::graph_builder::a nested expression becomes an interior slot") {
    auto types = input_types({logical_type::BIGINT, logical_type::BIGINT, logical_type::BIGINT});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    // c0 + (c1 * c2)
    auto inner = binary(scalar_type::multiply,
                        column(1),
                        column(2),
                        logical_type::BIGINT,
                        logical_type::BIGINT,
                        logical_type::BIGINT);
    expression_ptr inner_param{inner.get()};
    auto outer = binary(scalar_type::add,
                        column(0),
                        param_storage{inner_param},
                        logical_type::BIGINT,
                        logical_type::BIGINT,
                        logical_type::BIGINT);

    auto result = build_expression(&graph, parameters, outer.get(), types);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(graph.node_count() == 2);

    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk(types, 1);
    for (size_t index = 0; index < 3; index++) {
        input.data[index].set_null(0, false);
    }
    input.data[0].set_value(0, static_cast<int64_t>(2));
    input.data[1].set_value(0, static_cast<int64_t>(5));
    input.data[2].set_value(0, static_cast<int64_t>(8));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    REQUIRE(produced.value().data[0].get_value<int64_t>(0) == 42);
}

TEST_CASE("expressions::graph_builder::a parameter operand becomes a parameter node") {
    auto types = input_types({logical_type::BIGINT});
    parameter_map_t parameters(resource());
    parameters.emplace(core::parameter_id_t{1}, logical_value_t(resource(), int64_t{40}));
    execution_graph_t graph(resource());

    auto expr = binary(scalar_type::add,
                       column(0),
                       param_storage{core::parameter_id_t{1}},
                       logical_type::BIGINT,
                       logical_type::BIGINT,
                       logical_type::BIGINT);
    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(graph.node_count() == 2);

    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto input = chunk(types, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(2));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    REQUIRE(produced.value().data[0].get_value<int64_t>(0) == 42);
}

TEST_CASE("expressions::graph_builder::unary minus builds a one-operand node") {
    auto types = input_types({logical_type::BIGINT});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    auto expr = make_scalar_expression(resource(), scalar_type::unary_minus, expr_key_t{resource()});
    expr->append_param(column(0));
    expr->set_result_type(complex_logical_type(logical_type::BIGINT));

    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(graph.node_count() == 1);
    REQUIRE(graph.node_at(components::execution_graph::node_id_t{0}).input_indices().size() == 1);

    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk(types, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(42));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    REQUIRE(produced.value().data[0].get_value<int64_t>(0) == -42);
}

TEST_CASE("expressions::graph_builder::a reference whose stamp differs is refused") {
    auto types = input_types({logical_type::INTEGER});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    // CAST(c0 AS BIGINT): validation stamps the target but resolves no body for it yet
    auto expr = make_scalar_expression(resource(), scalar_type::get_field, expr_key_t{resource()});
    expr->append_param(column(0));
    expr->set_result_type(complex_logical_type(logical_type::BIGINT));

    REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
}

TEST_CASE("expressions::graph_builder::a plain reference claims only its column slot") {
    auto types = input_types({logical_type::BIGINT, logical_type::BIGINT});
    parameter_map_t parameters(resource());
    execution_graph_t graph(resource());

    auto expr = make_scalar_expression(resource(), scalar_type::get_field, expr_key_t{resource()});
    expr->append_param(column(1));
    expr->set_result_type(complex_logical_type(logical_type::BIGINT));

    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(graph.node_count() == 0);
    REQUIRE(graph.slot_count() == 1);
    REQUIRE(graph.input_bindings().front().column == 1);
}

TEST_CASE("expressions::graph_builder::what it cannot express is refused, not half-built") {
    auto types = input_types({logical_type::BIGINT, logical_type::BIGINT});
    parameter_map_t parameters(resource());

    SECTION("scaffolding that is neither an operator nor a blend") {
        // COALESCE and CASE build blend nodes now; what is left refused is scaffolding with no node
        // of its own, such as a star that should have been expanded long before execution.
        execution_graph_t graph(resource());
        auto expr = binary(scalar_type::star_expand,
                           column(0),
                           column(1),
                           logical_type::BIGINT,
                           logical_type::BIGINT,
                           logical_type::BIGINT);
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }

    SECTION("an operator validation never stamped") {
        execution_graph_t graph(resource());
        auto expr = make_scalar_expression(resource(), scalar_type::add, expr_key_t{resource()});
        expr->append_param(column(0));
        expr->append_param(column(1));
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }

    SECTION("a nested field path") {
        execution_graph_t graph(resource());
        expr_key_t nested{resource(), "outer"};
        nested.set_path(std::pmr::vector<size_t>({0, 1}, resource()));
        auto expr = binary(scalar_type::add,
                           nested,
                           column(1),
                           logical_type::BIGINT,
                           logical_type::BIGINT,
                           logical_type::BIGINT);
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }

    SECTION("a column ordinal the input does not have") {
        execution_graph_t graph(resource());
        auto expr = binary(scalar_type::add,
                           column(7),
                           column(1),
                           logical_type::BIGINT,
                           logical_type::BIGINT,
                           logical_type::BIGINT);
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }

    SECTION("an unstamped operator") {
        // A conversion is a spliced cast expression, so "an operand that has to move with no cast
        // beside it" is no longer expressible -- the only thing left to detect is a node validation
        // never stamped at all.
        execution_graph_t graph(resource());
        auto expr = make_scalar_expression(resource(), scalar_type::add, expr_key_t{resource()});
        expr->append_param(column(0));
        expr->append_param(column(1));
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }
}