#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <components/expressions/execution_dag_builder.hpp>
#include <components/vector/vector_builders.hpp>

using namespace components::expressions;

using components::graph_execution_context;
using components::execution_dag::execution_dag_t;
using components::execution_dag::slot_id_t;
using components::execution_dag::slot_list_t;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::data_chunk_t;
using components::vector::parameter_kind;
using components::vector::storage_parameters;

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
        key.set_path(std::pmr::vector<size_t>(std::initializer_list<size_t>{index}, resource()));
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
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

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
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

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
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

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
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

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
    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_scalar(resource(), int64_t{40}), parameter_kind::literal) ==
            core::parameter_id_t{0});
    execution_dag_t graph(resource());

    auto expr = binary(scalar_type::add,
                       column(0),
                       param_storage{core::parameter_id_t{0}},
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
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

    auto expr = make_scalar_expression(resource(), scalar_type::unary_minus, expr_key_t{resource()});
    expr->append_param(column(0));
    expr->set_result_type(complex_logical_type(logical_type::BIGINT));

    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(graph.node_count() == 1);
    REQUIRE(graph.node_at(components::execution_dag::node_id_t{0}).input_indices().size() == 1);

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
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

    // CAST(c0 AS BIGINT): validation stamps the target but resolves no body for it yet
    auto expr = make_scalar_expression(resource(), scalar_type::get_field, expr_key_t{resource()});
    expr->append_param(column(0));
    expr->set_result_type(complex_logical_type(logical_type::BIGINT));

    REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
}

TEST_CASE("expressions::graph_builder::a plain reference claims only its column slot") {
    auto types = input_types({logical_type::BIGINT, logical_type::BIGINT});
    storage_parameters parameters(resource());
    execution_dag_t graph(resource());

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
    storage_parameters parameters(resource());

    SECTION("scaffolding that is neither an operator nor a blend") {
        // COALESCE and CASE build blend nodes now; what is left refused is scaffolding with no node
        // of its own, such as a star that should have been expanded long before execution.
        execution_dag_t graph(resource());
        auto expr = binary(scalar_type::star_expand,
                           column(0),
                           column(1),
                           logical_type::BIGINT,
                           logical_type::BIGINT,
                           logical_type::BIGINT);
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }

    SECTION("an operator validation never stamped") {
        execution_dag_t graph(resource());
        auto expr = make_scalar_expression(resource(), scalar_type::add, expr_key_t{resource()});
        expr->append_param(column(0));
        expr->append_param(column(1));
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }

    SECTION("a nested field path") {
        execution_dag_t graph(resource());
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
        execution_dag_t graph(resource());
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
        execution_dag_t graph(resource());
        auto expr = make_scalar_expression(resource(), scalar_type::add, expr_key_t{resource()});
        expr->append_param(column(0));
        expr->append_param(column(1));
        REQUIRE(build_expression(&graph, parameters, expr.get(), types).has_error());
    }
}
// x = ANY (set): the set is ONE bound parameter, so the graph that tests it must not grow with the number of
// elements -- the builder emits a field node and a comparison node per element.
TEST_CASE("expressions::graph_builder::ANY over a bound set builds a graph independent of the set size") {
    auto types = input_types({logical_type::BIGINT});

    auto nodes_for = [&](int64_t elements) {
        std::vector<components::vector::vector_t> values;
        values.reserve(static_cast<size_t>(elements));
        for (int64_t i = 0; i < elements; i++) {
            values.push_back(components::vector::make_scalar(resource(), i));
        }
        storage_parameters parameters(resource());
        REQUIRE(parameters.add(
                    components::vector::make_array(resource(), complex_logical_type(logical_type::BIGINT), values),
                    parameter_kind::literal) == core::parameter_id_t{0});
        execution_dag_t graph(resource());
        auto expr = make_compare_expression(resource(),
                                            compare_type::any,
                                            param_storage{column(0)},
                                            param_storage{core::parameter_id_t{0}});
        expr->set_inner_op(compare_type::eq);
        auto result = build_expression(&graph, parameters, expr.get(), types);
        REQUIRE_FALSE(result.has_error());
        return graph.node_count();
    };

    const auto small = nodes_for(10);
    const auto large = nodes_for(1000);
    INFO("nodes for 10 elements: " << small << ", for 1000 elements: " << large);
    CHECK(large == small);
}

// Probe < ANY / >= ALL ... : every operator and both quantifiers are one membership node, whatever the set size
TEST_CASE("expressions::graph_builder::every ANY and ALL operator builds one membership node") {
    auto types = input_types({logical_type::BIGINT});
    std::vector<components::vector::vector_t> values;
    for (int64_t i = 0; i < 50; i++) {
        values.push_back(components::vector::make_scalar(resource(), i));
    }
    for (auto quantifier : {compare_type::any, compare_type::all}) {
        for (auto inner : {compare_type::eq,
                           compare_type::ne,
                           compare_type::lt,
                           compare_type::lte,
                           compare_type::gt,
                           compare_type::gte}) {
            storage_parameters parameters(resource());
            REQUIRE(parameters.add(
                        components::vector::make_array(resource(), complex_logical_type(logical_type::BIGINT), values),
                        parameter_kind::literal) == core::parameter_id_t{0});
            execution_dag_t graph(resource());
            auto expr = make_compare_expression(resource(),
                                                quantifier,
                                                param_storage{column(0)},
                                                param_storage{core::parameter_id_t{0}});
            expr->set_inner_op(inner);
            auto result = build_expression(&graph, parameters, expr.get(), types);
            REQUIRE_FALSE(result.has_error());
            INFO(to_string(quantifier) << " " << to_string(inner));
            // the set parameter and the membership
            CHECK(graph.node_count() == 2);
            CHECK(graph.slot_type(result.value()).type() == logical_type::BOOLEAN);
        }
    }
}

// The set is cast by the node that tests it, once: the comparison only names the target
TEST_CASE("expressions::graph_builder::a set cast recorded on the comparison reaches the membership node") {
    auto types = input_types({logical_type::BIGINT});
    const complex_logical_type integer{logical_type::INTEGER};
    std::vector<components::vector::vector_t> values;
    values.push_back(components::vector::make_scalar(resource(), int32_t{10}));
    values.push_back(components::vector::make_scalar(resource(), int32_t{30}));
    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_array(resource(), integer, values), parameter_kind::literal) ==
            core::parameter_id_t{0});
    execution_dag_t graph(resource());
    auto expr = make_compare_expression(resource(),
                                        compare_type::any,
                                        param_storage{column(0)},
                                        param_storage{core::parameter_id_t{0}});
    expr->set_inner_op(compare_type::eq);
    expr->set_set_element_cast(complex_logical_type(logical_type::BIGINT),
                               cast_between(logical_type::INTEGER, logical_type::BIGINT));
    auto result = build_expression(&graph, parameters, expr.get(), types);
    REQUIRE_FALSE(result.has_error());
    graph.set_output(only(result.value()));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto input = chunk(types, 3);
    const int64_t probes[] = {30, 31, 10};
    for (uint64_t row = 0; row < 3; row++) {
        input.data[0].set_null(row, false);
        input.data[0].set_value(row, probes[row]);
    }
    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto produced = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(produced.has_error());
    CHECK(produced.value().data[0].get_value<bool>(0));
    CHECK_FALSE(produced.value().data[0].get_value<bool>(1));
    CHECK(produced.value().data[0].get_value<bool>(2));
}

TEST_CASE("expressions::graph_builder::the set cast is part of what makes two comparisons the same") {
    auto make = [](logical_type target) {
        auto expr = make_compare_expression(resource(),
                                            compare_type::any,
                                            param_storage{column(0)},
                                            param_storage{core::parameter_id_t{0}});
        expr->set_inner_op(compare_type::eq);
        if (target != logical_type::INVALID) {
            expr->set_set_element_cast(complex_logical_type(target),
                                       cast_between(logical_type::INTEGER, logical_type::BIGINT));
        }
        return expr;
    };
    CHECK(*make(logical_type::BIGINT) == *make(logical_type::BIGINT));
    CHECK_FALSE(*make(logical_type::BIGINT) == *make(logical_type::INVALID));
    CHECK(make(logical_type::BIGINT)->hash() != make(logical_type::INVALID)->hash());
}
