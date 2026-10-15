#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include <components/casts/default_casts.hpp>
#include <components/execution_dag/execution_dag.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/counting_resource.hpp>

using namespace components::execution_dag;

using components::graph_execution_context;
using components::casts::cast_kind;
using components::operators::operator_code;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::parameter_kind;
using components::vector::storage_parameters;
using components::vector::data_chunk_t;

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

    data_chunk_t chunk(std::initializer_list<logical_type> types, uint64_t rows) {
        std::pmr::vector<complex_logical_type> column_types(resource());
        for (auto type : types) {
            column_types.push_back(complex_logical_type(type));
        }
        data_chunk_t input(resource(), column_types);
        input.set_cardinality(rows);
        return input;
    }

    slot_list_t slots(std::initializer_list<slot_id_t> list) { return slot_list_t(list, resource()); }

    // Declares a slot fed by column `column` of the input chunk.
    slot_id_t input_slot(execution_dag_t& graph, size_t column, logical_type type) {
        auto slot = graph.declare_slot();
        graph.bind_input(slot, column, type);
        return slot;
    }

} // namespace

TEST_CASE("execution_dag::an operator over two input columns") {
    execution_dag_t graph(resource());
    auto left = input_slot(graph, 0, logical_type::BIGINT);
    auto right = input_slot(graph, 1, logical_type::BIGINT);

    auto node = graph.add_operator(operator_code::add, left, right);
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));

    REQUIRE(graph.node_count() == 1);
    REQUIRE(graph.slot_count() == 3);
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk({logical_type::BIGINT, logical_type::BIGINT}, 2);
    for (uint64_t row = 0; row < 2; row++) {
        input.data[0].set_null(row, false);
        input.data[0].set_value(row, static_cast<int64_t>(row + 1));
        input.data[1].set_null(row, false);
        input.data[1].set_value(row, static_cast<int64_t>(10));
    }

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().size() == 2);
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 11);
    REQUIRE(result.value().data[0].get_value<int64_t>(1) == 12);
}

TEST_CASE("execution_dag::a node can be built before the nodes feeding it") {
    execution_dag_t graph(resource());
    auto column_a = input_slot(graph, 0, logical_type::BIGINT);
    auto column_b = input_slot(graph, 1, logical_type::BIGINT);
    auto column_c = input_slot(graph, 2, logical_type::BIGINT);

    // (a + b) * c, built root first: the product exists before its left operand does.
    auto product = graph.add_operator(operator_code::multiply, invalid_slot, column_c);
    auto sum = graph.add_operator(operator_code::add, column_a, column_b);
    graph.connect(product, 0, graph.output_slot(sum));

    graph.set_slot_type(graph.output_slot(sum), logical_type::BIGINT);
    graph.set_slot_type(graph.output_slot(product), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(product)}));

    REQUIRE_FALSE(graph.prepare().contains_error());
    // insertion order is product, sum -- execution order is the other way round
    REQUIRE(graph.order().size() == 2);
    REQUIRE(graph.order()[0] == sum);
    REQUIRE(graph.order()[1] == product);

    auto input = chunk({logical_type::BIGINT, logical_type::BIGINT, logical_type::BIGINT}, 1);
    for (size_t column = 0; column < 3; column++) {
        input.data[column].set_null(0, false);
    }
    input.data[0].set_value(0, static_cast<int64_t>(2));
    input.data[1].set_value(0, static_cast<int64_t>(3));
    input.data[2].set_value(0, static_cast<int64_t>(4));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 20);
}

TEST_CASE("execution_dag::a cast is inserted on the edge that needs it") {
    execution_dag_t graph(resource());
    auto left = input_slot(graph, 0, logical_type::INTEGER);
    auto right = input_slot(graph, 1, logical_type::DOUBLE);

    auto node = graph.add_operator(operator_code::multiply, left, right);
    graph.set_slot_type(graph.output_slot(node), logical_type::DOUBLE);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.validate().contains_error());
    REQUIRE(graph.order().size() == 1);

    auto cast = casts().resolve(complex_logical_type(logical_type::INTEGER),
                                complex_logical_type(logical_type::DOUBLE),
                                components::casts::cast_type::implicit);
    REQUIRE(cast.has_value());
    auto inserted = graph.insert_cast_before(node, 0, *cast, cast_kind::cast, logical_type::DOUBLE);

    // the appended slot replaced the operator's left input, and existing slot ids did not move
    REQUIRE(graph.node_count() == 2);
    REQUIRE(graph.slot_count() == 4);
    REQUIRE(graph.node_at(node).input_indices()[0] == graph.output_slot(inserted));
    REQUIRE(graph.node_at(node).input_indices()[1] == right);
    // and it runs before its consumer without a reordering pass
    REQUIRE(graph.order().size() == 2);
    REQUIRE(graph.order()[0] == inserted);
    REQUIRE(graph.order()[1] == node);

    REQUIRE_FALSE(graph.prepare().contains_error());
    auto input = chunk({logical_type::INTEGER, logical_type::DOUBLE}, 2);
    for (uint64_t row = 0; row < 2; row++) {
        input.data[0].set_null(row, false);
        input.data[0].set_value(row, static_cast<int32_t>(row + 2));
        input.data[1].set_null(row, false);
        input.data[1].set_value(row, 1.5);
    }

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].get_value<double>(0) == Catch::Approx(3.0));
    REQUIRE(result.value().data[0].get_value<double>(1) == Catch::Approx(4.5));
}

TEST_CASE("execution_dag::output order is independent of execution order") {
    execution_dag_t graph(resource());
    auto column_a = input_slot(graph, 0, logical_type::BIGINT);
    auto column_b = input_slot(graph, 1, logical_type::BIGINT);

    auto sum = graph.add_operator(operator_code::add, column_a, column_b);
    graph.set_slot_type(graph.output_slot(sum), logical_type::BIGINT);
    // computed column, then a pass-through input column
    graph.set_output(slots({graph.output_slot(sum), column_a}));

    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk({logical_type::BIGINT, logical_type::BIGINT}, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(7));
    input.data[1].set_null(0, false);
    input.data[1].set_value(0, static_cast<int64_t>(5));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().column_count() == 2);
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 12);
    REQUIRE(result.value().data[1].get_value<int64_t>(0) == 7);
}

// SELECT y, x + 1 over a table stored as (x, y, z): the chunk order, the slot order and the
// output order are all different, and z is never referenced.
TEST_CASE("execution_dag::slots follow the stored column order, not the query's") {
    execution_dag_t graph(resource());
    auto column_y = input_slot(graph, 1, logical_type::BIGINT);
    auto column_x = input_slot(graph, 0, logical_type::BIGINT);

    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_scalar(resource(), int64_t{1}), parameter_kind::literal) ==
            core::parameter_id_t{0});

    auto one = graph.add_parameter(core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(one), logical_type::BIGINT);
    auto sum = graph.add_operator(operator_code::add, column_x, graph.output_slot(one));
    graph.set_slot_type(graph.output_slot(sum), logical_type::BIGINT);
    graph.set_output(slots({column_y, graph.output_slot(sum)}));

    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    // z has no slot at all
    auto bindings = graph.input_bindings();
    REQUIRE(bindings.size() == 2);
    REQUIRE(bindings[0].slot == column_y);
    REQUIRE(bindings[0].column == 1);
    REQUIRE(bindings[1].slot == column_x);
    REQUIRE(bindings[1].column == 0);

    auto input = chunk({logical_type::BIGINT, logical_type::BIGINT, logical_type::BIGINT}, 1);
    for (size_t column = 0; column < 3; column++) {
        input.data[column].set_null(0, false);
    }
    input.data[0].set_value(0, static_cast<int64_t>(40)); // x
    input.data[1].set_value(0, static_cast<int64_t>(7));  // y
    input.data[2].set_value(0, static_cast<int64_t>(99)); // z, untouched

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().column_count() == 2);
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 7);
    REQUIRE(result.value().data[1].get_value<int64_t>(0) == 41);
}

TEST_CASE("execution_dag::a chunk laid out differently than the bound slots is refused") {
    execution_dag_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);
    auto node = graph.add_operator(operator_code::negate, column);
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    SECTION("the column is not there at all") {
        auto input = chunk({}, 1);
        REQUIRE(graph.process(input, graph_execution_context{}).contains_error());
    }

    SECTION("the column holds another type") {
        auto input = chunk({logical_type::DOUBLE}, 1);
        REQUIRE(graph.process(input, graph_execution_context{}).contains_error());
    }
}

TEST_CASE("execution_dag::a parameter becomes a constant operand") {
    execution_dag_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);

    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_scalar(resource(), int64_t{100}), parameter_kind::literal) ==
            core::parameter_id_t{0});

    auto placed = graph.add_parameter(core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), logical_type::BIGINT);
    auto sum = graph.add_operator(operator_code::add, column, graph.output_slot(placed));
    graph.set_slot_type(graph.output_slot(sum), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(sum)}));

    REQUIRE_FALSE(graph.prepare().contains_error());

    SECTION("without a bound map it refuses to run") {
        auto input = chunk({logical_type::BIGINT}, 1);
        input.data[0].set_null(0, false);
        input.data[0].set_value(0, static_cast<int64_t>(1));
        REQUIRE(graph.process(input, graph_execution_context{}).contains_error());
    }

    SECTION("bound, it feeds every row") {
        graph.set_parameters(&parameters);
        auto input = chunk({logical_type::BIGINT}, 2);
        for (uint64_t row = 0; row < 2; row++) {
            input.data[0].set_null(row, false);
            input.data[0].set_value(row, static_cast<int64_t>(row + 1));
        }
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, input.size());
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value().data[0].get_value<int64_t>(0) == 101);
        REQUIRE(result.value().data[0].get_value<int64_t>(1) == 102);
    }
}

TEST_CASE("execution_dag::a parameter slot references the parameter's buffer instead of copying it") {
    execution_dag_t graph(resource());

    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_scalar(resource(), int64_t{100}), parameter_kind::literal) ==
            core::parameter_id_t{0});

    auto placed = graph.add_parameter(core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(placed)}));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto input = chunk({logical_type::BIGINT}, 1);
    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    CHECK(result.value().data[0].data() == parameters.at(core::parameter_id_t{0}).data());
}

TEST_CASE("execution_dag::a parameter of another type than its slot is refused, not read through the wrong layout") {
    execution_dag_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);

    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_scalar(resource(), int32_t{100}), parameter_kind::literal) ==
            core::parameter_id_t{0});

    auto placed = graph.add_parameter(core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), logical_type::BIGINT);
    auto sum = graph.add_operator(operator_code::add, column, graph.output_slot(placed));
    graph.set_slot_type(graph.output_slot(sum), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(sum)}));

    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto input = chunk({logical_type::BIGINT}, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(1));
    auto error = graph.process(input, graph_execution_context{});
    REQUIRE(error.contains_error());
    CHECK(error.type == core::error_code_t::invalid_parameter);
}

TEST_CASE("execution_dag::a unary operator takes one operand") {
    execution_dag_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);

    auto node = graph.add_operator(operator_code::negate, column);
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));

    REQUIRE_FALSE(graph.prepare().contains_error());
    REQUIRE(graph.node_at(node).input_indices().size() == 1);

    auto input = chunk({logical_type::BIGINT}, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(7));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == -7);
}

TEST_CASE("execution_dag::a null operand yields a null result") {
    execution_dag_t graph(resource());
    auto left = input_slot(graph, 0, logical_type::BIGINT);
    auto right = input_slot(graph, 1, logical_type::BIGINT);

    auto node = graph.add_operator(operator_code::add, left, right);
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk({logical_type::BIGINT, logical_type::BIGINT}, 2);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(1));
    input.data[1].set_null(0, true);
    input.data[0].set_null(1, false);
    input.data[0].set_value(1, static_cast<int64_t>(2));
    input.data[1].set_null(1, false);
    input.data[1].set_value(1, static_cast<int64_t>(3));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].is_null(0));
    REQUIRE_FALSE(result.value().data[0].is_null(1));
    REQUIRE(result.value().data[0].get_value<int64_t>(1) == 5);
}

TEST_CASE("execution_dag::malformed graphs are refused") {
    SECTION("an input that was never connected") {
        execution_dag_t graph(resource());
        auto column = input_slot(graph, 0, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, invalid_slot, column);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("a slot nothing writes") {
        execution_dag_t graph(resource());
        auto column = input_slot(graph, 0, logical_type::BIGINT);
        auto orphan = graph.declare_slot();
        graph.set_slot_type(orphan, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, column, orphan);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("a slot with no type") {
        execution_dag_t graph(resource());
        auto left = input_slot(graph, 0, logical_type::BIGINT);
        auto right = input_slot(graph, 1, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, left, right);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("a cycle") {
        execution_dag_t graph(resource());
        auto column = input_slot(graph, 0, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, column, invalid_slot);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.connect(node, 1, graph.output_slot(node));
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("no output selected") {
        execution_dag_t graph(resource());
        auto left = input_slot(graph, 0, logical_type::BIGINT);
        auto right = input_slot(graph, 1, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, left, right);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("processing before prepare") {
        execution_dag_t graph(resource());
        auto left = input_slot(graph, 0, logical_type::BIGINT);
        auto right = input_slot(graph, 1, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, left, right);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        auto input = chunk({logical_type::BIGINT, logical_type::BIGINT}, 1);
        REQUIRE(graph.process(input, graph_execution_context{}).contains_error());
    }
}
TEST_CASE("execution_dag::coalesce takes the first operand that is not null") {
    execution_dag_t graph(resource());
    auto first = input_slot(graph, 0, logical_type::BIGINT);
    auto second = input_slot(graph, 1, logical_type::BIGINT);

    auto node = graph.add_blend(blend_node_t::blend_kind::coalesce, slots({first, second}));
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    // row 0: both present -> first. row 1: first null -> second. row 2: both null -> null.
    auto input = chunk({logical_type::BIGINT, logical_type::BIGINT}, 3);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, static_cast<int64_t>(7));
    input.data[1].set_null(0, false);
    input.data[1].set_value(0, static_cast<int64_t>(9));
    input.data[0].set_null(1, true);
    input.data[1].set_null(1, false);
    input.data[1].set_value(1, static_cast<int64_t>(4));
    input.data[0].set_null(2, true);
    input.data[1].set_null(2, true);

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 7);
    REQUIRE(result.value().data[0].get_value<int64_t>(1) == 4);
    REQUIRE(result.value().data[0].is_null(2));
}

TEST_CASE("execution_dag::case takes the arm of the first true condition") {
    execution_dag_t graph(resource());
    auto condition = input_slot(graph, 0, logical_type::BOOLEAN);
    auto when_true = input_slot(graph, 1, logical_type::BIGINT);
    auto otherwise = input_slot(graph, 2, logical_type::BIGINT);

    // CASE WHEN c THEN a ELSE b END -- the trailing odd input is the ELSE.
    auto node = graph.add_blend(blend_node_t::blend_kind::case_when, slots({condition, when_true, otherwise}));
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    // row 0: TRUE -> arm. row 1: FALSE -> else. row 2: UNKNOWN is not TRUE -> else.
    auto input = chunk({logical_type::BOOLEAN, logical_type::BIGINT, logical_type::BIGINT}, 3);
    for (uint64_t row = 0; row < 3; row++) {
        input.data[1].set_null(row, false);
        input.data[1].set_value(row, static_cast<int64_t>(100));
        input.data[2].set_null(row, false);
        input.data[2].set_value(row, static_cast<int64_t>(200));
    }
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, true);
    input.data[0].set_null(1, false);
    input.data[0].set_value(1, false);
    input.data[0].set_null(2, true);

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 100);
    REQUIRE(result.value().data[0].get_value<int64_t>(1) == 200);
    REQUIRE(result.value().data[0].get_value<int64_t>(2) == 200);
}

TEST_CASE("execution_dag::case with no else and no match is null") {
    execution_dag_t graph(resource());
    auto condition = input_slot(graph, 0, logical_type::BOOLEAN);
    auto when_true = input_slot(graph, 1, logical_type::BIGINT);

    auto node = graph.add_blend(blend_node_t::blend_kind::case_when, slots({condition, when_true}));
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto input = chunk({logical_type::BOOLEAN, logical_type::BIGINT}, 2);
    for (uint64_t row = 0; row < 2; row++) {
        input.data[1].set_null(row, false);
        input.data[1].set_value(row, static_cast<int64_t>(5));
        input.data[0].set_null(row, false);
    }
    input.data[0].set_value(0, true);
    input.data[0].set_value(1, false);

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 5);
    REQUIRE(result.value().data[0].is_null(1));
}

TEST_CASE("execution_dag::a selected null stays null") {
    execution_dag_t graph(resource());
    auto condition = input_slot(graph, 0, logical_type::BOOLEAN);
    auto when_true = input_slot(graph, 1, logical_type::BIGINT);
    auto otherwise = input_slot(graph, 2, logical_type::BIGINT);

    auto node = graph.add_blend(blend_node_t::blend_kind::case_when, slots({condition, when_true, otherwise}));
    graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    // The taken arm holds a null: validity comes from the selected side, so the result is null
    // even though the other arm has a value.
    auto input = chunk({logical_type::BOOLEAN, logical_type::BIGINT, logical_type::BIGINT}, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, true);
    input.data[1].set_null(0, true);
    input.data[2].set_null(0, false);
    input.data[2].set_value(0, static_cast<int64_t>(42));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().data[0].is_null(0));
}

TEST_CASE("execution_dag::a blend rejects a shape it cannot read") {
    SECTION("coalesce with no operands") {
        execution_dag_t graph(resource());
        auto node = graph.add_blend(blend_node_t::blend_kind::coalesce, slot_list_t(resource()));
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }
    SECTION("case with a condition but no result") {
        execution_dag_t graph(resource());
        auto condition = input_slot(graph, 0, logical_type::BOOLEAN);
        auto node = graph.add_blend(blend_node_t::blend_kind::case_when, slots({condition}));
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }
}

// A STRUCT is stored as one vector per entry, so reaching a field is a pointer walk, not a copy —
// and once the field node hands that vector on, the rest of the graph treats it as an ordinary
// column. Computing on it is what proves the slot really addresses the child's values.
TEST_CASE("execution_dag::a field node reaches a struct entry") {
    std::pmr::vector<complex_logical_type> entries(resource());
    entries.emplace_back(logical_type::BIGINT, "x");
    entries.emplace_back(logical_type::BIGINT, "y");
    auto struct_type = complex_logical_type::create_struct("point", entries);

    execution_dag_t graph(resource());
    auto column = graph.declare_slot();
    graph.bind_input(column, 0, struct_type);
    // Second entry of the struct, then +1 on it.
    std::pmr::vector<size_t> path(resource());
    path.push_back(1);
    auto field = graph.add_field(column, path);
    graph.set_slot_type(graph.output_slot(field), complex_logical_type(logical_type::BIGINT));

    auto one = graph.add_parameter(core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(one), complex_logical_type(logical_type::BIGINT));
    auto sum = graph.add_operator(operator_code::add, graph.output_slot(field), graph.output_slot(one));
    graph.set_slot_type(graph.output_slot(sum), complex_logical_type(logical_type::BIGINT));

    storage_parameters parameters(resource());
    REQUIRE(parameters.add(components::vector::make_scalar(resource(), static_cast<int64_t>(1)),
                           parameter_kind::literal) == core::parameter_id_t{0});
    graph.set_parameters(&parameters);
    graph.set_output(slots({graph.output_slot(field), graph.output_slot(sum)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    std::pmr::vector<complex_logical_type> column_types(resource());
    column_types.push_back(struct_type);
    data_chunk_t input(resource(), column_types);
    input.set_cardinality(2);
    input.data[0].entries()[0]->set_value(0, static_cast<int64_t>(10));
    input.data[0].entries()[1]->set_value(0, static_cast<int64_t>(20));
    input.data[0].entries()[0]->set_value(1, static_cast<int64_t>(30));
    input.data[0].entries()[1]->set_value(1, static_cast<int64_t>(40));

    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    // The field column is the struct's "y" entry, untouched...
    REQUIRE(result.value().data[0].get_value<int64_t>(0) == 20);
    REQUIRE(result.value().data[0].get_value<int64_t>(1) == 40);
    // ...and it feeds an operator like any other column.
    REQUIRE(result.value().data[1].get_value<int64_t>(0) == 21);
    REQUIRE(result.value().data[1].get_value<int64_t>(1) == 41);
}

// A path with no steps below the column addresses nothing, so the node is refused rather than
// silently behaving like a plain column reference.
TEST_CASE("execution_dag::a field node with no path is refused") {
    execution_dag_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);
    std::pmr::vector<size_t> empty(resource());
    auto field = graph.add_field(column, empty);
    graph.set_slot_type(graph.output_slot(field), complex_logical_type(logical_type::BIGINT));
    graph.set_output(slots({graph.output_slot(field)}));
    REQUIRE(graph.prepare().contains_error());
}

// -- membership: probe <op> ANY|ALL (set) --------------------------------------------------------------------------

namespace {

    using components::execution_dag::membership_node_t;
    using components::vector::make_array;
    using components::vector::make_null;
    using components::vector::make_scalar;
    using components::vector::vector_t;
    using optional_int_t = std::optional<int64_t>;

    enum class tri
    {
        no,
        yes,
        unknown
    };

    tri compare_3vl(operator_code op, const optional_int_t& left, const optional_int_t& right) {
        if (!left || !right) {
            return tri::unknown;
        }
        bool result = false;
        switch (op) {
            case operator_code::equal:
                result = *left == *right;
                break;
            case operator_code::not_equal:
                result = *left != *right;
                break;
            case operator_code::less:
                result = *left < *right;
                break;
            case operator_code::less_equal:
                result = *left <= *right;
                break;
            case operator_code::greater:
                result = *left > *right;
                break;
            default:
                result = *left >= *right;
                break;
        }
        return result ? tri::yes : tri::no;
    }

    // The definition, element by element: ANY is an OR of the comparisons, ALL an AND, both in three-valued logic.
    tri quantified_3vl(membership_node_t::quantifier kind,
                       operator_code op,
                       const optional_int_t& probe,
                       const std::vector<optional_int_t>& set) {
        const bool any = kind == membership_node_t::quantifier::any;
        tri outcome = any ? tri::no : tri::yes;
        for (const auto& element : set) {
            const tri one = compare_3vl(op, probe, element);
            if (one == (any ? tri::yes : tri::no)) {
                return one;
            }
            if (one == tri::unknown) {
                outcome = tri::unknown;
            }
        }
        return outcome;
    }

    vector_t bigint_set(const std::vector<optional_int_t>& set) {
        const complex_logical_type bigint{logical_type::BIGINT};
        std::vector<vector_t> elements;
        for (const auto& element : set) {
            elements.push_back(element ? make_scalar(resource(), *element) : make_null(resource(), bigint));
        }
        return make_array(resource(), bigint, elements);
    }

    // probe column (BIGINT, column 0) -> membership(set parameter 0) -> output
    struct bigint_membership_t {
        bigint_membership_t(operator_code op, membership_node_t::quantifier kind)
            : graph(resource()) {
            auto probe = input_slot(graph, 0, logical_type::BIGINT);
            set_slot = graph.output_slot(graph.add_parameter(core::parameter_id_t{0}));
            node = graph.add_membership(op, kind, probe, set_slot);
            output = graph.output_slot(node);
        }

        void bind(vector_t set) {
            parameters = std::make_unique<storage_parameters>(resource());
            REQUIRE(parameters->add(std::move(set), parameter_kind::literal) == core::parameter_id_t{0});
            graph.set_slot_type(set_slot, parameters->type(core::parameter_id_t{0}));
        }

        execution_dag_t graph;
        std::unique_ptr<storage_parameters> parameters;
        slot_id_t set_slot{0};
        node_id_t node{0};
        slot_id_t output{0};
    };

    std::vector<tri> run_membership(operator_code op,
                                    membership_node_t::quantifier kind,
                                    const std::vector<optional_int_t>& set,
                                    const std::vector<optional_int_t>& probes) {
        execution_dag_t graph(resource());
        auto probe = input_slot(graph, 0, logical_type::BIGINT);
        auto placed = graph.add_parameter(core::parameter_id_t{0});
        auto node = graph.add_membership(op, kind, probe, graph.output_slot(placed));

        storage_parameters parameters(resource());
        REQUIRE(parameters.add(bigint_set(set), parameter_kind::literal) == core::parameter_id_t{0});
        graph.set_slot_type(graph.output_slot(placed), parameters.type(core::parameter_id_t{0}));
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE_FALSE(graph.prepare().contains_error());
        graph.set_parameters(&parameters);

        auto input = chunk({logical_type::BIGINT}, probes.size());
        for (uint64_t row = 0; row < probes.size(); row++) {
            input.data[0].set_null(row, !probes[row].has_value());
            if (probes[row]) {
                input.data[0].set_value(row, *probes[row]);
            }
        }
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, input.size());
        REQUIRE_FALSE(result.has_error());
        std::vector<tri> answers;
        for (uint64_t row = 0; row < probes.size(); row++) {
            const auto& column = result.value().data[0];
            answers.push_back(column.is_null(row) ? tri::unknown
                                                  : (column.get_value<bool>(row) ? tri::yes : tri::no));
        }
        return answers;
    }

    const char* name_of(tri value) {
        return value == tri::yes ? "TRUE" : (value == tri::no ? "FALSE" : "UNKNOWN");
    }

} // namespace

TEST_CASE("execution_dag::membership follows three-valued logic for every operator and quantifier") {
    const std::vector<std::pair<const char*, std::vector<optional_int_t>>> sets = {
        {"plain", {1, 2, 3}},
        {"with a NULL", {1, std::nullopt, 3}},
        {"only NULLs", {std::nullopt, std::nullopt}},
        {"one value", {2}},
        {"duplicates", {2, 2, 7, 7}},
        {"empty", {}},
    };
    const std::vector<optional_int_t> probes = {0, 1, 2, 3, 4, 7, std::nullopt};
    const std::vector<operator_code> operators = {operator_code::equal,
                                                  operator_code::not_equal,
                                                  operator_code::less,
                                                  operator_code::less_equal,
                                                  operator_code::greater,
                                                  operator_code::greater_equal};

    for (const auto& [set_name, set] : sets) {
        for (auto op : operators) {
            for (auto kind : {membership_node_t::quantifier::any, membership_node_t::quantifier::all}) {
                const auto answers = run_membership(op, kind, set, probes);
                for (size_t row = 0; row < probes.size(); row++) {
                    const auto expected = quantified_3vl(kind, op, probes[row], set);
                    INFO("set: " << set_name << ", op: " << static_cast<int>(op)
                                 << ", quantifier: " << (kind == membership_node_t::quantifier::any ? "ANY" : "ALL")
                                 << ", probe row " << row << ", expected " << name_of(expected) << " got "
                                 << name_of(answers[row]));
                    CHECK(answers[row] == expected);
                }
            }
        }
    }
}

TEST_CASE("execution_dag::a membership graph does not grow with the set") {
    auto nodes_for = [](size_t elements) {
        execution_dag_t graph(resource());
        auto probe = input_slot(graph, 0, logical_type::BIGINT);
        auto placed = graph.add_parameter(core::parameter_id_t{0});
        graph.set_slot_type(graph.output_slot(placed),
                            complex_logical_type::create_array(complex_logical_type{logical_type::BIGINT}, elements));
        graph.add_membership(operator_code::equal,
                             membership_node_t::quantifier::any,
                             probe,
                             graph.output_slot(placed));
        return std::pair{graph.node_count(), graph.slot_count()};
    };
    CHECK(nodes_for(3) == nodes_for(300000));
}

TEST_CASE("execution_dag::membership probed by a constant and by a dictionary") {
    const std::vector<optional_int_t> set = {10, std::nullopt, 30};

    SECTION("a constant probe answers once for every row") {
        execution_dag_t graph(resource());
        auto placed_probe = graph.add_parameter(core::parameter_id_t{1});
        graph.set_slot_type(graph.output_slot(placed_probe), logical_type::BIGINT);
        auto placed_set = graph.add_parameter(core::parameter_id_t{0});
        auto node = graph.add_membership(operator_code::equal,
                                         membership_node_t::quantifier::any,
                                         graph.output_slot(placed_probe),
                                         graph.output_slot(placed_set));
        storage_parameters parameters(resource());
        REQUIRE(parameters.add(bigint_set(set), parameter_kind::literal) == core::parameter_id_t{0});
        REQUIRE(parameters.add(make_scalar(resource(), int64_t{30}), parameter_kind::literal) ==
                core::parameter_id_t{1});
        graph.set_slot_type(graph.output_slot(placed_set), parameters.type(core::parameter_id_t{0}));
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE_FALSE(graph.prepare().contains_error());
        graph.set_parameters(&parameters);

        auto input = chunk({logical_type::BIGINT}, 5);
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, input.size());
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value().size() == 5);
        for (uint64_t row = 0; row < 5; row++) {
            CHECK_FALSE(result.value().data[0].is_null(row));
            CHECK(result.value().data[0].get_value<bool>(row));
        }
    }

    SECTION("a dictionary probe is read through its indirection") {
        bigint_membership_t membership(operator_code::equal, membership_node_t::quantifier::any);
        membership.bind(bigint_set(set));
        membership.graph.set_output(slots({membership.output}));
        REQUIRE_FALSE(membership.graph.prepare().contains_error());
        membership.graph.set_parameters(membership.parameters.get());

        vector_t base(resource(), complex_logical_type{logical_type::BIGINT}, 4);
        for (uint64_t row = 0; row < 4; row++) {
            base.set_null(row, false);
            base.set_value(row, static_cast<int64_t>(10 * (row + 1)));
        }
        base.set_null(3, true);
        components::vector::indexing_vector_t selection(resource(), 3);
        selection.set_index(0, 2);
        selection.set_index(1, 3);
        selection.set_index(2, 1);
        vector_t dictionary(resource(), complex_logical_type{logical_type::BIGINT}, 4);
        dictionary.slice(base, selection, 3);

        data_chunk_t input(resource(),
                           std::pmr::vector<complex_logical_type>({complex_logical_type{logical_type::BIGINT}},
                                                                  resource()));
        input.data[0].reference(dictionary);
        input.set_cardinality(3);
        REQUIRE_FALSE(membership.graph.process(input, graph_execution_context{}).contains_error());
        auto result = membership.graph.finalize(graph_execution_context{}, input.size());
        REQUIRE_FALSE(result.has_error());
        const auto& answers = result.value().data[0];
        CHECK_FALSE(answers.is_null(0));
        CHECK(answers.get_value<bool>(0));  // 30
        CHECK(answers.is_null(1));          // NULL probe
        CHECK(answers.is_null(2));          // 20 is not in {10, NULL, 30}: UNKNOWN
    }
}

TEST_CASE("execution_dag::a membership node stays out of the rows its constraint masks") {
    execution_dag_t graph(resource());
    auto probe = input_slot(graph, 0, logical_type::BIGINT);
    auto mask = input_slot(graph, 1, logical_type::BOOLEAN);
    auto placed = graph.add_parameter(core::parameter_id_t{0});
    auto node = graph.add_membership(operator_code::equal,
                                     membership_node_t::quantifier::any,
                                     probe,
                                     graph.output_slot(placed));
    graph.set_constraint(node, mask);

    storage_parameters parameters(resource());
    REQUIRE(parameters.add(bigint_set({1, 2}), parameter_kind::literal) == core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), parameters.type(core::parameter_id_t{0}));
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto run = [&](std::vector<int64_t> values, std::vector<bool> active) {
        auto input = chunk({logical_type::BIGINT, logical_type::BOOLEAN}, values.size());
        for (uint64_t row = 0; row < values.size(); row++) {
            input.data[0].set_null(row, false);
            input.data[0].set_value(row, values[row]);
            input.data[1].set_null(row, false);
            input.data[1].set_value(row, static_cast<bool>(active[row]));
        }
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, input.size());
        REQUIRE_FALSE(result.has_error());
        return std::move(result.value());
    };

    auto first = run({1, 9}, {true, true});
    REQUIRE(first.data[0].get_value<bool>(0));
    REQUIRE_FALSE(first.data[0].get_value<bool>(1));
    // row 0 is masked now and holds a value that would flip its answer; row 1 is live and flips
    auto second = run({9, 2}, {false, true});
    CHECK(second.data[0].get_value<bool>(0));  // still the first run's answer: the node never touched it
    CHECK(second.data[0].get_value<bool>(1));
}

TEST_CASE("execution_dag::a membership state follows the set it is bound to") {
    bigint_membership_t membership(operator_code::equal, membership_node_t::quantifier::any);
    membership.bind(bigint_set({1, 2}));
    membership.graph.set_output(slots({membership.output}));
    REQUIRE_FALSE(membership.graph.prepare().contains_error());
    membership.graph.set_parameters(membership.parameters.get());

    auto answer = [&](int64_t value) {
        auto input = chunk({logical_type::BIGINT}, 1);
        input.data[0].set_null(0, false);
        input.data[0].set_value(0, value);
        REQUIRE_FALSE(membership.graph.process(input, graph_execution_context{}).contains_error());
        auto result = membership.graph.finalize(graph_execution_context{}, input.size());
        REQUIRE_FALSE(result.has_error());
        return result.value().data[0].get_value<bool>(0);
    };

    CHECK(answer(1));
    CHECK_FALSE(answer(5));

    SECTION("binding the same parameters again is not a new set") {
        membership.graph.set_parameters(membership.parameters.get());
        CHECK(answer(1));
        CHECK_FALSE(answer(5));
    }

    SECTION("a replaced set is read afresh") {
        storage_parameters other(resource());
        REQUIRE(other.add(bigint_set({5, 6}), parameter_kind::literal) == core::parameter_id_t{0});
        membership.graph.set_parameters(&other);
        CHECK(answer(5));
        CHECK_FALSE(answer(1));
        // and back
        membership.graph.set_parameters(membership.parameters.get());
        CHECK(answer(1));
        CHECK_FALSE(answer(5));
    }
}

TEST_CASE("execution_dag::a membership between different types is refused, not read through the wrong layout") {
    execution_dag_t graph(resource());
    auto probe = input_slot(graph, 0, logical_type::INTEGER);
    auto placed = graph.add_parameter(core::parameter_id_t{0});
    auto node = graph.add_membership(operator_code::equal,
                                     membership_node_t::quantifier::any,
                                     probe,
                                     graph.output_slot(placed));
    storage_parameters parameters(resource());
    REQUIRE(parameters.add(bigint_set({1, 2}), parameter_kind::literal) == core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), parameters.type(core::parameter_id_t{0}));
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto input = chunk({logical_type::INTEGER}, 1);
    input.data[0].set_null(0, false);
    input.data[0].set_value(0, int32_t{1});
    auto error = graph.process(input, graph_execution_context{});
    REQUIRE(error.contains_error());
    CHECK(error.type == core::error_code_t::invalid_parameter);
}

TEST_CASE("execution_dag::a membership casts the set once to the probe's type") {
    execution_dag_t graph(resource());
    auto probe = input_slot(graph, 0, logical_type::BIGINT);
    auto placed = graph.add_parameter(core::parameter_id_t{0});
    auto node = graph.add_membership(operator_code::less,
                                     membership_node_t::quantifier::any,
                                     probe,
                                     graph.output_slot(placed));
    auto cast = casts().resolve(complex_logical_type(logical_type::INTEGER),
                                complex_logical_type(logical_type::BIGINT),
                                components::casts::cast_type::implicit);
    REQUIRE(cast.has_value());
    graph.set_membership_cast(node, complex_logical_type(logical_type::BIGINT), *cast);

    const complex_logical_type integer{logical_type::INTEGER};
    std::vector<vector_t> elements;
    elements.push_back(make_scalar(resource(), int32_t{10}));
    elements.push_back(make_null(resource(), integer));
    elements.push_back(make_scalar(resource(), int32_t{30}));
    storage_parameters parameters(resource());
    REQUIRE(parameters.add(make_array(resource(), integer, elements), parameter_kind::literal) ==
            core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), parameters.type(core::parameter_id_t{0}));
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    auto input = chunk({logical_type::BIGINT}, 3);
    for (uint64_t row = 0; row < 3; row++) {
        input.data[0].set_null(row, false);
        input.data[0].set_value(row, static_cast<int64_t>(row == 0 ? 29 : (row == 1 ? 30 : 5000000000)));
    }
    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    const auto& answers = result.value().data[0];
    CHECK_FALSE(answers.is_null(0));
    CHECK(answers.get_value<bool>(0));   // 29 < 30
    CHECK(answers.is_null(1));           // 30 < {10, NULL, 30}: no witness, a NULL element
    CHECK(answers.is_null(2));           // beyond any int: the cast target holds it, still no witness
}

namespace {

    // `probe <op> ANY|ALL (elements)` over one value family: the probe is a constant, the set a bound array
    tri typed_membership(operator_code op,
                         membership_node_t::quantifier kind,
                         const complex_logical_type& element_type,
                         vector_t probe,
                         std::vector<vector_t> elements,
                         std::pmr::memory_resource* graph_resource = nullptr) {
        execution_dag_t graph(graph_resource != nullptr ? graph_resource : resource());
        storage_parameters parameters(resource());
        REQUIRE(parameters.add(make_array(resource(), element_type, elements), parameter_kind::literal) ==
                core::parameter_id_t{0});
        REQUIRE(parameters.add(std::move(probe), parameter_kind::literal) == core::parameter_id_t{1});

        auto placed_set = graph.add_parameter(core::parameter_id_t{0});
        graph.set_slot_type(graph.output_slot(placed_set), parameters.type(core::parameter_id_t{0}));
        auto placed_probe = graph.add_parameter(core::parameter_id_t{1});
        graph.set_slot_type(graph.output_slot(placed_probe), parameters.type(core::parameter_id_t{1}));
        auto node = graph.add_membership(op,
                                         kind,
                                         graph.output_slot(placed_probe),
                                         graph.output_slot(placed_set));
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE_FALSE(graph.prepare().contains_error());
        graph.set_parameters(&parameters);

        auto input = chunk({logical_type::BIGINT}, 1);
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, 1);
        REQUIRE_FALSE(result.has_error());
        const auto& column = result.value().data[0];
        return column.is_null(0) ? tri::unknown : (column.get_value<bool>(0) ? tri::yes : tri::no);
    }

    constexpr auto any_of = membership_node_t::quantifier::any;
    constexpr auto all_of = membership_node_t::quantifier::all;

    std::vector<vector_t> scalars_of(std::initializer_list<double> values) {
        std::vector<vector_t> elements;
        for (double value : values) {
            elements.push_back(make_scalar(resource(), value));
        }
        return elements;
    }

    template<typename Value, typename Null>
    void check_family(const complex_logical_type& type, Value value, Null null_of) {
        auto set = [&](std::initializer_list<int> ids) {
            std::vector<vector_t> elements;
            for (int id : ids) {
                elements.push_back(value(id));
            }
            return elements;
        };
        // = ANY finds a member, misses a non-member
        CHECK(typed_membership(operator_code::equal, any_of, type, value(2), set({1, 2, 3})) == tri::yes);
        CHECK(typed_membership(operator_code::equal, any_of, type, value(9), set({1, 2, 3})) == tri::no);
        // <> ALL is NOT IN
        CHECK(typed_membership(operator_code::not_equal, all_of, type, value(2), set({1, 2, 3})) == tri::no);
        CHECK(typed_membership(operator_code::not_equal, all_of, type, value(9), set({1, 2, 3})) == tri::yes);
        // a NULL element makes a miss UNKNOWN and leaves a hit TRUE
        auto with_null = set({1, 2});
        with_null.push_back(null_of());
        CHECK(typed_membership(operator_code::equal, any_of, type, value(2), with_null) == tri::yes);
        CHECK(typed_membership(operator_code::equal, any_of, type, value(9), with_null) == tri::unknown);
        // a NULL probe is UNKNOWN
        CHECK(typed_membership(operator_code::equal, any_of, type, null_of(), set({1, 2})) == tri::unknown);
    }

} // namespace

TEST_CASE("execution_dag::membership over strings") {
    const complex_logical_type text{logical_type::STRING_LITERAL};
    check_family(
        text,
        [&](int id) { return make_scalar(resource(), std::string("value-") + std::to_string(id)); },
        [&] { return make_null(resource(), text); });
    // ordering is lexicographic
    CHECK(typed_membership(operator_code::less,
                           any_of,
                           text,
                           make_scalar(resource(), std::string("b")),
                           [&] {
                               std::vector<vector_t> elements;
                               elements.push_back(make_scalar(resource(), std::string("a")));
                               elements.push_back(make_scalar(resource(), std::string("c")));
                               return elements;
                           }()) == tri::yes);
}

TEST_CASE("execution_dag::membership over decimals") {
    auto created = complex_logical_type::create_decimal(resource(), 10, 2);
    REQUIRE_FALSE(created.has_error());
    const auto decimal = created.value();
    check_family(
        decimal,
        [&](int id) { return components::vector::make_decimal(resource(), decimal, id * 125); },
        [&] { return make_null(resource(), decimal); });
    // 1.25 < ANY {2.50, 3.75}
    std::vector<vector_t> elements;
    elements.push_back(components::vector::make_decimal(resource(), decimal, 250));
    elements.push_back(components::vector::make_decimal(resource(), decimal, 375));
    CHECK(typed_membership(operator_code::less,
                           any_of,
                           decimal,
                           components::vector::make_decimal(resource(), decimal, 125),
                           std::move(elements)) == tri::yes);
}

TEST_CASE("execution_dag::membership over UUIDs") {
    const complex_logical_type uuid{logical_type::UUID};
    check_family(
        uuid,
        [&](int id) {
            const components::types::int128_t high = static_cast<components::types::int128_t>(id) << 64;
            return make_scalar<components::types::int128_t>(resource(), uuid, high | 77);
        },
        [&] { return make_null(resource(), uuid); });
}

TEST_CASE("execution_dag::membership over enums") {
    const auto mood = complex_logical_type::create_enum("mood", {{"sad", 0}, {"ok", 1}, {"happy", 2}});
    auto value_of = [&](int id) { return make_scalar<int32_t>(resource(), mood, id % 3); };
    CHECK(typed_membership(operator_code::equal,
                           any_of,
                           mood,
                           value_of(1),
                           [&] {
                               std::vector<vector_t> elements;
                               elements.push_back(value_of(0));
                               elements.push_back(value_of(1));
                               return elements;
                           }()) == tri::yes);
    CHECK(typed_membership(operator_code::not_equal,
                           all_of,
                           mood,
                           value_of(2),
                           [&] {
                               std::vector<vector_t> elements;
                               elements.push_back(value_of(0));
                               elements.push_back(value_of(1));
                               return elements;
                           }()) == tri::yes);
    CHECK(typed_membership(operator_code::equal,
                           any_of,
                           mood,
                           make_null(resource(), mood),
                           [&] {
                               std::vector<vector_t> elements;
                               elements.push_back(value_of(0));
                               return elements;
                           }()) == tri::unknown);
}

TEST_CASE("execution_dag::membership over nested values") {
    const complex_logical_type bigint{logical_type::BIGINT};
    const auto list_type = complex_logical_type::create_list(bigint);
    auto list_of = [&](std::initializer_list<int64_t> values) {
        std::vector<vector_t> items;
        for (auto item : values) {
            items.push_back(make_scalar(resource(), item));
        }
        return components::vector::make_list(resource(), bigint, items);
    };
    auto sets = [&] {
        std::vector<vector_t> elements;
        elements.push_back(list_of({1, 2}));
        elements.push_back(list_of({3}));
        elements.push_back(list_of({}));
        return elements;
    };
    CHECK(typed_membership(operator_code::equal, any_of, list_type, list_of({3}), sets()) == tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, list_type, list_of({}), sets()) == tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, list_type, list_of({2, 1}), sets()) == tri::no);
    CHECK(typed_membership(operator_code::not_equal, all_of, list_type, list_of({1, 2}), sets()) == tri::no);
    // lists order by their first difference, then by length
    CHECK(typed_membership(operator_code::less, any_of, list_type, list_of({1}), sets()) == tri::yes);
    CHECK(typed_membership(operator_code::greater, all_of, list_type, list_of({4}), sets()) == tri::yes);
}

TEST_CASE("execution_dag::membership over doubles keeps NaN and signed zero together") {
    const complex_logical_type dbl{logical_type::DOUBLE};
    const double nan = std::numeric_limits<double>::quiet_NaN();
    // NaN is a member of a set holding NaN, the way vector hashing and cells_equal treat it
    CHECK(typed_membership(operator_code::equal, any_of, dbl, make_scalar(resource(), nan), scalars_of({1.0, nan})) ==
          tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, dbl, make_scalar(resource(), nan), scalars_of({1.0, 2.0})) ==
          tri::no);
    CHECK(typed_membership(operator_code::not_equal, all_of, dbl, make_scalar(resource(), nan), scalars_of({nan})) ==
          tri::no);
    CHECK(typed_membership(operator_code::not_equal, all_of, dbl, make_scalar(resource(), 1.0), scalars_of({nan})) ==
          tri::yes);
    // -0.0 is 0.0
    CHECK(typed_membership(operator_code::equal, any_of, dbl, make_scalar(resource(), -0.0), scalars_of({0.0, 5.0})) ==
          tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, dbl, make_scalar(resource(), 0.0), scalars_of({-0.0})) ==
          tri::yes);
    CHECK(typed_membership(operator_code::not_equal, all_of, dbl, make_scalar(resource(), 0.0), scalars_of({-0.0})) ==
          tri::no);
    // no tolerance: the neighbour of 1.0 is a different value
    CHECK(typed_membership(operator_code::equal,
                           any_of,
                           dbl,
                           make_scalar(resource(), 1.0),
                           scalars_of({std::nextafter(1.0, 2.0)})) == tri::no);
}

TEST_CASE("execution_dag::membership over a set longer than one vector") {
    const complex_logical_type bigint{logical_type::BIGINT};
    constexpr int64_t elements_count = 5000;
    std::vector<vector_t> elements;
    for (int64_t i = 0; i < elements_count; i++) {
        elements.push_back(make_scalar(resource(), i * 2));
    }
    auto probe = [&](int64_t value) { return make_scalar(resource(), value); };
    auto elements_copy = [&] { return elements; };
    CHECK(typed_membership(operator_code::equal, any_of, bigint, probe(0), elements_copy()) == tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, bigint, probe(2 * 1024), elements_copy()) == tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, bigint, probe(2 * 4999), elements_copy()) == tri::yes);
    CHECK(typed_membership(operator_code::equal, any_of, bigint, probe(2 * 4999 + 1), elements_copy()) == tri::no);
    CHECK(typed_membership(operator_code::less, any_of, bigint, probe(9997), elements_copy()) == tri::yes);
    CHECK(typed_membership(operator_code::less, any_of, bigint, probe(9998), elements_copy()) == tri::no);
}

TEST_CASE("execution_dag::membership over a million elements stays within a few bytes per element") {
    const complex_logical_type bigint{logical_type::BIGINT};
    constexpr uint64_t elements_count = 1000000;
    core::pmr::counting_resource_t counting(resource());
    auto array_type = complex_logical_type::create_array(bigint, elements_count);
    vector_t set(&counting, array_type, 1);
    auto* values = set.entry().data<int64_t>();
    for (uint64_t i = 0; i < elements_count; i++) {
        values[i] = static_cast<int64_t>(i * 3);
    }
    set.set_vector_type(components::vector::vector_type::CONSTANT);

    execution_dag_t graph(&counting);
    auto probe = input_slot(graph, 0, logical_type::BIGINT);
    auto placed = graph.add_parameter(core::parameter_id_t{0});
    auto node = graph.add_membership(operator_code::equal, any_of, probe, graph.output_slot(placed));
    storage_parameters parameters(&counting);
    REQUIRE(parameters.add(std::move(set), parameter_kind::literal) == core::parameter_id_t{0});
    graph.set_slot_type(graph.output_slot(placed), parameters.type(core::parameter_id_t{0}));
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());
    graph.set_parameters(&parameters);

    data_chunk_t input(&counting, std::pmr::vector<complex_logical_type>({bigint}, &counting));
    input.set_cardinality(4);
    const int64_t probes[] = {0, 2999997, 5, 2999998};
    for (uint64_t row = 0; row < 4; row++) {
        input.data[0].set_null(row, false);
        input.data[0].set_value(row, probes[row]);
    }
    const auto before = counting.allocated_bytes();
    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    auto result = graph.finalize(graph_execution_context{}, input.size());
    REQUIRE_FALSE(result.has_error());
    CHECK(result.value().data[0].get_value<bool>(0));
    CHECK(result.value().data[0].get_value<bool>(1));
    CHECK_FALSE(result.value().data[0].get_value<bool>(2));
    CHECK_FALSE(result.value().data[0].get_value<bool>(3));

    // the hashes (8 bytes) and a table at most half full (4 bytes per slot, a power of two): never the ~9 KB per
    // element that a node per element costs
    const auto built = counting.allocated_bytes() - before;
    INFO("bytes allocated by the graph: " << built << ", per element: " << built / elements_count);
    CHECK(built < 32 * elements_count);

    // probing again reuses the state
    const auto settled = counting.allocated_bytes();
    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    graph.set_parameters(&parameters);
    REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
    CHECK(counting.allocated_bytes() - settled < 64 * 1024);
}

TEST_CASE("execution_dag::membership over tiny doubles tests for exact equality in every form") {
    const complex_logical_type dbl{logical_type::DOUBLE};
    auto probe = [&] { return make_scalar(resource(), 1e-20); };
    auto set = [&] { return scalars_of({2e-20}); };
    CHECK(typed_membership(operator_code::equal, any_of, dbl, probe(), set()) == tri::no);
    CHECK(typed_membership(operator_code::equal, all_of, dbl, probe(), set()) == tri::no);
    CHECK(typed_membership(operator_code::not_equal, any_of, dbl, probe(), set()) == tri::yes);
    CHECK(typed_membership(operator_code::not_equal, all_of, dbl, probe(), set()) == tri::yes);
    // the same value is still equal
    CHECK(typed_membership(operator_code::not_equal, any_of, dbl, probe(), scalars_of({1e-20, 1e-20})) == tri::no);
    CHECK(typed_membership(operator_code::equal, all_of, dbl, probe(), scalars_of({1e-20, 1e-20})) == tri::yes);
    // a set whose elements differ among themselves only by a tiny amount is not uniform
    CHECK(typed_membership(operator_code::not_equal, any_of, dbl, probe(), scalars_of({1e-20, 2e-20})) == tri::yes);
}

namespace {

    // A struct-element set rebound to another set of the same length: struct vectors have no data() of their own
    vector_t pair_set(const complex_logical_type& pair, std::initializer_list<std::pair<int64_t, int64_t>> values) {
        std::vector<vector_t> elements;
        for (auto [x, y] : values) {
            std::vector<vector_t> fields;
            fields.push_back(make_scalar(resource(), x));
            fields.push_back(make_scalar(resource(), y));
            elements.push_back(components::vector::make_struct(resource(), pair, fields));
        }
        return make_array(resource(), pair, elements);
    }

} // namespace

TEST_CASE("execution_dag::a membership over struct elements follows a replaced set of the same length") {
    const complex_logical_type bigint{logical_type::BIGINT};
    std::pmr::vector<complex_logical_type> fields(resource());
    fields.push_back(bigint);
    fields.push_back(bigint);
    const auto pair = complex_logical_type::create_struct("pair_t", fields);

    execution_dag_t graph(resource());
    auto placed_set = graph.add_parameter(core::parameter_id_t{0});
    auto placed_probe = graph.add_parameter(core::parameter_id_t{1});
    auto node = graph.add_membership(operator_code::equal,
                                     any_of,
                                     graph.output_slot(placed_probe),
                                     graph.output_slot(placed_set));
    auto make_parameters = [&](vector_t set) {
        auto parameters = std::make_unique<storage_parameters>(resource());
        REQUIRE(parameters->add(std::move(set), parameter_kind::literal) == core::parameter_id_t{0});
        std::vector<vector_t> probe_fields;
        probe_fields.push_back(make_scalar(resource(), int64_t{1}));
        probe_fields.push_back(make_scalar(resource(), int64_t{2}));
        REQUIRE(parameters->add(components::vector::make_struct(resource(), pair, probe_fields),
                                parameter_kind::literal) == core::parameter_id_t{1});
        return parameters;
    };
    auto first = make_parameters(pair_set(pair, {{1, 2}, {3, 4}}));
    graph.set_slot_type(graph.output_slot(placed_set), first->type(core::parameter_id_t{0}));
    graph.set_slot_type(graph.output_slot(placed_probe), first->type(core::parameter_id_t{1}));
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto answer = [&](storage_parameters* parameters) {
        graph.set_parameters(parameters);
        auto input = chunk({logical_type::BIGINT}, 1);
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, 1);
        REQUIRE_FALSE(result.has_error());
        return result.value().data[0].get_value<bool>(0);
    };
    CHECK(answer(first.get()));
    first.reset();
    auto second = make_parameters(pair_set(pair, {{7, 8}, {9, 10}}));
    CHECK_FALSE(answer(second.get()));
    second.reset();
    auto third = make_parameters(pair_set(pair, {{5, 6}, {1, 2}}));
    CHECK(answer(third.get()));
}

TEST_CASE("execution_dag::a membership that casts its set follows a replaced set of the same length") {
    execution_dag_t graph(resource());
    auto probe = input_slot(graph, 0, logical_type::BIGINT);
    auto placed = graph.add_parameter(core::parameter_id_t{0});
    auto node = graph.add_membership(operator_code::equal, any_of, probe, graph.output_slot(placed));
    auto cast = casts().resolve(complex_logical_type(logical_type::INTEGER),
                                complex_logical_type(logical_type::BIGINT),
                                components::casts::cast_type::implicit);
    REQUIRE(cast.has_value());
    graph.set_membership_cast(node, complex_logical_type(logical_type::BIGINT), *cast);

    const complex_logical_type integer{logical_type::INTEGER};
    auto make_parameters = [&](int32_t a, int32_t b) {
        std::vector<vector_t> elements;
        elements.push_back(make_scalar(resource(), a));
        elements.push_back(make_scalar(resource(), b));
        auto parameters = std::make_unique<storage_parameters>(resource());
        REQUIRE(parameters->add(make_array(resource(), integer, elements), parameter_kind::literal) ==
                core::parameter_id_t{0});
        return parameters;
    };
    auto first = make_parameters(1, 2);
    graph.set_slot_type(graph.output_slot(placed), first->type(core::parameter_id_t{0}));
    graph.set_output(slots({graph.output_slot(node)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    auto answer = [&](storage_parameters* parameters, int64_t value) {
        graph.set_parameters(parameters);
        auto input = chunk({logical_type::BIGINT}, 1);
        input.data[0].set_null(0, false);
        input.data[0].set_value(0, value);
        REQUIRE_FALSE(graph.process(input, graph_execution_context{}).contains_error());
        auto result = graph.finalize(graph_execution_context{}, 1);
        REQUIRE_FALSE(result.has_error());
        return result.value().data[0].get_value<bool>(0);
    };
    CHECK(answer(first.get(), 1));
    first.reset();
    for (int round = 0; round < 20; round++) {
        auto next = make_parameters(10 + round, 100 + round);
        CHECK(answer(next.get(), 10 + round));
        CHECK_FALSE(answer(next.get(), 1));
    }
}

namespace {

    // PostgreSQL's order of float8: NaN is the greatest value and equals NaN
    int pg_float_order(double left, double right) {
        const bool left_nan = std::isnan(left);
        const bool right_nan = std::isnan(right);
        if (left_nan || right_nan) {
            return left_nan == right_nan ? 0 : (left_nan ? 1 : -1);
        }
        return left < right ? -1 : (left > right ? 1 : 0);
    }

    bool pg_float_holds(operator_code op, double left, double right) {
        const int order = pg_float_order(left, right);
        switch (op) {
            case operator_code::equal:
                return order == 0;
            case operator_code::not_equal:
                return order != 0;
            case operator_code::less:
                return order < 0;
            case operator_code::less_equal:
                return order <= 0;
            case operator_code::greater:
                return order > 0;
            default:
                return order >= 0;
        }
    }

} // namespace

TEST_CASE("execution_dag::membership over doubles orders NaN as the greatest value") {
    const complex_logical_type dbl{logical_type::DOUBLE};
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const std::vector<std::vector<double>> sets = {{nan, 1, 2}, {1, nan, 2}, {1, 2, nan}, {nan}, {nan, nan}, {1, 2}};
    const std::vector<double> probes = {0, 1, 1.5, 2, 3, nan};
    for (auto op : {operator_code::equal,
                    operator_code::not_equal,
                    operator_code::less,
                    operator_code::less_equal,
                    operator_code::greater,
                    operator_code::greater_equal}) {
        for (auto kind : {any_of, all_of}) {
            for (const auto& set : sets) {
                for (double probe : probes) {
                    bool expected = kind == all_of;
                    for (double element : set) {
                        const bool holds = pg_float_holds(op, probe, element);
                        if (kind == any_of && holds) {
                            expected = true;
                        }
                        if (kind == all_of && !holds) {
                            expected = false;
                        }
                    }
                    std::vector<vector_t> elements;
                    for (double element : set) {
                        elements.push_back(make_scalar(resource(), element));
                    }
                    INFO("op " << static_cast<int>(op) << (kind == any_of ? " ANY" : " ALL") << " probe " << probe
                               << " first element " << set.front() << " size " << set.size());
                    CHECK(typed_membership(op, kind, dbl, make_scalar(resource(), probe), std::move(elements)) ==
                          (expected ? tri::yes : tri::no));
                }
            }
        }
    }
}
