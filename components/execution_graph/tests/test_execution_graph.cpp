#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <components/execution_graph/execution_graph.hpp>

using namespace components::execution_graph;

using components::graph_execution_context;
using components::casts::cast_kind;
using components::operators::operator_code;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::types::parameter_map_t;
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
    slot_id_t input_slot(execution_graph_t& graph, size_t column, logical_type type) {
        auto slot = graph.declare_slot();
        graph.bind_input(slot, column, type);
        return slot;
    }

} // namespace

TEST_CASE("execution_graph::an operator over two input columns") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::a node can be built before the nodes feeding it") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::a cast is inserted on the edge that needs it") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::output order is independent of execution order") {
    execution_graph_t graph(resource());
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
TEST_CASE("execution_graph::slots follow the stored column order, not the query's") {
    execution_graph_t graph(resource());
    auto column_y = input_slot(graph, 1, logical_type::BIGINT);
    auto column_x = input_slot(graph, 0, logical_type::BIGINT);

    parameter_map_t parameters(resource());
    parameters.emplace(core::parameter_id_t{1}, logical_value_t(resource(), int64_t{1}));

    auto one = graph.add_parameter(core::parameter_id_t{1});
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

TEST_CASE("execution_graph::a chunk laid out differently than the bound slots is refused") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::a parameter becomes a constant operand") {
    execution_graph_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);

    parameter_map_t parameters(resource());
    parameters.emplace(core::parameter_id_t{1}, logical_value_t(resource(), int64_t{100}));

    auto placed = graph.add_parameter(core::parameter_id_t{1});
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

TEST_CASE("execution_graph::a unary operator takes one operand") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::a null operand yields a null result") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::malformed graphs are refused") {
    SECTION("an input that was never connected") {
        execution_graph_t graph(resource());
        auto column = input_slot(graph, 0, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, invalid_slot, column);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("a slot nothing writes") {
        execution_graph_t graph(resource());
        auto column = input_slot(graph, 0, logical_type::BIGINT);
        auto orphan = graph.declare_slot();
        graph.set_slot_type(orphan, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, column, orphan);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("a slot with no type") {
        execution_graph_t graph(resource());
        auto left = input_slot(graph, 0, logical_type::BIGINT);
        auto right = input_slot(graph, 1, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, left, right);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("a cycle") {
        execution_graph_t graph(resource());
        auto column = input_slot(graph, 0, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, column, invalid_slot);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.connect(node, 1, graph.output_slot(node));
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("no output selected") {
        execution_graph_t graph(resource());
        auto left = input_slot(graph, 0, logical_type::BIGINT);
        auto right = input_slot(graph, 1, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, left, right);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        REQUIRE(graph.prepare().contains_error());
    }

    SECTION("processing before prepare") {
        execution_graph_t graph(resource());
        auto left = input_slot(graph, 0, logical_type::BIGINT);
        auto right = input_slot(graph, 1, logical_type::BIGINT);
        auto node = graph.add_operator(operator_code::add, left, right);
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        auto input = chunk({logical_type::BIGINT, logical_type::BIGINT}, 1);
        REQUIRE(graph.process(input, graph_execution_context{}).contains_error());
    }
}
TEST_CASE("execution_graph::coalesce takes the first operand that is not null") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::case takes the arm of the first true condition") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::case with no else and no match is null") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::a selected null stays null") {
    execution_graph_t graph(resource());
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

TEST_CASE("execution_graph::a blend rejects a shape it cannot read") {
    SECTION("coalesce with no operands") {
        execution_graph_t graph(resource());
        auto node = graph.add_blend(blend_node_t::blend_kind::coalesce, slot_list_t(resource()));
        graph.set_slot_type(graph.output_slot(node), logical_type::BIGINT);
        graph.set_output(slots({graph.output_slot(node)}));
        REQUIRE(graph.prepare().contains_error());
    }
    SECTION("case with a condition but no result") {
        execution_graph_t graph(resource());
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
TEST_CASE("execution_graph::a field node reaches a struct entry") {
    std::pmr::vector<complex_logical_type> entries(resource());
    entries.emplace_back(logical_type::BIGINT, "x");
    entries.emplace_back(logical_type::BIGINT, "y");
    auto struct_type = complex_logical_type::create_struct("point", entries);

    execution_graph_t graph(resource());
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

    parameter_map_t parameters(resource());
    parameters.emplace(core::parameter_id_t{0}, logical_value_t{resource(), static_cast<int64_t>(1)});
    graph.set_parameters(&parameters);
    graph.set_output(slots({graph.output_slot(field), graph.output_slot(sum)}));
    REQUIRE_FALSE(graph.prepare().contains_error());

    std::pmr::vector<complex_logical_type> column_types(resource());
    column_types.push_back(struct_type);
    data_chunk_t input(resource(), column_types);
    input.set_cardinality(2);
    input.data[0].entries()[0]->set_value(0, logical_value_t{resource(), static_cast<int64_t>(10)});
    input.data[0].entries()[1]->set_value(0, logical_value_t{resource(), static_cast<int64_t>(20)});
    input.data[0].entries()[0]->set_value(1, logical_value_t{resource(), static_cast<int64_t>(30)});
    input.data[0].entries()[1]->set_value(1, logical_value_t{resource(), static_cast<int64_t>(40)});

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
TEST_CASE("execution_graph::a field node with no path is refused") {
    execution_graph_t graph(resource());
    auto column = input_slot(graph, 0, logical_type::BIGINT);
    std::pmr::vector<size_t> empty(resource());
    auto field = graph.add_field(column, empty);
    graph.set_slot_type(graph.output_slot(field), complex_logical_type(logical_type::BIGINT));
    graph.set_output(slots({graph.output_slot(field)}));
    REQUIRE(graph.prepare().contains_error());
}
