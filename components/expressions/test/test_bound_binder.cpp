#include <catch2/catch_test_macros.hpp>

#include <components/expressions/bound/binder.hpp>
#include <components/expressions/bound/expression_executor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <core/operations_helper.hpp>
#include <memory_resource>

using namespace components;
using namespace components::expressions;
using types::complex_logical_type;
using types::logical_type;
using ekey = components::expressions::key_t;

namespace {

    bind_schema_t two_columns(std::pmr::memory_resource* resource) {
        bind_schema_t schema{resource};
        schema.add("a", complex_logical_type{logical_type::BIGINT});
        schema.add("b", complex_logical_type{logical_type::BIGINT});
        return schema;
    }

} // namespace

// ============================================================================
// The binder resolves a NAME against the input schema once and hands the tree a
// positional reference. Nothing downstream of it compares strings -- that is the
// point of the layer.
// ============================================================================
TEST_CASE("components::expressions::bound::binder_resolves_a_column_name_to_a_positional_reference") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    binder_context_t context{};
    context.left = &schema;

    ekey key{&resource, "b"};
    auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->kind() == bound_kind::reference);
    const auto* reference = static_cast<const bound_reference_t*>(bound.value().get());
    CHECK(reference->column_index() == 1);
    CHECK(reference->return_type().type() == logical_type::BIGINT);
}

TEST_CASE("components::expressions::bound::binder_reports_an_unknown_column") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    binder_context_t context{};
    context.left = &schema;

    ekey key{&resource, "nope"};
    auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE(bound.has_error());
    CHECK(bound.error().type == core::error_code_t::field_not_exists);
}

// Duplicate column names are legal in this engine (a computing table can carry two
// columns of the same name), so such a name has more than one right answer. A binder
// that silently picked one of them would bake an arbitrary choice into the compiled
// tree, so it refuses instead.
TEST_CASE("components::expressions::bound::binder_refuses_an_ambiguous_column_name") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("a", complex_logical_type{logical_type::BIGINT});
    schema.add("a", complex_logical_type{logical_type::DOUBLE});

    binder_context_t context{};
    context.left = &schema;

    ekey key{&resource, "a"};
    auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE(bound.has_error());
    CHECK(bound.error().type == core::error_code_t::ambiguous_name);
}

// ============================================================================
// A bound arithmetic tree carries the result type the kernel will produce, and
// evaluating it agrees with what the node promised.
// ============================================================================
TEST_CASE("components::expressions::bound::binder_types_arithmetic_and_the_executor_agrees") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("f", complex_logical_type{logical_type::FLOAT});
    schema.add("i", complex_logical_type{logical_type::INTEGER});

    binder_context_t context{};
    context.left = &schema;

    auto expression = make_scalar_expression(&resource, scalar_type::add);
    expression->append_param(ekey{&resource, "f"});
    expression->append_param(ekey{&resource, "i"});

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    CHECK(bound.value()->kind() == bound_kind::arithmetic);
    CHECK(bound.value()->return_type().type() == logical_type::FLOAT);

    std::pmr::vector<complex_logical_type> chunk_types{&resource};
    chunk_types.emplace_back(logical_type::FLOAT);
    chunk_types.emplace_back(logical_type::INTEGER);
    vector::data_chunk_t chunk(&resource, chunk_types, 2);
    chunk.set_value(0, uint64_t{0}, 1.5F);
    chunk.set_value(1, uint64_t{0}, static_cast<int32_t>(2));
    chunk.set_value(0, uint64_t{1}, 2.5F);
    chunk.set_value(1, uint64_t{1}, static_cast<int32_t>(3));
    chunk.set_cardinality(2);

    auto executor = expression_executor_t::create(&resource, bound.value(), 2);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    auto result = executor.value().execute(chunk, 2, execution);
    REQUIRE_FALSE(result.has_error());
    CHECK(result.value()->type().type() == bound.value()->return_type().type());
    CHECK(core::is_equals(result.value()->get_value<float>(0), 3.5F));
    CHECK(core::is_equals(result.value()->get_value<float>(1), 5.5F));
}

// ============================================================================
// A WHERE tree: AND of two comparisons, one against a plan parameter -- the
// shape operator_match selects with.
// ============================================================================
TEST_CASE("components::expressions::bound::binder_binds_a_conjunctive_predicate") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    auto parameters = logical_plan::make_parameter_node(&resource);
    parameters->add_parameter(core::parameter_id_t{0}, static_cast<int64_t>(2));

    binder_context_t context{};
    context.left = &schema;
    context.parameters = &parameters->parameters();

    auto left = make_compare_expression(&resource, compare_type::gte, ekey{&resource, "a"}, core::parameter_id_t{0});
    auto right = make_compare_expression(&resource, compare_type::lt, ekey{&resource, "a"}, ekey{&resource, "b"});
    auto conjunction = make_compare_union_expression(&resource, compare_type::union_and);
    conjunction->append_child(left);
    conjunction->append_child(right);

    binder_t binder{&resource};
    auto bound = binder.bind(conjunction, context);
    REQUIRE_FALSE(bound.has_error());
    CHECK(bound.value()->kind() == bound_kind::conjunction);
    CHECK(bound.value()->return_type().type() == logical_type::BOOLEAN);
    // The parameter slot is live, so nothing in this tree may be folded away.
    CHECK_FALSE(bound.value()->traits().foldable);

    std::pmr::vector<complex_logical_type> chunk_types{&resource};
    chunk_types.emplace_back(logical_type::BIGINT);
    chunk_types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, chunk_types, 5);
    for (uint64_t row = 0; row < 5; ++row) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
        chunk.set_value(1, row, static_cast<int64_t>(4));
    }
    chunk.set_cardinality(5);

    auto executor = expression_executor_t::create(&resource, bound.value(), 5);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    execution.parameters = &parameters->parameters();

    vector::indexing_vector_t selection(&resource, uint64_t{5});
    auto selected = executor.value().select(chunk, 5, execution, selection);
    REQUIRE_FALSE(selected.has_error());
    // a >= 2 AND a < b(=4): rows 2 and 3.
    REQUIRE(selected.value() == 2);
    CHECK(selection.get_index(0) == 2);
    CHECK(selection.get_index(1) == 3);
}

// A parameter slot is typed at bind time from its current binding, but its VALUE is
// still read live -- the two are different questions and only the first is settled
// when the tree is compiled.
TEST_CASE("components::expressions::bound::binder_types_a_parameter_slot_without_freezing_its_value") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    auto parameters = logical_plan::make_parameter_node(&resource);
    parameters->add_parameter(core::parameter_id_t{4}, static_cast<int64_t>(11));

    binder_context_t context{};
    context.left = &schema;
    context.parameters = &parameters->parameters();

    auto expression = make_scalar_expression(&resource, scalar_type::add);
    expression->append_param(ekey{&resource, "a"});
    expression->append_param(core::parameter_id_t{4});

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->children().size() == 2);
    CHECK(bound.value()->children()[1]->kind() == bound_kind::parameter);
    CHECK(bound.value()->children()[1]->return_type().type() == logical_type::BIGINT);
    CHECK_FALSE(bound.value()->children()[1]->traits().foldable);
}

// A key carrying a '::' cast is bound as a cast NODE over the reference, so the
// conversion is part of the compiled tree instead of being rediscovered per row.
TEST_CASE("components::expressions::bound::binder_wraps_a_cast_key_in_a_cast_node") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("i", complex_logical_type{logical_type::INTEGER});

    binder_context_t context{};
    context.left = &schema;

    ekey key{&resource, "i"};
    key.set_cast_type(complex_logical_type{logical_type::DOUBLE});
    auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->kind() == bound_kind::cast);
    CHECK(bound.value()->return_type().type() == logical_type::DOUBLE);
    REQUIRE(bound.value()->children().size() == 1);
    CHECK(bound.value()->children()[0]->kind() == bound_kind::reference);
}

// A join predicate names both sides; side_t picks the schema the name is resolved
// against, and the column index is positional WITHIN that side.
TEST_CASE("components::expressions::bound::binder_resolves_each_side_against_its_own_schema") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t left_schema{&resource};
    left_schema.add("id", complex_logical_type{logical_type::BIGINT});
    bind_schema_t right_schema{&resource};
    right_schema.add("other", complex_logical_type{logical_type::BIGINT});
    right_schema.add("id", complex_logical_type{logical_type::BIGINT});

    binder_context_t context{};
    context.left = &left_schema;
    context.right = &right_schema;

    ekey left_key{&resource, "id", side_t::left};
    ekey right_key{&resource, "id", side_t::right};
    auto expression = make_compare_expression(&resource, compare_type::eq, left_key, right_key);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->children().size() == 2);
    const auto* lhs = static_cast<const bound_reference_t*>(bound.value()->children()[0].get());
    const auto* rhs = static_cast<const bound_reference_t*>(bound.value()->children()[1].get());
    CHECK(lhs->column_index() == 0);
    CHECK(lhs->side() == side_t::left);
    CHECK(rhs->column_index() == 1);
    CHECK(rhs->side() == side_t::right);
}

// ============================================================================
// A comparison between two different widths has to be made comparable AT BIND
// TIME. The comparison kernels index both operands as the same C++ type, so a
// tree that hands them an INT32 column and an INT64 constant is not "slightly
// wrong", it reads the wrong bytes. Promotion belongs in the compiled tree.
// ============================================================================
TEST_CASE("components::expressions::bound::binder_promotes_a_mixed_width_comparison") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("i", complex_logical_type{logical_type::INTEGER});

    auto parameters = logical_plan::make_parameter_node(&resource);
    parameters->add_parameter(core::parameter_id_t{0}, static_cast<int64_t>(2));

    binder_context_t context{};
    context.left = &schema;
    context.parameters = &parameters->parameters();

    auto expression =
        make_compare_expression(&resource, compare_type::gte, ekey{&resource, "i"}, core::parameter_id_t{0});

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->children().size() == 2);
    // Both operands must reach the kernel as the SAME physical type.
    CHECK(bound.value()->children()[0]->physical_type() == bound.value()->children()[1]->physical_type());

    std::pmr::vector<complex_logical_type> chunk_types{&resource};
    chunk_types.emplace_back(logical_type::INTEGER);
    vector::data_chunk_t chunk(&resource, chunk_types, 5);
    for (uint64_t row = 0; row < 5; ++row) {
        chunk.set_value(0, row, static_cast<int32_t>(row));
    }
    chunk.set_cardinality(5);

    auto executor = expression_executor_t::create(&resource, bound.value(), 5);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    execution.parameters = &parameters->parameters();

    vector::indexing_vector_t selection(&resource, uint64_t{5});
    auto selected = executor.value().select(chunk, 5, execution, selection);
    REQUIRE_FALSE(selected.has_error());
    REQUIRE(selected.value() == 3);
    CHECK(selection.get_index(0) == 2);
    CHECK(selection.get_index(1) == 3);
    CHECK(selection.get_index(2) == 4);
}

// A sort key binds to the expression whose value the sort orders by -- the thing
// `sort_key` re-derives at execution time today.
TEST_CASE("components::expressions::bound::binder_binds_a_sort_key") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    binder_context_t context{};
    context.left = &schema;

    auto expression = make_sort_expression(ekey{&resource, "b"}, sort_order::desc);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->kind() == bound_kind::reference);
    CHECK(static_cast<const bound_reference_t*>(bound.value().get())->column_index() == 1);
}

// Everything the binder cannot express is REFUSED, loudly, through the error channel. No node is
// emitted that claims a semantics the executor does not have. The refusal example is a dotted NAME
// with no ordinals resolved into it, which needs the nested column shape the validator owns and
// which this layer will not guess at.
TEST_CASE("components::expressions::bound::binder_refuses_what_it_cannot_express") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    binder_context_t context{};
    context.left = &schema;
    binder_t binder{&resource};

    INFO("a column-valued regex pattern binds, as a per-row DYNAMIC node");
    {
        auto expression = make_compare_expression(&resource,
                                                  compare_type::regex,
                                                  ekey{&resource, "a"},
                                                  ekey{&resource, "b"});
        auto bound = binder.bind(expression, context);
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::regex);
        const auto* node = static_cast<const bound_regex_t*>(bound.value().get());
        CHECK(node->regex_mode() == bound_regex_t::mode::dynamic);
        CHECK(node->children().size() == 2); // subject AND pattern
        CHECK(node->return_type().type() == logical_type::BOOLEAN);
    }

    INFO("an UNRESOLVED nested name is still refused, and named as such");
    {
        // pmr uses-allocator construction appends the vector's own allocator, so an element must
        // NOT also be handed a resource of its own.
        std::pmr::vector<std::pmr::string> names{&resource};
        names.emplace_back("a");
        names.emplace_back("inner");
        ekey nested{std::move(names)};
        nested.set_side(side_t::left);
        auto expression = make_scalar_expression(&resource, scalar_type::get_field, nested);
        auto bound = binder.bind(expression, context);
        REQUIRE(bound.has_error());
        CHECK(bound.error().type == core::error_code_t::unimplemented_yet);
    }
}

// Binding never throws (rules 2 and 9): a null expression is an error, not a crash.
TEST_CASE("components::expressions::bound::binder_reports_a_missing_expression") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = two_columns(&resource);

    binder_context_t context{};
    context.left = &schema;

    binder_t binder{&resource};
    core::error_code_t observed = core::error_code_t::none;
    REQUIRE_NOTHROW([&] {
        auto bound = binder.bind(expression_ptr{}, context);
        if (bound.has_error()) {
            observed = bound.error().type;
        }
    }());
    CHECK(observed == core::error_code_t::invalid_parameter);
}
