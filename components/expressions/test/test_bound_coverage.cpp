// ============================================================================
// TOTAL COVERAGE OF WHAT REACHES operator_match — DEMONSTRATED, NOT ASSERTED.
//
// Rule 6 forbids a fallback, so before operator_match can be moved onto the bound
// layer the binder must bind EVERYTHING the planner can send it. What it sends is
// exactly the complement of create_plan_match_::is_pure_compare, which pushes a
// predicate into the scan when (and only when) it is:
//     (A) `column OP constant`
//     (B) `column OP column`, plain comparison only
//     (C) `f(column...) OP constant`, UDF-free
// and recursively so through a union AND/OR/NOT. Anything else lands on the filter.
//
// This file enumerates that complement and binds ONE TREE PER SHAPE. Each case
// names the reason the shape is not pushable, so the list can be checked against
// is_pure_compare by reading rather than by trusting.
//
// The enumeration was also taken empirically: instrumenting operator_match to print
// a structural signature of every expression it receives, over the whole 611-case
// integration suite, produced 31 distinct signatures. Every one of them is a case
// below. Two facts from that census shaped the work: every key arrived with a
// single resolved ordinal (so nested paths and '::?' are latent, not live), and
// all_true / all_false arrived 48 times with a NULL expression in both operand
// slots -- which is why they are answered before any operand is inspected.
// ============================================================================

#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/bound/binder.hpp>
#include <components/expressions/bound/expression_executor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <core/operations_helper.hpp>
#include <memory_resource>
#include <string_view>

using namespace components;
using namespace components::expressions;
using types::complex_logical_type;
using types::logical_type;
using ekey = components::expressions::key_t;

namespace {

    constexpr compute::function_uid kLengthUid = 6; // builtin `length`: row function, BIGINT out

    struct fixture_t {
        explicit fixture_t(std::pmr::memory_resource* resource)
            : registry(resource)
            , schema(resource)
            , parameters(logical_plan::make_parameter_node(resource))
            , binder(resource) {
            compute::register_default_functions(registry);
            schema.add("a", complex_logical_type{logical_type::BIGINT});   // 0
            schema.add("b", complex_logical_type{logical_type::BIGINT});   // 1
            schema.add("s", complex_logical_type{logical_type::STRING_LITERAL}); // 2
            schema.add("d", complex_logical_type{logical_type::DOUBLE});   // 3
            schema.add("t", complex_logical_type{logical_type::TIME});     // 4
            schema.add("ts", complex_logical_type{logical_type::TIMESTAMP}); // 5
        }

        binder_context_t context() {
            binder_context_t ctx{};
            ctx.left = &schema;
            ctx.functions = &registry;
            ctx.parameters = &parameters->parameters();
            return ctx;
        }

        compute::function_registry_t registry;
        bind_schema_t schema;
        logical_plan::parameter_node_ptr parameters;
        binder_t binder;
    };

    // A key with its ordinals already stamped in, which is the state EVERY key reaching an operator
    // is in: validate_logical_plan resolves the name and writes the path.
    ekey resolved(std::pmr::memory_resource* resource, const char* name, std::initializer_list<size_t> ordinals) {
        ekey key{resource, name};
        std::pmr::vector<size_t> path{resource};
        for (auto ordinal : ordinals) {
            path.push_back(ordinal);
        }
        key.set_path(std::move(path));
        key.set_side(side_t::left);
        return key;
    }

    // A chunk shaped like the fixture schema's first three columns, so a key resolved against that
    // schema addresses the same ordinal in the chunk. A bound reference is POSITIONAL: handing it a
    // chunk of a different width is exactly the mismatch the executor is meant to name.
    vector::data_chunk_t schema_shaped_chunk(std::pmr::memory_resource* resource, uint64_t capacity) {
        std::pmr::vector<complex_logical_type> types{resource};
        types.emplace_back(logical_type::BIGINT);         // 0: a
        types.emplace_back(logical_type::BIGINT);         // 1: b
        types.emplace_back(logical_type::STRING_LITERAL); // 2: s
        return vector::data_chunk_t{resource, types, capacity};
    }

} // namespace

// ---------------------------------------------------------------------------
// (1) A top-level FUNCTION expression. expression_group::function is rejected by
//     is_pure_compare's very first line, so `WHERE some_predicate_fn(x)` always
//     lands on the filter.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_top_level_function_expression") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};

    std::pmr::vector<param_storage> args{&resource};
    args.emplace_back(resolved(&resource, "s", {2}));
    auto expression = make_function_expression(&resource, "length", std::move(args));
    expression->add_function_uid(kLengthUid);

    auto bound = fixture.binder.bind(expression, fixture.context());
    REQUIRE_FALSE(bound.has_error());
    CHECK(bound.value()->kind() == bound_kind::function);
    CHECK(bound.value()->return_type().type() == logical_type::BIGINT);
}

// ---------------------------------------------------------------------------
// (2) all_true / all_false. Neither operand slot is a key, so col_op_const's
//     `is_key(left) != is_key(right)` is false and the shape is not pushable.
//     Both bind to a CONSTANT -- they read nothing, so they are foldable and the
//     executor evaluates them once in create() rather than per chunk.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_constant_predicates") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};

    for (const auto op : {compare_type::all_true, compare_type::all_false}) {
        // Built by the one-argument factory, so BOTH operand slots hold a null expression -- the
        // state the census found 48 times, and the reason this shape is decided before any operand
        // is looked at.
        auto expression = make_compare_expression(&resource, op);
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::constant);
        CHECK(bound.value()->traits().foldable);
        const auto* node = static_cast<const bound_constant_t*>(bound.value().get());
        CHECK(node->value().value<bool>() == (op == compare_type::all_true));

        auto executor = expression_executor_t::create(&resource, bound.value(), 4);
        REQUIRE_FALSE(executor.has_error());
        CHECK(executor.value().folded_node_count() == 1); // evaluated in create(), never again

        std::pmr::vector<complex_logical_type> types{&resource};
        types.emplace_back(logical_type::BIGINT);
        vector::data_chunk_t chunk(&resource, types, 4);
        chunk.set_cardinality(3);
        expression_executor_t::context_t execution;
        vector::indexing_vector_t selection(&resource, uint64_t{4});
        auto selected = executor.value().select(chunk, 3, execution, selection);
        REQUIRE_FALSE(selected.has_error());
        CHECK(selected.value() == (op == compare_type::all_true ? 3u : 0u));
    }
}

// ---------------------------------------------------------------------------
// (3) Comparison operand pairings that (A)/(B)/(C) do not cover.
//     - expr OP column : (C) needs a CONSTANT on the other side, not a column
//     - expr OP expr   : neither side is a constant
//     - param OP param : no key at all, so (A) and (B) both fail
//     - column OP column under a non-plain operator is covered by (5)/(7)
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_unpushable_operand_pairings") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};
    fixture.parameters->add_parameter(core::parameter_id_t{0}, static_cast<int64_t>(7));
    fixture.parameters->add_parameter(core::parameter_id_t{1}, static_cast<int64_t>(9));

    auto arithmetic = [&](const char* name, size_t ordinal) {
        auto expression = make_scalar_expression(&resource, scalar_type::add);
        expression->append_param(resolved(&resource, name, {ordinal}));
        expression->append_param(core::parameter_id_t{0});
        return expression;
    };

    INFO("expr OP column");
    {
        auto expression =
            make_compare_expression(&resource, compare_type::lte, arithmetic("a", 0), resolved(&resource, "b", {1}));
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        CHECK(bound.value()->children().front()->kind() == bound_kind::arithmetic);
        CHECK(bound.value()->children().back()->kind() == bound_kind::reference);
    }

    INFO("expr OP expr");
    {
        auto expression =
            make_compare_expression(&resource, compare_type::gt, arithmetic("a", 0), arithmetic("b", 1));
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        CHECK(bound.value()->children().front()->kind() == bound_kind::arithmetic);
        CHECK(bound.value()->children().back()->kind() == bound_kind::arithmetic);
    }

    INFO("param OP param -- a correlated sub-query compare, and the census found 10 of them");
    {
        auto expression = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  core::parameter_id_t{0},
                                                  core::parameter_id_t{1});
        expression->make_unfoldable();
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        CHECK(bound.value()->children().front()->kind() == bound_kind::parameter);
        CHECK(bound.value()->children().back()->kind() == bound_kind::parameter);
        // Both operands are live slots, so the tree must NOT fold -- a correlated sub-query
        // refills them per outer row.
        CHECK_FALSE(bound.value()->traits().foldable);
    }

    INFO("function expression as a comparison OPERAND");
    {
        std::pmr::vector<param_storage> args{&resource};
        args.emplace_back(resolved(&resource, "s", {2}));
        auto call = make_function_expression(&resource, "length", std::move(args));
        call->add_function_uid(kLengthUid);
        auto expression =
            make_compare_expression(&resource, compare_type::eq, expression_ptr{call}, core::parameter_id_t{0});
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        CHECK(bound.value()->children().front()->kind() == bound_kind::function);
    }
}

// ---------------------------------------------------------------------------
// (4) IS NULL / IS NOT NULL, and unions over a non-pushable child. A union is
//     pushable only if EVERY descendant is, so one unpushable leaf keeps the
//     whole tree on the filter.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_null_tests_and_unions") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};
    fixture.parameters->add_parameter(core::parameter_id_t{0}, static_cast<int64_t>(7));

    INFO("IS NULL / IS NOT NULL");
    for (const auto op : {compare_type::is_null, compare_type::is_not_null}) {
        auto expression = make_compare_expression(&resource, op, resolved(&resource, "a", {0}), param_storage{});
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        // A null test ANSWERS about a null, so it is the one comparison that does not propagate one.
        CHECK_FALSE(bound.value()->traits().propagates_nulls);
    }

    INFO("AND / OR over an unpushable child");
    for (const auto op : {compare_type::union_and, compare_type::union_or}) {
        auto pushable =
            make_compare_expression(&resource, compare_type::eq, resolved(&resource, "a", {0}), core::parameter_id_t{0});
        auto arithmetic = make_scalar_expression(&resource, scalar_type::multiply);
        arithmetic->append_param(resolved(&resource, "a", {0}));
        arithmetic->append_param(core::parameter_id_t{0});
        auto unpushable =
            make_compare_expression(&resource, compare_type::gt, arithmetic, resolved(&resource, "b", {1}));
        auto tree = make_compare_union_expression(&resource, op);
        tree->append_child(pushable);
        tree->append_child(unpushable);

        auto bound = fixture.binder.bind(tree, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::conjunction);
        CHECK(bound.value()->children().size() == 2);
    }

    INFO("NOT");
    {
        auto inner =
            make_compare_expression(&resource, compare_type::eq, resolved(&resource, "a", {0}), core::parameter_id_t{0});
        auto tree = make_compare_union_expression(&resource, compare_type::union_not);
        tree->append_child(inner);
        auto bound = fixture.binder.bind(tree, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        CHECK(bound.value()->kind() == bound_kind::conjunction);
    }
}

// ---------------------------------------------------------------------------
// (5) regex. A regex is not a `plain_cmp`, so (B) never accepts it: `col LIKE col`
//     lands on the filter, and so does a regex whose subject is an expression.
//     The pattern decides which of the two nodes is built, and a fixed pattern is
//     COMPILED at bind time -- the move that deletes regex_predicate.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_regex_shapes") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};
    fixture.parameters->add_parameter(core::parameter_id_t{0}, std::pmr::string{"^ab.*", &resource});
    fixture.parameters->add_parameter(core::parameter_id_t{1},
                                      types::logical_value_t{&resource, complex_logical_type{logical_type::NA}});
    fixture.parameters->add_parameter(core::parameter_id_t{2}, std::pmr::string{"(unclosed", &resource});
    fixture.parameters->add_parameter(core::parameter_id_t{3}, static_cast<int64_t>(5));

    auto bind_regex_against = [&](param_storage pattern) {
        auto expression =
            make_compare_expression(&resource, compare_type::regex, resolved(&resource, "s", {2}), pattern);
        return fixture.binder.bind(expression, fixture.context());
    };

    INFO("fixed pattern -> COMPILED once, here");
    {
        auto bound = bind_regex_against(core::parameter_id_t{0});
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::regex);
        const auto* node = static_cast<const bound_regex_t*>(bound.value().get());
        REQUIRE(node->regex_mode() == bound_regex_t::mode::compiled);
        REQUIRE(node->compiled() != nullptr);
        // Compiled at BIND time: the object is already usable, with no chunk in sight.
        CHECK(node->compiled()->match("abcdef"));
        CHECK_FALSE(node->compiled()->match("zzz"));
    }

    INFO("NULL pattern -> `x LIKE NULL` is UNKNOWN, so it selects nothing");
    {
        auto bound = bind_regex_against(core::parameter_id_t{1});
        REQUIRE_FALSE(bound.has_error());
        const auto* node = static_cast<const bound_regex_t*>(bound.value().get());
        CHECK(node->regex_mode() == bound_regex_t::mode::null_pattern);

        auto chunk = schema_shaped_chunk(&resource, 2);
        chunk.set_value(2, uint64_t{0}, std::string_view{"abc"});
        chunk.set_cardinality(1);
        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        vector::indexing_vector_t selection(&resource, uint64_t{2});
        auto selected = executor.value().select(chunk, 1, execution, selection);
        INFO("select error: " << (selected.has_error() ? std::string(selected.error().what) : std::string("none")));
        REQUIRE_FALSE(selected.has_error());
        CHECK(selected.value() == 0);
    }

    INFO("a pattern RE2 refuses -> a STORED error, answered at evaluation, never a throw");
    {
        auto bound = bind_regex_against(core::parameter_id_t{2});
        REQUIRE_FALSE(bound.has_error());
        const auto* node = static_cast<const bound_regex_t*>(bound.value().get());
        REQUIRE(node->regex_mode() == bound_regex_t::mode::failed);
        REQUIRE(node->failure().has_value());
    }

    INFO("a non-string pattern -> also a stored error, never a payload read as a string");
    {
        auto bound = bind_regex_against(core::parameter_id_t{3});
        REQUIRE_FALSE(bound.has_error());
        const auto* node = static_cast<const bound_regex_t*>(bound.value().get());
        REQUIRE(node->regex_mode() == bound_regex_t::mode::failed);
        REQUIRE(node->failure().has_value());
        CHECK(node->failure()->type == core::error_code_t::comparison_failure);
    }

    INFO("a COLUMN pattern varies per row -> dynamic");
    {
        auto bound = bind_regex_against(resolved(&resource, "s", {2}));
        REQUIRE_FALSE(bound.has_error());
        const auto* node = static_cast<const bound_regex_t*>(bound.value().get());
        CHECK(node->regex_mode() == bound_regex_t::mode::dynamic);
    }
}

// A compiled regex actually filters, and its 3VL is the WHERE rule: a NULL subject is UNKNOWN and
// does not select.
TEST_CASE("components::expressions::bound::coverage_regex_selects_with_three_valued_logic") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};
    fixture.parameters->add_parameter(core::parameter_id_t{0}, std::pmr::string{"^match", &resource});

    auto expression = make_compare_expression(&resource,
                                              compare_type::regex,
                                              resolved(&resource, "s", {2}),
                                              core::parameter_id_t{0});
    auto bound = fixture.binder.bind(expression, fixture.context());
    REQUIRE_FALSE(bound.has_error());

    auto chunk = schema_shaped_chunk(&resource, 4);
    chunk.set_value(2, uint64_t{0}, std::string_view{"match_me"});
    chunk.set_value(2, uint64_t{1}, std::string_view{"other"});
    chunk.set_value(2, uint64_t{2}, std::string_view{"matchless"});
    chunk.data[2].validity().set(3, false); // NULL subject
    chunk.set_cardinality(4);

    auto executor = expression_executor_t::create(&resource, bound.value(), 4);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    vector::indexing_vector_t selection(&resource, uint64_t{4});
    auto selected = executor.value().select(chunk, 4, execution, selection);
    INFO("select error: " << (selected.has_error() ? std::string(selected.error().what) : std::string("none")));
    REQUIRE_FALSE(selected.has_error());
    REQUIRE(selected.value() == 2);
    CHECK(selection.get_index(0) == 0);
    CHECK(selection.get_index(1) == 2);
}

// ---------------------------------------------------------------------------
// (6) ANY / ALL. The transformer marks these do_not_fold, which is is_pure_compare's
//     second rejection, so they always land on the filter. The array is a slot read
//     LIVE, and the fold is three-valued.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_any_all_shapes") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};

    // The sub-query result: an array VALUE in a parameter slot.
    std::vector<types::logical_value_t> elements;
    elements.emplace_back(&resource, static_cast<int64_t>(2));
    elements.emplace_back(&resource, static_cast<int64_t>(4));
    fixture.parameters->add_parameter(
        core::parameter_id_t{0},
        types::logical_value_t::create_array(&resource, complex_logical_type{logical_type::BIGINT}, elements));

    auto expression = make_compare_expression(&resource,
                                              compare_type::any,
                                              resolved(&resource, "a", {0}),
                                              core::parameter_id_t{0});
    expression->set_inner_op(compare_type::eq);
    expression->make_unfoldable();

    auto bound = fixture.binder.bind(expression, fixture.context());
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->kind() == bound_kind::any_all);
    const auto* node = static_cast<const bound_any_all_t*>(bound.value().get());
    CHECK(node->is_any());
    CHECK(node->inner_op() == compare_type::eq);
    // The array is a live slot, so nothing here folds.
    CHECK_FALSE(bound.value()->traits().foldable);

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, types, 6);
    for (uint64_t row = 0; row < 5; ++row) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
    }
    chunk.data[0].validity().set(5, false); // NULL subject -> UNKNOWN, does not select
    chunk.set_cardinality(6);

    auto executor = expression_executor_t::create(&resource, bound.value(), 6);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    execution.parameters = &fixture.parameters->parameters();
    vector::indexing_vector_t selection(&resource, uint64_t{6});
    auto selected = executor.value().select(chunk, 6, execution, selection);
    REQUIRE_FALSE(selected.has_error());
    // a IN (2, 4): rows 2 and 4. The NULL row does not sneak in.
    REQUIRE(selected.value() == 2);
    CHECK(selection.get_index(0) == 2);
    CHECK(selection.get_index(1) == 4);
}

// `x <> ALL(empty)` is TRUE and `x = ANY(empty)` is FALSE -- a TOTAL answer, so it holds even for
// a NULL subject. compact_to_array_value answers the NA-null sentinel for a zero-row sub-query,
// which is why this is decided before the subject is read.
TEST_CASE("components::expressions::bound::coverage_any_all_over_an_empty_subquery") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};
    fixture.parameters->add_parameter(core::parameter_id_t{0},
                                      types::logical_value_t{&resource, complex_logical_type{logical_type::NA}});

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.data[0].validity().set(1, false); // even a NULL subject gets the total answer
    chunk.set_cardinality(2);

    for (const auto kind : {compare_type::any, compare_type::all}) {
        auto expression =
            make_compare_expression(&resource, kind, resolved(&resource, "a", {0}), core::parameter_id_t{0});
        expression->set_inner_op(kind == compare_type::any ? compare_type::eq : compare_type::ne);
        expression->make_unfoldable();
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());

        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        execution.parameters = &fixture.parameters->parameters();
        vector::indexing_vector_t selection(&resource, uint64_t{2});
        auto selected = executor.value().select(chunk, 2, execution, selection);
        REQUIRE_FALSE(selected.has_error());
        CHECK(selected.value() == (kind == compare_type::any ? 0u : 2u));
    }
}

// A NULL element makes a MISSING match UNKNOWN rather than a definite answer, for ANY and ALL
// alike -- so the row drops either way and a NOT above cannot resurrect it.
TEST_CASE("components::expressions::bound::coverage_any_all_null_element_is_unknown") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};

    std::vector<types::logical_value_t> elements;
    elements.emplace_back(&resource, static_cast<int64_t>(2));
    elements.emplace_back(&resource, complex_logical_type{logical_type::NA}); // a NULL element
    fixture.parameters->add_parameter(
        core::parameter_id_t{0},
        types::logical_value_t::create_array(&resource, complex_logical_type{logical_type::BIGINT}, elements));

    auto expression = make_compare_expression(&resource,
                                              compare_type::any,
                                              resolved(&resource, "a", {0}),
                                              core::parameter_id_t{0});
    expression->set_inner_op(compare_type::eq);
    expression->make_unfoldable();
    auto bound = fixture.binder.bind(expression, fixture.context());
    REQUIRE_FALSE(bound.has_error());

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(2)); // hits -> definite TRUE
    chunk.set_value(0, uint64_t{1}, static_cast<int64_t>(9)); // misses, but a NULL element was seen
    chunk.set_cardinality(2);

    auto executor = expression_executor_t::create(&resource, bound.value(), 2);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    execution.parameters = &fixture.parameters->parameters();
    vector::indexing_vector_t selection(&resource, uint64_t{2});
    auto selected = executor.value().select(chunk, 2, execution, selection);
    REQUIRE_FALSE(selected.has_error());
    REQUIRE(selected.value() == 1);
    CHECK(selection.get_index(0) == 0);
}

// ---------------------------------------------------------------------------
// (7) Key decorations. A '::' cast becomes part of the tree; a '::?' variant
//     selection does NOT -- it picked a column, it did not convert a value, and
//     that pick is already baked into the resolved path.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_key_cast_and_variant_selection") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};

    INFO("'::' cast -> a cast node over the reference");
    {
        auto key = resolved(&resource, "a", {0});
        key.set_cast_type(complex_logical_type{logical_type::DOUBLE});
        auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::cast);
        CHECK(bound.value()->return_type().type() == logical_type::DOUBLE);
        CHECK(bound.value()->children().front()->kind() == bound_kind::reference);
    }

    INFO("'::?' variant selection -> a bare reference, at the ordinal validation already chose");
    {
        auto key = resolved(&resource, "d", {3});
        key.set_cast_type(complex_logical_type{logical_type::DOUBLE});
        key.set_variant_select(true);
        auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::reference);
        CHECK(static_cast<const bound_reference_t*>(bound.value().get())->column_index() == 3);
    }
}

// ---------------------------------------------------------------------------
// (8) Mixed operand types. A NUMERIC pair is made comparable at BIND time by
//     promoting both to their common type; anything else keeps both operands and
//     is answered by the promoting arm, which converts per row because the
//     conversion is semantic and value-dependent.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_mixed_type_comparisons") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};

    INFO("BIGINT vs DOUBLE -> both promoted at bind time, no promoting arm needed");
    {
        auto expression = make_compare_expression(&resource,
                                                  compare_type::lt,
                                                  resolved(&resource, "a", {0}),
                                                  resolved(&resource, "d", {3}));
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        CHECK_FALSE(static_cast<const bound_comparison_t*>(bound.value().get())->promoting());
        // The narrower side gained a cast; both operands now index as one C++ type.
        CHECK(bound.value()->children().front()->kind() == bound_kind::cast);
    }

    INFO("TIME vs TIMESTAMP -> the promoting arm, with both operands left alone");
    {
        auto expression = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  resolved(&resource, "t", {4}),
                                                  resolved(&resource, "ts", {5}));
        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::comparison);
        CHECK(static_cast<const bound_comparison_t*>(bound.value().get())->promoting());
        CHECK(bound.value()->children().front()->kind() == bound_kind::reference);
        CHECK(bound.value()->children().back()->kind() == bound_kind::reference);
    }

    INFO("BOOLEAN vs a non-boolean numeric is REFUSED, and at bind time");
    {
        bind_schema_t schema{&resource};
        schema.add("flag", complex_logical_type{logical_type::BOOLEAN});
        binder_context_t context{};
        context.left = &schema;
        context.parameters = &fixture.parameters->parameters();
        fixture.parameters->add_parameter(core::parameter_id_t{9}, static_cast<int64_t>(1));

        auto expression = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  resolved(&resource, "flag", {0}),
                                                  core::parameter_id_t{9});
        auto bound = fixture.binder.bind(expression, context);
        REQUIRE(bound.has_error());
        // The same rejection the boxed comparator made per row, decided once from the types.
        CHECK(bound.error().type == core::error_code_t::sql_parse_error);
    }
}

// ---------------------------------------------------------------------------
// (9) A nested key path. Latent rather than live -- the census found every key
//     reaching operator_match resolving to a single ordinal -- but binding it
//     means the layer refuses nothing the boxed value-getter accepted.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_nested_key_path") {
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<complex_logical_type> fields{&resource};
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_field_name("inner");
    auto struct_type = complex_logical_type::create_struct("outer", fields);

    bind_schema_t schema{&resource};
    schema.add("outer", struct_type);
    binder_context_t context{};
    context.left = &schema;

    binder_t binder{&resource};
    auto key = resolved(&resource, "outer", {0, 0});
    auto expression = make_scalar_expression(&resource, scalar_type::get_field, key);
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->kind() == bound_kind::reference);
    const auto* node = static_cast<const bound_reference_t*>(bound.value().get());
    CHECK(node->is_nested());
    CHECK(node->path().size() == 2);
    // The LEAF type, walked down the address -- not the STRUCT the address starts at.
    CHECK(node->return_type().type() == logical_type::BIGINT);
}

// ---------------------------------------------------------------------------
// (10) COALESCE. Not a comparison shape at all -- it is the encoding
//      select_column_t / group_key_t already carry as `coalesce_entries`, and the
//      node exists so those structs can be BOUND rather than reversed back into
//      parsed expressions. It branches on the VALIDITY of the value it is about to
//      return, which is why it is not a CASE in disguise, and it is the one node
//      that deliberately does NOT propagate nulls: absorbing them is its job.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_coalesce_takes_the_first_non_null") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("a", complex_logical_type{logical_type::BIGINT});
    schema.add("b", complex_logical_type{logical_type::BIGINT});
    binder_context_t context{};
    context.left = &schema;
    binder_t binder{&resource};

    std::pmr::vector<bound_expression_ptr> operands{&resource};
    for (size_t ordinal : {size_t{0}, size_t{1}}) {
        std::pmr::vector<size_t> path{&resource};
        path.push_back(ordinal);
        auto bound = binder.bind_column_path(path, side_t::left, context);
        REQUIRE_FALSE(bound.has_error());
        operands.push_back(std::move(bound.value()));
    }
    // A trailing CONSTANT, the shape coalesce_entries carries as source::constant.
    operands.push_back(bound_expression_ptr{
        make_bound_constant(&resource, types::logical_value_t{&resource, static_cast<int64_t>(-1)})});

    auto bound = make_bound_coalesce(&resource, complex_logical_type{logical_type::BIGINT}, std::move(operands));
    REQUIRE_FALSE(bound.has_error());
    REQUIRE(bound.value()->kind() == bound_kind::coalesce);
    // The point of the node: a NULL operand is the REASON to look further, not a reason to answer
    // NULL, so it must not claim to propagate nulls.
    CHECK_FALSE(bound.value()->traits().propagates_nulls);

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, types, 4);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(7)); // a valid   -> 7
    chunk.set_value(1, uint64_t{0}, static_cast<int64_t>(9));
    chunk.data[0].validity().set(1, false);                   // a NULL, b valid -> 8
    chunk.set_value(1, uint64_t{1}, static_cast<int64_t>(8));
    chunk.data[0].validity().set(2, false);                   // both NULL -> the constant, -1
    chunk.data[1].validity().set(2, false);
    chunk.set_cardinality(3);

    auto executor = expression_executor_t::create(&resource, bound.value(), 4);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    auto produced = executor.value().execute(chunk, 3, execution);
    REQUIRE_FALSE(produced.has_error());
    CHECK(produced.value()->get_value<int64_t>(0) == 7);
    CHECK(produced.value()->get_value<int64_t>(1) == 8);
    CHECK(produced.value()->get_value<int64_t>(2) == -1);
}

// Every operand NULL and no constant tail: the answer is NULL, in the validity mask.
TEST_CASE("components::expressions::bound::coverage_coalesce_is_null_when_every_operand_is") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("a", complex_logical_type{logical_type::BIGINT});
    binder_context_t context{};
    context.left = &schema;
    binder_t binder{&resource};

    std::pmr::vector<size_t> path{&resource};
    path.push_back(0);
    auto reference = binder.bind_column_path(path, side_t::left, context);
    REQUIRE_FALSE(reference.has_error());
    std::pmr::vector<bound_expression_ptr> operands{&resource};
    operands.push_back(std::move(reference.value()));

    auto bound = make_bound_coalesce(&resource, complex_logical_type{logical_type::BIGINT}, std::move(operands));
    REQUIRE_FALSE(bound.has_error());

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.data[0].validity().set(0, false);
    chunk.set_cardinality(1);

    auto executor = expression_executor_t::create(&resource, bound.value(), 2);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    auto produced = executor.value().execute(chunk, 1, execution);
    REQUIRE_FALSE(produced.has_error());
    CHECK_FALSE(produced.value()->validity().row_is_valid(0));
}

// An operand whose type contradicts the plan-resolved result type is REFUSED at bind time. Copying
// it into the result slot would read raw bytes at the wrong width -- the same class of misread the
// typed layer exists to stop.
TEST_CASE("components::expressions::bound::coverage_coalesce_refuses_a_contradicting_operand") {
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<bound_expression_ptr> operands{&resource};
    operands.push_back(bound_expression_ptr{
        make_bound_constant(&resource, types::logical_value_t{&resource, static_cast<int64_t>(1)})});

    auto bound = make_bound_coalesce(&resource, complex_logical_type{logical_type::FLOAT}, std::move(operands));
    REQUIRE(bound.has_error());
    CHECK(bound.error().type == core::error_code_t::schema_error);
}

// ---------------------------------------------------------------------------
// (11) DIVISION BY ZERO IS TWO DIFFERENT ANSWERS, AND THEY MUST NOT COLLAPSE.
//
// The engine pins both, and they disagree with each other on purpose:
//   * a SCALAR divisor that is zero  -> a query ERROR   (test_arithmetic.cpp:1089,
//     `SELECT count / 0`, commented "PostgreSQL behavior")
//   * a COLUMN divisor holding zero  -> that row is NULL (test_null_semantics_matrix.cpp:296,
//     `SELECT 10 / x`, commented "no SIGFPE")
//
// evaluate_arithmetic got this from WHICH BRANCH it took: resolve_operand answers a
// scalar for a parameter and a vector for a column, the scalar branches test the
// divisor and error, the vector-vector kernel (arithmetic.cpp:88) sets the row NULL.
// A bound tree materialises a parameter into a full vector, so without the flag under
// test here EVERY divide takes the vector path and `count / 0` silently becomes NULL.
//
// This case fails if the two ever collapse into one answer, in either direction.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_division_by_zero_scalar_errors_column_nulls") {
    std::pmr::monotonic_buffer_resource resource;
    fixture_t fixture{&resource};
    fixture.parameters->add_parameter(core::parameter_id_t{0}, static_cast<int64_t>(0)); // the literal 0

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT); // 0: a
    types.emplace_back(logical_type::BIGINT); // 1: b
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(10));
    chunk.set_value(1, uint64_t{0}, static_cast<int64_t>(0)); // a column that HOLDS zero
    chunk.set_cardinality(1);

    INFO("a SCALAR zero divisor is a query error");
    {
        auto expression = make_scalar_expression(&resource, scalar_type::divide);
        expression->append_param(resolved(&resource, "a", {0}));
        expression->append_param(core::parameter_id_t{0});

        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        execution.parameters = &fixture.parameters->parameters();
        auto produced = executor.value().execute(chunk, 1, execution);
        REQUIRE(produced.has_error());
        CHECK(produced.error().type == core::error_code_t::arithmetics_failure);
    }

    INFO("a COLUMN divisor holding zero NULLs that row, and does not error");
    {
        auto expression = make_scalar_expression(&resource, scalar_type::divide);
        expression->append_param(resolved(&resource, "a", {0}));
        expression->append_param(resolved(&resource, "b", {1}));

        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        execution.parameters = &fixture.parameters->parameters();
        auto produced = executor.value().execute(chunk, 1, execution);
        REQUIRE_FALSE(produced.has_error());
        CHECK_FALSE(produced.value()->validity().row_is_valid(0));
    }

    INFO("a non-zero scalar divisor still divides");
    {
        fixture.parameters->add_parameter(core::parameter_id_t{1}, static_cast<int64_t>(5));
        auto expression = make_scalar_expression(&resource, scalar_type::divide);
        expression->append_param(resolved(&resource, "a", {0}));
        expression->append_param(core::parameter_id_t{1});

        auto bound = fixture.binder.bind(expression, fixture.context());
        REQUIRE_FALSE(bound.has_error());
        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        execution.parameters = &fixture.parameters->parameters();
        auto produced = executor.value().execute(chunk, 1, execution);
        REQUIRE_FALSE(produced.has_error());
        CHECK(produced.value()->get_value<int64_t>(0) == 2);
    }
}

// ---------------------------------------------------------------------------
// (12) UNARY MINUS PRESERVES THE OPERAND'S TYPE.
//
// This is why it is a node and not the rewrite `0 - x`. The rewrite needs a zero
// CONSTANT, that constant has a type of its own, and arithmetic_result_type then
// widens the pair -- a BIGINT zero against a FLOAT column answers DOUBLE. The
// engine pins the opposite: `SELECT -r` over a REAL column stays FLOAT
// (test_float_arithmetic.cpp:81). compute_unary_neg writes the operand's own
// width, so a node that wraps it cannot widen; a rewrite could, silently.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_unary_minus_keeps_the_operand_type") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("r", complex_logical_type{logical_type::FLOAT});
    schema.add("i", complex_logical_type{logical_type::BIGINT});
    binder_context_t context{};
    context.left = &schema;
    binder_t binder{&resource};

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::FLOAT);
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.set_value(0, uint64_t{0}, 2.5F);
    chunk.set_value(1, uint64_t{0}, static_cast<int64_t>(7));
    chunk.set_cardinality(1);

    INFO("FLOAT stays FLOAT -- the widening the `0 - x` rewrite would have introduced");
    {
        auto expression = make_scalar_expression(&resource, scalar_type::unary_minus);
        expression->append_param(resolved(&resource, "r", {0}));
        auto bound = binder.bind(expression, context);
        REQUIRE_FALSE(bound.has_error());
        REQUIRE(bound.value()->kind() == bound_kind::negate);
        REQUIRE(bound.value()->return_type().type() == logical_type::FLOAT);

        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        auto produced = executor.value().execute(chunk, 1, execution);
        REQUIRE_FALSE(produced.has_error());
        // The DELIVERED vector's type, not only the promise.
        CHECK(produced.value()->type().type() == logical_type::FLOAT);
        CHECK(core::is_equals(produced.value()->get_value<float>(0), -2.5F));
    }

    INFO("BIGINT stays BIGINT");
    {
        auto expression = make_scalar_expression(&resource, scalar_type::unary_minus);
        expression->append_param(resolved(&resource, "i", {1}));
        auto bound = binder.bind(expression, context);
        REQUIRE_FALSE(bound.has_error());
        CHECK(bound.value()->return_type().type() == logical_type::BIGINT);

        auto executor = expression_executor_t::create(&resource, bound.value(), 2);
        REQUIRE_FALSE(executor.has_error());
        expression_executor_t::context_t execution;
        auto produced = executor.value().execute(chunk, 1, execution);
        REQUIRE_FALSE(produced.has_error());
        CHECK(produced.value()->type().type() == logical_type::BIGINT);
        CHECK(produced.value()->get_value<int64_t>(0) == -7);
    }

    INFO("a non-numeric operand is refused rather than negated as raw bytes");
    {
        bind_schema_t text{&resource};
        text.add("s", complex_logical_type{logical_type::STRING_LITERAL});
        binder_context_t text_context{};
        text_context.left = &text;
        auto expression = make_scalar_expression(&resource, scalar_type::unary_minus);
        expression->append_param(resolved(&resource, "s", {0}));
        auto bound = binder.bind(expression, text_context);
        REQUIRE(bound.has_error());
        CHECK(bound.error().type == core::error_code_t::arithmetics_failure);
    }
}

// ---------------------------------------------------------------------------
// (13) THE JOIN SHAPE: one left row broadcast against N right rows.
//
// A filter compares row k of one chunk against row k of the same chunk. A JOIN
// compares ONE probe row against every row of the build side -- which the boxed
// layer spelled batch_check_1vN, broadcasting the left index through an indexing
// vector. Without context_t::left_row a two-input bound tree could only express
// the filter shape, and a join predicate would silently compare row k to row k.
// ---------------------------------------------------------------------------
TEST_CASE("components::expressions::bound::coverage_left_row_broadcasts_against_the_right_chunk") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t left_schema{&resource};
    left_schema.add("k", complex_logical_type{logical_type::BIGINT});
    bind_schema_t right_schema{&resource};
    right_schema.add("k", complex_logical_type{logical_type::BIGINT});

    binder_context_t context{};
    context.left = &left_schema;
    context.right = &right_schema;
    binder_t binder{&resource};

    // left.k = right.k, with the sides explicitly stamped.
    auto left_key = resolved(&resource, "k", {0});
    auto right_key = resolved(&resource, "k", {0});
    right_key.set_side(side_t::right);
    auto expression = make_compare_expression(&resource, compare_type::eq, left_key, right_key);
    auto bound = binder.bind(expression, context);
    REQUIRE_FALSE(bound.has_error());

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);

    // Probe side: rows 0..2 hold 10, 20, 30.
    vector::data_chunk_t probe(&resource, types, 4);
    for (uint64_t row = 0; row < 3; ++row) {
        probe.set_value(0, row, static_cast<int64_t>((row + 1) * 10));
    }
    probe.set_cardinality(3);

    // Build side: 30, 20, 20, 99.
    vector::data_chunk_t build(&resource, types, 4);
    const int64_t build_values[] = {30, 20, 20, 99};
    for (uint64_t row = 0; row < 4; ++row) {
        build.set_value(0, row, build_values[row]);
    }
    build.set_cardinality(4);

    auto executor = expression_executor_t::create(&resource, bound.value(), 4);
    REQUIRE_FALSE(executor.has_error());

    auto matches_for = [&](uint64_t probe_row) {
        expression_executor_t::context_t execution;
        execution.right_input = &build;
        execution.left_row = probe_row; // THE broadcast
        vector::indexing_vector_t selection(&resource, uint64_t{4});
        auto selected = executor.value().select(probe, 4, execution, selection);
        REQUIRE_FALSE(selected.has_error());
        std::vector<uint64_t> rows;
        for (uint64_t i = 0; i < selected.value(); ++i) {
            rows.push_back(selection.get_index(i));
        }
        return rows;
    };

    // probe row 0 (10) matches nothing; row 1 (20) matches build rows 1 and 2; row 2 (30) matches 0.
    CHECK(matches_for(0).empty());
    CHECK(matches_for(1) == std::vector<uint64_t>{1, 2});
    CHECK(matches_for(2) == std::vector<uint64_t>{0});

    // Without the broadcast the same tree is the FILTER shape -- row k against row k. Probe row 0
    // (10) vs build row 0 (30) is false, row 1 (20) vs 20 is true, row 2 (30) vs 20 is false.
    // Pinned so the two shapes cannot be confused for one another.
    {
        expression_executor_t::context_t execution;
        execution.right_input = &build;
        vector::indexing_vector_t selection(&resource, uint64_t{4});
        auto selected = executor.value().select(probe, 3, execution, selection);
        REQUIRE_FALSE(selected.has_error());
        REQUIRE(selected.value() == 1);
        CHECK(selection.get_index(0) == 1);
    }
}

// A NULL in the broadcast row makes every pair UNKNOWN, and UNKNOWN does not select -- the join
// rule (a NULL join key matches nothing), carried by the same 3VL the filter uses.
TEST_CASE("components::expressions::bound::coverage_broadcast_null_matches_nothing") {
    std::pmr::monotonic_buffer_resource resource;

    bind_schema_t schema{&resource};
    schema.add("k", complex_logical_type{logical_type::BIGINT});
    binder_context_t context{};
    context.left = &schema;
    context.right = &schema;
    binder_t binder{&resource};

    auto left_key = resolved(&resource, "k", {0});
    auto right_key = resolved(&resource, "k", {0});
    right_key.set_side(side_t::right);
    auto bound =
        binder.bind(make_compare_expression(&resource, compare_type::eq, left_key, right_key), context);
    REQUIRE_FALSE(bound.has_error());

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    vector::data_chunk_t probe(&resource, types, 2);
    probe.data[0].validity().set(0, false); // the probe key is NULL
    probe.set_cardinality(1);
    vector::data_chunk_t build(&resource, types, 2);
    build.set_value(0, uint64_t{0}, static_cast<int64_t>(7));
    build.data[0].validity().set(1, false); // and one build key is NULL too
    build.set_cardinality(2);

    auto executor = expression_executor_t::create(&resource, bound.value(), 2);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t execution;
    execution.right_input = &build;
    execution.left_row = 0;
    vector::indexing_vector_t selection(&resource, uint64_t{2});
    auto selected = executor.value().select(probe, 2, execution, selection);
    REQUIRE_FALSE(selected.has_error());
    CHECK(selected.value() == 0);
}
