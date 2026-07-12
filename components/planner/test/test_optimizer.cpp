#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_cte_scan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_union.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan_generator/impl/create_plan_match.hpp>
#include <components/physical_plan_generator/impl/index_selection_helpers.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/optimizer/rules/hash_join.hpp>
#include <components/planner/optimizer/rules/promote_cross_join.hpp>
#include <components/tests/generaty.hpp>
#include <components/types/types.hpp>
#include <services/collection/context_storage.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include "pushdown_plan_builders.hpp"

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

using planner_test::make_agg_group;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

// ================================================================
// Helper: build a match node with a single expression
// ================================================================
static node_ptr make_match_with_expr(std::pmr::memory_resource* r, const expression_ptr& expr) {
    return make_node_match(r, core::dbname_t{database_name}, core::relname_t{collection_name}, expr);
}

// ================================================================
// T1. Scalar folding: add
// ================================================================
TEST_CASE("optimizer::scalar_fold_add") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);

    auto result = components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    REQUIRE(std::holds_alternative<core::parameter_id_t>(s->params()[0]));
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 5);
}

// ================================================================
// T2. Scalar folding: subtract
// ================================================================
TEST_CASE("optimizer::scalar_fold_subtract") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::subtract);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 7);
}

// ================================================================
// T3. Scalar folding: multiply
// ================================================================
TEST_CASE("optimizer::scalar_fold_multiply") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(4));
    auto id1 = params->add_parameter(int64_t(5));

    auto scalar = make_scalar_expression(&resource, scalar_type::multiply);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 20);
}

// ================================================================
// T4. Scalar folding: divide
// ================================================================
TEST_CASE("optimizer::scalar_fold_divide") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::divide);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 3);
}

// ================================================================
// T5. Scalar folding: mod
// ================================================================
TEST_CASE("optimizer::scalar_fold_mod") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::mod);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 1);
}

// ================================================================
// T6. Compare folding: eq true
// ================================================================
TEST_CASE("optimizer::compare_fold_eq_true") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T7. Compare folding: eq false
// ================================================================
TEST_CASE("optimizer::compare_fold_eq_false") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T8. Compare folding: gt true
// ================================================================
TEST_CASE("optimizer::compare_fold_gt_true") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9. Compare folding: lt false
// ================================================================
TEST_CASE("optimizer::compare_fold_lt_false") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9a. Compare folding: ne true
// ================================================================
TEST_CASE("optimizer::compare_fold_ne_true") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));

    auto comp = make_compare_expression(&resource, compare_type::ne, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9b. Compare folding: ne false
// ================================================================
TEST_CASE("optimizer::compare_fold_ne_false") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::ne, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9c. Compare folding: gte true (equal)
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_true_equal") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9d. Compare folding: gte true (greater)
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_true_greater") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9e. Compare folding: gte false
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_false") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9f. Compare folding: lte true (equal)
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_true_equal") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9g. Compare folding: lte true (less)
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_true_less") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9h. Compare folding: lte false
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_false") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9i. Compare folding: lt true
// ================================================================
TEST_CASE("optimizer::compare_fold_lt_true") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(10));

    auto comp = make_compare_expression(&resource, compare_type::lt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T10. No folding: key + param (mixed)
// ================================================================
TEST_CASE("optimizer::no_fold_key_param") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left), id0);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::eq);
}

// ================================================================
// T11. No folding: NULL param
// ================================================================
TEST_CASE("optimizer::no_fold_null_param") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(components::types::logical_value_t{
        &resource,
        components::types::complex_logical_type{components::types::logical_type::NA}});
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 2);
}

// ================================================================
// T12. No folding: group node (skip non-match)
// ================================================================
TEST_CASE("optimizer::no_fold_group_node") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id0);
    scalar->append_param(id1);

    std::vector<expression_ptr> expressions;
    expressions.emplace_back(std::move(scalar));
    auto group_node =
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, expressions);

    components::planner::optimize(&resource, group_node, params.get());

    // Group expressions should NOT be folded
    auto* s = static_cast<scalar_expression_t*>(group_node->expressions()[0].get());
    REQUIRE(s->params().size() == 2);
}

// ================================================================
// T13. Nested folding: scalar inside compare
// ================================================================
TEST_CASE("optimizer::nested_scalar_in_compare") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    // Scalar should fold to 1 param = 5
    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 5);

    // Compare should stay eq (not folded since one side is key)
    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::eq);
}

// ================================================================
// T14. Division by zero: skip
// ================================================================
TEST_CASE("optimizer::div_by_zero_skip") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(0));

    auto scalar = make_scalar_expression(&resource, scalar_type::divide);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    // Division by zero may fold (returns 0 in otterbrix) or may throw — either is acceptable.
    // The optimizer handles both via try/catch.
    // We just verify no crash occurred and params are in a valid state.
    REQUIRE((s->params().size() == 1 || s->params().size() == 2));
}

// ================================================================
// T15. Union AND: children fold independently
// ================================================================
TEST_CASE("optimizer::union_and_fold") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(10));

    auto child1 = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto child2 = make_compare_expression(&resource, compare_type::gt, key(&resource, "field", side_t::left), id2);

    auto union_and = make_compare_union_expression(&resource, compare_type::union_and);
    union_and->append_child(child1);
    union_and->append_child(child2);

    auto node = make_match_with_expr(&resource, union_and);
    components::planner::optimize(&resource, node, params.get());

    auto* c1 = static_cast<compare_expression_t*>(child1.get());
    REQUIRE(c1->type() == compare_type::all_true);

    auto* c2 = static_cast<compare_expression_t*>(child2.get());
    REQUIRE(c2->type() == compare_type::gt); // unchanged
}

// ================================================================
// T16. Union OR: children fold independently
// ================================================================
TEST_CASE("optimizer::union_or_fold") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));
    auto id2 = params->add_parameter(int64_t(10));
    auto id3 = params->add_parameter(int64_t(3));

    auto child1 = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto child2 = make_compare_expression(&resource, compare_type::gt, id2, id3);

    auto union_or = make_compare_union_expression(&resource, compare_type::union_or);
    union_or->append_child(child1);
    union_or->append_child(child2);

    auto node = make_match_with_expr(&resource, union_or);
    components::planner::optimize(&resource, node, params.get());

    auto* c1 = static_cast<compare_expression_t*>(child1.get());
    REQUIRE(c1->type() == compare_type::all_false);

    auto* c2 = static_cast<compare_expression_t*>(child2.get());
    REQUIRE(c2->type() == compare_type::all_true);
}

// ================================================================
// T17. Deep nested scalar: (2+3)*4
// ================================================================
TEST_CASE("optimizer::deep_nested_scalar") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));
    auto id2 = params->add_parameter(int64_t(4));

    auto inner = make_scalar_expression(&resource, scalar_type::add);
    inner->append_param(id0);
    inner->append_param(id1);

    auto outer = make_scalar_expression(&resource, scalar_type::multiply);
    outer->append_param(expression_ptr(inner));
    outer->append_param(id2);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(outer));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    // Inner folds: 2+3=5, outer folds: 5*4=20
    auto* s = static_cast<scalar_expression_t*>(outer.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 20);
}

// ================================================================
// T18. Triple nested: ((2+3)*4)+1
// ================================================================
TEST_CASE("optimizer::triple_nested_scalar") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));
    auto id2 = params->add_parameter(int64_t(4));
    auto id3 = params->add_parameter(int64_t(1));

    auto add_inner = make_scalar_expression(&resource, scalar_type::add);
    add_inner->append_param(id0);
    add_inner->append_param(id1);

    auto mul_mid = make_scalar_expression(&resource, scalar_type::multiply);
    mul_mid->append_param(expression_ptr(add_inner));
    mul_mid->append_param(id2);

    auto add_outer = make_scalar_expression(&resource, scalar_type::add);
    add_outer->append_param(expression_ptr(mul_mid));
    add_outer->append_param(id3);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(add_outer));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(add_outer.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 21);
}

// ================================================================
// T19. Scalar folding: double arithmetic
// ================================================================
TEST_CASE("optimizer::scalar_fold_double") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(double(2.5));
    auto id1 = params->add_parameter(double(1.5));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<double>() == Catch::Approx(4.0));
}

// ================================================================
// T20. Scalar folding: mixed int * double
// ================================================================
TEST_CASE("optimizer::scalar_fold_mixed_types") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(double(2.5));

    auto scalar = make_scalar_expression(&resource, scalar_type::multiply);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<double>() == Catch::Approx(7.5));
}

// ================================================================
// T21. Compare folding: double comparison
// ================================================================
TEST_CASE("optimizer::compare_fold_double") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(double(3.14));
    auto id1 = params->add_parameter(double(2.71));

    auto comp = make_compare_expression(&resource, compare_type::gt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T22. Aggregate pipeline: match → group → sort (match folds, group/sort untouched)
// ================================================================
TEST_CASE("optimizer::aggregate_match_folds_group_not") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(2));
    auto id3 = params->add_parameter(int64_t(3));

    auto aggregate = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});

    // Child 0: match(eq, #0=5, #1=5)
    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp));

    // Child 1: group with scalar(add, #2=2, #3=3)
    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id2);
    scalar->append_param(id3);
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(std::move(scalar));
    aggregate->append_child(
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, group_exprs));

    components::planner::optimize(&resource, aggregate, params.get());

    // Match should fold to all_true
    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);

    // Group scalar should NOT fold (stays 2 params)
    auto* gs = static_cast<scalar_expression_t*>(aggregate->children()[1]->expressions()[0].get());
    REQUIRE(gs->params().size() == 2);
}

// ================================================================
// T23. Multiple match nodes in aggregate
// ================================================================
TEST_CASE("optimizer::multiple_match_nodes") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(3));
    auto id3 = params->add_parameter(int64_t(10));

    auto aggregate = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});

    auto comp1 = make_compare_expression(&resource, compare_type::gt, id0, id1);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp1));

    auto comp2 = make_compare_expression(&resource, compare_type::lt, id2, id3);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp2));

    components::planner::optimize(&resource, aggregate, params.get());

    auto* c1 = static_cast<compare_expression_t*>(comp1.get());
    REQUIRE(c1->type() == compare_type::all_true);

    auto* c2 = static_cast<compare_expression_t*>(comp2.get());
    REQUIRE(c2->type() == compare_type::all_true);
}

// ================================================================
// T24. mirror_compare: lt ↔ gt
// ================================================================
TEST_CASE("optimizer::mirror_compare_lt_gt") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::lt) == compare_type::gt);
    REQUIRE(mirror_compare(compare_type::gt) == compare_type::lt);
}

// ================================================================
// T25. mirror_compare: lte ↔ gte
// ================================================================
TEST_CASE("optimizer::mirror_compare_lte_gte") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::lte) == compare_type::gte);
    REQUIRE(mirror_compare(compare_type::gte) == compare_type::lte);
}

// ================================================================
// T26. mirror_compare: eq/ne symmetric
// ================================================================
TEST_CASE("optimizer::mirror_compare_symmetric") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::eq) == compare_type::eq);
    REQUIRE(mirror_compare(compare_type::ne) == compare_type::ne);
}

// ================================================================
// T27. has_index_on: positive (single-field)
// ================================================================
TEST_CASE("optimizer::has_index_on_positive") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "age")) == true);
}

// ================================================================
// T28. has_index_on: negative (no match)
// ================================================================
TEST_CASE("optimizer::has_index_on_negative") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "name")) == false);
}

// ================================================================
// T29. has_index_on: multi-field index skip
// ================================================================
TEST_CASE("optimizer::has_index_on_multi_field_skip") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "a"));
    keys.push_back(key(&resource, "b"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "a")) == false);
}

// ================================================================
// T30. has_index_on: empty indexed_keys
// ================================================================
TEST_CASE("optimizer::has_index_on_empty") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    REQUIRE(ctx.has_index_on(key(&resource, "any")) == false);
}

// ================================================================
// Diagnostic: parameter copy chain
// ================================================================
TEST_CASE("optimizer::param_copy_survives") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    // Overwrite id0 with 5 (like optimizer does)
    params->set_parameter(id0, components::types::logical_value_t(&resource, int64_t(5)));
    REQUIRE(params->parameter(id0).value<int64_t>() == 5);

    // take_parameters (like dispatcher does)
    auto taken = params->take_parameters();
    REQUIRE(taken.parameters.count(id0) == 1);
    REQUIRE(taken.parameters.at(id0).value<int64_t>() == 5);
    REQUIRE(taken.parameters.count(id1) == 1);

    // Copy (like actor message chain does)
    storage_parameters copy1 = taken;
    REQUIRE(copy1.parameters.count(id0) == 1);
    REQUIRE(copy1.parameters.at(id0).value<int64_t>() == 5);

    // Another copy
    storage_parameters copy2 = copy1;
    REQUIRE(copy2.parameters.count(id0) == 1);
    REQUIRE(copy2.parameters.at(id0).value<int64_t>() == 5);

    // Copy via move
    storage_parameters moved = std::move(copy2);
    REQUIRE(moved.parameters.count(id0) == 1);
    REQUIRE(moved.parameters.at(id0).value<int64_t>() == 5);
}

static services::context_storage_t make_context_with_oid(std::pmr::memory_resource* resource,
                                                         components::catalog::oid_t oid,
                                                         const components::logical_plan::storage_parameters* params) {
    services::context_storage_t ctx(resource, log_t{}, {});
    ctx.known_oids.insert(oid);
    ctx.parameters = params;
    return ctx;
}

static services::context_storage_t make_context_with_oid(std::pmr::memory_resource* resource,
                                                         components::catalog::oid_t oid,
                                                         const components::logical_plan::parameter_node_t* params) {
    return make_context_with_oid(resource, oid, params ? &params->parameters() : nullptr);
}

static void add_single_field_index(services::context_storage_t& ctx,
                                   std::pmr::memory_resource* resource,
                                   const char* field,
                                   components::logical_plan::index_type type) {
    components::logical_plan::keys_base_storage_t keys(resource);
    keys.push_back(key(resource, field));
    ctx.indexed_keys.push_back(keys);

    components::index::index_description_t desc{
        components::logical_plan::keys_base_storage_t(resource),
        type,
    };
    desc.keys.push_back(key(resource, field));
    ctx.indexed_descriptions.push_back(std::move(desc));
}

TEST_CASE("create_plan_match::eq_uses_index_scan_hashed_preferred") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(42));
    constexpr auto table_oid = components::catalog::oid_t{777};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::hashed);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), pid));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::index_scan);
    auto* scan = static_cast<components::operators::index_scan*>(op.get());
    REQUIRE(scan->compare_type() == compare_type::eq);
    REQUIRE(scan->preferred_index_type() == components::logical_plan::index_type::hashed);
}

TEST_CASE("create_plan_match::range_uses_index_scan_single_preferred") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{778};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::single);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::gte, key(&resource, "age"), pid));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::index_scan);
    auto* scan = static_cast<components::operators::index_scan*>(op.get());
    REQUIRE(scan->compare_type() == compare_type::gte);
    REQUIRE(scan->preferred_index_type() == components::logical_plan::index_type::single);
}

TEST_CASE("create_plan_match::range_with_only_hashed_falls_back_to_full_scan") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{779};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::hashed);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::gt, key(&resource, "age"), pid));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::full_scan);
}

TEST_CASE("create_plan_match::key_on_right_mirrors_compare_type_for_index_scan") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{780};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::single);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::lt, pid, key(&resource, "age")));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::index_scan);
    auto* scan = static_cast<components::operators::index_scan*>(op.get());
    REQUIRE(scan->compare_type() == compare_type::gt);
}

TEST_CASE("create_plan_match::union_compare_uses_full_scan") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{781};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::single);

    auto union_expr = make_compare_union_expression(&resource, compare_type::union_and);
    union_expr->append_child(make_compare_expression(&resource, compare_type::gte, key(&resource, "age"), pid));

    auto node = make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, union_expr);
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::full_scan);
}

// ================================================================
// pushdown_aggregate rule — stamps node_group_t::pushdown() on
// single-owned-table, fragment-mergeable aggregate sub-plans.
// Driven through optimize() with can_push_to_agent=true (the hard
// capability precondition — an owning agent must be reachable); the
// rule itself is total (no-op on non-match).
// ================================================================
namespace {
    constexpr auto pushable_oid = components::catalog::oid_t{4242};

    // aggregate(pushable_oid) -> group: the shared make_agg bound to this suite's
    // pushable table oid.
    static node_aggregate_ptr make_agg(std::pmr::memory_resource* r, const node_group_ptr& group) {
        return planner_test::make_agg(r, group, pushable_oid);
    }

    static bool run_and_get_pushdown(std::pmr::memory_resource* r, const node_ptr& plan, bool enable) {
        auto params = make_parameter_node(r);
        auto root = components::planner::optimize(r, plan, params.get(), enable);
        // find the group child of the aggregate root to read its flag
        for (const auto& child : root->children()) {
            if (child && child->type() == node_type::group_t) {
                return static_cast<node_group_t*>(child.get())->pushdown();
            }
        }
        return false;
    }
} // namespace

TEST_CASE("optimizer::pushdown_aggregate::scalar_mergeable_is_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == true);
}

TEST_CASE("optimizer::pushdown_aggregate::grouped_mergeable_is_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/true, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == true);
}

TEST_CASE("optimizer::pushdown_aggregate::no_agent_capability_does_not_stamp") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // Capability precondition (NOT a rollout flag): can_push_to_agent==false means
    // there is no reachable owning agent (disk-less/in-memory mode), so optimize()
    // never calls the rule and nothing is stamped.
    REQUIRE(run_and_get_pushdown(&resource, agg, /*can_push=*/false) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::count_distinct_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/true);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::having_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto having = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "h"));
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false, expression_ptr(having));
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::join_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // A join sibling means this is not one owned base table => skip (a).
    agg->append_child(make_node_join(&resource,
                                     core::dbname_t{database_name},
                                     core::relname_t{collection_name},
                                     join_type::inner));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::nested_aggregate_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // A nested aggregate child => not a single owned base table => skip (a).
    auto nested = make_node_aggregate(&resource,
                                      core::dbname_t{database_name},
                                      core::relname_t{collection_name});
    agg->append_child(std::move(nested));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::union_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // A union sibling => multi-source, not one owned base table => skip (a).
    agg->append_child(make_node_union(&resource,
                                      make_node_cte_scan(&resource, std::pmr::string("l")),
                                      make_node_cte_scan(&resource, std::pmr::string("r")),
                                      /*all=*/false));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::cte_scan_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // A cte_scan sibling => multi-source, not one owned base table => skip (a).
    agg->append_child(make_node_cte_scan(&resource, std::pmr::string("cte")));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::non_mergeable_kind_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    // A non-whitelisted aggregate kind (no fragment-merge kernel) must stay
    // coordinator-side => skip (b), has_unmergeable_aggregate.
    std::vector<expression_ptr> exprs;
    auto agg_expr = make_aggregate_expression(&resource, "stddev", key(&resource, "s"));
    agg_expr->append_param(key(&resource, "v"));
    exprs.push_back(expression_ptr(agg_expr));
    auto group =
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs, nullptr);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::mergeable_capability_gates_stamp") {
    // The pushdown rule now reads the RESOLVED fragment-merge capability
    // (aggregate_expression::is_mergeable(), stamped at validate from
    // function::is_mergeable()) instead of a hardcoded name list.
    // A mergeable builtin SUM over one owned table IS stamped; the SAME SUM made
    // DISTINCT must stay coordinator-side.
    auto resource = core::pmr::otterbrix_resource();
    {
        std::vector<expression_ptr> exprs;
        auto sum = make_aggregate_expression(&resource, "sum", key(&resource, "s"));
        sum->append_param(key(&resource, "v"));
        sum->set_mergeable(true);
        exprs.push_back(expression_ptr(sum));
        auto group = make_node_group(&resource,
                                     core::dbname_t{database_name},
                                     core::relname_t{collection_name},
                                     exprs,
                                     nullptr);
        auto agg = make_agg(&resource, group);
        REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == true);
    }
    {
        std::vector<expression_ptr> exprs;
        auto sum = make_aggregate_expression(&resource, "sum", key(&resource, "s"));
        sum->append_param(key(&resource, "v"));
        sum->set_mergeable(true);
        sum->set_distinct(true);
        exprs.push_back(expression_ptr(sum));
        auto group = make_node_group(&resource,
                                     core::dbname_t{database_name},
                                     core::relname_t{collection_name},
                                     exprs,
                                     nullptr);
        auto agg = make_agg(&resource, group);
        REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
    }
}

TEST_CASE("optimizer::pushdown_aggregate::udf_reference_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    // A shape-/kind-pushable fragment, but an aggregate arg references a UDF
    // (function_uid >= DEFAULT_FUNCTIONS.size()). The owning agent rebuilds its
    // registry with builtins only, so the pushed fragment could not resolve it
    // => skip (e), subtree_references_udf.
    std::vector<expression_ptr> exprs;
    auto sum = make_aggregate_expression(&resource, "sum", key(&resource, "s"));
    // SUM itself is mergeable; the skip must come from the UDF gate, not from the
    // mergeability check — so stamp mergeable to reach subtree_references_udf.
    sum->set_mergeable(true);
    auto udf = make_function_expression(&resource, std::string("my_udf"));
    udf->add_function_uid(components::compute::DEFAULT_FUNCTIONS.size());
    sum->append_param(expression_ptr(udf));
    exprs.push_back(expression_ptr(sum));
    auto group =
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs, nullptr);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

// ================================================================
// Cross->inner promotion (promote_cross_join.cpp)
//
// SELECT SUM(ap) FROM a, b WHERE ak = bk AND ap < ?
// lowers to:
//   aggregate_t[ join{cross}(scan_a{ak,ap}, scan_b{bk}) + all_true,
//                match{ union_and[ eq(ak, bk), lt(ap, ?) ] },
//                group{ SUM(ap) } ].
//
// The comma-join uses UNQUALIFIED column names, so validate_schema stamps BOTH
// equi keys side=left over the merged [ak, ap, bk] schema (the same_schema path)
// and stamps output_types() on the scan children. Because both keys are side=left,
// detect_equi_columns cannot accept the cross join as-is.
//
// promote_cross_joins classifies the equi keys by PATH RANGE against the intact
// stamped scans (left_width = scan_a.output_types().size() == 2): ak -> merged
// idx 0 (left range), bk -> merged idx 2 (right range). It moves the eq onto a
// fresh INNER join, re-localizes + re-sides the right-range key (merged 2 ->
// right-local 0, side=right), and keeps the non-join lt filter as the residual
// match. rewrite_hash_joins (run AFTER, as optimize() orders it) then lowers the
// promoted inner join to algo()==hash.
//
// The scans are driven through the REAL validate_schema (never hand-stamped) so
// key.side()/key.path()/output_types() are exactly what the SQL pipeline produces
// (make_agg 5-arg would stamp the wrapper, not the scans).
// ================================================================
namespace {
    // A raw BIGINT scan, left UNstamped on purpose: validate_schema derives and
    // stamps its output_types() from the data chunk (the test must not hand-stamp).
    static node_ptr make_promote_scan(std::pmr::memory_resource* r, std::initializer_list<const char*> cols) {
        std::pmr::vector<components::types::complex_logical_type> types(r);
        for (const char* name : cols) {
            types.emplace_back(components::types::logical_type::BIGINT, name);
        }
        auto chunk = gen_data_chunk(/*size=*/1, /*start=*/0, types, r);
        return make_node_raw_data(r, std::move(chunk));
    }
} // namespace

TEST_CASE("optimizer::promote_cross_join::comma_join_becomes_inner_hash") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto lt_param = params->add_parameter(int64_t(5));

    // Distinct column names => each unqualified reference resolves to exactly one
    // merged column (the validator rejects ambiguous duplicates), which is what
    // makes the promote rule's path-range classification well-defined.
    auto scan_a = make_promote_scan(&resource, {"ak", "ap"});
    auto scan_b = make_promote_scan(&resource, {"bk"});

    auto join =
        make_node_join(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, join_type::cross);
    join->append_child(scan_a);
    join->append_child(scan_b);
    // FROM a, b lowers the join with an all_true ON placeholder.
    join->append_expression(make_compare_expression(&resource, compare_type::all_true));

    // WHERE ak = bk AND ap < ?  (unqualified keys => both stamped side=left)
    auto eq = make_compare_expression(&resource, compare_type::eq, key(&resource, "ak"), key(&resource, "bk"));
    auto lt = make_compare_expression(&resource, compare_type::lt, key(&resource, "ap"), lt_param);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(eq);
    where->append_child(lt);

    auto outer = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
    outer->append_child(join);
    outer->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, where));

    // group { SUM(ap) }
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_ap"));
    sum_expr->append_param(key(&resource, "ap"));
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(expression_ptr(sum_expr));
    outer->append_child(
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, group_exprs));

    // Drive the REAL validator: stamps key.side()/key.path() and output_types().
    auto validated = services::dispatcher::validate_schema(&resource, nullptr, outer.get(), params->parameters());
    REQUIRE_FALSE(validated.has_error());
    // Precondition the promote rule relies on: the scans carry their columns in
    // output_types() (left_width == 2, right_width == 1).
    REQUIRE(scan_a->output_types().size() == 2);
    REQUIRE(scan_b->output_types().size() == 1);

    // Rule under test, then the hash-selection that runs after it in optimize().
    node_ptr out = components::planner::optimizer::promote_cross_joins(&resource, outer);
    out = components::planner::optimizer::rewrite_hash_joins(&resource, out);

    REQUIRE(out == outer);
    auto* agg = static_cast<node_aggregate_t*>(out.get());
    REQUIRE(agg->children().size() == 3);

    // child[0] is now an INNER hash join with the detected equi columns.
    REQUIRE(agg->children()[0]->type() == node_type::join_t);
    auto* jn = static_cast<node_join_t*>(agg->children()[0].get());
    REQUIRE(jn->type() == join_type::inner);
    REQUIRE(jn->algo() == node_join_t::join_algo::hash);
    REQUIRE(jn->left_col() == 0);  // ak, left input col 0
    REQUIRE(jn->right_col() == 0); // bk, right input col 0

    // The equi moved onto the join's ON; the right-range key is re-localized
    // (merged idx 2 -> right-local 0) and re-sided (side=right).
    REQUIRE(jn->expressions().size() == 1);
    auto* on = static_cast<compare_expression_t*>(jn->expressions()[0].get());
    REQUIRE(on->type() == compare_type::eq);
    REQUIRE(is_key(on->left()));
    REQUIRE(is_key(on->right()));
    const auto& lk = as_key(on->left());
    const auto& rk = as_key(on->right());
    REQUIRE(lk.side() == side_t::left);
    REQUIRE(lk.path().size() == 1);
    REQUIRE(lk.path()[0] == 0);
    REQUIRE(rk.side() == side_t::right);
    REQUIRE(rk.path().size() == 1);
    REQUIRE(rk.path()[0] == 0);

    // The residual match holds ONLY the non-join lt filter.
    REQUIRE(agg->children()[1]->type() == node_type::match_t);
    auto residual_match = agg->children()[1];
    REQUIRE(residual_match->expressions().size() == 1);
    auto* residual = static_cast<compare_expression_t*>(residual_match->expressions()[0].get());
    REQUIRE(residual->type() == compare_type::lt);

    // The group{SUM} pipeline stage is left untouched by the promotion.
    REQUIRE(agg->children()[2]->type() == node_type::group_t);
}

// ================================================================
// NOT folding: union_not over a fully folded single child must fold
// to the complementary constant. Without this, `WHERE NOT (1=2)`
// survived folding and reached filter construction, whose all_false /
// key-shape guards were Release-erased asserts (crash / bad variant
// access), and `WHERE NOT (1=1)` produced a spurious error.
// ================================================================
TEST_CASE("optimizer::not_fold_all_false_child") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(1));
    auto id1 = params->add_parameter(int64_t(2));

    auto comp = make_compare_union_expression(&resource, compare_type::union_not);
    comp->append_child(make_compare_expression(&resource, compare_type::eq, id0, id1));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
    REQUIRE(c->children().empty());
}

TEST_CASE("optimizer::not_fold_all_true_child") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(1));
    auto id1 = params->add_parameter(int64_t(1));

    auto comp = make_compare_union_expression(&resource, compare_type::union_not);
    comp->append_child(make_compare_expression(&resource, compare_type::eq, id0, id1));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
    REQUIRE(c->children().empty());
}
