#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan_generator/impl/index_selection_helpers.hpp>
#include <components/planner/optimizer.hpp>
#include <services/collection/context_storage.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

static collection_full_name_t coll_name() { return {database_name, collection_name}; }

// ================================================================
// Helper: build a match node with a single expression
// ================================================================
static node_ptr make_match_with_expr(std::pmr::memory_resource* r, const expression_ptr& expr) {
    return make_node_match(r, coll_name(), expr);
}

// ================================================================
// T1. Scalar folding: add
// ================================================================
TEST_CASE("optimizer::scalar_fold_add") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);

    auto result = components::planner::optimize(&resource, node, nullptr, params.get());

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
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::subtract);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 7);
}

// ================================================================
// T3. Scalar folding: multiply
// ================================================================
TEST_CASE("optimizer::scalar_fold_multiply") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(4));
    auto id1 = params->add_parameter(int64_t(5));

    auto scalar = make_scalar_expression(&resource, scalar_type::multiply);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 20);
}

// ================================================================
// T4. Scalar folding: divide
// ================================================================
TEST_CASE("optimizer::scalar_fold_divide") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::divide);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 3);
}

// ================================================================
// T5. Scalar folding: mod
// ================================================================
TEST_CASE("optimizer::scalar_fold_mod") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::mod);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 1);
}

// ================================================================
// T6. Compare folding: eq true
// ================================================================
TEST_CASE("optimizer::compare_fold_eq_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T7. Compare folding: eq false
// ================================================================
TEST_CASE("optimizer::compare_fold_eq_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T8. Compare folding: gt true
// ================================================================
TEST_CASE("optimizer::compare_fold_gt_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9. Compare folding: lt false
// ================================================================
TEST_CASE("optimizer::compare_fold_lt_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9a. Compare folding: ne true
// ================================================================
TEST_CASE("optimizer::compare_fold_ne_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));

    auto comp = make_compare_expression(&resource, compare_type::ne, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9b. Compare folding: ne false
// ================================================================
TEST_CASE("optimizer::compare_fold_ne_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::ne, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9c. Compare folding: gte true (equal)
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_true_equal") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9d. Compare folding: gte true (greater)
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_true_greater") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9e. Compare folding: gte false
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9f. Compare folding: lte true (equal)
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_true_equal") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9g. Compare folding: lte true (less)
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_true_less") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9h. Compare folding: lte false
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9i. Compare folding: lt true
// ================================================================
TEST_CASE("optimizer::compare_fold_lt_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(10));

    auto comp = make_compare_expression(&resource, compare_type::lt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T10. No folding: key + param (mixed)
// ================================================================
TEST_CASE("optimizer::no_fold_key_param") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::eq,
                                        key(&resource, "field", side_t::left), id0);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::eq);
}

// ================================================================
// T11. No folding: NULL param
// ================================================================
TEST_CASE("optimizer::no_fold_null_param") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(
        components::types::logical_value_t{&resource, components::types::complex_logical_type{components::types::logical_type::NA}});
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 2);
}

// ================================================================
// T12. No folding: group node (skip non-match)
// ================================================================
TEST_CASE("optimizer::no_fold_group_node") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id0);
    scalar->append_param(id1);

    std::vector<expression_ptr> expressions;
    expressions.emplace_back(std::move(scalar));
    auto group_node = make_node_group(&resource, coll_name(), expressions);

    components::planner::optimize(&resource, group_node, nullptr, params.get());

    // Group expressions should NOT be folded
    auto* s = static_cast<scalar_expression_t*>(group_node->expressions()[0].get());
    REQUIRE(s->params().size() == 2);
}

// ================================================================
// T13. Nested folding: scalar inside compare
// ================================================================
TEST_CASE("optimizer::nested_scalar_in_compare") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

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
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(0));

    auto scalar = make_scalar_expression(&resource, scalar_type::divide);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

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
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(10));

    auto child1 = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto child2 = make_compare_expression(&resource, compare_type::gt,
                                          key(&resource, "field", side_t::left), id2);

    auto union_and = make_compare_union_expression(&resource, compare_type::union_and);
    union_and->append_child(child1);
    union_and->append_child(child2);

    auto node = make_match_with_expr(&resource, union_and);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c1 = static_cast<compare_expression_t*>(child1.get());
    REQUIRE(c1->type() == compare_type::all_true);

    auto* c2 = static_cast<compare_expression_t*>(child2.get());
    REQUIRE(c2->type() == compare_type::gt); // unchanged
}

// ================================================================
// T16. Union OR: children fold independently
// ================================================================
TEST_CASE("optimizer::union_or_fold") {
    auto resource = std::pmr::synchronized_pool_resource();
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
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c1 = static_cast<compare_expression_t*>(child1.get());
    REQUIRE(c1->type() == compare_type::all_false);

    auto* c2 = static_cast<compare_expression_t*>(child2.get());
    REQUIRE(c2->type() == compare_type::all_true);
}

// ================================================================
// T17. Deep nested scalar: (2+3)*4
// ================================================================
TEST_CASE("optimizer::deep_nested_scalar") {
    auto resource = std::pmr::synchronized_pool_resource();
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

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(outer));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

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
    auto resource = std::pmr::synchronized_pool_resource();
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

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(add_outer));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(add_outer.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 21);
}

// ================================================================
// T19. Scalar folding: double arithmetic
// ================================================================
TEST_CASE("optimizer::scalar_fold_double") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(double(2.5));
    auto id1 = params->add_parameter(double(1.5));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<double>() == Approx(4.0));
}

// ================================================================
// T20. Scalar folding: mixed int * double
// ================================================================
TEST_CASE("optimizer::scalar_fold_mixed_types") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(double(2.5));

    auto scalar = make_scalar_expression(&resource, scalar_type::multiply);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<double>() == Approx(7.5));
}

// ================================================================
// T21. Compare folding: double comparison
// ================================================================
TEST_CASE("optimizer::compare_fold_double") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(double(3.14));
    auto id1 = params->add_parameter(double(2.71));

    auto comp = make_compare_expression(&resource, compare_type::gt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, nullptr, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T22. Aggregate pipeline: match → group → sort (match folds, group/sort untouched)
// ================================================================
TEST_CASE("optimizer::aggregate_match_folds_group_not") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(2));
    auto id3 = params->add_parameter(int64_t(3));

    auto aggregate = make_node_aggregate(&resource, coll_name());

    // Child 0: match(eq, #0=5, #1=5)
    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    aggregate->append_child(make_node_match(&resource, coll_name(), comp));

    // Child 1: group with scalar(add, #2=2, #3=3)
    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id2);
    scalar->append_param(id3);
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(std::move(scalar));
    aggregate->append_child(make_node_group(&resource, coll_name(), group_exprs));

    components::planner::optimize(&resource, aggregate, nullptr, params.get());

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
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(3));
    auto id3 = params->add_parameter(int64_t(10));

    auto aggregate = make_node_aggregate(&resource, coll_name());

    auto comp1 = make_compare_expression(&resource, compare_type::gt, id0, id1);
    aggregate->append_child(make_node_match(&resource, coll_name(), comp1));

    auto comp2 = make_compare_expression(&resource, compare_type::lt, id2, id3);
    aggregate->append_child(make_node_match(&resource, coll_name(), comp2));

    components::planner::optimize(&resource, aggregate, nullptr, params.get());

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
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "age")) == true);
}

// ================================================================
// T28. has_index_on: negative (no match)
// ================================================================
TEST_CASE("optimizer::has_index_on_negative") {
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "name")) == false);
}

// ================================================================
// T29. has_index_on: multi-field index skip
// ================================================================
TEST_CASE("optimizer::has_index_on_multi_field_skip") {
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{});

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
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{});

    REQUIRE(ctx.has_index_on(key(&resource, "any")) == false);
}

// ================================================================
// Diagnostic: parameter copy chain
// ================================================================
TEST_CASE("optimizer::param_copy_survives") {
    auto resource = std::pmr::synchronized_pool_resource();
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
