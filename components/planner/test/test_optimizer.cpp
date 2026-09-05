#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <components/compute/function.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_cte_scan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_union.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan_generator/impl/create_plan_match.hpp>
#include <components/physical_plan_generator/impl/index_selection_helpers.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/optimizer/rules/drop_redundant_distinct.hpp>
#include <components/planner/optimizer/rules/eager_aggregation.hpp>
#include <components/planner/optimizer/rules/hash_join.hpp>
#include <components/planner/optimizer/rules/promote_cross_join.hpp>
#include <components/planner/optimizer/rules/pushdown_filter.hpp>
#include <components/tests/generaty.hpp>
#include <components/types/types.hpp>
#include <services/collection/context_storage.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include "pushdown_plan_builders.hpp"

namespace {
    // The resolver takes the cast registry unconditionally; these tests validate no DML node.
    // No (void)-cast to silence an unused local (rule 14): the registration runs
    // inside the static's own initializer, so there is nothing left over to ignore.
    const components::casts::cast_registry_t* test_cast_registry() {
        static const components::casts::cast_registry_t& registry = []() -> components::casts::cast_registry_t& {
            static components::casts::cast_registry_t r{std::pmr::new_delete_resource()};
            components::casts::register_default_casts(r);
            return r;
        }();
        return &registry;
    }

    services::dispatcher::validation::validation_context_t
    test_validation_context(std::pmr::memory_resource* resource) {
        static const components::graph_execution_context execution_context{};
        static components::compute::function_registry_t& functions = [] () -> components::compute::function_registry_t& {
            static components::compute::function_registry_t f{std::pmr::new_delete_resource()};
            components::compute::register_default_functions(f);
            return f;
        }();
        return {resource, nullptr, *test_cast_registry(), functions, execution_context};
    }
} // namespace

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
    constexpr auto table_oid = components::catalog::oid_t{701};

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.index_info_slot(table_oid).keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "age")) == true);
}

// ================================================================
// T28. has_index_on: negative (no match)
// ================================================================
TEST_CASE("optimizer::has_index_on_negative") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});
    constexpr auto table_oid = components::catalog::oid_t{702};

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.index_info_slot(table_oid).keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "name")) == false);
}

// ================================================================
// T29. has_index_on: multi-field index skip
// ================================================================
TEST_CASE("optimizer::has_index_on_multi_field_skip") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});
    constexpr auto table_oid = components::catalog::oid_t{703};

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "a"));
    keys.push_back(key(&resource, "b"));
    ctx.index_info_slot(table_oid).keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "a")) == false);
}

// ================================================================
// T30. has_index_on: no table_indexes entry for the oid
// ================================================================
TEST_CASE("optimizer::has_index_on_empty") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});
    constexpr auto table_oid = components::catalog::oid_t{704};

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "any")) == false);
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
                                   components::catalog::oid_t table_oid,
                                   const char* field,
                                   components::logical_plan::index_type type) {
    auto& info = ctx.index_info_slot(table_oid);
    components::logical_plan::keys_base_storage_t keys(resource);
    keys.push_back(key(resource, field));
    info.keys.push_back(keys);

    components::index::index_description_t desc{
        components::logical_plan::keys_base_storage_t(resource),
        type,
    };
    desc.keys.push_back(key(resource, field));
    info.descriptions.push_back(std::move(desc));
}

TEST_CASE("create_plan_match::eq_uses_index_scan_hashed_preferred") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(42));
    constexpr auto table_oid = components::catalog::oid_t{777};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, table_oid, "age", components::logical_plan::index_type::hashed);

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
    add_single_field_index(ctx, &resource, table_oid, "age", components::logical_plan::index_type::single);

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
    add_single_field_index(ctx, &resource, table_oid, "age", components::logical_plan::index_type::hashed);

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
    add_single_field_index(ctx, &resource, table_oid, "age", components::logical_plan::index_type::single);

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
    add_single_field_index(ctx, &resource, table_oid, "age", components::logical_plan::index_type::single);

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
        auto root = components::planner::optimize(r, plan, params.get(), nullptr, enable);
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
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    // HAVING is a node_having_t child of the AGGREGATE (not carried in the group); the
    // pushdown_aggregate gate scans the aggregate's children for it and skips.
    auto agg = planner_test::make_agg(&resource, group, pushable_oid, expression_ptr(having));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::join_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // A join sibling means this is not one owned base table => skip (a).
    agg->append_child(
        make_node_join(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, join_type::inner));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::nested_aggregate_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    // A nested aggregate child => not a single owned base table => skip (a).
    auto nested = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
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
    auto group = make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
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
        auto group = make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
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
        auto group = make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
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
    auto group = make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
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
    auto validated =
        services::dispatcher::validate_schema(test_validation_context(&resource), outer.get(), params->parameters());
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

// ================================================================
// Column pruning (column_pruning.cpp) — optimize() populates
// node_aggregate_t::projected_cols() so downstream scans read only the
// referenced storage columns. The rule is UNGATED (a projection hint,
// valid in-memory too), so it is exercised through the 3-arg optimize()
// with no owning agent (can_push_to_agent defaults false).
//
// Plans are built with paths pre-stamped by hand (validate_schema would
// stamp key.path()[0] to the storage column index at runtime); the rule
// reads those paths directly.
// ================================================================
namespace {
    using components::catalog::oid_t;
    constexpr auto prune_db = "database";
    constexpr auto prune_rel = "collection";

    core::dbname_t pdb() { return core::dbname_t{std::string{prune_db}}; }
    core::relname_t prel() { return core::relname_t{std::string{prune_rel}}; }

    // A key naming storage column `name`, with path()[0] pre-stamped to `idx` and an
    // explicit join side.
    key pruned_key(std::pmr::memory_resource* r, const char* name, size_t idx, side_t side = side_t::undefined) {
        key k(r, name, side);
        std::pmr::vector<size_t> p{r};
        p.push_back(idx);
        k.set_path(std::move(p));
        return k;
    }

    // A SELECT-list get_field projection scalar for storage column `name` at `idx`
    // (unaliased — key IS the input field, carrying the storage path).
    expression_ptr proj_get_field(std::pmr::memory_resource* r, const char* name, size_t idx) {
        return expression_ptr(make_scalar_expression(r, scalar_type::get_field, pruned_key(r, name, idx)));
    }

    // aggregate(oid) with a $select projection child (the plain-SELECT shape — no $group).
    node_aggregate_ptr make_select_agg(std::pmr::memory_resource* r, oid_t oid, const node_select_ptr& sel) {
        auto agg = make_node_aggregate(r, pdb(), prel());
        agg->set_table_oid(oid);
        agg->append_child(sel);
        return agg;
    }

    // A resolved table entry advertising `ncols` columns for `oid`, so
    // column_pruning's collect_table_md learns the per-side column count for JOIN splits.
    void add_resolved_table(std::pmr::memory_resource* r,
                            components::logical_plan::catalog_resolves_t& resolves,
                            oid_t oid,
                            const std::string& relname,
                            size_t ncols) {
        components::logical_plan::resolve_entry_t entry;
        entry.dbname = static_cast<const std::string&>(pdb());
        entry.relname = relname;
        components::logical_plan::resolved_table_metadata_t md;
        md.table_oid = oid;
        md.relkind = 'r';
        md.columns.resize(ncols);
        entry.table_md = std::move(md);
        resolves.ensure(r, components::logical_plan::resolve_kind::table).add(std::move(entry));
    }
} // namespace

TEST_CASE("optimizer::column_pruning::plain_select_projects_single_column") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    // SELECT a FROM t  (t has a=0, b=1, c=2)
    auto sel = make_node_select(&resource, pdb(), prel());
    sel->append_expression(proj_get_field(&resource, "a", 0));
    auto agg = make_select_agg(&resource, oid_t{9100}, sel);

    auto root = components::planner::optimize(&resource, agg, params.get());
    auto* a = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(a->projected_cols() == std::vector<size_t>{0});
}

TEST_CASE("optimizer::column_pruning::plain_select_two_columns") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    // SELECT a, c FROM t
    auto sel = make_node_select(&resource, pdb(), prel());
    sel->append_expression(proj_get_field(&resource, "a", 0));
    sel->append_expression(proj_get_field(&resource, "c", 2));
    auto agg = make_select_agg(&resource, oid_t{9101}, sel);

    auto root = components::planner::optimize(&resource, agg, params.get());
    auto* a = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(a->projected_cols() == (std::vector<size_t>{0, 2}));
}

TEST_CASE("optimizer::column_pruning::where_column_included_even_if_not_selected") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(5));
    // SELECT a FROM t WHERE b > 5  — projection must include b (referenced by WHERE).
    auto sel = make_node_select(&resource, pdb(), prel());
    sel->append_expression(proj_get_field(&resource, "a", 0));
    auto match =
        make_node_match(&resource,
                        pdb(),
                        prel(),
                        make_compare_expression(&resource, compare_type::gt, pruned_key(&resource, "b", 1), pid));
    auto agg = make_node_aggregate(&resource, pdb(), prel());
    agg->set_table_oid(oid_t{9102});
    agg->append_child(sel);
    agg->append_child(match);

    auto root = components::planner::optimize(&resource, agg, params.get());
    auto* a = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(a->projected_cols() == (std::vector<size_t>{0, 1}));
}

TEST_CASE("optimizer::column_pruning::select_star_disables_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    // SELECT * FROM t  — a star_expand projection must leave projected_cols empty (read all).
    auto sel = make_node_select(&resource, pdb(), prel());
    sel->append_expression(expression_ptr(make_scalar_expression(&resource, scalar_type::star_expand, key{&resource})));
    auto agg = make_select_agg(&resource, oid_t{9103}, sel);

    auto root = components::planner::optimize(&resource, agg, params.get());
    auto* a = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(a->projected_cols().empty());
}

TEST_CASE("optimizer::column_pruning::select_star_with_where_reads_all") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(5));
    // SELECT * FROM t WHERE a > 5  — no projection enumerator ⇒ read all columns.
    auto match =
        make_node_match(&resource,
                        pdb(),
                        prel(),
                        make_compare_expression(&resource, compare_type::gt, pruned_key(&resource, "a", 0), pid));
    auto agg = make_node_aggregate(&resource, pdb(), prel());
    agg->set_table_oid(oid_t{9104});
    agg->append_child(match);

    auto root = components::planner::optimize(&resource, agg, params.get());
    auto* a = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(a->projected_cols().empty());
}

TEST_CASE("optimizer::column_pruning::group_by_projects_key_and_agg_arg") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    // SELECT k, SUM(x) FROM t GROUP BY k  (t has k=0, y=1, x=2) → projection {0, 2}.
    std::vector<expression_ptr> group_exprs;
    group_exprs.push_back(
        expression_ptr(make_scalar_expression(&resource, scalar_type::group_field, pruned_key(&resource, "k", 0))));
    auto sum = make_aggregate_expression(&resource, "sum", key(&resource, "sum_x"));
    sum->append_param(pruned_key(&resource, "x", 2));
    group_exprs.push_back(expression_ptr(sum));
    auto group = make_node_group(&resource, pdb(), prel(), group_exprs);

    // Grouped queries also carry a $select over the group OUTPUT columns; the rule must
    // ignore it (its paths are output indices, not storage indices).
    auto sel = make_node_select(&resource, pdb(), prel());
    sel->append_expression(proj_get_field(&resource, "k", 0));
    sel->append_expression(proj_get_field(&resource, "sum_x", 1));

    auto agg = make_node_aggregate(&resource, pdb(), prel());
    agg->set_table_oid(oid_t{9105});
    agg->append_child(group);
    agg->append_child(sel);

    auto root = components::planner::optimize(&resource, agg, params.get());
    auto* a = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(a->projected_cols() == (std::vector<size_t>{0, 2}));
}

TEST_CASE("optimizer::column_pruning::inner_join_splits_columns_per_side") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    // SELECT t1.a FROM t1 JOIN t2 ON t1.k = t2.k
    //   t1 = (a=0, k=1), t2 = (k=0, m=1); merged schema [t1.a, t1.k, t2.k, t2.m].
    // Expect: t1 pruned to {a, k} = {0, 1}; t2 pruned to {k} = {0}.
    constexpr auto oid1 = oid_t{9110};
    constexpr auto oid2 = oid_t{9111};

    auto agg_t1 = make_node_aggregate(&resource, pdb(), prel());
    agg_t1->set_table_oid(oid1);
    auto agg_t2 = make_node_aggregate(&resource, pdb(), prel());
    agg_t2->set_table_oid(oid2);

    auto join = make_node_join(&resource, pdb(), prel(), join_type::inner);
    join->append_child(agg_t1);
    join->append_child(agg_t2);
    // ON t1.k (left, local idx 1) = t2.k (right, local idx 0)
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k", 0, side_t::right)));

    // SELECT t1.a → merged index 0
    auto sel = make_node_select(&resource, pdb(), prel());
    sel->append_expression(proj_get_field(&resource, "a", 0));

    auto parent = make_node_aggregate(&resource, pdb(), prel());
    parent->append_child(join);
    parent->append_child(sel);

    components::logical_plan::catalog_resolves_t resolves;
    add_resolved_table(&resource, resolves, oid1, "t1", 2);
    add_resolved_table(&resource, resolves, oid2, "t2", 2);

    components::planner::optimize(&resource,
                                  boost::static_pointer_cast<components::logical_plan::node_t>(parent),
                                  params.get(),
                                  &resolves);

    REQUIRE(agg_t1->projected_cols() == (std::vector<size_t>{0, 1}));
    REQUIRE(agg_t2->projected_cols() == std::vector<size_t>{0});
}

// ================================================================
// Filter pushdown THROUGH a UNION / UNION ALL.
//
// A WHERE match above a union_t source is cloned into a node_match above EACH
// union branch (positional column identity: union output column i == branch
// column i). The residual (a conjunct a branch does not expose identically)
// stays above the union. Built through the REAL validate_schema so union +
// branch output_types()/key paths are exactly what the SQL pipeline stamps.
//
// Helper: return a branch's match child (a node_match_t among children[1..]).
// ================================================================
namespace {
    static node_ptr branch_match_child(const node_ptr& branch) {
        if (!branch || branch->type() != node_type::aggregate_t) {
            return nullptr;
        }
        for (const auto& c : branch->children()) {
            if (c && c->type() == node_type::match_t) {
                return c;
            }
        }
        return nullptr;
    }

    // Build aggregate[ union{all}(raw[left_cols], raw[right_cols]), match(where) ]
    // and drive validate_schema so the union/branches carry output_types() and the
    // match keys carry stamped paths.
    static node_ptr build_union_over_where(std::pmr::memory_resource* r,
                                           components::logical_plan::parameter_node_t* params,
                                           std::initializer_list<const char*> left_cols,
                                           std::initializer_list<const char*> right_cols,
                                           bool all,
                                           const expression_ptr& where) {
        auto scan_l = make_promote_scan(r, left_cols);
        auto scan_r = make_promote_scan(r, right_cols);
        auto uni = make_node_union(r, scan_l, scan_r, all);
        auto outer = make_node_aggregate(r, core::dbname_t{database_name}, core::relname_t{collection_name});
        outer->append_child(uni);
        outer->append_child(make_node_match(r, core::dbname_t{database_name}, core::relname_t{collection_name}, where));
        auto validated =
            services::dispatcher::validate_schema(test_validation_context(r), outer.get(), params->parameters());
        REQUIRE_FALSE(validated.has_error());
        return outer;
    }
} // namespace

TEST_CASE("optimizer::pushdown_filter::union_all_pushes_into_each_branch") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto gt = params->add_parameter(int64_t(5));

    auto where = make_compare_expression(&resource, compare_type::gt, key(&resource, "a"), gt);
    auto outer = build_union_over_where(&resource, params.get(), {"a", "b"}, {"a", "b"}, /*all=*/true, where);

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    // The whole WHERE pushed into both branches → the outer aggregate collapses to the union.
    REQUIRE(out->type() == node_type::union_t);
    REQUIRE(out->children().size() == 2);
    for (const auto& branch : out->children()) {
        auto m = branch_match_child(branch);
        REQUIRE(m != nullptr);
        REQUIRE(m->expressions().size() == 1);
        auto* cmp = static_cast<compare_expression_t*>(m->expressions()[0].get());
        REQUIRE(cmp->type() == compare_type::gt);
        REQUIRE(is_key(cmp->left()));
        REQUIRE(as_key(cmp->left()).as_string() == "a");
        // the match sits directly above the branch's raw-data scan (index 0).
        REQUIRE(branch->children()[0]->type() == node_type::data_t);
    }
}

TEST_CASE("optimizer::pushdown_filter::plain_union_pushes_into_each_branch") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto gt = params->add_parameter(int64_t(5));

    auto where = make_compare_expression(&resource, compare_type::gt, key(&resource, "a"), gt);
    auto outer = build_union_over_where(&resource, params.get(), {"a", "b"}, {"a", "b"}, /*all=*/false, where);

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out->type() == node_type::union_t);
    REQUIRE(static_cast<node_union_t*>(out.get())->all() == false); // dedup preserved above
    REQUIRE(out->children().size() == 2);
    for (const auto& branch : out->children()) {
        REQUIRE(branch_match_child(branch) != nullptr);
    }
}

TEST_CASE("optimizer::pushdown_filter::union_conjunction_all_mappable") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto gt = params->add_parameter(int64_t(5));
    auto lt = params->add_parameter(int64_t(10));

    // WHERE a > 5 AND b < 10  (both columns present identically in both branches)
    auto c1 = make_compare_expression(&resource, compare_type::gt, key(&resource, "a"), gt);
    auto c2 = make_compare_expression(&resource, compare_type::lt, key(&resource, "b"), lt);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = build_union_over_where(&resource, params.get(), {"a", "b"}, {"a", "b"}, /*all=*/true, where);
    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out->type() == node_type::union_t);
    for (const auto& branch : out->children()) {
        auto m = branch_match_child(branch);
        REQUIRE(m != nullptr);
        REQUIRE(m->expressions().size() == 1);
        // both conjuncts pushed → a union_and of 2 children.
        auto* cmp = static_cast<compare_expression_t*>(m->expressions()[0].get());
        REQUIRE(is_union_compare_condition(cmp->type()));
        REQUIRE(cmp->children().size() == 2);
    }
}

TEST_CASE("optimizer::pushdown_filter::union_residual_stays_above_for_non_mappable") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto gt = params->add_parameter(int64_t(5));
    auto lt = params->add_parameter(int64_t(10));

    // Right branch renames position 1 ("b" -> "c"): a conjunct on "b" is NOT identity-
    // mappable to the right branch, so it stays in the residual above the union while
    // the "a" conjunct pushes into both branches.
    auto c1 = make_compare_expression(&resource, compare_type::gt, key(&resource, "a"), gt);
    auto c2 = make_compare_expression(&resource, compare_type::lt, key(&resource, "b"), lt);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = build_union_over_where(&resource, params.get(), {"a", "b"}, {"a", "c"}, /*all=*/true, where);
    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    // Residual remains → the outer aggregate is kept, union as child[0].
    REQUIRE(out->type() == node_type::aggregate_t);
    REQUIRE(out->children()[0]->type() == node_type::union_t);

    node_ptr residual_match;
    for (const auto& c : out->children()) {
        if (c->type() == node_type::match_t) {
            residual_match = c;
        }
    }
    REQUIRE(residual_match != nullptr);
    REQUIRE(residual_match->expressions().size() == 1);
    // Only the "b" conjunct survives above the union (a single lt, not a union_and).
    auto* rcmp = static_cast<compare_expression_t*>(residual_match->expressions()[0].get());
    REQUIRE(rcmp->type() == compare_type::lt);
    REQUIRE(is_key(rcmp->left()));
    REQUIRE(as_key(rcmp->left()).as_string() == "b");

    // Each branch got the "a > 5" conjunct pushed.
    for (const auto& branch : out->children()[0]->children()) {
        auto m = branch_match_child(branch);
        REQUIRE(m != nullptr);
        auto* cmp = static_cast<compare_expression_t*>(m->expressions()[0].get());
        REQUIRE(cmp->type() == compare_type::gt);
        REQUIRE(as_key(cmp->left()).as_string() == "a");
    }
}

// ================================================================
// Filter pushdown below a JOIN when the filtered column NAME collides
// with a same-named column on the OTHER join side.
//
// Both t1 and t2 expose "id" and "k" (merged schema [t1.id=0, t1.k=1,
// t2.id=2, t2.k=3], left_width=2). `WHERE t1.id=5 AND t2.id=7` — the bare
// name "id" is on BOTH sides, so NAME-based bucketing (is `id` a
// subset of one side's alias set?) would put BOTH conjuncts in the residual
// above the join, never reaching the scans. The validator stamps each
// key's merged path (t1.id->0, t2.id->2); bucketing by path()[0] vs
// left_width routes t1.id below t1 and t2.id below t2.
//
// A scan carries its columns in output_types() (has_output_types() true,
// so left_width is known); keys carry a stamped merged path (pruned_key),
// exactly the post-validate_schema shape.
// ================================================================
namespace {
    // aggregate_t{db,rel} scan carrying its columns ONLY in output_types()
    // (the disk-scan shape). has_output_types() is true, so pushdown reads a
    // known left_width off children()[0].
    node_aggregate_ptr join_scan(std::pmr::memory_resource* r, std::initializer_list<const char*> cols) {
        auto agg = make_node_aggregate(r, pdb(), prel());
        std::pmr::vector<components::types::complex_logical_type> out(r);
        for (const char* c : cols) {
            out.emplace_back(components::types::logical_type::BIGINT, c);
        }
        agg->set_output_types(std::move(out));
        return agg;
    }
} // namespace

TEST_CASE("optimizer::pushdown_filter::join_shared_column_name_buckets_by_side") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto p5 = params->add_parameter(int64_t(5));
    auto p7 = params->add_parameter(int64_t(7));

    // t1 = {id, k}, t2 = {id, k}: "id" and "k" collide across sides.
    auto left = join_scan(&resource, {"id", "k"});
    auto right = join_scan(&resource, {"id", "k"});
    auto join = make_node_join(&resource, pdb(), prel(), join_type::inner);
    join->append_child(left);
    join->append_child(right);
    // ON t1.k (merged 1) = t2.k (merged 3)
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k", 3, side_t::right)));

    // WHERE t1.id = 5 AND t2.id = 7  (bare name "id" collides).
    auto c1 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 0, side_t::left), p5);
    auto c2 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 2, side_t::right), p7);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    // Whole WHERE consumed → the bare join is exposed (no residual match above it).
    REQUIRE(out == join);
    // t1.id=5 pushed below t1's scan.
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);
    // t2.id=7 pushed below t2's scan.
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->children().size() == 2);
    REQUIRE(join->children()[1]->children()[0] == right);
    REQUIRE(join->children()[1]->children()[1]->type() == node_type::match_t);
}

// ================================================================
// Outer-join safety under the SAME name collision: a LEFT join null-pads
// the RIGHT side, so a filter on the right (null-padded) side must STAY in
// the residual above the join even though its bare name "id" collides. The
// left-side conjunct still pushes. This proves the side-based classifier
// does not weaken the row-preserving guard (can_push_right == false for a
// LEFT join).
// ================================================================
TEST_CASE("optimizer::pushdown_filter::left_join_null_padded_side_filter_stays_residual") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto p5 = params->add_parameter(int64_t(5));
    auto p7 = params->add_parameter(int64_t(7));

    auto left = join_scan(&resource, {"id", "k"});
    auto right = join_scan(&resource, {"id", "k"});
    auto join = make_node_join(&resource, pdb(), prel(), join_type::left);
    join->append_child(left);
    join->append_child(right);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k", 3, side_t::right)));

    // WHERE t1.id = 5 AND t2.id = 7 : t1.id pushes; t2.id is on the null-padded side.
    auto c1 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 0, side_t::left), p5);
    auto c2 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 2, side_t::right), p7);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    // Residual (t2.id=7) survives → the outer aggregate is kept, join as child[0].
    REQUIRE(out == outer);
    REQUIRE(out->children()[0] == join);

    // t1.id=5 pushed below t1's scan.
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);

    // Right (null-padded) side NOT wrapped — the filter did NOT push.
    REQUIRE(join->children()[1] == right);

    // The residual match above the join holds ONLY t2.id=7.
    node_ptr residual_match;
    for (const auto& c : out->children()) {
        if (c->type() == node_type::match_t) {
            residual_match = c;
        }
    }
    REQUIRE(residual_match != nullptr);
    REQUIRE(residual_match->expressions().size() == 1);
    auto* rcmp = static_cast<compare_expression_t*>(residual_match->expressions()[0].get());
    REQUIRE(rcmp->type() == compare_type::eq);
    REQUIRE(is_key(rcmp->left()));
    REQUIRE(as_key(rcmp->left()).path().size() == 1);
    REQUIRE(as_key(rcmp->left()).path()[0] == 2); // t2.id, merged index 2 (>= left_width)
}

// ================================================================
// TRANSITIVE EQUI-PREDICATE PROPAGATION (inner join).
//
// t1 = {a, k}, t2 = {b, k2}, joined `ON t1.k = t2.k2` (an equi-pair). The
// merged schema is [t1.a=0, t1.k=1, t2.b=2, t2.k2=3], left_width=2. The ON keys
// are stamped SIDE-LOCAL (validate_key resolves each against its own side: left
// key path=1, right key path=1), which is what promote_cross_join and the real
// validator produce.
//
// `WHERE t1.k = 5` filters ONE equi-key column. On a matched inner-join row
// t1.k == t2.k2, so the same literal holds on the partner: the optimizer
// SYNTHESIZES `t2.k2 = 5` and routes it (via the existing merged-path bucketer +
// relocalizer) below t2's scan — in ADDITION to the original t1.k=5 below t1.
// The synthesized key must NAME the partner column (k2) and localize to t2's
// right-local index (1). The whole WHERE (t1.k=5 + derived t2.k2=5) pushes, so
// the bare join is exposed.
// ================================================================
TEST_CASE("optimizer::pushdown_filter::inner_join_transitive_equi_propagation") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto p5 = params->add_parameter(int64_t(5));

    auto left = join_scan(&resource, {"a", "k"});
    auto right = join_scan(&resource, {"b", "k2"});
    auto join = make_node_join(&resource, pdb(), prel(), join_type::inner);
    join->append_child(left);
    join->append_child(right);
    // ON t1.k (left-local 1) = t2.k2 (right-local 1)  -- side-local paths.
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k2", 1, side_t::right)));

    // WHERE t1.k = 5  (merged index 1, left).
    auto where = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "k", 1, side_t::left), p5);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    // Whole WHERE (original + derived) consumed → the bare join is exposed.
    REQUIRE(out == join);

    // t1.k = 5 pushed below t1's scan.
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    auto lm = join->children()[0]->children()[1];
    REQUIRE(lm->type() == node_type::match_t);
    auto* lcmp = static_cast<compare_expression_t*>(lm->expressions()[0].get());
    REQUIRE(lcmp->type() == compare_type::eq);
    REQUIRE(is_key(lcmp->left()));
    REQUIRE(as_key(lcmp->left()).as_string() == "k");
    REQUIRE(as_key(lcmp->left()).path()[0] == 1);
    REQUIRE(is_parameter(lcmp->right()));
    REQUIRE(as_parameter(lcmp->right()) == p5);

    // DERIVED t2.k2 = 5 pushed below t2's scan — names the PARTNER column,
    // re-localized to t2's right-local index 1, reusing the SAME parameter.
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->children().size() == 2);
    REQUIRE(join->children()[1]->children()[0] == right);
    auto rm = join->children()[1]->children()[1];
    REQUIRE(rm->type() == node_type::match_t);
    auto* rc = static_cast<compare_expression_t*>(rm->expressions()[0].get());
    REQUIRE(rc->type() == compare_type::eq);
    REQUIRE(is_key(rc->left()));
    REQUIRE(as_key(rc->left()).as_string() == "k2");
    REQUIRE(as_key(rc->left()).path().size() == 1);
    REQUIRE(as_key(rc->left()).path()[0] == 1);
    REQUIRE(is_parameter(rc->right()));
    REQUIRE(as_parameter(rc->right()) == p5);
}

// ================================================================
// Transitive propagation carries ANY comparison op through the equality:
// `WHERE t1.k > 5` on an equi-key derives `t2.k2 > 5` (a matched row has
// t1.k == t2.k2, so the same range predicate holds on the partner).
// ================================================================
TEST_CASE("optimizer::pushdown_filter::inner_join_transitive_range_propagation") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto p5 = params->add_parameter(int64_t(5));

    auto left = join_scan(&resource, {"a", "k"});
    auto right = join_scan(&resource, {"b", "k2"});
    auto join = make_node_join(&resource, pdb(), prel(), join_type::inner);
    join->append_child(left);
    join->append_child(right);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k2", 1, side_t::right)));

    // WHERE t1.k > 5
    auto where = make_compare_expression(&resource, compare_type::gt, pruned_key(&resource, "k", 1, side_t::left), p5);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out == join);

    // DERIVED t2.k2 > 5 (same op) below t2's scan.
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->children().size() == 2);
    REQUIRE(join->children()[1]->children()[0] == right);
    auto rm = join->children()[1]->children()[1];
    REQUIRE(rm->type() == node_type::match_t);
    auto* rc = static_cast<compare_expression_t*>(rm->expressions()[0].get());
    REQUIRE(rc->type() == compare_type::gt);
    REQUIRE(is_key(rc->left()));
    REQUIRE(as_key(rc->left()).as_string() == "k2");
    REQUIRE(as_key(rc->left()).path()[0] == 1);
    REQUIRE(is_parameter(rc->right()));
    REQUIRE(as_parameter(rc->right()) == p5);
}

// ================================================================
// OUTER-join safety: propagation is UNSOUND on the null-padded side. For
// `t1 LEFT JOIN t2 ON t1.k = t2.k2 WHERE t1.k = 5`, a preserved left row with
// no t2 match has t2.k2 = NULL; deriving `t2.k2 = 5` and pushing it below t2
// would wrongly drop such rows. The derivation is gated to INNER/CROSS, so
// NOTHING is synthesized here — t2's scan stays UNFILTERED. (t1.k=5 still
// pushes below t1, the row-preserving side.)
// ================================================================
TEST_CASE("optimizer::pushdown_filter::left_join_no_transitive_propagation") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto p5 = params->add_parameter(int64_t(5));

    auto left = join_scan(&resource, {"a", "k"});
    auto right = join_scan(&resource, {"b", "k2"});
    auto join = make_node_join(&resource, pdb(), prel(), join_type::left);
    join->append_child(left);
    join->append_child(right);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k2", 1, side_t::right)));

    auto where = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "k", 1, side_t::left), p5);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    // t1.k=5 pushes below t1 (left preserved); the whole WHERE consumed → bare join.
    REQUIRE(out == join);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);

    // NO derived predicate on the null-padded right side — t2's scan is untouched.
    REQUIRE(join->children()[1] == right);
}

// ================================================================
// drop_redundant_distinct: clear a DISTINCT a GROUP BY already makes
// redundant (group keys ⊆ projection / DISTINCT ON columns). These build the
// post-validate shape directly (group keys as leading group_field entries; select
// get_field columns carrying their resolved group-output ordinal in key.path()) and
// call the rule in isolation.
// ================================================================
namespace {
    using components::logical_plan::make_node_group;
    using components::logical_plan::make_node_select;
    using components::logical_plan::node_aggregate_t;

    // A get_field projection column resolved to group-output ordinal `pos`.
    scalar_expression_ptr drd_proj_col(std::pmr::memory_resource* r, const std::string& name, size_t pos) {
        auto se = make_scalar_expression(r, scalar_type::get_field, key(r, name));
        se->key().path().push_back(pos);
        return se;
    }

    key drd_on_key(std::pmr::memory_resource* r, const std::string& name, size_t pos) {
        key k(r, name);
        k.path().push_back(pos);
        return k;
    }

    size_t drd_column(const std::string& name) {
        if (name == "a") {
            return 0;
        }
        if (name == "b") {
            return 1;
        }
        return 2; // v
    }

    node_group_ptr drd_group(std::pmr::memory_resource* r,
                             const std::vector<std::string>& keys,
                             const std::vector<std::string>& projected,
                             bool with_count) {
        std::vector<expression_ptr> exprs;
        for (const auto& k : keys) {
            auto se = make_scalar_expression(r, scalar_type::group_field, key(r, k));
            se->key().path().push_back(drd_column(k));
            exprs.push_back(se);
        }
        for (const auto& p : projected) {
            exprs.push_back(drd_proj_col(r, p, drd_column(p)));
        }
        if (with_count) {
            auto cnt = make_aggregate_expression(r, "count", key(r, "c"));
            cnt->append_param(key(r, "v"));
            exprs.push_back(expression_ptr(cnt));
        }
        return make_node_group(r, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
    }

    node_aggregate_ptr
    drd_agg(std::pmr::memory_resource* r, const node_group_ptr& group, const node_select_ptr& select) {
        auto agg = make_node_aggregate(r, core::dbname_t{database_name}, core::relname_t{collection_name});
        agg->set_distinct(true);
        if (group) {
            agg->append_child(group);
        }
        if (select) {
            agg->append_child(select);
        }
        return agg;
    }

    bool drd_is_distinct_after(std::pmr::memory_resource* r, const node_aggregate_ptr& agg) {
        auto out = components::planner::optimizer::drop_redundant_distinct(r, agg);
        return static_cast<node_aggregate_t*>(out.get())->is_distinct();
    }
} // namespace

// Positive: group keys == projection ({a,b} ⊆ {a,b}) -> cleared.
TEST_CASE("optimizer::drop_redundant_distinct::plain_keys_equal_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a", "b"}, {"a", "b"}, /*with_count=*/false);
    REQUIRE_FALSE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

// Positive (subset direction — `SELECT DISTINCT a, b ... GROUP BY a`): group {a} ⊆
// projection {a,b}; the target list names the one grouping key.
TEST_CASE("optimizer::drop_redundant_distinct::plain_keys_subset_of_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a"}, {"a", "b"}, /*with_count=*/false);
    REQUIRE_FALSE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

// Positive (executable subset form): group {a} ⊆ non-aggregate projection {a}; the
// extra emitted column is an aggregate (count), which names no input column.
TEST_CASE("optimizer::drop_redundant_distinct::plain_subset_with_aggregate_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a"}, {"a"}, /*with_count=*/true);
    REQUIRE_FALSE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

// NEGATIVE (the trap): group {a,b} ⊄ projection {a}. Two groups (a,b1),(a,b2) both
// project a -> DISTINCT a is NOT redundant and MUST be kept.
TEST_CASE("optimizer::drop_redundant_distinct::plain_trap_group_not_subset") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a", "b"}, {"a"}, /*with_count=*/false);
    REQUIRE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

// Negative: no GROUP BY (no group_t child) -> DISTINCT untouched.
TEST_CASE("optimizer::drop_redundant_distinct::no_group_by_untouched") {
    auto resource = core::pmr::otterbrix_resource();
    auto select = make_node_select(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
    select->append_expression(drd_proj_col(&resource, "a", 0));
    REQUIRE(drd_is_distinct_after(&resource, drd_agg(&resource, node_group_ptr{}, select)));
}

// DISTINCT ON positive: group keys ⊆ ON columns ({a,b} ⊆ {a,b}) -> cleared.
TEST_CASE("optimizer::drop_redundant_distinct::distinct_on_keys_subset") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a", "b"}, {"a", "b"}, /*with_count=*/false);
    auto agg = drd_agg(&resource, group, node_select_ptr{});
    std::pmr::vector<key> on(&resource);
    on.push_back(drd_on_key(&resource, "a", 0));
    on.push_back(drd_on_key(&resource, "b", 1));
    agg->set_distinct_on_keys(std::move(on));
    auto out = components::planner::optimizer::drop_redundant_distinct(&resource, agg);
    auto* a = static_cast<node_aggregate_t*>(out.get());
    REQUIRE_FALSE(a->is_distinct());
    REQUIRE(a->distinct_on_keys().empty()); // dead ON list dropped
}

// DISTINCT ON trap: group keys ⊄ ON columns ({a,b} ⊄ {a}) -> kept.
TEST_CASE("optimizer::drop_redundant_distinct::distinct_on_keys_not_subset") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a", "b"}, {"a", "b"}, /*with_count=*/false);
    auto agg = drd_agg(&resource, group, node_select_ptr{});
    std::pmr::vector<key> on(&resource);
    on.push_back(drd_on_key(&resource, "a", 0));
    agg->set_distinct_on_keys(std::move(on));
    auto out = components::planner::optimizer::drop_redundant_distinct(&resource, agg);
    REQUIRE(static_cast<node_aggregate_t*>(out.get())->is_distinct());
}

// ================================================================
// eager_aggregation rule — pushes a MIN/MAX partial aggregate onto
// the single join side that owns every group key + aggregate arg,
// leaving a FINAL merge above the join. Fires only on the provably-
// sound (duplication-insensitive) MIN/MAX shape. Built here in the
// post-rewrite_hash_joins state (equi-key stamped) and driven through
// the rule directly.
// ================================================================
namespace { namespace eag {
    using components::expressions::side_t;

    std::pmr::vector<size_t> path1(std::pmr::memory_resource* r, size_t i) {
        std::pmr::vector<size_t> p{r};
        p.push_back(i);
        return p;
    }

    node_aggregate_ptr leaf(std::pmr::memory_resource* r,
                            const char* rel,
                            components::catalog::oid_t oid,
                            std::initializer_list<const char*> cols) {
        auto a = make_node_aggregate(r, core::dbname_t{"db"}, core::relname_t{rel});
        a->set_table_oid(oid);
        std::pmr::vector<components::types::complex_logical_type> types(r);
        for (const char* c : cols) {
            types.emplace_back(components::types::logical_type::BIGINT, c);
        }
        a->set_output_types(std::move(types));
        return a;
    }

    key col(std::pmr::memory_resource* r, const char* name, size_t path, side_t side = side_t::undefined) {
        key k(r, name, side);
        k.set_path(path1(r, path));
        return k;
    }

    // outer aggregate { join[hash,inner]( a=(g,k,x), b=(k) ) ON a.k=b.k,
    //                   group( group_by g, fn(x)->m ) }
    // `agg_arg_path` / `key_path` let a caller move the measure or key to the
    // OTHER (b) side for the cross-side negative test.
    node_aggregate_ptr make_join_agg(std::pmr::memory_resource* r,
                                     const std::string& fn,
                                     bool hash = true,
                                     size_t key_path = 0,
                                     size_t agg_arg_path = 2) {
        auto a = leaf(r, "a", components::catalog::oid_t{100}, {"g", "k", "x"});
        auto b = leaf(r, "b", components::catalog::oid_t{200}, {"k"});
        auto join = make_node_join(r, core::dbname_t{}, core::relname_t{}, join_type::inner);
        join->append_child(a);
        join->append_child(b);
        join->append_expression(make_compare_expression(r,
                                                        compare_type::eq,
                                                        param_storage{col(r, "k", 1, side_t::left)},
                                                        param_storage{col(r, "k", 0, side_t::right)}));
        if (hash) {
            join->set_equi_columns(1, 0); // left_col=1 (a.k), right_col=0 (b.k); flips algo->hash
        }
        auto gexpr = make_scalar_expression(r, scalar_type::group_field, col(r, "g", key_path));
        auto aexpr = make_aggregate_expression(r, fn, key(r, "m"), col(r, "x", agg_arg_path));
        aexpr->set_mergeable(true);
        // Stands in for what validation stamps: the rule runs after it, so the partial it
        // builds has to carry this type over itself.
        aexpr->set_result_type(components::types::complex_logical_type{components::types::logical_type::BIGINT});
        std::vector<expression_ptr> gxs;
        gxs.emplace_back(gexpr);
        gxs.emplace_back(expression_ptr(aexpr));
        auto group = make_node_group(r, core::dbname_t{}, core::relname_t{}, gxs);
        auto outer = make_node_aggregate(r, core::dbname_t{}, core::relname_t{});
        outer->append_child(join);
        outer->append_child(group);
        return outer;
    }

    // The group_t child spliced onto the pushed (left) join side, or nullptr.
    node_group_t* pushed_partial(const node_ptr& outer) {
        auto* join = static_cast<node_join_t*>(outer->children()[0].get());
        for (const auto& c : join->children()[0]->children()) {
            if (c && c->type() == node_type::group_t) {
                return static_cast<node_group_t*>(c.get());
            }
        }
        return nullptr;
    }
}} // namespace ::eag

TEST_CASE("optimizer::eager_aggregation::min_is_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    auto outer = eag::make_join_agg(&resource, "min");
    // Before the rule: the left join side is a BARE table aggregate (no group).
    REQUIRE(eag::pushed_partial(outer) == nullptr);

    components::planner::optimizer::eager_aggregation(&resource, outer);

    // After the rule: a partial group is spliced onto side a, emitting [g, k(join key), min(x)].
    auto* partial = eag::pushed_partial(outer);
    REQUIRE(partial != nullptr);
    // A key is both GROUPED ON and NAMED — a group emits its target list, and a key that no entry
    // names is not emitted at all — so each key appears twice in the expression list: as the
    // group_field that reduces and as the get_field that outputs it. Hence 5 expressions for a
    // 3-column output: [group_field g, group_field k, get_field g, get_field k, min(x)].
    REQUIRE(partial->expressions().size() == 5);
    for (size_t i = 0; i < 2; i++) {
        REQUIRE(partial->expressions()[i]->group() == expression_group::scalar);
        CHECK(static_cast<scalar_expression_t*>(partial->expressions()[i].get())->type() == scalar_type::group_field);
    }
    for (size_t i = 2; i < 4; i++) {
        REQUIRE(partial->expressions()[i]->group() == expression_group::scalar);
        CHECK(static_cast<scalar_expression_t*>(partial->expressions()[i].get())->type() == scalar_type::get_field);
    }
    REQUIRE(partial->expressions()[4]->group() == expression_group::aggregate);
    CHECK(static_cast<aggregate_expression_t*>(partial->expressions()[4].get())->function_name() == "min");
    // The rule runs after validation, so it must stamp what it builds itself — an unstamped
    // aggregate is rejected by the execution graph builder at run time. MIN(MIN)=MIN over the same
    // column, so the partial reduces to the type the final one was resolved to.
    CHECK(static_cast<aggregate_expression_t*>(partial->expressions()[4].get())->result_type().type() ==
          components::types::logical_type::BIGINT);

    // The OUTPUT layout is what every ordinal below addresses: g@0, k@1 (join key), min(x)@2.

    // Join equi re-stamped: a.k now sits at its partial-output position (1).
    auto* join = static_cast<node_join_t*>(outer->children()[0].get());
    CHECK(join->left_col() == 1);
    CHECK(join->right_col() == 0);

    // FINAL group now reads the partial's output: g@0, MIN over partial-min @2.
    node_group_t* final_group = nullptr;
    for (const auto& c : outer->children()) {
        if (c->type() == node_type::group_t) {
            final_group = static_cast<node_group_t*>(c.get());
        }
    }
    REQUIRE(final_group != nullptr);
    auto* final_agg = static_cast<aggregate_expression_t*>(final_group->expressions()[1].get());
    REQUIRE(final_agg->function_name() == "min"); // MIN(MIN)=MIN — function unchanged
    REQUIRE(is_key(final_agg->params()[0]));
    CHECK(as_key(final_agg->params()[0]).path().size() == 1);
    CHECK(as_key(final_agg->params()[0]).path()[0] == 2);
}

TEST_CASE("optimizer::eager_aggregation::max_is_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    auto outer = eag::make_join_agg(&resource, "max");
    components::planner::optimizer::eager_aggregation(&resource, outer);
    auto* partial = eag::pushed_partial(outer);
    REQUIRE(partial != nullptr);
    // [group_field g, group_field k, get_field g, get_field k, max(x)] — see min_is_pushed.
    REQUIRE(partial->expressions().size() == 5);
    CHECK(static_cast<aggregate_expression_t*>(partial->expressions()[4].get())->function_name() == "max");
}

TEST_CASE("optimizer::eager_aggregation::sum_is_not_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    // SUM over-counts on join duplication and needs a uniqueness proof the plan
    // lacks -> deliberately excluded.
    auto outer = eag::make_join_agg(&resource, "sum");
    components::planner::optimizer::eager_aggregation(&resource, outer);
    REQUIRE(eag::pushed_partial(outer) == nullptr);
}

TEST_CASE("optimizer::eager_aggregation::nested_loop_join_is_not_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    // No single equi-key (algo stays nested) -> no join column to add to the
    // partial grouping -> skip.
    auto outer = eag::make_join_agg(&resource, "min", /*hash=*/false);
    components::planner::optimizer::eager_aggregation(&resource, outer);
    REQUIRE(eag::pushed_partial(outer) == nullptr);
}

TEST_CASE("optimizer::eager_aggregation::cross_side_reference_is_not_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    // Group key on the left (a), aggregate argument on the right (b, merged idx 3):
    // the measure spans both sides -> not pushable to one side -> skip.
    auto outer = eag::make_join_agg(&resource, "min", /*hash=*/true, /*key_path=*/0, /*agg_arg_path=*/3);
    components::planner::optimizer::eager_aggregation(&resource, outer);
    REQUIRE(eag::pushed_partial(outer) == nullptr);
}

// ================================================================
// Constant folding refusals: unfoldable kinds and non-numeric operands.
//
// try_fold_compare used to `assert(false)` when eval_compare answered "this
// comparison kind is not foldable" — an assert on ordinary plan data (any
// constant-vs-constant regex/ANY/ALL comparison the transformer emits), which
// aborted Debug builds and was Release-erased into the very skip it forbade.
// Not folding is the CORRECT outcome for such a kind: the runtime evaluator is
// the canonical answer for it, folding is only an optimization.
//
// try_fold_scalar used to box both constants into 1-element vectors because the
// logical_value_t arithmetic entry points threw; compute_binary_arithmetic
// still throws std::logic_error on non-numeric operands, so a constant
// `'a' + 1` aborted plan-time folding instead of being refused by value.
// ================================================================
TEST_CASE("optimizer::constant_folding::unfoldable_comparison_kind_is_left_unfolded") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(std::string("abc"));
    auto id1 = params->add_parameter(std::string("b.*"));

    // Both sides constant, kind regex: eval_compare cannot fold it. The rule
    // must leave the expression for the runtime matcher — no assert, no abort.
    auto comp = make_compare_expression(&resource, compare_type::regex, id0, id1);
    auto node = make_match_with_expr(&resource, comp);

    components::planner::optimize(&resource, node, params.get());

    REQUIRE(comp->type() == compare_type::regex);
    REQUIRE(std::holds_alternative<core::parameter_id_t>(comp->left()));
    REQUIRE(std::holds_alternative<core::parameter_id_t>(comp->right()));
}

TEST_CASE("optimizer::constant_folding::non_numeric_constant_arithmetic_is_declined_not_folded") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(std::string("a"));
    auto id1 = params->add_parameter(int64_t(1));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);

    // A mixed STRING/BIGINT constant pair: the fold declines (no throw, no
    // silent NULL constant) and the expression survives for the runtime
    // evaluator. The retired vector-boxing path folded this to a constant NULL.
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 2);
    REQUIRE(s->type() == scalar_type::add);
}
