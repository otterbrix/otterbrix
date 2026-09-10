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
}

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

using planner_test::make_agg_group;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

static node_ptr make_match_with_expr(std::pmr::memory_resource* r, const expression_ptr& expr) {
    return make_node_match(r, core::dbname_t{database_name}, core::relname_t{collection_name}, expr);
}

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

    auto* s = static_cast<scalar_expression_t*>(group_node->expressions()[0].get());
    REQUIRE(s->params().size() == 2);
}

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

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 5);

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::eq);
}

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
    // Division by zero may fold, throw, or otherwise stay valid; either is acceptable here.
    REQUIRE((s->params().size() == 1 || s->params().size() == 2));
}

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
    REQUIRE(c2->type() == compare_type::gt);
}

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

    auto* s = static_cast<scalar_expression_t*>(outer.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 20);
}

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

TEST_CASE("optimizer::aggregate_match_folds_group_not") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(2));
    auto id3 = params->add_parameter(int64_t(3));

    auto aggregate = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp));

    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id2);
    scalar->append_param(id3);
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(std::move(scalar));
    aggregate->append_child(
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, group_exprs));

    components::planner::optimize(&resource, aggregate, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);

    auto* gs = static_cast<scalar_expression_t*>(aggregate->children()[1]->expressions()[0].get());
    REQUIRE(gs->params().size() == 2);
}

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

TEST_CASE("optimizer::mirror_compare_lt_gt") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::lt) == compare_type::gt);
    REQUIRE(mirror_compare(compare_type::gt) == compare_type::lt);
}

TEST_CASE("optimizer::mirror_compare_lte_gte") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::lte) == compare_type::gte);
    REQUIRE(mirror_compare(compare_type::gte) == compare_type::lte);
}

TEST_CASE("optimizer::mirror_compare_symmetric") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::eq) == compare_type::eq);
    REQUIRE(mirror_compare(compare_type::ne) == compare_type::ne);
}

TEST_CASE("optimizer::has_index_on_positive") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, components::catalog::session_catalog_t{});
    constexpr auto table_oid = components::catalog::oid_t{701};

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.index_info_slot(table_oid).keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "age")) == true);
}

TEST_CASE("optimizer::has_index_on_negative") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, components::catalog::session_catalog_t{});
    constexpr auto table_oid = components::catalog::oid_t{702};

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.index_info_slot(table_oid).keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "name")) == false);
}

TEST_CASE("optimizer::has_index_on_multi_field_skip") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, components::catalog::session_catalog_t{});
    constexpr auto table_oid = components::catalog::oid_t{703};

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "a"));
    keys.push_back(key(&resource, "b"));
    ctx.index_info_slot(table_oid).keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "a")) == false);
}

TEST_CASE("optimizer::has_index_on_empty") {
    auto resource = core::pmr::otterbrix_resource();
    services::context_storage_t ctx(&resource, log_t{}, components::catalog::session_catalog_t{});
    constexpr auto table_oid = components::catalog::oid_t{704};

    REQUIRE(ctx.has_index_on(table_oid, key(&resource, "any")) == false);
}

TEST_CASE("optimizer::param_copy_survives") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    params->set_parameter(id0, components::types::logical_value_t(&resource, int64_t(5)));
    REQUIRE(params->parameter(id0).value<int64_t>() == 5);

    auto taken = params->take_parameters();
    REQUIRE(taken.parameters.count(id0) == 1);
    REQUIRE(taken.parameters.at(id0).value<int64_t>() == 5);
    REQUIRE(taken.parameters.count(id1) == 1);

    storage_parameters copy1 = taken;
    REQUIRE(copy1.parameters.count(id0) == 1);
    REQUIRE(copy1.parameters.at(id0).value<int64_t>() == 5);

    storage_parameters copy2 = copy1;
    REQUIRE(copy2.parameters.count(id0) == 1);
    REQUIRE(copy2.parameters.at(id0).value<int64_t>() == 5);

    storage_parameters moved = std::move(copy2);
    REQUIRE(moved.parameters.count(id0) == 1);
    REQUIRE(moved.parameters.at(id0).value<int64_t>() == 5);
}

static services::context_storage_t make_context_with_oid(std::pmr::memory_resource* resource,
                                                         components::catalog::oid_t oid,
                                                         const components::logical_plan::storage_parameters* params) {
    services::context_storage_t ctx(resource, log_t{}, components::catalog::session_catalog_t{});
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

namespace {
    constexpr auto pushable_oid = components::catalog::oid_t{4242};

    static node_aggregate_ptr make_agg(std::pmr::memory_resource* r, const node_group_ptr& group) {
        return planner_test::make_agg(r, group, pushable_oid);
    }

    static bool run_and_get_pushdown(std::pmr::memory_resource* r, const node_ptr& plan, bool enable) {
        auto params = make_parameter_node(r);
        auto root = components::planner::optimize(r, plan, params.get(), nullptr, enable);
        for (const auto& child : root->children()) {
            if (child && child->type() == node_type::group_t) {
                return static_cast<node_group_t*>(child.get())->pushdown();
            }
        }
        return false;
    }
}

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
    auto agg = planner_test::make_agg(&resource, group, pushable_oid, expression_ptr(having));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::join_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    agg->append_child(
        make_node_join(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, join_type::inner));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::nested_aggregate_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
    auto nested = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
    agg->append_child(std::move(nested));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::union_child_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = make_agg_group(&resource, /*with_group_key=*/false, /*distinct=*/false);
    auto agg = make_agg(&resource, group);
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
    agg->append_child(make_node_cte_scan(&resource, std::pmr::string("cte")));
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::non_mergeable_kind_is_skipped") {
    auto resource = core::pmr::otterbrix_resource();
    std::vector<expression_ptr> exprs;
    auto agg_expr = make_aggregate_expression(&resource, "stddev", key(&resource, "s"));
    agg_expr->append_param(key(&resource, "v"));
    exprs.push_back(expression_ptr(agg_expr));
    auto group = make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

TEST_CASE("optimizer::pushdown_aggregate::mergeable_capability_gates_stamp") {
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
    // A UDF-referencing aggregate arg is skipped: the owning agent rebuilds its pushed registry with builtins only.
    std::vector<expression_ptr> exprs;
    auto sum = make_aggregate_expression(&resource, "sum", key(&resource, "s"));
    sum->set_mergeable(true);
    auto udf = make_function_expression(&resource, std::string("my_udf"));
    udf->add_function_uid(components::compute::DEFAULT_FUNCTIONS.size());
    sum->append_param(expression_ptr(udf));
    exprs.push_back(expression_ptr(sum));
    auto group = make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, exprs);
    auto agg = make_agg(&resource, group);
    REQUIRE(run_and_get_pushdown(&resource, agg, /*enable=*/true) == false);
}

// Unqualified comma-join columns get merged with ambiguous side=left stamps that detect_equi_columns can't
// use; promote_cross_joins reclassifies them by path range instead, onto a fresh INNER join.
namespace {
    // Left unstamped on purpose: validate_schema derives output_types() from the data chunk.
    static node_ptr make_promote_scan(std::pmr::memory_resource* r, std::initializer_list<const char*> cols) {
        std::pmr::vector<components::types::complex_logical_type> types(r);
        for (const char* name : cols) {
            types.emplace_back(components::types::logical_type::BIGINT, name);
        }
        auto chunk = gen_data_chunk(/*size=*/1, /*start=*/0, types, r);
        return make_node_raw_data(r, std::move(chunk));
    }
}

TEST_CASE("optimizer::promote_cross_join::comma_join_becomes_inner_hash") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto lt_param = params->add_parameter(int64_t(5));

    auto scan_a = make_promote_scan(&resource, {"ak", "ap"});
    auto scan_b = make_promote_scan(&resource, {"bk"});

    auto join =
        make_node_join(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, join_type::cross);
    join->append_child(scan_a);
    join->append_child(scan_b);
    join->append_expression(make_compare_expression(&resource, compare_type::all_true));

    auto eq = make_compare_expression(&resource, compare_type::eq, key(&resource, "ak"), key(&resource, "bk"));
    auto lt = make_compare_expression(&resource, compare_type::lt, key(&resource, "ap"), lt_param);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(eq);
    where->append_child(lt);

    auto outer = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
    outer->append_child(join);
    outer->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, where));

    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_ap"));
    sum_expr->append_param(key(&resource, "ap"));
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(expression_ptr(sum_expr));
    outer->append_child(
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, group_exprs));

    auto validated =
        services::dispatcher::validate_schema(test_validation_context(&resource), outer.get(), params->parameters());
    REQUIRE_FALSE(validated.has_error());
    REQUIRE(scan_a->output_types().size() == 2);
    REQUIRE(scan_b->output_types().size() == 1);

    node_ptr out = components::planner::optimizer::promote_cross_joins(&resource, outer);
    out = components::planner::optimizer::rewrite_hash_joins(&resource, out);

    REQUIRE(out == outer);
    auto* agg = static_cast<node_aggregate_t*>(out.get());
    REQUIRE(agg->children().size() == 3);

    REQUIRE(agg->children()[0]->type() == node_type::join_t);
    auto* jn = static_cast<node_join_t*>(agg->children()[0].get());
    REQUIRE(jn->type() == join_type::inner);
    REQUIRE(jn->algo() == node_join_t::join_algo::hash);
    REQUIRE(jn->left_col() == 0);
    REQUIRE(jn->right_col() == 0);

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

    REQUIRE(agg->children()[1]->type() == node_type::match_t);
    auto residual_match = agg->children()[1];
    REQUIRE(residual_match->expressions().size() == 1);
    auto* residual = static_cast<compare_expression_t*>(residual_match->expressions()[0].get());
    REQUIRE(residual->type() == compare_type::lt);

    REQUIRE(agg->children()[2]->type() == node_type::group_t);
}

// Regression: folding union_not must yield the complementary constant, not a partial fold that asserts in Release.
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

namespace {
    using components::catalog::oid_t;
    constexpr auto prune_db = "database";
    constexpr auto prune_rel = "collection";

    core::dbname_t pdb() { return core::dbname_t{std::string{prune_db}}; }
    core::relname_t prel() { return core::relname_t{std::string{prune_rel}}; }

    key pruned_key(std::pmr::memory_resource* r, const char* name, size_t idx, side_t side = side_t::undefined) {
        key k(r, name, side);
        std::pmr::vector<size_t> p{r};
        p.push_back(idx);
        k.set_path(std::move(p));
        return k;
    }

    expression_ptr proj_get_field(std::pmr::memory_resource* r, const char* name, size_t idx) {
        return expression_ptr(make_scalar_expression(r, scalar_type::get_field, pruned_key(r, name, idx)));
    }

    node_aggregate_ptr make_select_agg(std::pmr::memory_resource* r, oid_t oid, const node_select_ptr& sel) {
        auto agg = make_node_aggregate(r, pdb(), prel());
        agg->set_table_oid(oid);
        agg->append_child(sel);
        return agg;
    }

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
}

TEST_CASE("optimizer::column_pruning::plain_select_projects_single_column") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
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
    std::vector<expression_ptr> group_exprs;
    group_exprs.push_back(
        expression_ptr(make_scalar_expression(&resource, scalar_type::group_field, pruned_key(&resource, "k", 0))));
    auto sum = make_aggregate_expression(&resource, "sum", key(&resource, "sum_x"));
    sum->append_param(pruned_key(&resource, "x", 2));
    group_exprs.push_back(expression_ptr(sum));
    auto group = make_node_group(&resource, pdb(), prel(), group_exprs);

    // The group's $select carries output indices, not storage indices; the rule must ignore it.
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
    constexpr auto oid1 = oid_t{9110};
    constexpr auto oid2 = oid_t{9111};

    auto agg_t1 = make_node_aggregate(&resource, pdb(), prel());
    agg_t1->set_table_oid(oid1);
    auto agg_t2 = make_node_aggregate(&resource, pdb(), prel());
    agg_t2->set_table_oid(oid2);

    auto join = make_node_join(&resource, pdb(), prel(), join_type::inner);
    join->append_child(agg_t1);
    join->append_child(agg_t2);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k", 0, side_t::right)));

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

// A WHERE match above a union is cloned onto each branch via positional identity (output column i == branch
// column i); a conjunct a branch can't expose via that mapping stays residual above instead.
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
}

TEST_CASE("optimizer::pushdown_filter::union_all_pushes_into_each_branch") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto gt = params->add_parameter(int64_t(5));

    auto where = make_compare_expression(&resource, compare_type::gt, key(&resource, "a"), gt);
    auto outer = build_union_over_where(&resource, params.get(), {"a", "b"}, {"a", "b"}, /*all=*/true, where);

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

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
    REQUIRE(static_cast<node_union_t*>(out.get())->all() == false);
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

    auto c1 = make_compare_expression(&resource, compare_type::gt, key(&resource, "a"), gt);
    auto c2 = make_compare_expression(&resource, compare_type::lt, key(&resource, "b"), lt);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = build_union_over_where(&resource, params.get(), {"a", "b"}, {"a", "c"}, /*all=*/true, where);
    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

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
    auto* rcmp = static_cast<compare_expression_t*>(residual_match->expressions()[0].get());
    REQUIRE(rcmp->type() == compare_type::lt);
    REQUIRE(is_key(rcmp->left()));
    REQUIRE(as_key(rcmp->left()).as_string() == "b");

    for (const auto& branch : out->children()[0]->children()) {
        auto m = branch_match_child(branch);
        REQUIRE(m != nullptr);
        auto* cmp = static_cast<compare_expression_t*>(m->expressions()[0].get());
        REQUIRE(cmp->type() == compare_type::gt);
        REQUIRE(as_key(cmp->left()).as_string() == "a");
    }
}

// A filtered column name colliding across join sides would strand both under name-based bucketing; this
// buckets by the stamped path instead.
namespace {
    node_aggregate_ptr join_scan(std::pmr::memory_resource* r, std::initializer_list<const char*> cols) {
        auto agg = make_node_aggregate(r, pdb(), prel());
        std::pmr::vector<components::types::complex_logical_type> out(r);
        for (const char* c : cols) {
            out.emplace_back(components::types::logical_type::BIGINT, c);
        }
        agg->set_output_types(std::move(out));
        return agg;
    }
}

TEST_CASE("optimizer::pushdown_filter::join_shared_column_name_buckets_by_side") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto p5 = params->add_parameter(int64_t(5));
    auto p7 = params->add_parameter(int64_t(7));

    auto left = join_scan(&resource, {"id", "k"});
    auto right = join_scan(&resource, {"id", "k"});
    auto join = make_node_join(&resource, pdb(), prel(), join_type::inner);
    join->append_child(left);
    join->append_child(right);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    pruned_key(&resource, "k", 1, side_t::left),
                                                    pruned_key(&resource, "k", 3, side_t::right)));

    auto c1 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 0, side_t::left), p5);
    auto c2 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 2, side_t::right), p7);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out == join);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->children().size() == 2);
    REQUIRE(join->children()[1]->children()[0] == right);
    REQUIRE(join->children()[1]->children()[1]->type() == node_type::match_t);
}

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

    auto c1 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 0, side_t::left), p5);
    auto c2 = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "id", 2, side_t::right), p7);
    auto where = make_compare_union_expression(&resource, compare_type::union_and);
    where->append_child(c1);
    where->append_child(c2);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out == outer);
    REQUIRE(out->children()[0] == join);

    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);

    REQUIRE(join->children()[1] == right);

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
    REQUIRE(as_key(rcmp->left()).path()[0] == 2);
}

// An equi-join condition lets a WHERE on one side (`t1.k=5`) synthesize the same filter on the other side
// (`t2.k2=5`) and push it below both scans.
TEST_CASE("optimizer::pushdown_filter::inner_join_transitive_equi_propagation") {
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

    auto where = make_compare_expression(&resource, compare_type::eq, pruned_key(&resource, "k", 1, side_t::left), p5);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out == join);

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

    auto where = make_compare_expression(&resource, compare_type::gt, pruned_key(&resource, "k", 1, side_t::left), p5);

    auto outer = make_node_aggregate(&resource, pdb(), prel());
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, pdb(), prel(), where));

    node_ptr out = components::planner::optimizer::pushdown_filter(&resource, outer);

    REQUIRE(out == join);

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

// Transitive propagation is gated to INNER/CROSS: on LEFT join, an unmatched row has the right key NULL.
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

    REQUIRE(out == join);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);

    REQUIRE(join->children()[1] == right);
}

namespace {
    using components::logical_plan::make_node_group;
    using components::logical_plan::make_node_select;
    using components::logical_plan::node_aggregate_t;

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
        return 2;
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
}

TEST_CASE("optimizer::drop_redundant_distinct::plain_keys_equal_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a", "b"}, {"a", "b"}, /*with_count=*/false);
    REQUIRE_FALSE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

TEST_CASE("optimizer::drop_redundant_distinct::plain_keys_subset_of_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a"}, {"a", "b"}, /*with_count=*/false);
    REQUIRE_FALSE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

TEST_CASE("optimizer::drop_redundant_distinct::plain_subset_with_aggregate_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a"}, {"a"}, /*with_count=*/true);
    REQUIRE_FALSE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

TEST_CASE("optimizer::drop_redundant_distinct::plain_trap_group_not_subset") {
    auto resource = core::pmr::otterbrix_resource();
    auto group = drd_group(&resource, {"a", "b"}, {"a"}, /*with_count=*/false);
    REQUIRE(drd_is_distinct_after(&resource, drd_agg(&resource, group, node_select_ptr{})));
}

TEST_CASE("optimizer::drop_redundant_distinct::no_group_by_untouched") {
    auto resource = core::pmr::otterbrix_resource();
    auto select = make_node_select(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
    select->append_expression(drd_proj_col(&resource, "a", 0));
    REQUIRE(drd_is_distinct_after(&resource, drd_agg(&resource, node_group_ptr{}, select)));
}

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
    REQUIRE(a->distinct_on_keys().empty());
}

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

// eager_aggregation pushes a MIN/MAX partial onto the single join side owning every group key and aggregate arg.
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
            join->set_equi_columns(1, 0);
        }
        auto gexpr = make_scalar_expression(r, scalar_type::group_field, col(r, "g", key_path));
        auto aexpr = make_aggregate_expression(r, fn, key(r, "m"), col(r, "x", agg_arg_path));
        aexpr->set_mergeable(true);
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

    node_group_t* pushed_partial(const node_ptr& outer) {
        auto* join = static_cast<node_join_t*>(outer->children()[0].get());
        for (const auto& c : join->children()[0]->children()) {
            if (c && c->type() == node_type::group_t) {
                return static_cast<node_group_t*>(c.get());
            }
        }
        return nullptr;
    }
}}

TEST_CASE("optimizer::eager_aggregation::min_is_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    auto outer = eag::make_join_agg(&resource, "min");
    REQUIRE(eag::pushed_partial(outer) == nullptr);

    components::planner::optimizer::eager_aggregation(&resource, outer);

    auto* partial = eag::pushed_partial(outer);
    REQUIRE(partial != nullptr);
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
    CHECK(static_cast<aggregate_expression_t*>(partial->expressions()[4].get())->result_type().type() ==
          components::types::logical_type::BIGINT);

    auto* join = static_cast<node_join_t*>(outer->children()[0].get());
    CHECK(join->left_col() == 1);
    CHECK(join->right_col() == 0);

    node_group_t* final_group = nullptr;
    for (const auto& c : outer->children()) {
        if (c->type() == node_type::group_t) {
            final_group = static_cast<node_group_t*>(c.get());
        }
    }
    REQUIRE(final_group != nullptr);
    auto* final_agg = static_cast<aggregate_expression_t*>(final_group->expressions()[1].get());
    REQUIRE(final_agg->function_name() == "min");
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
    REQUIRE(partial->expressions().size() == 5);
    CHECK(static_cast<aggregate_expression_t*>(partial->expressions()[4].get())->function_name() == "max");
}

TEST_CASE("optimizer::eager_aggregation::sum_is_not_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    // SUM over-counts on join duplication and needs a uniqueness proof the plan lacks.
    auto outer = eag::make_join_agg(&resource, "sum");
    components::planner::optimizer::eager_aggregation(&resource, outer);
    REQUIRE(eag::pushed_partial(outer) == nullptr);
}

TEST_CASE("optimizer::eager_aggregation::nested_loop_join_is_not_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    auto outer = eag::make_join_agg(&resource, "min", /*hash=*/false);
    components::planner::optimizer::eager_aggregation(&resource, outer);
    REQUIRE(eag::pushed_partial(outer) == nullptr);
}

TEST_CASE("optimizer::eager_aggregation::cross_side_reference_is_not_pushed") {
    auto resource = core::pmr::otterbrix_resource();
    auto outer = eag::make_join_agg(&resource, "min", /*hash=*/true, /*key_path=*/0, /*agg_arg_path=*/3);
    components::planner::optimizer::eager_aggregation(&resource, outer);
    REQUIRE(eag::pushed_partial(outer) == nullptr);
}

// try_fold_compare must skip (not assert) an unfoldable kind like regex; try_fold_scalar must decline
// non-numeric constants rather than box them, since compute_binary_arithmetic throws on them.
TEST_CASE("optimizer::constant_folding::unfoldable_comparison_kind_is_left_unfolded") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(std::string("abc"));
    auto id1 = params->add_parameter(std::string("b.*"));

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

    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 2);
    REQUIRE(s->type() == scalar_type::add);
}
