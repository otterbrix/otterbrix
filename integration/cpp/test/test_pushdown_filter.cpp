// Reference-trace tests for the predicate pushdown optimizer (planner::optimize).
// Each test builds a known input logical plan, runs optimize(), and asserts the
// exact shape of the resulting node tree — verifying the transformation itself,
// not just the final query result. Covers all four pushdown variants:
// filter through projection (select), through sort, through join, through group by.

#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/planner/optimizer.hpp>
#include <components/tests/generaty.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static collection_full_name_t coll() {
    return {database_name_t("db"), collection_name_t("t")};
}

static node_data_ptr make_data(std::pmr::memory_resource* r,
                                std::initializer_list<const char*> col_names) {
    std::pmr::vector<components::types::complex_logical_type> types(r);
    for (const char* name : col_names) {
        types.emplace_back(components::types::logical_type::BIGINT, name);
    }
    // The optimizer reads only the schema, so a minimal 1-row chunk suffices.
    auto chunk = gen_data_chunk(/*size=*/1, /*start=*/0, types, r);
    return make_node_raw_data(r, std::move(chunk));
}

// --- Filter through projection ----------------------------------------------

// in:  aggregate[ aggregate[data, select(a)], match(a > ?) ]
// out: aggregate[ data, select(a), match(a > ?) ]  -- filter pushed under the identity projection
TEST_CASE("logical_plan::pushdown_filter_under_identity_select") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    auto select = make_node_select(&resource, c);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "b")));
    inner->append_child(select);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// A renaming projection (select outputs "x" aliased from "a") does not let the
// filter on "x" move below it: "x" does not exist in the pre-projection schema.
TEST_CASE("logical_plan::pushdown_filter_skips_renamed_select_output") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    auto select = make_node_select(&resource, c);
    auto renamed = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "x"));
    renamed->append_param(key(&resource, "a"));
    select->append_expression(std::move(renamed));
    inner->append_child(select);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "x", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(inner->children().size() == 2);
}

// --- Filter through sort ----------------------------------------------------

// in:  aggregate[ aggregate[data, sort(b)], match(a > ?) ]
// out: aggregate[ data, sort(b), match(a > ?) ]  -- filter swapped below the sort
TEST_CASE("logical_plan::pushdown_filter_under_sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    std::vector<components::expressions::expression_ptr> sort_exprs;
    sort_exprs.emplace_back(make_sort_expression(key(&resource, "b"), sort_order::asc));
    inner->append_child(make_node_sort(&resource, c, sort_exprs));

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::sort_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// --- Filter through join ----------------------------------------------------

// in:  aggregate[ join[ data(a,b), data(c,d) ], match(a > ?) ]
// out: join[ aggregate[ data(a,b), match(a > ?) ], data(c,d) ]
// The predicate touches only left-side columns, so it descends into the left branch.
TEST_CASE("logical_plan::pushdown_filter_into_join_branch") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, c, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == join);
    REQUIRE(join->children().size() == 2);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->type() == node_type::data_t);

    auto pushed = join->children()[0];
    REQUIRE(pushed->children().size() == 2);
    REQUIRE(pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(pushed->children()[1]->type() == node_type::match_t);
}

// A predicate referencing columns from both join sides cannot descend into
// either branch and must stay above the join.
TEST_CASE("logical_plan::pushdown_filter_skips_join_predicate_on_both_sides") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, c, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp = make_compare_expression(&resource,
                                       compare_type::eq,
                                       key(&resource, "a", side_t::left),
                                       key(&resource, "c"));
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[0] == join);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(join->children().size() == 2);
    REQUIRE(join->children()[0]->type() == node_type::data_t);
    REQUIRE(join->children()[1]->type() == node_type::data_t);
}

// --- Filter through group by ------------------------------------------------

// in:  aggregate[ aggregate[data, group(key a, sum b)], match(a > ?) ]
// out: aggregate[ data, group(key a, sum b), match(a > ?) ]
// The predicate references only the grouping key, so it descends below the group node.
TEST_CASE("logical_plan::pushdown_filter_under_group_by_key") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    auto group = make_node_group(&resource, c);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    inner->append_child(group);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::group_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// A predicate on an aggregate output column (sum_b), not a grouping key, must
// not be pushed below the group node — it would change the query result.
TEST_CASE("logical_plan::pushdown_filter_skips_group_by_aggregate_output") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    auto group = make_node_group(&resource, c);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    inner->append_child(group);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "sum_b", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[0] == inner);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(inner->children().size() == 2);
    REQUIRE(inner->children()[1]->type() == node_type::group_t);
}

// --- Conjunction splitting through join ------------------------------------

// in:  aggregate[ join[data(a,b), data(c,d)], match((a > ?) AND (c > ?)) ]
// out: join[ aggregate[data(a,b), match(a > ?)], aggregate[data(c,d), match(c > ?)] ]
// Each conjunct references only one side, so the AND is split across branches.
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_into_both_join_branches") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, c, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_c =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{2});
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_c);

    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, c, conj));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == join);
    REQUIRE(join->children().size() == 2);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);

    auto left_pushed = join->children()[0];
    REQUIRE(left_pushed->children().size() == 2);
    REQUIRE(left_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(left_pushed->children()[1]->type() == node_type::match_t);

    auto right_pushed = join->children()[1];
    REQUIRE(right_pushed->children().size() == 2);
    REQUIRE(right_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(right_pushed->children()[1]->type() == node_type::match_t);
}

// in:  aggregate[ join[data(a,b), data(c,d)], match((a > ?) AND (a == c)) ]
// out: aggregate[ join[ aggregate[data(a,b), match(a > ?)], data(c,d) ], match(a == c) ]
// The (a == c) conjunct touches both sides and stays above the join as residual.
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_with_residual_join") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, c, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_ac = make_compare_expression(&resource,
                                          compare_type::eq,
                                          key(&resource, "a", side_t::left),
                                          key(&resource, "c"));
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_ac);

    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, c, conj));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[0] == join);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->type() == node_type::data_t);

    auto left_pushed = join->children()[0];
    REQUIRE(left_pushed->children().size() == 2);
    REQUIRE(left_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(left_pushed->children()[1]->type() == node_type::match_t);
}

// in:  aggregate[ join[data(a,b), data(c,d)], match((a > ?) AND (c > ?) AND (a == c)) ]
// out: aggregate[ join[ aggregate[data(a,b), match(a > ?)],
//                       aggregate[data(c,d), match(c > ?)] ],
//                 match(a == c) ]
// All three buckets are populated at once: the left-only and right-only
// conjuncts descend into their branches, the cross-side conjunct stays as residual.
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_into_all_three_buckets") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, c, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_c =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{2});
    auto cmp_ac = make_compare_expression(&resource,
                                          compare_type::eq,
                                          key(&resource, "a", side_t::left),
                                          key(&resource, "c"));
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_c);
    conj->append_child(cmp_ac);

    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, c, conj));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[0] == join);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(join->children().size() == 2);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);

    auto left_pushed = join->children()[0];
    REQUIRE(left_pushed->children().size() == 2);
    REQUIRE(left_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(left_pushed->children()[1]->type() == node_type::match_t);

    auto right_pushed = join->children()[1];
    REQUIRE(right_pushed->children().size() == 2);
    REQUIRE(right_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(right_pushed->children()[1]->type() == node_type::match_t);
}

// in:  aggregate[ join[data(a,b), data(c,d)], match((a > ?) AND ((b < ?) AND (c > ?))) ]
// out: join[ aggregate[data(a,b), match((a > ?) AND (b < ?))],
//            aggregate[data(c,d), match(c > ?)] ]
// The nested AND is flattened before classification. Were it treated as one
// opaque conjunct, (b < ?) AND (c > ?) would touch both join sides and stay as
// residual; flattening lets b descend left and c descend right independently.
TEST_CASE("logical_plan::pushdown_filter_flattens_nested_conjunction") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, c, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_b =
        make_compare_expression(&resource, compare_type::lt, key(&resource, "b", side_t::left), id_par{2});
    auto cmp_c =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{3});
    auto inner_conj = make_compare_union_expression(&resource, compare_type::union_and);
    inner_conj->append_child(cmp_b);
    inner_conj->append_child(cmp_c);
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(inner_conj);

    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, c, conj));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == join);
    REQUIRE(join->children().size() == 2);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);

    auto left_pushed = join->children()[0];
    REQUIRE(left_pushed->children().size() == 2);
    REQUIRE(left_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(left_pushed->children()[1]->type() == node_type::match_t);
    auto left_match = left_pushed->children()[1];
    REQUIRE(left_match->expressions().size() == 1);
    auto* left_cmp = static_cast<compare_expression_t*>(left_match->expressions()[0].get());
    REQUIRE(left_cmp->type() == compare_type::union_and);
    REQUIRE(left_cmp->children().size() == 2);

    auto right_pushed = join->children()[1];
    REQUIRE(right_pushed->children().size() == 2);
    REQUIRE(right_pushed->children()[0]->type() == node_type::data_t);
    REQUIRE(right_pushed->children()[1]->type() == node_type::match_t);
    auto right_match = right_pushed->children()[1];
    REQUIRE(right_match->expressions().size() == 1);
    auto* right_cmp = static_cast<compare_expression_t*>(right_match->expressions()[0].get());
    REQUIRE(right_cmp->type() == compare_type::gt);
}

// --- Conjunction splitting through group by --------------------------------

// in:  aggregate[ aggregate[data, group(key a, sum b->sum_b)],
//                 match((a > ?) AND (sum_b > ?)) ]
// out: aggregate[ aggregate[data, group(...), match(a > ?)], match(sum_b > ?) ]
// The grouping-key conjunct descends below group; the aggregate-output
// conjunct stays above as residual.
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_through_group_by") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    auto group = make_node_group(&resource, c);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    inner->append_child(group);

    auto cmp_key =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_agg =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "sum_b", side_t::left), id_par{2});
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_key);
    conj->append_child(cmp_agg);

    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, conj));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[0] == inner);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::group_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// --- Cost guard: pushdown under a projection -------------------------------

// data has 3 columns, the projection narrows to 1 -> pushing the filter below
// it would widen the filter's input rows, so the cost guard vetoes the move.
TEST_CASE("logical_plan::pushdown_filter_vetoed_by_narrowing_projection") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b", "c"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    auto select = make_node_select(&resource, c);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    inner->append_child(select);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(inner->children().size() == 2);
}

// A non-narrowing projection that merely reorders columns keeps the same row
// width, so the cost guard allows the pushdown.
TEST_CASE("logical_plan::pushdown_filter_allowed_through_non_narrowing_projection") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    auto select = make_node_select(&resource, c);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "b")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    inner->append_child(select);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// A projection whose visible columns are not all plain get_field (here an
// identity column alongside a computed/constant one) has an inestimable width:
// estimate_projection_width returns 0. The cost guard treats width 0 as
// "unknown", not "narrow", so it does not veto and the pushdown still happens —
// even though the three-column source is wider than the two-column projection.
TEST_CASE("logical_plan::pushdown_filter_allowed_when_projection_width_unknown") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto c = coll();
    auto data = make_data(&resource, {"a", "b", "c"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, c);
    inner->append_child(data);
    auto select = make_node_select(&resource, c);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::constant, key(&resource, "k")));
    inner->append_child(select);

    auto cmp =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, c);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, c, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}
