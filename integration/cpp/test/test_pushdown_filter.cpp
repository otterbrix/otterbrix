// Filter-pushdown tests for planner::optimize
// build a plan, optimize it, then
// check the shape of the resulting node tree

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
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/optimizer.hpp>
#include <components/tests/generaty.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const core::dbname_t db{"db"};
static const core::relname_t rel{"t"};

// data node with all BIGINT columns
static node_data_ptr make_data(std::pmr::memory_resource* r, std::initializer_list<const char*> col_names) {
    std::pmr::vector<components::types::complex_logical_type> types(r);
    for (const char* name : col_names) {
        types.emplace_back(components::types::logical_type::BIGINT, name);
    }
    // optimizer only looks at the schema, so one row is enough
    auto chunk = gen_data_chunk(/*size=*/1, /*start=*/0, types, r);
    auto node = make_node_raw_data(r, std::move(chunk));
    // Stage 3: collect_subtree_columns() now reads a node's output_types() (the
    // node_data_t column-name branch was removed). Stamp them here so a scan used
    // under a join exposes its columns to pushdown exactly as validate_schema would
    // — otherwise the columns come back empty and nothing is pushed. Build the list
    // on `r` (rule 8) and move it in.
    std::pmr::vector<components::types::complex_logical_type> out_types(r);
    for (const char* name : col_names) {
        out_types.emplace_back(components::types::logical_type::BIGINT, name);
    }
    node->set_output_types(std::move(out_types));
    return node;
}

// disk-shaped scan: an aggregate_t{db,rel} that carries its columns ONLY in
// output_types() (a real disk scan has no in-memory node_data_t child). Used to
// prove collect_subtree_columns() reads output_types() rather than a data node.
static node_aggregate_ptr
make_disk_scan(std::pmr::memory_resource* r, std::initializer_list<const char*> col_names) {
    auto agg = make_node_aggregate(r, db, rel);
    std::pmr::vector<components::types::complex_logical_type> out_types(r);
    for (const char* name : col_names) {
        out_types.emplace_back(components::types::logical_type::BIGINT, name);
    }
    agg->set_output_types(std::move(out_types));
    return agg;
}

// --- Filter through projection ----------------------------------------------

// select a, b (both columns unchanged)
// filter a > ?: pushed down
TEST_CASE("logical_plan::pushdown_filter_under_identity_select") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    auto select = make_node_select(&resource, db, rel);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "b")));
    inner->append_child(select);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// select a as x (renames a)
// filter x > ?: not pushed (x has no matching input column)
TEST_CASE("logical_plan::pushdown_filter_skips_renamed_select_output") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    auto select = make_node_select(&resource, db, rel);
    auto renamed = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "x"));
    renamed->append_param(key(&resource, "a"));
    select->append_expression(std::move(renamed));
    inner->append_child(select);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "x", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(inner->children().size() == 2);
}

// --- Filter through sort ----------------------------------------------------

// sort by b
// filter a > ?: pushed down (sort preserves rows)
TEST_CASE("logical_plan::pushdown_filter_under_sort") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    std::vector<components::expressions::expression_ptr> sort_exprs;
    sort_exprs.emplace_back(make_sort_expression(key(&resource, "b"), sort_order::asc));
    inner->append_child(make_node_sort(&resource, db, rel, sort_exprs));

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::sort_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// --- Filter through join ----------------------------------------------------

// inner join of (a, b) and (c, d)
// filter a > ?: pushed into the left branch (left columns only)
TEST_CASE("logical_plan::pushdown_filter_into_join_branch") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

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

// inner join of (a, b) and (c, d)
// filter a == c: not pushed (references both sides)
TEST_CASE("logical_plan::pushdown_filter_skips_join_predicate_on_both_sides") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp =
        make_compare_expression(&resource, compare_type::eq, key(&resource, "a", side_t::left), key(&resource, "c"));
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

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

// group by a, sum(b) as sum_b
// filter a > ?: pushed below the group (a is the grouping key)
TEST_CASE("logical_plan::pushdown_filter_under_group_by_key") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    auto group = make_node_group(&resource, db, rel);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    inner->append_child(group);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::group_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// group by a, sum(b) as sum_b
// filter sum_b > ?: not pushed (aggregate output, not a key)
TEST_CASE("logical_plan::pushdown_filter_skips_group_by_aggregate_output") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    auto group = make_node_group(&resource, db, rel);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    inner->append_child(group);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "sum_b", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[0] == inner);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(inner->children().size() == 2);
    REQUIRE(inner->children()[1]->type() == node_type::group_t);
}

// --- Conjunction splitting through join ------------------------------------

// inner join of (a, b) and (c, d)
// filter (a > ?) AND (c > ?): split into both branches
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_into_both_join_branches") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_c = make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{2});
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_c);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, conj));

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

// inner join of (a, b) and (c, d)
// filter (a > ?) AND (a == c): a > ? pushed left, a == c kept above
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_with_residual_join") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_ac =
        make_compare_expression(&resource, compare_type::eq, key(&resource, "a", side_t::left), key(&resource, "c"));
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_ac);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, conj));

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

// inner join of (a, b) and (c, d)
// filter (a > ?) AND (c > ?) AND (a == c): a left, c right, a == c kept above
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_into_all_three_buckets") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_c = make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{2});
    auto cmp_ac =
        make_compare_expression(&resource, compare_type::eq, key(&resource, "a", side_t::left), key(&resource, "c"));
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_c);
    conj->append_child(cmp_ac);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, conj));

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

// inner join of (a, b) and (c, d)
// filter (a > ?) AND ((b < ?) AND (c > ?)): flattened, then a,b pushed left and c right
TEST_CASE("logical_plan::pushdown_filter_flattens_nested_conjunction") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_data = make_data(&resource, {"a", "b"});
    auto right_data = make_data(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_data);
    join->append_child(right_data);

    auto cmp_a = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_b = make_compare_expression(&resource, compare_type::lt, key(&resource, "b", side_t::left), id_par{2});
    auto cmp_c = make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{3});
    auto inner_conj = make_compare_union_expression(&resource, compare_type::union_and);
    inner_conj->append_child(cmp_b);
    inner_conj->append_child(cmp_c);
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(inner_conj);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, conj));

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

// group by a, sum(b) as sum_b
// filter (a > ?) AND (sum_b > ?): a > ? pushed below the group, sum_b > ? kept above
TEST_CASE("logical_plan::pushdown_filter_splits_conjunction_through_group_by") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    auto group = make_node_group(&resource, db, rel);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    inner->append_child(group);

    auto cmp_key = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_agg =
        make_compare_expression(&resource, compare_type::gt, key(&resource, "sum_b", side_t::left), id_par{2});
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_key);
    conj->append_child(cmp_agg);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, conj));

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

// select a (drops b, c)
// filter a > ?: not pushed (cost guard vetoes the wider scan)
TEST_CASE("logical_plan::pushdown_filter_vetoed_by_narrowing_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b", "c"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    auto select = make_node_select(&resource, db, rel);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    inner->append_child(select);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == outer);
    REQUIRE(outer->children().size() == 2);
    REQUIRE(outer->children()[1]->type() == node_type::match_t);
    REQUIRE(inner->children().size() == 2);
}

// select b, a (reorders, same width)
// filter a > ?: pushed down
TEST_CASE("logical_plan::pushdown_filter_allowed_through_non_narrowing_projection") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    auto select = make_node_select(&resource, db, rel);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "b")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    inner->append_child(select);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

// select a, k (k is a constant, so width can't be estimated)
// filter a > ?: pushed down (cost guard doesn't veto unknown width)
TEST_CASE("logical_plan::pushdown_filter_allowed_when_projection_width_unknown") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b", "c"});

    node_aggregate_ptr inner = make_node_aggregate(&resource, db, rel);
    inner->append_child(data);
    auto select = make_node_select(&resource, db, rel);
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::constant, key(&resource, "k")));
    inner->append_child(select);

    auto cmp = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(inner);
    outer->append_child(make_node_match(&resource, db, rel, std::move(cmp)));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == inner);
    REQUIRE(inner->children().size() == 3);
    REQUIRE(inner->children()[0]->type() == node_type::data_t);
    REQUIRE(inner->children()[1]->type() == node_type::select_t);
    REQUIRE(inner->children()[2]->type() == node_type::match_t);
}

TEST_CASE("kernel_bug_proof::join_keeps_all_physical_columns") {
    auto resource = core::pmr::otterbrix_resource();
    // left {id, k}, right {k, val}: column "k" shared, both with empty result_alias
    auto left = make_data(&resource, {"id", "k"});
    auto right = make_data(&resource, {"k", "val"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left);
    join->append_child(right);
    // ON predicate on unique columns (id == val)
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    key(&resource, "id", side_t::left),
                                                    key(&resource, "val", side_t::right)));

    components::logical_plan::storage_parameters params(&resource);
    auto res = services::dispatcher::validate_schema(&resource, nullptr, join.get(), params);
    REQUIRE_FALSE(res.has_error());

    auto& schema = res.value();
    REQUIRE(schema.size() == 4);
}

TEST_CASE("kernel_bug_proof::projection_reports_selected_columns") {
    auto resource = core::pmr::otterbrix_resource();
    auto data = make_data(&resource, {"a", "b", "c"});

    auto agg = make_node_aggregate(&resource, db, rel);
    agg->append_child(data);
    auto select = make_node_select(&resource, db, rel);
    // project "c", "a"
    // reorder + drop "b"
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "c")));
    select->append_expression(make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "a")));
    agg->append_child(select);

    components::logical_plan::storage_parameters params(&resource);
    auto res = services::dispatcher::validate_schema(&resource, nullptr, agg.get(), params);
    REQUIRE_FALSE(res.has_error());

    auto& schema = res.value();
    // Projection output = [c, a] = 2 columns
    // FIXED kernel returns [c, a]
    // BUGGY kernel returns incoming [a, b, c] = 3
    REQUIRE(schema.size() == 2);
}

// --- Stage 3: pushdown reads output_types(), not a node_data_t --------------

// A disk scan is an aggregate_t{db,rel} whose columns live ONLY in output_types()
// (no in-memory node_data_t). Stage 3 made collect_subtree_columns() read
// output_types(), so single-table filters still bucket to the correct join side.
// RED before Stage 3: the old code looked for a node_data_t under each side, found
// none, produced empty column sets, and pushed nothing (out == outer).
TEST_CASE("logical_plan::pushdown_filter_into_join_branch_disk_shaped_scans") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_scan = make_disk_scan(&resource, {"a", "b"});
    auto right_scan = make_disk_scan(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_scan);
    join->append_child(right_scan);

    auto cmp_a = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_c = make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{2});
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_c);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, conj));

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    REQUIRE(out == join);
    REQUIRE(join->children().size() == 2);
    REQUIRE(join->children()[0]->type() == node_type::aggregate_t);
    REQUIRE(join->children()[1]->type() == node_type::aggregate_t);

    // Each single-table filter was pushed below its side, wrapping the disk-shaped
    // scan (still present, unchanged) in aggregate{scan, match}.
    auto left_pushed = join->children()[0];
    REQUIRE(left_pushed->children().size() == 2);
    REQUIRE(left_pushed->children()[0] == left_scan);
    REQUIRE(left_pushed->children()[1]->type() == node_type::match_t);

    auto right_pushed = join->children()[1];
    REQUIRE(right_pushed->children().size() == 2);
    REQUIRE(right_pushed->children()[0] == right_scan);
    REQUIRE(right_pushed->children()[1]->type() == node_type::match_t);
}

// --- Stage 3 part 2: pushdown below a join UNDER GROUP BY + ORDER BY ---------

// The bail-lift: a group_t AND a sort_t above an inner join no longer stop
// single-table filters from being pushed below the join. group/sort commute with
// pushing a per-side filter down (they only reorder/aggregate the join's output).
// Both filters land on their respective join side; the match above the join is
// fully consumed and dropped; group_t + sort_t stay above the join. This is the
// structural counterpart of the end-to-end repro (test_batch_execution
// "join + WHERE ... GROUP BY", test_column_projection "inner JOIN + GROUP BY with
// WHERE on non-select column") that returned 0 rows before the executor fix.
// RED before the bail-lift: the old group/sort guard returned the node unchanged,
// leaving both filters in the match above the join.
TEST_CASE("logical_plan::pushdown_filter_into_join_branch_under_group_and_sort") {
    auto resource = core::pmr::otterbrix_resource();
    auto left_scan = make_disk_scan(&resource, {"a", "b"});
    auto right_scan = make_disk_scan(&resource, {"c", "d"});

    auto join = make_node_join(&resource, db, rel, join_type::inner);
    join->append_child(left_scan);
    join->append_child(right_scan);

    auto cmp_a = make_compare_expression(&resource, compare_type::gt, key(&resource, "a", side_t::left), id_par{1});
    auto cmp_c = make_compare_expression(&resource, compare_type::gt, key(&resource, "c", side_t::left), id_par{2});
    auto conj = make_compare_union_expression(&resource, compare_type::union_and);
    conj->append_child(cmp_a);
    conj->append_child(cmp_c);

    auto group = make_node_group(&resource, db, rel);
    group->append_expression(make_scalar_expression(&resource, scalar_type::group_field, key(&resource, "a")));
    auto sum_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_b"));
    sum_expr->append_param(key(&resource, "b"));
    group->append_expression(std::move(sum_expr));

    std::vector<components::expressions::expression_ptr> sort_exprs;
    sort_exprs.emplace_back(make_sort_expression(key(&resource, "a"), sort_order::asc));
    auto sort = make_node_sort(&resource, db, rel, sort_exprs);

    node_aggregate_ptr outer = make_node_aggregate(&resource, db, rel);
    outer->append_child(join);
    outer->append_child(make_node_match(&resource, db, rel, conj));
    outer->append_child(group);
    outer->append_child(sort);

    node_ptr out = components::planner::optimize(&resource, outer, nullptr);

    // outer keeps group + sort, so it is not elided; the join stays its source.
    REQUIRE(out == outer);
    REQUIRE(outer->children()[0] == join);
    bool has_group = false, has_sort = false, has_match = false;
    for (size_t i = 1; i < outer->children().size(); ++i) {
        auto t = outer->children()[i]->type();
        if (t == node_type::group_t)
            has_group = true;
        if (t == node_type::sort_t)
            has_sort = true;
        if (t == node_type::match_t)
            has_match = true;
    }
    REQUIRE(has_group);
    REQUIRE(has_sort);
    REQUIRE_FALSE(has_match); // both filters consumed -> no residual above the join

    // Each single-table filter was pushed below its side despite group + sort.
    REQUIRE(join->children()[0]->children().size() == 2);
    REQUIRE(join->children()[0]->children()[0] == left_scan);
    REQUIRE(join->children()[0]->children()[1]->type() == node_type::match_t);
    REQUIRE(join->children()[1]->children().size() == 2);
    REQUIRE(join->children()[1]->children()[0] == right_scan);
    REQUIRE(join->children()[1]->children()[1]->type() == node_type::match_t);
}

