#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/optimizer/rules/hash_join.hpp>
#include <components/planner/optimizer/rules/promote_cross_join.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <memory_resource>
#include <utility>

using namespace components;
using namespace components::logical_plan;
using expressions::compare_type;
using expressions::side_t;

// ----------------------------------------------------------------------------
// Multi-way (nested) cross-join promotion.
//
// `FROM a, b, c WHERE a.k = b.k AND b.v = c.k` lowers to a left-deep chain
//   aggregate_t
//     child0: join{cross}( join{cross}(a, b), c )   <- source
//     child1: match_t( a_k=b_k AND b_v=c_k AND a_id!=b_v )
// with the WHERE keys stamped side=left over the merged [a|b|c] schema by the
// validator. promote_cross_joins must promote BOTH nested cross joins: the inner
// join claims a_k=b_k (its boundary), the outer join claims b_v=c_k (its
// boundary), each re-localizing its right-range key. The non-join residual
// (a_id != b_v) stays in the match. rewrite_hash_joins then lowers both to hash.
//
// This drives the real services::dispatcher::validate_schema so the keys carry
// the true both-sides-left, merged-relative paths and every scan/join node gets
// output_schema() stamped (the promote rule reads those widths).
//
// A depth-1-only rule would promote only the outer join, leaving the inner join
// a nested-loop cross join.
// ----------------------------------------------------------------------------
namespace {

    // A raw-data scan with two BIGINT columns named `c0`/`c1` — the bare column names the
    // WHERE keys resolve against, carried by the COLUMNS (M3-B5).
    node_data_ptr make_scan(std::pmr::memory_resource* res, const char* c0, const char* c1) {
        vector::schema_t schema(res);
        for (const char* name : {c0, c1}) {
            vector::column_schema_t record{res};
            record.name = name;
            record.type = types::complex_logical_type{types::logical_type::BIGINT};
            schema.push_back(std::move(record));
        }
        auto chunk = vector::make_chunk(res, schema, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, int64_t{1});
        chunk.set_value(1, 0, int64_t{2});
        return make_node_raw_data(res, std::move(chunk));
    }

    components::expressions::key_t bare_key(std::pmr::memory_resource* res, const char* name) {
        return components::expressions::key_t{res, name};
    }

    expressions::expression_ptr
    make_cmp(std::pmr::memory_resource* res, compare_type type, const char* l, const char* r) {
        return expressions::make_compare_expression(res,
                                                    type,
                                                    expressions::param_storage{bare_key(res, l)},
                                                    expressions::param_storage{bare_key(res, r)});
    }

} // namespace

TEST_CASE("optimizer::promote_cross_join::multiway_three_table") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    // Tables: a(a_id, a_k) | b(b_k, b_v) | c(c_k, c_v)
    // merged schema indices: a_id=0 a_k=1 | b_k=2 b_v=3 | c_k=4 c_v=5
    auto a = make_scan(res, "a_id", "a_k");
    auto b = make_scan(res, "b_k", "b_v");
    auto c = make_scan(res, "c_k", "c_v");

    auto inner_cross = make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::cross);
    inner_cross->append_child(a);
    inner_cross->append_child(b);
    inner_cross->append_expression(expressions::make_compare_expression(res, compare_type::all_true));

    auto outer_cross = make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::cross);
    outer_cross->append_child(inner_cross);
    outer_cross->append_child(c);
    outer_cross->append_expression(expressions::make_compare_expression(res, compare_type::all_true));

    // WHERE a_k = b_k AND b_v = c_k AND a_id != b_v
    auto where = expressions::make_compare_union_expression(res, compare_type::union_and);
    where->append_child(make_cmp(res, compare_type::eq, "a_k", "b_k"));  // straddles the inner join
    where->append_child(make_cmp(res, compare_type::eq, "b_v", "c_k"));  // straddles the outer join
    where->append_child(make_cmp(res, compare_type::ne, "a_id", "b_v")); // residual (non-join filter)
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(outer_cross);
    agg->append_child(match);

    // Real validation: stamps side()/path() on the keys and output_schema() on the
    // scans + both joins (left_width/right_width the promote rule classifies against).
    logical_plan::storage_parameters params{res};
    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params);
    REQUIRE_FALSE(validated.has_error());
    REQUIRE(inner_cross->has_output_schema());
    REQUIRE(inner_cross->output_schema().size() == 4); // [a|b]
    REQUIRE(outer_cross->has_output_schema());
    REQUIRE(outer_cross->output_schema().size() == 6); // [a|b|c]

    node_ptr root = agg;
    root = planner::optimizer::promote_cross_joins(res, root);
    root = planner::optimizer::rewrite_hash_joins(res, root);

    auto* agg_after = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(agg_after->children().size() >= 2);

    // Outer join: promoted to INNER + hash on b_v (left, merged idx 3) = c_k (right,
    // re-localized to 0 within c).
    auto outer_after = agg_after->children()[0];
    REQUIRE(outer_after->type() == node_type::join_t);
    auto* oj = static_cast<node_join_t*>(outer_after.get());
    CHECK(oj->type() == join_type::inner);
    CHECK(oj->algo() == node_join_t::join_algo::hash);
    CHECK(oj->left_col() == 3);
    CHECK(oj->right_col() == 0);
    REQUIRE(oj->children().size() == 2);
    CHECK(oj->children()[1].get() == c.get()); // right child is still table c

    // Inner join (child0 of the outer): promoted to INNER + hash on a_k (left idx 1)
    // = b_k (right, re-localized to 0 within b).
    auto inner_after = oj->children()[0];
    REQUIRE(inner_after->type() == node_type::join_t);
    auto* ij = static_cast<node_join_t*>(inner_after.get());
    CHECK(ij->type() == join_type::inner);
    CHECK(ij->algo() == node_join_t::join_algo::hash);
    CHECK(ij->left_col() == 1);
    CHECK(ij->right_col() == 0);
    REQUIRE(ij->children().size() == 2);
    CHECK(ij->children()[0].get() == a.get());
    CHECK(ij->children()[1].get() == b.get());

    // Residual match keeps only the non-join filter (a_id != b_v).
    node_ptr match_after = nullptr;
    for (size_t i = 1; i < agg_after->children().size(); ++i) {
        if (agg_after->children()[i]->type() == node_type::match_t) {
            match_after = agg_after->children()[i];
            break;
        }
    }
    REQUIRE(match_after);
    REQUIRE(match_after->expressions().size() == 1);
    auto* residual = static_cast<expressions::compare_expression_t*>(match_after->expressions()[0].get());
    CHECK(residual->type() == compare_type::ne);
}

// ----------------------------------------------------------------------------
// Characterization pin for the join-boundary width.
//
// Four sites derive "how many merged columns the LEFT input owns" from
// children()[0]->output_schema(), each with a DIFFERENT policy for the unstamped
// case. These pin promote_cross_join's: an unstamped cross boundary is not a
// promotable boundary (the join stays CROSS), and a join that IS promoted gets
// output_schema() = left ++ right so the parent reads a reliable width.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_cross_join::unstamped_boundary_is_not_promoted") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    auto a = make_scan(res, "a_id", "a_k");
    auto b = make_scan(res, "b_k", "b_v");
    auto cross = make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::cross);
    cross->append_child(a);
    cross->append_child(b);
    cross->append_expression(expressions::make_compare_expression(res, compare_type::all_true));

    auto where = expressions::make_compare_union_expression(res, compare_type::union_and);
    where->append_child(make_cmp(res, compare_type::eq, "a_k", "b_k"));
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(cross);
    agg->append_child(match);

    // Deliberately NOT validated: the children carry no output_schema() stamp, so
    // the boundary width is unknown.
    REQUIRE_FALSE(a->has_output_schema());
    REQUIRE_FALSE(b->has_output_schema());

    node_ptr root = agg;
    root = planner::optimizer::promote_cross_joins(res, root);

    auto* agg_after = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(agg_after->children().size() >= 1);
    REQUIRE(agg_after->children()[0]->type() == node_type::join_t);
    // Unknown width -> not classifiable -> left CROSS, untouched.
    CHECK(static_cast<node_join_t*>(agg_after->children()[0].get())->type() == join_type::cross);
}

TEST_CASE("optimizer::promote_cross_join::promoted_join_output_schema_is_left_then_right") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    auto a = make_scan(res, "a_id", "a_k");
    auto b = make_scan(res, "b_k", "b_v");
    auto cross = make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::cross);
    cross->append_child(a);
    cross->append_child(b);
    cross->append_expression(expressions::make_compare_expression(res, compare_type::all_true));

    auto where = expressions::make_compare_union_expression(res, compare_type::union_and);
    where->append_child(make_cmp(res, compare_type::eq, "a_k", "b_k"));
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(cross);
    agg->append_child(match);

    logical_plan::storage_parameters params{res};
    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params);
    REQUIRE_FALSE(validated.has_error());

    node_ptr root = agg;
    root = planner::optimizer::promote_cross_joins(res, root);

    auto* agg_after = static_cast<node_aggregate_t*>(root.get());
    REQUIRE(agg_after->children()[0]->type() == node_type::join_t);
    auto* promoted = static_cast<node_join_t*>(agg_after->children()[0].get());
    REQUIRE(promoted->type() == join_type::inner);
    REQUIRE(promoted->children().size() == 2);

    // The promoted join is a FRESH node the validator never saw; the rule stamps it
    // as the ordered concatenation of its children's stamps, so a consumer still
    // reads a reliable left_width from children()[0].
    const auto& l = promoted->children()[0]->output_schema();
    const auto& r = promoted->children()[1]->output_schema();
    REQUIRE(promoted->has_output_schema());
    REQUIRE(promoted->output_schema().size() == l.size() + r.size());
    for (size_t i = 0; i < l.size(); ++i) {
        CHECK(promoted->output_schema()[i].name == l[i].name);
        CHECK(promoted->output_schema()[i].type == l[i].type);
    }
    for (size_t i = 0; i < r.size(); ++i) {
        CHECK(promoted->output_schema()[l.size() + i].name == r[i].name);
        CHECK(promoted->output_schema()[l.size() + i].type == r[i].type);
    }
}
