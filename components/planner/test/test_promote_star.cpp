#include <catch2/catch_test_macros.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/expressions/key.hpp>
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
#include <components/planner/optimizer/rules/hash_join.hpp>
#include <components/planner/optimizer/rules/promote_cross_join.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <initializer_list>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace components;
using namespace components::logical_plan;
using namespace components::expressions;
using expressions::compare_type;
using expressions::scalar_type;
using expressions::side_t;
using expressions::sort_order;

// ============================================================================
// Star-schema join reordering (SSB q4 fix) — planner unit tests.
//
// Model: test_promote_multiway.cpp / test_optimizer.cpp's
// `comma_join_becomes_inner_hash`. These drive the REAL
// services::dispatcher::validate_schema on synthetic node_data scans (so the
// keys carry the true both-sides-left, merged-relative paths and every scan/join
// node gets output_schema() stamped), then run promote_cross_joins +
// rewrite_hash_joins and assert on node structure + resolved column indices.
//
// The scenario is SSB q4-1: a fact-LAST 5-table star
//   FROM dim_date, customer, supplier, part, lineorder
// which lowers to a left-deep CROSS chain
//   ((((dim_date x customer) x supplier) x part) x lineorder)
// Fact-last => the inner cross joins hold only dimension tables => no straddling
// equi => they stay CROSS (dimension cartesian). The star pre-normalizer must
// detect the star and reorder the fact to the FRONT so every boundary becomes a
// claimable fact<->dim equi:
//   ((((lineorder x dim_date) x customer) x supplier) x part)
// then the canonical promote_join_subtree lowers each level to an inner hash join
// with each dim on the (small) build side.
//
// SSB column layout and merged FROM-order offsets:
//   dim_date  (17 cols) @ 0    d_datekey=0  ... d_year=4  ...
//   customer  ( 8 cols) @ 17   c_custkey=17 ... c_nation=21 c_region=22 ...
//   supplier  ( 7 cols) @ 25   s_suppkey=25 ... s_region=30 ...
//   part      ( 9 cols) @ 32   p_partkey=32 p_name=33 p_mfgr=34 ...
//   lineorder (17 cols) @ 41   lo_orderkey=41 lo_custkey=43 lo_partkey=44
//                              lo_suppkey=45 lo_orderdate=46 ...
//                              lo_revenue=53 lo_supplycost=54 ...
// Total width = 58.
//
// The chosen fact-first order (v1 = FROM order, fact pinned to position 0) is
//   lineorder(17) @ 0, dim_date(17) @ 17, customer(8) @ 34, supplier(7) @ 42,
//   part(9) @ 49.
// Block permutation P(old) = offset_new[T] + (old - offset_old[T]):
//   d_year       4  -> 17 + (4-0)   = 21     (dim_date)
//   c_nation     21 -> 34 + (21-17) = 38     (customer)
//   c_region     22 -> 34 + (22-17) = 39     (customer)
//   s_region     30 -> 42 + (30-25) = 47     (supplier)
//   p_mfgr       34 -> 49 + (34-32) = 51     (part)
//   lo_revenue   53 ->  0 + (53-41) = 12     (lineorder)
//   lo_supplycost54 ->  0 + (54-41) = 13     (lineorder)
// ============================================================================
namespace {

    // An all-BIGINT scan whose column type aliases are the SSB column names the
    // unqualified WHERE/GROUP/SORT keys resolve against. Widths (not types) are
    // what the star reorder + P remap depend on, so BIGINT throughout is fine for
    // a planner-only test.
    node_data_ptr make_scan(std::pmr::memory_resource* res, std::initializer_list<const char*> cols) {
        // The column NAME is the bare name the WHERE / ON keys resolve against, and it lives
        // on the column (M3-B5), so the chunk is built from a schema.
        vector::schema_t schema(res);
        for (const char* name : cols) {
            vector::column_schema_t record{res};
            record.name = name;
            record.type = types::complex_logical_type{types::logical_type::BIGINT};
            schema.push_back(std::move(record));
        }
        auto chunk = vector::make_chunk(res, schema, 1);
        chunk.set_cardinality(1);
        for (size_t i = 0; i < schema.size(); ++i) {
            chunk.set_value(i, 0, static_cast<int64_t>(i + 1));
        }
        return make_node_raw_data(res, std::move(chunk));
    }

    // `key_t` (unqualified) is ambiguous — POSIX also declares a `key_t` (aka int).
    // Always name the expressions one fully-qualified and never rely on a `using`.
    components::expressions::key_t bare_key(std::pmr::memory_resource* res, const char* name) {
        return components::expressions::key_t{res, name};
    }

    // Operands must be wrapped in param_storage{...} (mirrors test_promote_multiway.cpp).
    expressions::expression_ptr
    cmp_keys(std::pmr::memory_resource* res, compare_type type, const char* l, const char* r) {
        return make_compare_expression(res,
                                       type,
                                       expressions::param_storage{bare_key(res, l)},
                                       expressions::param_storage{bare_key(res, r)});
    }

    expressions::expression_ptr eq_keys(std::pmr::memory_resource* res, const char* l, const char* r) {
        return cmp_keys(res, compare_type::eq, l, r);
    }

    expressions::expression_ptr
    cmp_key_param(std::pmr::memory_resource* res, compare_type type, const char* l, core::parameter_id_t p) {
        return make_compare_expression(res,
                                       type,
                                       expressions::param_storage{bare_key(res, l)},
                                       expressions::param_storage{p});
    }

    expressions::expression_ptr eq_key_param(std::pmr::memory_resource* res, const char* l, core::parameter_id_t p) {
        return cmp_key_param(res, compare_type::eq, l, p);
    }

    // Left-deep CROSS chain over `leaves` in FROM order, each boundary carrying an
    // all_true ON placeholder (the comma-join lowering).
    node_ptr cross_chain(std::pmr::memory_resource* res, const std::vector<node_ptr>& leaves) {
        node_ptr acc = leaves.front();
        for (size_t i = 1; i < leaves.size(); ++i) {
            auto j = make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::cross);
            j->append_child(acc);
            j->append_child(leaves[i]);
            j->append_expression(make_compare_expression(res, compare_type::all_true));
            acc = j;
        }
        return acc;
    }

    // The 5 SSB spine leaves in FROM order (fact LAST).
    struct star_leaves {
        node_data_ptr dim_date;
        node_data_ptr customer;
        node_data_ptr supplier;
        node_data_ptr part;
        node_data_ptr lineorder;

        std::vector<node_ptr> from_order() const { return {dim_date, customer, supplier, part, lineorder}; }
    };

    star_leaves make_ssb_leaves(std::pmr::memory_resource* res) {
        star_leaves s;
        s.dim_date = make_scan(res,
                               {"d_datekey",
                                "d_date",
                                "d_dayofweek",
                                "d_month",
                                "d_year",
                                "d_yearmonthnum",
                                "d_yearmonth",
                                "d_daynuminweek",
                                "d_daynuminmonth",
                                "d_daynuminyear",
                                "d_monthnuminyear",
                                "d_weeknuminyear",
                                "d_sellingseason",
                                "d_lastdayinweekfl",
                                "d_lastdayinmonthfl",
                                "d_holidayfl",
                                "d_weekdayfl"});
        s.customer = make_scan(
            res,
            {"c_custkey", "c_name", "c_address", "c_city", "c_nation", "c_region", "c_phone", "c_mktsegment"});
        s.supplier = make_scan(res, {"s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region", "s_phone"});
        s.part = make_scan(
            res,
            {"p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1", "p_color", "p_type", "p_size", "p_container"});
        s.lineorder = make_scan(res,
                                {"lo_orderkey",
                                 "lo_linenumber",
                                 "lo_custkey",
                                 "lo_partkey",
                                 "lo_suppkey",
                                 "lo_orderdate",
                                 "lo_orderpriority",
                                 "lo_shippriority",
                                 "lo_quantity",
                                 "lo_extendedprice",
                                 "lo_ordtotalprice",
                                 "lo_discount",
                                 "lo_revenue",
                                 "lo_supplycost",
                                 "lo_tax",
                                 "lo_commitdate",
                                 "lo_shipmode"});
        return s;
    }

    // The four fact<->dim equis, in FROM (WHERE) order.
    void append_star_equis(std::pmr::memory_resource* res, const expressions::compare_expression_ptr& where) {
        where->append_child(eq_keys(res, "lo_orderdate", "d_datekey")); // lineorder <-> dim_date
        where->append_child(eq_keys(res, "lo_custkey", "c_custkey"));   // lineorder <-> customer
        where->append_child(eq_keys(res, "lo_suppkey", "s_suppkey"));   // lineorder <-> supplier
        where->append_child(eq_keys(res, "lo_partkey", "p_partkey"));   // lineorder <-> part
    }

    bool is_join_equi(const expressions::expression_ptr& e) {
        if (!e || e->group() != expression_group::compare) {
            return false;
        }
        auto* c = static_cast<compare_expression_t*>(e.get());
        if (c->type() != compare_type::eq) {
            return false;
        }
        return is_key(c->left()) && is_key(c->right());
    }

    size_t count_cross(const node_ptr& node) {
        if (!node) {
            return 0;
        }
        size_t n = 0;
        if (node->type() == node_type::join_t && static_cast<node_join_t*>(node.get())->type() == join_type::cross) {
            ++n;
        }
        for (const auto& c : node->children()) {
            n += count_cross(c);
        }
        return n;
    }

    node_ptr find_child_by_type(const node_aggregate_t* agg, node_type type) {
        for (size_t i = 1; i < agg->children().size(); ++i) {
            if (agg->children()[i]->type() == type) {
                return agg->children()[i];
            }
        }
        return nullptr;
    }

    // Merged column index of the left operand of a single conjunct. Used to inspect
    // residual single-table filters.
    //
    // Empty when the operand is not a key: the containers this walks also hold union
    // conjuncts, whose operands are the null-expression sentinel that
    // make_compare_union_expression builds. Reading a key out of one is undefined now that
    // param_storage is a tagged union — it used to be a clean bad_variant_access.
    std::optional<size_t> left_key_path0(const expressions::expression_ptr& e) {
        auto* c = static_cast<compare_expression_t*>(e.get());
        if (!is_key(c->left())) {
            return std::nullopt;
        }
        return as_key(c->left()).path()[0];
    }

} // namespace

// ----------------------------------------------------------------------------
// Primary case — SSB q4-1: fact-LAST 5-table star with GROUP BY + ORDER BY.
//
// Expected AFTER the star reorder + canonical promotion + hash lowering:
//  * a fact-first left-deep tree, `lineorder` at the bottom-left leaf;
//  * all four joins inner + hash with (left_col/right_col), bottom-up:
//        lineorder x dim_date : 5/0   (lo_orderdate=5, d_datekey local 0)
//        ... x customer       : 2/0   (lo_custkey=2,  c_custkey  local 0)
//        ... x supplier       : 4/0   (lo_suppkey=4,  s_suppkey  local 0)
//        ... x part           : 3/0   (lo_partkey=3,  p_partkey  local 0)
//  * NO residual join-equi (all four claimed onto the joins);
//  * MERGED loci remapped by P: group keys d_year 4->21, c_nation 21->38;
//    aggregate args lo_revenue 53->12, lo_supplycost 54->13; residual filters
//    c_region 22->39, s_region 30->47, p_mfgr 34->51;
//  * GROUP-OUTPUT loci (sort/select keys) UNCHANGED (they index the small group
//    output schema 0,1,2 — never the merged join schema).
//
// Without the star pre-normalizer, the rule promotes only the top (fact-bearing)
// boundary, leaving three CROSS joins and the three fact-dim equis unclaimed in
// the residual match.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::fact_last_star_reordered_fact_first") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    auto params = make_parameter_node(res);
    auto p_creg = params->add_parameter(int64_t(1));
    auto p_sreg = params->add_parameter(int64_t(1));
    auto p_mfgr1 = params->add_parameter(int64_t(1));
    auto p_mfgr2 = params->add_parameter(int64_t(2));

    auto s = make_ssb_leaves(res);
    auto source = cross_chain(res, s.from_order());

    // WHERE: 4 fact-dim equis + c_region=? + s_region=? + (p_mfgr=? OR p_mfgr=?)
    auto where = make_compare_union_expression(res, compare_type::union_and);
    append_star_equis(res, where);
    where->append_child(eq_key_param(res, "c_region", p_creg));
    where->append_child(eq_key_param(res, "s_region", p_sreg));
    auto p_or = make_compare_union_expression(res, compare_type::union_or);
    p_or->append_child(eq_key_param(res, "p_mfgr", p_mfgr1));
    p_or->append_child(eq_key_param(res, "p_mfgr", p_mfgr2));
    where->append_child(p_or);
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    // GROUP BY d_year, c_nation ; SUM(lo_revenue - lo_supplycost) AS profit
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(make_scalar_expression(res, scalar_type::group_field, bare_key(res, "d_year")));
    group_exprs.emplace_back(make_scalar_expression(res, scalar_type::group_field, bare_key(res, "c_nation")));
    auto sum_profit = make_aggregate_expression(res, "sum", bare_key(res, "profit"));
    auto profit_arith = make_scalar_expression(res, scalar_type::subtract);
    profit_arith->append_param(bare_key(res, "lo_revenue"));
    profit_arith->append_param(bare_key(res, "lo_supplycost"));
    sum_profit->append_param(std::move(profit_arith));
    group_exprs.emplace_back(expression_ptr(sum_profit));
    auto group = make_node_group(res, core::dbname_t{}, core::relname_t{}, group_exprs);

    // ORDER BY d_year, c_nation  (resolved against the GROUP OUTPUT schema)
    std::vector<expression_ptr> sort_exprs;
    sort_exprs.emplace_back(make_sort_expression(bare_key(res, "d_year"), sort_order::asc));
    sort_exprs.emplace_back(make_sort_expression(bare_key(res, "c_nation"), sort_order::asc));
    auto sort = make_node_sort(res, core::dbname_t{}, core::relname_t{}, sort_exprs);

    // SELECT d_year, c_nation, profit  (resolved against the GROUP OUTPUT schema)
    auto select = make_node_select(res, core::dbname_t{}, core::relname_t{});
    select->append_expression(make_scalar_expression(res, scalar_type::get_field, bare_key(res, "d_year")));
    select->append_expression(make_scalar_expression(res, scalar_type::get_field, bare_key(res, "c_nation")));
    select->append_expression(make_scalar_expression(res, scalar_type::get_field, bare_key(res, "profit")));

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);
    agg->append_child(group);
    agg->append_child(sort);
    agg->append_child(select);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params->parameters());
    REQUIRE_FALSE(validated.has_error());

    // --- Sanity: the validator resolved every locus against the MERGED FROM-order
    // schema exactly as the offsets above predict (this pins the layout the P remap
    // is derived from). ---
    auto* g_dyear = static_cast<scalar_expression_t*>(group->expressions()[0].get());
    auto* g_cnat = static_cast<scalar_expression_t*>(group->expressions()[1].get());
    auto* g_sum = static_cast<aggregate_expression_t*>(group->expressions()[2].get());
    REQUIRE(g_dyear->key().path()[0] == 4); // d_year   (dim_date @0 + 4)
    REQUIRE(g_cnat->key().path()[0] == 21); // c_nation (customer @17 + 4)
    REQUIRE(g_sum->params().size() == 1);
    REQUIRE(is_expr(g_sum->params()[0]));
    auto* g_sub = static_cast<scalar_expression_t*>(as_expr(g_sum->params()[0]).get());
    REQUIRE(g_sub->params().size() == 2);
    REQUIRE(is_key(g_sub->params()[0]));
    REQUIRE(is_key(g_sub->params()[1]));
    REQUIRE(as_key(g_sub->params()[0]).path()[0] == 53); // lo_revenue
    REQUIRE(as_key(g_sub->params()[1]).path()[0] == 54); // lo_supplycost

    // GROUP-OUTPUT loci: sort + select carry small group-output indices (0,1,2),
    // NOT merged indices. Capture them to prove the reorder leaves them untouched.
    auto* srt_dyear = static_cast<sort_expression_t*>(sort->expressions()[0].get());
    auto* srt_cnat = static_cast<sort_expression_t*>(sort->expressions()[1].get());
    const size_t sort_dyear_before = srt_dyear->key().path()[0];
    const size_t sort_cnat_before = srt_cnat->key().path()[0];
    std::vector<size_t> select_before;
    for (auto& e : select->expressions()) {
        select_before.push_back(static_cast<scalar_expression_t*>(e.get())->key().path()[0]);
    }
    // Group output schema is [d_year(0), c_nation(1), profit(2)]; the sort keys
    // index THAT, not the merged join schema (merged d_year/c_nation would be 4/21).
    REQUIRE(sort_dyear_before == 0);
    REQUIRE(sort_cnat_before == 1);

    // The rule under test, then the hash selection that runs after it.
    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);
    REQUIRE(out.get() == agg.get());
    auto* agg_after = static_cast<node_aggregate_t*>(out.get());

    // --- Fact-first left-deep tree ---------------------------------------------
    // agg child0 = outermost join (x part), descend to the bottom-left lineorder leaf.
    REQUIRE(agg_after->children()[0]->type() == node_type::join_t);
    auto* j_part = static_cast<node_join_t*>(agg_after->children()[0].get());
    CHECK(j_part->type() == join_type::inner);
    CHECK(j_part->algo() == node_join_t::join_algo::hash);
    CHECK(j_part->left_col() == 3);
    CHECK(j_part->right_col() == 0);
    REQUIRE(j_part->children().size() == 2);
    CHECK(j_part->children()[1].get() == s.part.get());

    auto* j_supp = static_cast<node_join_t*>(j_part->children()[0].get());
    REQUIRE(j_supp->type() == join_type::inner);
    CHECK(j_supp->algo() == node_join_t::join_algo::hash);
    CHECK(j_supp->left_col() == 4);
    CHECK(j_supp->right_col() == 0);
    CHECK(j_supp->children()[1].get() == s.supplier.get());

    auto* j_cust = static_cast<node_join_t*>(j_supp->children()[0].get());
    REQUIRE(j_cust->type() == join_type::inner);
    CHECK(j_cust->algo() == node_join_t::join_algo::hash);
    CHECK(j_cust->left_col() == 2);
    CHECK(j_cust->right_col() == 0);
    CHECK(j_cust->children()[1].get() == s.customer.get());

    auto* j_date = static_cast<node_join_t*>(j_cust->children()[0].get());
    REQUIRE(j_date->type() == join_type::inner);
    CHECK(j_date->algo() == node_join_t::join_algo::hash);
    CHECK(j_date->left_col() == 5);
    CHECK(j_date->right_col() == 0);
    CHECK(j_date->children()[0].get() == s.lineorder.get()); // fact bottom-left
    CHECK(j_date->children()[1].get() == s.dim_date.get());

    // --- Residual match: no join-equi left; single-table filters P-remapped ------
    auto match_after = find_child_by_type(agg_after, node_type::match_t);
    REQUIRE(match_after);
    REQUIRE(match_after->expressions().size() == 1);
    auto* residual = static_cast<compare_expression_t*>(match_after->expressions()[0].get());
    REQUIRE(residual->type() == compare_type::union_and);
    REQUIRE(residual->children().size() == 3); // c_region, s_region, (p_mfgr OR)
    for (const auto& conj : residual->children()) {
        CHECK_FALSE(is_join_equi(conj)); // every fact-dim equi was claimed onto a join
    }
    CHECK(left_key_path0(residual->children()[0]) == 39); // c_region 22 -> 39
    CHECK(left_key_path0(residual->children()[1]) == 47); // s_region 30 -> 47
    auto* or_after = static_cast<compare_expression_t*>(residual->children()[2].get());
    REQUIRE(or_after->type() == compare_type::union_or);
    CHECK(left_key_path0(or_after->children()[0]) == 51); // p_mfgr 34 -> 51
    CHECK(left_key_path0(or_after->children()[1]) == 51);

    // --- MERGED group/aggregate loci remapped by P ------------------------------
    // Re-fetch from the optimized tree (robust to whether Phase-B mutates in place
    // or rebuilds the sibling nodes).
    auto group_after = find_child_by_type(agg_after, node_type::group_t);
    REQUIRE(group_after);
    auto* g_dyear_a = static_cast<scalar_expression_t*>(group_after->expressions()[0].get());
    auto* g_cnat_a = static_cast<scalar_expression_t*>(group_after->expressions()[1].get());
    auto* g_sum_a = static_cast<aggregate_expression_t*>(group_after->expressions()[2].get());
    REQUIRE(is_expr(g_sum_a->params()[0]));
    auto* g_sub_a = static_cast<scalar_expression_t*>(as_expr(g_sum_a->params()[0]).get());
    CHECK(g_dyear_a->key().path()[0] == 21);             // d_year        4  -> 21
    CHECK(g_cnat_a->key().path()[0] == 38);              // c_nation      21 -> 38
    REQUIRE(is_key(g_sub_a->params()[0]));
    REQUIRE(is_key(g_sub_a->params()[1]));
    CHECK(as_key(g_sub_a->params()[0]).path()[0] == 12); // lo_revenue    53 -> 12
    CHECK(as_key(g_sub_a->params()[1]).path()[0] == 13); // lo_supplycost 54 -> 13

    // --- GROUP-OUTPUT loci UNCHANGED --------------------------------------------
    auto sort_after = find_child_by_type(agg_after, node_type::sort_t);
    auto select_after = find_child_by_type(agg_after, node_type::select_t);
    REQUIRE(sort_after);
    REQUIRE(select_after);
    CHECK(static_cast<sort_expression_t*>(sort_after->expressions()[0].get())->key().path()[0] == sort_dyear_before);
    CHECK(static_cast<sort_expression_t*>(sort_after->expressions()[1].get())->key().path()[0] == sort_cnat_before);
    for (size_t i = 0; i < select_after->expressions().size(); ++i) {
        CHECK(static_cast<scalar_expression_t*>(select_after->expressions()[i].get())->key().path()[0] ==
              select_before[i]);
    }
}

// ----------------------------------------------------------------------------
// Chain-negative — the 3-table chain from test_promote_multiway must be left
// exactly as the canonical path handles it. The star pre-normalizer's load-bearing
// short-circuit: when EVERY boundary already claims an equi, the input is
// a chain / fact-threaded shape (here the middle `b` is incident to both edges and
// would be MIS-SELECTED as the "fact" and reordered), so it returns source
// unchanged and the canonical path promotes it exactly as today.
//
// Observable proof of "not reordered": leaves stay in FROM order (a,b,c) and the
// two joins claim their ORIGINAL boundaries (a_k=b_k -> 1/0, b_v=c_k -> 3/0). A
// mistaken star reorder would permute the leaves and change these columns.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::three_table_chain_not_reordered") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    // a(a_id, a_k) | b(b_k, b_v) | c(c_k, c_v)  => merged a_id=0 a_k=1 b_k=2 b_v=3 c_k=4 c_v=5
    auto a = make_scan(res, {"a_id", "a_k"});
    auto b = make_scan(res, {"b_k", "b_v"});
    auto c = make_scan(res, {"c_k", "c_v"});
    auto source = cross_chain(res, {a, b, c});

    auto where = make_compare_union_expression(res, compare_type::union_and);
    where->append_child(eq_keys(res, "a_k", "b_k")); // straddles the inner join
    where->append_child(eq_keys(res, "b_v", "c_k")); // straddles the outer join
    where->append_child(cmp_keys(res, compare_type::ne, "a_id", "b_v"));
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);

    logical_plan::storage_parameters sp{res};
    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), sp);
    REQUIRE_FALSE(validated.has_error());

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);
    auto* agg_after = static_cast<node_aggregate_t*>(out.get());

    // Outer join claims b_v=c_k, keeps `c` on the right (NOT reordered).
    auto* oj = static_cast<node_join_t*>(agg_after->children()[0].get());
    CHECK(oj->type() == join_type::inner);
    CHECK(oj->algo() == node_join_t::join_algo::hash);
    CHECK(oj->left_col() == 3);
    CHECK(oj->right_col() == 0);
    CHECK(oj->children()[1].get() == c.get());

    // Inner join claims a_k=b_k, leaves a,b in FROM order.
    auto* ij = static_cast<node_join_t*>(oj->children()[0].get());
    CHECK(ij->type() == join_type::inner);
    CHECK(ij->algo() == node_join_t::join_algo::hash);
    CHECK(ij->left_col() == 1);
    CHECK(ij->right_col() == 0);
    CHECK(ij->children()[0].get() == a.get());
    CHECK(ij->children()[1].get() == b.get());

    CHECK(count_cross(agg_after->children()[0]) == 0);
}

// ----------------------------------------------------------------------------
// No-group computed ORDER BY — a fact-LAST star WITHOUT a group. Its sort/select
// index the MERGED join schema, so the reorder must P-remap them. The computed
// ORDER BY is a scalar_expression whose OWN key carries the sort direction in
// path()[0] (0=asc, 1=desc) and whose operands are the merged column keys.
//
// Expect after reorder: the sort operands are remapped (lo_revenue 53->12,
// lo_supplycost 54->13), the direction (desc) is preserved on the OWN key, and the
// no-group SELECT get_field is remapped too (lo_orderkey 41->0).
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::no_group_computed_order_by_remapped") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    logical_plan::storage_parameters sp{res};

    auto s = make_ssb_leaves(res);
    auto source = cross_chain(res, s.from_order());

    auto where = make_compare_union_expression(res, compare_type::union_and);
    append_star_equis(res, where);
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    // ORDER BY (lo_revenue - lo_supplycost) DESC — encoded exactly as the SQL
    // transformer does: a subtract scalar, direction in the OWN key's path()[0].
    components::expressions::key_t order_key(res);
    order_key.set_path({size_t(1)}); // 1 == descending
    auto computed_sort = make_scalar_expression(res, scalar_type::subtract, order_key);
    computed_sort->append_param(bare_key(res, "lo_revenue"));
    computed_sort->append_param(bare_key(res, "lo_supplycost"));
    std::vector<expression_ptr> sort_exprs;
    sort_exprs.emplace_back(expression_ptr(computed_sort));
    auto sort = make_node_sort(res, core::dbname_t{}, core::relname_t{}, sort_exprs);

    // A minimal projection so the (no-group) aggregate has a SELECT list.
    auto select = make_node_select(res, core::dbname_t{}, core::relname_t{});
    select->append_expression(make_scalar_expression(res, scalar_type::get_field, bare_key(res, "lo_orderkey")));

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);
    agg->append_child(sort);
    agg->append_child(select);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), sp);
    REQUIRE_FALSE(validated.has_error());
    // Pre-reorder merged coordinates.
    REQUIRE(is_key(computed_sort->params()[0]));
    REQUIRE(is_key(computed_sort->params()[1]));
    REQUIRE(as_key(computed_sort->params()[0]).path()[0] == 53);
    REQUIRE(as_key(computed_sort->params()[1]).path()[0] == 54);
    REQUIRE(computed_sort->key().path()[0] == 1);
    auto* sel0 = static_cast<scalar_expression_t*>(select->expressions()[0].get());
    REQUIRE(sel0->key().path()[0] == 41); // lo_orderkey merged

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);

    // Sort operands P-remapped; ASC/DESC preserved on the own key.
    REQUIRE(is_key(computed_sort->params()[0]));
    REQUIRE(is_key(computed_sort->params()[1]));
    CHECK(as_key(computed_sort->params()[0]).path()[0] == 12); // lo_revenue    53 -> 12
    CHECK(as_key(computed_sort->params()[1]).path()[0] == 13); // lo_supplycost 54 -> 13
    CHECK(computed_sort->key().path()[0] == 1);                // DESC preserved
    CHECK(sel0->key().path()[0] == 0);                         // lo_orderkey   41 -> 0
    CHECK(count_cross(out->children()[0]) == 0);               // fully promoted fact-first
}

// ----------------------------------------------------------------------------
// No-group SELECT CASE — a fact-LAST star WITHOUT a group, projecting a real SQL
// CASE. The CASE lowers to a case_expr scalar whose params are [condition, result,
// ..., default]; the condition is a compare_expression whose column keys index the
// MERGED schema. The walker must recurse INTO the case_expr and its nested compare
// and P-remap the condition key (the OWN case key is an alias/sentinel — untouched).
//
// Expect after reorder: the CASE condition key lo_revenue is remapped 53->12.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::no_group_select_case_condition_remapped") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    auto params = make_parameter_node(res);
    auto p_hi = params->add_parameter(int64_t(1000));
    auto p_then = params->add_parameter(int64_t(1));
    auto p_else = params->add_parameter(int64_t(0));

    auto s = make_ssb_leaves(res);
    auto source = cross_chain(res, s.from_order());

    auto where = make_compare_union_expression(res, compare_type::union_and);
    append_star_equis(res, where);
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    // SELECT CASE WHEN lo_revenue > ? THEN ? ELSE ? END AS flag
    auto cond = cmp_key_param(res, compare_type::gt, "lo_revenue", p_hi);
    auto case_expr = make_scalar_expression(res, scalar_type::case_expr, bare_key(res, "flag"));
    case_expr->append_param(expression_ptr(cond)); // condition
    case_expr->append_param(p_then);               // result
    case_expr->append_param(p_else);               // default (ELSE)
    auto select = make_node_select(res, core::dbname_t{}, core::relname_t{});
    select->append_expression(expression_ptr(case_expr));

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);
    agg->append_child(select);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), params->parameters());
    REQUIRE_FALSE(validated.has_error());
    // The condition key resolved against the merged schema.
    REQUIRE(is_expr(case_expr->params()[0]));
    auto* cond_after_validate = static_cast<compare_expression_t*>(as_expr(case_expr->params()[0]).get());
    REQUIRE(is_key(cond_after_validate->left()));
    REQUIRE(as_key(cond_after_validate->left()).path()[0] == 53); // lo_revenue merged

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);

    REQUIRE(is_expr(case_expr->params()[0]));
    auto* cond_after = static_cast<compare_expression_t*>(as_expr(case_expr->params()[0]).get());
    REQUIRE(is_key(cond_after->left()));
    CHECK(as_key(cond_after->left()).path()[0] == 12); // lo_revenue 53 -> 12 (condition remapped)
    CHECK(count_cross(out->children()[0]) == 0);
}

// ----------------------------------------------------------------------------
// SELECT * bail — a fact-LAST star with NO explicit projection (SELECT *) leaks the
// merged column order upward, which the reorder would silently permute. The star
// pre-normalizer must bail (leave the tree pristine) so the canonical path runs its
// per-boundary promotion: fact-last => only the top (fact-bearing) boundary claims,
// three CROSS joins remain. Assert the canonical PARTIAL promotion (CROSS joins
// survive) — i.e. the star full-reorder did NOT run.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::select_star_bails_to_canonical") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    logical_plan::storage_parameters sp{res};

    auto s = make_ssb_leaves(res);
    auto source = cross_chain(res, s.from_order());

    auto where = make_compare_union_expression(res, compare_type::union_and);
    append_star_equis(res, where);
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    // No node_select / no node_group at all => pure SELECT * (merged order leaks).
    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), sp);
    REQUIRE_FALSE(validated.has_error());

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);

    // Canonical partial promotion: at least one CROSS join survives (the dimension
    // boundaries the fact-last shape cannot claim). A full star reorder would leave
    // zero CROSS joins, so this pins "bailed, did not reorder".
    CHECK(count_cross(out->children()[0]) >= 1);
    // The fact stays where the FROM order put it (bottom-left is dim_date, not
    // lineorder) — the tree was not fact-fronted.
    auto* top = static_cast<node_join_t*>(out->children()[0].get());
    CHECK(top->children()[1].get() == s.lineorder.get()); // lineorder still the last (right) leaf
}

// ----------------------------------------------------------------------------
// Fact-last snowflake — a dim-dim edge makes this NOT a clean single-fact star, so
// the pre-normalizer bails and the canonical per-boundary promotion runs (a partial
// promotion, not a full reorder).
//
// FROM d1, d2, d3, fact with edges fact<->d1, fact<->d3, and a snowflake d2<->d3
// (d2 has NO fact edge). Fact LAST lowers to (((d1 x d2) x d3) x fact):
//   (d1 x d2)          : no straddling equi          -> stays CROSS
//   (.. x d3)          : d2<->d3 straddles           -> inner
//   (.. x fact)        : fact<->d1 (or fact<->d3)    -> inner
// => exactly one surviving CROSS.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::snowflake_bails_to_partial_promotion") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    logical_plan::storage_parameters sp{res};

    // d1(d1_k, d1_v) | d2(d2_k, d2_r) | d3(d3_k, d3_r) | fact(f_d1, f_d3, f_v)
    // merged: d1_k=0 d1_v=1 | d2_k=2 d2_r=3 | d3_k=4 d3_r=5 | f_d1=6 f_d3=7 f_v=8
    auto d1 = make_scan(res, {"d1_k", "d1_v"});
    auto d2 = make_scan(res, {"d2_k", "d2_r"});
    auto d3 = make_scan(res, {"d3_k", "d3_r"});
    auto fact = make_scan(res, {"f_d1", "f_d3", "f_v"});
    auto source = cross_chain(res, {d1, d2, d3, fact});

    auto where = make_compare_union_expression(res, compare_type::union_and);
    where->append_child(eq_keys(res, "f_d1", "d1_k")); // fact <-> d1
    where->append_child(eq_keys(res, "f_d3", "d3_k")); // fact <-> d3
    where->append_child(eq_keys(res, "d2_r", "d3_r")); // d2 <-> d3 (snowflake edge)
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), sp);
    REQUIRE_FALSE(validated.has_error());

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);

    // Partial promotion: at least one CROSS remains, and the leaves were NOT
    // fact-fronted (fact stays the outermost right leaf).
    CHECK(count_cross(out->children()[0]) >= 1);
    auto* top = static_cast<node_join_t*>(out->children()[0].get());
    CHECK(top->children()[1].get() == fact.get());
}

// ----------------------------------------------------------------------------
// Bail-then-pristine — a star with a composite fact<->dim key (two equis for the
// SAME (fact,dim) pair) is not a clean single-key star; the detector must track
// multiplicity and bail, leaving the tree pristine for the canonical partial
// promotion. Whatever the precise bail trigger (detection multiplicity here, or a
// Phase-A verify failure on some other shape), the OBSERVABLE contract is identical:
// the star full-reorder does NOT run, and the canonical path promotes what it can.
//
// FROM d1, d2, fact ; fact<->d1 via TWO equis (f_a=d1_a AND f_b=d1_b), fact<->d2 via
// one. Fact LAST ((d1 x d2) x fact): (d1 x d2) stays CROSS (no straddle); the top
// boundary claims one equi -> partial.
// ----------------------------------------------------------------------------
TEST_CASE("optimizer::promote_star::composite_key_bails_to_partial_promotion") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    logical_plan::storage_parameters sp{res};

    // d1(d1_a, d1_b) | d2(d2_k, d2_v) | fact(f_a, f_b, f_d2)
    auto d1 = make_scan(res, {"d1_a", "d1_b"});
    auto d2 = make_scan(res, {"d2_k", "d2_v"});
    auto fact = make_scan(res, {"f_a", "f_b", "f_d2"});
    auto source = cross_chain(res, {d1, d2, fact});

    auto where = make_compare_union_expression(res, compare_type::union_and);
    where->append_child(eq_keys(res, "f_a", "d1_a"));  // fact <-> d1  (composite, key 1)
    where->append_child(eq_keys(res, "f_b", "d1_b"));  // fact <-> d1  (composite, key 2)
    where->append_child(eq_keys(res, "f_d2", "d2_k")); // fact <-> d2
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    auto agg = make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
    agg->append_child(source);
    agg->append_child(match);

    auto validated = services::dispatcher::validate_schema(res, nullptr, agg.get(), sp);
    REQUIRE_FALSE(validated.has_error());

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);

    // Not fact-fronted; a CROSS survives (canonical partial promotion).
    CHECK(count_cross(out->children()[0]) >= 1);
    auto* top = static_cast<node_join_t*>(out->children()[0].get());
    CHECK(top->children()[1].get() == fact.get());
}
