#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>

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
#include <string>
#include <vector>

using namespace components;
using namespace components::logical_plan;
using namespace components::expressions;
using expressions::compare_type;
using expressions::scalar_type;
using expressions::sort_order;

// ============================================================================
// Star-schema join reordering (SSB q4 fix) — end-to-end.
//
// A SMALL SSB-q4-shaped 5-table star: one fact table + four dimensions, joined by
// comma-join, with single-table filters (including an OR group like q4's p_mfgr),
// a GROUP BY, a two-column SUM arithmetic (SUM(f_rev - f_cost), like q4's
// SUM(lo_revenue - lo_supplycost)) and an ORDER BY. Fact is LAST in the FROM list
// (the shape that pushes the star reorder).
//
// This is the ONLY test that runs the reorder + aggregate-arg remap under REAL
// EXECUTION on `aggregate_t` table-scan leaves:
//   Part A asserts the result rows are correct (a wrong aggregate-arg or a wrong
//     column remap changes the SUM values or the group keys — caught here).
//   Part B builds the identical star as a hand plan (node_data leaves), drives the
//     real validator + promote_cross_joins + rewrite_hash_joins, and inspects the
//     optimized plan: N inner hash joins, no `$type: cross`, no residual join-equi.
// ============================================================================

static const std::string db = "starjoindb";

TEST_CASE("integration::cpp::star_join_e2e::rows_correct") {
    auto config = test_create_config(test_temp_path("test_star_join_e2e/rows"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE " + db + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };
    auto create = [&](const std::string& t) { REQUIRE(run("CREATE TABLE " + db + "." + t + "();")->is_success()); };

    // dim_date, customer, supplier, part, fact — fact LAST (the q4 FROM order).
    create("dd");   // (d_key, d_year)
    create("cust"); // (c_key, c_nation, c_region)
    create("supp"); // (s_key, s_region)
    create("prt");  // (p_key, p_mfgr)
    create("fct");  // (f_dk, f_ck, f_sk, f_pk, f_rev, f_cost)

    REQUIRE(run("INSERT INTO " + db + ".dd (d_key, d_year) VALUES (1, 1997), (2, 1998);")->is_success());
    REQUIRE(run("INSERT INTO " + db +
                ".cust (c_key, c_nation, c_region) VALUES "
                "(10, 'BRAZIL', 'AMERICA'), (11, 'CANADA', 'AMERICA'), (12, 'FRANCE', 'EUROPE');")
                ->is_success());
    REQUIRE(run("INSERT INTO " + db + ".supp (s_key, s_region) VALUES (20, 'AMERICA'), (21, 'EUROPE');")->is_success());
    REQUIRE(run("INSERT INTO " + db + ".prt (p_key, p_mfgr) VALUES (30, 'MFGR#1'), (31, 'MFGR#2'), (32, 'MFGR#3');")
                ->is_success());
    // fact rows: (date, cust, supp, part, revenue, cost)
    //   surviving all filters (c_region/s_region = AMERICA, p_mfgr in {1,2}):
    //     (1,10,20,30,100,40)  -> profit 60,  (1997, BRAZIL)
    //     (1,10,20,31,200,90)  -> profit 110, (1997, BRAZIL)   [same group]
    //     (2,11,20,30,300,100) -> profit 200, (1998, CANADA)
    //     (2,11,20,31,500,150) -> profit 350, (1998, CANADA)   [same group]
    //   filtered/dropped:
    //     (1,12,20,30,999,1)   -> c_region EUROPE  (filtered)
    //     (1,10,21,30,999,1)   -> s_region EUROPE  (filtered)
    //     (1,10,20,32,999,1)   -> p_mfgr MFGR#3    (filtered by the OR group)
    //     (1,99,20,30,999,1)   -> no customer key  (dropped by the inner join)
    REQUIRE(run("INSERT INTO " + db +
                ".fct (f_dk, f_ck, f_sk, f_pk, f_rev, f_cost) VALUES "
                "(1, 10, 20, 30, 100, 40), (1, 10, 20, 31, 200, 90), "
                "(2, 11, 20, 30, 300, 100), (2, 11, 20, 31, 500, 150), "
                "(1, 12, 20, 30, 999, 1), (1, 10, 21, 30, 999, 1), "
                "(1, 10, 20, 32, 999, 1), (1, 99, 20, 30, 999, 1);")
                ->is_success());

    // SSB-q4-shaped star. Fact LAST; unqualified WHERE columns (distinct names).
    const std::string sql = "SELECT d_year, c_nation, SUM(f_rev - f_cost) AS profit "
                            "FROM " +
                            db + ".dd, " + db + ".cust, " + db + ".supp, " + db + ".prt, " + db +
                            ".fct "
                            "WHERE f_dk = d_key AND f_ck = c_key AND f_sk = s_key AND f_pk = p_key "
                            "AND c_region = 'AMERICA' AND s_region = 'AMERICA' "
                            "AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') "
                            "GROUP BY d_year, c_nation ORDER BY d_year, c_nation;";

    auto cur = run(sql);
    REQUIRE(cur->is_success());
    // Two surviving groups, ORDER BY d_year, c_nation ASC:
    //   (1997, BRAZIL): SUM(profit) = 60 + 110 = 170
    //   (1998, CANADA): SUM(profit) = 200 + 350 = 550
    REQUIRE(cur->size() == 2);

    REQUIRE(cur->value(0, 0).value<int64_t>() == 1997);
    REQUIRE(cur->value(1, 0).value<std::string_view>() == "BRAZIL");
    REQUIRE(cur->value(2, 0).value<int64_t>() == 170);

    REQUIRE(cur->value(0, 1).value<int64_t>() == 1998);
    REQUIRE(cur->value(1, 1).value<std::string_view>() == "CANADA");
    REQUIRE(cur->value(2, 1).value<int64_t>() == 550);
}

// ----------------------------------------------------------------------------
// Part B — plan structure. Rebuild the identical star as a hand plan over
// node_data leaves (mirroring the five tables), drive the real validator, then run
// promote_cross_joins + rewrite_hash_joins and inspect the optimized plan.
//
// Small FROM-order layout (fact LAST):
//   dd(2) @0  cust(3) @2  supp(2) @5  prt(2) @7  fct(6) @9   (total 15)
// Fact-first target order fct(6) @0, dd(2) @6, cust(3) @8, supp(2) @11, prt(2) @13,
// so the four joins come out (left_col/right_col) bottom-up 0/0, 1/0, 2/0, 3/0.
//
// Without the star reorder, the fact-last star promotes only the top
// boundary, leaving three CROSS joins and three unclaimed fact-dim equis in the
// residual match.
// ----------------------------------------------------------------------------
namespace {

    node_data_ptr make_scan(std::pmr::memory_resource* res, std::initializer_list<const char*> cols) {
        // The column NAME the join / WHERE keys resolve against lives on the COLUMN (M3-B5).
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
    expression_ptr eq_keys(std::pmr::memory_resource* res, const char* l, const char* r) {
        return make_compare_expression(res,
                                       compare_type::eq,
                                       expressions::param_storage{bare_key(res, l)},
                                       expressions::param_storage{bare_key(res, r)});
    }

    expression_ptr eq_key_param(std::pmr::memory_resource* res, const char* l, core::parameter_id_t p) {
        return make_compare_expression(res,
                                       compare_type::eq,
                                       expressions::param_storage{bare_key(res, l)},
                                       expressions::param_storage{p});
    }

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

    size_t count_substr(const std::string& hay, const std::string& needle) {
        size_t n = 0;
        for (size_t pos = hay.find(needle); pos != std::string::npos; pos = hay.find(needle, pos + needle.size())) {
            ++n;
        }
        return n;
    }

    // Any residual join-equi (eq(column_key, column_key)) surviving in a match node
    // anywhere in the optimized tree — the fix must have claimed them all onto joins.
    bool has_residual_join_equi(const node_ptr& node) {
        if (!node) {
            return false;
        }
        if (node->type() == node_type::match_t) {
            std::pmr::vector<expression_ptr> stack{node->resource()};
            for (const auto& e : node->expressions()) {
                stack.push_back(e);
            }
            while (!stack.empty()) {
                auto e = stack.back();
                stack.pop_back();
                if (!e || e->group() != expression_group::compare) {
                    continue;
                }
                auto* c = static_cast<compare_expression_t*>(e.get());
                if (c->type() == compare_type::eq && is_key(c->left()) && is_key(c->right())) {
                    return true;
                }
                for (const auto& ch : c->children()) {
                    stack.push_back(ch);
                }
            }
        }
        for (const auto& ch : node->children()) {
            if (has_residual_join_equi(ch)) {
                return true;
            }
        }
        return false;
    }

} // namespace

TEST_CASE("integration::cpp::star_join_e2e::optimized_plan_all_hash_no_cross") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    auto params = make_parameter_node(res);
    auto p_creg = params->add_parameter(int64_t(1));
    auto p_sreg = params->add_parameter(int64_t(1));
    auto p_m1 = params->add_parameter(int64_t(1));
    auto p_m2 = params->add_parameter(int64_t(2));

    auto dd = make_scan(res, {"d_key", "d_year"});
    auto cust = make_scan(res, {"c_key", "c_nation", "c_region"});
    auto supp = make_scan(res, {"s_key", "s_region"});
    auto prt = make_scan(res, {"p_key", "p_mfgr"});
    auto fct = make_scan(res, {"f_dk", "f_ck", "f_sk", "f_pk", "f_rev", "f_cost"});
    auto source = cross_chain(res, {dd, cust, supp, prt, fct});

    auto where = make_compare_union_expression(res, compare_type::union_and);
    where->append_child(eq_keys(res, "f_dk", "d_key"));
    where->append_child(eq_keys(res, "f_ck", "c_key"));
    where->append_child(eq_keys(res, "f_sk", "s_key"));
    where->append_child(eq_keys(res, "f_pk", "p_key"));
    where->append_child(eq_key_param(res, "c_region", p_creg));
    where->append_child(eq_key_param(res, "s_region", p_sreg));
    auto p_or = make_compare_union_expression(res, compare_type::union_or);
    p_or->append_child(eq_key_param(res, "p_mfgr", p_m1));
    p_or->append_child(eq_key_param(res, "p_mfgr", p_m2));
    where->append_child(p_or);
    auto match = make_node_match(res, core::dbname_t{}, core::relname_t{}, where);

    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(make_scalar_expression(res, scalar_type::group_field, bare_key(res, "d_year")));
    group_exprs.emplace_back(make_scalar_expression(res, scalar_type::group_field, bare_key(res, "c_nation")));
    auto sum_profit = make_aggregate_expression(res, "sum", bare_key(res, "profit"));
    auto profit_arith = make_scalar_expression(res, scalar_type::subtract);
    profit_arith->append_param(bare_key(res, "f_rev"));
    profit_arith->append_param(bare_key(res, "f_cost"));
    sum_profit->append_param(std::move(profit_arith));
    group_exprs.emplace_back(expression_ptr(sum_profit));
    auto group = make_node_group(res, core::dbname_t{}, core::relname_t{}, group_exprs);

    std::vector<expression_ptr> sort_exprs;
    sort_exprs.emplace_back(make_sort_expression(bare_key(res, "d_year"), sort_order::asc));
    sort_exprs.emplace_back(make_sort_expression(bare_key(res, "c_nation"), sort_order::asc));
    auto sort = make_node_sort(res, core::dbname_t{}, core::relname_t{}, sort_exprs);

    // An EXPLICIT projection (present, not SELECT *) so the reorder does not bail.
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

    node_ptr out = planner::optimizer::promote_cross_joins(res, agg);
    out = planner::optimizer::rewrite_hash_joins(res, out);

    const std::string plan = out->to_string();
    INFO(plan);
    // Four inner hash joins, no cross join survives.
    CHECK(count_substr(plan, "$algo: hash") == 4);
    CHECK(plan.find("$type: cross") == std::string::npos);
    // No fact-dim equi left behind in any residual match — all claimed onto joins.
    CHECK_FALSE(has_residual_join_equi(out));

    // Fact-first left-deep tree with the expected equi columns (bottom-up 0/0,1/0,2/0,3/0).
    auto* j_prt = static_cast<node_join_t*>(out->children()[0].get());
    REQUIRE(j_prt->type() == join_type::inner);
    CHECK(j_prt->left_col() == 3);
    CHECK(j_prt->right_col() == 0);
    CHECK(j_prt->children()[1].get() == prt.get());
    auto* j_supp = static_cast<node_join_t*>(j_prt->children()[0].get());
    CHECK(j_supp->left_col() == 2);
    CHECK(j_supp->right_col() == 0);
    auto* j_cust = static_cast<node_join_t*>(j_supp->children()[0].get());
    CHECK(j_cust->left_col() == 1);
    CHECK(j_cust->right_col() == 0);
    auto* j_dd = static_cast<node_join_t*>(j_cust->children()[0].get());
    CHECK(j_dd->left_col() == 0);
    CHECK(j_dd->right_col() == 0);
    CHECK(j_dd->children()[0].get() == fct.get()); // fact at the bottom-left
    CHECK(j_dd->children()[1].get() == dd.get());
}

// ============================================================================
// Eager (partial) aggregation pushdown through an inner join — end-to-end.
//
// `SELECT g, MIN/MAX(x) FROM a JOIN b ON a.k = b.k GROUP BY g` where g, k and x
// all live on `a`, and `a` has many rows per (g, k). The eager_aggregation rule
// pushes a MIN/MAX PARTIAL reduce (grouped by g, k) onto side `a` before the
// join, leaving a FINAL merge above it. Part A proves the rewritten plan still
// returns the correct rows; the plan asserts the partial is physically pushed
// under the Hash Join for MIN/MAX and NOT for SUM (which is excluded).
// ============================================================================
TEST_CASE("integration::cpp::eager_aggregation::min_max_pushed_sum_not") {
    auto config = test_create_config(test_temp_path("test_eager_agg/rows"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string edb = "eageraggdb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + edb + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };
    REQUIRE(run("CREATE TABLE " + edb + ".a ();")->is_success()); // (g, k, x)
    REQUIRE(run("CREATE TABLE " + edb + ".b ();")->is_success()); // (k) dimension
    // a: many rows per (g, k). b matches keys 100 and 101 (key 999 has no match).
    REQUIRE(run("INSERT INTO " + edb +
                ".a (g, k, x) VALUES "
                "(1,100,1),(1,100,2),(1,100,3),(1,101,4),(1,101,5),"
                "(2,100,10),(2,100,20),(2,999,7);")
                ->is_success());
    REQUIRE(run("INSERT INTO " + edb + ".b (k) VALUES (100),(101);")->is_success());

    auto plan_text = [&](const std::string& sql) {
        auto c = run("EXPLAIN " + sql);
        REQUIRE(c->is_success());
        std::string t;
        for (size_t r = 0; r < c->size(); ++r) {
            t += std::string(c->value(0, r).value<std::string_view>());
            t += '\n';
        }
        return t;
    };
    auto has = [](const std::string& hay, const std::string& needle) { return hay.find(needle) != std::string::npos; };

    const std::string min_sql =
        "SELECT g, MIN(x) AS m FROM " + edb + ".a JOIN " + edb + ".b ON a.k = b.k GROUP BY g ORDER BY g";
    const std::string max_sql =
        "SELECT g, MAX(x) AS m FROM " + edb + ".a JOIN " + edb + ".b ON a.k = b.k GROUP BY g ORDER BY g";
    const std::string sum_sql =
        "SELECT g, SUM(x) AS s FROM " + edb + ".a JOIN " + edb + ".b ON a.k = b.k GROUP BY g ORDER BY g";

    // --- Plan: MIN/MAX push a partial aggregate under the Hash Join; SUM does not.
    const std::string min_plan = plan_text(min_sql);
    INFO(min_plan);
    CHECK(has(min_plan, "Hash Join"));
    CHECK(has(min_plan, "Pushed Aggregate Scan on a")); // partial reduce pushed onto a
    CHECK(has(plan_text(max_sql), "Pushed Aggregate Scan on a"));
    const std::string sum_plan = plan_text(sum_sql);
    INFO(sum_plan);
    CHECK(has(sum_plan, "Hash Join"));
    CHECK_FALSE(has(sum_plan, "Pushed Aggregate Scan")); // SUM is excluded — not pushed

    // --- Rows: the rewrite is result-preserving. Key 999 has no match in b, so its
    // a-rows (group 2) are dropped by the inner join before contributing.
    //   g=1: x in {1,2,3,4,5}  -> MIN 1,  MAX 5,  SUM 15
    //   g=2: x in {10,20}      -> MIN 10, MAX 20, SUM 30   (x=7 @ k=999 dropped)
    auto mn = run(min_sql);
    REQUIRE(mn->is_success());
    REQUIRE(mn->size() == 2);
    CHECK(mn->value(0, 0).value<int64_t>() == 1);
    CHECK(mn->value(1, 0).value<int64_t>() == 1);
    CHECK(mn->value(0, 1).value<int64_t>() == 2);
    CHECK(mn->value(1, 1).value<int64_t>() == 10);

    auto mx = run(max_sql);
    REQUIRE(mx->is_success());
    REQUIRE(mx->size() == 2);
    CHECK(mx->value(1, 0).value<int64_t>() == 5);
    CHECK(mx->value(1, 1).value<int64_t>() == 20);

    auto sm = run(sum_sql);
    REQUIRE(sm->is_success());
    REQUIRE(sm->size() == 2);
    CHECK(sm->value(1, 0).value<int64_t>() == 15);
    CHECK(sm->value(1, 1).value<int64_t>() == 30);
}

// ----------------------------------------------------------------------------
// The crux of the MIN/MAX-only envelope: a DUPLICATING dimension. When `b` has a
// repeated join key, the inner join duplicates each `a` row. MIN/MAX absorb that
// duplication (MIN(MIN over dups) == MIN over originals), so eager-pushing them is
// result-preserving even though `a`-rows are multiplied. SUM does NOT absorb it
// (the join inflates the total), which is exactly why SUM stays un-pushed. This
// test proves both: the duplication is real (SUM is inflated) AND MIN/MAX are
// unchanged with the partial physically pushed.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::eager_aggregation::duplicating_dimension_min_max_safe") {
    auto config = test_create_config(test_temp_path("test_eager_agg/dup"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string edb = "eageraggdupdb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + edb + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };
    REQUIRE(run("CREATE TABLE " + edb + ".a ();")->is_success()); // (g, k, x)
    REQUIRE(run("CREATE TABLE " + edb + ".b ();")->is_success()); // (k) dimension
    // a: many rows per (g, k).
    REQUIRE(run("INSERT INTO " + edb +
                ".a (g, k, x) VALUES "
                "(1,100,1),(1,100,2),(1,100,3),(2,200,10),(2,200,20);")
                ->is_success());
    // b DUPLICATES key 100 (x2) and carries key 200 once -> every a-row @k=100 is
    // doubled by the inner join; a-rows @k=200 appear once.
    REQUIRE(run("INSERT INTO " + edb + ".b (k) VALUES (100),(100),(200);")->is_success());

    auto plan_text = [&](const std::string& sql) {
        auto c = run("EXPLAIN " + sql);
        REQUIRE(c->is_success());
        std::string t;
        for (size_t r = 0; r < c->size(); ++r) {
            t += std::string(c->value(0, r).value<std::string_view>());
            t += '\n';
        }
        return t;
    };
    auto has = [](const std::string& hay, const std::string& needle) { return hay.find(needle) != std::string::npos; };

    const std::string min_sql =
        "SELECT g, MIN(x) AS m FROM " + edb + ".a JOIN " + edb + ".b ON a.k = b.k GROUP BY g ORDER BY g";
    const std::string max_sql =
        "SELECT g, MAX(x) AS m FROM " + edb + ".a JOIN " + edb + ".b ON a.k = b.k GROUP BY g ORDER BY g";
    const std::string sum_sql =
        "SELECT g, SUM(x) AS s FROM " + edb + ".a JOIN " + edb + ".b ON a.k = b.k GROUP BY g ORDER BY g";

    // MIN/MAX push the partial under the join; SUM does not.
    CHECK(has(plan_text(min_sql), "Pushed Aggregate Scan on a"));
    CHECK(has(plan_text(max_sql), "Pushed Aggregate Scan on a"));
    CHECK_FALSE(has(plan_text(sum_sql), "Pushed Aggregate Scan"));

    // MIN/MAX absorb the k=100 duplication -> identical to the no-duplication oracle:
    //   g=1: x in {1,2,3} (each doubled) -> MIN 1,  MAX 3
    //   g=2: x in {10,20}                -> MIN 10, MAX 20
    auto mn = run(min_sql);
    REQUIRE(mn->is_success());
    REQUIRE(mn->size() == 2);
    CHECK(mn->value(1, 0).value<int64_t>() == 1);
    CHECK(mn->value(1, 1).value<int64_t>() == 10);

    auto mx = run(max_sql);
    REQUIRE(mx->is_success());
    REQUIRE(mx->size() == 2);
    CHECK(mx->value(1, 0).value<int64_t>() == 3);
    CHECK(mx->value(1, 1).value<int64_t>() == 20);

    // SUM is inflated by the duplication -> proves the join really doubles k=100 and
    // therefore why SUM must NOT be eager-pushed:
    //   g=1: (1+2+3) * 2 = 12   g=2: (10+20) * 1 = 30
    auto sm = run(sum_sql);
    REQUIRE(sm->is_success());
    REQUIRE(sm->size() == 2);
    CHECK(sm->value(1, 0).value<int64_t>() == 12);
    CHECK(sm->value(1, 1).value<int64_t>() == 30);
}
