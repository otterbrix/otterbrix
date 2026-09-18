#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer/rules/hash_join.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <memory_resource>
#include <sstream>
#include <vector>

using namespace components;
using expressions::compare_type;
using expressions::side_t;
using logical_plan::join_type;
using operators::operator_type;

// rewrite_hash_joins stamps algo()==hash exactly for a single eq(left.key,right.key) on an inner/left/right/full
// join (nested otherwise); nodes are hand-built already carrying side()+path(), the real post-validate state.
namespace {

    vector::data_chunk_t build_two_int_chunk(std::pmr::memory_resource* res) {
        std::pmr::vector<types::complex_logical_type> types(res);
        types.emplace_back(types::logical_type::BIGINT, "key");
        types.emplace_back(types::logical_type::BIGINT, "val");
        vector::data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, int64_t{1});
        chunk.set_value(1, 0, int64_t{2});
        return chunk;
    }

    expressions::key_t make_key(std::pmr::memory_resource* res, const char* name, side_t side, size_t col) {
        expressions::key_t k{res, name, side};
        std::pmr::vector<size_t> path{res};
        path.push_back(col);
        k.set_path(std::move(path));
        return k;
    }

    // Two-column chunk with caller-chosen names/rows; distinct names let a test tell build from probe after a swap.
    vector::data_chunk_t
    build_named_chunk(std::pmr::memory_resource* res, const char* key_name, const char* val_name, uint64_t rows) {
        std::pmr::vector<types::complex_logical_type> types(res);
        types.emplace_back(types::logical_type::BIGINT, key_name);
        types.emplace_back(types::logical_type::BIGINT, val_name);
        vector::data_chunk_t chunk(res, types, rows == 0 ? 1 : rows);
        chunk.set_cardinality(rows);
        for (uint64_t i = 0; i < rows; ++i) {
            chunk.set_value(0, i, static_cast<int64_t>(i + 1));
            chunk.set_value(1, i, static_cast<int64_t>((i + 1) * 10));
        }
        return chunk;
    }

} // namespace

TEST_CASE("integration::cpp::hash_join::substitution") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
    compute::function_registry_t registry(res);

    auto plan_type = [&](join_type jt, compare_type cmp, side_t ls, side_t rs) {
        auto cond = expressions::make_compare_expression(res,
                                                         cmp,
                                                         expressions::param_storage{make_key(res, "l", ls, 0)},
                                                         expressions::param_storage{make_key(res, "r", rs, 0)});
        auto join = logical_plan::make_node_join(res, core::dbname_t{}, core::relname_t{}, jt);
        join->append_child(logical_plan::make_node_raw_data(res, build_two_int_chunk(res)));
        join->append_child(logical_plan::make_node_raw_data(res, build_two_int_chunk(res)));
        join->append_expression(cond);
        auto optimized = planner::optimizer::rewrite_hash_joins(res, join);
        auto plan =
            services::planner::create_plan(context, registry, optimized, logical_plan::limit_t::unlimit(), nullptr);
        REQUIRE(plan);
        return plan->type();
    };

    INFO("equi-join (eq, left/right keys) is rewritten to hash_join");
    {
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::left, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::right, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::full, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::right, side_t::left) == operator_type::hash_join);
    }

    INFO("non-equi conditions keep the nested-loop join");
    {
        CHECK(plan_type(join_type::inner, compare_type::gt, side_t::left, side_t::right) == operator_type::join);
        CHECK(plan_type(join_type::inner, compare_type::ne, side_t::left, side_t::right) == operator_type::join);
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::left, side_t::left) == operator_type::join);
    }

    INFO("cross join is never a hash join");
    { CHECK(plan_type(join_type::cross, compare_type::eq, side_t::left, side_t::right) == operator_type::join); }

    INFO("nested-field equi-join (multi-element path) keeps the nested-loop join");
    {
        // A multi-element path addresses a nested struct field; the hash probe only understands a top-level column.
        auto lk = make_key(res, "l", side_t::left, 0);
        std::pmr::vector<size_t> lp{res};
        lp.push_back(0);
        lp.push_back(1); // (col 0).field 1 — two-element path
        lk.set_path(std::move(lp));
        auto rk = make_key(res, "r", side_t::right, 0);
        std::pmr::vector<size_t> rp{res};
        rp.push_back(0);
        rp.push_back(1);
        rk.set_path(std::move(rp));

        auto cond = expressions::make_compare_expression(res,
                                                         compare_type::eq,
                                                         expressions::param_storage{std::move(lk)},
                                                         expressions::param_storage{std::move(rk)});
        auto join = logical_plan::make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::inner);
        join->append_child(logical_plan::make_node_raw_data(res, build_two_int_chunk(res)));
        join->append_child(logical_plan::make_node_raw_data(res, build_two_int_chunk(res)));
        join->append_expression(cond);
        auto optimized = planner::optimizer::rewrite_hash_joins(res, join);
        auto plan =
            services::planner::create_plan(context, registry, optimized, logical_plan::limit_t::unlimit(), nullptr);
        REQUIRE(plan);
        CHECK(plan->type() == operator_type::join);
    }
}

static const std::string db = "hashjoindb";

TEST_CASE("integration::cpp::hash_join::correctness") {
    auto config = test_create_config(integration_fixture_path("test_hash_join/base"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE " + db + ";");

    auto create = [&](const std::string& t) {
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + db + "." + t + "();")->is_success());
    };
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };

    INFO("duplicate keys on both sides — inner/left/right/full");
    {
        create("dl");
        create("dr");
        REQUIRE(run("INSERT INTO " + db + ".dl (k, lv) VALUES (1, 10), (1, 11), (2, 20);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".dr (k, rv) VALUES (1, 100), (1, 101), (3, 300);")->is_success());

        CHECK(run("SELECT * FROM " + db + ".dl INNER JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 4);
        CHECK(run("SELECT * FROM " + db + ".dl LEFT JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 5);
        CHECK(run("SELECT * FROM " + db + ".dl RIGHT JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 5);
        CHECK(run("SELECT * FROM " + db + ".dl FULL JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 6);
    }

    INFO("NULL keys never match (skipped in build and probe)");
    {
        create("nl");
        create("nr");
        REQUIRE(run("INSERT INTO " + db + ".nl (k, lv) VALUES (1, 10), (NULL, 20);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".nr (k, rv) VALUES (1, 100), (NULL, 200);")->is_success());

        CHECK(run("SELECT * FROM " + db + ".nl INNER JOIN " + db + ".nr ON nl.k = nr.k;")->size() == 1);
        CHECK(run("SELECT * FROM " + db + ".nl LEFT JOIN " + db + ".nr ON nl.k = nr.k;")->size() == 2);
        CHECK(run("SELECT * FROM " + db + ".nl FULL JOIN " + db + ".nr ON nl.k = nr.k;")->size() == 3);
    }

    INFO("multi-chunk inputs (> 1024 rows force chunk boundaries)");
    {
        create("bl");
        create("br");
        const int n = 2500; // > 2 * DEFAULT_VECTOR_CAPACITY on each side
        std::stringstream l, r;
        l << "INSERT INTO " << db << ".bl (k, lv) VALUES ";
        r << "INSERT INTO " << db << ".br (k, rv) VALUES ";
        for (int i = 0; i < n; ++i) {
            l << "(" << i << ", " << i * 10 << ")" << (i == n - 1 ? ";" : ", ");
            r << "(" << (i + n / 2) << ", " << i << ")" << (i == n - 1 ? ";" : ", ");
        }
        REQUIRE(run(l.str())->is_success());
        REQUIRE(run(r.str())->is_success());
        // Right keys are shifted by n/2, giving a partial overlap so both matched and left-only rows are exercised.
        CHECK(run("SELECT * FROM " + db + ".bl INNER JOIN " + db + ".br ON bl.k = br.k;")->size() ==
              static_cast<size_t>(n / 2));
        CHECK(run("SELECT * FROM " + db + ".bl LEFT JOIN " + db + ".br ON bl.k = br.k;")->size() ==
              static_cast<size_t>(n));
    }

    INFO("string join keys");
    {
        create("sl");
        create("sr");
        REQUIRE(run("INSERT INTO " + db + ".sl (s, lv) VALUES ('a', 1), ('b', 2), ('a', 3);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".sr (s, rv) VALUES ('a', 10), ('c', 30);")->is_success());
        CHECK(run("SELECT * FROM " + db + ".sl INNER JOIN " + db + ".sr ON sl.s = sr.s;")->size() == 2);
    }
}

// The batched join_builder reorders rows (one indexed copy per build-chunk/column), so results here are asserted
// under ORDER BY; row counts here cross both build-chunk (2500 rows) and output-chunk (>1024 matches) boundaries.
TEST_CASE("integration::cpp::hash_join::multi_build_chunk_values") {
    auto config = test_create_config(integration_fixture_path("test_hash_join/mbchunk"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string mdb = "mbchunkdb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + mdb + ";");
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + mdb + ".mbl();")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + mdb + ".mbr();")->is_success());

    const int n = 2500;     // > 2 * DEFAULT_VECTOR_CAPACITY → 3 chunks per side
    const int shift = 1250; // build keys start here → half the probe keys are left-only
    {
        std::stringstream l, r;
        l << "INSERT INTO " << mdb << ".mbl (k, lv) VALUES ";
        r << "INSERT INTO " << mdb << ".mbr (k, rv) VALUES ";
        for (int i = 0; i < n; ++i) {
            l << "(" << i << ", " << static_cast<int64_t>(i) * 100 << ")" << (i == n - 1 ? ";" : ", ");
            r << "(" << (i + shift) << ", " << static_cast<int64_t>(i + shift) * 7 << ")" << (i == n - 1 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, l.str())->is_success());
        REQUIRE(dispatcher->execute_sql(session, r.str())->is_success());
    }
    // Right keys are shifted by "shift", giving n-shift (>1024) matched keys to exercise multi-chunk output.
    // SELECT * column order is [mbl.k, mbl.lv, mbr.k, mbr.rv].
    const int matched = n - shift; // 1250

    INFO("inner: every matched row gathered correctly across build chunks");
    {
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + mdb + ".mbl INNER JOIN " + mdb +
                                               ".mbr ON mbl.k = mbr.k ORDER BY mbl.k ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(matched));
        for (size_t row = 0; row < static_cast<size_t>(matched); ++row) {
            const int64_t k = static_cast<int64_t>(row) + shift;     // 1250, 1251, ...
            REQUIRE(cur->value(0, row).value<int64_t>() == k);       // mbl.k
            REQUIRE(cur->value(1, row).value<int64_t>() == k * 100); // mbl.lv
            REQUIRE(cur->value(2, row).value<int64_t>() == k);       // mbr.k
            REQUIRE(cur->value(3, row).value<int64_t>() == k * 7);   // mbr.rv
        }
    }

    INFO("left: > 1024 left-only NULL-pad rows mixed with matched, all correct");
    {
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + mdb + ".mbl LEFT JOIN " + mdb +
                                               ".mbr ON mbl.k = mbr.k ORDER BY mbl.k ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(n)); // every left row once
        for (size_t row = 0; row < static_cast<size_t>(n); ++row) {
            const int64_t k = static_cast<int64_t>(row);             // 0, 1, ... (sorted probe key)
            REQUIRE(cur->value(0, row).value<int64_t>() == k);       // mbl.k
            REQUIRE(cur->value(1, row).value<int64_t>() == k * 100); // mbl.lv
            if (k < shift) {
                REQUIRE(cur->value(2, row).is_null());
                REQUIRE(cur->value(3, row).is_null());
            } else {
                REQUIRE(cur->value(2, row).value<int64_t>() == k);     // mbr.k
                REQUIRE(cur->value(3, row).value<int64_t>() == k * 7); // mbr.rv
            }
        }
    }
}

// create_plan_join moves the SMALLER table onto the physical RIGHT (build) slot IFF INNER, both children are
// distinct base tables with known counts, and the default (right) build is larger; outer/self-join cases don't swap.
TEST_CASE("integration::cpp::hash_join::build_side_selection") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    compute::function_registry_t registry(res);

    using components::catalog::oid_t;
    constexpr oid_t left_table_oid = 42;
    constexpr oid_t right_table_oid = 43;

    // Returns the key-column name of whichever physical child became the hash build: "lk" means swapped, "rk" default.
    auto build_side_key_name =
        [&](join_type jt, uint64_t left_rows, uint64_t right_rows, bool populate_counts, bool same_oid) -> std::string {
        services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
        const oid_t l = left_table_oid;
        const oid_t r = same_oid ? left_table_oid : right_table_oid;
        context.known_oids.insert(l);
        context.known_oids.insert(r);
        if (populate_counts) {
            context.row_counts[l] = left_rows;
            context.row_counts[r] = right_rows;
        }

        auto cond =
            expressions::make_compare_expression(res,
                                                 compare_type::eq,
                                                 expressions::param_storage{make_key(res, "lk", side_t::left, 0)},
                                                 expressions::param_storage{make_key(res, "rk", side_t::right, 0)});
        auto join = logical_plan::make_node_join(res, core::dbname_t{}, core::relname_t{}, jt);
        auto left_child = logical_plan::make_node_raw_data(res, build_named_chunk(res, "lk", "lv", left_rows));
        auto right_child = logical_plan::make_node_raw_data(res, build_named_chunk(res, "rk", "rv", right_rows));
        left_child->set_table_oid(l);
        right_child->set_table_oid(r);
        join->append_child(left_child);
        join->append_child(right_child);
        join->append_expression(cond);

        auto optimized = planner::optimizer::rewrite_hash_joins(res, join);
        auto plan =
            services::planner::create_plan(context, registry, optimized, logical_plan::limit_t::unlimit(), nullptr);
        REQUIRE(plan);
        REQUIRE(plan->type() == operator_type::hash_join);
        REQUIRE(plan->right()); // physical build side
        REQUIRE(plan->right()->output());
        REQUIRE(!plan->right()->output()->chunks().empty());
        return plan->right()->output()->chunks().front().types()[0].alias();
    };

    INFO("INNER, larger table on the RIGHT → smaller (logical-left) side becomes build");
    { CHECK(build_side_key_name(join_type::inner, 2, 5, true, false) == "lk"); }
    INFO("INNER, larger table on the LEFT → no swap (right is already the smaller build)");
    { CHECK(build_side_key_name(join_type::inner, 5, 2, true, false) == "rk"); }
    INFO("INNER, equal row counts → no swap");
    { CHECK(build_side_key_name(join_type::inner, 4, 4, true, false) == "rk"); }
    INFO("INNER, counts missing (in-memory / no disk agent) → no swap");
    { CHECK(build_side_key_name(join_type::inner, 2, 5, false, false) == "rk"); }
    INFO("INNER self-join (same table oid) → no swap even though counts would favor it");
    { CHECK(build_side_key_name(join_type::inner, 2, 5, true, true) == "rk"); }
    INFO("OUTER (left) join is never swapped, even with a larger right table");
    { CHECK(build_side_key_name(join_type::left, 2, 5, true, false) == "rk"); }
}

// SMALL is on the LEFT and LARGE on the RIGHT, so the default (right) build is the larger side and a correct impl
// swaps; a SELECT * per-cell check and a two-sided SUM catch any column-order inversion the swap could introduce.
TEST_CASE("integration::cpp::hash_join::build_side_swap_values") {
    auto config = test_create_config(integration_fixture_path("test_hash_join/buildside"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string sdb = "buildsidedb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + sdb + ";");
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + sdb + ".small();")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + sdb + ".large();")->is_success());

    REQUIRE(
        dispatcher->execute_sql(session, "INSERT INTO " + sdb + ".small (k, sv) VALUES (1, 100), (2, 200), (3, 300);")
            ->is_success());
    {
        std::stringstream l;
        l << "INSERT INTO " << sdb << ".large (k, lv) VALUES ";
        for (int i = 1; i <= 30; ++i) {
            l << "(" << i << ", " << i << ")" << (i == 30 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, l.str())->is_success());
    }

    INFO("small on LEFT, large on RIGHT → swap fires; SELECT * columns/values stay correct");
    {
        // logical output columns are [small.k, small.sv, large.k, large.lv].
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + sdb + ".small INNER JOIN " + sdb +
                                               ".large ON small.k = large.k ORDER BY small.k ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        for (size_t row = 0; row < 3; ++row) {
            const int64_t k = static_cast<int64_t>(row) + 1;         // 1, 2, 3
            REQUIRE(cur->value(0, row).value<int64_t>() == k);       // small.k
            REQUIRE(cur->value(1, row).value<int64_t>() == k * 100); // small.sv
            REQUIRE(cur->value(2, row).value<int64_t>() == k);       // large.k
            REQUIRE(cur->value(3, row).value<int64_t>() == k);       // large.lv
        }
    }

    INFO("SUM over each side after the swap is correct (catches a column inversion)");
    {
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT SUM(s.sv) AS ssv, SUM(l.lv) AS slv FROM " + sdb +
                                               ".small s "
                                               "INNER JOIN " +
                                               sdb + ".large l ON s.k = l.k;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 600);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 6);
    }

    // A filter matching nothing on the build side must yield zero build chunks treated as "no build", not a crash
    // (the SSB-load EXC_BAD_ACCESS shrunk to a deterministic 2-table case); small stays the build side (it's smaller).
    INFO("empty build side (filter matches nothing) → 0 rows, no crash");
    {
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + sdb + ".large INNER JOIN " + sdb +
                                               ".small ON large.k = small.k WHERE small.sv = 99999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("empty build side under GROUP BY + ORDER BY (SSB shape) → 0 groups, no crash");
    {
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT small.k, SUM(large.lv) AS s FROM " + sdb + ".large INNER JOIN " + sdb +
                                        ".small ON large.k = small.k WHERE small.sv = 99999 "
                                        "GROUP BY small.k ORDER BY small.k;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// A 3-table comma join (SSB q2 shape) lowers to nested CROSS joins with both equi-predicates in a sibling WHERE;
// promote_cross_joins must promote both to INNER so rewrite_hash_joins can hash them, keeping the residual filter.
// Hash lowering is checked at the plan level by components/planner/test/test_promote_multiway.cpp; this is E2E rows.
TEST_CASE("integration::cpp::hash_join::multiway_comma_join") {
    auto config = test_create_config(integration_fixture_path("test_hash_join/multiway"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string wdb = "multiwaydb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + wdb + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };

    REQUIRE(run("CREATE TABLE " + wdb + ".lo();")->is_success()); // lineorder-shaped fact
    REQUIRE(run("CREATE TABLE " + wdb + ".p();")->is_success());  // part-shaped dim
    REQUIRE(run("CREATE TABLE " + wdb + ".s();")->is_success());  // supplier-shaped dim

    // lo=(l_pk,l_sk,l_rev): part/supplier key + revenue. p covers every part key; s covers only suppliers 10,20,
    // so the l_sk=30 row has no matching supplier.
    REQUIRE(run("INSERT INTO " + wdb +
                ".lo (l_pk, l_sk, l_rev) VALUES (1, 10, 100), (1, 20, 200), (2, 10, 300), (2, 20, 400), (3, 30, 500);")
                ->is_success());
    REQUIRE(run("INSERT INTO " + wdb + ".p (p_pk, p_cat) VALUES (1, 'A'), (2, 'B'), (3, 'A');")->is_success());
    REQUIRE(run("INSERT INTO " + wdb + ".s (s_sk, s_reg) VALUES (10, 'X'), (20, 'Y');")->is_success());

    // SELECT * column order = [l_pk, l_sk, l_rev, p_pk, p_cat, s_sk, s_reg].
    INFO("3-table comma join returns the correctly joined rows");
    {
        auto cur = run("SELECT * FROM " + wdb + ".lo, " + wdb + ".p, " + wdb + ".s WHERE l_pk = p_pk AND l_sk = s_sk;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        int64_t rev_sum = 0;
        for (size_t row = 0; row < cur->size(); ++row) {
            REQUIRE(cur->value(0, row).value<int64_t>() == cur->value(3, row).value<int64_t>()); // l_pk == p_pk
            REQUIRE(cur->value(1, row).value<int64_t>() == cur->value(5, row).value<int64_t>()); // l_sk == s_sk
            rev_sum += cur->value(2, row).value<int64_t>();                                      // l_rev
        }
        CHECK(rev_sum == 1000); // 100 + 200 + 300 + 400 (the l_sk=30 / rev=500 row is unmatched)
    }

    INFO("residual single-table filter still applies through multi-way promotion");
    {
        auto cur = run("SELECT * FROM " + wdb + ".lo, " + wdb + ".p, " + wdb +
                       ".s WHERE l_pk = p_pk AND l_sk = s_sk AND p_cat = 'A';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        int64_t rev_sum = 0;
        for (size_t row = 0; row < cur->size(); ++row) {
            rev_sum += cur->value(2, row).value<int64_t>();
        }
        CHECK(rev_sum == 300); // 100 + 200
    }
}

// pushdown_filter wraps a join input's single-table WHERE in an oid-less aggregate{source,match}, so a direct-oid
// count gate can't see through it; unresolved, a statistics-free tiebreaker could swap a weakly-filtered HUGE side
// onto the build. create_plan_join resolves the EFFECTIVE relation; filtered vs unfiltered ties only at equal counts.
TEST_CASE("integration::cpp::hash_join::filtered_side_swap_requires_size_evidence") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    compute::function_registry_t registry(res);

    using components::catalog::oid_t;
    constexpr oid_t big_oid = 52;
    constexpr oid_t tiny_oid = 53;

    auto lower = [&](uint64_t big_rows, uint64_t tiny_rows) {
        services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
        context.known_oids.insert(big_oid);
        context.known_oids.insert(tiny_oid);
        context.row_counts[big_oid] = big_rows;
        context.row_counts[tiny_oid] = tiny_rows;

        auto big_table = logical_plan::make_node_raw_data(res, build_named_chunk(res, "bk", "bv", 100));
        big_table->set_table_oid(big_oid);
        // Mimics pushdown_filter's wrapper shape: an oid-less aggregate{source, match}; the WHERE is never executed.
        auto where =
            expressions::make_compare_expression(res,
                                                 compare_type::gt,
                                                 expressions::param_storage{make_key(res, "bv", side_t::left, 1)},
                                                 expressions::param_storage{make_key(res, "bk", side_t::left, 0)});
        auto wrapper = logical_plan::make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
        wrapper->append_child(big_table);
        wrapper->append_child(logical_plan::make_node_match(res, core::dbname_t{}, core::relname_t{}, where));

        auto tiny = logical_plan::make_node_raw_data(res, build_named_chunk(res, "rk", "rv", 2));
        tiny->set_table_oid(tiny_oid);

        auto cond =
            expressions::make_compare_expression(res,
                                                 compare_type::eq,
                                                 expressions::param_storage{make_key(res, "bk", side_t::left, 0)},
                                                 expressions::param_storage{make_key(res, "rk", side_t::right, 0)});
        auto join = logical_plan::make_node_join(res, core::dbname_t{}, core::relname_t{}, join_type::inner);
        join->append_child(wrapper);
        join->append_child(tiny);
        join->append_expression(cond);
        join->set_equi_columns(0, 0); // post-rewrite_hash_joins state: algo -> hash

        auto plan = services::planner::create_plan(context, registry, join, logical_plan::limit_t::unlimit(), nullptr);
        REQUIRE(plan);
        REQUIRE(plan->type() == operator_type::hash_join);
        REQUIRE(plan->right()); // physical build side
        return plan;
    };

    INFO("huge filtered LEFT vs tiny RIGHT with live counts -> the tiny right STAYS the build");
    {
        auto plan = lower(1000, 2);
        REQUIRE(plan->right()->type() == operator_type::raw_data);
        REQUIRE(plan->right()->output());
        REQUIRE(!plan->right()->output()->chunks().empty());
        CHECK(plan->right()->output()->chunks().front().types()[0].alias() == "rk");
    }

    INFO("EXACT pre-filter count tie -> the filtered left (certainly <= tie) becomes the build");
    {
        auto plan = lower(2, 2);
        CHECK(plan->right()->type() == operator_type::match);
    }
}

// At an exact pre-filter count tie (filt and pln both 3 rows), collect_inner_hash_join_oids resolves filt through
// pushdown_filter's wrapper; since a filtered side is certainly <= its pre-filter count, the tie-break moves filt
// onto the build. EXPLAIN renders probe before build, so pln (probe) renders before filt (build); rows are unaffected.
TEST_CASE("integration::cpp::hash_join::build_side_syntactic_inmemory") {
    auto config = test_create_config(integration_fixture_path("test_hash_join/syntactic"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string sdb = "synbuilddb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + sdb + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };
    REQUIRE(run("CREATE TABLE " + sdb + ".filt ();")->is_success()); // (k, x)
    REQUIRE(run("CREATE TABLE " + sdb + ".pln ();")->is_success());  // (k, y)
    REQUIRE(run("INSERT INTO " + sdb + ".filt (k, x) VALUES (1, 5), (2, 9), (3, 5);")->is_success());
    REQUIRE(run("INSERT INTO " + sdb + ".pln (k, y) VALUES (1, 10), (2, 20), (3, 30);")->is_success());

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

    const std::string q = "SELECT * FROM " + sdb + ".filt JOIN " + sdb + ".pln ON filt.k = pln.k WHERE filt.x = 5";

    INFO("EXPLAIN: the filtered relation (filt) is the hash BUILD side, rendered AFTER the probe (pln)");
    {
        const std::string t = plan_text(q);
        INFO(t);
        REQUIRE(t.find("Hash Join") != std::string::npos);
        const auto pos_filt = t.find("on filt");
        const auto pos_pln = t.find("on pln");
        REQUIRE(pos_filt != std::string::npos);
        REQUIRE(pos_pln != std::string::npos);
        CHECK(pos_pln < pos_filt);
    }

    INFO("rows: the build-side swap is answer-neutral");
    {
        auto cur = run(q + " ORDER BY filt.k ASC;");
        REQUIRE(cur->is_success());
        // SELECT * column order = [filt.k, filt.x, pln.k, pln.y].
        REQUIRE(cur->size() == 2);
        CHECK(cur->value(0, 0).value<int64_t>() == 1);  // filt.k
        CHECK(cur->value(1, 0).value<int64_t>() == 5);  // filt.x
        CHECK(cur->value(2, 0).value<int64_t>() == 1);  // pln.k
        CHECK(cur->value(3, 0).value<int64_t>() == 10); // pln.y
        CHECK(cur->value(0, 1).value<int64_t>() == 3);  // filt.k
        CHECK(cur->value(1, 1).value<int64_t>() == 5);  // filt.x
        CHECK(cur->value(2, 1).value<int64_t>() == 3);  // pln.k
        CHECK(cur->value(3, 1).value<int64_t>() == 30); // pln.y
    }
}

// A weak WHERE (keeps all rows) still makes pushdown wrap big in an oid-less aggregate{scan, match}; without
// descending through it in collect_inner_hash_join_oids, the count gate would abstain and risk swapping the HUGE
// filtered side onto the build. With effective-oid descent, big 8 vs tiny 2 keeps tiny (probe renders before build).
TEST_CASE("integration::cpp::hash_join::filtered_left_count_fetched_through_wrapper") {
    auto config = test_create_config(integration_fixture_path("test_hash_join/wrapped_count"));
    test_clear_directory(config);
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string wdb = "wrapcountdb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + wdb + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };
    REQUIRE(run("CREATE TABLE " + wdb + ".big ();")->is_success());  // (k, x)
    REQUIRE(run("CREATE TABLE " + wdb + ".tiny ();")->is_success()); // (k, y)
    REQUIRE(run("INSERT INTO " + wdb +
                ".big (k, x) VALUES (1, 1), (2, 2), (3, 3), (4, 4), "
                "(5, 5), (6, 6), (7, 7), (8, 8);")
                ->is_success());
    REQUIRE(run("INSERT INTO " + wdb + ".tiny (k, y) VALUES (1, 10), (2, 20);")->is_success());

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

    const std::string q = "SELECT * FROM " + wdb + ".big JOIN " + wdb + ".tiny ON big.k = tiny.k WHERE big.x < 100";

    INFO("EXPLAIN: live counts resolved through the wrapper -> tiny (2 rows) stays the build");
    {
        const std::string t = plan_text(q);
        INFO(t);
        REQUIRE(t.find("Hash Join") != std::string::npos);
        const auto pos_big = t.find("on big");
        const auto pos_tiny = t.find("on tiny");
        REQUIRE(pos_big != std::string::npos);
        REQUIRE(pos_tiny != std::string::npos);
        CHECK(pos_big < pos_tiny);
    }

    INFO("rows: the build-side decision is answer-neutral");
    {
        auto cur = run(q + " ORDER BY big.k ASC;");
        REQUIRE(cur->is_success());
        // SELECT * column order = [big.k, big.x, tiny.k, tiny.y].
        REQUIRE(cur->size() == 2);
        CHECK(cur->value(0, 0).value<int64_t>() == 1);  // big.k
        CHECK(cur->value(1, 0).value<int64_t>() == 1);  // big.x
        CHECK(cur->value(2, 0).value<int64_t>() == 1);  // tiny.k
        CHECK(cur->value(3, 0).value<int64_t>() == 10); // tiny.y
        CHECK(cur->value(0, 1).value<int64_t>() == 2);  // big.k
        CHECK(cur->value(1, 1).value<int64_t>() == 2);  // big.x
        CHECK(cur->value(2, 1).value<int64_t>() == 2);  // tiny.k
        CHECK(cur->value(3, 1).value<int64_t>() == 20); // tiny.y
    }
}
