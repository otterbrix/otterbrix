#include "test_config.hpp"
#include <catch2/catch.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
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

// ----------------------------------------------------------------------------
// Part 1 — substitution: the optimizer's rewrite_hash_joins must turn a JOIN into
// a node_hash_join_t (lowered to operator_hash_join_t) exactly when the condition
// is a single eq(left.key, right.key) on an inner/left/right/full join, and leave
// it a node_join_t (lowered to operator_join_t) otherwise.
//
// We hand-build a logical join node whose ON-condition keys already carry
// side()+path() — the exact post-validate state the real SQL→logical pipeline
// reaches (transformer sets side(), validate_schema sets path()) — run the
// optimizer rule, then lower with create_plan, mirroring the dispatcher pipeline.
// ----------------------------------------------------------------------------
namespace {

    vector::data_chunk_t build_two_int_chunk(std::pmr::memory_resource* res) {
        std::pmr::vector<types::complex_logical_type> types(res);
        types.emplace_back(types::logical_type::BIGINT, "key");
        types.emplace_back(types::logical_type::BIGINT, "val");
        vector::data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, types::logical_value_t{res, int64_t{1}});
        chunk.set_value(1, 0, types::logical_value_t{res, int64_t{2}});
        return chunk;
    }

    expressions::key_t make_key(std::pmr::memory_resource* res, const char* name, side_t side, size_t col) {
        expressions::key_t k{res, name, side};
        std::pmr::vector<size_t> path{res};
        path.push_back(col);
        k.set_path(std::move(path));
        return k;
    }

} // namespace

TEST_CASE("integration::cpp::hash_join::substitution") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
    compute::function_registry_t registry(res);

    // Builds a fresh join node (two raw-data children + one comparison condition),
    // runs the optimizer's hash-join rewrite, lowers it with create_plan, and
    // returns the resulting physical operator type.
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

    INFO("equi-join (eq, left/right keys) is rewritten to hash_join") {
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::left, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::right, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::full, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        // Operands swapped (right.key = left.key) is still an equi-join.
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::right, side_t::left) == operator_type::hash_join);
    }

    INFO("non-equi conditions keep the nested-loop join") {
        // Not an equality comparison.
        CHECK(plan_type(join_type::inner, compare_type::gt, side_t::left, side_t::right) == operator_type::join);
        CHECK(plan_type(join_type::inner, compare_type::ne, side_t::left, side_t::right) == operator_type::join);
        // eq, but both keys reference the same side — not a left↔right equi-join.
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::left, side_t::left) == operator_type::join);
    }

    INFO("cross join is never a hash join") {
        CHECK(plan_type(join_type::cross, compare_type::eq, side_t::left, side_t::right) == operator_type::join);
    }

    INFO("nested-field equi-join (multi-element path) keeps the nested-loop join") {
        // A path like [custom_type_col, f1] addresses a nested struct field; the hash
        // probe only understands a single top-level column, so this must NOT be rewritten.
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

// ----------------------------------------------------------------------------
// Part 2 — correctness: drive the substituted hash join through real SQL and
// check join cardinality/semantics for cases that stress the hash path:
// duplicate keys, NULL keys, multi-chunk inputs (> DEFAULT_VECTOR_CAPACITY),
// and string keys.
// ----------------------------------------------------------------------------
static const std::string db = "hashjoindb";

TEST_CASE("integration::cpp::hash_join::correctness") {
    auto config = test_create_config("/tmp/test_hash_join/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE " + db + ";");

    auto create = [&](const std::string& t) {
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + db + "." + t + "();")->is_success());
    };
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };

    INFO("duplicate keys on both sides — inner/left/right/full") {
        create("dl");
        create("dr");
        // left k=1 twice, k=2 once; right k=1 twice, k=3 once.
        REQUIRE(run("INSERT INTO " + db + ".dl (k, lv) VALUES (1, 10), (1, 11), (2, 20);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".dr (k, rv) VALUES (1, 100), (1, 101), (3, 300);")->is_success());

        // k=1 → 2×2 cartesian = 4 matched rows; k=2,k=3 unmatched.
        CHECK(run("SELECT * FROM " + db + ".dl INNER JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 4);
        // LEFT: 4 matched + left-only (k=2) = 5.
        CHECK(run("SELECT * FROM " + db + ".dl LEFT JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 5);
        // RIGHT: 4 matched + right-only (k=3) = 5.
        CHECK(run("SELECT * FROM " + db + ".dl RIGHT JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 5);
        // FULL: 4 matched + left-only (k=2) + right-only (k=3) = 6.
        CHECK(run("SELECT * FROM " + db + ".dl FULL JOIN " + db + ".dr ON dl.k = dr.k;")->size() == 6);
    }

    INFO("NULL keys never match (skipped in build and probe)") {
        create("nl");
        create("nr");
        REQUIRE(run("INSERT INTO " + db + ".nl (k, lv) VALUES (1, 10), (NULL, 20);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".nr (k, rv) VALUES (1, 100), (NULL, 200);")->is_success());

        // Only k=1 matches; both NULL keys are dropped from the equi-join.
        CHECK(run("SELECT * FROM " + db + ".nl INNER JOIN " + db + ".nr ON nl.k = nr.k;")->size() == 1);
        // LEFT: matched k=1 (1) + NULL-key left row as left-only (1) = 2.
        CHECK(run("SELECT * FROM " + db + ".nl LEFT JOIN " + db + ".nr ON nl.k = nr.k;")->size() == 2);
        // FULL: matched k=1 (1) + NULL left-only (1) + NULL right-only (1) = 3.
        CHECK(run("SELECT * FROM " + db + ".nl FULL JOIN " + db + ".nr ON nl.k = nr.k;")->size() == 3);
    }

    INFO("multi-chunk inputs (> 1024 rows force chunk boundaries)") {
        create("bl");
        create("br");
        const int n = 2500; // > 2 * DEFAULT_VECTOR_CAPACITY on each side
        std::stringstream l, r;
        l << "INSERT INTO " << db << ".bl (k, lv) VALUES ";
        r << "INSERT INTO " << db << ".br (k, rv) VALUES ";
        for (int i = 0; i < n; ++i) {
            l << "(" << i << ", " << i * 10 << ")" << (i == n - 1 ? ";" : ", ");
            // right keys are the odd half [0, n) so exactly the even... use shifted overlap:
            r << "(" << (i + n / 2) << ", " << i << ")" << (i == n - 1 ? ";" : ", ");
        }
        REQUIRE(run(l.str())->is_success());
        REQUIRE(run(r.str())->is_success());
        // left keys: [0, n); right keys: [n/2, n + n/2). Overlap = [n/2, n) = n/2 keys,
        // each unique on both sides → n/2 matched rows.
        CHECK(run("SELECT * FROM " + db + ".bl INNER JOIN " + db + ".br ON bl.k = br.k;")->size() ==
              static_cast<size_t>(n / 2));
        // LEFT join emits every left row at least once → n rows (n/2 matched + n/2 left-only).
        CHECK(run("SELECT * FROM " + db + ".bl LEFT JOIN " + db + ".br ON bl.k = br.k;")->size() ==
              static_cast<size_t>(n));
    }

    INFO("string join keys") {
        create("sl");
        create("sr");
        REQUIRE(run("INSERT INTO " + db + ".sl (s, lv) VALUES ('a', 1), ('b', 2), ('a', 3);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".sr (s, rv) VALUES ('a', 10), ('c', 30);")->is_success());
        // 'a' → 2 left × 1 right = 2 matched rows; 'b','c' unmatched.
        CHECK(run("SELECT * FROM " + db + ".sl INNER JOIN " + db + ".sr ON sl.s = sr.s;")->size() == 2);
    }
}
