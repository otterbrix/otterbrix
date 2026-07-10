#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

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
// Part 1 — substitution: the optimizer's rewrite_hash_joins must stamp a node_join_t
// with algo()==hash (lowered to operator_hash_join_t by create_plan_join) exactly
// when the condition is a single eq(left.key, right.key) on an inner/left/right/full
// join, and leave it algo()==nested (lowered to operator_join_t) otherwise.
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

    // Two-column (key,val) chunk with caller-chosen column names + row count. The
    // distinct column names let a create_plan-level test identify which source
    // table backs each physical join child (build vs probe) after a build-side swap.
    vector::data_chunk_t build_named_chunk(std::pmr::memory_resource* res,
                                           const char* key_name,
                                           const char* val_name,
                                           uint64_t rows) {
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

    INFO("equi-join (eq, left/right keys) is rewritten to hash_join");
    {
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::left, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::right, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        CHECK(plan_type(join_type::full, compare_type::eq, side_t::left, side_t::right) == operator_type::hash_join);
        // Operands swapped (right.key = left.key) is still an equi-join.
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::right, side_t::left) == operator_type::hash_join);
    }

    INFO("non-equi conditions keep the nested-loop join");
    {
        // Not an equality comparison.
        CHECK(plan_type(join_type::inner, compare_type::gt, side_t::left, side_t::right) == operator_type::join);
        CHECK(plan_type(join_type::inner, compare_type::ne, side_t::left, side_t::right) == operator_type::join);
        // eq, but both keys reference the same side — not a left↔right equi-join.
        CHECK(plan_type(join_type::inner, compare_type::eq, side_t::left, side_t::left) == operator_type::join);
    }

    INFO("cross join is never a hash join");
    { CHECK(plan_type(join_type::cross, compare_type::eq, side_t::left, side_t::right) == operator_type::join); }

    INFO("nested-field equi-join (multi-element path) keeps the nested-loop join");
    {
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

    INFO("duplicate keys on both sides — inner/left/right/full");
    {
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

    INFO("NULL keys never match (skipped in build and probe)");
    {
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

    INFO("string join keys");
    {
        create("sl");
        create("sr");
        REQUIRE(run("INSERT INTO " + db + ".sl (s, lv) VALUES ('a', 1), ('b', 2), ('a', 3);")->is_success());
        REQUIRE(run("INSERT INTO " + db + ".sr (s, rv) VALUES ('a', 10), ('c', 30);")->is_success());
        // 'a' → 2 left × 1 right = 2 matched rows; 'b','c' unmatched.
        CHECK(run("SELECT * FROM " + db + ".sl INNER JOIN " + db + ".sr ON sl.s = sr.s;")->size() == 2);
    }
}

// ----------------------------------------------------------------------------
// The batched join_builder gathers matched rows one output chunk
// at a time with ONE indexed copy per (build-chunk, column), REORDERING rows so
// that each such copy targets a contiguous range from a single build chunk. That
// reorder makes output row order unspecified, so every value assert here is under
// ORDER BY. A build side wider than DEFAULT_VECTOR_CAPACITY (2500 rows → 3 build
// chunks) forces one output flush to span several build chunks, and > 1024 matches
// force several output chunks — the exact paths a mis-built gather index would
// scramble. The LEFT join additionally mixes > 1024 left-only NULL-pad rows with
// matched rows inside a single builder.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::hash_join::multi_build_chunk_values") {
    auto config = test_create_config("/tmp/test_hash_join/mbchunk");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string mdb = "mbchunkdb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + mdb + ";");
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + mdb + ".mbl();")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + mdb + ".mbr();")->is_success());

    const int n = 2500;   // > 2 * DEFAULT_VECTOR_CAPACITY → 3 chunks per side
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
    // Left keys [0, n); right keys [shift, n + shift). Overlap = [shift, n) = n-shift
    // unique keys → n-shift matched rows (> 1024). SELECT * column order is logical
    // [left, right] = [mbl.k, mbl.lv, mbr.k, mbr.rv].
    const int matched = n - shift; // 1250

    INFO("inner: every matched row gathered correctly across build chunks");
    {
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + mdb + ".mbl INNER JOIN " + mdb +
                                               ".mbr ON mbl.k = mbr.k ORDER BY mbl.k ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(matched));
        for (size_t row = 0; row < static_cast<size_t>(matched); ++row) {
            const int64_t k = static_cast<int64_t>(row) + shift; // 1250, 1251, ...
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
            const int64_t k = static_cast<int64_t>(row); // 0, 1, ... (sorted probe key)
            REQUIRE(cur->value(0, row).value<int64_t>() == k);       // mbl.k
            REQUIRE(cur->value(1, row).value<int64_t>() == k * 100); // mbl.lv
            if (k < shift) {
                // left-only: no build match → NULL right columns.
                REQUIRE(cur->value(2, row).is_null());
                REQUIRE(cur->value(3, row).is_null());
            } else {
                REQUIRE(cur->value(2, row).value<int64_t>() == k);     // mbr.k
                REQUIRE(cur->value(3, row).value<int64_t>() == k * 7); // mbr.rv
            }
        }
    }
}

// ----------------------------------------------------------------------------
// Build-side selection ("smaller side → hash build"), plan-time.
//
// operator_hash_join_t materializes its physical RIGHT child as the hash build.
// create_plan_join moves the SMALLER table onto that build slot IFF the join is
// INNER, both children are distinct base tables whose live row counts are known
// (fetched into context.row_counts by execute_plan_full), and the current build
// (logical-right) is the LARGER side. The swap re-orders the physical children,
// so the ORIGIN table of the build (right_) child flips — observable here via the
// build child's distinct key-column name. Outer joins, equal/missing/self-join
// counts keep the default order. swapped_=true restores the
// logical [left,right] output order so results stay identical (checked E2E below).
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::hash_join::build_side_selection") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    compute::function_registry_t registry(res);

    using components::catalog::oid_t;
    constexpr oid_t left_table_oid = 42;
    constexpr oid_t right_table_oid = 43;

    // Build an INNER-shaped hash join over two base tables (logical-left key col
    // "lk", logical-right key col "rk"), stamp per-child table oids + the given row
    // counts, lower via create_plan, and return the key-column NAME of whichever
    // physical child became the hash build (right_). "lk" ⇒ the logical-left table
    // moved into the build slot (swapped); "rk" ⇒ default order (not swapped).
    auto build_side_key_name = [&](join_type jt,
                                   uint64_t left_rows,
                                   uint64_t right_rows,
                                   bool populate_counts,
                                   bool same_oid) -> std::string {
        services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
        const oid_t l = left_table_oid;
        const oid_t r = same_oid ? left_table_oid : right_table_oid;
        context.known_oids.insert(l);
        context.known_oids.insert(r);
        if (populate_counts) {
            context.row_counts[l] = left_rows;
            context.row_counts[r] = right_rows;
        }

        auto cond = expressions::make_compare_expression(res,
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

// ----------------------------------------------------------------------------
// Build-side selection end-to-end (disk ON). execute_plan_full fetches
// live row counts for the INNER hash join's child tables, and create_plan_join
// moves the SMALLER table onto the hash build. This query puts the SMALL table on
// the LEFT and the LARGE table on the RIGHT, so the default build (right) is the
// larger side → a correct impl swaps. The swap is correctness-neutral, so a
// SELECT * per-cell check and a two-sided SUM over the join catch any column-order
// inversion the swap could introduce.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::hash_join::build_side_swap_values") {
    auto config = test_create_config("/tmp/test_hash_join/buildside");
    test_clear_directory(config);
    config.disk.on = true; // row-count fetch is gated on an owning disk agent
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string sdb = "buildsidedb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + sdb + ";");
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + sdb + ".small();")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + sdb + ".large();")->is_success());

    // small: keys 1..3 (sv = k*100). large: keys 1..30 (lv = k). Overlap = {1,2,3}.
    REQUIRE(dispatcher
                ->execute_sql(session,
                              "INSERT INTO " + sdb + ".small (k, sv) VALUES (1, 100), (2, 200), (3, 300);")
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
            const int64_t k = static_cast<int64_t>(row) + 1; // 1, 2, 3
            REQUIRE(cur->value(0, row).value<int64_t>() == k);       // small.k
            REQUIRE(cur->value(1, row).value<int64_t>() == k * 100); // small.sv
            REQUIRE(cur->value(2, row).value<int64_t>() == k);       // large.k
            REQUIRE(cur->value(3, row).value<int64_t>() == k);       // large.lv
        }
    }

    INFO("SUM over each side after the swap is correct (catches a column inversion)");
    {
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT SUM(s.sv) AS ssv, SUM(l.lv) AS slv FROM " + sdb + ".small s "
                                           "INNER JOIN " + sdb + ".large l ON s.k = l.k;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // matched keys {1,2,3}: SUM(small.sv) = 100+200+300 = 600; SUM(large.lv) = 1+2+3 = 6.
        REQUIRE(cur->value(0, 0).value<int64_t>() == 600);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 6);
    }

    // A single-table filter that matches NOTHING on the build (right) side. Pushdown
    // pushes it below the join, so the build scan yields ZERO chunks (not one empty
    // chunk). operator_hash_join_t::push must treat an empty build the same as an
    // absent one — emit nothing — instead of dereferencing build_chunks.front() on
    // an empty vector. Before the fix: null data_chunk_t → EXC_BAD_ACCESS in
    // compute_join_layout under -O2 (Debug fires assert(!build_chunks.empty())).
    // `small` (3 rows) stays the build side: it is smaller than `large` (30), so
    // build-side selection does not swap it out. This is the SSB-load crash shrunk
    // to a deterministic 2-table case.
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
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT small.k, SUM(large.lv) AS s FROM " + sdb + ".large INNER JOIN " +
                                               sdb + ".small ON large.k = small.k WHERE small.sv = 99999 "
                                               "GROUP BY small.k ORDER BY small.k;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// ----------------------------------------------------------------------------
// Multi-way (nested) cross-join promotion. An SSB-q2-shaped 3-table
// comma join `FROM lo, p, s` lowers to a left-deep chain of CROSS joins with the
// two equi predicates in a sibling match_t (WHERE), both keys stamped side=left.
// promote_cross_joins must promote BOTH nested cross joins to INNER (each claiming
// the conjunct that straddles its boundary) so rewrite_hash_joins can lower them to
// hash joins; the single-table residual filter must keep applying. Column names are
// distinct so the unqualified WHERE columns resolve unambiguously (the SSB shape).
//
// Hash lowering of the promoted joins is asserted at the plan level by
// components/planner/test/test_promote_multiway.cpp; here we assert end-to-end row
// correctness of the 3-table comma join (which without promotion runs as a slow
// cross-product + residual filter — same rows, so this pins the join semantics).
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::hash_join::multiway_comma_join") {
    auto config = test_create_config("/tmp/test_hash_join/multiway");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string wdb = "multiwaydb";
    dispatcher->execute_sql(session, "CREATE DATABASE " + wdb + ";");
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };

    REQUIRE(run("CREATE TABLE " + wdb + ".lo();")->is_success()); // lineorder-shaped fact
    REQUIRE(run("CREATE TABLE " + wdb + ".p();")->is_success());  // part-shaped dim
    REQUIRE(run("CREATE TABLE " + wdb + ".s();")->is_success());  // supplier-shaped dim

    // lo(l_pk, l_sk, l_rev): part key, supplier key, revenue.
    REQUIRE(run("INSERT INTO " + wdb +
                ".lo (l_pk, l_sk, l_rev) VALUES (1, 10, 100), (1, 20, 200), (2, 10, 300), (2, 20, 400), (3, 30, 500);")
                ->is_success());
    // p(p_pk, p_cat): every part key present; the l_sk=30 row has NO supplier.
    REQUIRE(run("INSERT INTO " + wdb + ".p (p_pk, p_cat) VALUES (1, 'A'), (2, 'B'), (3, 'A');")->is_success());
    // s(s_sk, s_reg): only suppliers 10 and 20 (so l_sk=30 drops in the join).
    REQUIRE(run("INSERT INTO " + wdb + ".s (s_sk, s_reg) VALUES (10, 'X'), (20, 'Y');")->is_success());

    // SELECT * column order = [lo | p | s] = [l_pk, l_sk, l_rev, p_pk, p_cat, s_sk, s_reg].
    // Matches: l_pk=p_pk (all lo rows) then l_sk=s_sk drops the l_sk=30 row -> 4 rows.
    INFO("3-table comma join returns the correctly joined rows");
    {
        auto cur =
            run("SELECT * FROM " + wdb + ".lo, " + wdb + ".p, " + wdb + ".s WHERE l_pk = p_pk AND l_sk = s_sk;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        int64_t rev_sum = 0;
        for (size_t row = 0; row < cur->size(); ++row) {
            // Both equi predicates must actually hold on every emitted row.
            REQUIRE(cur->value(0, row).value<int64_t>() == cur->value(3, row).value<int64_t>()); // l_pk == p_pk
            REQUIRE(cur->value(1, row).value<int64_t>() == cur->value(5, row).value<int64_t>()); // l_sk == s_sk
            rev_sum += cur->value(2, row).value<int64_t>();                                      // l_rev
        }
        CHECK(rev_sum == 1000); // 100 + 200 + 300 + 400 (the l_sk=30 / rev=500 row is unmatched)
    }

    // A single-table residual filter (p_cat = 'A') must still apply after promotion:
    // it keeps only the two rev-100/rev-200 rows (both p_pk=1, cat 'A').
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
