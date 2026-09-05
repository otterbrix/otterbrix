// ============================================================================
// Planner index selection must be PER-OID, not "last table wins".
//
// enrich_logical_plan fetches index info per table of the statement; it must be
// stored per table_oid (context_storage_t::table_indexes) and read through the
// oid-taking accessors. Historically the info landed in two FLAT vectors that
// each loop iteration OVERWROTE, so every scan of a multi-table statement was
// judged against the index set of whichever table happened to enumerate LAST
// (table_oid_dependencies() is an unordered_set — the "winner" is hash-order):
//
//   * a predicate on an UNINDEXED column of table X became an index_scan on
//     X's oid whenever the surviving set (another table's!) carried that column
//     name — manager_index_t::search finds nothing on X and the scan silently
//     returned ZERO rows;
//   * the mirror: a real index on X was invisible whenever another table's
//     key set survived — silent full scan, correct rows, lost index.
//
// The multi-table statements that actually reach create_plan_match_'s index
// selection are UNION branches and scalar-sub-query (InitPlan) statements —
// JOIN children deliberately lower to Filter-over-Seq-Scan, so a join predicate
// never consults the index info. The tests below use:
//   * UNION ALL "symmetric" shapes: each branch's predicate column is indexed
//     only on the OTHER table, so EITHER unordered_set iteration order poisons
//     exactly one branch — red without depending on oid hashing;
//   * a scalar-sub-query shape: the acceptance case verbatim (scan target has
//     no index on the predicate column; the sub-query's table, possibly
//     enumerated last, carries an index on a same-named column).
//
// Tables: ta(id, ka INDEXED, kb) and tb(id, ka, kb INDEXED); each table's
// UNindexed column carries the SAME NAME as the other table's indexed one.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <sstream>
#include <string>

using namespace components;
using namespace components::cursor;

namespace {

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    std::size_t count_occurrences(const std::string& hay, const std::string& needle) {
        std::size_t count = 0;
        for (auto pos = hay.find(needle); pos != std::string::npos; pos = hay.find(needle, pos + needle.size())) {
            ++count;
        }
        return count;
    }

    // ta: id 1..20, ka = id (INDEXED), kb = id % 2 (unindexed).
    // tb: id 1..20, ka = id % 2 (unindexed), kb = id (INDEXED).
    constexpr unsigned kRows = 20;

    void create_and_seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE oiddb;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE oiddb.ta (id bigint, ka bigint, kb bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE oiddb.tb (id bigint, ka bigint, kb bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE INDEX idx_ta_ka ON oiddb.ta (ka);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE INDEX idx_tb_kb ON oiddb.tb (kb);")->is_success());

        std::stringstream qa;
        qa << "INSERT INTO oiddb.ta (id, ka, kb) VALUES ";
        for (unsigned i = 1; i <= kRows; ++i) {
            qa << "(" << i << ", " << i << ", " << (i % 2) << ")" << (i == kRows ? ";" : ", ");
        }
        auto ca = exec(dispatcher, qa.str());
        REQUIRE(ca->is_success());
        REQUIRE(ca->size() == kRows);

        std::stringstream qb;
        qb << "INSERT INTO oiddb.tb (id, ka, kb) VALUES ";
        for (unsigned i = 1; i <= kRows; ++i) {
            qb << "(" << i << ", " << (i % 2) << ", " << i << ")" << (i == kRows ? ";" : ", ");
        }
        auto cb = exec(dispatcher, qb.str());
        REQUIRE(cb->is_success());
        REQUIRE(cb->size() == kRows);
    }

} // namespace

// Direction 1 — CORRECTNESS: a scan whose predicate column is NOT indexed on
// its own table, while another table of the same statement carries an index on
// a same-named column. An oid-blind planner lowers that predicate to an
// index_scan on the scan target's oid; the index manager finds no such index
// there and the empty id set silently reads as "matched nothing" — zero rows.
TEST_CASE("integration::cpp::index_oid_selection::unindexed_predicate_column_returns_rows") {
    auto config = test_create_config("/tmp/test_index_oid_selection/rows");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    create_and_seed(dispatcher);

    INFO("symmetric UNION ALL: each branch filters on ITS OWN unindexed column "
         "(indexed on the other table), so either enumeration order poisons one "
         "branch. Odd ids match in both branches -> 20 rows total.");
    {
        auto cur = exec(dispatcher,
                        "SELECT id FROM oiddb.ta WHERE kb = 1 "
                        "UNION ALL "
                        "SELECT id FROM oiddb.tb WHERE ka = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }

    INFO("scalar sub-query (acceptance shape): the scan target ta has NO index "
         "on kb; the sub-query's tb carries an index on a same-named column. "
         "The 10 odd-id rows must come back.");
    {
        auto cur = exec(dispatcher,
                        "SELECT id, (SELECT MAX(kb) FROM oiddb.tb) AS m "
                        "FROM oiddb.ta WHERE kb = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows / 2);
    }
}

// Direction 2 — the MIRROR (performance): the predicate column IS indexed on
// the scan's own table; another table of the statement has no index on it. An
// oid-blind planner sees only the surviving table's key set, so at most ONE
// branch keeps its index scan and the other silently degrades to a full scan.
// Per-oid selection must produce an Index Scan for BOTH branches.
TEST_CASE("integration::cpp::index_oid_selection::each_table_uses_its_own_index") {
    auto config = test_create_config("/tmp/test_index_oid_selection/explain");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    create_and_seed(dispatcher);

    INFO("symmetric UNION ALL EXPLAIN: each branch filters on ITS OWN indexed "
         "column, so the plan must carry TWO Index Scans regardless of "
         "enumeration order.");
    {
        auto cur = exec(dispatcher,
                        "EXPLAIN SELECT id FROM oiddb.ta WHERE ka = 3 "
                        "UNION ALL "
                        "SELECT id FROM oiddb.tb WHERE kb = 3;");
        REQUIRE(cur->is_success());
        auto text = plan_text(cur);
        INFO("plan:\n" << text);
        REQUIRE(count_occurrences(text, "Index Scan") == 2);
    }

    INFO("scalar sub-query EXPLAIN + result (acceptance mirror): the scan "
         "target ta HAS the index on ka; the sub-query's tb does not — tb's "
         "index-free key set must not hide ta's real index.");
    {
        auto cur = exec(dispatcher,
                        "EXPLAIN SELECT id, (SELECT MAX(ka) FROM oiddb.tb) AS m "
                        "FROM oiddb.ta WHERE ka = 3;");
        REQUIRE(cur->is_success());
        auto text = plan_text(cur);
        INFO("plan:\n" << text);
        REQUIRE(count_occurrences(text, "Index Scan") == 1);

        auto rows = exec(dispatcher,
                         "SELECT id, (SELECT MAX(ka) FROM oiddb.tb) AS m "
                         "FROM oiddb.ta WHERE ka = 3;");
        REQUIRE(rows->is_success());
        REQUIRE(rows->size() == 1);
        REQUIRE(rows->value(0, 0).value<int64_t>() == 3);
    }
}
