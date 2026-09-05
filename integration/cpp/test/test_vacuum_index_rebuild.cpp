#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <chrono>
#include <map>
#include <string>
#include <string_view>
#include <vector>

// WHO IS ALLOWED TO REBUILD AN INDEX, AND ON WHAT FACT.
//
// An index entry stores a PHYSICAL row id. Exactly one operation in the tree hands a
// surviving row a NEW physical id: data_table_t::compact, which rebuilds the table at row id
// 0. It has ONE call site -- agent_disk_t::checkpoint_inner -- so a full index rebuild is owed
// by, and only by, a round that ran that call site. Everything else that rebuilds is paying
// for a renumbering that did not happen: a full drained scan of the table plus a clear() that
// unlinks the index directory plus a refill of every entry, per table, per call.
//
// THE TWO CASES BELOW ARE THE TWO SIDES OF THAT ONE RULE, and they are deliberately written
// against the SAME counter so neither can be satisfied by weakening the other:
//
//   * VACUUM compacts NOTHING. agent_disk_t::vacuum_inner calls data_table_t::cleanup_versions
//     and nothing else; cleanup_versions reaches row_version_manager_t::cleanup_append, which
//     swaps chunk_info objects inside vector_info_ and moves no row. So VACUUM owes ZERO
//     rebuilds, and the first case says so with a number rather than with a comment.
//
//   * A CHECKPOINT compacts, so it owes one rebuild per indexed table -- and the second case
//     is the guard that stops the first from being "fixed" by deleting the rebuild outright.
//     It is written to fail TWICE over if the rebuild is removed from operator_checkpoint_t:
//     the counter drops to zero AND the indexed lookup starts answering with a row that
//     merely moved into the id the stale entry names.
//
// WHY A COUNTER AND NOT A STOPWATCH. The defect is WORK THAT NEED NOT HAPPEN, and a stopwatch
// measures the machine as much as the code -- the same reason the _id dedup on this branch was
// gated on rows read rather than on milliseconds. index_repopulations() counts calls to
// manager_index_t::repopulate_table, which is the clear+refill itself.
//
// AND A COUNTER ALONE IS NOT ENOUGH, so every case also pins the ANSWER: after the operation
// the indexed lookup must return exactly what a full scan of the table returns, key by key.
// Without that half, "0 rebuilds" would also be satisfied by an index that answers nothing.

namespace {

    // > row_group_size (1024) by a wide margin: 3000 rows span three row groups, and deleting
    // the middle third moves every surviving tail row by a full 1000 ids, so a stale index
    // cannot accidentally still name the right row.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001; // inclusive
    constexpr int64_t kDeleteTo = 2000;   // inclusive

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    void load(otterbrix::wrapper_dispatcher_t* d, const std::string& db) {
        for (int64_t start = 1; start <= kRows; start += 500) {
            std::string sql = "INSERT INTO " + db + ".t (id, k) VALUES ";
            for (int64_t i = start; i < start + 500 && i <= kRows; ++i) {
                if (i != start) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
            }
            sql += ";";
            auto session = otterbrix::session_id_t();
            REQUIRE(d->execute_sql(session, sql)->is_success());
        }
    }

    // THE FULL SCAN IS THE TRUTH. `SELECT id, k` carries no predicate an index could serve, so
    // this is the table's own answer about which rows exist and what key each one holds.
    std::map<int64_t, int64_t> full_scan_truth(otterbrix::wrapper_dispatcher_t* d, const std::string& db) {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "SELECT id, k FROM " + db + ".t;");
        REQUIRE(cur->is_success());
        std::map<int64_t, int64_t> key_to_id;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            const auto id = cur->value(0, r).value<int64_t>();
            const auto k = cur->value(1, r).value<int64_t>();
            key_to_id.emplace(k, id);
        }
        return key_to_id;
    }

    // The index must answer EXACTLY what the full scan above says, key by key -- both for keys
    // that survive (one row, and it must be the RIGHT row) and for keys that were deleted (no
    // row at all). The EXPLAIN is load-bearing: without it a planner that stopped routing
    // `WHERE k = ...` to the index would pass every row assertion here while the index rotted.
    void index_must_agree_with_the_full_scan(otterbrix::wrapper_dispatcher_t* d, const std::string& db) {
        const auto truth = full_scan_truth(d, db);
        REQUIRE(truth.size() == static_cast<std::size_t>(kRows - (kDeleteTo - kDeleteFrom + 1)));

        {
            auto session = otterbrix::session_id_t();
            auto plan = d->execute_sql(session, "EXPLAIN SELECT id FROM " + db + ".t WHERE k = 10;");
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO("plan for the indexed predicate:\n" << text);
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }

        // A spread of keys across all three row groups, including the ones whose physical id a
        // compaction would have moved by a full 1000, and the deleted middle that must stay
        // absent through the index just as it is absent from the scan.
        std::vector<int64_t> probes;
        for (int64_t id = 1; id <= kRows; id += 97) {
            probes.push_back(10 * id);
        }
        probes.push_back(10 * kDeleteFrom);
        probes.push_back(10 * kDeleteTo);
        probes.push_back(10 * kRows);

        for (const auto key : probes) {
            auto session = otterbrix::session_id_t();
            auto cur = d->execute_sql(session, "SELECT id FROM " + db + ".t WHERE k = " + std::to_string(key) + ";");
            REQUIRE(cur->is_success());
            const auto expected = truth.find(key);
            INFO("indexed lookup for k = " << key);
            if (expected == truth.end()) {
                CHECK(cur->size() == 0);
                continue;
            }
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == expected->second);
        }
    }

} // namespace

// VACUUM RENUMBERS NOTHING, SO IT OWES NO REBUILD.
//
// RED before the fix: operator_vacuum_t scanned pg_class and called repopulate_table for EVERY
// relation it found -- a full drained scan of each table plus a clear-and-refill of each of its
// indexes -- on the strength of a comment saying "the compact pass above invalidated row
// positions". There is no compact pass above: agent_disk_t::vacuum_inner carries its own note
// saying nothing is compacted there, because under the split free pool a compact whose release
// no header commits can only spend space. So the counter read one repopulate per relation where
// the correct number is none.
TEST_CASE("integration::cpp::vacuum_index_rebuild::vacuum_does_not_rebuild_what_it_never_renumbers") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_vacuum_index_rebuild/vacuum");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // Far above anything this case writes: an automatic checkpoint round DOES compact, and one
    // firing mid-case would legitimately repopulate and make the number below unattributable.
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE vdb;")->is_success());
    REQUIRE(exec("CREATE TABLE vdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX k_idx ON vdb.t (k);")->is_success());
    load(d, "vdb");
    REQUIRE(exec("DELETE FROM vdb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                 " AND id <= " + std::to_string(kDeleteTo) + ";")
                ->is_success());

    INFO("the index and the table agree BEFORE the VACUUM, so a disagreement after it is the VACUUM's");
    index_must_agree_with_the_full_scan(d, "vdb");

    services::index::reset_index_repopulations();
    services::disk::reset_table_checkpoints();

    REQUIRE(exec("VACUUM;")->is_success());

    INFO("NOT VACUOUS: a checkpoint round inside the VACUUM would compact, and then a rebuild "
         "would be owed after all");
    REQUIRE(services::disk::table_checkpoints() == 0);

    INFO("VACUUM moves no physical row id, so it owes no index rebuild at all");
    CHECK(services::index::index_repopulations() == 0);

    INFO("and the answer must be unchanged: the index says exactly what the full scan says");
    index_must_agree_with_the_full_scan(d, "vdb");

    // A second VACUUM over the same unchanged table: still nothing to renumber, still nothing
    // to rebuild. This is the shape that made the old cost recurring rather than one-off.
    services::index::reset_index_repopulations();
    REQUIRE(exec("VACUUM;")->is_success());
    CHECK(services::index::index_repopulations() == 0);
    index_must_agree_with_the_full_scan(d, "vdb");
}

// THE OTHER SIDE OF THE SAME RULE, and the guard that keeps the case above honest.
//
// A CHECKPOINT reaches data_table_t::compact through agent_disk_t::checkpoint_inner, so it DOES
// renumber, so it DOES owe the rebuild. Remove the rebuild from operator_checkpoint_t and this
// case fails twice: the counter reads 0, and `WHERE k = <key of a tail row>` answers with
// whichever row moved into the physical id the stale entry still names.
TEST_CASE("integration::cpp::vacuum_index_rebuild::a_compacting_checkpoint_still_owes_the_rebuild") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_vacuum_index_rebuild/checkpoint");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX k_idx ON cdb.t (k);")->is_success());
    load(d, "cdb");
    REQUIRE(exec("DELETE FROM cdb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                 " AND id <= " + std::to_string(kDeleteTo) + ";")
                ->is_success());

    index_must_agree_with_the_full_scan(d, "cdb");

    services::index::reset_index_repopulations();
    services::disk::reset_table_checkpoints();

    REQUIRE(exec("CHECKPOINT;")->is_success());

    INFO("NOT VACUOUS: with no checkpoint round there is no compaction and nothing to rebuild for");
    REQUIRE(services::disk::table_checkpoints() > 0);

    INFO("a round that renumbered must rebuild what it renumbered");
    CHECK(services::index::index_repopulations() > 0);

    INFO("and the rebuilt index must name the rows the table now holds, not the ones it used to");
    index_must_agree_with_the_full_scan(d, "cdb");
}

// THE PRICE, in the one place a number belongs. Hidden by default ([.]) because it loads a
// table big enough for the per-table scan-and-refill to dominate; run it with [vacuumcost].
TEST_CASE("integration::cpp::vacuum_index_rebuild::what_a_vacuum_costs_on_an_indexed_table",
          "[.][vacuumcost]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_vacuum_index_rebuild/cost");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    constexpr int64_t kCostRows = 20000;

    REQUIRE(exec("CREATE DATABASE pdb;")->is_success());
    REQUIRE(exec("CREATE TABLE pdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX k_idx ON pdb.t (k);")->is_success());
    for (int64_t start = 1; start <= kCostRows; start += 1000) {
        std::string sql = "INSERT INTO pdb.t (id, k) VALUES ";
        for (int64_t i = start; i < start + 1000 && i <= kCostRows; ++i) {
            if (i != start) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        sql += ";";
        REQUIRE(exec(sql)->is_success());
    }

    // One warm VACUUM first so the number below is the steady-state cost, not the first-touch one.
    REQUIRE(exec("VACUUM;")->is_success());

    std::vector<double> ms;
    for (int round = 0; round < 3; ++round) {
        services::index::reset_index_repopulations();
        const auto begin = std::chrono::steady_clock::now();
        REQUIRE(exec("VACUUM;")->is_success());
        const auto end = std::chrono::steady_clock::now();
        ms.push_back(std::chrono::duration<double, std::milli>(end - begin).count());
        WARN("VACUUM over " << kCostRows << " indexed rows: " << ms.back() << " ms, "
                            << services::index::index_repopulations() << " index repopulations");
    }
    CHECK(ms.size() == 3);
}
