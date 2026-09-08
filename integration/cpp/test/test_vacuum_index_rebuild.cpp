#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <chrono>
#include <map>
#include <string>
#include <string_view>
#include <vector>

// Only data_table_t::compact (reached solely via agent_disk_t::checkpoint_inner) gives a
// surviving row a new physical id, so only a round that compacts owes an index rebuild
// (manager_index_t::repopulate_table). VACUUM's vacuum_inner reaches only
// cleanup_versions/cleanup_append, which swaps chunk_info and moves no row, so it owes zero.
// The two cases below check that with the same counter, index_repopulations(), plus that the
// index still agrees with a full scan key by key -- a counter of 0 alone would also be
// satisfied by an index that answers nothing.

namespace {

    // 3000 spans three row groups (> row_group_size 1024); deleting the middle third moves
    // every surviving tail row by 1000 ids, so a stale index can't accidentally still be right.
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

    // `SELECT id, k` carries no predicate an index could serve, so this is the table's own
    // answer about which rows exist and what key each holds.
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

    // The EXPLAIN is load-bearing: without it a planner that stopped routing `WHERE k = ...`
    // to the index would pass every row assertion here while the index rotted.
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

        // A spread across all three row groups, plus the deleted middle, which must stay
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

// VACUUM's vacuum_inner never compacts (split free pool: a compact with no committed header
// can only spend space, not free it), so it renumbers nothing and owes no index rebuild.
TEST_CASE("integration::cpp::vacuum_index_rebuild::vacuum_does_not_rebuild_what_it_never_renumbers") {
    auto config = test_create_config(integration_fixture_path("test_vacuum_index_rebuild/vacuum"));
    test_clear_directory(config);
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

    // A second VACUUM over the same unchanged table: still nothing to renumber or rebuild.
    services::index::reset_index_repopulations();
    REQUIRE(exec("VACUUM;")->is_success());
    CHECK(services::index::index_repopulations() == 0);
    index_must_agree_with_the_full_scan(d, "vdb");
}

// A CHECKPOINT reaches data_table_t::compact via checkpoint_inner, so it DOES renumber and owes
// the rebuild: remove it from operator_checkpoint_t and this fails twice -- the counter reads 0,
// and a tail-row lookup answers with whichever row moved into the stale id.
TEST_CASE("integration::cpp::vacuum_index_rebuild::a_compacting_checkpoint_still_owes_the_rebuild") {
    auto config = test_create_config(integration_fixture_path("test_vacuum_index_rebuild/checkpoint"));
    test_clear_directory(config);
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
    auto config = test_create_config(integration_fixture_path("test_vacuum_index_rebuild/cost"));
    test_clear_directory(config);
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
