#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

// ROLLBACK does not take the rows back out of a user table, and the pending stamps it leaves
// behind lock that table out of every later checkpoint -- no crash required.
//
// txn_abort_drain_msg keeps only the OIDs of the tables the txn appended to
// (services/dispatcher/dispatcher.cpp:1064-1067) where the commit drain keeps the RANGES
// (:1036-1041), and operator_abort_transaction hands storage_revert_appends the pg_catalog
// ranges alone (components/physical_plan/operators/operator_abort_transaction.cpp:48-54). So the
// appended rows keep insert_id = the aborted transaction id -- >= TRANSACTION_ID_START,
// 4611686018427388000 (components/table/row_version_manager.hpp:32) -- and only commit_append
// ever rewrites such a stamp (components/table/row_version_manager.cpp:493-501). MVCC hides the
// rows, so nothing in a SELECT says anything is wrong; but
// table_storage_t::has_versions_above(compact_watermark) (services/disk/manager_disk.cpp:225-230)
// answers true forever, and agent_disk_t::checkpoint_inner takes the deferral branch every round
// (services/disk/agent_disk.cpp:2047-2055).
//
// Two independent observables, both taken here:
//   * services::disk::checkpoint_entries_deferred() (agent_disk.cpp:2133, declared under DEV_MODE
//     at agent_disk.hpp:42-44; DEV_MODE is on for this binary, integration/cpp/test/CMakeLists.txt).
//     NOT services::disk::table_checkpoints() -- that one counts ROUNDS and is bumped
//     unconditionally at the top of checkpoint_inner (agent_disk.cpp:1988), so it keeps rising
//     while the table is being skipped.
//   * the table's durable checkpoint sidecar `table.otbx.wal_id`. The deferral `continue`s at
//     :2055, BEFORE stage_checkpoint_sidecar at :2060, so a skipped entry's sidecar is never
//     restaged and never republished (:2115) -- its wal id freezes at the last round that
//     actually processed the table. An entry with nothing to write still advances it
//     (advance_wal_id_without_rewrite, :2073-2080), so "the id did not move" means "skipped",
//     not "nothing to do".

using namespace test_helpers;

namespace {

    constexpr int64_t kSeedRows = 200;
    constexpr int64_t kBatchRows = 50;
    constexpr int kRoundsAfterTheRollback = 4;

    std::string insert_batch(int64_t first, int64_t count) {
        std::string sql = "INSERT INTO rdb.t (id, payload) VALUES ";
        for (int64_t i = 0; i < count; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            const auto id = std::to_string(first + i);
            sql += "(" + id + ", 'payload-" + id + "')";
        }
        sql += ";";
        return sql;
    }

    // The sidecar is a bare uint64 wal id (stage_checkpoint_sidecar, services/disk/agent_disk.cpp:1914-1915).
    uint64_t read_sidecar_wal_id(const std::filesystem::path& sidecar) {
        std::ifstream in(sidecar, std::ios::binary);
        uint64_t value = 0;
        if (!in.is_open()) {
            return 0;
        }
        in.read(reinterpret_cast<char*>(&value), sizeof(value));
        return in ? value : 0;
    }

    // config.disk.path holds one directory per namespace oid plus the fixed system one
    // (manager_disk_t::system_dir_oid(), services/disk/manager_disk.hpp:405-408); these cases
    // create exactly one user table, so exactly one sidecar must answer.
    std::filesystem::path the_one_user_table_sidecar(const std::filesystem::path& db_root) {
        const auto system_dir =
            std::to_string(static_cast<unsigned>(services::disk::manager_disk_t::system_dir_oid()));
        std::vector<std::filesystem::path> found;
        for (const auto& ns : std::filesystem::directory_iterator(db_root)) {
            if (!ns.is_directory() || ns.path().filename().string() == system_dir) {
                continue;
            }
            for (const auto& tbl : std::filesystem::directory_iterator(ns.path())) {
                if (!tbl.is_directory()) {
                    continue;
                }
                auto sidecar = tbl.path() / "table.otbx.wal_id";
                if (std::filesystem::exists(sidecar)) {
                    found.push_back(sidecar);
                }
            }
        }
        REQUIRE(found.size() == 1);
        return found.front();
    }

    std::size_t visible_rows(otterbrix::wrapper_dispatcher_t* d) {
        auto cur = exec(d, "SELECT id FROM rdb.t;");
        REQUIRE(cur->is_success());
        return cur->size();
    }

    // auto_checkpoint_threshold_bytes = 0 disables the background round outright
    // (services/wal/manager_wal_replicate.hpp:134), so every number below belongs to a CHECKPOINT
    // this test issued.
    configuration::config quiet_config(const std::filesystem::path& path) {
        auto config = make_test_config(path);
        config.log.level = log_t::level::off;
        config.wal.auto_checkpoint_threshold_bytes = 0;
        return config;
    }

    void create_the_table(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE rdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE rdb.t (id bigint, payload text);")->is_success());
    }

} // namespace

// Measured on this tree: the round right after the ROLLBACK reports deferred=1 rewritten=0 and
// the sidecar wal id stops moving; the four rounds after it repeat that, each with a fresh
// COMMITTED insert waiting to be written.
TEST_CASE("integration::cpp::rollback_freezes_checkpoint::a_rolled_back_insert_may_not_lock_the_table_out_of_every_later_checkpoint") {
    auto config = quiet_config(integration_fixture_path("test_rollback_freezes_checkpoint/rolled_back"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    create_the_table(d);
    REQUIRE(exec(d, insert_batch(1, kSeedRows))->is_success());
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    const auto sidecar = the_one_user_table_sidecar(config.disk.path);
    const auto sidecar_at_seed = read_sidecar_wal_id(sidecar);

    // ---------------------------------------------------------------------------------------
    // CONTROL, same table, same fixture, same statements minus the rollback: a committed insert
    // followed by CHECKPOINT. If this half is not green the case is accusing the wrong thing --
    // the counters, the sidecar, or the fixture -- rather than the rollback.
    // ---------------------------------------------------------------------------------------
    REQUIRE(exec(d, insert_batch(kSeedRows + 1, kBatchRows))->is_success());
    services::disk::reset_checkpoint_entry_tallies();
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    const auto control_deferred = services::disk::checkpoint_entries_deferred();
    const auto control_rewritten = services::disk::checkpoint_entries_rewritten();
    const auto sidecar_after_control = read_sidecar_wal_id(sidecar);

    INFO("CONTROL round: deferred=" << control_deferred << " rewritten=" << control_rewritten
                                    << " sidecar " << sidecar_at_seed << " -> " << sidecar_after_control);
    REQUIRE(control_rewritten >= 1);
    REQUIRE(control_deferred == 0);
    REQUIRE(sidecar_after_control > sidecar_at_seed);

    const auto committed_rows = visible_rows(d);
    REQUIRE(committed_rows == static_cast<std::size_t>(kSeedRows + kBatchRows));

    // ---------------------------------------------------------------------------------------
    // THE DEFECT: one BEGIN / INSERT / ROLLBACK on a shared session. Nothing else changes.
    // ---------------------------------------------------------------------------------------
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(session, "BEGIN;")->is_success());
        REQUIRE(d->execute_sql(session, insert_batch(100000, kBatchRows))->is_success());
        REQUIRE(d->execute_sql(session, "ROLLBACK;")->is_success());
    }

    INFO("NOT THE ACCUSATION: rollback visibility is correct -- the rolled-back rows are hidden");
    REQUIRE(visible_rows(d) == committed_rows);

    // A fresh COMMITTED insert before every round below, so a round that skips this table skips a
    // table that unambiguously has committed data waiting to be written.
    std::ostringstream rounds;
    uint64_t rounds_that_skipped_the_table = 0;
    uint64_t deferrals_after_the_rollback = 0;
    auto sidecar_before_round = sidecar_after_control;
    int64_t next_id = kSeedRows + kBatchRows + 1;

    for (int round = 1; round <= kRoundsAfterTheRollback; ++round) {
        REQUIRE(exec(d, insert_batch(next_id, kBatchRows))->is_success());
        next_id += kBatchRows;

        services::disk::reset_checkpoint_entry_tallies();
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        const auto deferred = services::disk::checkpoint_entries_deferred();
        const auto rewritten = services::disk::checkpoint_entries_rewritten();
        const auto sidecar_now = read_sidecar_wal_id(sidecar);

        deferrals_after_the_rollback += deferred;
        if (sidecar_now == sidecar_before_round) {
            ++rounds_that_skipped_the_table;
        }
        rounds << "round " << round << ": deferred=" << deferred << " rewritten=" << rewritten << " sidecar "
               << sidecar_before_round << " -> " << sidecar_now << '\n';
        sidecar_before_round = sidecar_now;
    }

    INFO("after ROLLBACK, with a committed insert waiting before each round:\n" << rounds.str());

    INFO("the durable per-table fact: `table.otbx.wal_id` must move on every round that processed "
         "the entry -- an entry with nothing to write still advances it");
    REQUIRE(rounds_that_skipped_the_table == 0);

    INFO("and the round tally must name no deferral: the control round above proved this fixture "
         "defers nothing of its own");
    REQUIRE(deferrals_after_the_rollback == 0);

    INFO("the committed rows written after the rollback must still be readable");
    REQUIRE(visible_rows(d) ==
            static_cast<std::size_t>(kSeedRows + kBatchRows + kRoundsAfterTheRollback * kBatchRows));
}

// The control the case above cannot contain: the identical explicit transaction that COMMITS.
// It must be green on this tree AND after the fix -- if it ever reddens, the case above is
// accusing "an explicit transaction" or "a second batch of inserts", not ROLLBACK.
TEST_CASE("integration::cpp::rollback_freezes_checkpoint::the_same_explicit_transaction_that_commits_keeps_the_table_checkpointable") {
    auto config = quiet_config(integration_fixture_path("test_rollback_freezes_checkpoint/committed"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    create_the_table(d);
    REQUIRE(exec(d, insert_batch(1, kSeedRows))->is_success());
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    const auto sidecar = the_one_user_table_sidecar(config.disk.path);
    auto sidecar_before_round = read_sidecar_wal_id(sidecar);

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(session, "BEGIN;")->is_success());
        REQUIRE(d->execute_sql(session, insert_batch(100000, kBatchRows))->is_success());
        REQUIRE(d->execute_sql(session, "COMMIT;")->is_success());
    }

    REQUIRE(visible_rows(d) == static_cast<std::size_t>(kSeedRows + kBatchRows));

    std::ostringstream rounds;
    uint64_t rounds_that_skipped_the_table = 0;
    uint64_t deferrals_after_the_commit = 0;
    int64_t next_id = kSeedRows + 1;

    for (int round = 1; round <= kRoundsAfterTheRollback; ++round) {
        REQUIRE(exec(d, insert_batch(next_id, kBatchRows))->is_success());
        next_id += kBatchRows;

        services::disk::reset_checkpoint_entry_tallies();
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        const auto deferred = services::disk::checkpoint_entries_deferred();
        const auto rewritten = services::disk::checkpoint_entries_rewritten();
        const auto sidecar_now = read_sidecar_wal_id(sidecar);

        deferrals_after_the_commit += deferred;
        if (sidecar_now == sidecar_before_round) {
            ++rounds_that_skipped_the_table;
        }
        rounds << "round " << round << ": deferred=" << deferred << " rewritten=" << rewritten << " sidecar "
               << sidecar_before_round << " -> " << sidecar_now << '\n';
        sidecar_before_round = sidecar_now;
    }

    INFO("after COMMIT, with a committed insert waiting before each round:\n" << rounds.str());
    REQUIRE(rounds_that_skipped_the_table == 0);
    REQUIRE(deferrals_after_the_commit == 0);
}

// The guard on the fix, not the fix. Removing the rows at abort is a TRUNCATION
// (components/table/collection.cpp collection_t::revert_append: total_rows_ -= count, and each
// touched row group reverts from the local start), so it is only ever safe while the aborted range
// is still the table's LAST one. This case puts a committed range behind it and requires that the
// ROLLBACK leave that range alone. Drop the tail_only guard in
// agent_disk_t::storage_revert_appends_inner and it goes red: the truncation walks back over the
// neighbour's rows and they vanish from a table that never touched them.
TEST_CASE("integration::cpp::rollback_freezes_checkpoint::a_rollback_may_not_truncate_a_neighbours_committed_rows") {
    auto config = quiet_config(integration_fixture_path("test_rollback_freezes_checkpoint/neighbour"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();

    create_the_table(d);
    REQUIRE(exec(d, insert_batch(1, kSeedRows))->is_success());
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    // A appends first, so its rows take the row ids right after the seed -- and stay uncommitted.
    constexpr int64_t kAbortedFirstId = 500000;
    constexpr int64_t kNeighbourFirstId = 900000;
    auto aborting = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(aborting, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(aborting, insert_batch(kAbortedFirstId, kBatchRows))->is_success());

    // B appends AFTER A and commits, which is what takes A's range out of the tail position.
    REQUIRE(exec(d, insert_batch(kNeighbourFirstId, kBatchRows))->is_success());

    const auto before_rollback = visible_rows(d);
    REQUIRE(before_rollback == static_cast<std::size_t>(kSeedRows + kBatchRows));

    REQUIRE(d->execute_sql(aborting, "ROLLBACK;")->is_success());

    INFO("the neighbour's committed rows must all survive a ROLLBACK that did not append them");
    REQUIRE(visible_rows(d) == before_rollback);
    for (int64_t i = 0; i < kBatchRows; ++i) {
        auto cur = exec(d, "SELECT id FROM rdb.t WHERE id = " + std::to_string(kNeighbourFirstId + i) + ";");
        REQUIRE(cur->is_success());
        INFO("neighbour row id=" << (kNeighbourFirstId + i) << " must still be there");
        REQUIRE(cur->size() == 1);
    }

    INFO("and the aborted rows must stay invisible, whether or not they were physically removed");
    for (int64_t i = 0; i < kBatchRows; ++i) {
        auto cur = exec(d, "SELECT id FROM rdb.t WHERE id = " + std::to_string(kAbortedFirstId + i) + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // Not asserted: whether this table checkpoints again. It is exactly the case the guard declines
    // to fix, and pinning today's answer either way would freeze a decision this test does not own.
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());
}
