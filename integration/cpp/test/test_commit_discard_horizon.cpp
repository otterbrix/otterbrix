#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/table/test/fault_injection_file.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/wal_page.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

// ===========================================================================
// AN ORPHANED commit_id MUST NOT PIN THE HORIZON FOR THE LIFE OF THE PROCESS.
//
// operator_commit_transaction inserts the commit_id into
// transaction_manager_t::in_flight_commits_ in its first hop (txn_commit_drain_msg ->
// commit()) and removes it only in its last (txn_publish_msg -> publish()). Any co_return
// between them leaks the id: the txn is already out of active_, so neither ROLLBACK nor the
// dispatcher's failure-release net can reach it. visible_to_all_locked() floors the horizon
// on min(in_flight_commits_) - 1, and that ONE number gates every reclaim path --
// data_table_t::compact()'s MVCC gate, the DROP-GC tombstone sweep, and the deferred
// index-delete sweep in manager_index_t::on_horizon_advanced. The deferred-delete queue is
// UNBOUNDED BY CONSTRUCTION (eviction from it is the very defect it exists to prevent), so
// it grows for the rest of the process.
//
// in_flight_commits_ is private to the dispatcher and unreachable from a SQL session, so what
// is asserted is the CONSEQUENCE, through two DEV_MODE meters:
// index_deferred_deletes() falls only in on_horizon_advanced, so its return to baseline IS
// the horizon having moved -- process-wide and non-resetting, hence every check is a
// DIFFERENCE; index_repopulations() guards that the un-pinning was not bought with a full
// index rebuild. The queue is polled for QUEUE STATE against a deadline, never for
// wall-clock time.
//
// The COMPACTION half is proved one level down, in
// components/table/test/test_mvcc_operations.cpp ("orphaned_commit_blocks_compaction"),
// where compact() takes the watermark as an argument. Deliberately not re-attempted here:
// the CHECKPOINT statement rebuilds every indexed table whether or not its compact was
// refused, so no counter at this level tells a compaction that ran from one that was skipped.
//
// THE EARLY EXIT IS DRIVEN, NOT SIMULATED. services::wal::dev_set_wal_file_interposer refuses
// the fsync of the COMMIT marker (written under wal_sync_mode::FULL, and the operator checks
// its reply), so COMMIT returns an error -- the proof the run really reached that exit. The
// seam is armed only between the last DML statement and the COMMIT, so nothing else in the
// transaction can be the refusal.
// ===========================================================================

namespace {

    using namespace test_helpers;

    // Process-wide seam, scoped by this object and narrowed to WAL segment files by path.
    // The plan starts switched off: setup traffic must succeed, only the marked fsync fails.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string faulty_marker; // these segment files get the faulty handle driven by `plan`
        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            const auto name = path.string();
            if (inner != nullptr && !faulty_marker.empty() && name.find(faulty_marker) != std::string::npos) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };

    // > row_group_size (1024) so the rows under test are not all in the first row group.
    constexpr unsigned kSeedRows = 2000;

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table) {
        std::stringstream q;
        q << "INSERT INTO " << table << " (id, v) VALUES ";
        for (unsigned i = 0; i < kSeedRows; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kSeedRows ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
    }

    // WHAT IS WAITED FOR IS THE QUEUE, NOT THE CLOCK: the deadline only bounds the
    // failure, the loop exits on the state the sweep produces.
    bool await_deferred_deletes_at(uint64_t target) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (services::index::index_deferred_deletes() > target &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        return services::index::index_deferred_deletes() <= target;
    }

} // namespace

// ===========================================================================
// THE PIN. A transaction that dies at the WAL-marker exit must not stop the sweeps
// that every LATER transaction depends on.
//
// THE DEFECT: the orphaned id sits in in_flight_commits_ forever, the
// horizon never rises past it, try_trigger_cleanup_if_horizon_advanced's
// `new_lowest > last_broadcast_horizon_` gate never re-fires, and the deferred
// index-delete queue -- fed by the ORDINARY delete that follows -- never drains.
// ===========================================================================
TEST_CASE("integration::cpp::commit_discard_horizon::an_orphaned_commit_id_stops_the_sweeps") {
    auto config = make_test_config(integration_fixture_path("test_commit_discard_horizon/sweeps"),
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_"; // WAL segment files only; the .otbx files stay untouched

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE pin;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE pin.t (id bigint, v bigint);")->is_success());
    seed(dispatcher, "pin.t");
    REQUIRE(exec(dispatcher, "CREATE INDEX pin_t_id ON pin.t (id);")->is_success());

    // Warm-up: one ordinary DELETE, drained, so the baseline below is a settled queue
    // rather than whatever the CREATE INDEX left behind.
    REQUIRE(exec(dispatcher, "DELETE FROM pin.t WHERE id = 1900;")->is_success());
    REQUIRE(await_deferred_deletes_at(0));

    const auto deferred_baseline = services::index::index_deferred_deletes();
    const auto repopulations_before = services::index::index_repopulations();

    // ---- the doomed transaction -------------------------------------------------
    auto doomed = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(doomed, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(doomed, "DELETE FROM pin.t WHERE id = 1901;")->is_success());

    // Arm ONLY now: everything above had to reach the journal, so the refusal below can
    // only be the COMMIT marker's own fsync.
    const auto syncs_before = fault.plan.syncs_seen;
    fault.plan.fail_syncs_from = fault.plan.syncs_seen + 1;
    auto commit_cursor = dispatcher->execute_sql(doomed, "COMMIT;");
    fault.plan.fail_syncs_from = 0;

    INFO("the COMMIT must FAIL -- that failure is the proof the run reached the early exit");
    REQUIRE(commit_cursor->is_error());
    REQUIRE(fault.plan.syncs_seen > syncs_before);

    // ---- an ordinary transaction after it ---------------------------------------
    // Its own delete is queued behind the same horizon. Nothing about it is unusual;
    // that is the point -- one dead COMMIT stops every reclaim that follows it.
    REQUIRE(exec(dispatcher, "DELETE FROM pin.t WHERE id = 1902;")->is_success());

    INFO("the deferred index-delete queue must drain again after a discarded transaction");
    REQUIRE(await_deferred_deletes_at(deferred_baseline));

    INFO("un-pinning the horizon must not cost an index rebuild");
    CHECK(services::index::index_repopulations() == repopulations_before);
}

// ===========================================================================
// THE ORDERING GUARD -- it pins the step ORDER itself, not the discard.
//
// The discard is only sound because NOTHING durable or reader-visible carries the
// discarded commit_id: after the reorder the WAL marker is the LAST step that can
// fail, so every step that stamps the id (the DROP-GC remap, the pg_attribute
// backfill, commit_deletes' queue entry and both storage_publish_* calls) runs
// strictly below it and cannot have run at the exit.
//
// Put storage_publish_* back above the marker and this test fails: the doomed rows
// would carry the discarded id as their added_at_commit_id, the discard would take
// that id out of every future snapshot's in-flight set, and the very next published
// commit would drag published_horizon_ above it -- publishing, late and silently, a
// transaction the engine refused and reported as an error.
// ===========================================================================
TEST_CASE("integration::cpp::commit_discard_horizon::a_discarded_transactions_rows_never_appear") {
    auto config = make_test_config(integration_fixture_path("test_commit_discard_horizon/visibility"),
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE ghost;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE ghost.t (id bigint, v bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO ghost.t (id, v) VALUES (1, 1), (2, 2);")->is_success());

    auto doomed = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(doomed, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(doomed, "INSERT INTO ghost.t (id, v) VALUES (777, 777);")->is_success());
    REQUIRE(dispatcher->execute_sql(doomed, "DELETE FROM ghost.t WHERE id = 1;")->is_success());
    fault.plan.fail_syncs_from = fault.plan.syncs_seen + 1;
    auto commit_cursor = dispatcher->execute_sql(doomed, "COMMIT;");
    fault.plan.fail_syncs_from = 0;
    REQUIRE(commit_cursor->is_error());

    // A LATER commit, published normally, drags published_horizon_ ABOVE the discarded
    // id. This is the step that turns a wrongly-erased in-flight entry into visible rows.
    REQUIRE(exec(dispatcher, "INSERT INTO ghost.t (id, v) VALUES (3, 3);")->is_success());

    INFO("the refused transaction's INSERT must never become visible");
    {
        auto cur = exec(dispatcher, "SELECT id FROM ghost.t WHERE id = 777;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("and its DELETE must never take effect");
    {
        auto cur = exec(dispatcher, "SELECT id FROM ghost.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}
