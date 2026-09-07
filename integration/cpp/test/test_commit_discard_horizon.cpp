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

// An orphaned commit_id must not pin the horizon for the life of the process: operator_commit_transaction
// inserts it into transaction_manager_t::in_flight_commits_ on its first hop and removes it only on its
// last, so a co_return between them leaks it past ROLLBACK -- visible_to_all_locked() floors the horizon
// on min(in_flight_commits_) - 1, gating compact(), the DROP-GC sweep, and manager_index_t::on_horizon_advanced.
// in_flight_commits_ is private, so this is asserted indirectly via index_deferred_deletes() and
// index_repopulations().
//
// The compaction half is proved separately in components/table/test/test_mvcc_operations.cpp
// ("orphaned_commit_blocks_compaction"); not repeated here because CHECKPOINT rebuilds every indexed
// table regardless of a refused compact, so no counter at this level distinguishes the two.

namespace {

    using namespace test_helpers;

    // Process-wide seam, narrowed to WAL segment files by path; the plan starts disabled so
    // setup traffic succeeds before the marked fsync is armed.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string faulty_marker;
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

    // Polls the queue's own state against a deadline; it does not wait for wall-clock time.
    bool await_deferred_deletes_at(uint64_t target) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (services::index::index_deferred_deletes() > target &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        return services::index::index_deferred_deletes() <= target;
    }

} // namespace

// try_trigger_cleanup_if_horizon_advanced's `new_lowest > last_broadcast_horizon_` gate must
// re-fire after the discard, or the deferred-delete queue never drains again.
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

    // Warm-up DELETE settles the queue so the baseline below isn't CREATE INDEX's leftovers.
    REQUIRE(exec(dispatcher, "DELETE FROM pin.t WHERE id = 1900;")->is_success());
    REQUIRE(await_deferred_deletes_at(0));

    const auto deferred_baseline = services::index::index_deferred_deletes();
    const auto repopulations_before = services::index::index_repopulations();

    auto doomed = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(doomed, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(doomed, "DELETE FROM pin.t WHERE id = 1901;")->is_success());

    // Armed only now: everything above already reached the journal, so any fsync refusal
    // below is the COMMIT marker's own.
    const auto syncs_before = fault.plan.syncs_seen;
    fault.plan.fail_syncs_from = fault.plan.syncs_seen + 1;
    auto commit_cursor = dispatcher->execute_sql(doomed, "COMMIT;");
    fault.plan.fail_syncs_from = 0;

    INFO("the COMMIT must FAIL -- that failure is the proof the run reached the early exit");
    REQUIRE(commit_cursor->is_error());
    REQUIRE(fault.plan.syncs_seen > syncs_before);

    REQUIRE(exec(dispatcher, "DELETE FROM pin.t WHERE id = 1902;")->is_success());

    INFO("the deferred index-delete queue must drain again after a discarded transaction");
    REQUIRE(await_deferred_deletes_at(deferred_baseline));

    INFO("un-pinning the horizon must not cost an index rebuild");
    CHECK(services::index::index_repopulations() == repopulations_before);
}

// The WAL marker must be the last step that can fail -- every id-stamping step (the DROP-GC
// remap, the pg_attribute backfill, commit_deletes' queue entry, both storage_publish_* calls)
// runs below it, so none of them can have run at this exit. Move storage_publish_* above the
// marker and the doomed rows would carry the discarded id as added_at_commit_id, and the next
// published commit would drag published_horizon_ above it -- silently publishing a transaction
// the engine refused.
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

    // Publishing this commit drags published_horizon_ past the discarded id -- the step that
    // would leak it into visibility.
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
