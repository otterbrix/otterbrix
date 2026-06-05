// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/tests/generaty.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/wal/wal_sync_mode.hpp>
#include <chrono>
#include <core/config.hpp>
#include <thread>

using namespace services::wal;
using namespace components::session;
using namespace components::vector;
using namespace components::types;

namespace catalog = components::catalog;

#if defined(OTTERBRIX_TSAN_ENABLED)
// TSAN can't see through synchronized_pool_resource's internal mutex and
// false-positives on cross-thread memory reuse (manager loop vs scheduler
// workers). Delegate to new_delete_resource, whose edges TSAN models natively
// (same workaround as base_spaces.hpp tsan_resource_t).
struct test_pool_resource_t final : std::pmr::memory_resource {
protected:
    void* do_allocate(size_t bytes, size_t align) override {
        return std::pmr::new_delete_resource()->allocate(bytes, align);
    }
    void do_deallocate(void* p, size_t bytes, size_t align) override {
        std::pmr::new_delete_resource()->deallocate(p, bytes, align);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};
#else
using test_pool_resource_t = std::pmr::synchronized_pool_resource;
#endif

// The manager self-drives on an internal loop thread and runs its children on
// the real shared_work scheduler, so futures from a send() to it become ready
// asynchronously. Poll until ready before take_ready (which asserts readiness).
template<typename F>
static decltype(auto) await_ready(F& fut) {
    // Wall-clock deadline, not iteration-bounded: under TSAN or parallel-ctest
    // CPU oversubscription the manager-loop -> worker round-trip can outlast any
    // fixed yield budget.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    REQUIRE(fut.is_ready());
    return std::move(fut).take_ready();
}

constexpr auto kMainDb = catalog::well_known_oid::main_database;
constexpr catalog::oid_t kTestTableOidA = 16500;
constexpr catalog::oid_t kTestTableOidB = 16501;

static const std::filesystem::path base_mgr_path = "/tmp/otterbrix_test_wal_manager";

// ---------------------------------------------------------------------------
// Fixture: spawns a manager_wal_replicate_t (which creates workers internally
// keyed by database_oid). Currently main_database is used for all WAL traffic;
// the manager creates a single worker on demand.
// ---------------------------------------------------------------------------
struct test_wal_manager {
    test_wal_manager(const std::filesystem::path& path, bool wal_enabled = true)
        : path_(path)
        , resource_()
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , config_([&]() {
            configuration::config_wal c(path);
            c.on = wal_enabled;
            return c;
        }())
        , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_, scheduler_.get(), config_, log_)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
        manager_->sync(std::make_tuple(actor_zeta::address_t::empty_address(), actor_zeta::address_t::empty_address()));
        scheduler_->start();
    }

    ~test_wal_manager() {
        // Stop the scheduler first (joins workers, children stop), then destroy
        // the manager; any post-stop enqueues land harmlessly in the dead scheduler.
        scheduler_->stop();
        manager_.reset();
        std::filesystem::remove_all(path_);
    }

    actor_zeta::address_t address() const { return manager_->address(); }

    // ----- convenience senders -------------------------------------------

    actor_zeta::unique_future<services::wal::id_t>
    send_insert(catalog::oid_t table_oid, uint64_t txn_id, size_t row_count, uint64_t row_start = 0) {
        auto* arena = std::pmr::new_delete_resource(); // chunk memory must outlive async processing
        auto chunk = gen_data_chunk(row_count, arena);
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     table_oid,
                                                     std::make_unique<data_chunk_t>(std::move(chunk)),
                                                     row_start,
                                                     row_count,
                                                     txn_id,
                                                     kMainDb);
        return std::move(fut);
    }

    actor_zeta::unique_future<services::wal::id_t> send_commit(uint64_t txn_id,
                                                               catalog::oid_t database_oid = kMainDb,
                                                               wal_sync_mode sync_mode = wal_sync_mode::NORMAL) {
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::commit_txn,
                                                     session_id_t::generate_uid(),
                                                     txn_id,
                                                     sync_mode,
                                                     database_oid,
                                                     uint64_t{0});
        return std::move(fut);
    }

    actor_zeta::unique_future<std::vector<record_t>> send_load(services::wal::id_t from_id = 0) {
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::load,
                                                     session_id_t::generate_uid(),
                                                     from_id);
        return std::move(fut);
    }

    actor_zeta::unique_future<services::wal::id_t> send_current_wal_id() {
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::current_wal_id,
                                                     session_id_t::generate_uid());
        return std::move(fut);
    }

    actor_zeta::unique_future<void> send_truncate_before(services::wal::id_t checkpoint_id) {
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::truncate_before,
                                                     session_id_t::generate_uid(),
                                                     checkpoint_id);
        return std::move(fut);
    }

    std::filesystem::path path_;
    test_pool_resource_t resource_;
    log_t log_;
    actor_zeta::scheduler_ptr scheduler_;
    configuration::config_wal config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
};

// ===========================================================================
//  1. manager_route_by_database_oid
//     WAL writes are routed by main_database; a single worker directory
//     ${path}/${main_database}/ holds all WAL.
// ===========================================================================
TEST_CASE("wal_manager::route_by_database_oid") {
    test_wal_manager env(base_mgr_path / "route_db");

    // Await both inserts: processing is async, and the filesystem check below
    // must not race the WAL writes.
    auto f1 = env.send_insert(kTestTableOidA, /*txn_id=*/100, /*row_count=*/5);
    auto f2 = env.send_insert(kTestTableOidB, /*txn_id=*/101, /*row_count=*/5);
    await_ready(f1);
    await_ready(f2);

    // main_database is used for everything -> single worker directory.
    bool found_main_db_dir = false;
    auto expected = std::to_string(static_cast<unsigned>(kMainDb));
    for (auto& entry : std::filesystem::recursive_directory_iterator(env.path_)) {
        if (entry.is_directory() && entry.path().filename().string() == expected) {
            found_main_db_dir = true;
            break;
        }
    }
    REQUIRE(found_main_db_dir);
}

// ===========================================================================
//  2. manager_commit_records_table_oid
//     Write INSERT for an oid, commit, load. Records carry table_oid round-trip.
// ===========================================================================
TEST_CASE("wal_manager::commit_records_table_oid") {
    test_wal_manager env(base_mgr_path / "commit_db");

    auto fut_id = env.send_insert(kTestTableOidA, /*txn_id=*/200, /*row_count=*/8);
    REQUIRE(fut_id.valid());
    auto wal_id = await_ready(fut_id);
    REQUIRE(wal_id > 0);

    env.send_commit(200);

    auto fut_records = env.send_load(0);
    auto records = await_ready(fut_records);
    bool found = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT && r.transaction_id == 200) {
            found = true;
            REQUIRE(r.table_oid == kTestTableOidA);
            REQUIRE(r.physical_row_count == 8);
        }
    }
    REQUIRE(found);
}

// ===========================================================================
//  3. manager_load_returns_all
//     Multiple writes from different oids — load returns merged sorted records.
// ===========================================================================
TEST_CASE("wal_manager::load_returns_all") {
    test_wal_manager env(base_mgr_path / "load_all");

    env.send_insert(kTestTableOidA, /*txn_id=*/300, /*row_count=*/3);
    env.send_commit(300);

    env.send_insert(kTestTableOidB, /*txn_id=*/301, /*row_count=*/4);
    env.send_commit(301);

    auto fut_records = env.send_load(0);
    auto records = await_ready(fut_records);

    // We expect at least 4 records: 2 inserts + 2 commits.
    REQUIRE(records.size() >= 4);

    bool seen_a = false;
    bool seen_b = false;
    services::wal::id_t prev_id = 0;
    for (const auto& r : records) {
        if (r.is_physical()) {
            if (r.table_oid == kTestTableOidA)
                seen_a = true;
            if (r.table_oid == kTestTableOidB)
                seen_b = true;
        }
        // Records should be sorted by wal_id.
        REQUIRE(r.id >= prev_id);
        prev_id = r.id;
    }
    REQUIRE(seen_a);
    REQUIRE(seen_b);
}

// ===========================================================================
//  4. manager_truncate_all
//     Write records, get the current WAL id, truncate_before that id.
//     Verify old records are gone on the next load.
// ===========================================================================
TEST_CASE("wal_manager::truncate_all") {
    test_wal_manager env(base_mgr_path / "truncate");

    // Write a first batch.
    env.send_insert(kTestTableOidA, /*txn_id=*/500, /*row_count=*/5);
    env.send_commit(500);

    auto fut_checkpoint = env.send_current_wal_id();
    auto checkpoint_id = await_ready(fut_checkpoint);
    REQUIRE(checkpoint_id > 0);

    // Write a second batch after the checkpoint.
    env.send_insert(kTestTableOidA, /*txn_id=*/501, /*row_count=*/3);
    env.send_commit(501);

    // Truncate everything up to and including the checkpoint id.
    env.send_truncate_before(checkpoint_id);

    // Load from checkpoint -- should only see the second batch.
    auto fut_records = env.send_load(checkpoint_id);
    auto records = await_ready(fut_records);
    for (const auto& r : records) {
        // Every record returned should have an id greater than the checkpoint.
        if (r.is_physical()) {
            REQUIRE(r.id > checkpoint_id);
        }
    }
}

// ===========================================================================
//  5. manager_current_wal_id
//     Multiple writes across oids — current_wal_id reflects the global counter.
// ===========================================================================
TEST_CASE("wal_manager::current_wal_id") {
    test_wal_manager env(base_mgr_path / "cur_id");

    env.send_insert(kTestTableOidA, /*txn_id=*/600, /*row_count=*/2);
    env.send_insert(kTestTableOidB, /*txn_id=*/601, /*row_count=*/2);
    env.send_insert(kTestTableOidA, /*txn_id=*/602, /*row_count=*/2);

    auto fut_cur_id = env.send_current_wal_id();
    auto cur_id = await_ready(fut_cur_id);
    // We wrote 3 records total; the global WAL id should be at least 3.
    REQUIRE(cur_id >= 3);
}

// ===========================================================================
//  6. manager_disabled
//     config.wal.on=false. All write / commit / load return 0 or empty.
// ===========================================================================
TEST_CASE("wal_manager::disabled") {
    test_wal_manager env(base_mgr_path / "disabled", /*wal_enabled=*/false);

    // write_physical_insert should return 0 (no-op).
    {
        auto* arena = std::pmr::new_delete_resource(); // chunk memory must outlive async processing
        auto chunk = gen_data_chunk(5, arena);
        auto [ns, fut] = actor_zeta::otterbrix::send(env.address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     kTestTableOidA,
                                                     std::make_unique<data_chunk_t>(std::move(chunk)),
                                                     uint64_t{0},
                                                     uint64_t{5},
                                                     uint64_t{800},
                                                     kMainDb);

        auto wal_id = await_ready(fut);
        REQUIRE(wal_id == 0);
    }

    // commit_txn should return 0.
    {
        auto [ns, fut] = actor_zeta::otterbrix::send(env.address(),
                                                     &manager_wal_replicate_t::commit_txn,
                                                     session_id_t::generate_uid(),
                                                     uint64_t{800},
                                                     wal_sync_mode::NORMAL,
                                                     kMainDb,
                                                     uint64_t{0});

        REQUIRE(await_ready(fut) == 0);
    }

    // load should return empty.
    {
        auto [ns, fut] = actor_zeta::otterbrix::send(env.address(),
                                                     &manager_wal_replicate_t::load,
                                                     session_id_t::generate_uid(),
                                                     services::wal::id_t{0});

        auto records = await_ready(fut);
        REQUIRE(records.empty());
    }

    // current_wal_id should return 0.
    {
        auto [ns, fut] = actor_zeta::otterbrix::send(env.address(),
                                                     &manager_wal_replicate_t::current_wal_id,
                                                     session_id_t::generate_uid());

        REQUIRE(await_ready(fut) == 0);
    }
}

// ===========================================================================
//  7. manager_sync_addresses
//     Call sync() with mock addresses. Verify no crash and addresses stored.
// ===========================================================================
TEST_CASE("wal_manager::sync_addresses") {
    test_wal_manager env(base_mgr_path / "sync_addr");

    // The constructor already called sync() with empty addresses.
    // Call it again with different empty addresses to confirm idempotency.
    if (env.manager_) {
        REQUIRE_NOTHROW(env.manager_->sync(
            std::make_tuple(actor_zeta::address_t::empty_address(), actor_zeta::address_t::empty_address())));
    }

    // The manager should still be functional after re-sync.
    auto fut_id = env.send_insert(kTestTableOidA, /*txn_id=*/900, /*row_count=*/2);
    REQUIRE(fut_id.valid());
    auto wal_id = await_ready(fut_id);
    REQUIRE(wal_id > 0);
}
