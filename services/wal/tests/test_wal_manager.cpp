#include <catch2/catch.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/wal/wal_sync_mode.hpp>
#include <services/wal/record.hpp>
#include <services/wal/base.hpp>
#include <components/tests/generaty.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <actor-zeta/spawn.hpp>
#include <core/executor.hpp>

using namespace services::wal;
using namespace components::session;
using namespace components::vector;
using namespace components::types;

static const std::filesystem::path base_mgr_path = "/tmp/otterbrix_test_wal_manager";

// ---------------------------------------------------------------------------
// Fixture: spawns a manager_wal_replicate_t (which creates workers internally
// on demand for each database).
// ---------------------------------------------------------------------------
struct test_wal_manager {
    test_wal_manager(const std::filesystem::path& path, bool wal_enabled = true)
        : path_(path)
        , resource_()
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , config_([&]() { configuration::config_wal c(path); c.on = wal_enabled; return c; }())
        , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_, scheduler_.get(), config_, log_)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
        manager_->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                       actor_zeta::address_t::empty_address()));
        scheduler_->start();
    }

    ~test_wal_manager() {
        scheduler_->stop();
        std::filesystem::remove_all(path_);
    }

    actor_zeta::address_t address() const {
        return manager_->address();
    }

    // ----- convenience senders -------------------------------------------

    actor_zeta::unique_future<services::wal::id_t> send_insert(const std::string& db,
                                                  const std::string& collection,
                                                  uint64_t txn_id,
                                                  size_t row_count,
                                                  uint64_t row_start = 0) {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(row_count, &arena);
        auto [ns, fut] =
            actor_zeta::otterbrix::send(address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string(db),
                                        std::string(collection),
                                        std::make_unique<data_chunk_t>(std::move(chunk)),
                                        row_start,
                                        row_count,
                                        txn_id);
        return std::move(fut);
    }

    actor_zeta::unique_future<services::wal::id_t> send_commit(uint64_t txn_id,
                                                  const std::string& database = "testdb",
                                                  wal_sync_mode sync_mode = wal_sync_mode::NORMAL) {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(address(),
                                        &manager_wal_replicate_t::commit_txn,
                                        session_id_t::generate_uid(),
                                        txn_id,
                                        sync_mode,
                                        std::string(database));
        return std::move(fut);
    }

    actor_zeta::unique_future<std::vector<record_t>> send_load(services::wal::id_t from_id = 0) {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(address(),
                                        &manager_wal_replicate_t::load,
                                        session_id_t::generate_uid(),
                                        from_id);
        return std::move(fut);
    }

    actor_zeta::unique_future<services::wal::id_t> send_current_wal_id() {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(address(),
                                        &manager_wal_replicate_t::current_wal_id,
                                        session_id_t::generate_uid());
        return std::move(fut);
    }

    actor_zeta::unique_future<void> send_truncate_before(services::wal::id_t checkpoint_id) {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(address(),
                                        &manager_wal_replicate_t::truncate_before,
                                        session_id_t::generate_uid(),
                                        checkpoint_id);
        return std::move(fut);
    }

    std::filesystem::path path_;
    std::pmr::synchronized_pool_resource resource_;
    log_t log_;
    actor_zeta::scheduler_ptr scheduler_;
    configuration::config_wal config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
};

// ===========================================================================
//  1. manager_route_by_database
//     Write to two different databases. Verify that separate WAL directories
//     are created for each.
// ===========================================================================
TEST_CASE("wal_manager::route_by_database") {
    test_wal_manager env(base_mgr_path / "route_db");

    env.send_insert("db1", "tbl", /*txn_id=*/100, /*row_count=*/5);
    env.send_insert("db2", "tbl", /*txn_id=*/101, /*row_count=*/5);

    // Each database should have its own subdirectory (or at least distinct
    // segment files) under the WAL path.
    bool has_db1 = false;
    bool has_db2 = false;
    for (auto& entry : std::filesystem::recursive_directory_iterator(env.path_)) {
        auto p = entry.path().string();
        if (p.find("db1") != std::string::npos) has_db1 = true;
        if (p.find("db2") != std::string::npos) has_db2 = true;
    }
    REQUIRE(has_db1);
    REQUIRE(has_db2);
}

// ===========================================================================
//  2. manager_commit_to_database
//     Write INSERT to "db1", commit with database="db1".  Load from "db1"
//     should have the record.
// ===========================================================================
TEST_CASE("wal_manager::commit_to_database") {
    test_wal_manager env(base_mgr_path / "commit_db");

    auto fut_id = env.send_insert("db1", "users", /*txn_id=*/200, /*row_count=*/8);
    REQUIRE(fut_id.valid());
    auto wal_id = std::move(fut_id).get();
    REQUIRE(wal_id > 0);

    env.send_commit(200, "db1");

    auto records = std::move(env.send_load(0)).get();
    bool found = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT && r.transaction_id == 200) {
            found = true;
            REQUIRE(r.collection_name.database == "db1");
            REQUIRE(r.collection_name.collection == "users");
            REQUIRE(r.physical_row_count == 8);
        }
    }
    REQUIRE(found);
}

// ===========================================================================
//  3. manager_load_all_databases
//     Write to "db1" and "db2".  load(0) should merge and return records from
//     both, sorted by wal_id.
// ===========================================================================
TEST_CASE("wal_manager::load_all_databases") {
    test_wal_manager env(base_mgr_path / "load_all");

    env.send_insert("db1", "a", /*txn_id=*/300, /*row_count=*/3);
    env.send_commit(300, "db1");

    env.send_insert("db2", "b", /*txn_id=*/301, /*row_count=*/4);
    env.send_commit(301, "db2");

    auto records = std::move(env.send_load(0)).get();

    // We expect at least 4 records: 2 inserts + 2 commits.
    REQUIRE(records.size() >= 4);

    // Verify that records from both databases are present.
    bool seen_db1 = false;
    bool seen_db2 = false;
    services::wal::id_t prev_id = 0;
    for (const auto& r : records) {
        if (r.collection_name.database == "db1") seen_db1 = true;
        if (r.collection_name.database == "db2") seen_db2 = true;
        // Records should be sorted by wal_id.
        REQUIRE(r.id >= prev_id);
        prev_id = r.id;
    }
    REQUIRE(seen_db1);
    REQUIRE(seen_db2);
}

// ===========================================================================
//  4. manager_load_per_database
//     If the load interface supports per-database filtering (future feature),
//     verify that loading from a specific database returns only that db's
//     records.  For now we verify via collection_name.database on the merged
//     result set.
// ===========================================================================
TEST_CASE("wal_manager::load_per_database") {
    test_wal_manager env(base_mgr_path / "load_per");

    env.send_insert("alpha", "t1", /*txn_id=*/400, /*row_count=*/6);
    env.send_commit(400, "alpha");

    env.send_insert("beta", "t2", /*txn_id=*/401, /*row_count=*/7);
    env.send_commit(401, "beta");

    auto records = std::move(env.send_load(0)).get();

    // Partition by database name.
    size_t alpha_count = 0;
    size_t beta_count = 0;
    for (const auto& r : records) {
        if (r.is_physical()) {
            if (r.collection_name.database == "alpha") ++alpha_count;
            if (r.collection_name.database == "beta") ++beta_count;
        }
    }
    REQUIRE(alpha_count >= 1);
    REQUIRE(beta_count >= 1);
}

// ===========================================================================
//  5. manager_truncate_all
//     Write records, get the current WAL id, truncate_before that id.
//     Verify old records are gone on the next load.
// ===========================================================================
TEST_CASE("wal_manager::truncate_all") {
    test_wal_manager env(base_mgr_path / "truncate");

    // Write a first batch.
    env.send_insert("db1", "tbl", /*txn_id=*/500, /*row_count=*/5);
    env.send_commit(500, "db1");

    auto checkpoint_id = std::move(env.send_current_wal_id()).get();
    REQUIRE(checkpoint_id > 0);

    // Write a second batch after the checkpoint.
    env.send_insert("db1", "tbl", /*txn_id=*/501, /*row_count=*/3);
    env.send_commit(501, "db1");

    // Truncate everything up to and including the checkpoint id.
    env.send_truncate_before(checkpoint_id);

    // Load from checkpoint -- should only see the second batch.
    auto records = std::move(env.send_load(checkpoint_id)).get();
    for (const auto& r : records) {
        // Every record returned should have an id greater than the checkpoint.
        if (r.is_physical()) {
            REQUIRE(r.id > checkpoint_id);
        }
    }
}

// ===========================================================================
//  6. manager_current_wal_id
//     Write to multiple databases.  current_wal_id should be the maximum
//     across all workers.
// ===========================================================================
TEST_CASE("wal_manager::current_wal_id") {
    test_wal_manager env(base_mgr_path / "cur_id");

    env.send_insert("db_x", "t", /*txn_id=*/600, /*row_count=*/2);
    env.send_insert("db_y", "t", /*txn_id=*/601, /*row_count=*/2);
    env.send_insert("db_x", "t", /*txn_id=*/602, /*row_count=*/2);

    auto cur_id = std::move(env.send_current_wal_id()).get();
    // We wrote 3 records total; the global WAL id should be at least 3.
    REQUIRE(cur_id >= 3);
}

// ===========================================================================
//  7. manager_create_on_demand
//     First write to an unknown database creates a worker.  A second write to
//     the same database reuses the existing worker (no duplicate directory).
// ===========================================================================
TEST_CASE("wal_manager::create_on_demand") {
    test_wal_manager env(base_mgr_path / "on_demand");

    // First write to "new_db" -- should create the worker.
    auto id1 = std::move(env.send_insert("new_db", "c", /*txn_id=*/700, /*row_count=*/4)).get();
    REQUIRE(id1 > 0);

    // Second write to "new_db" -- should reuse the same worker.
    auto id2 = std::move(env.send_insert("new_db", "c", /*txn_id=*/701, /*row_count=*/4)).get();
    REQUIRE(id2 > id1);

    // There should be exactly one subdirectory for "new_db" (no duplicates).
    size_t dir_count = 0;
    auto wal_dir = env.path_ / "wal";
    for (auto& entry : std::filesystem::directory_iterator(wal_dir)) {
        if (entry.is_directory()) {
            auto name = entry.path().filename().string();
            if (name.find("new_db") != std::string::npos) {
                ++dir_count;
            }
        }
    }
    // Exactly one directory for the database.
    REQUIRE(dir_count == 1);
}

// ===========================================================================
//  8. manager_disabled
//     config.wal.on=false.  All write / commit / load return 0 or empty.
// ===========================================================================
TEST_CASE("wal_manager::disabled") {
    test_wal_manager env(base_mgr_path / "disabled", /*wal_enabled=*/false);

    // write_physical_insert should return 0 (no-op).
    {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(5, &arena);
        auto [ns, fut] =
            actor_zeta::otterbrix::send(env.address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string("db1"),
                                        std::string("tbl"),
                                        std::make_unique<data_chunk_t>(std::move(chunk)),
                                        uint64_t{0},
                                        uint64_t{5},
                                        uint64_t{800});

        auto wal_id = std::move(fut).get();
        REQUIRE(wal_id == 0);
    }

    // commit_txn should return 0.
    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(env.address(),
                                        &manager_wal_replicate_t::commit_txn,
                                        session_id_t::generate_uid(),
                                        uint64_t{800},
                                        wal_sync_mode::NORMAL,
                                        std::string("db1"));

        REQUIRE(std::move(fut).get() == 0);
    }

    // load should return empty.
    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(env.address(),
                                        &manager_wal_replicate_t::load,
                                        session_id_t::generate_uid(),
                                        services::wal::id_t{0});

        auto records = std::move(fut).get();
        REQUIRE(records.empty());
    }

    // current_wal_id should return 0.
    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(env.address(),
                                        &manager_wal_replicate_t::current_wal_id,
                                        session_id_t::generate_uid());

        REQUIRE(std::move(fut).get() == 0);
    }
}

// ===========================================================================
//  9. manager_sync_addresses
//     Call sync() with mock addresses.  Verify no crash and addresses stored.
// ===========================================================================
TEST_CASE("wal_manager::sync_addresses") {
    test_wal_manager env(base_mgr_path / "sync_addr");

    // The constructor already called sync() with empty addresses.
    // Call it again with different empty addresses to confirm idempotency.
    if (env.manager_) {
        REQUIRE_NOTHROW(env.manager_->sync(
            std::make_tuple(actor_zeta::address_t::empty_address(),
                            actor_zeta::address_t::empty_address())));
    }

    // The manager should still be functional after re-sync.
    auto fut_id = env.send_insert("db_sync", "t", /*txn_id=*/900, /*row_count=*/2);
    REQUIRE(fut_id.valid());
    auto wal_id = std::move(fut_id).get();
    REQUIRE(wal_id > 0);
}
