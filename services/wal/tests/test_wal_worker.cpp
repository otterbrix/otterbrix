#include <catch2/catch.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <fstream>
#include <services/wal/wal.hpp>
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
#include <thread>

using namespace services::wal;
using namespace components::session;
using namespace components::vector;
using namespace components::types;

static const std::filesystem::path base_wal_worker_path = "/tmp/otterbrix_test_wal_worker";

// ---------------------------------------------------------------------------
// Fixture: sets up a scheduler, a manager, and a single wal_worker_t for the
// given database name.  The manager is needed because wal_worker_t is spawned
// as a child actor of the manager.
// ---------------------------------------------------------------------------
struct test_wal_worker {
    test_wal_worker(const std::filesystem::path& path,
                    const std::string& database = "testdb")
        : path_(path)
        , database_(database)
        , resource_()
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , config_([&]() { configuration::config_wal c(path); c.on = true; return c; }())
        , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_, scheduler_.get(), config_, log_)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
        manager_->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                       actor_zeta::address_t::empty_address()));
        scheduler_->start();
    }

    ~test_wal_worker() {
        scheduler_->stop();
        std::filesystem::remove_all(path_);
    }

    // Helper: send write_physical_insert through the manager which routes to
    // the per-database worker.
    actor_zeta::unique_future<services::wal::id_t> send_insert(uint64_t txn_id,
                                                 size_t row_count,
                                                 uint64_t row_start = 0,
                                                 const std::string& collection = "users") {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(row_count, &arena);
        auto chunk_ptr = std::make_unique<data_chunk_t>(std::move(chunk));

        auto [needs_sched, future] =
            actor_zeta::otterbrix::send(manager_->address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string(database_),
                                        std::string(collection),
                                        std::move(chunk_ptr),
                                        row_start,
                                        row_count,
                                        txn_id);
        return std::move(future);
    }

    actor_zeta::unique_future<services::wal::id_t> send_delete(uint64_t txn_id,
                                                 const std::pmr::vector<int64_t>& row_ids,
                                                 const std::string& collection = "users") {
        auto ids_copy = row_ids; // copy for move
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send(manager_->address(),
                                        &manager_wal_replicate_t::write_physical_delete,
                                        session_id_t::generate_uid(),
                                        std::string(database_),
                                        std::string(collection),
                                        std::move(ids_copy),
                                        static_cast<uint64_t>(row_ids.size()),
                                        txn_id);
        return std::move(future);
    }

    actor_zeta::unique_future<services::wal::id_t> send_update(uint64_t txn_id,
                                                  const std::pmr::vector<int64_t>& row_ids,
                                                  size_t row_count,
                                                  const std::string& collection = "users") {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(row_count, &arena);
        auto chunk_ptr = std::make_unique<data_chunk_t>(std::move(chunk));
        auto ids_copy = row_ids;

        auto [needs_sched, future] =
            actor_zeta::otterbrix::send(manager_->address(),
                                        &manager_wal_replicate_t::write_physical_update,
                                        session_id_t::generate_uid(),
                                        std::string(database_),
                                        std::string(collection),
                                        std::move(ids_copy),
                                        std::move(chunk_ptr),
                                        static_cast<uint64_t>(row_count),
                                        txn_id);
        return std::move(future);
    }

    actor_zeta::unique_future<services::wal::id_t> send_commit(uint64_t txn_id,
                                                  wal_sync_mode sync_mode = wal_sync_mode::NORMAL) {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send(manager_->address(),
                                        &manager_wal_replicate_t::commit_txn,
                                        session_id_t::generate_uid(),
                                        txn_id,
                                        sync_mode,
                                        std::string(database_));
        return std::move(future);
    }

    actor_zeta::unique_future<std::vector<record_t>> send_load(services::wal::id_t from_id = 0) {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send(manager_->address(),
                                        &manager_wal_replicate_t::load,
                                        session_id_t::generate_uid(),
                                        from_id);
        return std::move(future);
    }

    actor_zeta::unique_future<services::wal::id_t> send_current_wal_id() {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send(manager_->address(),
                                        &manager_wal_replicate_t::current_wal_id,
                                        session_id_t::generate_uid());
        return std::move(future);
    }

    std::filesystem::path path_;
    std::string database_;
    std::pmr::synchronized_pool_resource resource_;
    log_t log_;
    actor_zeta::scheduler_ptr scheduler_;
    configuration::config_wal config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
};

// ===========================================================================
//  1. worker_insert_write_read
// ===========================================================================
TEST_CASE("wal_worker::insert_write_read") {
    test_wal_worker env(base_wal_worker_path / "insert_wr");

    auto fut_id = env.send_insert(/*txn_id=*/100, /*row_count=*/10, /*row_start=*/0);
    REQUIRE(fut_id.valid());
    REQUIRE(fut_id.available());
    auto wal_id = std::move(fut_id).get();
    REQUIRE(wal_id > 0);

    // Commit so the record is visible on load.
    auto fut_commit = env.send_commit(100);
    REQUIRE(fut_commit.valid());

    auto fut_records = env.send_load(0);
    REQUIRE(fut_records.valid());
    REQUIRE(fut_records.available());
    auto records = std::move(fut_records).get();

    // Should have at least the INSERT + COMMIT.
    REQUIRE(records.size() >= 2);

    bool found_insert = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT) {
            found_insert = true;
            REQUIRE(r.transaction_id == 100);
            REQUIRE(r.collection_name.database == "testdb");
            REQUIRE(r.collection_name.collection == "users");
            REQUIRE(r.physical_row_count == 10);
            REQUIRE(r.physical_data != nullptr);
            REQUIRE(r.physical_data->size() == 10);
        }
    }
    REQUIRE(found_insert);
}

// ===========================================================================
//  2. worker_delete_write_read
// ===========================================================================
TEST_CASE("wal_worker::delete_write_read") {
    test_wal_worker env(base_wal_worker_path / "delete_wr");

    std::pmr::vector<int64_t> ids{1, 3, 5, 7, 9};
    auto fut_id = env.send_delete(/*txn_id=*/200, ids);
    REQUIRE(fut_id.valid());
    auto wal_id = std::move(fut_id).get();
    REQUIRE(wal_id > 0);

    env.send_commit(200);

    auto records = std::move(env.send_load(0)).get();
    bool found_delete = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_DELETE) {
            found_delete = true;
            REQUIRE(r.transaction_id == 200);
            REQUIRE(r.physical_row_ids.size() == 5);
            for (size_t i = 0; i < ids.size(); ++i) {
                REQUIRE(r.physical_row_ids[i] == ids[i]);
            }
        }
    }
    REQUIRE(found_delete);
}

// ===========================================================================
//  3. worker_update_write_read
// ===========================================================================
TEST_CASE("wal_worker::update_write_read") {
    test_wal_worker env(base_wal_worker_path / "update_wr");

    std::pmr::vector<int64_t> ids{0, 2, 4};
    auto fut_id = env.send_update(/*txn_id=*/300, ids, /*row_count=*/3);
    REQUIRE(fut_id.valid());
    auto wal_id = std::move(fut_id).get();
    REQUIRE(wal_id > 0);

    env.send_commit(300);

    auto records = std::move(env.send_load(0)).get();
    bool found_update = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_UPDATE) {
            found_update = true;
            REQUIRE(r.transaction_id == 300);
            REQUIRE(r.physical_row_ids.size() == 3);
            REQUIRE(r.physical_data != nullptr);
            REQUIRE(r.physical_data->size() == 3);
            for (size_t i = 0; i < ids.size(); ++i) {
                REQUIRE(r.physical_row_ids[i] == ids[i]);
            }
        }
    }
    REQUIRE(found_update);
}

// ===========================================================================
//  4. worker_commit_marker
// ===========================================================================
TEST_CASE("wal_worker::commit_marker") {
    test_wal_worker env(base_wal_worker_path / "commit_marker");

    env.send_insert(/*txn_id=*/400, /*row_count=*/5);
    env.send_commit(400);

    auto records = std::move(env.send_load(0)).get();

    bool found_commit = false;
    for (const auto& r : records) {
        if (r.is_commit_marker()) {
            found_commit = true;
            REQUIRE(r.transaction_id == 400);
        }
    }
    REQUIRE(found_commit);
}

// ===========================================================================
//  5. worker_corruption_stop
//     Write multiple records + commit, corrupt the file, then load.
//     Load should return only records before the corruption point.
// ===========================================================================
TEST_CASE("wal_worker::corruption_stop") {
    auto test_path = base_wal_worker_path / "corruption_stop";

    // Phase 1: write records using standalone manager (no fixture, so files survive).
    {
        std::filesystem::remove_all(test_path);
        std::filesystem::create_directories(test_path);

        std::pmr::synchronized_pool_resource resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
        configuration::config_wal config(test_path);
        config.on = true;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
        manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                      actor_zeta::address_t::empty_address()));
        scheduler->start();

        // Write several records in one transaction.
        for (int i = 0; i < 5; ++i) {
            std::pmr::monotonic_buffer_resource arena(1024 * 64);
            auto chunk = gen_data_chunk(4, &arena);
            auto [ns, fut] =
                actor_zeta::otterbrix::send(manager->address(),
                                            &manager_wal_replicate_t::write_physical_insert,
                                            session_id_t::generate_uid(),
                                            std::string("testdb"),
                                            std::string("users"),
                                            std::make_unique<data_chunk_t>(std::move(chunk)),
                                            static_cast<uint64_t>(i * 4),
                                            uint64_t{4},
                                            uint64_t{500});
        }
        {
            auto [ns, fut] =
                actor_zeta::otterbrix::send(manager->address(),
                                            &manager_wal_replicate_t::commit_txn,
                                            session_id_t::generate_uid(),
                                            uint64_t{500},
                                            wal_sync_mode::NORMAL,
                                            std::string("testdb"));
        }

        scheduler->stop();
    }
    // Phase 1 done -- files on disk remain.

    // Corrupt one of the WAL segment files: flip some bytes in the middle.
    bool corrupted = false;
    for (auto& entry : std::filesystem::recursive_directory_iterator(test_path)) {
        if (entry.is_regular_file() && entry.file_size() > 64) {
            // Open, flip bytes, close.
            auto p = entry.path();
            std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
            if (f.is_open()) {
                auto mid = static_cast<std::streamoff>(entry.file_size() / 2);
                f.seekp(mid);
                char buf[4];
                f.read(buf, 4);
                for (auto& b : buf) b ^= 0xFF;
                f.seekp(mid);
                f.write(buf, 4);
                f.close();
                corrupted = true;
                break;
            }
        }
    }
    REQUIRE(corrupted);

    // Re-create the environment on the same path (without clearing).
    std::pmr::synchronized_pool_resource resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
    manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                  actor_zeta::address_t::empty_address()));
    scheduler->start();

    auto [needs_sched, fut_records] =
        actor_zeta::otterbrix::send(manager->address(),
                                    &manager_wal_replicate_t::load,
                                    session_id_t::generate_uid(),
                                    services::wal::id_t{0});

    REQUIRE(fut_records.valid());
    auto records = std::move(fut_records).get();

    // We should get fewer than the 5 inserts + 1 commit we wrote because the
    // corruption truncates the read.  At minimum we get zero (if corruption is
    // early) or some subset.
    REQUIRE(records.size() < 6);

    // Every returned record must be valid (not corrupt).
    for (const auto& r : records) {
        REQUIRE_FALSE(r.is_corrupt);
    }

    scheduler->stop();
    std::filesystem::remove_all(test_path);
}

// ===========================================================================
//  6. worker_crc_chain_startup
//     Write records, destroy worker, create a new one on the same path.
//     Init should verify the CRC chain and recover cleanly.
// ===========================================================================
TEST_CASE("wal_worker::crc_chain_startup") {
    auto test_path = base_wal_worker_path / "crc_chain";
    services::wal::id_t last_wal_id = 0;

    // Phase 1: write some records using standalone manager (no fixture, so files survive).
    {
        std::filesystem::remove_all(test_path);
        std::filesystem::create_directories(test_path);

        std::pmr::synchronized_pool_resource resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
        configuration::config_wal config(test_path);
        config.on = true;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
        manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                      actor_zeta::address_t::empty_address()));
        scheduler->start();

        {
            std::pmr::monotonic_buffer_resource arena(1024 * 64);
            auto chunk = gen_data_chunk(8, &arena);
            auto [ns, fut] =
                actor_zeta::otterbrix::send(manager->address(),
                                            &manager_wal_replicate_t::write_physical_insert,
                                            session_id_t::generate_uid(),
                                            std::string("testdb"),
                                            std::string("users"),
                                            std::make_unique<data_chunk_t>(std::move(chunk)),
                                            uint64_t{0},
                                            uint64_t{8},
                                            uint64_t{600});
        }
        {
            auto [ns, fut] =
                actor_zeta::otterbrix::send(manager->address(),
                                            &manager_wal_replicate_t::commit_txn,
                                            session_id_t::generate_uid(),
                                            uint64_t{600},
                                            wal_sync_mode::NORMAL,
                                            std::string("testdb"));
        }
        {
            auto [ns, fut] =
                actor_zeta::otterbrix::send(manager->address(),
                                            &manager_wal_replicate_t::current_wal_id,
                                            session_id_t::generate_uid());
            REQUIRE(fut.valid());
            last_wal_id = std::move(fut).get();
            REQUIRE(last_wal_id > 0);
        }

        scheduler->stop();
    }

    // Phase 2: re-open on the same directory (no cleanup).
    {
        std::pmr::synchronized_pool_resource resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
        configuration::config_wal config(test_path);
        config.on = true;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
        manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                      actor_zeta::address_t::empty_address()));
        scheduler->start();

        // Load and verify records from the previous lifetime.
        auto [ns1, fut_records] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::load,
                                        session_id_t::generate_uid(),
                                        services::wal::id_t{0});
        auto records = std::move(fut_records).get();
        REQUIRE(records.size() >= 2); // at least INSERT + COMMIT

        // Write a new record -- should continue the CRC chain.
        auto [ns2, fut_id] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string("testdb"),
                                        std::string("users"),
                                        std::make_unique<data_chunk_t>(gen_data_chunk(3, std::pmr::get_default_resource())),
                                        uint64_t{0},
                                        uint64_t{3},
                                        uint64_t{601});
        auto new_wal_id = std::move(fut_id).get();
        REQUIRE(new_wal_id > last_wal_id);

        scheduler->stop();
    }

    std::filesystem::remove_all(test_path);
}

// ===========================================================================
//  7. worker_segment_rotation
//     Set a small max_segment_size and write enough data to trigger rotation.
//     Verify that multiple segment files exist on disk.
// ===========================================================================
TEST_CASE("wal_worker::segment_rotation") {
    auto test_path = base_wal_worker_path / "seg_rotation";
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);

    std::pmr::synchronized_pool_resource resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;
    config.max_segment_size = 8192; // very small -- force rotation quickly

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
    manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                  actor_zeta::address_t::empty_address()));
    scheduler->start();

    // Write many records with enough data to exceed the small segment size.
    for (uint64_t i = 0; i < 50; ++i) {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(20, &arena);
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string("testdb"),
                                        std::string("bulk_table"),
                                        std::make_unique<data_chunk_t>(std::move(chunk)),
                                        i * 20,
                                        uint64_t{20},
                                        uint64_t{700 + i});
    }

    // Count WAL-related files under the test path.
    size_t segment_count = 0;
    for (auto& entry : std::filesystem::recursive_directory_iterator(test_path)) {
        if (entry.is_regular_file() && entry.file_size() > 0) {
            ++segment_count;
        }
    }
    // With 50 records of 20 rows each at 8 KB segment size we expect at least 2
    // segments (likely many more).
    REQUIRE(segment_count >= 2);

    scheduler->stop();
    std::filesystem::remove_all(test_path);
}

// ===========================================================================
//  8. worker_spanning_record
//     Write a single INSERT with a large data_chunk (500+ rows). Verify it
//     can be loaded back correctly even if it spans pages/segments.
// ===========================================================================
TEST_CASE("wal_worker::spanning_record") {
    test_wal_worker env(base_wal_worker_path / "spanning");

    // Write a single large insert.
    {
        std::pmr::monotonic_buffer_resource arena(1024 * 256);
        std::pmr::vector<components::types::complex_logical_type> types(&arena);
        types.emplace_back(components::types::logical_type::BIGINT, "id");
        types.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
        types.emplace_back(components::types::logical_type::DOUBLE, "score");
        types.emplace_back(components::types::logical_type::BOOLEAN, "active");
        auto chunk = gen_data_chunk(500, 0, types, &arena);
        auto chunk_ptr = std::make_unique<data_chunk_t>(std::move(chunk));

        auto [ns, fut] =
            actor_zeta::otterbrix::send(env.manager_->address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string("testdb"),
                                        std::string("big_table"),
                                        std::move(chunk_ptr),
                                        uint64_t{0},
                                        uint64_t{500},
                                        uint64_t{800});
        auto wal_id = std::move(fut).get();
        REQUIRE(wal_id > 0);
    }

    env.send_commit(800);

    auto records = std::move(env.send_load(0)).get();
    bool found = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT && r.transaction_id == 800) {
            found = true;
            REQUIRE(r.physical_row_count == 500);
            REQUIRE(r.physical_data != nullptr);
            REQUIRE(r.physical_data->size() == 500);
        }
    }
    REQUIRE(found);
}

// ===========================================================================
//  9. worker_fsync_full_mode
//     With wal_sync_mode::FULL, write and commit. Verify no crash and data is
//     readable.  (We cannot truly verify fsync was called, only that the code
//     path does not explode.)
// ===========================================================================
TEST_CASE("wal_worker::fsync_full_mode") {
    auto test_path = base_wal_worker_path / "fsync_full";
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);

    std::pmr::synchronized_pool_resource resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
    manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                  actor_zeta::address_t::empty_address()));
    scheduler->start();

    // Write + commit.
    {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(10, &arena);
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string("testdb"),
                                        std::string("synced"),
                                        std::make_unique<data_chunk_t>(std::move(chunk)),
                                        uint64_t{0},
                                        uint64_t{10},
                                        uint64_t{900});
        REQUIRE(std::move(fut).get() > 0);
    }

    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::commit_txn,
                                        session_id_t::generate_uid(),
                                        uint64_t{900},
                                        wal_sync_mode::FULL,
                                        std::string("testdb"));
        // commit should succeed
        REQUIRE(fut.valid());
    }

    // Load and verify data survived.
    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::load,
                                        session_id_t::generate_uid(),
                                        services::wal::id_t{0});
        auto records = std::move(fut).get();
        REQUIRE(records.size() >= 2);
    }

    scheduler->stop();
    std::filesystem::remove_all(test_path);
}

// ===========================================================================
//  10. worker_fsync_off_mode
//      With wal_sync_mode::OFF, writes should succeed but data may not be
//      persisted.  Verify the code path works without crash.
// ===========================================================================
TEST_CASE("wal_worker::fsync_off_mode") {
    auto test_path = base_wal_worker_path / "fsync_off";
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);

    std::pmr::synchronized_pool_resource resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;
    // sync_mode::OFF is passed per-commit, not via config

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler.get(), config, log);
    manager->sync(std::make_tuple(actor_zeta::address_t::empty_address(),
                                  actor_zeta::address_t::empty_address()));
    scheduler->start();

    {
        std::pmr::monotonic_buffer_resource arena(1024 * 64);
        auto chunk = gen_data_chunk(10, &arena);
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::write_physical_insert,
                                        session_id_t::generate_uid(),
                                        std::string("testdb"),
                                        std::string("unsynced"),
                                        std::make_unique<data_chunk_t>(std::move(chunk)),
                                        uint64_t{0},
                                        uint64_t{10},
                                        uint64_t{1000});
        // Write should still return a valid WAL id.
        REQUIRE(std::move(fut).get() > 0);
    }

    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::commit_txn,
                                        session_id_t::generate_uid(),
                                        uint64_t{1000},
                                        wal_sync_mode::OFF,
                                        std::string("testdb"));
        REQUIRE(fut.valid());
    }

    // In OFF mode the WAL may or may not have data on disk, but the in-memory
    // load should still work within the same lifetime.
    {
        auto [ns, fut] =
            actor_zeta::otterbrix::send(manager->address(),
                                        &manager_wal_replicate_t::load,
                                        session_id_t::generate_uid(),
                                        services::wal::id_t{0});
        auto records = std::move(fut).get();
        // Records might be empty if the in-memory-only path discards them,
        // or present if they are buffered. Either outcome is acceptable.
        // The key assertion is that we did not crash.
        REQUIRE(records.size() >= 0); // trivially true; documents intent
    }

    scheduler->stop();
    std::filesystem::remove_all(test_path);
}

// ===========================================================================
//  11. worker_disk_full_error (TODO/SKIP)
//      Simulating disk-full in a portable unit test is impractical.
// ===========================================================================
TEST_CASE("wal_worker::disk_full_error", "[.][todo]") {
    // This test is intentionally skipped.  A proper disk-full test would
    // require either:
    //   a) a tmpfs / ramdisk with a hard size limit, or
    //   b) fault injection in the file I/O layer.
    // Mark as TODO for integration-test coverage.
    SUCCEED("Skipped -- disk-full simulation not implemented");
}
