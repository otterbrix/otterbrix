// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>
#include <components/context/context.hpp>
#include <chrono>
#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/tests/generaty.hpp>
#include <core/config.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <fstream>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/wal/wal_sync_mode.hpp>
#include <thread>
#include <unistd.h>

using namespace services::wal;
using namespace components::session;
using namespace components::vector;
using namespace components::types;

#if defined(OTTERBRIX_TSAN_ENABLED)
// TSAN false-positives on synchronized_pool_resource's cross-thread reuse; delegate to new_delete_resource.
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
using test_pool_resource_t = core::pmr::otterbrix_resource;
#endif

namespace catalog_ns = components::catalog;
constexpr auto kMainDb = catalog_ns::well_known_oid::main_database;
constexpr catalog_ns::oid_t kTestTableOid = 16500;

inline std::pmr::vector<data_chunk_t> to_batch(std::unique_ptr<data_chunk_t> chunk) {
    std::pmr::vector<data_chunk_t> batch(chunk->resource());
    batch.emplace_back(std::move(*chunk));
    return batch;
}

// PID-QUALIFIED, or a second concurrent run (e.g. `ctest -j`) could delete segments this one is writing.
static const std::filesystem::path base_wal_worker_path =
    "/tmp/otterbrix_test_wal_worker_" + std::to_string(static_cast<long>(::getpid()));

// The manager self-drives, so a future becomes ready asynchronously; poll before take_ready (which asserts it).
template<typename F>
static decltype(auto) await_ready(F& fut) {
    // Wall-clock deadline: under TSAN or parallel-ctest oversubscription no fixed yield budget is safe.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    REQUIRE(fut.is_ready());
    return std::move(fut).take_ready();
}

// Twin of await_ready for a core::result_wrapper_t; a refusal is a test failure here.
template<typename F>
static auto await_value(F& fut) {
    auto result = await_ready(fut);
    REQUIRE_FALSE(result.has_error());
    return std::move(result.value());
}

struct test_wal_worker {
    test_wal_worker(const std::filesystem::path& path)
        : path_(path)
        , resource_()
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , config_([&]() {
            configuration::config_wal c(path);
            c.on = true;
            return c;
        }())
        , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_,
                                                              scheduler_.get(),
                                                              config_,
                                                              log_,
                                                              components::pipeline::no_mailbox(),
                                                              components::pipeline::no_mailbox())) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
        scheduler_->start();
    }

    ~test_wal_worker() {
        // Stop first (joins workers); a post-stop enqueue then lands harmlessly in the dead scheduler.
        scheduler_->stop();
        manager_.reset();
        std::filesystem::remove_all(path_);
    }

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>>
    send_insert(uint64_t txn_id,
                size_t row_count,
                uint64_t row_start = 0,
                catalog_ns::oid_t table_oid = kTestTableOid) {
        // Built on the fixture's own arena, not the process-global singleton ASAN would flag as leaked.
        auto* arena = &resource_;
        auto chunk = gen_data_chunk(row_count, arena);
        auto chunk_ptr = to_batch(std::make_unique<data_chunk_t>(std::move(chunk)));

        auto [needs_sched, future] = actor_zeta::otterbrix::send(manager_->address(),
                                                                 &manager_wal_replicate_t::write_physical_insert,
                                                                 session_id_t::generate_uid(),
                                                                 table_oid,
                                                                 std::move(chunk_ptr),
                                                                 row_start,
                                                                 row_count,
                                                                 txn_id,
                                                                 kMainDb);
        return std::move(future);
    }

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>>
    send_delete(uint64_t txn_id,
                const std::pmr::vector<int64_t>& row_ids,
                catalog_ns::oid_t table_oid = kTestTableOid) {
        auto ids_copy = row_ids;
        auto [needs_sched, future] = actor_zeta::otterbrix::send(manager_->address(),
                                                                 &manager_wal_replicate_t::write_physical_delete,
                                                                 session_id_t::generate_uid(),
                                                                 table_oid,
                                                                 std::move(ids_copy),
                                                                 static_cast<uint64_t>(row_ids.size()),
                                                                 txn_id,
                                                                 kMainDb);
        return std::move(future);
    }

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>>
    send_update(uint64_t txn_id,
                const std::pmr::vector<int64_t>& row_ids,
                size_t row_count,
                catalog_ns::oid_t table_oid = kTestTableOid) {
        auto* arena = &resource_;
        auto chunk = gen_data_chunk(row_count, arena);
        auto chunk_ptr = to_batch(std::make_unique<data_chunk_t>(std::move(chunk)));
        auto ids_copy = row_ids;

        auto [needs_sched, future] = actor_zeta::otterbrix::send(manager_->address(),
                                                                 &manager_wal_replicate_t::write_physical_update,
                                                                 session_id_t::generate_uid(),
                                                                 table_oid,
                                                                 std::move(ids_copy),
                                                                 std::move(chunk_ptr),
                                                                 static_cast<uint64_t>(row_count),
                                                                 txn_id,
                                                                 kMainDb);
        return std::move(future);
    }

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>>
    send_commit(uint64_t txn_id, wal_sync_mode sync_mode = wal_sync_mode::NORMAL, uint64_t commit_id = 0) {
        // commit_id is the MVCC version timestamp; tests pass 0 unless exercising snapshot-aware replay.
        auto [needs_sched, future] = actor_zeta::otterbrix::send(manager_->address(),
                                                                 &manager_wal_replicate_t::commit_txn,
                                                                 session_id_t::generate_uid(),
                                                                 txn_id,
                                                                 sync_mode,
                                                                 kMainDb,
                                                                 commit_id);
        return std::move(future);
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::vector<record_t>>>
    send_load(services::wal::id_t from_id = 0) {
        auto [needs_sched, future] = actor_zeta::otterbrix::send(manager_->address(),
                                                                 &manager_wal_replicate_t::load,
                                                                 session_id_t::generate_uid(),
                                                                 from_id);
        return std::move(future);
    }

    actor_zeta::unique_future<services::wal::id_t> send_current_wal_id() {
        auto [needs_sched, future] = actor_zeta::otterbrix::send(manager_->address(),
                                                                 &manager_wal_replicate_t::current_wal_id,
                                                                 session_id_t::generate_uid());
        return std::move(future);
    }

    std::filesystem::path path_;
    test_pool_resource_t resource_;
    log_t log_;
    actor_zeta::scheduler_ptr scheduler_;
    configuration::config_wal config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
};

TEST_CASE("wal_worker::insert_write_read") {
    test_wal_worker env(base_wal_worker_path / "insert_wr");

    auto fut_id = env.send_insert(/*txn_id=*/100, /*row_count=*/10, /*row_start=*/0);
    REQUIRE(fut_id.valid());
    auto wal_id = await_value(fut_id);
    REQUIRE(wal_id > 0);

    auto fut_commit = env.send_commit(100);
    REQUIRE(fut_commit.valid());

    auto fut_records = env.send_load(0);
    REQUIRE(fut_records.valid());
    auto records = await_value(fut_records);

    REQUIRE(records.size() >= 2);

    bool found_insert = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT) {
            found_insert = true;
            REQUIRE(r.transaction_id == 100);
            REQUIRE(r.table_oid == kTestTableOid);
            REQUIRE(r.physical_row_count == 10);
            REQUIRE(!r.physical_data.empty());
            REQUIRE(r.physical_data.front().size() == 10);
        }
    }
    REQUIRE(found_insert);
}

TEST_CASE("wal_worker::delete_write_read") {
    test_wal_worker env(base_wal_worker_path / "delete_wr");

    std::pmr::vector<int64_t> ids{1, 3, 5, 7, 9};
    auto fut_id = env.send_delete(/*txn_id=*/200, ids);
    REQUIRE(fut_id.valid());
    auto wal_id = await_value(fut_id);
    REQUIRE(wal_id > 0);

    env.send_commit(200);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);
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

TEST_CASE("wal_worker::update_write_read") {
    test_wal_worker env(base_wal_worker_path / "update_wr");

    std::pmr::vector<int64_t> ids{0, 2, 4};
    auto fut_id = env.send_update(/*txn_id=*/300, ids, /*row_count=*/3);
    REQUIRE(fut_id.valid());
    auto wal_id = await_value(fut_id);
    REQUIRE(wal_id > 0);

    env.send_commit(300);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);
    bool found_update = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_UPDATE) {
            found_update = true;
            REQUIRE(r.transaction_id == 300);
            REQUIRE(r.physical_row_ids.size() == 3);
            REQUIRE(!r.physical_data.empty());
            REQUIRE(r.physical_data.front().size() == 3);
            for (size_t i = 0; i < ids.size(); ++i) {
                REQUIRE(r.physical_row_ids[i] == ids[i]);
            }
        }
    }
    REQUIRE(found_update);
}

TEST_CASE("wal_worker::commit_marker") {
    test_wal_worker env(base_wal_worker_path / "commit_marker");

    env.send_insert(/*txn_id=*/400, /*row_count=*/5);
    env.send_commit(400);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);

    bool found_commit = false;
    for (const auto& r : records) {
        if (r.is_commit_marker()) {
            found_commit = true;
            REQUIRE(r.transaction_id == 400);
        }
    }
    REQUIRE(found_commit);
}

// Corrupt the file after writing, then load: only records before the corruption point should return.
TEST_CASE("wal_worker::corruption_stop") {
    auto test_path = base_wal_worker_path / "corruption_stop";

    // Write records using standalone manager (no fixture, so files survive).
    {
        std::filesystem::remove_all(test_path);
        std::filesystem::create_directories(test_path);

        test_pool_resource_t resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
        configuration::config_wal config(test_path);
        config.on = true;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                                  scheduler.get(),
                                                                  config,
                                                                  log,
                                                                  components::pipeline::no_mailbox(),
                                                                  components::pipeline::no_mailbox());
        scheduler->start();

        for (int i = 0; i < 5; ++i) {
            auto* arena = &resource;
            auto chunk = gen_data_chunk(4, arena);
            auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                         &manager_wal_replicate_t::write_physical_insert,
                                                         session_id_t::generate_uid(),
                                                         kTestTableOid,
                                                         to_batch(std::make_unique<data_chunk_t>(std::move(chunk))),
                                                         static_cast<uint64_t>(i * 4),
                                                         uint64_t{4},
                                                         uint64_t{500},
                                                         kMainDb);
        }
        {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                         &manager_wal_replicate_t::commit_txn,
                                                         session_id_t::generate_uid(),
                                                         uint64_t{500},
                                                         wal_sync_mode::NORMAL,
                                                         kMainDb,
                                                         uint64_t{0});
            // Ordered after the inserts on the same worker; await it so all records flush before stop.
            await_value(fut);
        }

        scheduler->stop();
        manager.reset();
    }

    bool corrupted = false;
    for (auto& entry : std::filesystem::recursive_directory_iterator(test_path)) {
        if (entry.is_regular_file() && entry.file_size() > 64) {
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

    test_pool_resource_t resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                              scheduler.get(),
                                                              config,
                                                              log,
                                                              components::pipeline::no_mailbox(),
                                                              components::pipeline::no_mailbox());
    scheduler->start();

    auto [needs_sched, fut_records] = actor_zeta::otterbrix::send(manager->address(),
                                                                  &manager_wal_replicate_t::load,
                                                                  session_id_t::generate_uid(),
                                                                  services::wal::id_t{0});

    REQUIRE(fut_records.valid());
    auto records = await_value(fut_records);

    REQUIRE(records.size() < 6);

    for (const auto& r : records) {
        REQUIRE_FALSE(r.is_corrupt);
    }

    scheduler->stop();
    manager.reset();
    std::filesystem::remove_all(test_path);
}

// Destroy the worker and re-create it on the same path: init must verify the CRC chain and recover.
TEST_CASE("wal_worker::crc_chain_startup") {
    auto test_path = base_wal_worker_path / "crc_chain";
    services::wal::id_t last_wal_id = 0;

    {
        std::filesystem::remove_all(test_path);
        std::filesystem::create_directories(test_path);

        test_pool_resource_t resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
        configuration::config_wal config(test_path);
        config.on = true;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                                  scheduler.get(),
                                                                  config,
                                                                  log,
                                                                  components::pipeline::no_mailbox(),
                                                                  components::pipeline::no_mailbox());
        scheduler->start();

        {
            auto* arena = &resource;
            auto chunk = gen_data_chunk(8, arena);
            auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                         &manager_wal_replicate_t::write_physical_insert,
                                                         session_id_t::generate_uid(),
                                                         kTestTableOid,
                                                         to_batch(std::make_unique<data_chunk_t>(std::move(chunk))),
                                                         uint64_t{0},
                                                         uint64_t{8},
                                                         uint64_t{600},
                                                         kMainDb);
        }
        {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                         &manager_wal_replicate_t::commit_txn,
                                                         session_id_t::generate_uid(),
                                                         uint64_t{600},
                                                         wal_sync_mode::NORMAL,
                                                         kMainDb,
                                                         uint64_t{0});
        }
        {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                         &manager_wal_replicate_t::current_wal_id,
                                                         session_id_t::generate_uid());
            REQUIRE(fut.valid());
            last_wal_id = await_ready(fut);
            REQUIRE(last_wal_id > 0);
        }

        scheduler->stop();
        manager.reset();
    }

    {
        test_pool_resource_t resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
        configuration::config_wal config(test_path);
        config.on = true;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                                  scheduler.get(),
                                                                  config,
                                                                  log,
                                                                  components::pipeline::no_mailbox(),
                                                                  components::pipeline::no_mailbox());
        scheduler->start();

        auto [ns1, fut_records] = actor_zeta::otterbrix::send(manager->address(),
                                                              &manager_wal_replicate_t::load,
                                                              session_id_t::generate_uid(),
                                                              services::wal::id_t{0});
        auto records = await_value(fut_records);
        REQUIRE(records.size() >= 2);

        auto [ns2, fut_id] = actor_zeta::otterbrix::send(
            manager->address(),
            &manager_wal_replicate_t::write_physical_insert,
            session_id_t::generate_uid(),
            kTestTableOid,
            to_batch(std::make_unique<data_chunk_t>(gen_data_chunk(3, std::pmr::get_default_resource()))),
            uint64_t{0},
            uint64_t{3},
            uint64_t{601},
            kMainDb);
        auto new_wal_id = await_value(fut_id);
        REQUIRE(new_wal_id > last_wal_id);

        scheduler->stop();
        manager.reset();
    }

    std::filesystem::remove_all(test_path);
}

// A small max_segment_size and enough data must trigger rotation into multiple segment files.
TEST_CASE("wal_worker::segment_rotation") {
    auto test_path = base_wal_worker_path / "seg_rotation";
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);

    test_pool_resource_t resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;
    config.max_segment_size = 8192;

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                              scheduler.get(),
                                                              config,
                                                              log,
                                                              components::pipeline::no_mailbox(),
                                                              components::pipeline::no_mailbox());
    scheduler->start();

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>> last_fut;
    for (uint64_t i = 0; i < 50; ++i) {
        auto* arena = &resource;
        auto chunk = gen_data_chunk(20, arena);
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     kTestTableOid,
                                                     to_batch(std::make_unique<data_chunk_t>(std::move(chunk))),
                                                     i * 20,
                                                     uint64_t{20},
                                                     uint64_t{700 + i},
                                                     kMainDb);
        last_fut = std::move(fut);
    }
    await_value(last_fut);

    size_t segment_count = 0;
    for (auto& entry : std::filesystem::recursive_directory_iterator(test_path)) {
        if (entry.is_regular_file() && entry.file_size() > 0) {
            ++segment_count;
        }
    }
    REQUIRE(segment_count >= 2);

    scheduler->stop();
    manager.reset();
    std::filesystem::remove_all(test_path);
}

// A large INSERT (500+ rows) must load back correctly even if it spans pages/segments.
TEST_CASE("wal_worker::spanning_record") {
    test_wal_worker env(base_wal_worker_path / "spanning");

    {
        auto* arena = &env.resource_;
        std::pmr::vector<components::types::complex_logical_type> types(arena);
        types.emplace_back(components::types::logical_type::BIGINT, "id");
        types.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
        types.emplace_back(components::types::logical_type::DOUBLE, "score");
        types.emplace_back(components::types::logical_type::BOOLEAN, "active");
        auto chunk = gen_data_chunk(500, 0, types, arena);
        auto chunk_ptr = to_batch(std::make_unique<data_chunk_t>(std::move(chunk)));

        auto [ns, fut] = actor_zeta::otterbrix::send(env.manager_->address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     kTestTableOid,
                                                     std::move(chunk_ptr),
                                                     uint64_t{0},
                                                     uint64_t{500},
                                                     uint64_t{800},
                                                     kMainDb);
        auto wal_id = await_value(fut);
        REQUIRE(wal_id > 0);
    }

    env.send_commit(800);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);
    bool found = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT && r.transaction_id == 800) {
            found = true;
            REQUIRE(r.physical_row_count == 500);
            REQUIRE(!r.physical_data.empty());
            REQUIRE(r.physical_data.front().size() == 500);
        }
    }
    REQUIRE(found);
}

// wal_sync_mode::FULL: cannot verify fsync was actually called, only that the path doesn't explode.
TEST_CASE("wal_worker::fsync_full_mode") {
    auto test_path = base_wal_worker_path / "fsync_full";
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);

    test_pool_resource_t resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                              scheduler.get(),
                                                              config,
                                                              log,
                                                              components::pipeline::no_mailbox(),
                                                              components::pipeline::no_mailbox());
    scheduler->start();

    {
        auto* arena = &resource;
        auto chunk = gen_data_chunk(10, arena);
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     kTestTableOid,
                                                     to_batch(std::make_unique<data_chunk_t>(std::move(chunk))),
                                                     uint64_t{0},
                                                     uint64_t{10},
                                                     uint64_t{900},
                                                     kMainDb);
        REQUIRE(await_value(fut) > 0);
    }

    {
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::commit_txn,
                                                     session_id_t::generate_uid(),
                                                     uint64_t{900},
                                                     wal_sync_mode::FULL,
                                                     kMainDb,
                                                     uint64_t{0});
        REQUIRE(fut.valid());
    }

    {
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::load,
                                                     session_id_t::generate_uid(),
                                                     services::wal::id_t{0});
        auto records = await_value(fut);
        REQUIRE(records.size() >= 2);
    }

    scheduler->stop();
    manager.reset();
    std::filesystem::remove_all(test_path);
}

// wal_sync_mode::OFF: writes should succeed even though data may not be persisted.
TEST_CASE("wal_worker::fsync_off_mode") {
    auto test_path = base_wal_worker_path / "fsync_off";
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);

    test_pool_resource_t resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto scheduler = std::make_unique<actor_zeta::shared_work>(3, 1000);
    configuration::config_wal config(test_path);
    config.on = true;

    auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource,
                                                              scheduler.get(),
                                                              config,
                                                              log,
                                                              components::pipeline::no_mailbox(),
                                                              components::pipeline::no_mailbox());
    scheduler->start();

    {
        auto* arena = &resource;
        auto chunk = gen_data_chunk(10, arena);
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     kTestTableOid,
                                                     to_batch(std::make_unique<data_chunk_t>(std::move(chunk))),
                                                     uint64_t{0},
                                                     uint64_t{10},
                                                     uint64_t{1000},
                                                     kMainDb);
        REQUIRE(await_value(fut) > 0);
    }

    {
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::commit_txn,
                                                     session_id_t::generate_uid(),
                                                     uint64_t{1000},
                                                     wal_sync_mode::OFF,
                                                     kMainDb,
                                                     uint64_t{0});
        REQUIRE(fut.valid());
    }

    // In OFF mode data may or may not be on disk, but the in-memory load must still work.
    {
        auto [ns, fut] = actor_zeta::otterbrix::send(manager->address(),
                                                     &manager_wal_replicate_t::load,
                                                     session_id_t::generate_uid(),
                                                     services::wal::id_t{0});
        auto records = await_value(fut);
        for (const auto& record : records) {
            REQUIRE_FALSE(record.is_corrupt);
        }
    }

    scheduler->stop();
    manager.reset();
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
