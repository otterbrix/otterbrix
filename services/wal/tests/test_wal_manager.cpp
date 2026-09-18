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
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/wal/wal_sync_mode.hpp>
#include <thread>
#include <unistd.h>

using namespace services::wal;
using namespace components::session;
using namespace components::vector;
using namespace components::types;

namespace catalog = components::catalog;

// Wraps a single chunk into the batch write_physical_insert/update expects.
inline std::pmr::vector<data_chunk_t> to_batch(std::unique_ptr<data_chunk_t> chunk) {
    std::pmr::vector<data_chunk_t> batch(chunk->resource());
    batch.emplace_back(std::move(*chunk));
    return batch;
}

#if defined(OTTERBRIX_TSAN_ENABLED)
// TSAN false-positives on synchronized_pool_resource's cross-thread reuse (manager loop vs
// scheduler workers); delegate to new_delete_resource instead (same workaround as base_spaces.hpp).
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

// The manager runs its own loop thread, so send() futures become ready asynchronously; poll
// with a wall-clock deadline (survives TSAN/ctest -j oversubscription) before take_ready.
template<typename F>
static decltype(auto) await_ready(F& fut) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    REQUIRE(fut.is_ready());
    return std::move(fut).take_ready();
}

// Twin of await_ready: expects the journal to ACCEPT the work (a refusal here is a test failure).
template<typename F>
static auto await_value(F& fut) {
    auto result = await_ready(fut);
    REQUIRE_FALSE(result.has_error());
    return std::move(result.value());
}

constexpr auto kMainDb = catalog::well_known_oid::main_database;
constexpr catalog::oid_t kTestTableOidA = 16500;
constexpr catalog::oid_t kTestTableOidB = 16501;

// PID-qualified like every fixture root here, since a shared root would race concurrent ctest -j runs.
static const std::filesystem::path base_mgr_path =
    "/tmp/otterbrix_test_wal_manager_" + std::to_string(static_cast<long>(::getpid()));

struct test_wal_manager {
    // 0 keeps auto-checkpoint off; non-zero lets the auto-checkpoint TEST_CASE trip it in one commit.
    test_wal_manager(const std::filesystem::path& path, std::uintmax_t auto_checkpoint_threshold_bytes = 0)
        : path_(path)
        , resource_()
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , config_([&]() {
            configuration::config_wal c(path);
            if (auto_checkpoint_threshold_bytes > 0) {
                c.auto_checkpoint_threshold_bytes = auto_checkpoint_threshold_bytes;
            }
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

    ~test_wal_manager() {
        // Stop the scheduler (joins workers) first; post-stop enqueues land harmlessly in the dead one.
        scheduler_->stop();
        manager_.reset();
        std::filesystem::remove_all(path_);
    }

    actor_zeta::address_t address() const { return manager_->address(); }

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>>
    send_insert(catalog::oid_t table_oid, uint64_t txn_id, size_t row_count, uint64_t row_start = 0) {
        // Uses the fixture's own arena (not the ASAN-tracked global): resource_ outlives the manager.
        auto* arena = &resource_;
        auto chunk = gen_data_chunk(row_count, arena);
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::write_physical_insert,
                                                     session_id_t::generate_uid(),
                                                     table_oid,
                                                     to_batch(std::make_unique<data_chunk_t>(std::move(chunk))),
                                                     row_start,
                                                     row_count,
                                                     txn_id,
                                                     kMainDb);
        return std::move(fut);
    }

    actor_zeta::unique_future<core::result_wrapper_t<services::wal::id_t>>
    send_commit(uint64_t txn_id,
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

    actor_zeta::unique_future<core::result_wrapper_t<std::vector<record_t>>>
    send_load(services::wal::id_t from_id = 0) {
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

    actor_zeta::unique_future<core::error_t> send_truncate_before(services::wal::id_t checkpoint_id) {
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::truncate_before,
                                                     session_id_t::generate_uid(),
                                                     checkpoint_id);
        return std::move(fut);
    }

    actor_zeta::unique_future<void> send_run_auto_checkpoint() {
        auto [ns, fut] = actor_zeta::otterbrix::send(address(),
                                                     &manager_wal_replicate_t::run_auto_checkpoint,
                                                     session_id_t::generate_uid());
        return std::move(fut);
    }

    std::filesystem::path path_;
    test_pool_resource_t resource_;
    log_t log_;
    actor_zeta::scheduler_ptr scheduler_;
    configuration::config_wal config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
};

TEST_CASE("wal_manager::route_by_database_oid") {
    test_wal_manager env(base_mgr_path / "route_db");

    // Await both inserts: processing is async, and the filesystem check below must not race them.
    auto f1 = env.send_insert(kTestTableOidA, /*txn_id=*/100, /*row_count=*/5);
    auto f2 = env.send_insert(kTestTableOidB, /*txn_id=*/101, /*row_count=*/5);
    await_value(f1);
    await_value(f2);

    // Everything routes through main_database -> a single worker directory.
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

TEST_CASE("wal_manager::commit_records_table_oid") {
    test_wal_manager env(base_mgr_path / "commit_db");

    auto fut_id = env.send_insert(kTestTableOidA, /*txn_id=*/200, /*row_count=*/8);
    REQUIRE(fut_id.valid());
    auto wal_id = await_value(fut_id);
    REQUIRE(wal_id > 0);

    env.send_commit(200);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);
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

TEST_CASE("wal_manager::load_returns_all") {
    test_wal_manager env(base_mgr_path / "load_all");

    env.send_insert(kTestTableOidA, /*txn_id=*/300, /*row_count=*/3);
    env.send_commit(300);

    env.send_insert(kTestTableOidB, /*txn_id=*/301, /*row_count=*/4);
    env.send_commit(301);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);

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
        REQUIRE(r.id >= prev_id);
        prev_id = r.id;
    }
    REQUIRE(seen_a);
    REQUIRE(seen_b);
}

TEST_CASE("wal_manager::truncate_all") {
    test_wal_manager env(base_mgr_path / "truncate");

    env.send_insert(kTestTableOidA, /*txn_id=*/500, /*row_count=*/5);
    env.send_commit(500);

    auto fut_checkpoint = env.send_current_wal_id();
    auto checkpoint_id = await_ready(fut_checkpoint);
    REQUIRE(checkpoint_id > 0);

    env.send_insert(kTestTableOidA, /*txn_id=*/501, /*row_count=*/3);
    env.send_commit(501);

    // A refusal reply (an unreadable segment) stops the truncate instead of deleting the file.
    auto fut_truncate = env.send_truncate_before(checkpoint_id);
    REQUIRE_FALSE(await_ready(fut_truncate).contains_error());

    auto fut_records = env.send_load(checkpoint_id);
    auto records = await_value(fut_records);
    for (const auto& r : records) {
        if (r.is_physical()) {
            REQUIRE(r.id > checkpoint_id);
        }
    }
}

TEST_CASE("wal_manager::current_wal_id") {
    test_wal_manager env(base_mgr_path / "cur_id");

    env.send_insert(kTestTableOidA, /*txn_id=*/600, /*row_count=*/2);
    env.send_insert(kTestTableOidB, /*txn_id=*/601, /*row_count=*/2);
    env.send_insert(kTestTableOidA, /*txn_id=*/602, /*row_count=*/2);

    auto fut_cur_id = env.send_current_wal_id();
    auto cur_id = await_ready(fut_cur_id);
    REQUIRE(cur_id >= 3);
}

TEST_CASE("wal_manager::rewire_dispatcher_address") {
    test_wal_manager env(base_mgr_path / "sync_addr");

    // Setting the absent mailbox again (twice, as a bootstrap retry would) must not disturb it.
    if (env.manager_) {
        REQUIRE_NOTHROW(env.manager_->set_manager_dispatcher_sync(components::pipeline::no_mailbox()));
        REQUIRE_NOTHROW(env.manager_->set_manager_dispatcher_sync(components::pipeline::no_mailbox()));
    }

    auto fut_id = env.send_insert(kTestTableOidA, /*txn_id=*/900, /*row_count=*/2);
    REQUIRE(fut_id.valid());
    auto wal_id = await_value(fut_id);
    REQUIRE(wal_id > 0);
}

// No disk manager is wired, so run_auto_checkpoint takes the early-return path (checkpoint_all
// never runs, truncate_before must not fire); the full chain is exercised by the disk integration fixtures.
TEST_CASE("wal_manager::auto_checkpoint_triggers_on_byte_threshold") {
    // Tiny threshold so a single commit's WAL bytes cross it.
    test_wal_manager env(base_mgr_path / "auto_ckpt", /*auto_checkpoint_threshold_bytes=*/1);

    REQUIRE_FALSE(env.manager_->needs_auto_checkpoint());

    // commit_txn updates wal_bytes_since_checkpoint_ to the total WAL dir size, tripping the threshold.
    auto fut_ins = env.send_insert(kTestTableOidA, /*txn_id=*/1000, /*row_count=*/8);
    REQUIRE(await_value(fut_ins) > 0);
    auto fut_commit = env.send_commit(1000);
    REQUIRE(await_value(fut_commit) > 0);

    // commit_txn consumes the threshold inline, so needs_auto_checkpoint() is already false here.
    REQUIRE_FALSE(env.manager_->needs_auto_checkpoint());

    auto fut_cur = env.send_current_wal_id();
    auto cur_id = await_ready(fut_cur);
    REQUIRE(cur_id > 0);

    // With no disk manager wired, this must take the early-return path without crashing.
    auto fut_ckpt = env.send_run_auto_checkpoint();
    await_ready(fut_ckpt);

    auto fut_records = env.send_load(0);
    auto records = await_value(fut_records);
    bool found = false;
    for (const auto& r : records) {
        if (r.record_type == wal_record_type::PHYSICAL_INSERT && r.transaction_id == 1000) {
            found = true;
            REQUIRE(r.table_oid == kTestTableOidA);
        }
    }
    REQUIRE(found);

    auto fut_ins2 = env.send_insert(kTestTableOidB, /*txn_id=*/1001, /*row_count=*/4);
    REQUIRE(await_value(fut_ins2) > 0);
    auto fut_commit2 = env.send_commit(1001);
    REQUIRE(await_value(fut_commit2) > 0);
}
