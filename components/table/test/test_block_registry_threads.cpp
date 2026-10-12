// A block handle's death writes nothing on its block manager: a second thread drops the last strong
// reference while the manager's thread goes on registering and freeing. Not a production topology (one
// storage stack per table, one table per agent); under TSAN any write from the destructor is a report.

#include <catch2/catch_test_macros.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

namespace {
    std::string threads_db_path() {
        return "/tmp/test_otterbrix_block_registry_threads_" + std::to_string(::getpid()) + ".otbx";
    }

    struct threads_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;

        threads_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };
} // namespace

TEST_CASE("block_registry_threads: the last handle of a released block dies on another thread", "[holder_tsan]") {
    using namespace components::table::storage;
    const auto path = threads_db_path();
    std::remove(path.c_str());
    threads_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());

    constexpr int ROUNDS = 64;
    for (int round = 0; round < ROUNDS; ++round) {
        const uint64_t id = bm.free_block_id();
        auto owner = bm.register_block(id);
        std::weak_ptr<block_handle_t> node = owner;
        std::atomic<int> phase{0};

        std::thread other([&] {
            auto temporary = node.lock();
            REQUIRE(temporary);
            phase.store(1, std::memory_order_release);
            while (phase.load(std::memory_order_acquire) < 2) {
            }
            temporary.reset(); // the owner is gone: ~block_handle_t runs on this thread
            phase.store(3, std::memory_order_release);
        });

        while (phase.load(std::memory_order_acquire) < 1) {
        }
        // The manager's thread releases the block and registers the next one; nothing orders this
        // against the other thread's reset.
        owner.reset();
        phase.store(2, std::memory_order_release);
        bm.mark_as_free(id);
        const uint64_t next = bm.free_block_id();
        auto next_handle = bm.register_block(next);
        CHECK(bm.registry_alive(next));
        other.join();
        CHECK(phase.load() == 3);
        next_handle.reset();
        bm.mark_as_free(next);
    }
    std::remove(path.c_str());
}
