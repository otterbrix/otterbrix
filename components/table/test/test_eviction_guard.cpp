// Eviction guard for MANAGED (non-reloadable) blocks: before the guard, such a block was still
// treated as evictable, so a memory-pressure pass could unload one still referenced and the next
// pin() dereferenced a null buffer_ -> SIGSEGV (the large-table-scan crash).

#include <catch2/catch_test_macros.hpp>

#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/file_buffer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <cstring>
#include <memory_resource>

namespace {
    using namespace components::table::storage;

    // Re-exports buffer_pool_t's protected eviction internals so the test can drive a pass deterministically.
    struct test_buffer_pool_t final : buffer_pool_t {
        using buffer_pool_t::add_to_eviction_queue;
        using buffer_pool_t::buffer_pool_t;
        using buffer_pool_t::evict_blocks;
        using buffer_pool_t::eviction_result; // protected nested type -> make namable here
        using buffer_pool_t::maximum_memory;
    };

    struct test_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        test_buffer_pool_t buffer_pool;
        standard_buffer_manager_t buffer_manager;

        explicit test_env_t(uint64_t pool_limit)
            : buffer_pool(&resource, pool_limit, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };
} // namespace

TEST_CASE("buffer manager: re-pinning an evicted managed block does not crash", "[step1]") {
    using namespace components::table::storage;

    // Two-step limit: block_allocation_size() needs a live buffer_manager, so a provisional pool limit is set first.
    test_env_t env(uint64_t(1) << 24);
    const uint64_t block_alloc = env.buffer_manager.block_allocation_size();
    env.buffer_pool.maximum_memory = 8 * block_alloc;

    const uint64_t block_size = env.buffer_manager.block_size();

    // size == block_size routes register_transient_memory() to the managed allocate() path.
    auto h1_result = env.buffer_manager.register_transient_memory(block_size, block_size);
    REQUIRE_FALSE(h1_result.has_error());
    std::shared_ptr<block_handle_t> h1 = std::move(h1_result.value());
    REQUIRE(h1 != nullptr);
    REQUIRE_FALSE(h1->is_reloadable());
    REQUIRE(h1->block_id() >= MAXIMUM_BLOCK);
    REQUIRE(h1->state() == block_state::LOADED);

    constexpr std::byte PATTERN[8] = {std::byte{0xDE},
                                      std::byte{0xAD},
                                      std::byte{0xBE},
                                      std::byte{0xEF},
                                      std::byte{0xCA},
                                      std::byte{0xFE},
                                      std::byte{0xBA},
                                      std::byte{0xBE}};
    {
        auto pinned_result = env.buffer_manager.pin(h1);
        REQUIRE_FALSE(pinned_result.has_error());
        buffer_handle_t pinned = std::move(pinned_result.value());
        REQUIRE(pinned.is_valid());
        std::byte* data = pinned.ptr();
        for (uint64_t j = 0; j < block_size; j++) {
            data[j] = PATTERN[j % sizeof(PATTERN)];
        }
        // pinned's destructor unpins; with the guard, unpin() skips enqueueing the managed block.
    }
    REQUIRE(h1->readers() == 0);

    // Enqueues h1 explicitly and runs an eviction pass, exercising can_unload() directly.
    env.buffer_pool.add_to_eviction_queue(h1);
    auto eviction = env.buffer_pool.evict_blocks(memory_tag::TRANSIENT_TABLE,
                                                 /*extra_memory=*/0,
                                                 /*memory_limit=*/0,
                                                 /*buffer=*/nullptr);
    (void) eviction.success;

    // The discriminator: without the guard, h1 was UNLOADED so pin() derefs a null buffer_ ->
    // SIGSEGV; with the guard h1 is still LOADED and pin() returns a valid buffer.
    auto repinned_result = env.buffer_manager.pin(h1);
    REQUIRE_FALSE(repinned_result.has_error());
    buffer_handle_t repinned = std::move(repinned_result.value());
    REQUIRE(repinned.is_valid());
    REQUIRE(h1->state() == block_state::LOADED);
    REQUIRE_FALSE(h1->is_unloaded());

    const std::byte* data = repinned.ptr();
    for (uint64_t j = 0; j < block_size; j++) {
        REQUIRE(data[j] == PATTERN[j % sizeof(PATTERN)]);
    }

}

// Companion: genuine pool exhaustion is a clean OOM ERROR VALUE, not a throw. Every managed block
// is held by a live pin, so evict_blocks() can free NOTHING once the pool fills; the (N+1)-th
// registration must surface out_of_memory and return normally rather than throwing.
TEST_CASE("buffer manager: pool exhaustion of pinned managed blocks returns out_of_memory, not throw", "[step1]") {
    using namespace components::table::storage;

    test_env_t env(uint64_t(1) << 24);
    const uint64_t block_alloc = env.buffer_manager.block_allocation_size();
    const uint64_t block_size = env.buffer_manager.block_size();

    constexpr uint64_t N = 4;
    env.buffer_pool.maximum_memory = N * block_alloc;

    std::vector<std::shared_ptr<block_handle_t>> handles;
    std::vector<buffer_handle_t> pins;
    handles.reserve(N);
    pins.reserve(N);

    bool saw_out_of_memory = false;

    REQUIRE_NOTHROW([&] {
        for (uint64_t i = 0; i < N; i++) {
            auto h_result = env.buffer_manager.register_transient_memory(block_size, block_size);
            REQUIRE_FALSE(h_result.has_error());
            std::shared_ptr<block_handle_t> h = std::move(h_result.value());
            REQUIRE(h != nullptr);
            REQUIRE_FALSE(h->is_reloadable());

            auto pin_result = env.buffer_manager.pin(h);
            REQUIRE_FALSE(pin_result.has_error());
            buffer_handle_t pinned = std::move(pin_result.value());
            REQUIRE(pinned.is_valid());
            REQUIRE(h->readers() > 0);

            handles.push_back(std::move(h));
            pins.push_back(std::move(pinned));
        }

        for (uint64_t i = 0; i < 3; i++) {
            auto over_result = env.buffer_manager.register_transient_memory(block_size, block_size);
            REQUIRE(over_result.has_error());
            REQUIRE(over_result.error().type == core::error_code_t::out_of_memory);
            saw_out_of_memory = true;
        }
    }());

    REQUIRE(saw_out_of_memory);
}

// unpin() must decrement readers_ for TINY_BUFFER too: skipping it leaks the reader count, invisible
// today only because can_unload() rejects transient blocks — it would matter once they're spillable.
TEST_CASE("buffer manager: unpinning a tiny buffer releases its reader", "[step1]") {
    using namespace components::table::storage;

    test_env_t env(uint64_t(1) << 24);
    const uint64_t block_alloc = env.buffer_manager.block_allocation_size();
    env.buffer_pool.maximum_memory = 8 * block_alloc;

    const uint64_t block_size = env.buffer_manager.block_size();
    // Smaller than a block: register_transient_memory routes this to register_small_memory (TINY_BUFFER).
    const uint64_t tiny_size = block_size / 32;
    REQUIRE(tiny_size > 0);

    auto handle_result = env.buffer_manager.register_transient_memory(tiny_size, block_size);
    REQUIRE_FALSE(handle_result.has_error());
    std::shared_ptr<block_handle_t> handle = std::move(handle_result.value());
    REQUIRE(handle != nullptr);
    REQUIRE(handle->buffer_type() == file_buffer_type::TINY_BUFFER);

    // A freshly registered handle is already LOADED with one reader held by the registration.
    const int32_t readers_at_rest = handle->readers();

    for (int i = 0; i < 5; ++i) {
        auto pinned = env.buffer_manager.pin(handle);
        REQUIRE_FALSE(pinned.has_error());
        REQUIRE(pinned.value().is_valid());
    }

    INFO("readers after five balanced pin/unpin pairs: " << handle->readers() << ", at rest it was "
                                                         << readers_at_rest);
    CHECK(handle->readers() == readers_at_rest);
}
