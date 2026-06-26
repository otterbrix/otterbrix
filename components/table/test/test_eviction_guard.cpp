// ---------------------------------------------------------------------------
// Eviction guard for MANAGED (non-reloadable) buffer-pool blocks.
//
// The defect this codifies:
//   A MANAGED in-memory block (block_id >= MAXIMUM_BLOCK) has no backing store,
//   so block_handle_t::load() returns an EMPTY buffer_handle_t for it once it is
//   UNLOADED -- there is nothing to re-read. Before the guard, such a block was
//   still treated as evictable: unpin() enqueued it on the eviction queue, and
//   can_unload() returned true for it, so a later memory-pressure pass
//   (evict_blocks) UNLOADED it and freed its buffer. The next pin() then took the
//   "reload" path -- load() returned an invalid buffer and pin() dereferenced the
//   now-null buffer_ -> SIGSEGV. This is the large-table-scan crash: a scan
//   registers many managed segment blocks, the pool fills, eviction unloads an
//   earlier (still-referenced) managed block, and re-pinning it crashes.
//
// The guard:
//   * block_handle_t::is_reloadable()  -> block_id_ < MAXIMUM_BLOCK
//   * block_handle_t::can_unload()     -> false for non-reloadable blocks
//     (the load-bearing safety net: evict/purge can never unload them)
//   * standard_buffer_manager_t::unpin() skips enqueueing them (optimization)
//   With the guard the managed block stays resident through the eviction pass, so
//   the re-pin returns a VALID buffer with the original bytes intact.
//
// This test drives the load-bearing half of the guard -- can_unload() -- directly
// by enqueueing the managed block itself and running an eviction pass against it,
// so it does not depend on the unpin() optimization to reproduce the crash.
// ---------------------------------------------------------------------------

#include <catch2/catch.hpp>

#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/file_buffer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <cstring>
#include <memory_resource>

namespace {
    using namespace components::table::storage;

    // buffer_pool_t::evict_blocks() / ::add_to_eviction_queue() / ::maximum_memory
    // are protected; re-export them in a test-only subclass so the test can drive
    // an eviction pass deterministically. evict_blocks() never throws: it returns
    // {found, reservation}.
    struct test_buffer_pool_t final : buffer_pool_t {
        using buffer_pool_t::add_to_eviction_queue;
        using buffer_pool_t::buffer_pool_t;
        using buffer_pool_t::evict_blocks;
        using buffer_pool_t::eviction_result; // protected nested type -> make namable here
        using buffer_pool_t::maximum_memory;
    };

    struct test_env_t {
        std::pmr::synchronized_pool_resource resource;
        core::filesystem::local_file_system_t fs;
        test_buffer_pool_t buffer_pool;
        standard_buffer_manager_t buffer_manager;

        // Small pool: a couple of managed blocks fit. The eviction pass below is
        // driven explicitly (memory_limit = 0), so the absolute limit only needs
        // to be large enough that registering the block never itself OOMs.
        explicit test_env_t(uint64_t pool_limit)
            : buffer_pool(&resource, pool_limit, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };
} // namespace

TEST_CASE("buffer manager: re-pinning an evicted managed block does not crash", "[step1]") {
    using namespace components::table::storage;

    // Construct with a provisional limit, then set the real one once we can query
    // block_allocation_size(). Room for several managed blocks so that registering
    // and re-pinning the block never itself trips the over-subscribe OOM path --
    // the eviction pass below is driven explicitly with memory_limit = 0.
    test_env_t env(uint64_t(1) << 24);
    const uint64_t block_alloc = env.buffer_manager.block_allocation_size();
    env.buffer_pool.maximum_memory = 8 * block_alloc;

    const uint64_t block_size = env.buffer_manager.block_size();

    // Allocate a MANAGED (non-reloadable) block. size == block_size routes
    // register_transient_memory() to the managed allocate() path, giving a
    // block_id >= MAXIMUM_BLOCK with destroy_condition == BLOCK.
    auto h1_result = env.buffer_manager.register_transient_memory(block_size, block_size);
    REQUIRE_FALSE(h1_result.has_error());
    std::shared_ptr<block_handle_t> h1 = std::move(h1_result.value());
    REQUIRE(h1 != nullptr);
    REQUIRE_FALSE(h1->is_reloadable());
    REQUIRE(h1->block_id() >= MAXIMUM_BLOCK);
    REQUIRE(h1->state() == block_state::LOADED);

    // Pin to obtain the buffer, write a recognizable pattern, unpin.
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
        // pinned's destructor unpins (readers -> 0). With the guard, unpin() skips
        // enqueueing the managed block; without it the block gets enqueued.
    }
    REQUIRE(h1->readers() == 0);

    // Drive memory pressure ONTO h1: enqueue it explicitly (modelling that the
    // scan marked it evictable), then run an eviction pass with memory_limit 0.
    // This exercises can_unload() -- the load-bearing half of the guard --
    // directly, independent of the unpin() optimization.
    //   Without the guard: can_unload() == true  -> evict_blocks UNLOADS h1.
    //   With the guard:    can_unload() == false -> h1 is skipped, stays LOADED.
    // evict_blocks() returns {found,...} and never throws, so "nothing freed" is
    // benign.
    env.buffer_pool.add_to_eviction_queue(h1);
    auto eviction = env.buffer_pool.evict_blocks(memory_tag::IN_MEMORY_TABLE,
                                                 /*extra_memory=*/0,
                                                 /*memory_limit=*/0,
                                                 /*buffer=*/nullptr);
    // Bind the result (do not (void)-cast a [[nodiscard]]); its reservation is
    // released by its destructor. Whether eviction "succeeded" differs pre/post
    // guard and is not asserted here -- the next pin() is the discriminator.
    (void) eviction.success;

    // Re-pin h1 -- the discriminator.
    //   Without the guard: h1 was UNLOADED -> load() returns {} (no disk copy) ->
    //                      pin() derefs the null buffer_ -> SIGSEGV (test crashes).
    //   With the guard:    h1 is still LOADED -> pin() returns a valid buffer.
    auto repinned_result = env.buffer_manager.pin(h1);
    REQUIRE_FALSE(repinned_result.has_error());
    buffer_handle_t repinned = std::move(repinned_result.value());
    REQUIRE(repinned.is_valid());
    REQUIRE(h1->state() == block_state::LOADED);
    REQUIRE_FALSE(h1->is_unloaded());

    // The original bytes survived the eviction pass.
    const std::byte* data = repinned.ptr();
    for (uint64_t j = 0; j < block_size; j++) {
        REQUIRE(data[j] == PATTERN[j % sizeof(PATTERN)]);
    }

    // repinned's destructor unpins; h1's shared_ptr releases the block at scope end.
}

// ---------------------------------------------------------------------------
// Companion: GENUINE pool exhaustion is a clean OOM ERROR VALUE, not a throw,
// now that register_transient_memory()/allocate()/pin() return
// core::result_wrapper_t. Every managed (non-reloadable) block is held resident
// by a live pin, so once the pool fills, evict_blocks() can free NOTHING
// (can_unload() == false for all of them) -- the next registration must surface
//   result.has_error() == true && result.error().type == out_of_memory
// and complete normally rather than throwing.
//
// Deterministic exhaustion: maximum_memory = N * block_allocation_size(). Each
// managed register_transient_memory(block_size, block_size) reserves exactly one
// block_allocation_size(); holding its pin keeps it non-evictable. After N such
// allocations used_memory == N * block_allocation_size() == maximum_memory (the
// last one that still fits), so the (N+1)-th over-subscribes the limit with
// nothing to evict and must fail. We make several attempts past N to be safe.
// ---------------------------------------------------------------------------
TEST_CASE("buffer manager: pool exhaustion of pinned managed blocks returns out_of_memory, not throw", "[step1]") {
    using namespace components::table::storage;

    // Provisional limit large enough that the first registrations never trip OOM
    // before we set the real, tight limit (needs block_allocation_size()).
    test_env_t env(uint64_t(1) << 24);
    const uint64_t block_alloc = env.buffer_manager.block_allocation_size();
    const uint64_t block_size = env.buffer_manager.block_size();

    // Pool holds exactly N managed blocks; the (N+1)-th must OOM.
    constexpr uint64_t N = 4;
    env.buffer_pool.maximum_memory = N * block_alloc;

    // Keep block handles AND their pins alive: a live pin (readers > 0) plus
    // can_unload() == false make every block strictly non-evictable, so the pool
    // genuinely cannot reclaim anything.
    std::vector<std::shared_ptr<block_handle_t>> handles;
    std::vector<buffer_handle_t> pins;
    handles.reserve(N);
    pins.reserve(N);

    bool saw_out_of_memory = false;

    // The whole body must complete normally -- no exception may escape. REQUIRE
    // NOTHROW around the exhaustion loop makes the "no throw" half explicit.
    REQUIRE_NOTHROW([&] {
        // Fill the pool: the first N allocations fit exactly (used_memory grows
        // to N * block_alloc == maximum_memory).
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

        // The pool is now full of pinned, non-reloadable blocks. Every further
        // registration over-subscribes the limit, and evict_blocks() can free
        // nothing -> a clean out_of_memory ERROR VALUE. Try a few past the limit.
        for (uint64_t i = 0; i < 3; i++) {
            auto over_result = env.buffer_manager.register_transient_memory(block_size, block_size);
            REQUIRE(over_result.has_error());
            REQUIRE(over_result.error().type == core::error_code_t::out_of_memory);
            saw_out_of_memory = true;
        }
    }());

    // The OOM was observed as a returned error value, and control reached here
    // normally -- no exception was thrown.
    REQUIRE(saw_out_of_memory);

    // pins/handles destructors release the blocks at scope end.
}
