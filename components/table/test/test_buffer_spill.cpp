// The buffer pool cannot free anything a bulk load allocates.
//
// block_handle_t::can_unload() refuses any block without a disk copy, and every transient column
// segment is exactly that: created through register_transient_memory with an id past MAXIMUM_BLOCK,
// never enqueued by unpin(), and there is no path anywhere that writes such a buffer out. So when
// the pool fills, eviction walks an empty queue and reports out_of_memory rather than making room.
//
// Measured consequence: with the WAL off — hence no checkpoints, hence nothing reaching the .otbx —
// a load fails at 24 263 880 rows while holding 9 MiB on disk. The ceiling is the hardwired 4 GiB
// pool limit, and no amount of disk space helps.
//
// This test asks for exactly that behaviour in miniature: a pool sized for a handful of buffers,
// oversubscribed. It must be possible to allocate past the limit, and every earlier buffer must read
// back byte-for-byte afterwards.

#include <catch2/catch_test_macros.hpp>

#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/file_buffer.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <cstring>
#include <vector>

namespace {
    using namespace components::table::storage;

    struct spill_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        buffer_pool_t buffer_pool;
        standard_buffer_manager_t buffer_manager;

        explicit spill_env_t(uint64_t pool_limit)
            : buffer_pool(&resource, pool_limit, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // A recognisable pattern per buffer, so a mixed-up or half-written spill is visible rather than
    // merely "some bytes".
    void fill_pattern(std::byte* data, uint64_t size, uint64_t seed) {
        for (uint64_t i = 0; i < size; ++i) {
            data[i] = static_cast<std::byte>((seed * 131u + i * 7u) & 0xFF);
        }
    }

    bool check_pattern(const std::byte* data, uint64_t size, uint64_t seed) {
        for (uint64_t i = 0; i < size; ++i) {
            if (data[i] != static_cast<std::byte>((seed * 131u + i * 7u) & 0xFF)) {
                return false;
            }
        }
        return true;
    }
} // namespace

TEST_CASE("buffer manager: transient buffers spill to disk instead of exhausting the pool", "[spill]") {
    // Room for four buffers; we will ask for twelve.
    spill_env_t env(uint64_t(1) << 24);
    const uint64_t block_size = env.buffer_manager.block_size();
    const uint64_t tiny_size = block_size / 32; // < block_size => the TINY_BUFFER path

    // Sized off the real allocation so the limit means the same thing whatever the block size is.
    auto probe = env.buffer_manager.register_transient_memory(tiny_size, block_size);
    REQUIRE_FALSE(probe.has_error());
    const uint64_t per_buffer = probe.value()->memory_usage();
    REQUIRE(per_buffer > 0);
    probe.value().reset();
    auto limit_set = env.buffer_pool.set_limit(4 * per_buffer);
    REQUIRE_FALSE(limit_set.has_error());

    constexpr int kBuffers = 12;
    std::vector<std::shared_ptr<block_handle_t>> handles;
    handles.reserve(kBuffers);

    for (int i = 0; i < kBuffers; ++i) {
        auto created = env.buffer_manager.register_transient_memory(tiny_size, block_size);
        INFO("allocating transient buffer " << i << " of " << kBuffers << " into a pool sized for 4");
        REQUIRE_FALSE(created.has_error());
        auto handle = std::move(created.value());
        REQUIRE(handle != nullptr);
        {
            auto pinned = env.buffer_manager.pin(handle);
            REQUIRE_FALSE(pinned.has_error());
            REQUIRE(pinned.value().is_valid());
            fill_pattern(pinned.value().ptr(), tiny_size, static_cast<uint64_t>(i) + 1);
        }
        handles.push_back(std::move(handle));
    }

    // Positive control: the point is that buffers went to disk, not that twelve small allocations
    // happened to fit. Without this a pool that silently grew past its limit would pass.
    INFO("bytes written to the scratch file: " << env.buffer_pool.spilled_bytes());
    REQUIRE(env.buffer_pool.spilled_bytes() > 0);
    CHECK(env.buffer_pool.used_memory() <= 4 * per_buffer);

    // Everything written must still be readable: whatever was pushed out has to come back intact.
    for (int i = 0; i < kBuffers; ++i) {
        auto pinned = env.buffer_manager.pin(handles[static_cast<size_t>(i)]);
        INFO("reading transient buffer " << i << " back");
        REQUIRE_FALSE(pinned.has_error());
        REQUIRE(pinned.value().is_valid());
        CHECK(check_pattern(pinned.value().ptr(), tiny_size, static_cast<uint64_t>(i) + 1));
    }
}
