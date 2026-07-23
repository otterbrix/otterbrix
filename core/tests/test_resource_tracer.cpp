#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <memory_resource>

#include <core/resource_tracer.hpp>

namespace {

    // Upstream memory_resource that counts outstanding (allocated-but-not-freed)
    // blocks, so a test can observe whether resource_tracer_t reclaims its live
    // allocations on destruction. Backed by new_delete_resource.
    struct counting_resource_t final : std::pmr::memory_resource {
        std::size_t outstanding{0};

        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            ++outstanding;
            return std::pmr::new_delete_resource()->allocate(bytes, alignment);
        }

        void do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) override {
            --outstanding;
            std::pmr::new_delete_resource()->deallocate(ptr, bytes, alignment);
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    };

} // namespace

// Failing-first: before the fix the tracer destructor only PRINTS the leak
// report and frees nothing, so `outstanding` stays 2 and this CHECK fails.
// After the fix (destructor calls upstream_->deallocate for every live block +
// live_.clear()) it drops to 0, mirroring synchronized_pool_resource::release().
TEST_CASE("resource_tracer reclaims still-live blocks on destruction") {
    counting_resource_t upstream;
    {
        resource_tracer_t tracer(&upstream);
        void* a = tracer.allocate(64, alignof(std::max_align_t));
        void* b = tracer.allocate(128, alignof(std::max_align_t));
        REQUIRE(a != nullptr);
        REQUIRE(b != nullptr);
        REQUIRE(upstream.outstanding == 2);
        // Intentionally NOT deallocated: simulates the engine's under-torn-down
        // actor graph. The tracer destructor must reclaim these on scope exit.
    }
    CHECK(upstream.outstanding == 0);
}

// Guard: the normal allocate+deallocate path is unchanged and the fix does not
// double-free (the block is already gone from live_ before the destructor runs).
TEST_CASE("resource_tracer forwards a matched deallocation to upstream") {
    counting_resource_t upstream;
    {
        resource_tracer_t tracer(&upstream);
        void* p = tracer.allocate(32, alignof(std::max_align_t));
        REQUIRE(upstream.outstanding == 1);
        tracer.deallocate(p, 32, alignof(std::max_align_t));
        CHECK(upstream.outstanding == 0);
    }
    CHECK(upstream.outstanding == 0);
}
