#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <memory_resource>

#include <core/counting_resource.hpp>
#include <core/resource_tracer.hpp>

// Leaks belong to LeakSanitizer, which names the stack that allocated each block: the tracer
// frees nothing it still holds. The test hands both blocks back itself, or LSan flags it.
TEST_CASE("resource_tracer leaves still-live blocks to the leak checker") {
    core::pmr::counting_resource_t upstream;
    void* a = nullptr;
    void* b = nullptr;
    {
        resource_tracer_t tracer(&upstream);
        a = tracer.allocate(64, alignof(std::max_align_t));
        b = tracer.allocate(128, alignof(std::max_align_t));
        REQUIRE(a != nullptr);
        REQUIRE(b != nullptr);
        REQUIRE(upstream.outstanding() == 2);
    }
    REQUIRE(upstream.outstanding() == 2);
    upstream.deallocate(a, 64, alignof(std::max_align_t));
    upstream.deallocate(b, 128, alignof(std::max_align_t));
    CHECK(upstream.outstanding() == 0);
}

TEST_CASE("resource_tracer forwards a matched deallocation to upstream") {
    core::pmr::counting_resource_t upstream;
    {
        resource_tracer_t tracer(&upstream);
        void* p = tracer.allocate(32, alignof(std::max_align_t));
        REQUIRE(upstream.outstanding() == 1);
        tracer.deallocate(p, 32, alignof(std::max_align_t));
        CHECK(upstream.outstanding() == 0);
    }
    CHECK(upstream.outstanding() == 0);
}
