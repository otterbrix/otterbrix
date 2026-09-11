#include <catch2/catch_test_macros.hpp>

#include <core/counting_resource.hpp>

#include <array>
#include <cstddef>
#include <memory_resource>
#include <string>

using core::pmr::counting_resource_t;
using core::pmr::default_resource_counter_t;
using core::pmr::default_resource_window_t;

namespace {
    constexpr std::size_t kAlign = alignof(std::max_align_t);

    // Restores whatever was the default before, so a failing CHECK cannot leave the process
    // default pointing at a dead stack object.
    struct default_guard_t final {
        std::pmr::memory_resource* previous{std::pmr::get_default_resource()};
        ~default_guard_t() { std::pmr::set_default_resource(previous); }
    };
} // namespace

TEST_CASE("core::pmr::counting_resource::a fresh resource has counted nothing") {
    counting_resource_t probe;
    CHECK(probe.allocations() == 0);
    CHECK(probe.deallocations() == 0);
    CHECK(probe.allocated_bytes() == 0);
    CHECK(probe.outstanding() == 0);
}

TEST_CASE("core::pmr::counting_resource::allocated_bytes accumulates, outstanding does not") {
    counting_resource_t probe;
    void* p = probe.allocate(64, kAlign);
    CHECK(probe.allocations() == 1);
    CHECK(probe.allocated_bytes() == 64);
    CHECK(probe.outstanding() == 1);

    probe.deallocate(p, 64, kAlign);
    CHECK(probe.deallocations() == 1);
    CHECK(probe.outstanding() == 0);
    // The point of the two names: bytes is a running total of what was asked for, not what is live.
    CHECK(probe.allocated_bytes() == 64);
}

TEST_CASE("core::pmr::counting_resource::reset zeroes the counters and leaves the upstream alone") {
    std::array<std::byte, 4096> storage{};
    std::pmr::monotonic_buffer_resource arena{storage.data(), storage.size(), std::pmr::null_memory_resource()};
    counting_resource_t probe{&arena};

    void* p = probe.allocate(32, kAlign);
    probe.deallocate(p, 32, kAlign);
    probe.reset();

    CHECK(probe.allocations() == 0);
    CHECK(probe.deallocations() == 0);
    CHECK(probe.allocated_bytes() == 0);
    CHECK(probe.upstream_resource() == &arena);
}

TEST_CASE("core::pmr::counting_resource::an uncaptured resource answers new_delete_resource") {
    counting_resource_t probe;
    CHECK(probe.upstream_resource() == std::pmr::new_delete_resource());

    void* p = probe.allocate(16, kAlign);
    CHECK(probe.upstream_resource() == std::pmr::new_delete_resource());
    probe.deallocate(p, 16, kAlign);
}

TEST_CASE("core::pmr::counting_resource::a named upstream really serves the block") {
    std::array<std::byte, 4096> storage{};
    std::pmr::monotonic_buffer_resource arena{storage.data(), storage.size(), std::pmr::null_memory_resource()};
    counting_resource_t probe{&arena};

    auto* p = static_cast<std::byte*>(probe.allocate(64, kAlign));
    REQUIRE(p != nullptr);
    // Inside the stack array, so it came from `arena` -- null_memory_resource above it would have
    // thrown had the monotonic buffer needed to grow, and new_delete would land elsewhere entirely.
    CHECK(p >= storage.data());
    CHECK(p < storage.data() + storage.size());
    probe.deallocate(p, 64, kAlign);
}

// The three cases below are why arm() is a latch and not an assignment.

TEST_CASE("core::pmr::counting_resource::arm captures exactly once") {
    default_guard_t guard;
    counting_resource_t a;
    counting_resource_t b;
    counting_resource_t probe;

    std::pmr::set_default_resource(&a);
    probe.arm();
    REQUIRE(probe.upstream_resource() == &a);

    std::pmr::set_default_resource(&b);
    probe.arm();
    // Still `a`: a block served through `a` must be given back through `a`, whatever the default
    // has become since.
    CHECK(probe.upstream_resource() == &a);
}

TEST_CASE("core::pmr::counting_resource::arm after an allocation does not move the upstream") {
    default_guard_t guard;
    counting_resource_t named;
    counting_resource_t probe;

    void* p = probe.allocate(48, kAlign); // latches new_delete_resource
    std::pmr::set_default_resource(&named);
    probe.arm();

    CHECK(probe.upstream_resource() == std::pmr::new_delete_resource());

    probe.deallocate(p, 48, kAlign);
    // -1, not 0: arm() zeroed the counters while that block was still outstanding, so its
    // release is counted without the matching allocation. This is why outstanding() is signed --
    // unsigned it would read 18446744073709551615 and look like corruption.
    CHECK(probe.outstanding() == -1);
}

TEST_CASE("core::pmr::counting_resource::arm never captures itself") {
    default_guard_t guard;
    counting_resource_t probe;

    std::pmr::set_default_resource(&probe);
    probe.arm();
    // Capturing itself would make do_allocate recurse forever.
    CHECK(probe.upstream_resource() != &probe);

    void* p = probe.allocate(24, kAlign);
    CHECK(probe.allocations() == 1);
    probe.deallocate(p, 24, kAlign);
}

TEST_CASE("core::pmr::counting_resource::is_equal is identity, not a shared upstream") {
    counting_resource_t a{std::pmr::new_delete_resource()};
    counting_resource_t b{std::pmr::new_delete_resource()};

    CHECK(a.is_equal(a));
    CHECK_FALSE(a.is_equal(b));
}

TEST_CASE("core::pmr::default_resource_window restores the PREVIOUS default, not new_delete") {
    default_guard_t guard;
    counting_resource_t a;
    counting_resource_t b;

    std::pmr::set_default_resource(&a);
    {
        default_resource_window_t window{&b};
        CHECK(std::pmr::get_default_resource() == &b);
    }
    CHECK(std::pmr::get_default_resource() == &a);
}

TEST_CASE("core::pmr::default_resource_counter counts what the scope took from the default") {
    default_guard_t guard;
    counting_resource_t probe;
    {
        default_resource_counter_t counter{probe};
        std::pmr::string s{"a string far too long to live inside the string object itself"};
        CHECK(counter.allocations() >= 1);
        CHECK(counter.allocated_bytes() >= s.size());
    }
    CHECK(std::pmr::get_default_resource() != &probe);
}
