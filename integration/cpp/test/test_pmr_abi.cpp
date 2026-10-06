#include <catch2/catch_test_macros.hpp>

#include <core/pmr.hpp>

#include <cstddef>

std::size_t otterbrix_resource_size_seen_by_sanitizer_host();
std::size_t arena_resource_size_seen_by_sanitizer_host();

// A host builds against a prebuilt library: no layout it can see may depend on which sanitizer
// macros its own translation unit was compiled with.
TEST_CASE("integration::cpp::pmr_abi::otterbrix_resource_layout_ignores_sanitizer_macros") {
    CHECK(otterbrix_resource_size_seen_by_sanitizer_host() == sizeof(core::pmr::otterbrix_resource));
}

TEST_CASE("integration::cpp::pmr_abi::arena_resource_layout_ignores_sanitizer_macros") {
    CHECK(arena_resource_size_seen_by_sanitizer_host() == sizeof(core::pmr::arena_resource_t));
}
