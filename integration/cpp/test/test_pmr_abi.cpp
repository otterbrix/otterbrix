#include <catch2/catch_test_macros.hpp>

#include <core/pmr.hpp>
#include <integration/cpp/base_spaces.hpp>
#include <integration/python/module_arena.hpp>

#include <cstddef>

std::size_t otterbrix_resource_size_seen_by_sanitizer_host();
std::size_t base_otterbrix_size_seen_by_sanitizer_host();
std::size_t module_arena_size_seen_by_sanitizer_host();

// A host builds against a prebuilt library: no layout it can see may depend on which sanitizer
// macros its own translation unit was compiled with.
TEST_CASE("integration::cpp::pmr_abi::otterbrix_resource_layout_ignores_sanitizer_macros") {
    CHECK(otterbrix_resource_size_seen_by_sanitizer_host() == sizeof(core::pmr::otterbrix_resource));
}

TEST_CASE("integration::cpp::pmr_abi::base_otterbrix_layout_ignores_sanitizer_macros") {
    CHECK(base_otterbrix_size_seen_by_sanitizer_host() == sizeof(otterbrix::base_otterbrix_t));
}

TEST_CASE("integration::cpp::pmr_abi::module_arena_layout_ignores_sanitizer_macros") {
    CHECK(module_arena_size_seen_by_sanitizer_host() == sizeof(otterbrix::module_arena_t));
}
