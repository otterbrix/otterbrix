// Compiled with the macros a sanitizer build defines (see CMakeLists.txt), which the library itself
// was not built with: this is how a host translation unit built under ASAN or TSAN sees these types.
#include <core/pmr.hpp>
#include <integration/cpp/base_spaces.hpp>
#include <integration/python/module_arena.hpp>

#include <cstddef>

std::size_t otterbrix_resource_size_seen_by_sanitizer_host() { return sizeof(core::pmr::otterbrix_resource); }
std::size_t base_otterbrix_size_seen_by_sanitizer_host() { return sizeof(otterbrix::base_otterbrix_t); }
std::size_t module_arena_size_seen_by_sanitizer_host() { return sizeof(otterbrix::module_arena_t); }
