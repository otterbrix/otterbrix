// Compiled with the macros a sanitizer build defines (see CMakeLists.txt), which the library itself
// was not built with: this is how a host translation unit built under ASAN or TSAN sees these types.
#include <core/pmr.hpp>

#include <cstddef>

std::size_t otterbrix_resource_size_seen_by_sanitizer_host() { return sizeof(core::pmr::otterbrix_resource); }
std::size_t arena_resource_size_seen_by_sanitizer_host() { return sizeof(core::pmr::arena_resource_t); }
