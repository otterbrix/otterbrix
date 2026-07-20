#pragma once

#include <components/types/types.hpp>

#include <cstddef>

// Cast-specific comparisons, which may group some types into a single 'family' to avoid infinite permutations
namespace components::casts {

    [[nodiscard]] bool same_cast_type(const types::complex_logical_type& a,
                                      const types::complex_logical_type& b) noexcept;

    [[nodiscard]] size_t cast_type_hash(const types::complex_logical_type& type) noexcept;

} // namespace components::casts
