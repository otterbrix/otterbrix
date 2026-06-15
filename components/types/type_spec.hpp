#pragma once

#include "types.hpp"

#include <memory_resource>
#include <string>
#include <string_view>

namespace components::types {

    std::string encode_type_spec(const complex_logical_type& type);
    complex_logical_type decode_type_spec(std::pmr::memory_resource* resource, std::string_view spec);

} // namespace components::types
