#pragma once

#include <components/types/types.hpp>

#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>

namespace components::types {
    uint32_t type_binary_size(const complex_logical_type& type);

    char* type_binary_write(char* output, const complex_logical_type& type);

    core::result_wrapper_t<complex_logical_type>
    type_binary_read(const char*& scan, const char* end, std::pmr::memory_resource* resource);
} // namespace components::types
