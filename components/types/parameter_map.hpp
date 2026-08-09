#pragma once

#include <components/types/logical_value.hpp>
#include <core/parameter_id.hpp>

#include <memory_resource>
#include <unordered_map>

namespace components::types {

    using parameter_map_t = std::pmr::unordered_map<core::parameter_id_t, logical_value_t>;

} // namespace components::types