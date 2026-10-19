#include "param_storage.hpp"

#include <core/pmr.hpp>

namespace components::logical_plan {

    parameter_node_ptr make_parameter_node(std::pmr::memory_resource* resource) {
        return {new parameter_node_t(resource)};
    }
} // namespace components::logical_plan
