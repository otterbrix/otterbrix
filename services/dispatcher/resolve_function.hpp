#pragma once

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <components/types/types.hpp>

#include <string_view>

namespace services::dispatcher {

    struct resolved_argument_t {
        //! Empty when the argument already fits
        components::casts::cast_t cast;
        components::types::complex_logical_type target;
    };

    struct resolved_function_t {
        components::compute::function_uid uid;
        std::pmr::vector<resolved_argument_t> arguments;
        components::types::complex_logical_type result;
        components::compute::function_type_t function_type{components::compute::function_type_t::invalid};
        bool mergeable{false};

        explicit resolved_function_t(std::pmr::memory_resource* resource)
            : arguments(resource) {}
    };

    [[nodiscard]] core::result_wrapper_t<resolved_function_t>
    resolve_function(std::pmr::memory_resource* resource,
                     const components::casts::cast_registry_t& cast_registry,
                     const components::graph_execution_context& graph_execution_context,
                     const components::compute::function_registry_t& function_registry,
                     std::string_view name,
                     const std::pmr::vector<components::types::complex_logical_type>& arguments,
                     components::compute::function_types_mask allowed_function_types);

} // namespace services::dispatcher