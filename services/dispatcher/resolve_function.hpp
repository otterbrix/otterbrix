#pragma once

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>

#include <span>
#include <string_view>

namespace services::dispatcher {
    struct resolved_argument_t {
        //! Empty when the argument already fits
        components::casts::cast_t cast;
        components::types::complex_logical_type target;
    };

    struct resolved_function_t {
        components::compute::function_uid uid;
        // Index into the function's get_signatures().
        size_t signature{0};
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
                     const qualified_name_t& name,
                     const std::pmr::vector<components::types::complex_logical_type>& arguments,
                     components::compute::function_types_mask allowed_function_types,
                     std::span<const components::compute::function_pin_t> pins);
} // namespace services::dispatcher