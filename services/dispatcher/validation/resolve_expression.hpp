#pragma once

#include "schema.hpp"

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_signature.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::dispatcher::validation {

    // used to address expressions from previous steps, without recomputing them
    struct precomputed_column_t {
        components::expressions::key_t reference;
        components::expressions::expression_ptr expression;
        components::expressions::cardinality_t cardinality{components::expressions::cardinality_t::row};

        explicit precomputed_column_t(std::pmr::memory_resource* resource)
            : reference(resource) {}
    };

    struct expression_context_t {
        std::pmr::memory_resource* resource;
        // Node's The input schema
        const named_schema& schema;
        const components::logical_plan::storage_parameters& parameters;
        const components::casts::cast_registry_t& cast_registry;
        const components::compute::function_registry_t& function_registry;
        const components::graph_execution_context& execution_context;
        components::compute::function_types_mask allowed_functions;
        const named_schema* schema_right{nullptr};
        const std::pmr::vector<precomputed_column_t>* precomputed{nullptr};
        components::types::complex_logical_type required_type{components::types::logical_type::ANY};
    };

    [[nodiscard]] core::error_t resolve_expression(components::expressions::expression_ptr& expression,
                                                   const expression_context_t& context,
                                                   bool* saw_reduction = nullptr);

} // namespace services::dispatcher::validation
