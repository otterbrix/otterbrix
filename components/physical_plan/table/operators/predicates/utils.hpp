#pragma once
#include <components/expressions/function_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::table::operators::predicates::impl {

    // Because expression parameters could be nested, including expr containing an expression
    // And multiple getters to get a function call
    // Resulting template tree becomes infinite, so we are forced to use types::logical_value_t
    // TODO: custom impl might be faster then std::function
    using value_getter =
        std::function<types::logical_value_t(const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t)>;

    value_getter create_value_getter(const expressions::key_t& key);

    value_getter create_value_getter(core::parameter_id_t id, const logical_plan::storage_parameters* parameters);

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::function_expression_ptr& expr,
                                     const logical_plan::storage_parameters* parameters);

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::param_storage& var,
                                     const logical_plan::storage_parameters* parameters);

} // namespace components::table::operators::predicates::impl