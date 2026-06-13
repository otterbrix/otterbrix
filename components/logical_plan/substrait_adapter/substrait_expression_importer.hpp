#pragma once

#include "from_substrait.hpp"
#include "substrait_adapter_types.hpp"
#include "substrait_function_mapping.hpp"

#include <components/expressions/expression.hpp>
#include <components/types/logical_value.hpp>

#include <string>
#include <unordered_map>

#include "substrait/algebra.pb.h"

namespace components::logical_plan::substrait_adapter {

    struct from_substrait_context_t {
        std::unordered_map<uint32_t, std::string> functions;
        parameter_node_ptr params;
        import_profile_t profile = import_profile_t::internal_roundtrip;

        explicit from_substrait_context_t(std::pmr::memory_resource* resource,
                                          import_profile_t profile_ = import_profile_t::internal_roundtrip);
        bool is_external_canonical() const;
    };

    std::string get_function_name(const from_substrait_context_t& ctx, uint32_t ref);
    components::types::logical_value_t logical_value_from_literal(std::pmr::memory_resource* resource,
                                                                  const substrait::Expression_Literal& literal);
    expressions::param_storage param_from_expression(std::pmr::memory_resource* resource,
                                                     const substrait::Expression& expr,
                                                     const field_mapping_t& mapping,
                                                     from_substrait_context_t& ctx);
    expressions::expression_ptr expression_with_alias(std::pmr::memory_resource* resource,
                                                      const expressions::expression_ptr& expr,
                                                      const std::string& alias);
    expressions::expression_ptr expression_from_substrait(std::pmr::memory_resource* resource,
                                                          const substrait::Expression& expr,
                                                          const field_mapping_t& mapping,
                                                          from_substrait_context_t& ctx);

} // namespace components::logical_plan::substrait_adapter
