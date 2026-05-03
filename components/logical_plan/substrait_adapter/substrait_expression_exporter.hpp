#pragma once

#include "substrait_adapter_types.hpp"
#include "substrait_function_mapping.hpp"
#include "to_substrait.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>

#include "components/logical_plan/substrait_adapter/substrait/algebra.pb.h"
#include "components/logical_plan/substrait_adapter/substrait/plan.pb.h"

#include <unordered_map>

namespace components::logical_plan::substrait_adapter {

    struct field_context_t {
        field_mapping_t* mapping = nullptr;
        const field_mapping_t* left = nullptr;
        const field_mapping_t* right = nullptr;
        int32_t left_size = 0;
    };

    int32_t resolve_field_index(const expressions::key_t& key, field_context_t& ctx);
    substrait::Type resolve_field_type(const expressions::key_t& key, field_context_t& ctx);
    void set_field_ref(substrait::Expression* expr, int32_t field_idx);

    struct function_registry_t {
        substrait::Plan* plan = nullptr;
        std::unordered_map<std::string, uint32_t> function_ids;
        uint32_t next_function_id = 1;
        uint32_t urn_id = 0;

        explicit function_registry_t(substrait::Plan* plan_);
        uint32_t register_function(const std::string& name);
    };

    struct to_substrait_context_t {
        function_registry_t registry;
        export_profile_t profile = export_profile_t::internal_roundtrip;
        const components::catalog::catalog* catalog = nullptr;

        explicit to_substrait_context_t(substrait::Plan* plan,
                                        export_profile_t profile_,
                                        const components::catalog::catalog* catalog_ = nullptr);
        bool is_external_canonical() const;
    };

    substrait::Type infer_param_type(const expressions::param_storage& param, field_context_t& fields);
    substrait::Type infer_expression_type(const expressions::expression_ptr& expr, field_context_t& fields);
    substrait::Expression* to_substrait_param(const expressions::param_storage& param,
                                              field_context_t& fields,
                                              to_substrait_context_t& ctx);
    substrait::Expression* to_substrait_expression(const expressions::expression_ptr& expr,
                                                   field_context_t& fields,
                                                   to_substrait_context_t& ctx);
    std::string expr_output_name(const expressions::expression_ptr& expr);
    std::string default_expr_output_name(size_t index);
    field_mapping_t project_output_mapping(const std::pmr::vector<expressions::expression_ptr>& expressions,
                                           field_context_t& fields);
    field_mapping_t aggregate_output_mapping(const std::pmr::vector<expressions::expression_ptr>& expressions,
                                             field_context_t& fields);

} // namespace components::logical_plan::substrait_adapter
