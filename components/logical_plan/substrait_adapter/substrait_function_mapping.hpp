#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <string>

namespace components::logical_plan::substrait_adapter {

    std::string compare_type_to_function(expressions::compare_type type);
    expressions::compare_type invert_compare_type(expressions::compare_type type);
    std::string scalar_type_to_function(expressions::scalar_type type);
    std::string normalize_aggregate_function_name(const std::string& function_name);
    bool is_known_aggregate_function(const std::string& function_name);

} // namespace components::logical_plan::substrait_adapter
