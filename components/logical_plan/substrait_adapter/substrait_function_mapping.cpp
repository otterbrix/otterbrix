#include "substrait_function_mapping.hpp"

namespace components::logical_plan::substrait_adapter {

    std::string compare_type_to_function(expressions::compare_type type) {
        using expressions::compare_type;
        switch (type) {
            case compare_type::eq:
                return "eq";
            case compare_type::ne:
                return "ne";
            case compare_type::gt:
                return "gt";
            case compare_type::lt:
                return "lt";
            case compare_type::gte:
                return "gte";
            case compare_type::lte:
                return "lte";
            case compare_type::regex:
                return "regex";
            case compare_type::any:
                return "any";
            case compare_type::all:
                return "all";
            case compare_type::union_and:
                return "and";
            case compare_type::union_or:
                return "or";
            case compare_type::union_not:
                return "not";
            default:
                return "";
        }
    }

    expressions::compare_type invert_compare_type(expressions::compare_type type) {
        using expressions::compare_type;
        switch (type) {
            case compare_type::gt:
                return compare_type::lt;
            case compare_type::lt:
                return compare_type::gt;
            case compare_type::gte:
                return compare_type::lte;
            case compare_type::lte:
                return compare_type::gte;
            default:
                return type;
        }
    }

    std::string scalar_type_to_function(expressions::scalar_type type) {
        using expressions::scalar_type;
        switch (type) {
            case scalar_type::add:
                return "add";
            case scalar_type::subtract:
                return "subtract";
            case scalar_type::multiply:
                return "multiply";
            case scalar_type::divide:
                return "divide";
            case scalar_type::round:
                return "round";
            case scalar_type::ceil:
                return "ceil";
            case scalar_type::floor:
                return "floor";
            case scalar_type::abs:
                return "abs";
            case scalar_type::mod:
                return "mod";
            case scalar_type::pow:
                return "pow";
            case scalar_type::sqrt:
                return "sqrt";
            default:
                return "";
        }
    }

    std::string normalize_aggregate_function_name(const std::string& function_name) {
        if (function_name == "count" || function_name == "sum" || function_name == "min" || function_name == "max" ||
            function_name == "avg") {
            return function_name;
        }
        return function_name;
    }

    bool is_known_aggregate_function(const std::string& function_name) {
        return function_name == "count" || function_name == "sum" || function_name == "min" ||
               function_name == "max" || function_name == "avg";
    }

} // namespace components::logical_plan::substrait_adapter
