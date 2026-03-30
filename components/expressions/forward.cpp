#include "forward.hpp"

namespace components::expressions {

    std::string to_string(compare_type type) {
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
                return "union_and";
            case compare_type::union_or:
                return "union_or";
            case compare_type::union_not:
                return "union_not";
            case compare_type::all_true:
                return "all_true";
            case compare_type::all_false:
                return "all_false";
            case compare_type::is_null:
                return "is_null";
            case compare_type::is_not_null:
                return "is_not_null";
            default:
                return "invalid";
        }
    }

    std::string to_string(scalar_type type) {
        switch (type) {
            case scalar_type::get_field:
                return "get_field";
            case scalar_type::constant:
                return "constant";
            case scalar_type::group_field:
                return "group_field";
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
            case scalar_type::case_expr:
                return "case_expr";
            case scalar_type::coalesce:
                return "coalesce";
            case scalar_type::case_when:
                return "case_when";
            case scalar_type::unary_minus:
                return "unary_minus";
            default:
                return "invalid";
        }
    }

} // namespace components::expressions