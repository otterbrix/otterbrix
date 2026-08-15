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
            case scalar_type::mod:
                return "mod";
            case scalar_type::case_expr:
                return "case_expr";
            case scalar_type::coalesce:
                return "coalesce";
            case scalar_type::case_when:
                return "case_when";
            case scalar_type::unary_minus:
                return "unary_minus";
            case scalar_type::bit_and:
                return "bit_and";
            case scalar_type::bit_or:
                return "bit_or";
            case scalar_type::bit_xor:
                return "bit_xor";
            case scalar_type::bit_not:
                return "bit_not";
            case scalar_type::shift_left:
                return "shift_left";
            case scalar_type::shift_right:
                return "shift_right";
            case scalar_type::star_expand:
                return "star_expand";
            case scalar_type::jsonb_expand:
                return "jsonb_expand";
            case scalar_type::jsonb_delete:
                return "jsonb_delete";
            default:
                return "invalid";
        }
    }

    operators::operator_code to_operator_code(scalar_type type) noexcept {
        switch (type) {
            case scalar_type::add:
                return operators::operator_code::add;
            case scalar_type::subtract:
                return operators::operator_code::subtract;
            case scalar_type::multiply:
                return operators::operator_code::multiply;
            case scalar_type::divide:
                return operators::operator_code::divide;
            case scalar_type::mod:
                return operators::operator_code::mod;
            case scalar_type::unary_minus:
                return operators::operator_code::negate;
            case scalar_type::bit_and:
                return operators::operator_code::bit_and;
            case scalar_type::bit_or:
                return operators::operator_code::bit_or;
            case scalar_type::bit_xor:
                return operators::operator_code::bit_xor;
            case scalar_type::bit_not:
                return operators::operator_code::bit_not;
            case scalar_type::shift_left:
                return operators::operator_code::shift_left;
            case scalar_type::shift_right:
                return operators::operator_code::shift_right;
            default:
                return operators::operator_code::invalid;
        }
    }

    operators::operator_code to_operator_code(compare_type type) noexcept {
        switch (type) {
            case compare_type::eq:
                return operators::operator_code::equal;
            case compare_type::ne:
                return operators::operator_code::not_equal;
            case compare_type::gt:
                return operators::operator_code::greater;
            case compare_type::gte:
                return operators::operator_code::greater_equal;
            case compare_type::lt:
                return operators::operator_code::less;
            case compare_type::lte:
                return operators::operator_code::less_equal;
            case compare_type::union_and:
                return operators::operator_code::logical_and;
            case compare_type::union_or:
                return operators::operator_code::logical_or;
            case compare_type::union_not:
                return operators::operator_code::logical_not;
            case compare_type::is_null:
                return operators::operator_code::is_null;
            case compare_type::is_not_null:
                return operators::operator_code::is_not_null;
            default:
                // regex / any / all / all_true / all_false are not operators
                return operators::operator_code::invalid;
        }
    }

} // namespace components::expressions