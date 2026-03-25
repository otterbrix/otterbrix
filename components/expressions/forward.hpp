#pragma once

#include <core/strong_typedef.hpp>

STRONG_TYPEDEF(uint16_t, parameter_id_t);

namespace components::expressions {

    using hash_t = std::size_t;

    enum class expression_group : uint8_t
    {
        invalid,
        compare,
        aggregate,
        scalar,
        sort,
        function
    };

    enum class compare_type : uint8_t
    {
        invalid,
        eq,
        ne,
        gt,
        lt,
        gte,
        lte,
        regex,
        any,
        all,
        union_and,
        union_or,
        union_not,
        all_true,
        all_false,
        is_null,
        is_not_null
    };

    enum class scalar_type : uint8_t
    {
        invalid,
        get_field,
        group_field,
        add,
        subtract,
        multiply,
        divide,
        round,
        ceil,
        floor,
        abs,
        mod,
        pow,
        sqrt,
        case_expr,
        coalesce,
        case_when,
        unary_minus
    };

    enum class sort_order : std::int8_t
    {
        desc = -1,
        asc = 1
    };

    enum class side_t : uint8_t
    {
        undefined = 0,
        left,
        right
    };

    inline std::string to_string(compare_type type) {
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

    inline std::string to_string(scalar_type type) {
        switch (type) {
            case scalar_type::get_field:
                return "get_field";
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

    template<class OStream>
    OStream& operator<<(OStream& stream, const compare_type& type) {
        if (type == compare_type::union_and) {
            stream << "$and";
        } else if (type == compare_type::union_or) {
            stream << "$or";
        } else if (type == compare_type::union_not) {
            stream << "$not";
        } else {
            stream << "$" << to_string(type);
        }
        return stream;
    }

} // namespace components::expressions
