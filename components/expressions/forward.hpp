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
        constant,
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

    std::string to_string(compare_type type);

    std::string to_string(scalar_type type);

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
