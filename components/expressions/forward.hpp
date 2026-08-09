#pragma once

#include <components/operators/operator_code.hpp>
#include <core/parameter_id.hpp>

namespace components::expressions {

    using hash_t = std::size_t;

    enum class expression_group : uint8_t
    {
        invalid,
        compare,
        aggregate,
        scalar,
        sort,
        function,
        cast
    };

    // How many values an expression yields per group of input rows
    enum class cardinality_t : uint8_t
    {
        unknown,
        constant,
        row,
        group
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
        mod,
        case_expr,
        coalesce,
        case_when,
        unary_minus,
        bit_and,
        bit_or,
        bit_xor,
        bit_not,
        shift_left,
        shift_right,
        star_expand,
        // JSONB table-valued operators on computing tables. Both carry a path
        // prefix in the expression key; validate_logical_plan expands them into
        // get_field columns against the resolved schema:
        //   jsonb_expand — '->' / '#>' : columns under the prefix, rerooted
        //   jsonb_delete — '-'  / '#-' : all columns EXCEPT those under the prefix
        jsonb_expand,
        jsonb_delete
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

    operators::operator_code to_operator_code(scalar_type type) noexcept;
    operators::operator_code to_operator_code(compare_type type) noexcept;

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
