#pragma once

#include <core/arithmetic_op.hpp>
#include <core/strong_typedef.hpp>

#include <optional>

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
        unary_minus,
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

    // `default` -> placement was not specified and must be resolved from the
    // sort direction per the SQL standard: ASC -> nulls last, DESC -> nulls first.
    enum class sort_null_order : std::int8_t
    {
        nulls_default = 0,
        nulls_first = 1,
        nulls_last = 2
    };

    enum class side_t : uint8_t
    {
        undefined = 0,
        left,
        right
    };

    // THE scalar_type -> vector::arithmetic_op mapping. nullopt for a scalar_type that is
    // not one of the five binary arithmetic operators. Lives beside the enum so a sixth
    // operator is added in ONE switch — the binder, the validator and the group operator
    // each used to keep a private copy that had to agree by hand.
    constexpr std::optional<vector::arithmetic_op> to_arithmetic_op(scalar_type type) noexcept {
        switch (type) {
            case scalar_type::add:
                return vector::arithmetic_op::add;
            case scalar_type::subtract:
                return vector::arithmetic_op::subtract;
            case scalar_type::multiply:
                return vector::arithmetic_op::multiply;
            case scalar_type::divide:
                return vector::arithmetic_op::divide;
            case scalar_type::mod:
                return vector::arithmetic_op::mod;
            default:
                return std::nullopt;
        }
    }

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
