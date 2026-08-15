#include "resolve_operator.hpp"

namespace components::operators {

    namespace {

        using types::complex_logical_type;
        using types::logical_type;

        bool is_temporal(logical_type type) noexcept {
            return type == logical_type::DATE || type == logical_type::TIME || type == logical_type::TIME_TZ ||
                   type == logical_type::TIMESTAMP || type == logical_type::TIMESTAMP_TZ;
        }

        complex_logical_type boolean() noexcept { return complex_logical_type{logical_type::BOOLEAN}; }

        bool is_comparable(const complex_logical_type& type) noexcept {
            if (type.type() == logical_type::LIST || type.type() == logical_type::ARRAY) {
                return is_comparable(type.child_type());
            }
            return types::is_numeric(type.type()) || type.type() == logical_type::DECIMAL || is_temporal(type.type()) ||
                   type.type() == logical_type::INTERVAL || types::is_string(type.type()) ||
                   type.type() == logical_type::ENUM;
        }

        bool same_decimal(const complex_logical_type& lhs, const complex_logical_type& rhs) noexcept {
            if (lhs.type() != logical_type::DECIMAL || rhs.type() != logical_type::DECIMAL) {
                return false;
            }
            const auto* left = lhs.extension_as<types::decimal_logical_type_extension>();
            const auto* right = rhs.extension_as<types::decimal_logical_type_extension>();
            return left->width() == right->width() && left->scale() == right->scale();
        }

    } // namespace

    std::optional<resolved_operator_t> resolve_operator(operator_code code, const complex_logical_type& operand) {
        // Every unary operator over NULL is NULL
        if (operand.type() == logical_type::NA && code != operator_code::is_null &&
            code != operator_code::is_not_null) {
            return resolved_operator_t{operand};
        }
        switch (code) {
            case operator_code::negate:
                if (types::is_numeric(operand.type()) || operand.type() == logical_type::DECIMAL ||
                    operand.type() == logical_type::INTERVAL) {
                    return resolved_operator_t{operand};
                }
                return std::nullopt;
            case operator_code::bit_not:
                if (types::is_integer(operand.type())) {
                    return resolved_operator_t{operand};
                }
                return std::nullopt;
            case operator_code::logical_not:
                if (operand.type() == logical_type::BOOLEAN) {
                    return resolved_operator_t{boolean()};
                }
                return std::nullopt;
            case operator_code::is_null:
            case operator_code::is_not_null:
                if (operand.type() == logical_type::INVALID) {
                    return std::nullopt;
                }
                return resolved_operator_t{boolean()};
            default:
                return std::nullopt;
        }
    }

    std::optional<resolved_operator_t>
    resolve_operator(operator_code code, const complex_logical_type& lhs, const complex_logical_type& rhs) {
        switch (code) {
            case operator_code::bit_and:
            case operator_code::bit_or:
            case operator_code::bit_xor:
            case operator_code::shift_left:
            case operator_code::shift_right:
                if (types::is_integer(lhs.type()) && lhs.type() == rhs.type()) {
                    return resolved_operator_t{lhs};
                }
                return std::nullopt;
            case operator_code::equal:
            case operator_code::not_equal:
            case operator_code::less:
            case operator_code::less_equal:
            case operator_code::greater:
            case operator_code::greater_equal:
                if (lhs == rhs && is_comparable(lhs)) {
                    return resolved_operator_t{boolean()};
                }
                return std::nullopt;
            case operator_code::logical_and:
            case operator_code::logical_or:
                if (lhs.type() == logical_type::BOOLEAN && rhs.type() == logical_type::BOOLEAN) {
                    return resolved_operator_t{boolean()};
                }
                return std::nullopt;
            case operator_code::invalid:
            case operator_code::negate:
            case operator_code::bit_not:
            case operator_code::logical_not:
            case operator_code::is_null:
            case operator_code::is_not_null:
                return std::nullopt;
            default:
                break;
        }

        // Same numeric type on both sides: the operation stays in that type.
        if (types::is_numeric(lhs.type()) && lhs.type() == rhs.type()) {
            return resolved_operator_t{lhs};
        }
        // TODO: the SQL standard widens the result
        // add: width + 1 for the carry, multiply: width1 + width2 / scale1 + scale2)
        // But it has to be done by operator itself
        if (same_decimal(lhs, rhs)) {
            return resolved_operator_t{lhs};
        }

        switch (code) {
            case operator_code::add:
            case operator_code::subtract: {
                if (is_temporal(lhs.type()) && rhs.type() == logical_type::INTERVAL) {
                    return resolved_operator_t{lhs};
                }
                if (lhs.type() == logical_type::INTERVAL && rhs.type() == logical_type::INTERVAL) {
                    return resolved_operator_t{lhs};
                }
                if (code == operator_code::add && lhs.type() == logical_type::INTERVAL && is_temporal(rhs.type())) {
                    return resolved_operator_t{rhs};
                }
                // The one case that makes result and operand types genuinely independent.
                if (code == operator_code::subtract && is_temporal(lhs.type()) && lhs.type() == rhs.type()) {
                    return resolved_operator_t{complex_logical_type{logical_type::INTERVAL}};
                }
                return std::nullopt;
            }
            case operator_code::multiply: {
                if (lhs.type() == logical_type::INTERVAL && types::is_numeric(rhs.type())) {
                    return resolved_operator_t{lhs};
                }
                if (types::is_numeric(lhs.type()) && rhs.type() == logical_type::INTERVAL) {
                    return resolved_operator_t{rhs};
                }
                return std::nullopt;
            }
            case operator_code::divide: {
                if (lhs.type() == logical_type::INTERVAL && types::is_numeric(rhs.type())) {
                    return resolved_operator_t{lhs};
                }
                return std::nullopt;
            }
            default:
                return std::nullopt;
        }
        return std::nullopt;
    }

} // namespace components::operators