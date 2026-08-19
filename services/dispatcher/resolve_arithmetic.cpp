#include "resolve_arithmetic.hpp"

namespace services::dispatcher {

    using components::casts::cast_registry_t;
    using components::operators::operator_code;
    using components::operators::resolve_operator;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    inline resolved_arithmetic_t not_defined() {
        return resolved_arithmetic_t{{}, {}, {}, {}, {complex_logical_type{logical_type::INVALID}}};
    }

    resolved_arithmetic_t resolve_arithmetic(const cast_registry_t& registry,
                                             operator_code code,
                                             const complex_logical_type& lhs,
                                             const complex_logical_type& rhs) {
        if (auto exact = resolve_operator(code, lhs, rhs)) {
            return resolved_arithmetic_t{{}, {}, lhs, rhs, *exact};
        }

        auto common = registry.find_best_common_type(lhs, rhs);
        if (!common.has_value()) {
            return not_defined();
        }

        auto unified = resolve_operator(code, common->type, common->type);
        if (!unified.has_value()) {
            return not_defined();
        }

        return resolved_arithmetic_t{std::move(common->left_cast),
                                     std::move(common->right_cast),
                                     common->type,
                                     common->type,
                                     *unified};
    }

    resolved_arithmetic_t
    resolve_arithmetic(const cast_registry_t&, operator_code code, const complex_logical_type& operand) {
        auto exact = resolve_operator(code, operand);
        if (!exact.has_value()) {
            return not_defined();
        }
        return resolved_arithmetic_t{{}, {}, operand, operand, *exact};
    }

} // namespace services::dispatcher
