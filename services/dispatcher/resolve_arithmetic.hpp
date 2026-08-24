#pragma once

#include <components/casts/cast_registry.hpp>
#include <components/operators/resolve_operator.hpp>
#include <components/types/types.hpp>

namespace services::dispatcher {

    struct resolved_arithmetic_t {
        components::casts::cast_t lhs_cast;
        components::casts::cast_t rhs_cast;
        components::types::complex_logical_type lhs_target;
        components::types::complex_logical_type rhs_target;
        components::operators::resolved_operator_t op;
    };

    [[nodiscard]] resolved_arithmetic_t resolve_arithmetic(const components::casts::cast_registry_t& registry,
                                                           components::operators::operator_code code,
                                                           const components::types::complex_logical_type& lhs,
                                                           const components::types::complex_logical_type& rhs);

    [[nodiscard]] resolved_arithmetic_t resolve_arithmetic(const components::casts::cast_registry_t& registry,
                                                           components::operators::operator_code code,
                                                           const components::types::complex_logical_type& operand);

} // namespace services::dispatcher