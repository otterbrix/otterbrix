#pragma once

#include <components/operators/operator_code.hpp>
#include <components/types/types.hpp>

#include <optional>

namespace components::operators {

    // TODO: include actual operator function
    struct resolved_operator_t {
        types::complex_logical_type result;
    };

    [[nodiscard]] std::optional<resolved_operator_t> resolve_operator(operator_code code,
                                                                      const types::complex_logical_type& operand);

    [[nodiscard]] std::optional<resolved_operator_t> resolve_operator(operator_code code,
                                                                      const types::complex_logical_type& lhs,
                                                                      const types::complex_logical_type& rhs);

} // namespace components::operators