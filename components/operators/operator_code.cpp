#include "operator_code.hpp"

namespace components::operators {

    operator_arity arity_of(operator_code code) noexcept {
        switch (code) {
            case operator_code::negate:
            case operator_code::bit_not:
            case operator_code::logical_not:
            case operator_code::is_null:
            case operator_code::is_not_null:
                return operator_arity::unary;
            default:
                return operator_arity::binary;
        }
    }

} // namespace components::operators