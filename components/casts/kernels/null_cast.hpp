#pragma once

#include <components/casts/cast_function.hpp>

namespace components::casts::kernels {

    // Every type carries validity, but plain NULL from parser can not express type
    inline core::error_t null_cast(const vector::vector_t&,
                                   vector::vector_t* result,
                                   const graph_execution_context&,
                                   uint64_t count) noexcept {
        // const_vector redirects all [N] to the first element
        // TODO: it might be dangerous to modify vector type without callers knowledge
        if (result->get_vector_type() == vector::vector_type::CONSTANT) {
            result->set_null(0, true);
            return core::error_t::no_error();
        }
        for (uint64_t row = 0; row < count; ++row) {
            result->set_null(row, true);
        }
        return core::error_t::no_error();
    }

} // namespace components::casts::kernels