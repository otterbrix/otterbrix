#pragma once

#include <components/casts/cast_function.hpp>

#include <core/date/date_cast.hpp>

namespace components::casts::kernels {

    // date/time <-> date/time conversion
    template<typename From, typename To>
    core::error_t datetime_convert_cast(const vector::vector_t& source,
                                        vector::vector_t* result,
                                        const graph_execution_context& context,
                                        uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const From from = source.get_value<From>(row);
            result->set_value(row, core::date::convert_date_time<To>(from, context.timezone_offset));
        }
        return core::error_t::no_error();
    }

} // namespace components::casts::kernels
