#pragma once

#include <limits>
#include <type_traits>

namespace components::casts::kernels::detail {

    template<typename Target, typename Source>
    [[nodiscard]] inline Target numeric_convert(Source value) noexcept {
        if constexpr (std::is_integral_v<Source> && !std::numeric_limits<Source>::is_signed &&
                      sizeof(Source) < sizeof(int)) {
            return static_cast<Target>(static_cast<unsigned int>(value));
        } else {
            return static_cast<Target>(value);
        }
    }

} // namespace components::casts::kernels::detail
