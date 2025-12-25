#pragma once

#include <string_view>

namespace core::detail {

    [[noreturn]] void failed(std::string_view expr,
                             const char* file,
                             unsigned int line,
                             const char* function,
                             std::string_view msg) noexcept;

    [[noreturn]] void log_and_throw_invariant_error(std::string_view condition, std::string_view message);

#ifdef NDEBUG
    inline constexpr bool enable_assert = false;
#else
    inline constexpr bool enable_assert = true;
#endif

} // namespace core::detail
/*
#define assertion_failed_msg(expr, msg)                    \
    do {                                                   \
        if (core::detail::enable_assert && !(expr)) {      \
            core::detail::failed(#expr, __FILE__,          \
                                 __LINE__, __func__, msg); \
        }                                                  \
    } while (0)

#define assertion_failed(expr) assertion_failed_msg(expr, std::string_view{})
*/
#define assertion_exception_msg(condition, message)                                                                    \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            if constexpr (core::detail::enable_assert) {                                                               \
                core::detail::failed(#condition, __FILE__, __LINE__, __func__, message);                               \
            } else {                                                                                                   \
                core::detail::log_and_throw_invariant_error(#condition, message);                                      \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define assertion_exception(condition)                                                                                 \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            if constexpr (core::detail::enable_assert) {                                                               \
                core::detail::failed(#condition, __FILE__, __LINE__, __func__, {});                                    \
            } else {                                                                                                   \
                core::detail::log_and_throw_invariant_error(#condition, {});                                           \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)
