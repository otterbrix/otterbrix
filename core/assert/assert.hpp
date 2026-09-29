#pragma once

#include <string_view>

class log_t;

namespace core::detail {

    // `log` is the caller's own logger; without one (nullptr) the report goes to stderr, never to
    // a process-wide logger another engine may own.
    [[noreturn]] void failed(log_t* log,
                             std::string_view expr,
                             const char* file,
                             unsigned int line,
                             const char* function,
                             std::string_view msg) noexcept;

    [[noreturn]] void
    log_and_throw_invariant_error(log_t* log, std::string_view condition, std::string_view message);

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
// Invalid states only (e.g. a pointer that cannot be null) — not a replacement for core::error_t.
#define assertion_log_msg(log_ptr, condition, message)                                                                 \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            if constexpr (core::detail::enable_assert) {                                                               \
                core::detail::failed(log_ptr, #condition, __FILE__, __LINE__, __func__, message);                      \
            } else {                                                                                                   \
                core::detail::log_and_throw_invariant_error(log_ptr, #condition, message);                             \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define assertion_exception_msg(condition, message) assertion_log_msg(nullptr, condition, message)

#define assertion_exception(condition) assertion_log_msg(nullptr, condition, std::string_view{})
