#include "assert.hpp"

#include <cstdio>
#include <cstdlib>

#include <boost/stacktrace.hpp>
#include <fmt/format.h>

#include <components/log/log.hpp>

#include "trace_full_exception.hpp"

namespace core::detail {

    class InvariantError : public trace_full_exception {
        using trace_full_exception::trace_full_exception;
    };

    namespace {
        void report(log_t* log, const std::string& text) noexcept {
            if (log != nullptr && log->is_valid()) {
                error(*log, text);
                (*log)->flush();
                return;
            }
            std::fputs(text.c_str(), stderr);
            std::fputc('\n', stderr);
            std::fflush(stderr);
        }
    } // namespace

    void failed(log_t* log,
                std::string_view expr,
                const char* file,
                unsigned int line,
                const char* function,
                std::string_view msg) noexcept {
        auto trace = boost::stacktrace::stacktrace();
        report(log,
               fmt::format("error at {}:{}:{}. assertion '{}' failed{}{}.\n Stacktrace:\n{}\n",
                           file,
                           line,
                           (function ? function : ""),
                           expr,
                           (msg.empty() ? std::string_view{} : std::string_view{": "}),
                           msg,
                           to_string(trace)));
        abort();
    }

    void log_and_throw_invariant_error(log_t* log, std::string_view condition, std::string_view message) {
        const std::string err_str = fmt::format("invariant ({}) violation: {}", condition, message);
        report(log, err_str);
        throw InvariantError(err_str);
    }

} // namespace core::detail
