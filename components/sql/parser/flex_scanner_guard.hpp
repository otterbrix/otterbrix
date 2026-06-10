#pragma once

/*-------------------------------------------------------------------------
 *
 * flex_scanner_guard.hpp
 *      RAII guard over a reentrant flex scanner, for parser extensions backed
 *      by a flex+bison grammar.
 *
 * bison's yyparse() drives the scanner itself (it calls yylex), so an extension
 * only needs to (1) stand a scanner up over the query string and (2) tear it
 * down — including when yyparse throws. This guard does exactly that.
 *
 * Example usage:
 *
 *     using my_scanner = EXTENSION_FLEX_SCANNER(my_yy);   // my_yy is flex's %prefix
 *     ...
 *     my_scanner scanner(query.c_str());
 *
 *-------------------------------------------------------------------------
 */

namespace components::sql::parser {
    namespace detail {
        template<class ScanString, class Lex>
        struct flex_scanner_types;

        template<class Buffer, class Handle, class Value>
        struct flex_scanner_types<Buffer (*)(const char*, Handle), int (*)(Value*, Handle)> {
            using handle = Handle;
            using buffer = Buffer;
            using value = Value;
        };
    } // namespace detail

    template<auto LexInit, auto LexDestroy, auto ScanString, auto DeleteBuffer, auto Lex>
    class flex_scanner_guard {
        using types = detail::flex_scanner_types<decltype(ScanString), decltype(Lex)>;
        using handle_t = typename types::handle;
        using buffer_t = typename types::buffer;

    public:
        using value_t = typename types::value;

        explicit flex_scanner_guard(const char* query) {
            if (LexInit(&handle_) == 0) {
                buffer_ = ScanString(query, handle_);
            }
        }

        ~flex_scanner_guard() {
            if (handle_ != handle_t{}) {
                if (buffer_ != buffer_t{}) {
                    DeleteBuffer(buffer_, handle_);
                }
                LexDestroy(handle_);
            }
        }

        flex_scanner_guard(const flex_scanner_guard&) = delete;
        flex_scanner_guard& operator=(const flex_scanner_guard&) = delete;

        [[nodiscard]] bool valid() const noexcept { return handle_ != handle_t{}; }
        [[nodiscard]] handle_t handle() const noexcept { return handle_; }
        int next_token(value_t* value) { return Lex(value, handle_); }

    private:
        handle_t handle_{};
        buffer_t buffer_{};
    };

} // namespace components::sql::parser

// expands to the flex_scanner_guard type for a reentrant scanner generated with %option prefix="<prefix>"
#define EXTENSION_FLEX_SCANNER(prefix)                                                                                 \
    ::components::sql::parser::flex_scanner_guard<&prefix##lex_init,                                                   \
                                                  &prefix##lex_destroy,                                                \
                                                  &prefix##_scan_string,                                               \
                                                  &prefix##_delete_buffer,                                             \
                                                  &prefix##lex>
