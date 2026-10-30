#pragma once

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>

namespace re2 {
    class RE2;
} // namespace re2

namespace core {

    struct regex_options_t {
        bool case_insensitive{false};
        // '.' matches '\n'
        bool dot_matches_newline{false};
        // '^' and '$' match at every line, not only at the ends of the subject
        bool multiline{false};
        // the pattern is a plain string
        bool literal{false};
        bool utf8{false};
        // '\w' and '\W' take a Unicode letter or decimal digit, as PostgreSQL's do; RE2's take ASCII only. '\b' is
        // a backspace and '\B' a backslash, as in PostgreSQL; RE2's are word boundaries
        bool postgres_escapes{false};

        bool operator==(const regex_options_t&) const = default;
    };

    // Compile-once wrapper over Google RE2. RE2 never throws: an invalid pattern is reported by
    // RE2::ok() at construction, which compile() surfaces as a core::error_t — so a bad user
    // pattern (`col regexp '('`) becomes a clean error instead of an uncaught exception.
    // match() is a PARTIAL search (RE2::PartialMatch); a self-anchored like_to_regex
    // pattern therefore matches identically. Case-insensitivity (ILIKE) is a compile-time option.
    // Matching is BYTE-wise (RE2 Latin-1 encoding) unless options.utf8 or options.postgres_escapes: '.'
    // advances over any byte, so non-UTF-8 payloads and patterns keep matching.
    class regex_t {
    public:
        // Compile `pattern` (RE2/Google syntax; the like_to_regex output alphabet ^ $ . .* and
        // escaped literals is a compatible subset). Returns an error — never throws or aborts —
        // when the pattern does not compile.
        [[nodiscard]] static result_wrapper_t<regex_t>
        compile(std::pmr::memory_resource* resource, std::string_view pattern, regex_options_t options = {});

        regex_t(regex_t&&) noexcept;
        regex_t& operator=(regex_t&&) noexcept;
        regex_t(const regex_t&) = delete;
        regex_t& operator=(const regex_t&) = delete;
        ~regex_t();

        // True when `subject` contains a substring matching the pattern (partial search).
        [[nodiscard]] bool match(std::string_view subject) const;

        [[nodiscard]] size_t capturing_groups() const noexcept;

        // The leftmost match that starts at `start` or later; the bytes before `start` still decide
        // '^' and '\b'. groups[0] receives the match, groups[i] the i-th group, a view with a null
        // data() when that group took no part in it.
        [[nodiscard]] bool find(std::string_view subject, size_t start, std::span<std::string_view> groups) const;

    private:
        explicit regex_t(std::unique_ptr<re2::RE2> re) noexcept;
        std::unique_ptr<re2::RE2> re_; // pimpl — keeps <re2/re2.h> out of this header
    };

} // namespace core
