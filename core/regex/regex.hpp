#pragma once

#include <core/result_wrapper.hpp>

#include <memory>
#include <string_view>

namespace re2 {
    class RE2;
} // namespace re2

namespace core {

    // Compile-once wrapper over Google RE2. RE2 never throws: an invalid pattern is reported by
    // RE2::ok() at construction, which compile() surfaces as a core::error_t — so a bad user
    // pattern (`col regexp '('`) becomes a clean error instead of an uncaught std::regex_error
    // that std::regex has no non-throwing way to report. match() is a PARTIAL search
    // (RE2::PartialMatch), the exact analogue of std::regex_search; a self-anchored like_to_regex
    // pattern therefore matches identically. Case-insensitivity (ILIKE) is a compile-time option.
    class regex_t {
    public:
        // Compile `pattern` (RE2/Google syntax; the like_to_regex output alphabet ^ $ . .* and
        // escaped literals is a compatible subset). Returns an error — never throws or aborts —
        // when the pattern does not compile.
        [[nodiscard]] static result_wrapper_t<regex_t>
        compile(std::pmr::memory_resource* resource, std::string_view pattern, bool case_insensitive = false);

        regex_t(regex_t&&) noexcept;
        regex_t& operator=(regex_t&&) noexcept;
        regex_t(const regex_t&) = delete;
        regex_t& operator=(const regex_t&) = delete;
        ~regex_t();

        // True when `subject` contains a substring matching the pattern (partial search).
        [[nodiscard]] bool match(std::string_view subject) const;

    private:
        explicit regex_t(std::unique_ptr<re2::RE2> re) noexcept;
        std::unique_ptr<re2::RE2> re_; // pimpl — keeps <re2/re2.h> out of this header
    };

} // namespace core
