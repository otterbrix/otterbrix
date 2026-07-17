#include "regex.hpp"

#include <re2/re2.h>

#include <string>

namespace core {

    regex_t::regex_t(std::unique_ptr<re2::RE2> re) noexcept
        : re_(std::move(re)) {}

    regex_t::regex_t(regex_t&&) noexcept = default;
    regex_t& regex_t::operator=(regex_t&&) noexcept = default;
    regex_t::~regex_t() = default;

    result_wrapper_t<regex_t>
    regex_t::compile(std::pmr::memory_resource* resource, std::string_view pattern, bool case_insensitive) {
        re2::RE2::Options options;
        options.set_log_errors(false); // a bad USER pattern must not spam stderr
        // Byte-wise (Latin-1) matching: the replaced std::regex engine matched bytes, so the
        // like_to_regex '.'/'.*' must advance over ANY byte. RE2's default UTF-8 mode silently
        // fails to match a non-UTF-8 payload ("caf\xE9" vs LIKE '%') and rejects a pattern
        // carrying such a byte at compile time — both regressions, not features.
        options.set_encoding(re2::RE2::Options::EncodingLatin1);
        if (case_insensitive) {
            options.set_case_sensitive(false);
        }
        auto re = std::make_unique<re2::RE2>(re2::StringPiece(pattern.data(), pattern.size()), options);
        if (!re->ok()) {
            const std::string message = "invalid regular expression: " + re->error();
            return error_t{error_code_t::comparison_failure, std::pmr::string{message.c_str(), resource}};
        }
        return regex_t(std::move(re));
    }

    bool regex_t::match(std::string_view subject) const {
        return re2::RE2::PartialMatch(re2::StringPiece(subject.data(), subject.size()), *re_);
    }

} // namespace core
