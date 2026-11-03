#include "regex.hpp"

#include <re2/re2.h>

#include <memory_resource>
#include <string>
#include <vector>

namespace core {

    namespace {
        // RE2 has no multi-line option outside its POSIX syntax, which drops the Perl classes and (?:), so
        // multi-line matching is the pattern's own flag group.
        constexpr std::string_view multiline_prefix = "(?m)";

        constexpr std::string_view word_items = "\\p{L}\\p{Nd}_";

        struct origin_t {
            size_t begin;
            size_t end;
        };

        struct source_t {
            explicit source_t(std::pmr::memory_resource* resource)
                : text(resource)
                , origin(resource) {}

            void put(std::string_view piece, size_t begin, size_t end) {
                text.append(piece);
                origin.insert(origin.end(), piece.size(), origin_t{begin, end});
            }

            void copy(std::string_view pattern, size_t begin, size_t end) {
                for (size_t at = begin; at < end; ++at) {
                    put(pattern.substr(at, 1), at, at + 1);
                }
            }

            void append(const source_t& other) {
                text.append(other.text);
                origin.insert(origin.end(), other.origin.begin(), other.origin.end());
            }

            std::pmr::string text;
            std::pmr::vector<origin_t> origin;
        };

        error_t invalid_pattern(std::pmr::memory_resource* resource, std::string_view reason) {
            std::pmr::string message{"invalid regular expression: ", resource};
            message.append(reason);
            return error_t{error_code_t::invalid_regular_expression, std::move(message)};
        }

        error_t refusal_of(std::pmr::memory_resource* resource,
                           const re2::RE2& re,
                           const source_t& source,
                           std::string_view pattern) {
            const std::string_view reason = re.error();
            const std::string_view fragment = re.error_arg();
            const auto at = std::string_view{source.text}.rfind(fragment);
            if (fragment.empty() || at == std::string_view::npos || !reason.ends_with(fragment)) {
                return invalid_pattern(resource, reason);
            }
            const auto begin = source.origin[at].begin;
            const auto end = source.origin[at + fragment.size() - 1].end;
            std::pmr::string quoted{reason.substr(0, reason.size() - fragment.size()), resource};
            quoted.append(pattern.substr(begin, end - begin));
            return invalid_pattern(resource, quoted);
        }

        bool is_class_escape(char escaped) noexcept {
            return escaped == 'w' || escaped == 'W' || escaped == 'd' || escaped == 'D' || escaped == 's' ||
                   escaped == 'S';
        }

        // A bracket expression from its '[' on; answers the offset past its ']'. RE2 has no class subtraction:
        // [x\W] is an alternation, and [^x\W] the word characters outside x, one negated bracket per part of \w.
        result_wrapper_t<size_t>
        append_bracket(std::pmr::memory_resource* resource, std::string_view pattern, size_t open, source_t* out) {
            size_t at = open + 1;
            const bool negated = at < pattern.size() && pattern[at] == '^';
            if (negated) {
                ++at;
            }
            const size_t first_item = at;
            source_t items{resource};
            bool non_word = false;
            for (bool first = true; at < pattern.size(); first = false) {
                const char symbol = pattern[at];
                if (symbol == ']' && !first) {
                    break;
                }
                if (symbol == '\\' && at + 1 < pattern.size()) {
                    const char escaped = pattern[at + 1];
                    if (is_class_escape(escaped) && at + 2 < pattern.size() && pattern[at + 2] == '-' &&
                        (at + 3 == pattern.size() || pattern[at + 3] != ']')) {
                        return invalid_pattern(resource, "invalid character range");
                    }
                    if (escaped == 'w') {
                        items.put(word_items, at, at + 2);
                    } else if (escaped == 'W') {
                        non_word = true;
                    } else if (escaped == 'b') {
                        items.put("\\x08", at, at + 2);
                    } else if (escaped == 'B') {
                        items.put("\\\\", at, at + 2);
                    } else {
                        items.copy(pattern, at, at + 2);
                    }
                    at += 2;
                    continue;
                }
                if (symbol == '[' && at + 1 < pattern.size() && pattern[at + 1] == ':') {
                    const auto close = pattern.find(":]", at + 2);
                    if (close != std::string_view::npos) {
                        items.copy(pattern, at, close + 2);
                        at = close + 2;
                        continue;
                    }
                }
                items.copy(pattern, at, at + 1);
                ++at;
            }
            if (at == pattern.size()) {
                out->copy(pattern, open, at);
                return at;
            }
            if (!non_word) {
                out->copy(pattern, open, first_item);
                out->append(items);
                out->copy(pattern, at, at + 1);
                return at + 1;
            }
            const std::string_view written{items.text};
            std::pmr::string rewritten{resource};
            if (written.empty()) {
                rewritten.append(negated ? "[" : "[^");
                rewritten.append(word_items);
                rewritten.push_back(']');
            } else if (!negated) {
                rewritten.append(written.front() == '^' ? "(?:[\\" : "(?:[");
                rewritten.append(written);
                rewritten.append("]|[^");
                rewritten.append(word_items);
                rewritten.append("])");
            } else {
                const std::string_view rest = written.front() == ']' ? written.substr(1) : written;
                const std::string_view literal_bracket = written.front() == ']' ? "\\]" : "";
                const char* separator = "(?:";
                for (std::string_view outside : {"\\P{L}", "\\P{Nd}", "\\x00-\\x5E\\x60-\\x{10FFFF}"}) {
                    rewritten.append(separator);
                    rewritten.append("[^");
                    rewritten.append(outside);
                    rewritten.append(literal_bracket);
                    rewritten.append(rest);
                    rewritten.push_back(']');
                    separator = "|";
                }
                rewritten.push_back(')');
            }
            out->put(rewritten, open, at + 1);
            return at + 1;
        }

        error_t append_postgres_escapes(std::pmr::memory_resource* resource, std::string_view pattern, source_t* out) {
            size_t at = 0;
            while (at < pattern.size()) {
                const char symbol = pattern[at];
                if (symbol == '[') {
                    auto past = append_bracket(resource, pattern, at, out);
                    if (past.has_error()) {
                        return past.error();
                    }
                    at = past.value();
                    continue;
                }
                if (symbol != '\\' || at + 1 == pattern.size()) {
                    out->copy(pattern, at, at + 1);
                    ++at;
                    continue;
                }
                const char escaped = pattern[at + 1];
                if (escaped == 'w') {
                    out->put("[", at, at + 2);
                    out->put(word_items, at, at + 2);
                    out->put("]", at, at + 2);
                    at += 2;
                } else if (escaped == 'W') {
                    out->put("[^", at, at + 2);
                    out->put(word_items, at, at + 2);
                    out->put("]", at, at + 2);
                    at += 2;
                } else if (escaped == 'b') {
                    out->put("\\x08", at, at + 2);
                    at += 2;
                } else if (escaped == 'B') {
                    out->put("\\\\", at, at + 2);
                    at += 2;
                } else if (escaped == 'Q') {
                    return invalid_pattern(resource, "invalid escape sequence: \\Q");
                } else {
                    out->copy(pattern, at, at + 2);
                    at += 2;
                }
            }
            return error_t::no_error();
        }
    } // namespace

    regex_t::regex_t(std::unique_ptr<re2::RE2> re) noexcept
        : re_(std::move(re)) {}

    regex_t::regex_t(regex_t&&) noexcept = default;
    regex_t& regex_t::operator=(regex_t&&) noexcept = default;
    regex_t::~regex_t() = default;

    result_wrapper_t<regex_t>
    regex_t::compile(std::pmr::memory_resource* resource, std::string_view pattern, regex_options_t options) {
        re2::RE2::Options re2_options;
        re2_options.set_log_errors(false); // a bad USER pattern must not spam stderr
        // Byte-wise (Latin-1) unless UTF-8: the like_to_regex '.'/'.*' must advance over ANY byte. RE2's
        // default UTF-8 mode silently fails to match a non-UTF-8 payload ("caf\xE9" vs LIKE '%') and
        // rejects a pattern carrying such a byte at compile time.
        const bool utf8 = options.utf8 || options.postgres_escapes;
        re2_options.set_encoding(utf8 ? re2::RE2::Options::EncodingUTF8 : re2::RE2::Options::EncodingLatin1);
        re2_options.set_case_sensitive(!options.case_insensitive);
        re2_options.set_dot_nl(options.dot_matches_newline);
        re2_options.set_literal(options.literal);
        const std::string_view prefix = options.multiline && !options.literal ? multiline_prefix : std::string_view{};
        source_t source{resource};
        source.put(prefix, 0, 0);
        if (options.postgres_escapes && !options.literal) {
            if (auto error = append_postgres_escapes(resource, pattern, &source); error.contains_error()) {
                return error;
            }
        } else {
            source.copy(pattern, 0, pattern.size());
        }
        auto re = std::make_unique<re2::RE2>(std::string_view{source.text}, re2_options);
        if (!re->ok()) {
            return refusal_of(resource, *re, source, pattern);
        }
        return regex_t(std::move(re));
    }

    bool regex_t::match(std::string_view subject) const { return re2::RE2::PartialMatch(subject, *re_); }

    size_t regex_t::capturing_groups() const noexcept { return static_cast<size_t>(re_->NumberOfCapturingGroups()); }

    bool regex_t::find(std::string_view subject, size_t start, std::span<std::string_view> groups) const {
        return re_->Match(subject,
                          start,
                          subject.size(),
                          re2::RE2::UNANCHORED,
                          groups.data(),
                          static_cast<int>(groups.size()));
    }

} // namespace core
