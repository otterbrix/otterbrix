#include "helpers.hpp"

#include <charconv>
#include <string_view>

namespace components::catalog {

    std::vector<oid_t> parse_oid_csv(const std::string& s, bool& ok) {
        ok = true;
        std::vector<oid_t> out;
        if (s.empty()) {
            // An ABSENT list is an empty list, not an unreadable one: the callers
            // refuse those two with different words, so they must not merge here.
            return out;
        }
        std::size_t i = 0;
        // Not `while (i < s.size())`: that form stops as soon as the last comma is
        // the final character, so the empty token BEHIND it is never looked at —
        // and that empty token is the only trace a list TRUNCATED at a separator
        // leaves. "7,11,13" cut down to "7,11," read back as a clean two-element
        // list, which is precisely the shape no caller can recover: the surviving
        // tokens agree with every length guard downstream. Every token, including
        // the one after the final separator, is visited.
        for (;;) {
            const std::size_t j = s.find(',', i);
            const std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
            if (tok.empty()) {
                // encode_oid_csv never writes an empty token, so one here says the
                // string is not what this function's inverse produced.
                ok = false;
            } else {
                // Read STRAIGHT INTO oid_t. from_chars answers result_out_of_range
                // for a value the target type cannot hold, and that answer is the
                // whole point: reading into a wider integer and casting down turns
                // 2^32 + N into N — the key bound to whichever column oid N happens
                // to be, with one token in and one token out, so not a single length
                // guard anywhere could see the swap.
                oid_t v{};
                const auto [ptr, ec] = std::from_chars(tok.data(), tok.data() + tok.size(), v);
                // The WHOLE token has to be the number. from_chars stops at the first
                // character it cannot use and still reports success, so "12x" would
                // read as 12 — a key column silently swapped for another one.
                if (ec == std::errc{} && ptr == tok.data() + tok.size()) {
                    out.push_back(v);
                } else {
                    ok = false;
                }
            }
            if (j == std::string::npos) {
                break;
            }
            i = j + 1;
        }
        return out;
    }

    std::string encode_oid_csv(const std::vector<oid_t>& oids) {
        std::string out;
        for (std::size_t i = 0; i < oids.size(); ++i) {
            if (i)
                out += ',';
            out += std::to_string(oids[i]);
        }
        return out;
    }

} // namespace components::catalog
