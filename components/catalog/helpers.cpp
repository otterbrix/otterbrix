#include "helpers.hpp"

#include <charconv>
#include <string_view>

namespace components::catalog {

    std::vector<oid_t> parse_oid_csv(const std::string& s, bool& ok) {
        ok = true;
        std::vector<oid_t> out;
        if (s.empty()) {
            // An absent list is a valid empty list, not malformed; callers refuse the two differently.
            return out;
        }
        std::size_t i = 0;
        // Not `while (i < s.size())`: it would never visit the empty token after a trailing comma --
        // the only trace of truncation -- so "7,11,13" cut to "7,11," would silently read back clean.
        for (;;) {
            const std::size_t j = s.find(',', i);
            const std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
            if (tok.empty()) {
                // encode_oid_csv never writes an empty token, so seeing one means this isn't its inverse's output.
                ok = false;
            } else {
                // Reads straight into oid_t: from_chars flags out-of-range directly -- a wider int
                // cast down would wrap 2^32+N into N, silently binding the key to the wrong column.
                oid_t v{};
                const auto [ptr, ec] = std::from_chars(tok.data(), tok.data() + tok.size(), v);
                // The whole token must match, or "12x" would silently read as 12 (from_chars alone accepts it).
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
