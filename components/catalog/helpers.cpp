#include "helpers.hpp"

#include <string_view>

namespace components::catalog {

    std::vector<oid_t> parse_oid_csv(const std::string& s) {
        std::vector<oid_t> out;
        std::size_t i = 0;
        while (i < s.size()) {
            std::size_t j = s.find(',', i);
            std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
            if (!tok.empty()) {
                try {
                    out.push_back(static_cast<oid_t>(std::stoul(std::string(tok))));
                } catch (...) {
                    // malformed token — skip
                }
            }
            if (j == std::string::npos) break;
            i = j + 1;
        }
        return out;
    }

    std::string encode_oid_csv(const std::vector<oid_t>& oids) {
        std::string out;
        for (std::size_t i = 0; i < oids.size(); ++i) {
            if (i) out += ',';
            out += std::to_string(oids[i]);
        }
        return out;
    }

} // namespace components::catalog
