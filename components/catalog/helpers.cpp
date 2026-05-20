#include "helpers.hpp"

#include <charconv>
#include <string_view>

namespace components::catalog {

    std::vector<oid_t> parse_oid_csv(const std::string& s) {
        std::vector<oid_t> out;
        std::size_t i = 0;
        while (i < s.size()) {
            std::size_t j = s.find(',', i);
            std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
            if (!tok.empty()) {
                unsigned long v{};
                auto [ptr, ec] = std::from_chars(tok.data(), tok.data() + tok.size(), v);
                if (ec == std::errc{}) {
                    out.push_back(static_cast<oid_t>(v));
                }
            }
            if (j == std::string::npos)
                break;
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

    std::vector<std::string>
    attoids_to_names(const std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>& attr_rows,
                     const std::vector<oid_t>& attoids) {
        std::vector<std::string> out;
        out.reserve(attoids.size());
        for (const auto& wanted_oid : attoids) {
            for (const auto& row : attr_rows) {
                if (row.size() <= pg_attribute_col::attname)
                    continue;
                auto row_attoid = static_cast<oid_t>(row[pg_attribute_col::attoid].value<std::uint32_t>());
                if (row_attoid == wanted_oid) {
                    out.emplace_back(std::string(row[pg_attribute_col::attname].value<std::string_view>()));
                    break;
                }
            }
        }
        return out;
    }

} // namespace components::catalog
