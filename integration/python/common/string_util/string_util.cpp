#include "string_util.hpp"

#include <algorithm>
#include <unordered_map>

namespace otterbrix {

    // Jenkins hash function: https://en.wikipedia.org/wiki/Jenkins_hash_function
    uint64_t string_utils::ci_hash(const std::string& str) {
        uint32_t hash = 0;
        for (auto c : str) {
            hash += static_cast<uint32_t>(string_utils::character_to_lower(static_cast<char>(c)));
            hash += hash << 10;
            hash ^= hash >> 6;
        }
        hash += hash << 3;
        hash ^= hash >> 11;
        hash += hash << 15;
        return hash;
    }

    bool string_utils::ci_equals(const std::string& l1, const std::string& l2) {
        if (l1.length() != l2.length()) {
            return false;
        } else {
            for (size_t i = 0; i < l1.length(); i++) {
                auto low1 = string_utils::character_to_lower(static_cast<char>(l1[i]));
                auto low2 = string_utils::character_to_lower(static_cast<char>(l2[i]));
                if (low1 != low2) {
                    return false;
                }
            }
        }
        return true;
    }

    std::string string_utils::lower(const std::string& str) {
        std::string copy(str);
        std::transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) {
            return string_utils::character_to_lower(static_cast<char>(c));
        });
        return copy;
    }

    void string_utils::deduplicate_columns(std::vector<std::string>& names) {
        std::unordered_map<std::string, uint64_t> name_map;
        for (auto& column_name : names) {
            // put it all lower_case
            auto low_column_name = string_utils::lower(column_name);
            if (name_map.find(low_column_name) == name_map.end()) {
                // name does not exist yet
                name_map[low_column_name]++;
            } else {
                // name already exists, we add _x where x is the repetition number
                std::string new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
                auto new_column_name_low = string_utils::lower(new_column_name);
                while (name_map.find(new_column_name_low) != name_map.end()) {
                    // This name is already here due to a previous definition
                    name_map[low_column_name]++;
                    new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
                    new_column_name_low = string_utils::lower(new_column_name);
                }
                column_name = new_column_name;
                name_map[new_column_name_low]++;
            }
        }
    }

} // namespace otterbrix
