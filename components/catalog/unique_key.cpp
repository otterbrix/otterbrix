#include "unique_key.hpp"

namespace components::catalog {

    std::vector<std::vector<std::string>> unique_key_columns(const std::vector<unique_key_t>& keys) {
        std::vector<std::vector<std::string>> groups;
        groups.reserve(keys.size());
        for (const auto& key : keys) {
            groups.push_back(key.columns);
        }
        return groups;
    }

} // namespace components::catalog
