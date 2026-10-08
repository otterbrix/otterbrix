#pragma once

#include <string>
#include <vector>

namespace components::catalog {

    struct unique_key_t {
        std::string name;
        std::vector<std::string> columns;
    };

    std::vector<std::vector<std::string>> unique_key_columns(const std::vector<unique_key_t>& keys);

} // namespace components::catalog
