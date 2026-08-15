#pragma once

#include <string>

namespace core {

    // Translate a SQL LIKE pattern into a regex
    std::string like_to_regex(const std::string& pattern);

} // namespace core