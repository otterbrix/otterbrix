#pragma once

#include <memory_resource>
#include <string>
#include <string_view>

namespace core {

    // Translate a SQL LIKE pattern into a regex
    std::pmr::string like_to_regex(std::pmr::memory_resource* resource, std::string_view pattern);

} // namespace core