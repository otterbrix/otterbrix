#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace core::filesystem::path_utils {

    std::vector<std::string> split(const std::string& str, char delimiter);
    std::vector<std::string> split(const std::string& input, const std::string& split);

    bool
    glob(const char* string, std::uint64_t slen, const char* pattern, std::uint64_t plen, bool allow_question_mark);

} // namespace core::filesystem::path_utils