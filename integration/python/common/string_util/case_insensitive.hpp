#pragma once

#include "string_util.hpp"

#include <string>
#include <unordered_map>

namespace otterbrix {

    struct case_insensitive_string_hash_function_t {
        uint64_t operator()(const std::string& str) const { return string_utils::ci_hash(str); }
    };

    struct case_insensitive_string_equality_t {
        bool operator()(const std::string& a, const std::string& b) const { return string_utils::ci_equals(a, b); }
    };

    template<typename T>
    using case_insensitive_map_t =
        std::unordered_map<std::string, T, case_insensitive_string_hash_function_t, case_insensitive_string_equality_t>;

} // namespace otterbrix
