#pragma once

#include "string_util.hpp"

#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <string>

namespace otterbrix {

    struct CaseInsensitiveStringHashFunction {
        uint64_t operator()(const std::string &str) const {
            return string_utils::CIHash(str);
        }
    };

    struct CaseInsensitiveStringEquality {
        bool operator()(const std::string &a, const std::string &b) const {
            return string_utils::CIEquals(a, b);
        }
    };

    struct CaseInsensitiveStringCompare {
        bool operator()(const std::string &s1, const std::string &s2) const {
            return string_utils::CILessThan(s1, s2);
        }
    };

    template <typename T>
    using case_insensitive_map_t = std::unordered_map<std::string, T, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>;

    using case_insensitive_set_t = std::unordered_set<std::string, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>;

    template <typename T>
    using case_insensitive_tree_t = std::map<std::string, T, CaseInsensitiveStringCompare>;

}
