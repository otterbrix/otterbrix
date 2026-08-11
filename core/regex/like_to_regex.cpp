#include "like_to_regex.hpp"

namespace core {

    std::string like_to_regex(const std::string& pattern) {
        std::string result = "^";
        for (size_t i = 0; i < pattern.size(); ++i) {
            char c = pattern[i];
            if (c == '%') {
                result += ".*";
            } else if (c == '_') {
                result += '.';
            } else if (c == '\\' && i + 1 < pattern.size()) {
                ++i;
                // escape the next character literally
                char next = pattern[i];
                if (next == '.' || next == '*' || next == '+' || next == '?' || next == '(' || next == ')' ||
                    next == '[' || next == ']' || next == '{' || next == '}' || next == '|' || next == '^' ||
                    next == '$' || next == '\\') {
                    result += '\\';
                }
                result += next;
            } else if (c == '.' || c == '*' || c == '+' || c == '?' || c == '(' || c == ')' || c == '[' || c == ']' ||
                       c == '{' || c == '}' || c == '|' || c == '^' || c == '$' || c == '\\') {
                result += '\\';
                result += c;
            } else {
                result += c;
            }
        }
        result += '$';
        return result;
    }

} // namespace core