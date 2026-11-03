#include "path_utils.hpp"
#include <algorithm>
#include <sstream>

namespace core::filesystem::path_utils {

    std::vector<std::string> split(const std::string& str, char delimiter) {
        std::stringstream ss(str);
        std::vector<std::string> lines;
        std::string temp;
        while (std::getline(ss, temp, delimiter)) {
            lines.push_back(temp);
        }
        return (lines);
    }

    std::vector<std::string> split(const std::string& input, const std::string& split) {
        std::vector<std::string> splits;

        uint64_t last = 0;
        uint64_t input_len = input.size();
        uint64_t split_len = split.size();
        while (last <= input_len) {
            uint64_t next = input.find(split, last);
            if (next == std::string::npos) {
                next = input_len;
            }

            std::string substr = input.substr(last, next - last);
            if (!substr.empty()) {
                splits.push_back(substr);
            }
            last = next + split_len;
        }
        if (splits.empty()) {
            splits.push_back(input);
        }
        return splits;
    }

    bool glob(const char* string, uint64_t slen, const char* pattern, uint64_t plen, bool allow_question_mark) {
        uint64_t sidx = 0;
        uint64_t pidx = 0;
    main_loop : {
        while (sidx < slen && pidx < plen) {
            char s = string[sidx];
            char p = pattern[pidx];
            switch (p) {
                case '*': {
                    pidx++;
                    while (pidx < plen && pattern[pidx] == '*') {
                        pidx++;
                    }
                    if (pidx == plen) {
                        return true;
                    }
                    for (; sidx < slen; sidx++) {
                        if (glob(string + sidx, slen - sidx, pattern + pidx, plen - pidx, allow_question_mark)) {
                            return true;
                        }
                    }
                    return false;
                }
                case '?':
                    if (allow_question_mark) {
                        break;
                    }
                    return false;
                case '[':
                    pidx++;
                    goto parse_bracket;
                case '\\':
                    pidx++;
                    if (pidx == plen) {
                        return false;
                    }
                    p = pattern[pidx];
                    if (s != p) {
                        return false;
                    }
                    break;
                default:
                    if (s != p) {
                        return false;
                    }
                    break;
            }
            sidx++;
            pidx++;
        }
        while (pidx < plen && pattern[pidx] == '*') {
            pidx++;
        }
        return pidx == plen && sidx == slen;
    }
    parse_bracket : {
        if (pidx == plen) {
            return false;
        }
        char p = pattern[pidx];
        char s = string[sidx];
        bool invert = false;
        if (p == '!') {
            invert = true;
            pidx++;
        }
        bool found_match = invert;
        uint64_t start_pos = pidx;
        bool found_closing_bracket = false;
        while (pidx < plen) {
            p = pattern[pidx];
            if (p == ']' && pidx > start_pos) {
                found_closing_bracket = true;
                pidx++;
                break;
            }
            if (pidx + 1 == plen) {
                break;
            }
            bool matches;
            if (pattern[pidx + 1] == '-') {
                if (pidx + 2 == plen) {
                    break;
                }
                char next_char = pattern[pidx + 2];
                matches = s >= p && s <= next_char;
                pidx += 3;
            } else {
                matches = p == s;
                pidx++;
            }
            if (found_match == invert && matches) {
                found_match = !invert;
            }
        }
        if (!found_closing_bracket) {
            return false;
        }
        if (!found_match) {
            return false;
        }
        sidx++;
        goto main_loop;
    }
    }
} // namespace core::filesystem::path_utils