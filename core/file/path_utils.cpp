#include "path_utils.hpp"
#include <algorithm>
#include <memory>
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

#ifdef PLATFORM_WINDOWS

    std::wstring UTF8_to_Unicode(const char* input) {
        uint64_t result_size;

        result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
        if (result_size == 0) {
            throw std::runtime_error("Failure in MultiByteToWideChar");
        }
        auto buffer = std::make_unique<wchar_t[]>(result_size);
        result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result_size);
        if (result_size == 0) {
            throw std::runtime_error("Failure in MultiByteToWideChar");
        }
        return std::wstring(buffer.get(), result_size);
    }

    std::string wchar_to_MultiByteWrapper(LPCWSTR input, uint32_t code_page) {
        uint64_t result_size;

        result_size = WideCharToMultiByte(code_page, 0, input, -1, 0, 0, 0, 0);
        if (result_size == 0) {
            throw std::runtime_error("Failure in WideCharToMultiByte");
        }
        auto buffer = std::make_unique<char[]>(result_size);
        result_size = WideCharToMultiByte(code_page, 0, input, -1, buffer.get(), result_size, 0, 0);
        if (result_size == 0) {
            throw std::runtime_error("Failure in WideCharToMultiByte");
        }
        return std::string(buffer.get(), result_size - 1);
    }

    std::string Unicode_to_UTF8(LPCWSTR input) { return wchar_to_MultiByteWrapper(input, CP_UTF8); }

    std::string windows_Unicode_to_MBCS(LPCWSTR unicode_text, int use_ansi) {
        uint32_t code_page = use_ansi ? CP_ACP : CP_OEMCP;
        return wchar_to_MultiByteWrapper(unicode_text, code_page);
    }

    std::string UTF8_to_MBCS(const char* input, bool use_ansi) {
        auto unicode = UTF8_to_Unicode(input);
        return windows_Unicode_to_MBCS(unicode.c_str(), use_ansi);
    }

#endif
} // namespace core::filesystem::path_utils