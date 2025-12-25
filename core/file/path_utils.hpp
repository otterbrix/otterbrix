#pragma once

#include <cstdint>

#include <stdexcept>
#include <vector>

#if defined(PLATFORM_WINDOWS)
#include <windows.h>
#endif

namespace core::filesystem::path_utils {

    std::vector<std::string> split(const std::string& str, char delimiter);
    std::vector<std::string> split(const std::string& input, const std::string& split);

    bool
    glob(const char* string, std::uint64_t slen, const char* pattern, std::uint64_t plen, bool allow_question_mark);

#ifdef PLATFORM_WINDOWS

    std::wstring UTF8_to_Unicode(const char* input);

    std::string wchar_to_MultiByteWrapper(LPCWSTR input, uint32_t code_page);

    std::string Unicode_to_UTF8(LPCWSTR input);

    std::string windows_Unicode_to_MBCS(LPCWSTR unicode_text, int use_ansi);

    std::string UTF8_to_MBCS(const char* input, bool use_ansi);

#endif

} // namespace core::filesystem::path_utils