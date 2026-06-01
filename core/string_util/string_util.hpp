#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <utility>

namespace otterbrix {

#ifndef OTTERBRIX_QUOTE_DEFINE
// Preprocessor trick to allow text to be converted to C-string / string
// Expecte use is:
//	#ifdef SOME_DEFINE
//	string str = DUCKDB_QUOTE_DEFINE(SOME_DEFINE)
//	...do something with str
//	#endif SOME_DEFINE
#define OTTERBRIX_QUOTE_DEFINE_IMPL(x) #x
#define OTTERBRIX_QUOTE_DEFINE(x)      OTTERBRIX_QUOTE_DEFINE_IMPL(x)
#endif

// string_utils as std
namespace string_utils {

	static char CharacterToLower(char c) {
		if (c >= 'A' && c <= 'Z') {
			return static_cast<char>(c + ('a' - 'A'));
		}
		return c;
	}

    std::string GenerateRandomName(uint64_t length = 16);

	//! Case insensitive hash
	uint64_t CIHash(const std::string &str);

	//! Case insensitive equals
	bool CIEquals(const std::string &l1, const std::string &l2);

	bool CILessThan(const std::string &l1, const std::string &l2);

    std::string Lower(const std::string& str);

    void DeduplicateColumns(std::vector<std::string> &names);

} // namespace string_utils

} // namespace otterbrix

