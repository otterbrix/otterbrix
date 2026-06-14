#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace otterbrix {

// string_utils as std
namespace string_utils {

	inline char character_to_lower(char c) {
		if (c >= 'A' && c <= 'Z') {
			return static_cast<char>(c + ('a' - 'A'));
		}
		return c;
	}

	//! Case insensitive hash
	uint64_t ci_hash(const std::string &str);

	//! Case insensitive equals
	bool ci_equals(const std::string &l1, const std::string &l2);

    std::string lower(const std::string& str);

    void deduplicate_columns(std::vector<std::string> &names);

} // namespace string_utils

} // namespace otterbrix

