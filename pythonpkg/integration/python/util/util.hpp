#pragma once 

#include <pybind11/pybind_wrapper.hpp>
#include <components/document/value.hpp>
#include <components/types/logical_value.hpp>
#include <core/types/string.hpp>
#include <core/typedefs.hpp>

#include <absl/numeric/int128.h>

namespace otterbrix {
    namespace util {
        string DocumentValueToString(const components::document::value_t& value);
        
        string LogicalValueToString(const components::types::logical_value_t& value);


        template<class T>
        T ParseToNumeric(const string& numeric_string) {
        	bool is_neg = false;
        	idx_t i = 0;
        	T res = 0;
        	if (numeric_string.length() > 0 && numeric_string[0] == '-') {
        		is_neg = true;
        		i++;
        	}
        	if (numeric_string.length() > 0 && numeric_string[0] == '+') {
        		i++;
        	}
        	for (; i < numeric_string.length(); i++) {
        		res *= 10;
        		res += (numeric_string[i] - '0');
        	}
        	if (is_neg) {
        		return -res;
        	} 
        	return res;
        }

        string ParseNumericToString(absl::int128 num);
    } // namespace util
} // namespace otterbrix
