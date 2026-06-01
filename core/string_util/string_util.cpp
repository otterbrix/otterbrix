#include "string_util.hpp"

#include <algorithm>
#include <random>
#include <sstream>
#include <unordered_map>

namespace otterbrix {

std::string string_utils::GenerateRandomName(uint64_t length) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::stringstream ss;
    for (uint64_t i = 0; i < length; i++) {
        ss << "0123456789abcdef"[dis(gen)];
    }
    return ss.str();
}


// Jenkins hash function: https://en.wikipedia.org/wiki/Jenkins_hash_function
uint64_t string_utils::CIHash(const std::string &str) {
    uint32_t hash = 0;
    for (auto c : str) {
        hash += static_cast<uint32_t>(
            string_utils::CharacterToLower(static_cast<char>(c))
        );
        hash += hash << 10;
        hash ^= hash >> 6;
    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}



bool string_utils::CIEquals(const std::string &l1, const std::string &l2) {
    if (l1.length() != l2.length()) {
        return false;
    } else {
        for (size_t i = 0; i < l1.length(); i++) {
            auto low1 = string_utils::CharacterToLower(static_cast<char>(l1[i]));
            auto low2 = string_utils::CharacterToLower(static_cast<char>(l2[i]));
            if (low1 != low2) {
                return false;
            }
        }
        
    }
    return true;
}

bool string_utils::CILessThan(const std::string &l1, const std::string &l2) {
    auto min_len = std::min(l1.length(), l2.length());
    for (size_t i = 0; i < min_len; i++) {
        auto low1 = string_utils::CharacterToLower(static_cast<char>(l1[i]));
        auto low2 = string_utils::CharacterToLower(static_cast<char>(l2[i]));
        if (low1 != low2) {
            return low1 < low2;
        }    
    }
    return l1.length() < l2.length();
}

std::string string_utils::Lower(const std::string& str) {
    std::string copy(str);
    std::transform(copy.begin(), copy.end(), copy.begin(),
              [](unsigned char c) { return string_utils::CharacterToLower(static_cast<char>(c)); });
    return copy;
}

void string_utils::DeduplicateColumns(std::vector<std::string> &names) {
    std::unordered_map<std::string, uint64_t> name_map;
    for (auto &column_name : names) {   
        // put it all lower_case        
        auto low_column_name = string_utils::Lower(column_name);
        if (name_map.find(low_column_name) == name_map.end()) {
            // Name does not exist yet      
            name_map[low_column_name]++;
        } else {
            // Name already exists, we add _x where x is the repetition number
            std::string new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
            auto new_column_name_low = string_utils::Lower(new_column_name);
            while (name_map.find(new_column_name_low) != name_map.end()) {
                // This name is already here due to a previous definition
                name_map[low_column_name]++;
                new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
                new_column_name_low = string_utils::Lower(new_column_name);                                                                                                                                  
            }
            column_name = new_column_name;  
            name_map[new_column_name_low]++;
        }
    } 
} 

} // namespace otterbrix
