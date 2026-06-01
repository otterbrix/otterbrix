#include "schema_metadata.hpp"

#include <algorithm>
#include <cstring>
#include <string>

namespace components::arrow {
    
    ArrowSchemaMetadata::ArrowSchemaMetadata(const char *metadata) {
    	if (metadata) {
    		// Read the number of key-value pairs (int32)
    		int32_t num_pairs;
            std::memcpy(&num_pairs, metadata, sizeof(int32_t));
    		metadata += sizeof(int32_t);
    
    		// Loop through each key-value pair
    		for (int32_t i = 0; i < num_pairs; ++i) {
    			// Read the length of the key (int32)
    			int32_t key_length;
                std::memcpy(&key_length, metadata, sizeof(int32_t));
    			metadata += sizeof(int32_t);
    
    			// Read the key
    			std::string key(metadata, static_cast<uint64_t>(key_length));
    			metadata += key_length;
    
    			// Read the length of the value (int32)
    			int32_t value_length;
                std::memcpy(&value_length, metadata, sizeof(int32_t));
    			metadata += sizeof(int32_t);
    
    			// Read the value
    			const std::string value(metadata, static_cast<uint64_t>(value_length));
    			metadata += value_length;
    			metadata_map[key] = value;
    		}
    	}
    }
    
    void ArrowSchemaMetadata::AddOption(const std::string &key, const std::string &value) {
    	metadata_map[key] = value;
    }
    std::string ArrowSchemaMetadata::GetOption(const std::string &key) const {
    	auto it = metadata_map.find(key);
    	if (it != metadata_map.end()) {
    		return it->second;
    	} else {
    		return "";
    	}
    }
    
    std::string ArrowSchemaMetadata::GetExtensionName() const {
    	return GetOption(ARROW_EXTENSION_NAME);
    }
    
    ArrowSchemaMetadata ArrowSchemaMetadata::MetadataFromName(const std::string &extension_name) {
    	ArrowSchemaMetadata metadata;
    	metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, extension_name);
    	metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, "");
    	return metadata;
    }
    
    bool ArrowSchemaMetadata::HasExtension() {
    	auto arrow_extension = GetOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME);
    	// FIXME: We are currently ignoring the ogc extensions
        auto start_with = [](const std::string& prefix, const std::string& word) {
            return word.length() >= prefix.length() &&
                std::mismatch(prefix.begin(), prefix.end(), 
                        word.begin()).first == prefix.end();
        };

    	return !arrow_extension.empty() && !start_with("ogc", arrow_extension);
    }
    
    std::unique_ptr<char[]> ArrowSchemaMetadata::SerializeMetadata() const {
    	// First we have to figure out the total size:
    	// 1. number of key-value pairs (int32)
    	uint64_t total_size = sizeof(int32_t);
    	for (const auto &option : metadata_map) {
    		// 2. Length of the key and value (2 * int32)
    		total_size += 2 * sizeof(int32_t);
    		// 3. Length of key
    		total_size += option.first.size();
    		// 4. Length of value
    		total_size += option.second.size();
    	}
    	auto metadata_array_ptr = std::make_unique<char[]>(total_size);
    	auto metadata_ptr = metadata_array_ptr.get();
    	// 1. number of key-value pairs (int32)
    	const uint64_t map_size = metadata_map.size();
        std::memcpy(metadata_ptr, &map_size, sizeof(int32_t));
    	metadata_ptr += sizeof(int32_t);
    	// Iterate through each key-value pair in the map
    	for (const auto &pair : metadata_map) {
    		const std::string &key = pair.first;
    		uint64_t key_size = key.size();
    		// Length of the key (int32)
            std::memcpy(metadata_ptr, &key_size, sizeof(int32_t));
    		metadata_ptr += sizeof(int32_t);
    		// Key
            std::memcpy(metadata_ptr, key.c_str(), key_size);
    		metadata_ptr += key_size;
    		const std::string &value = pair.second;
    		const uint64_t value_size = value.size();
    		// Length of the value (int32)
            std::memcpy(metadata_ptr, &value_size, sizeof(int32_t));
    		metadata_ptr += sizeof(int32_t);
    		// Value
            std::memcpy(metadata_ptr, value.c_str(), value_size);
    		metadata_ptr += value_size;
    	}
    	return metadata_array_ptr;
    }
} // namespace components::arrow
