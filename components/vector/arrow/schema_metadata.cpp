#include "schema_metadata.hpp"

#include <algorithm>
#include <cstring>
#include <string>

namespace components::vector::arrow {

    arrow_schema_metadata_t::arrow_schema_metadata_t(const char* metadata) {
        if (metadata) {
            int32_t num_pairs;
            std::memcpy(&num_pairs, metadata, sizeof(int32_t));
            metadata += sizeof(int32_t);

            for (int32_t i = 0; i < num_pairs; ++i) {
                int32_t key_length;
                std::memcpy(&key_length, metadata, sizeof(int32_t));
                metadata += sizeof(int32_t);

                std::string key(metadata, static_cast<uint64_t>(key_length));
                metadata += key_length;

                int32_t value_length;
                std::memcpy(&value_length, metadata, sizeof(int32_t));
                metadata += sizeof(int32_t);

                const std::string value(metadata, static_cast<uint64_t>(value_length));
                metadata += value_length;
                metadata_map_[key] = value;
            }
        }
    }

    void arrow_schema_metadata_t::add_option(const std::string& key, const std::string& value) {
        metadata_map_[key] = value;
    }

    std::string arrow_schema_metadata_t::get_option(const std::string& key) const {
        auto it = metadata_map_.find(key);
        if (it != metadata_map_.end()) {
            return it->second;
        } else {
            return "";
        }
    }

    std::string arrow_schema_metadata_t::extension_name() const { return get_option(ARROW_EXTENSION_NAME); }

    bool arrow_schema_metadata_t::has_extension() {
        auto arrow_extension = get_option(arrow_schema_metadata_t::ARROW_EXTENSION_NAME);
        auto start_with = [](const std::string& prefix, const std::string& word) {
            return word.length() >= prefix.length() &&
                   std::mismatch(prefix.begin(), prefix.end(), word.begin()).first == prefix.end();
        };

        return !arrow_extension.empty() && !start_with("ogc", arrow_extension);
    }

    std::unique_ptr<char[]> arrow_schema_metadata_t::serialize_metadata() const {
        uint64_t total_size = sizeof(int32_t);
        for (const auto& option : metadata_map_) {
            total_size += 2 * sizeof(int32_t);
            total_size += option.first.size();
            total_size += option.second.size();
        }
        auto metadata_array_ptr = std::make_unique<char[]>(total_size);
        auto metadata_ptr = metadata_array_ptr.get();
        const uint64_t map_size = metadata_map_.size();
        std::memcpy(metadata_ptr, &map_size, sizeof(int32_t));
        metadata_ptr += sizeof(int32_t);
        for (const auto& pair : metadata_map_) {
            const std::string& key = pair.first;
            uint64_t key_size = key.size();

            std::memcpy(metadata_ptr, &key_size, sizeof(int32_t));
            metadata_ptr += sizeof(int32_t);

            std::memcpy(metadata_ptr, key.c_str(), key_size);
            metadata_ptr += key_size;
            const std::string& value = pair.second;
            const uint64_t value_size = value.size();

            std::memcpy(metadata_ptr, &value_size, sizeof(int32_t));
            metadata_ptr += sizeof(int32_t);

            std::memcpy(metadata_ptr, value.c_str(), value_size);
            metadata_ptr += value_size;
        }
        return metadata_array_ptr;
    }

    arrow_schema_metadata_t arrow_schema_metadata_t::arrow_canonical_type(const std::string& extension_name) {
        arrow_schema_metadata_t metadata;
        metadata.add_option(arrow_schema_metadata_t::ARROW_EXTENSION_NAME, extension_name);
        metadata.add_option(arrow_schema_metadata_t::ARROW_METADATA_KEY, "");
        return metadata;
    }

    arrow_schema_metadata_t arrow_schema_metadata_t::non_canonical_type(const std::string& type_name,
                                                                        const std::string& vendor_name) {
        arrow_schema_metadata_t metadata;
        metadata.add_option(ARROW_EXTENSION_NAME, arrow_extension_metadata_t::ARROW_EXTENSION_NON_CANONICAL);
        return metadata;
    }

} // namespace components::vector::arrow
