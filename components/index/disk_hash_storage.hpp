#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace components::index {

    class disk_hash_storage_t {
    public:
        struct value_ref_t {
            int64_t value{0};
            uint32_t log_file_id{0};
            uint64_t log_offset{0};
            bool key_truncated{false};
        };

        using full_key_loader_t = std::function<bool(uint32_t, uint64_t, std::string&)>;

        virtual ~disk_hash_storage_t() = default;

        virtual bool put(std::string_view key,
                         int64_t value,
                         uint32_t log_file_id,
                         uint64_t log_offset,
                         const full_key_loader_t& key_loader = {}) = 0;
        virtual std::optional<value_ref_t> get(std::string_view key, const full_key_loader_t& key_loader = {}) const = 0;
        virtual std::vector<value_ref_t> get_all(std::string_view key, const full_key_loader_t& key_loader = {}) const = 0;
        virtual bool erase(std::string_view key, const full_key_loader_t& key_loader = {}) = 0;
        virtual bool erase(std::string_view key, int64_t value, const full_key_loader_t& key_loader = {}) = 0;
        virtual void sync() = 0;

    };

} // namespace components::index
