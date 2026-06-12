#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace components::index {

    class disk_hash_storage_t : public boost::intrusive_ref_counter<disk_hash_storage_t> {
    public:
        struct value_ref_t {
            int64_t value{0};
            uint32_t log_file_id{0};
            uint64_t log_offset{0};
            bool key_truncated{false};
        };

        using full_key_loader_t = std::function<bool(uint32_t, uint64_t, std::string&, bool lock_bitcask)>;

        virtual ~disk_hash_storage_t() = default;

        virtual bool put(std::string_view key,
                         int64_t value,
                         uint32_t log_file_id,
                         uint64_t log_offset) = 0;
        virtual std::optional<value_ref_t> get(std::string_view key, bool lock_bitcask = true) const = 0;
        virtual std::vector<value_ref_t> get_all(std::string_view key) const = 0;
        virtual bool erase(std::string_view key, bool lock_bitcask = true) = 0;
        virtual bool erase(std::string_view key, int64_t value, bool lock_bitcask = true) = 0;
        virtual void sync() = 0;
    };

    using disk_hash_storage_ptr = boost::intrusive_ptr<disk_hash_storage_t>;

} // namespace components::index
