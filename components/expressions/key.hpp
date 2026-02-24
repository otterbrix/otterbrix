#pragma once

#include "forward.hpp"
#include <boost/container_hash/hash.hpp>
#include <core/pmr.hpp>
#include <string>
#include <vector>

namespace components::expressions {

    class key_t final {
    public:
        explicit key_t(std::pmr::memory_resource* resource)
            : side_(side_t::undefined)
            , storage_(resource)
            , path_(resource) {}

        key_t(key_t&& key) noexcept
            : side_{key.side_}
            , storage_{std::move(key.storage_)}
            , path_{std::move(key.path_)} {}

        key_t(const key_t& key) = default;
        key_t& operator=(const key_t& key) = default;

        explicit key_t(std::pmr::vector<std::pmr::string> str_vector, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::move(str_vector))
            , path_(storage_.get_allocator().resource()) {}

        explicit key_t(std::pmr::memory_resource* resource, std::string_view str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str.data(), str.size(), resource)}, resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource,
                       const std::pmr::string& str,
                       side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str.data(), str.size(), resource)}, resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource, std::pmr::string&& str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::move(str)}, resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource, const char* str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str, resource)}, resource)
            , path_(resource) {}

        template<typename CharT>
        key_t(std::pmr::memory_resource* resource, const CharT* data, size_t size, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(data, size, resource)}, resource)
            , path_(resource) {}

        [[nodiscard]] auto as_pmr_string() const -> std::pmr::string {
            std::pmr::string result(resource());
            bool separator = false;
            for (const auto& str : storage_) {
                if (separator) {
                    result += "/";
                }
                result += str;
                separator = true;
            }
            return result;
        }

        [[nodiscard]] auto as_string() const -> std::string {
            std::string result;
            bool separator = false;
            for (const auto& str : storage_) {
                if (separator) {
                    result += "/";
                }
                result += str;
                separator = true;
            }
            return result;
        }

        explicit operator std::pmr::string() const { return as_pmr_string(); }
        explicit operator std::string() const { return as_string(); }

        auto storage() -> std::pmr::vector<std::pmr::string>& { return storage_; }

        auto storage() const -> const std::pmr::vector<std::pmr::string>& { return storage_; }

        auto path() -> std::pmr::vector<size_t>& { return path_; }

        auto path() const -> const std::pmr::vector<size_t>& { return path_; }

        void set_path(std::pmr::vector<size_t> path) { path_ = std::move(path); }

        auto is_null() const -> bool { return storage_.empty(); }

        auto side() const -> side_t { return side_; }

        void set_side(side_t side) { side_ = side; }

        bool operator<(const key_t& other) const { return storage_ < other.storage_; }

        bool operator<=(const key_t& other) const { return storage_ <= other.storage_; }

        bool operator>(const key_t& other) const { return storage_ > other.storage_; }

        bool operator>=(const key_t& other) const { return storage_ >= other.storage_; }

        bool operator==(const key_t& other) const { return storage_ == other.storage_; }

        bool operator!=(const key_t& rhs) const { return !(*this == rhs); }

        hash_t hash() const {
            hash_t hash_{0};
            for (const auto& str : storage_) {
                boost::hash_combine(hash_, std::hash<std::pmr::string>()(str));
            }
            return hash_;
        }

        std::pmr::memory_resource* resource() const { return storage_.get_allocator().resource(); }

    private:
        side_t side_;
        std::pmr::vector<std::pmr::string> storage_;
        std::pmr::vector<size_t> path_;
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const key_t& key) {
        stream << key.as_string();
        return stream;
    }

} // namespace components::expressions