#pragma once

#include "forward.hpp"
#include <string>
#include <variant>

namespace components::expressions {

    class key_t final {
    public:
        key_t()
            : side_(side_t::undefined)
            , storage_({}) {}

        key_t(key_t&& key) noexcept
            : side_{key.side_}
            , storage_{std::move(key.storage_)} {}

        key_t(const key_t& key) = default;
        key_t& operator=(const key_t& key) = default;

        explicit key_t(std::string_view str, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::string(str.data(), str.size())) {}

        explicit key_t(const std::string& str, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::string(str.data(), str.size())) {}

        explicit key_t(std::string&& str, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::move(str)) {}

        explicit key_t(const char* str, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::string(str)) {}

        template<typename CharT>
        key_t(const CharT* data, size_t size, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::string(data, size)) {}

        auto as_string() const -> const std::string& { return storage_; }

        explicit operator std::string() const { return as_string(); }

        auto is_null() const -> bool { return storage_.empty(); }

        auto side() const -> side_t { return side_; }

        void set_side(side_t side) { side_ = side; }

        bool operator<(const key_t& other) const { return storage_ < other.storage_; }

        bool operator<=(const key_t& other) const { return storage_ <= other.storage_; }

        bool operator>(const key_t& other) const { return storage_ > other.storage_; }

        bool operator>=(const key_t& other) const { return storage_ >= other.storage_; }

        bool operator==(const key_t& other) const { return storage_ == other.storage_; }

        bool operator!=(const key_t& rhs) const { return !(*this == rhs); }

        hash_t hash() const { return std::hash<std::string>()(storage_); }

    private:
        side_t side_;
        std::string storage_;
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const key_t& key) {
        stream << key.as_string();
        return stream;
    }

} // namespace components::expressions