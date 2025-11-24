#pragma once

#include "forward.hpp"
#include <string>
#include <variant>

namespace components::expressions {

    class key_t final {
    public:
        key_t()
            : type_(type::non)
            , side_(side_t::undefined)
            , storage_({}) {}

        key_t(key_t&& key) noexcept
            : type_{key.type_}
            , side_{key.side_}
            , storage_{std::move(key.storage_)} {}

        key_t(const key_t& key) = default;
        key_t& operator=(const key_t& key) = default;

        explicit key_t(std::string_view str, side_t side = side_t::undefined)
            : type_(type::string)
            , side_(side)
            , storage_(std::string(str.data(), str.size())) {}

        explicit key_t(const std::string& str, side_t side = side_t::undefined)
            : type_(type::string)
            , side_(side)
            , storage_(std::string(str.data(), str.size())) {}

        explicit key_t(std::string&& str, side_t side = side_t::undefined)
            : type_(type::string)
            , side_(side)
            , storage_(std::move(str)) {}

        explicit key_t(const char* str, side_t side = side_t::undefined)
            : type_(type::string)
            , side_(side)
            , storage_(std::string(str)) {}

        template<typename CharT>
        key_t(const CharT* data, size_t size, side_t side = side_t::undefined)
            : type_(type::string)
            , side_(side)
            , storage_(std::string(data, size)) {}

        explicit key_t(int32_t index, side_t side = side_t::undefined)
            : type_(type::int32)
            , side_(side)
            , storage_(index) {}

        explicit key_t(uint32_t index, side_t side = side_t::undefined)
            : type_(type::uint32)
            , side_(side)
            , storage_(index) {}

        enum class type
        {
            non,
            string,
            int32,
            uint32
        };

        auto as_int() const -> int32_t { return std::get<int32_t>(storage_); }

        auto as_uint() const -> uint32_t { return std::get<uint32_t>(storage_); }

        auto as_string() const -> const std::string& { return std::get<std::string>(storage_); }

        explicit operator std::string() const { return as_string(); }

        type which() const { return type_; }

        auto is_int() const -> bool { return type_ == type::int32; }

        auto is_uint() const -> bool { return type_ == type::uint32; }

        auto is_string() const -> bool { return type_ == type::string; }

        auto is_null() const -> bool { return type_ == type::non; }

        auto side() const -> side_t { return side_; }

        void set_side(side_t side) { side_ = side; }

        bool operator<(const key_t& other) const { return storage_ < other.storage_; }

        bool operator<=(const key_t& other) const { return storage_ <= other.storage_; }

        bool operator>(const key_t& other) const { return storage_ > other.storage_; }

        bool operator>=(const key_t& other) const { return storage_ >= other.storage_; }

        bool operator==(const key_t& other) const { return storage_ == other.storage_; }

        bool operator!=(const key_t& rhs) const { return !(*this == rhs); }

        hash_t hash() const {
            if (type_ == type::string) {
                return std::hash<std::string>()(as_string());
            } else if (type_ == type::int32) {
                return std::hash<int32_t>()(std::get<int32_t>(storage_));
            } else if (type_ == type::uint32) {
                return std::hash<uint32_t>()(std::get<uint32_t>(storage_));
            }
            return 0;
        }

    private:
        type type_;
        side_t side_;
        std::variant<std::monostate, bool, int32_t, uint32_t, std::string> storage_;
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const key_t& key) {
        if (key.is_string()) {
            stream << key.as_string();
        } else if (key.is_int()) {
            stream << key.as_int();
        } else if (key.is_uint()) {
            stream << key.as_uint();
        }
        return stream;
    }

} // namespace components::expressions