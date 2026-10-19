#pragma once

#include <cassert>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <type_traits>

#include <components/types/types.hpp>

namespace components::vector {
    class vector_t;
} // namespace components::vector

namespace components::table::storage {
    class metadata_writer_t;
    class metadata_reader_t;
} // namespace components::table::storage

namespace components::table {

    class base_statistics_t {
        enum class slot : uint8_t
        {
            none,
            signed_int,
            unsigned_int,
            real,
            str
        };

        template<typename T>
        static constexpr slot slot_of() {
            if constexpr (std::is_same_v<T, std::string_view>) {
                return slot::str;
            } else if constexpr (std::is_floating_point_v<T>) {
                return slot::real;
            } else if constexpr (std::is_same_v<T, types::uint128_t>) {
                return slot::unsigned_int;
            } else if constexpr (std::is_same_v<T, types::int128_t>) {
                return slot::signed_int;
            } else if constexpr (std::is_same_v<T, bool>) {
                return slot::signed_int;
            } else if constexpr (std::is_unsigned_v<T>) {
                return slot::unsigned_int;
            } else {
                return slot::signed_int;
            }
        }

        static constexpr slot slot_of_type(types::logical_type type) {
            switch (type) {
                case types::logical_type::BOOLEAN:
                case types::logical_type::TINYINT:
                case types::logical_type::SMALLINT:
                case types::logical_type::INTEGER:
                case types::logical_type::BIGINT:
                case types::logical_type::HUGEINT:
                case types::logical_type::DECIMAL:
                    return slot::signed_int;
                case types::logical_type::UTINYINT:
                case types::logical_type::USMALLINT:
                case types::logical_type::UINTEGER:
                case types::logical_type::UBIGINT:
                case types::logical_type::UHUGEINT:
                    return slot::unsigned_int;
                case types::logical_type::FLOAT:
                case types::logical_type::DOUBLE:
                    return slot::real;
                case types::logical_type::STRING_LITERAL:
                    return slot::str;
                default:
                    return slot::none;
            }
        }

    public:
        explicit base_statistics_t(std::pmr::memory_resource* resource);

        base_statistics_t(std::pmr::memory_resource* resource, types::logical_type type);

        // has_stats() without bounds: the null count is known, min/max are not.
        base_statistics_t(std::pmr::memory_resource* resource, types::logical_type type, uint64_t null_count);

        base_statistics_t(const base_statistics_t& other);
        base_statistics_t& operator=(const base_statistics_t& other);
        base_statistics_t(base_statistics_t&& other) noexcept = default;
        base_statistics_t& operator=(base_statistics_t&& other);

        bool has_bounds() const { return has_bounds_; }

        // Bool, signed integers, HUGEINT and the raw scaled integer of a DECIMAL live in the int128 slot,
        // unsigned integers and UHUGEINT in the uint128 slot, FLOAT/DOUBLE in the double slot.
        template<typename T>
        T min_as() const {
            assert(has_bounds_ && slot_of<T>() == slot_of_type(type_));
            if constexpr (slot_of<T>() == slot::signed_int) {
                return static_cast<T>(min_i_);
            } else if constexpr (slot_of<T>() == slot::unsigned_int) {
                return static_cast<T>(min_u_);
            } else {
                return static_cast<T>(min_d_);
            }
        }

        template<typename T>
        T max_as() const {
            assert(has_bounds_ && slot_of<T>() == slot_of_type(type_));
            if constexpr (slot_of<T>() == slot::signed_int) {
                return static_cast<T>(max_i_);
            } else if constexpr (slot_of<T>() == slot::unsigned_int) {
                return static_cast<T>(max_u_);
            } else {
                return static_cast<T>(max_d_);
            }
        }

        std::string_view min_string() const {
            assert(has_bounds_ && type_ == types::logical_type::STRING_LITERAL);
            return min_s_;
        }
        std::string_view max_string() const {
            assert(has_bounds_ && type_ == types::logical_type::STRING_LITERAL);
            return max_s_;
        }

        uint8_t decimal_width() const { return decimal_width_; }
        uint8_t decimal_scale() const { return decimal_scale_; }

        uint64_t null_count() const { return null_count_; }
        types::logical_type type() const { return type_; }
        bool has_stats() const { return has_stats_; }

        template<typename T>
        void set_bounds(T min, T max) {
            assert(slot_of<T>() == slot_of_type(type_) && type_ != types::logical_type::DECIMAL);
            if constexpr (slot_of<T>() == slot::signed_int) {
                using wide_t = std::conditional_t<std::is_same_v<T, types::int128_t>, types::int128_t, int64_t>;
                min_i_ = static_cast<types::int128_t>(static_cast<wide_t>(min));
                max_i_ = static_cast<types::int128_t>(static_cast<wide_t>(max));
            } else if constexpr (slot_of<T>() == slot::unsigned_int) {
                using wide_t = std::conditional_t<std::is_same_v<T, types::uint128_t>, types::uint128_t, uint64_t>;
                min_u_ = static_cast<types::uint128_t>(static_cast<wide_t>(min));
                max_u_ = static_cast<types::uint128_t>(static_cast<wide_t>(max));
            } else if constexpr (slot_of<T>() == slot::real) {
                min_d_ = static_cast<double>(min);
                max_d_ = static_cast<double>(max);
            } else {
                min_s_.assign(min);
                max_s_.assign(max);
            }
            has_bounds_ = true;
            has_stats_ = true;
        }

        void set_decimal_bounds(uint8_t width, uint8_t scale, types::int128_t min_raw, types::int128_t max_raw);

        void set_null_count(uint64_t count) { null_count_ = count; }

        void merge(const base_statistics_t& other);
        void update(vector::vector_t& vec, uint64_t offset, uint64_t count);

        void serialize(storage::metadata_writer_t& writer) const;
        static base_statistics_t deserialize(std::pmr::memory_resource* resource, storage::metadata_reader_t& reader);

    private:
        std::pmr::memory_resource* resource_;
        types::logical_type type_;
        types::int128_t min_i_{0};
        types::int128_t max_i_{0};
        types::uint128_t min_u_{0};
        types::uint128_t max_u_{0};
        double min_d_{0};
        double max_d_{0};
        std::pmr::string min_s_;
        std::pmr::string max_s_;
        uint8_t decimal_width_{0};
        uint8_t decimal_scale_{0};
        uint64_t null_count_{0};
        bool has_stats_{false};
        bool has_bounds_{false};
    };

} // namespace components::table
