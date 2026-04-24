#pragma once

#include <cassert>
#include <chrono>
#include <cstring>
#include <memory>
#include <memory_resource>

#include "types.hpp"

namespace components::types {

    class logical_value_t {
    public:
        logical_value_t(std::pmr::memory_resource* r, logical_type type);
        logical_value_t(std::pmr::memory_resource* r, complex_logical_type type);

        template<typename T>
        logical_value_t(std::pmr::memory_resource* r, T value);
        logical_value_t(std::pmr::memory_resource* r, const logical_value_t& other);
        logical_value_t(const logical_value_t& other);
        logical_value_t(logical_value_t&& other) noexcept;
        logical_value_t& operator=(const logical_value_t& other);
        logical_value_t& operator=(logical_value_t&& other) noexcept;
        ~logical_value_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        const complex_logical_type& type() const noexcept;
        template<typename T>
        T value() const;
        bool is_null() const noexcept;
        logical_value_t cast_as(const complex_logical_type& type) const;
        void set_alias(const std::string& alias);

        bool operator==(const logical_value_t& rhs) const;
        bool operator!=(const logical_value_t& rhs) const;
        size_t hash() const noexcept;
        bool operator<(const logical_value_t& rhs) const;
        bool operator>(const logical_value_t& rhs) const;
        bool operator<=(const logical_value_t& rhs) const;
        bool operator>=(const logical_value_t& rhs) const;

        compare_t compare(const logical_value_t& rhs) const;

        const std::vector<logical_value_t>& children() const;

        static logical_value_t
        create_struct(std::pmr::memory_resource* r, std::string name, const std::vector<logical_value_t>& fields);
        static logical_value_t create_struct(std::pmr::memory_resource* r,
                                             const complex_logical_type& type,
                                             const std::vector<logical_value_t>& struct_values);
        static logical_value_t create_array(std::pmr::memory_resource* r,
                                            const complex_logical_type& internal_type,
                                            const std::vector<logical_value_t>& values);
        static logical_value_t
        create_numeric(std::pmr::memory_resource* r, const complex_logical_type& type, int64_t value);
        static logical_value_t
        create_enum(std::pmr::memory_resource* r, const complex_logical_type& enum_type, std::string_view key);
        static logical_value_t
        create_enum(std::pmr::memory_resource* r, const complex_logical_type& enum_type, int32_t value);
        static logical_value_t
        create_decimal(std::pmr::memory_resource* r, const complex_logical_type& decimal_type, int64_t value);
        static logical_value_t
        create_decimal(std::pmr::memory_resource* r, const complex_logical_type& decimal_type, int128_t value);
        static logical_value_t create_map(std::pmr::memory_resource* r,
                                          const complex_logical_type& key_type,
                                          const complex_logical_type& value_type,
                                          const std::vector<logical_value_t>& keys,
                                          const std::vector<logical_value_t>& values);
        static logical_value_t create_map(std::pmr::memory_resource* r,
                                          const complex_logical_type& child_type,
                                          const std::vector<logical_value_t>& values);
        static logical_value_t create_list(std::pmr::memory_resource* r,
                                           const complex_logical_type& type,
                                           const std::vector<logical_value_t>& values);
        static logical_value_t create_union(std::pmr::memory_resource* r,
                                            std::vector<complex_logical_type> types,
                                            uint8_t tag,
                                            logical_value_t value);
        static logical_value_t create_variant(std::pmr::memory_resource* r, std::vector<logical_value_t> values);

        static logical_value_t sum(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t subtract(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t mult(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t divide(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t modulus(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t exponent(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t sqr_root(const logical_value_t& value);
        static logical_value_t cube_root(const logical_value_t& value);
        static logical_value_t factorial(const logical_value_t& value);
        static logical_value_t absolute(const logical_value_t& value);
        static logical_value_t bit_and(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t bit_or(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t bit_xor(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t bit_not(const logical_value_t& value);
        static logical_value_t bit_shift_l(const logical_value_t& value1, const logical_value_t& value2);
        static logical_value_t bit_shift_r(const logical_value_t& value1, const logical_value_t& value2);

        void serialize(serializer::msgpack_serializer_t* serializer) const;
        static logical_value_t deserialize(std::pmr::memory_resource* r,
                                           serializer::msgpack_deserializer_t* deserializer);

    private:
        complex_logical_type type_;
        std::pmr::memory_resource* resource_ = nullptr;

        union {
            uint64_t data_ = 0;
            int128_t data128_;
            uint128_t udata128_;
        };

        void destroy_heap();
        std::string* str_ptr() const { return reinterpret_cast<std::string*>(data_); }
        std::vector<logical_value_t>* vec_ptr() const { return reinterpret_cast<std::vector<logical_value_t>*>(data_); }

        template<typename T, typename... Args>
        T* heap_new(Args&&... args) {
            assert(resource_);
            void* mem = resource_->allocate(sizeof(T), alignof(T));
            return new (mem) T(std::forward<Args>(args)...);
        }

        template<typename T>
        void heap_delete(T* ptr) {
            if (ptr) {
                assert(resource_);
                ptr->~T();
                resource_->deallocate(ptr, sizeof(T), alignof(T));
            }
        }
    };

    size_t hash_row(const std::pmr::vector<logical_value_t>& row) noexcept;

    static const logical_value_t NULL_LOGICAL_VALUE =
        logical_value_t{std::pmr::null_memory_resource(), complex_logical_type{logical_type::NA}};

    template<typename T>
    logical_value_t::logical_value_t(std::pmr::memory_resource* r, T value)
        : type_(to_logical_type<T>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        if constexpr (std::is_floating_point_v<T>) {
            std::memcpy(&data_, &value, sizeof(value));
        } else if constexpr (std::is_pointer_v<T>) {
            data_ = reinterpret_cast<uint64_t>(value);
        } else {
            data_ = static_cast<uint64_t>(value);
        }
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::nullptr_t)
        : type_(logical_type::NA)
        , resource_(r) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, core::date::date_t value)
        : type_(to_logical_type<core::date::date_t>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = static_cast<uint64_t>(value.value.count());
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, core::date::time_t value)
        : type_(to_logical_type<core::date::time_t>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = std::bit_cast<uint64_t>(value);
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, core::date::timetz_t value)
        : type_(to_logical_type<core::date::timetz_t>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        auto* vec = heap_new<std::vector<logical_value_t>>();
        vec->emplace_back(r, value.time.count());
        vec->emplace_back(r, value.zone.count());
        data_ = reinterpret_cast<uint64_t>(vec);
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, core::date::timestamp_t value)
        : type_(to_logical_type<core::date::timestamp_t>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = std::bit_cast<uint64_t>(value);
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, core::date::timestamptz_t value)
        : type_(to_logical_type<core::date::timestamptz_t>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = std::bit_cast<uint64_t>(value);
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, core::date::interval_t value)
        : type_(to_logical_type<core::date::interval_t>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        auto* vec = heap_new<std::vector<logical_value_t>>();
        vec->emplace_back(r, value.time.count());
        vec->emplace_back(r, value.day.count());
        vec->emplace_back(r, value.month.count());
        data_ = reinterpret_cast<uint64_t>(vec);
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, int128_t value)
        : type_(logical_type::HUGEINT)
        , resource_(r)
        , data128_(value) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, uint128_t value)
        : type_(logical_type::UHUGEINT)
        , resource_(r)
        , udata128_(value) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::string value)
        : type_(logical_type::STRING_LITERAL)
        , resource_(r)
        , data_(reinterpret_cast<uint64_t>(heap_new<std::string>(std::move(value)))) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::pmr::string value)
        : type_(logical_type::STRING_LITERAL)
        , resource_(r)
        , data_(reinterpret_cast<uint64_t>(heap_new<std::string>(value.data(), value.size()))) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::string_view value)
        : type_(logical_type::STRING_LITERAL)
        , resource_(r)
        , data_(reinterpret_cast<uint64_t>(heap_new<std::string>(value))) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, char* value)
        : type_(logical_type::STRING_LITERAL)
        , resource_(r)
        , data_(reinterpret_cast<uint64_t>(heap_new<std::string>(value))) {}

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, const char* value)
        : type_(logical_type::STRING_LITERAL)
        , resource_(r)
        , data_(reinterpret_cast<uint64_t>(heap_new<std::string>(value))) {}

    template<typename T>
    T logical_value_t::value() const {
        throw std::logic_error("logical_value_t::value<T>(): is not implemented for a given T");
    }

    template<>
    inline bool logical_value_t::value<bool>() const {
        return static_cast<bool>(data_);
    }
    template<>
    inline uint8_t logical_value_t::value<uint8_t>() const {
        return static_cast<uint8_t>(data_);
    }
    template<>
    inline int8_t logical_value_t::value<int8_t>() const {
        return static_cast<int8_t>(data_);
    }
    template<>
    inline uint16_t logical_value_t::value<uint16_t>() const {
        return static_cast<uint16_t>(data_);
    }
    template<>
    inline int16_t logical_value_t::value<int16_t>() const {
        return static_cast<int16_t>(data_);
    }
    template<>
    inline uint32_t logical_value_t::value<uint32_t>() const {
        return static_cast<uint32_t>(data_);
    }
    template<>
    inline int32_t logical_value_t::value<int32_t>() const {
        return static_cast<int32_t>(data_);
    }
    template<>
    inline uint64_t logical_value_t::value<uint64_t>() const {
        return data_;
    }
    template<>
    inline int64_t logical_value_t::value<int64_t>() const {
        return static_cast<int64_t>(data_);
    }
    template<>
    inline uint128_t logical_value_t::value<uint128_t>() const {
        return udata128_;
    }
    template<>
    inline int128_t logical_value_t::value<int128_t>() const {
        return data128_;
    }
    template<>
    inline float logical_value_t::value<float>() const {
        float f;
        uint32_t bits = static_cast<uint32_t>(data_);
        std::memcpy(&f, &bits, sizeof(f));
        return f;
    }
    template<>
    inline double logical_value_t::value<double>() const {
        double d;
        std::memcpy(&d, &data_, sizeof(d));
        return d;
    }
    template<>
    inline core::date::date_t logical_value_t::value<core::date::date_t>() const {
        return {core::date::days{static_cast<int32_t>(data_)}};
    }
    template<>
    inline core::date::time_t logical_value_t::value<core::date::time_t>() const {
        return std::bit_cast<core::date::time_t>(data_);
    }
    template<>
    inline core::date::timetz_t logical_value_t::value<core::date::timetz_t>() const {
        auto& ch = *vec_ptr();
        return {core::date::microseconds{ch[0].value<int64_t>()}, core::date::seconds_i32{ch[1].value<int32_t>()}};
    }
    template<>
    inline core::date::timestamp_t logical_value_t::value<core::date::timestamp_t>() const {
        return std::bit_cast<core::date::timestamp_t>(data_);
    }
    template<>
    inline core::date::timestamptz_t logical_value_t::value<core::date::timestamptz_t>() const {
        return std::bit_cast<core::date::timestamptz_t>(data_);
    }
    template<>
    inline core::date::interval_t logical_value_t::value<core::date::interval_t>() const {
        auto& ch = *vec_ptr();
        return {core::date::microseconds{ch[0].value<int64_t>()},
                core::date::days{ch[1].value<int32_t>()},
                core::date::months{ch[2].value<int32_t>()}};
    }
    template<>
    inline void* logical_value_t::value<void*>() const {
        return reinterpret_cast<void*>(data_);
    }
    template<>
    inline std::string* logical_value_t::value<std::string*>() const {
        return str_ptr();
    }
    template<>
    inline const std::string& logical_value_t::value<const std::string&>() const {
        return *str_ptr();
    }
    template<>
    inline std::string_view logical_value_t::value<std::string_view>() const {
        return *str_ptr();
    }
    template<>
    inline std::vector<logical_value_t>* logical_value_t::value<std::vector<logical_value_t>*>() const {
        return vec_ptr();
    }

    class enum_logical_type_extension : public logical_type_extension {
    public:
        explicit enum_logical_type_extension(std::string name, std::vector<logical_value_t> entries);

        const std::string& type_name() const { return type_name_; }
        const std::vector<logical_value_t>& entries() const noexcept { return entries_; }

        void serialize(serializer::msgpack_serializer_t* serializer) const override;
        static std::unique_ptr<logical_type_extension> deserialize(std::pmr::memory_resource* resource,
                                                                   serializer::msgpack_deserializer_t* deserializer);

    private:
        std::string type_name_;
        std::vector<logical_value_t> entries_; // integer literal for value and alias for entry name
    };

    class user_logical_type_extension : public logical_type_extension {
    public:
        explicit user_logical_type_extension(std::string catalog, std::vector<logical_value_t> user_type_modifiers);

        const std::string& catalog() const noexcept { return catalog_; }
        const std::vector<logical_value_t>& user_type_modifiers() const noexcept { return user_type_modifiers_; }

        void serialize(serializer::msgpack_serializer_t* serializer) const override;
        static std::unique_ptr<logical_type_extension> deserialize(std::pmr::memory_resource* resource,
                                                                   serializer::msgpack_deserializer_t* deserializer);

    private:
        std::string catalog_;
        std::vector<logical_value_t> user_type_modifiers_;
    };

} // namespace components::types