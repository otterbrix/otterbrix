#pragma once

#include <cassert>
#include <chrono>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <msgpack.hpp>

#include "types.hpp"

namespace components::types {

    class logical_value_t {
    public:
        logical_value_t(std::pmr::memory_resource* r, logical_type type);
        logical_value_t(std::pmr::memory_resource* r, complex_logical_type type);

        template<typename T>
        logical_value_t(std::pmr::memory_resource* r, T value);
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
        create_decimal(std::pmr::memory_resource* r, int64_t value, uint8_t width, uint8_t scale);
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
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::chrono::nanoseconds value)
        : type_(to_logical_type<std::chrono::nanoseconds>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = static_cast<uint64_t>(value.count());
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::chrono::microseconds value)
        : type_(to_logical_type<std::chrono::microseconds>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = static_cast<uint64_t>(value.count());
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::chrono::milliseconds value)
        : type_(to_logical_type<std::chrono::milliseconds>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = static_cast<uint64_t>(value.count());
    }

    template<>
    inline logical_value_t::logical_value_t(std::pmr::memory_resource* r, std::chrono::seconds value)
        : type_(to_logical_type<std::chrono::seconds>())
        , resource_(r) {
        assert(type_ != logical_type::INVALID);
        data_ = static_cast<uint64_t>(value.count());
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
    inline std::chrono::nanoseconds logical_value_t::value<std::chrono::nanoseconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return static_cast<nanoseconds>(static_cast<int64_t>(data_));
            case logical_type::TIMESTAMP_US:
                return duration_cast<nanoseconds>(static_cast<microseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_MS:
                return duration_cast<nanoseconds>(static_cast<milliseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_SEC:
                return duration_cast<nanoseconds>(static_cast<seconds>(static_cast<int64_t>(data_)));
            default:
                throw std::logic_error(
                    "logical_value_t::value<std::chrono::nanoseconds>(): incorrect value logical type");
                return nanoseconds{0};
        }
    }
    template<>
    inline std::chrono::microseconds logical_value_t::value<std::chrono::microseconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return duration_cast<microseconds>(static_cast<nanoseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_US:
                return static_cast<microseconds>(static_cast<int64_t>(data_));
            case logical_type::TIMESTAMP_MS:
                return duration_cast<microseconds>(static_cast<milliseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_SEC:
                return duration_cast<microseconds>(static_cast<seconds>(static_cast<int64_t>(data_)));
            default:
                throw std::logic_error(
                    "logical_value_t::value<std::chrono::microseconds>(): incorrect value logical type");
                return microseconds{0};
        }
    }
    template<>
    inline std::chrono::milliseconds logical_value_t::value<std::chrono::milliseconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return duration_cast<milliseconds>(static_cast<nanoseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_US:
                return duration_cast<milliseconds>(static_cast<microseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_MS:
                return static_cast<milliseconds>(static_cast<int64_t>(data_));
            case logical_type::TIMESTAMP_SEC:
                return duration_cast<milliseconds>(static_cast<seconds>(static_cast<int64_t>(data_)));
            default:
                throw std::logic_error(
                    "logical_value_t::value<std::chrono::milliseconds>(): incorrect value logical type");
                return milliseconds{0};
        }
    }
    template<>
    inline std::chrono::seconds logical_value_t::value<std::chrono::seconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return duration_cast<seconds>(static_cast<nanoseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_US:
                return duration_cast<seconds>(static_cast<microseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_MS:
                return duration_cast<seconds>(static_cast<milliseconds>(static_cast<int64_t>(data_)));
            case logical_type::TIMESTAMP_SEC:
                return static_cast<seconds>(static_cast<int64_t>(data_));
            default:
                throw std::logic_error("logical_value_t::value<std::chrono::seconds>(): incorrect value logical type");
                return seconds{0};
        }
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

        const std::string& type_name() { return type_name_; }
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

        void serialize(serializer::msgpack_serializer_t* serializer) const override;
        static std::unique_ptr<logical_type_extension> deserialize(std::pmr::memory_resource* resource,
                                                                   serializer::msgpack_deserializer_t* deserializer);

    private:
        std::string catalog_;
        std::vector<logical_value_t> user_type_modifiers_;
    };

} // namespace components::types

template<typename Stream>
void to_msgpack_(msgpack::packer<Stream>& o, const components::types::logical_value_t& value) {
    switch (value.type().type()) {
        case components::types::logical_type::BOOLEAN: {
            o.pack(value.value<bool>());
            break;
        }
        case components::types::logical_type::UTINYINT: {
            o.pack(value.value<uint8_t>());
            break;
        }
        case components::types::logical_type::USMALLINT: {
            o.pack(value.value<uint16_t>());
            break;
        }
        case components::types::logical_type::UINTEGER: {
            o.pack(value.value<uint32_t>());
            break;
        }
        case components::types::logical_type::UBIGINT: {
            o.pack(value.value<uint64_t>());
            break;
        }
        case components::types::logical_type::TINYINT: {
            o.pack(value.value<int8_t>());
            break;
        }
        case components::types::logical_type::SMALLINT: {
            o.pack(value.value<int16_t>());
            break;
        }
        case components::types::logical_type::INTEGER: {
            o.pack(value.value<int32_t>());
            break;
        }
        case components::types::logical_type::BIGINT: {
            o.pack(value.value<int64_t>());
            break;
        }
        case components::types::logical_type::FLOAT: {
            o.pack(value.value<float>());
            break;
        }
        case components::types::logical_type::DOUBLE: {
            o.pack(value.value<double>());
            break;
        }
        case components::types::logical_type::STRING_LITERAL: {
            o.pack(value.value<const std::string&>());
            break;
        }
        case components::types::logical_type::NA: {
            o.pack(msgpack::type::nil_t());
            break;
        }
        default:
            throw std::logic_error("logical_value_t::to_msgpack_: incorrect logical type");
            break;
    }
}

inline void to_msgpack_(const components::types::logical_value_t& value, msgpack::object& o) {
    switch (value.type().type()) {
        case components::types::logical_type::BOOLEAN: {
            o.type = msgpack::type::BOOLEAN;
            o.via.boolean = value.value<bool>();
            break;
        }
        case components::types::logical_type::UTINYINT: {
            o.type = msgpack::type::POSITIVE_INTEGER;
            o.via.u64 = value.value<uint8_t>();
            break;
        }
        case components::types::logical_type::USMALLINT: {
            o.type = msgpack::type::POSITIVE_INTEGER;
            o.via.u64 = value.value<uint16_t>();
            break;
        }
        case components::types::logical_type::UINTEGER: {
            o.type = msgpack::type::POSITIVE_INTEGER;
            o.via.u64 = value.value<uint32_t>();
            break;
        }
        case components::types::logical_type::UBIGINT: {
            o.type = msgpack::type::POSITIVE_INTEGER;
            o.via.u64 = value.value<uint64_t>();
            break;
        }
        case components::types::logical_type::TINYINT: {
            o.type = msgpack::type::NEGATIVE_INTEGER;
            o.via.i64 = value.value<int8_t>();
            break;
        }
        case components::types::logical_type::SMALLINT: {
            o.type = msgpack::type::NEGATIVE_INTEGER;
            o.via.i64 = value.value<int16_t>();
            break;
        }
        case components::types::logical_type::INTEGER: {
            o.type = msgpack::type::NEGATIVE_INTEGER;
            o.via.i64 = value.value<int32_t>();
            break;
        }
        case components::types::logical_type::BIGINT: {
            o.type = msgpack::type::NEGATIVE_INTEGER;
            o.via.i64 = value.value<int64_t>();
            break;
        }
        case components::types::logical_type::FLOAT: {
            o.type = msgpack::type::FLOAT32;
            o.via.f64 = value.value<float>();
            break;
        }
        case components::types::logical_type::DOUBLE: {
            o.type = msgpack::type::FLOAT64;
            o.via.f64 = value.value<double>();
            break;
        }
        case components::types::logical_type::STRING_LITERAL: {
            std::string s = value.value<const std::string&>();
            o.type = msgpack::type::object_type::STR;
            o.via.str.size = uint32_t(s.size());
            o.via.str.ptr = s.c_str();
            break;
        }
        case components::types::logical_type::NA: {
            o.type = msgpack::type::object_type::NIL;
            break;
        }
        default:
            assert(false); // should be unreachable;
            break;
    }
}

namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::types::logical_value_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           const components::types::logical_value_t& v) const {
                    to_msgpack_(o, v);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::types::logical_value_t> final {
                void operator()(msgpack::object::with_zone& o, const components::types::logical_value_t& v) const {
                    to_msgpack_(v, o);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack