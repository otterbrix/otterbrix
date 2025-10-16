#pragma once

#include <cassert>
#include <chrono>
#include <memory>
#include <msgpack.hpp>
#include <variant>

#include "types.hpp"

namespace components::types {

    class logical_value_t {
    public:
        explicit logical_value_t(complex_logical_type type = logical_type::NA);

        template<typename T>
        explicit logical_value_t(T value);
        logical_value_t(const logical_value_t& other);
        logical_value_t(logical_value_t&& other) noexcept;
        logical_value_t& operator=(const logical_value_t& other);
        logical_value_t& operator=(logical_value_t&& other) noexcept;
        ~logical_value_t() = default;

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

        static logical_value_t create_struct(const std::vector<logical_value_t>& fields);
        static logical_value_t create_struct(const complex_logical_type& type,
                                             const std::vector<logical_value_t>& struct_values);
        static logical_value_t create_array(const complex_logical_type& internal_type,
                                            const std::vector<logical_value_t>& values);
        static logical_value_t create_numeric(const complex_logical_type& type, int64_t value);
        static logical_value_t create_decimal(int64_t value, uint8_t width, uint8_t scale);
        static logical_value_t create_map(const complex_logical_type& key_type,
                                          const complex_logical_type& value_type,
                                          const std::vector<logical_value_t>& keys,
                                          const std::vector<logical_value_t>& values);
        static logical_value_t create_map(const complex_logical_type& child_type,
                                          const std::vector<logical_value_t>& values);
        static logical_value_t create_list(const complex_logical_type& type,
                                           const std::vector<logical_value_t>& values);
        static logical_value_t
        create_union(std::vector<complex_logical_type> types, uint8_t tag, logical_value_t value);

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

    private:
        complex_logical_type type_;

        std::variant<nullptr_t,
                     bool,
                     int8_t,
                     int16_t,
                     int32_t,
                     int64_t,
                     uint8_t,
                     uint16_t,
                     uint32_t,
                     uint64_t,
                     float,
                     double,
                     void*,

                     // everything bigger then 8 bytes or has no fixed size is allocated on the heap

                     std::unique_ptr<int128_t>,
                     std::unique_ptr<uint128_t>,
                     std::unique_ptr<std::string>,
                     std::unique_ptr<std::vector<logical_value_t>> // nested
                     >
            value_;
    };

    template<typename T>
    logical_value_t::logical_value_t(T value)
        : type_(to_logical_type<T>())
        , value_(value) {
        assert(type_ != logical_type::INVALID);
    }

    template<>
    inline logical_value_t::logical_value_t(std::chrono::nanoseconds value)
        : type_(to_logical_type<std::chrono::nanoseconds>())
        , value_(value.count()) {
        assert(type_ != logical_type::INVALID);
    }

    template<>
    inline logical_value_t::logical_value_t(std::chrono::microseconds value)
        : type_(to_logical_type<std::chrono::microseconds>())
        , value_(value.count()) {
        assert(type_ != logical_type::INVALID);
    }

    template<>
    inline logical_value_t::logical_value_t(std::chrono::milliseconds value)
        : type_(to_logical_type<std::chrono::milliseconds>())
        , value_(value.count()) {
        assert(type_ != logical_type::INVALID);
    }

    template<>
    inline logical_value_t::logical_value_t(std::chrono::seconds value)
        : type_(to_logical_type<std::chrono::seconds>())
        , value_(value.count()) {
        assert(type_ != logical_type::INVALID);
    }

    template<>
    inline logical_value_t::logical_value_t(int128_t value)
        : type_(logical_type::HUGEINT)
        , value_(std::make_unique<int128_t>(std::move(value))) {}

    template<>
    inline logical_value_t::logical_value_t(uint128_t value)
        : type_(logical_type::UHUGEINT)
        , value_(std::make_unique<uint128_t>(std::move(value))) {}

    template<>
    inline logical_value_t::logical_value_t(std::string value)
        : type_(logical_type::STRING_LITERAL)
        , value_(std::make_unique<std::string>(std::move(value))) {}

    template<>
    inline logical_value_t::logical_value_t(std::string_view value)
        : type_(logical_type::STRING_LITERAL)
        , value_(std::make_unique<std::string>(std::move(value))) {}

    template<typename T>
    T logical_value_t::value() const {
        assert(false);
    }

    template<>
    inline bool logical_value_t::value<bool>() const {
        return std::get<bool>(value_);
    }
    template<>
    inline uint8_t logical_value_t::value<uint8_t>() const {
        return std::get<uint8_t>(value_);
    }
    template<>
    inline int8_t logical_value_t::value<int8_t>() const {
        return std::get<int8_t>(value_);
    }
    template<>
    inline uint16_t logical_value_t::value<uint16_t>() const {
        return std::get<uint16_t>(value_);
    }
    template<>
    inline int16_t logical_value_t::value<int16_t>() const {
        return std::get<int16_t>(value_);
    }
    template<>
    inline uint32_t logical_value_t::value<uint32_t>() const {
        return std::get<uint32_t>(value_);
    }
    template<>
    inline int32_t logical_value_t::value<int32_t>() const {
        return std::get<int32_t>(value_);
    }
    template<>
    inline uint64_t logical_value_t::value<uint64_t>() const {
        return std::get<uint64_t>(value_);
    }
    template<>
    inline int64_t logical_value_t::value<int64_t>() const {
        return std::get<int64_t>(value_);
    }
    template<>
    inline uint128_t logical_value_t::value<uint128_t>() const {
        return *std::get<std::unique_ptr<uint128_t>>(value_);
    }
    template<>
    inline int128_t logical_value_t::value<int128_t>() const {
        return *std::get<std::unique_ptr<int128_t>>(value_);
    }
    template<>
    inline float logical_value_t::value<float>() const {
        return std::get<float>(value_);
    }
    template<>
    inline double logical_value_t::value<double>() const {
        return std::get<double>(value_);
    }
    template<>
    inline std::chrono::nanoseconds logical_value_t::value<std::chrono::nanoseconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return static_cast<nanoseconds>(std::get<int64_t>(value_));
            case logical_type::TIMESTAMP_US:
                return duration_cast<nanoseconds>(static_cast<microseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_MS:
                return duration_cast<nanoseconds>(static_cast<milliseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_SEC:
                return duration_cast<nanoseconds>(static_cast<seconds>(std::get<int64_t>(value_)));
            default:
                assert(false && "incorrect value logical type");
                return nanoseconds{0};
        }
    }
    template<>
    inline std::chrono::microseconds logical_value_t::value<std::chrono::microseconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return duration_cast<microseconds>(static_cast<nanoseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_US:
                return static_cast<microseconds>(std::get<int64_t>(value_));
            case logical_type::TIMESTAMP_MS:
                return duration_cast<microseconds>(static_cast<milliseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_SEC:
                return duration_cast<microseconds>(static_cast<seconds>(std::get<int64_t>(value_)));
            default:
                assert(false && "incorrect value logical type");
                return microseconds{0};
        }
    }
    template<>
    inline std::chrono::milliseconds logical_value_t::value<std::chrono::milliseconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return duration_cast<milliseconds>(static_cast<nanoseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_US:
                return duration_cast<milliseconds>(static_cast<microseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_MS:
                return static_cast<milliseconds>(std::get<int64_t>(value_));
            case logical_type::TIMESTAMP_SEC:
                return duration_cast<milliseconds>(static_cast<seconds>(std::get<int64_t>(value_)));
            default:
                assert(false && "incorrect value logical type");
                return milliseconds{0};
        }
    }
    template<>
    inline std::chrono::seconds logical_value_t::value<std::chrono::seconds>() const {
        using namespace std::chrono;
        switch (type_.type()) {
            case logical_type::TIMESTAMP_NS:
                return duration_cast<seconds>(static_cast<nanoseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_US:
                return duration_cast<seconds>(static_cast<microseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_MS:
                return duration_cast<seconds>(static_cast<milliseconds>(std::get<int64_t>(value_)));
            case logical_type::TIMESTAMP_SEC:
                return static_cast<seconds>(std::get<int64_t>(value_));
            default:
                assert(false && "incorrect value logical type");
                return seconds{0};
        }
    }
    template<>
    inline void* logical_value_t::value<void*>() const {
        return std::get<void*>(value_);
    }
    template<>
    inline std::string* logical_value_t::value<std::string*>() const {
        return std::get<std::unique_ptr<std::string>>(value_).get();
    }
    template<>
    inline const std::string& logical_value_t::value<const std::string&>() const {
        return *std::get<std::unique_ptr<std::string>>(value_);
    }
    template<>
    inline std::string_view logical_value_t::value<std::string_view>() const {
        return *std::get<std::unique_ptr<std::string>>(value_);
    }
    template<>
    inline std::vector<logical_value_t>* logical_value_t::value<std::vector<logical_value_t>*>() const {
        return std::get<std::unique_ptr<std::vector<logical_value_t>>>(value_).get();
    }

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
            assert(false && "unsupported type");
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

// User defined class template specialization
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