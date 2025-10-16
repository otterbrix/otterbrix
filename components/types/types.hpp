#pragma once
#include <absl/numeric/int128.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace components::types {

    using int128_t = absl::int128;
    using uint128_t = absl::uint128;

    class logical_value_t;
    class logical_type_extension;

    enum class compare_t
    {
        less = -1,
        equals = 0,
        more = 1
    };

    // order change may break physical_value comparators
    enum class physical_type : uint8_t
    {
        BOOL = 1,
        UINT8 = 2,
        INT8 = 3,
        UINT16 = 4,
        INT16 = 5,
        UINT32 = 6,
        INT32 = 7,
        UINT64 = 8,
        INT64 = 9,
        UINT128 = 10,
        INT128 = 11,
        // HALF_FLOAT = 12,
        FLOAT = 13,
        DOUBLE = 14,
        STRING = 15,
        // BINARY = 16,                // Variable-length bytes (no guarantee of UTF8-ness)
        // FIXED_SIZE_BINARY = 17,     // Fixed-size binary. Each value occupies the same number of bytes
        // DATE32 = 18,                // int32_t days since the UNIX epoch
        // DATE64 = 19,                // int64_t milliseconds since the UNIX epoch
        // TIMESTAMP = 20,             // Exact timestamp encoded with int64 since UNIX epoch, ms
        // TIME32 = 21,                // Time as signed 32-bit integer, representing either seconds or us since midnight
        // TIME64 = 22,                // Time as signed 64-bit integer, representing either us or ns since midnight
        // INTERVAL = 23,              // YEAR_MONTH or DAY_TIME interval in SQL style
        // DECIMAL = 24,               // Precision- and scale-based decimal type. Storage type depends on the parameters.
        LIST = 25,   // A list of some logical data type
        STRUCT = 26, // Struct of logical types
        UNION = 27,  // Unions of logical types

        // Dictionary-encoded type, also called "categorical" or "factor"
        // in other programming languages. Holds the dictionary value
        // type but not the dictionary itself, which is part of the
        // ArrayData struct
        // DICTIONARY = 28,

        // EXTENSION = 29,             // Custom data type, implemented by user
        ARRAY = 30, // Array with fixed length of some logical type (a fixed-size list)
        // DURATION = 31,              // Measure of elapsed time in either seconds, ms, us or ns.
        // LARGE_STRING = 32,          // Like STRING, but with 64-bit offsets
        // LARGE_BINARY = 33,          // Like BINARY, but with 64-bit offsets
        // LARGE_LIST = 34,            // Like LIST, but with 64-bit offsets

        NA = 127,      // NULL value
        UNKNOWN = 128, // Unknown physical type of user defined types
        BIT = 192,     // bitmap
        INVALID = 255
    };

    // order change may break logical_value comparators
    enum class logical_type : uint8_t
    {
        NA = 0,   // NULL type, used for constant NULL
        ANY = 1,  // ANY type, used for functions that accept any type as parameter
        USER = 2, // A User Defined type (e.g., ENUMs before the binder)
        BOOLEAN = 10,
        TINYINT = 11,
        SMALLINT = 12,
        INTEGER = 13,
        BIGINT = 14,
        HUGEINT = 15,
        // DATE = 16,
        // TIME = 17,
        TIMESTAMP_SEC = 18, // seconds
        TIMESTAMP_MS = 19,  // milliseconds
        TIMESTAMP_US = 20,  // microseconds
        TIMESTAMP_NS = 21,  // nanoseconds
        DECIMAL = 22,
        FLOAT = 23,
        DOUBLE = 24,
        BLOB = 25,
        INTERVAL = 26,
        UTINYINT = 27,
        USMALLINT = 28,
        UINTEGER = 29,
        UBIGINT = 30,
        UHUGEINT = 31,
        // TIMESTAMP_TZ = 32,
        // TIME_TZ = 33,
        BIT = 34,
        STRING_LITERAL = 35,  // String literals, used for constant strings
        INTEGER_LITERAL = 36, // Integer literals, used for constant integers

        POINTER = 51,
        VALIDITY = 53,
        UUID = 54,

        STRUCT = 100,
        LIST = 101,
        MAP = 102,
        TABLE = 103,
        ENUM = 104,
        FUNCTION = 105,
        LAMBDA = 106,
        UNION = 107,
        ARRAY = 108,

        UNKNOWN = 127, // Unknown type, used for parameter expressions
        INVALID = 255
    };

    constexpr logical_type to_logical(physical_type type) {
        switch (type) {
            case physical_type::BOOL:
                return logical_type::BOOLEAN;
            case physical_type::UINT8:
                return logical_type::UTINYINT;
            case physical_type::INT8:
                return logical_type::TINYINT;
            case physical_type::UINT16:
                return logical_type::USMALLINT;
            case physical_type::INT16:
                return logical_type::SMALLINT;
            case physical_type::UINT32:
                return logical_type::UINTEGER;
            case physical_type::INT32:
                return logical_type::INTEGER;
            case physical_type::UINT64:
                return logical_type::UBIGINT;
            case physical_type::INT64:
                return logical_type::BIGINT;
            case physical_type::UINT128:
                return logical_type::UHUGEINT;
            case physical_type::INT128:
                return logical_type::HUGEINT;
            case physical_type::FLOAT:
                return logical_type::FLOAT;
            case physical_type::DOUBLE:
                return logical_type::DOUBLE;
            case physical_type::STRING:
                return logical_type::STRING_LITERAL;
            case physical_type::NA:
                return logical_type::NA;
            case physical_type::UNKNOWN:
                return logical_type::UNKNOWN;
            default:
                return logical_type::UNKNOWN;
        }
    }

    constexpr bool is_numeric(logical_type type) {
        switch (type) {
            case logical_type::BOOLEAN:
            case logical_type::UTINYINT:
            case logical_type::TINYINT:
            case logical_type::USMALLINT:
            case logical_type::SMALLINT:
            case logical_type::UINTEGER:
            case logical_type::INTEGER:
            case logical_type::UBIGINT:
            case logical_type::BIGINT:
            case logical_type::UHUGEINT:
            case logical_type::HUGEINT:
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
                return true;
            default:
                return false;
        }
    }

    constexpr bool is_signed(logical_type type) {
        switch (type) {
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT:
            case logical_type::HUGEINT:
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
                return true;
            default:
                return false;
        }
    }

    constexpr bool is_unsigned(logical_type type) {
        switch (type) {
            case logical_type::BOOLEAN:
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
            case logical_type::UHUGEINT:
                return true;
            default:
                return false;
        }
    }

    constexpr bool is_duration(logical_type type) {
        switch (type) {
            case logical_type::TIMESTAMP_SEC:
            case logical_type::TIMESTAMP_MS:
            case logical_type::TIMESTAMP_US:
            case logical_type::TIMESTAMP_NS:
                return true;
            default:
                return false;
        }
    }

    // for now only supports std::string for string types
    // TODO: convert multiple string formats to logical_type::STRING_LITERAL
    template<typename T>
    constexpr logical_type to_logical_type() {
        if constexpr (std::is_same<T, std::nullptr_t>::value) {
            return logical_type::NA;
        } else if constexpr (std::is_pointer<T>::value) {
            return logical_type::POINTER;
        } else if constexpr (std::is_same<T, std::chrono::nanoseconds>::value) {
            return logical_type::TIMESTAMP_NS;
        } else if constexpr (std::is_same<T, std::chrono::microseconds>::value) {
            return logical_type::TIMESTAMP_US;
        } else if constexpr (std::is_same<T, std::chrono::milliseconds>::value) {
            return logical_type::TIMESTAMP_MS;
        } else if constexpr (std::is_same<T, std::chrono::seconds>::value) {
            return logical_type::TIMESTAMP_SEC;
        } else if constexpr (std::is_same<T, bool>::value) {
            return logical_type::BOOLEAN;
        } else if constexpr (std::is_same<T, int128_t>::value) {
            return logical_type::HUGEINT;
        } else if constexpr (std::is_same<T, uint128_t>::value) {
            return logical_type::UHUGEINT;
        } else if constexpr (std::is_integral<T>::value) {
            if constexpr (std::is_signed<T>::value) {
                if constexpr (sizeof(T) == 1) {
                    return logical_type::TINYINT;
                } else if constexpr (sizeof(T) == 2) {
                    return logical_type::SMALLINT;
                } else if constexpr (sizeof(T) == 4) {
                    return logical_type::INTEGER;
                } else if constexpr (sizeof(T) == 8) {
                    return logical_type::BIGINT;
                }
            } else {
                if constexpr (sizeof(T) == 1) {
                    return logical_type::UTINYINT;
                } else if constexpr (sizeof(T) == 2) {
                    return logical_type::USMALLINT;
                } else if constexpr (sizeof(T) == 4) {
                    return logical_type::UINTEGER;
                } else if constexpr (sizeof(T) == 8) {
                    return logical_type::UBIGINT;
                }
            }
        } else if constexpr (std::is_same<T, float>::value) {
            return logical_type::FLOAT;
        } else if constexpr (std::is_same<T, double>::value) {
            return logical_type::DOUBLE;
        } else if constexpr (std::is_same<T, std::string>::value) {
            return logical_type::STRING_LITERAL;
        } else {
            return logical_type::INVALID;
        }
    }

    constexpr logical_type promote_type(logical_type type1, logical_type type2) {
        using namespace std::chrono;
        if (type1 == type2) {
            return type1;
        }
        assert(is_numeric(type1) && is_numeric(type2) || is_duration(type1) && is_duration(type2));

        constexpr uint8_t signage_difference =
            static_cast<uint8_t>(logical_type::UTINYINT) - static_cast<uint8_t>(logical_type::TINYINT);

        if (is_duration(type1) && is_duration(type2)) {
            switch (type1) {
                case logical_type::TIMESTAMP_SEC:
                    switch (type2) {
                        case logical_type::TIMESTAMP_MS:
                            return to_logical_type<std::common_type<seconds, milliseconds>::type>();
                        case logical_type::TIMESTAMP_US:
                            return to_logical_type<std::common_type<seconds, microseconds>::type>();
                        case logical_type::TIMESTAMP_NS:
                            return to_logical_type<std::common_type<seconds, nanoseconds>::type>();
                        default:
                            break;
                    }
                case logical_type::TIMESTAMP_MS:
                    switch (type2) {
                        case logical_type::TIMESTAMP_SEC:
                            return to_logical_type<std::common_type<milliseconds, seconds>::type>();
                        case logical_type::TIMESTAMP_US:
                            return to_logical_type<std::common_type<milliseconds, microseconds>::type>();
                        case logical_type::TIMESTAMP_NS:
                            return to_logical_type<std::common_type<milliseconds, nanoseconds>::type>();
                        default:
                            break;
                    }
                case logical_type::TIMESTAMP_US:
                    switch (type2) {
                        case logical_type::TIMESTAMP_SEC:
                            return to_logical_type<std::common_type<microseconds, seconds>::type>();
                        case logical_type::TIMESTAMP_MS:
                            return to_logical_type<std::common_type<microseconds, milliseconds>::type>();
                        case logical_type::TIMESTAMP_NS:
                            return to_logical_type<std::common_type<microseconds, nanoseconds>::type>();
                        default:
                            break;
                    }
                case logical_type::TIMESTAMP_NS:
                    switch (type2) {
                        case logical_type::TIMESTAMP_SEC:
                            return to_logical_type<std::common_type<nanoseconds, seconds>::type>();
                        case logical_type::TIMESTAMP_MS:
                            return to_logical_type<std::common_type<nanoseconds, milliseconds>::type>();
                        case logical_type::TIMESTAMP_US:
                            return to_logical_type<std::common_type<nanoseconds, microseconds>::type>();
                        default:
                            break;
                    }
                default:
                    break;
            }
        }

        // This is dependent on enum encoding values
        if (is_signed(type1) == is_signed(type2) || is_unsigned(type1) == is_unsigned(type2)) {
            return static_cast<logical_type>(std::max(static_cast<uint8_t>(type1), static_cast<uint8_t>(type2)));
        } else if (is_signed(type1)) {
            return static_cast<logical_type>(
                std::max<uint8_t>(static_cast<uint8_t>(type1), static_cast<uint8_t>(type2) - signage_difference));
        } else {
            return static_cast<logical_type>(
                std::max<uint8_t>(static_cast<uint8_t>(type1) - signage_difference, static_cast<uint8_t>(type2)));
        }
    }

    class complex_logical_type {
    public:
        complex_logical_type(logical_type type = logical_type::NA, std::string alias = "");
        complex_logical_type(logical_type type,
                             std::unique_ptr<logical_type_extension> extension,
                             std::string alias = "");
        complex_logical_type(const complex_logical_type& other);
        complex_logical_type(complex_logical_type&& other) noexcept = default;
        complex_logical_type& operator=(const complex_logical_type& other);
        complex_logical_type& operator=(complex_logical_type&& other) = default;
        ~complex_logical_type() = default;

        bool operator==(const complex_logical_type& rhs) const;
        bool operator!=(const complex_logical_type& rhs) const;

        logical_type type() const noexcept { return type_; }
        size_t size() const noexcept;
        size_t align() const noexcept;
        physical_type to_physical_type() const;
        void set_alias(const std::string& alias);
        bool has_alias() const;
        const std::string& alias() const;
        const std::string& child_name(uint64_t index) const;
        bool is_unnamed() const;
        bool is_nested() const;

        const complex_logical_type& child_type() const;
        const std::vector<complex_logical_type>& child_types() const;
        logical_type_extension* extension() const noexcept;

        template<typename T>
        static bool contains(const complex_logical_type& type, T&& predicate);

        static bool contains(const complex_logical_type& type, logical_type type_id) {
            return contains(type, [&](const complex_logical_type& t) { return t.type() == type_id; });
        }

        static bool type_is_constant_size(logical_type type);

        static complex_logical_type create_decimal(uint8_t width, uint8_t scale, std::string alias = "");
        static complex_logical_type create_list(const complex_logical_type& internal_type, std::string alias = "");
        static complex_logical_type
        create_array(const complex_logical_type& internal_type, size_t array_size, std::string alias = "");
        static complex_logical_type create_map(const complex_logical_type& key_type,
                                               const complex_logical_type& value_type,
                                               std::string alias = "");
        static complex_logical_type create_struct(const std::vector<complex_logical_type>& fields,
                                                  std::string alias = "");
        static complex_logical_type create_union(std::vector<complex_logical_type> fields, std::string alias = "");

    private:
        logical_type type_ = logical_type::NA;
        std::unique_ptr<logical_type_extension> extension_ = nullptr; // for complex types
    };

    struct list_entry_t {
        list_entry_t() = default;
        list_entry_t(uint64_t offset, uint64_t length)
            : offset(offset)
            , length(length) {}
        constexpr bool operator!=(const list_entry_t& other) const { return !(*this == other); }
        constexpr bool operator==(const list_entry_t& other) const {
            return offset == other.offset && length == other.length;
        }

        uint64_t offset;
        uint64_t length;
    };

    struct field_description {
        field_description() = default;

        field_description(uint64_t field_id, bool required = true, std::string doc = "")
            : field_id(field_id)
            , required(required)
            , doc(std::move(doc)) {}

        uint64_t field_id;
        bool required;
        std::string doc;
    };

    class logical_type_extension {
    public:
        // duplicates from logical_type, but declared separatly for clarity
        enum class extension_type : uint8_t
        {
            GENERIC = 0,
            ARRAY = 1,
            MAP = 2,
            LIST = 3,
            STRUCT = 4,
            DECIMAL = 5,
            ENUM = 6,
            USER = 7,
            FUNCTION = 8
        };

        logical_type_extension() = default;
        explicit logical_type_extension(extension_type t, std::string alias = "");
        virtual ~logical_type_extension() = default;

        extension_type type() const noexcept { return type_; }
        const std::string& alias() const noexcept { return alias_; }
        void set_alias(const std::string& alias);

    protected:
        extension_type type_ = extension_type::GENERIC;
        std::string alias_;
    };

    class array_logical_type_extension : public logical_type_extension {
    public:
        explicit array_logical_type_extension(const complex_logical_type& type, uint64_t size);

        const complex_logical_type& internal_type() const noexcept { return items_type_; }
        size_t size() const noexcept { return size_; }

    private:
        complex_logical_type items_type_;
        uint64_t size_;
    };

    class map_logical_type_extension : public logical_type_extension {
    public:
        map_logical_type_extension(const complex_logical_type& key, const complex_logical_type& value);
        map_logical_type_extension(uint64_t key_id,
                                   const types::complex_logical_type& key,
                                   uint64_t value_id,
                                   const types::complex_logical_type& value,
                                   bool value_required);

        const complex_logical_type& key() const noexcept { return key_; }
        const complex_logical_type& value() const noexcept { return value_; }
        uint64_t key_id() const { return key_id_; }
        uint64_t value_id() const { return value_id_; }
        bool value_required() const { return value_required_; }

    private:
        complex_logical_type key_;
        complex_logical_type value_;
        uint64_t key_id_;
        uint64_t value_id_;
        bool value_required_;
    };

    class list_logical_type_extension : public logical_type_extension {
    public:
        explicit list_logical_type_extension(complex_logical_type type);
        list_logical_type_extension(uint64_t field_id, complex_logical_type type, bool required);

        const complex_logical_type& node() const noexcept { return type_; }
        uint64_t field_id() const noexcept { return field_id_; }
        bool required() const noexcept { return required_; }

    private:
        complex_logical_type type_;
        uint64_t field_id_;
        bool required_;
    };

    class struct_logical_type_extension : public logical_type_extension {
    public:
        explicit struct_logical_type_extension(const std::vector<complex_logical_type>& fields);

        // fields must be aliased
        struct_logical_type_extension(const std::vector<types::complex_logical_type>& columns,
                                      std::vector<field_description> descriptions);

        const std::vector<complex_logical_type>& child_types() const { return fields_; }
        const std::vector<field_description>& descriptions() const { return descriptions_; }

    private:
        std::vector<complex_logical_type> fields_;
        std::vector<field_description> descriptions_;
    };

    class decimal_logical_type_extension : public logical_type_extension {
    public:
        explicit decimal_logical_type_extension(uint8_t width, uint8_t scale);

        uint8_t width() const noexcept { return width_; }
        uint8_t scale() const noexcept { return scale_; }

    private:
        uint8_t width_;
        uint8_t scale_;
    };

    class enum_logical_type_extension : public logical_type_extension {
    public:
        explicit enum_logical_type_extension(std::vector<logical_value_t> entries);

        const std::vector<logical_value_t>& entries() const noexcept { return entries_; }

    private:
        std::vector<logical_value_t> entries_; // integer literal for value and alias for entry name
    };

    class user_logical_type_extension : public logical_type_extension {
    public:
        explicit user_logical_type_extension(std::string catalog, std::vector<logical_value_t> user_type_modifiers);

    private:
        std::string catalog_;
        std::vector<logical_value_t> user_type_modifiers_;
    };

    class function_logical_type_extension : public logical_type_extension {
    public:
        explicit function_logical_type_extension(complex_logical_type return_type,
                                                 std::vector<complex_logical_type> arguments);

    private:
        complex_logical_type return_type_;
        std::vector<complex_logical_type> argument_types_;
    };

    template<typename T>
    bool complex_logical_type::contains(const complex_logical_type& type, T&& predicate) {
        if (predicate(type)) {
            return true;
        }
        switch (type.type_) {
            case logical_type::STRUCT:
            case logical_type::UNION: {
                for (const auto& child : type.child_types()) {
                    if (contains(child, predicate)) {
                        return true;
                    }
                }
                return false;
            }
            case logical_type::LIST:
                return contains(type.child_type(), predicate);
            case logical_type::ARRAY:
                return contains(type.child_type(), predicate);
            case logical_type::MAP:
                return contains(static_cast<map_logical_type_extension*>(type.extension_.get())->key(), predicate) ||
                       contains(static_cast<map_logical_type_extension*>(type.extension_.get())->value(), predicate);
            default:
                return false;
        }
    }

} // namespace components::types