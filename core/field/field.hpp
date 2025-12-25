#pragma once

#include <map>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include <fmt/format.h>

class field_t;

using field_vector = std::vector<field_t>;

#define DEFINE_FIELD_VECTOR(X)                                                                                         \
    struct X : public field_vector {                                                                                   \
        using field_vector::field_vector;                                                                              \
    }

DEFINE_FIELD_VECTOR(field_array_t);
DEFINE_FIELD_VECTOR(field_tuple_t);
DEFINE_FIELD_VECTOR(field_map_t);

#undef DEFINE_FIELD_VECTOR

using field_map = std::map<std::string, field_t, std::less<>>;

#define DEFINE_FIELD_MAP(X)                                                                                            \
    struct X : public field_map {                                                                                      \
        using field_map::field_map;                                                                                    \
    }

DEFINE_FIELD_MAP(field_object_t);

#undef DEFINE_FIELD_MAP

#define MAY_ALIAS __attribute__((__may_alias__))

struct AggregateFunctionStateData {
    std::string name;
    std::string data;

    bool operator<(const AggregateFunctionStateData&) const { throw std::exception(); }

    bool operator<=(const AggregateFunctionStateData&) const { throw std::exception(); }

    bool operator>(const AggregateFunctionStateData&) const { throw std::exception(); }

    bool operator>=(const AggregateFunctionStateData&) const { throw std::exception(); }

    bool operator==(const AggregateFunctionStateData& rhs) const {
        if (name != rhs.name)
            throw std::exception();

        return data == rhs.data;
    }
};

template<typename T, typename SFINAE = void>
struct nearest_field_type_impl;

template<typename T>
using nearest_field_type = typename nearest_field_type_impl<T>::Type;

template<>
struct nearest_field_type_impl<char> {
    using Type = std::conditional_t<std::is_signed_v<char>, std::int64_t, std::uint64_t>;
};
template<>
struct nearest_field_type_impl<signed char> {
    using Type = std::int64_t;
};
template<>
struct nearest_field_type_impl<unsigned char> {
    using Type = std::uint64_t;
};

template<>
struct nearest_field_type_impl<std::uint16_t> {
    using Type = std::uint64_t;
};
template<>
struct nearest_field_type_impl<std::uint32_t> {
    using Type = std::uint64_t;
};

template<>
struct nearest_field_type_impl<std::int16_t> {
    using Type = std::int32_t;
};
template<>
struct nearest_field_type_impl<std::int32_t> {
    using Type = std::int64_t;
};

/// long and long long are always different types that may behave identically or not.
/// This is different on Linux and Mac.
template<>
struct nearest_field_type_impl<long> {
    using Type = std::int64_t;
}; /// NOLINT
template<>
struct nearest_field_type_impl<long long> {
    using Type = std::int64_t;
}; /// NOLINT
template<>
struct nearest_field_type_impl<unsigned long> {
    using Type = std::uint64_t;
}; /// NOLINT
template<>
struct nearest_field_type_impl<unsigned long long> {
    using Type = std::uint64_t;
}; /// NOLINT
/*
template <> struct nearest_field_type_impl<UInt256> { using Type = UInt256; };
template <> struct nearest_field_type_impl<Int256> { using Type = Int256; };
template <> struct nearest_field_type_impl<UInt128> { using Type = UInt128; };
template <> struct nearest_field_type_impl<Int128> { using Type = Int128; };

template <> struct nearest_field_type_impl<Decimal32> { using Type = DecimalField<Decimal32>; };
template <> struct nearest_field_type_impl<Decimal64> { using Type = DecimalField<Decimal64>; };
template <> struct nearest_field_type_impl<Decimal128> { using Type = DecimalField<Decimal128>; };
template <> struct nearest_field_type_impl<Decimal256> { using Type = DecimalField<Decimal256>; };
template <> struct nearest_field_type_impl<DateTime64> { using Type = DecimalField<DateTime64>; };

template <> struct nearest_field_type_impl<DecimalField<Decimal32>> { using Type = DecimalField<Decimal32>; };
template <> struct nearest_field_type_impl<DecimalField<Decimal64>> { using Type = DecimalField<Decimal64>; };
template <> struct nearest_field_type_impl<DecimalField<Decimal128>> { using Type = DecimalField<Decimal128>; };
template <> struct nearest_field_type_impl<DecimalField<Decimal256>> { using Type = DecimalField<Decimal256>; };
template <> struct nearest_field_type_impl<DecimalField<DateTime64>> { using Type = DecimalField<DateTime64>; };
 */
template<>
struct nearest_field_type_impl<float> {
    using Type = float;
};
template<>
struct nearest_field_type_impl<double> {
    using Type = double;
};
template<>
struct nearest_field_type_impl<const char*> {
    using Type = std::string;
};
template<>
struct nearest_field_type_impl<std::string_view> {
    using Type = std::string;
};
template<>
struct nearest_field_type_impl<std::string> {
    using Type = std::string;
};
template<>
struct nearest_field_type_impl<field_array_t> {
    using Type = field_array_t;
};
template<>
struct nearest_field_type_impl<field_tuple_t> {
    using Type = field_tuple_t;
};
template<>
struct nearest_field_type_impl<field_map_t> {
    using Type = field_map_t;
};
template<>
struct nearest_field_type_impl<field_object_t> {
    using Type = field_object_t;
};
template<>
struct nearest_field_type_impl<bool> {
    using Type = std::uint64_t;
};
///template <> struct nearest_field_type_impl<Null> { using Type = Null; };

template<>
struct nearest_field_type_impl<AggregateFunctionStateData> {
    using Type = AggregateFunctionStateData;
};

/*
template <typename T = std::is_enum_v<T>>
struct nearest_field_type_impl<T> {
    using Type = nearest_field_type<std::underlying_type_t<T>>;
};
 */

template<typename T>
decltype(auto) cast_to_nearest_field_type(T&& x) {
    using U = nearest_field_type<std::decay_t<T>>;
    if constexpr (std::is_same_v<std::decay_t<T>, U>)
        return std::forward<T>(x);
    else
        return U(x);
}

class field_t {
public:
    using storage_t = std::variant<bool,
                                   uint64_t,
                                   int64_t,
                                   float,
                                   double,
                                   std::string,
                                   void*,
                                   field_array_t,
                                   field_object_t /*, tuple_t, map_t, AggregateFunctionStateData*/>;

    enum type_which_t
    {
        Null = 0,
        UInt64 = 1,
        Int64 = 2,
        Float64 = 3,
        //UInt128 = 4,
        //Int128 = 5,

        String = 16,
        Array = 17,
        //Tuple = 18,
        //Decimal32 = 19,
        //Decimal64 = 20,
        //Decimal128 = 21,
        //AggregateFunctionState = 22,
        //Decimal256 = 23,
        //UInt256 = 24,
        //Int256 = 25,
        //Map = 26,
        //UUID = 27,
        Bool = 28,
        Object = 29,
    };

    type_which_t which() const { return type_which_; }

    bool is_null() const { return type_which_t::Null == type_which_; }

    field_t()
        : storage_()
        , type_which_(type_which_t::Null) {}

    explicit field_t(bool value)
        : storage_(value)
        , type_which_(type_which_t::Bool) {}

    explicit field_t(int64_t value)
        : storage_(value)
        , type_which_(type_which_t::Int64) {}

    explicit field_t(uint64_t value)
        : storage_(value)
        , type_which_(type_which_t::UInt64) {}

    explicit field_t(double value)
        : storage_(value)
        , type_which_(type_which_t::Float64) {}

    explicit field_t(const std::string& value)
        : storage_(value)
        , type_which_(type_which_t::String) {}

    explicit field_t(const field_array_t& value)
        : storage_(value)
        , type_which_(type_which_t::Array) {}

    explicit field_t(const field_object_t& value)
        : storage_(value)
        , type_which_(type_which_t::Object) {}

    static field_t new_array() { return field_t(field_array_t()); }

    static field_t new_object() { return field_t(field_object_t()); }

    //    void assign(bool value){
    //
    //    }
    //    void assign(int64_t value)
    //        {}
    //    void assign(uint64_t value)
    //         {}
    //    void assign(const std::string& value)
    //         {}

    bool operator<(const field_t& other) const { return storage_ < other.storage_; }

    bool operator<=(const field_t& other) const { return storage_ <= other.storage_; }

    bool operator>(const field_t& other) const { return storage_ > other.storage_; }

    bool operator>=(const field_t& other) const { return storage_ >= other.storage_; }

    bool operator==(const field_t& other) const { return storage_ == other.storage_; }

    bool operator!=(const field_t& rhs) const { return !(*this == rhs); }

    std::string to_string() const {
        return std::visit(
            [](const auto& c) {
                using command_type = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<command_type, bool>) {
                    return std::string(c ? "true" : "false");
                } else if constexpr (std::is_same_v<command_type, uint64_t>) {
                    return std::to_string(c);
                } else if constexpr (std::is_same_v<command_type, int64_t>) {
                    return std::to_string(c);
                } else if constexpr (std::is_same_v<command_type, float>) {
                    return std::to_string(c);
                } else if constexpr (std::is_same_v<command_type, double>) {
                    return std::to_string(c);
                } else if constexpr (std::is_same_v<command_type, std::string>) {
                    return "\"" + c + "\"";
                }
                return std::string();
            },
            storage_);
    }
    /*
    template<typename T>
    nearest_field_type<std::decay_t<T>>& get() {

        using StoredType = nearest_field_type<std::decay_t<T>>;
        StoredType* MAY_ALIAS ptr = reinterpret_cast<StoredType*>(&storage_);
        return *ptr;
    }
*/

    template<typename T>
    const auto& get() const {
        return std::get<T>(storage_);
    }

    // clang-format off
    template <typename F, typename FieldRef>
    static auto dispatch(F && f, FieldRef && field) {
        switch (field.which){
            case type_which_t::Null:    return f(field.template get<Null>());

            case type_which_t::UInt64:  return f(field.template get<UInt64>());
          ///  case type_which_t::UInt128: return f(field.template get<UInt128>());
          ///  case type_which_t::UInt256: return f(field.template get<UInt256>());
            case type_which_t::Int64:   return f(field.template get<Int64>());
         ///   case type_which_t::Int128:  return f(field.template get<Int128>());
         ///   case type_which_t::Int256:  return f(field.template get<Int256>());
         ///   case type_which_t::UUID:    return f(field.template get<UUID>());
            case type_which_t::Float64: return f(field.template get<Float64>());
            case type_which_t::String:  return f(field.template get<String>());
            case type_which_t::Array:   return f(field.template get<Array>());
        ///    case type_which_t::Tuple:   return f(field.template get<Tuple>());
        ///    case type_which_t::Map:     return f(field.template get<Map>());
            case type_which_t::Bool:
            {
                bool value = bool(field.template get<UInt64>());
                return f(value);
            }
            case type_which_t::Object:     return f(field.template get<Object>());
           /// case type_which_t::Decimal32:  return f(field.template get<DecimalField<Decimal32>>());
           /// case type_which_t::Decimal64:  return f(field.template get<DecimalField<Decimal64>>());
           /// case type_which_t::Decimal128: return f(field.template get<DecimalField<Decimal128>>());
           /// case type_which_t::Decimal256: return f(field.template get<DecimalField<Decimal256>>());
           /// case type_which_t::AggregateFunctionState: return f(field.template get<AggregateFunctionStateData>());
        }
    }
    // clang-format on

private:
    storage_t storage_;
    type_which_t type_which_;
};