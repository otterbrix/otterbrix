#include "logical_value.hpp"
#include "operations_helper.hpp"
#include <components/serialization/deserializer.hpp>

#include <stdexcept>

namespace std {
    template<>
    struct make_signed<components::types::uint128_t> {
        typedef components::types::int128_t type;
    };
    template<>
    struct make_signed<components::types::int128_t> {
        typedef components::types::int128_t type;
    };
    template<>
    struct make_signed<float> {
        typedef float type;
    };
    template<>
    struct make_signed<double> {
        typedef double type;
    };
    template<>
    struct make_unsigned<components::types::uint128_t> {
        typedef components::types::uint128_t type;
    };
    template<>
    struct make_unsigned<components::types::int128_t> {
        typedef components::types::uint128_t type;
    };
    template<>
    struct make_unsigned<float> {
        typedef float type;
    };
    template<>
    struct make_unsigned<double> {
        typedef double type;
    };

    template<>
    struct is_signed<components::types::int128_t> : true_type {};

    template<>
    struct is_unsigned<components::types::uint128_t> : true_type {};

} // namespace std

namespace components::types {

    logical_value_t::~logical_value_t() { destroy_heap(); }

    void logical_value_t::destroy_heap() {
        if (!data_) {
            return;
        }
        switch (type_.type()) {
            case logical_type::STRING_LITERAL:
                heap_delete(str_ptr());
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
            case logical_type::UNION:
            case logical_type::VARIANT:
                heap_delete(vec_ptr());
                break;
            default:
                break;
        }
        data_ = 0;
    }

    logical_value_t::logical_value_t(std::pmr::memory_resource* r, logical_type type)
        : logical_value_t(r, complex_logical_type{type}) {}

    logical_value_t::logical_value_t(std::pmr::memory_resource* r, complex_logical_type type)
        : type_(std::move(type))
        , resource_(r) {
        switch (type_.type()) {
            case logical_type::HUGEINT:
                data128_ = 0;
                break;
            case logical_type::UHUGEINT:
                udata128_ = 0;
                break;
            case logical_type::STRING_LITERAL:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::string>());
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::vector<logical_value_t>>());
                break;
            case logical_type::UNION:
            case logical_type::VARIANT:
                assert(false && "UNION/VARIANT must be created via factory methods");
                break;
            default:
                break;
        }
    }

    logical_value_t::logical_value_t(const logical_value_t& other)
        : type_(other.type_)
        , resource_(other.resource_) {
        switch (type_.type()) {
            case logical_type::HUGEINT:
                data128_ = other.data128_;
                break;
            case logical_type::UHUGEINT:
                udata128_ = other.udata128_;
                break;
            case logical_type::STRING_LITERAL:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::string>(*other.str_ptr()));
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::vector<logical_value_t>>(*other.vec_ptr()));
                break;
            case logical_type::UNION:
            case logical_type::VARIANT:
                if (other.data_) {
                    data_ = reinterpret_cast<uint64_t>(heap_new<std::vector<logical_value_t>>(*other.vec_ptr()));
                }
                break;
            default:
                data_ = other.data_;
                break;
        }
    }

    logical_value_t::logical_value_t(logical_value_t&& other) noexcept
        : type_(std::move(other.type_))
        , resource_(other.resource_) {
        switch (type_.type()) {
            case logical_type::HUGEINT:
                data128_ = other.data128_;
                break;
            case logical_type::UHUGEINT:
                udata128_ = other.udata128_;
                break;
            case logical_type::STRING_LITERAL:
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
            case logical_type::UNION:
            case logical_type::VARIANT:
                data_ = other.data_;
                other.data_ = 0;
                break;
            default:
                data_ = other.data_;
                break;
        }
    }

    logical_value_t& logical_value_t::operator=(const logical_value_t& other) {
        if (this == &other)
            return *this;
        destroy_heap();
        type_ = other.type_;
        resource_ = other.resource_;
        switch (type_.type()) {
            case logical_type::HUGEINT:
                data128_ = other.data128_;
                break;
            case logical_type::UHUGEINT:
                udata128_ = other.udata128_;
                break;
            case logical_type::STRING_LITERAL:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::string>(*other.str_ptr()));
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::vector<logical_value_t>>(*other.vec_ptr()));
                break;
            case logical_type::UNION:
            case logical_type::VARIANT:
                if (other.data_) {
                    data_ = reinterpret_cast<uint64_t>(heap_new<std::vector<logical_value_t>>(*other.vec_ptr()));
                }
                break;
            default:
                data_ = other.data_;
                break;
        }
        return *this;
    }

    logical_value_t& logical_value_t::operator=(logical_value_t&& other) noexcept {
        if (this == &other)
            return *this;
        destroy_heap();
        type_ = std::move(other.type_);
        resource_ = other.resource_;
        switch (type_.type()) {
            case logical_type::HUGEINT:
                data128_ = other.data128_;
                break;
            case logical_type::UHUGEINT:
                udata128_ = other.udata128_;
                break;
            case logical_type::STRING_LITERAL:
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
            case logical_type::UNION:
            case logical_type::VARIANT:
                data_ = other.data_;
                other.data_ = 0;
                break;
            default:
                data_ = other.data_;
                break;
        }
        return *this;
    }

    const complex_logical_type& logical_value_t::type() const noexcept { return type_; }

    bool logical_value_t::is_null() const noexcept { return type_.type() == logical_type::NA; }

    template<typename T = void>
    struct cast_callback_t;

    template<>
    struct cast_callback_t<void> {
        template<typename LeftValueType, typename RightValueType>
        auto operator()(const logical_value_t& value) const -> logical_value_t {
            auto* r = value.resource();
            if constexpr (std::is_same_v<LeftValueType, RightValueType>) {
                return value;
            } else if constexpr (std::is_same_v<RightValueType, bool>) {
                if constexpr (std::is_same_v<LeftValueType, std::string_view>) {
                    return logical_value_t{r, value.template value<bool>() ? "TRUE" : "FALSE"};
                } else {
                    return logical_value_t{r, LeftValueType{1}};
                }
            } else if constexpr (std::is_same_v<LeftValueType, std::string_view>) {
                if constexpr (std::is_signed_v<RightValueType>) {
                    return logical_value_t{
                        r,
                        std::to_string(static_cast<int64_t>(value.template value<RightValueType>()))};
                } else {
                    return logical_value_t{
                        r,
                        std::to_string(static_cast<uint64_t>(value.template value<RightValueType>()))};
                }
            } else if constexpr (std::is_same_v<LeftValueType, bool>) {
                return logical_value_t{r, core::is_equals(RightValueType{}, value.template value<RightValueType>())};
            } else if constexpr (std::is_same_v<RightValueType, std::string_view>) {
                if constexpr (std::is_floating_point_v<LeftValueType>) {
                    return logical_value_t{
                        r,
                        static_cast<LeftValueType>(std::atof(value.template value<RightValueType>().data()))};
                } else {
                    return logical_value_t{
                        r,
                        static_cast<LeftValueType>(std::atoll(value.template value<RightValueType>().data()))};
                }
            } else if constexpr (std::is_same_v<LeftValueType, int128_t>) {
                return logical_value_t{
                    r,
                    static_cast<LeftValueType>(static_cast<int64_t>(value.template value<RightValueType>()))};
            } else if constexpr (std::is_same_v<LeftValueType, uint128_t>) {
                return logical_value_t{
                    r,
                    static_cast<LeftValueType>(static_cast<uint64_t>(value.template value<RightValueType>()))};
            } else {
                return logical_value_t{r, static_cast<LeftValueType>(value.template value<RightValueType>())};
            }
        }
    };

    logical_value_t logical_value_t::cast_as(const complex_logical_type& type) const {
        using namespace std::chrono;

        if (type_ == type) {
            return logical_value_t(*this);
        }
        if (is_numeric(type.type())) {
            // same problem as in physical_value
            // ideally use something like this
            // return logicaL_value<type.type()>{value<type_.type()>()};
            // but type is not a constexpr, so here is a huge switch:

            return double_simple_physical_type_switch<cast_callback_t>(type.to_physical_type(),
                                                                       type_.to_physical_type(),
                                                                       *this);
        } else if (is_duration(type_.type()) && is_duration(type.type())) {
            switch (type.type()) {
                case logical_type::TIMESTAMP_SEC:
                    return logical_value_t{resource_, value<seconds>()};
                case logical_type::TIMESTAMP_MS:
                    return logical_value_t{resource_, value<milliseconds>()};
                case logical_type::TIMESTAMP_US:
                    return logical_value_t{resource_, value<microseconds>()};
                case logical_type::TIMESTAMP_NS:
                    return logical_value_t{resource_, value<nanoseconds>()};
                default:
                    break;
            }
        } else if (type_.type() == logical_type::STRUCT && type.type() == logical_type::STRUCT) {
            if (type_.child_types().size() != type.child_types().size()) {
                assert(false && "incorrect type");
                return logical_value_t{resource_, complex_logical_type{logical_type::NA}};
            }

            std::vector<logical_value_t> fields;
            fields.reserve(children().size());
            for (size_t i = 0; i < children().size(); i++) {
                fields.emplace_back(children()[i].cast_as(type.child_types()[i]));
            }

            return create_struct(resource_, type, fields);
        }
        assert(false && "cast to value is not implemented");
        return logical_value_t{resource_, complex_logical_type{logical_type::NA}};
    }

    void logical_value_t::set_alias(const std::string& alias) { type_.set_alias(alias); }

    bool logical_value_t::operator==(const logical_value_t& rhs) const {
        if (type_ != rhs.type_) {
            if ((is_numeric(type_.type()) && is_numeric(rhs.type_.type())) ||
                (is_duration(type_.type()) && is_duration(rhs.type_.type()))) {
                auto promoted_type = promote_type(type_.type(), rhs.type_.type());

                if (promoted_type == logical_type::FLOAT) {
                    return core::is_equals(cast_as(promoted_type).value<float>(),
                                           rhs.cast_as(promoted_type).value<float>());
                } else if (promoted_type == logical_type::DOUBLE) {
                    return core::is_equals(cast_as(promoted_type).value<double>(),
                                           rhs.cast_as(promoted_type).value<double>());
                } else {
                    return cast_as(promoted_type) == rhs.cast_as(promoted_type);
                }
            }
            return false;
        } else {
            switch (type_.type()) {
                case logical_type::NA:
                    return true;
                case logical_type::BOOLEAN:
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT:
                case logical_type::UTINYINT:
                case logical_type::USMALLINT:
                case logical_type::UINTEGER:
                case logical_type::UBIGINT:
                case logical_type::POINTER:
                    return data_ == rhs.data_;
                case logical_type::FLOAT:
                    return core::is_equals(value<float>(), rhs.value<float>());
                case logical_type::DOUBLE:
                    return core::is_equals(value<double>(), rhs.value<double>());
                case logical_type::STRING_LITERAL:
                    return *str_ptr() == *rhs.str_ptr();
                case logical_type::LIST:
                case logical_type::ARRAY:
                case logical_type::MAP:
                case logical_type::STRUCT:
                    return *vec_ptr() == *rhs.vec_ptr();
                case logical_type::UNION:
                case logical_type::VARIANT:
                    if (!data_ && !rhs.data_)
                        return true;
                    if (!data_ || !rhs.data_)
                        return false;
                    return *vec_ptr() == *rhs.vec_ptr();
                default:
                    return false;
            }
        }
    }

    bool logical_value_t::operator!=(const logical_value_t& rhs) const { return !(*this == rhs); }

    bool logical_value_t::operator<(const logical_value_t& rhs) const {
        if (type_ != rhs.type_) {
            if (type_.type() == logical_type::NA)
                return false;
            if (rhs.type_.type() == logical_type::NA)
                return true;
            if ((is_numeric(type_.type()) && is_numeric(rhs.type_.type())) ||
                (is_duration(type_.type()) && is_duration(rhs.type_.type()))) {
                auto promoted_type = promote_type(type_.type(), rhs.type_.type());
                return cast_as(promoted_type) < rhs.cast_as(promoted_type);
            }
            return false;
        } else {
            switch (type_.type()) {
                case logical_type::BOOLEAN:
                    return static_cast<bool>(data_) < static_cast<bool>(rhs.data_);
                case logical_type::TINYINT:
                    return static_cast<int8_t>(data_) < static_cast<int8_t>(rhs.data_);
                case logical_type::SMALLINT:
                    return static_cast<int16_t>(data_) < static_cast<int16_t>(rhs.data_);
                case logical_type::INTEGER:
                    return static_cast<int32_t>(data_) < static_cast<int32_t>(rhs.data_);
                case logical_type::BIGINT:
                    return static_cast<int64_t>(data_) < static_cast<int64_t>(rhs.data_);
                case logical_type::FLOAT:
                    return value<float>() < rhs.value<float>();
                case logical_type::DOUBLE:
                    return value<double>() < rhs.value<double>();
                case logical_type::UTINYINT:
                    return static_cast<uint8_t>(data_) < static_cast<uint8_t>(rhs.data_);
                case logical_type::USMALLINT:
                    return static_cast<uint16_t>(data_) < static_cast<uint16_t>(rhs.data_);
                case logical_type::UINTEGER:
                    return static_cast<uint32_t>(data_) < static_cast<uint32_t>(rhs.data_);
                case logical_type::UBIGINT:
                    return data_ < rhs.data_;
                case logical_type::STRING_LITERAL:
                    return *str_ptr() < *rhs.str_ptr();
                default:
                    return false;
            }
        }
    }

    bool logical_value_t::operator>(const logical_value_t& rhs) const { return rhs < *this; }

    bool logical_value_t::operator<=(const logical_value_t& rhs) const { return !(*this > rhs); }

    bool logical_value_t::operator>=(const logical_value_t& rhs) const { return !(*this < rhs); }

    compare_t logical_value_t::compare(const logical_value_t& rhs) const {
        if (*this == rhs) {
            return compare_t::equals;
        } else if (*this < rhs) {
            return compare_t::less;
        } else {
            return compare_t::more;
        }
    }

    const std::vector<logical_value_t>& logical_value_t::children() const { return *vec_ptr(); }

    logical_value_t logical_value_t::create_struct(std::pmr::memory_resource* r,
                                                   const complex_logical_type& type,
                                                   const std::vector<logical_value_t>& struct_values) {
        logical_value_t result(r, complex_logical_type{logical_type::NA});
        result.data_ = reinterpret_cast<uint64_t>(result.heap_new<std::vector<logical_value_t>>(struct_values));
        result.type_ = type;
        return result;
    }

    logical_value_t logical_value_t::create_struct(std::pmr::memory_resource* r,
                                                   std::string name,
                                                   const std::vector<logical_value_t>& fields) {
        std::vector<complex_logical_type> child_types;
        for (auto& child : fields) {
            child_types.push_back(child.type());
        }
        return create_struct(r, complex_logical_type::create_struct(std::move(name), child_types), fields);
    }

    logical_value_t logical_value_t::create_array(std::pmr::memory_resource* r,
                                                  const complex_logical_type& internal_type,
                                                  const std::vector<logical_value_t>& values) {
        logical_value_t result(r, complex_logical_type{logical_type::NA});
        result.type_ = complex_logical_type::create_array(internal_type, values.size());
        result.data_ = reinterpret_cast<uint64_t>(result.heap_new<std::vector<logical_value_t>>(values));
        return result;
    }

    logical_value_t
    logical_value_t::create_numeric(std::pmr::memory_resource* r, const complex_logical_type& type, int64_t value) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
                assert(value == 0 || value == 1);
                return logical_value_t(r, value ? true : false);
            case logical_type::TINYINT:
                assert(value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max());
                return logical_value_t(r, static_cast<int8_t>(value));
            case logical_type::SMALLINT:
                assert(value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max());
                return logical_value_t(r, static_cast<int16_t>(value));
            case logical_type::INTEGER:
                assert(value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max());
                return logical_value_t(r, static_cast<int32_t>(value));
            case logical_type::BIGINT:
                return logical_value_t(r, value);
            case logical_type::UTINYINT:
                assert(value >= std::numeric_limits<uint8_t>::min() && value <= std::numeric_limits<uint8_t>::max());
                return logical_value_t(r, static_cast<uint8_t>(value));
            case logical_type::USMALLINT:
                assert(value >= std::numeric_limits<uint16_t>::min() && value <= std::numeric_limits<uint16_t>::max());
                return logical_value_t(r, static_cast<uint16_t>(value));
            case logical_type::UINTEGER:
                assert(value >= std::numeric_limits<uint32_t>::min() && value <= std::numeric_limits<uint32_t>::max());
                return logical_value_t(r, static_cast<uint32_t>(value));
            case logical_type::UBIGINT:
                assert(value >= 0);
                return logical_value_t(r, static_cast<uint64_t>(value));
            case logical_type::HUGEINT:
                return logical_value_t(r, static_cast<int128_t>(value));
            case logical_type::UHUGEINT:
                return logical_value_t(r, static_cast<uint128_t>(value));
            case logical_type::DECIMAL:
                return create_decimal(r,
                                      value,
                                      static_cast<decimal_logical_type_extension*>(type.extension())->width(),
                                      static_cast<decimal_logical_type_extension*>(type.extension())->scale());
            case logical_type::FLOAT:
                return logical_value_t(r, static_cast<float>(value));
            case logical_type::DOUBLE:
                return logical_value_t(r, static_cast<double>(value));
            case logical_type::POINTER:
                return logical_value_t(r, reinterpret_cast<void*>(value));
            default:
                throw std::runtime_error("logical_value_t::create_numeric: Numeric requires numeric type");
        }
    }

    logical_value_t logical_value_t::create_enum(std::pmr::memory_resource* r,
                                                 const complex_logical_type& enum_type,
                                                 std::string_view key) {
        const auto& enum_values =
            reinterpret_cast<const enum_logical_type_extension*>(enum_type.extension())->entries();
        auto it = std::find_if(enum_values.begin(), enum_values.end(), [key](const logical_value_t& v) {
            return v.type().alias() == key;
        });
        if (it == enum_values.end()) {
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        } else {
            logical_value_t result(r, enum_type);
            result.data_ = static_cast<uint64_t>(it->value<int32_t>());
            return result;
        }
    }

    logical_value_t
    logical_value_t::create_enum(std::pmr::memory_resource* r, const complex_logical_type& enum_type, int32_t value) {
        logical_value_t result(r, enum_type);
        result.data_ = static_cast<uint64_t>(value);
        return result;
    }

    logical_value_t
    logical_value_t::create_decimal(std::pmr::memory_resource* r, int64_t value, uint8_t width, uint8_t scale) {
        auto decimal_type = complex_logical_type::create_decimal(width, scale);
        logical_value_t result(r, decimal_type);
        result.data_ = static_cast<uint64_t>(value);
        return result;
    }

    logical_value_t logical_value_t::create_map(std::pmr::memory_resource* r,
                                                const complex_logical_type& key_type,
                                                const complex_logical_type& value_type,
                                                const std::vector<logical_value_t>& keys,
                                                const std::vector<logical_value_t>& values) {
        assert(keys.size() == values.size());
        logical_value_t result(r, complex_logical_type{logical_type::NA});
        result.type_ = complex_logical_type::create_map(key_type, value_type);
        auto keys_value = create_array(r, key_type, keys);
        auto values_value = create_array(r, value_type, values);
        result.data_ = reinterpret_cast<uint64_t>(
            result.heap_new<std::vector<logical_value_t>>(std::vector{std::move(keys_value), std::move(values_value)}));
        return result;
    }

    logical_value_t logical_value_t::create_map(std::pmr::memory_resource* r,
                                                const complex_logical_type& type,
                                                const std::vector<logical_value_t>& values) {
        std::vector<logical_value_t> map_keys;
        std::vector<logical_value_t> map_values;
        for (auto& val : values) {
            assert(val.type().type() == logical_type::STRUCT);
            auto& children = val.children();
            assert(children.size() == 2);
            map_keys.push_back(children[0]);
            map_values.push_back(children[1]);
        }
        auto& key_type = type.child_types()[0];
        auto& value_type = type.child_types()[1];
        return create_map(r, key_type, value_type, std::move(map_keys), std::move(map_values));
    }

    logical_value_t logical_value_t::create_list(std::pmr::memory_resource* r,
                                                 const complex_logical_type& internal_type,
                                                 const std::vector<logical_value_t>& values) {
        logical_value_t result(r, complex_logical_type{logical_type::NA});
        result.type_ = complex_logical_type::create_list(internal_type);
        result.data_ = reinterpret_cast<uint64_t>(result.heap_new<std::vector<logical_value_t>>(values));
        return result;
    }

    logical_value_t logical_value_t::create_union(std::pmr::memory_resource* r,
                                                  std::vector<complex_logical_type> types,
                                                  uint8_t tag,
                                                  logical_value_t value) {
        assert(!types.empty());
        assert(types.size() > tag);

        assert(value.type() == types[tag]);

        logical_value_t result(r, complex_logical_type{logical_type::NA});
        auto union_values = result.heap_new<std::vector<logical_value_t>>();
        union_values->emplace_back(r, static_cast<uint8_t>(tag));
        for (size_t i = 0; i < types.size(); i++) {
            if (i != tag) {
                union_values->emplace_back(r, types[i]);
            } else {
                union_values->emplace_back(r, nullptr);
            }
        }
        (*union_values)[static_cast<size_t>(tag) + 1] = std::move(value);
        result.data_ = reinterpret_cast<uint64_t>(union_values);
        result.type_ = complex_logical_type::create_union(std::move(types));
        return result;
    }

    logical_value_t logical_value_t::create_variant(std::pmr::memory_resource* r, std::vector<logical_value_t> values) {
        assert(values.size() == 4);
        assert(values[0].type().type() == logical_type::LIST);
        assert(values[1].type().type() == logical_type::LIST);
        assert(values[2].type().type() == logical_type::LIST);
        assert(values[3].type().type() == logical_type::BLOB);
        return create_struct(r, complex_logical_type::create_variant(), std::move(values));
    }

    /*
    * TODO: absl::int128 does not have implementations for all operations
    * Add them in operations_helper.hpp
    */
    template<typename OP, typename GET>
    logical_value_t op(const logical_value_t& value, GET getter_function) {
        OP operation{};
        return logical_value_t{value.resource(), operation((value.*getter_function)())};
    }

    template<typename OP, typename GET>
    logical_value_t op(const logical_value_t& value1, const logical_value_t& value2, GET getter_function) {
        using T = typename std::invoke_result<decltype(getter_function), logical_value_t>::type;
        OP operation{};
        auto* r = value1.resource() ? value1.resource() : value2.resource();
        if (value1.is_null()) {
            return logical_value_t{r, operation(T{}, (value2.*getter_function)())};
        } else if (value2.is_null()) {
            return logical_value_t{r, operation((value1.*getter_function)(), T{})};
        } else {
            return logical_value_t{r, operation((value1.*getter_function)(), (value2.*getter_function)())};
        }
    }

    logical_value_t logical_value_t::sum(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() &&
            value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return sum(value1.cast_as(complex_logical_type(promoted)),
                       value2.cast_as(complex_logical_type(promoted)));
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<uint128_t>);
            case logical_type::TIMESTAMP_SEC:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<std::chrono::seconds>);
            case logical_type::TIMESTAMP_MS:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<std::chrono::milliseconds>);
            case logical_type::TIMESTAMP_US:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<std::chrono::microseconds>);
            case logical_type::TIMESTAMP_NS:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<std::chrono::nanoseconds>);
            case logical_type::FLOAT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<double>);
            case logical_type::STRING_LITERAL:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<std::string>);
            default:
                throw std::runtime_error("logical_value_t::sum unable to process given types");
        }
    }

    logical_value_t logical_value_t::subtract(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() &&
            value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return subtract(value1.cast_as(complex_logical_type(promoted)),
                            value2.cast_as(complex_logical_type(promoted)));
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<uint128_t>);
            case logical_type::TIMESTAMP_SEC:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<std::chrono::seconds>);
            case logical_type::TIMESTAMP_MS:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<std::chrono::milliseconds>);
            case logical_type::TIMESTAMP_US:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<std::chrono::microseconds>);
            case logical_type::TIMESTAMP_NS:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<std::chrono::nanoseconds>);
            case logical_type::FLOAT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<double>);
            default:
                throw std::runtime_error("logical_value_t::subtract unable to process given types");
        }
    }

    logical_value_t logical_value_t::mult(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() &&
            value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return mult(value1.cast_as(complex_logical_type(promoted)),
                        value2.cast_as(complex_logical_type(promoted)));
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<uint128_t>);
            case logical_type::FLOAT:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<std::multiplies<>>(value1, value2, &logical_value_t::value<double>);
            default:
                throw std::runtime_error("logical_value_t::mult unable to process given types");
        }
    }

    logical_value_t logical_value_t::divide(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        // Division by zero: return 0 of the appropriate type
        if (!value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            auto zero = logical_value_t{r, value2.type()};
            if (value2 == zero) {
                auto result_type = value1.is_null() ? value2.type() : value1.type();
                return logical_value_t{r, result_type};
            }
        }

        if (!value1.is_null() && !value2.is_null() &&
            value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return divide(value1.cast_as(complex_logical_type(promoted)),
                          value2.cast_as(complex_logical_type(promoted)));
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<uint128_t>);
            case logical_type::FLOAT:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<std::divides<>>(value1, value2, &logical_value_t::value<double>);
            default:
                throw std::runtime_error("logical_value_t::divide unable to process given types");
        }
    }

    logical_value_t logical_value_t::modulus(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() &&
            value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return modulus(value1.cast_as(complex_logical_type(promoted)),
                           value2.cast_as(complex_logical_type(promoted)));
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<uint128_t>);
            case logical_type::TIMESTAMP_SEC:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<std::chrono::seconds>);
            case logical_type::TIMESTAMP_MS:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<std::chrono::milliseconds>);
            case logical_type::TIMESTAMP_US:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<std::chrono::microseconds>);
            case logical_type::TIMESTAMP_NS:
                return op<std::modulus<>>(value1, value2, &logical_value_t::value<std::chrono::nanoseconds>);
            default:
                throw std::runtime_error("logical_value_t::divide unable to process given types");
        }
    }

    logical_value_t logical_value_t::exponent(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<pow<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<pow<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<pow<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<pow<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<pow<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<pow<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<pow<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<pow<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<pow<>>(value1, value2, &logical_value_t::value<uint64_t>);
            // case logical_type::HUGEINT:
            // return op<pow<>>(value1, value2, &logical_value_t::value<int128_t>);
            // case logical_type::UHUGEINT:
            // return op<pow<>>(value1, value2, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::exponent unable to process given types");
        }
    }

    logical_value_t logical_value_t::sqr_root(const logical_value_t& value) {
        if (value.is_null()) {
            return value;
        }

        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return op<sqrt<>>(value, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<sqrt<>>(value, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<sqrt<>>(value, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<sqrt<>>(value, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<sqrt<>>(value, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<sqrt<>>(value, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<sqrt<>>(value, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<sqrt<>>(value, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<sqrt<>>(value, &logical_value_t::value<uint64_t>);
            // case logical_type::HUGEINT:
            // return op<sqrt<>>(value, &logical_value_t::value<int128_t>);
            // case logical_type::UHUGEINT:
            // return op<sqrt<>>(value, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::sqr_root unable to process given types");
        }
    }

    logical_value_t logical_value_t::cube_root(const logical_value_t& value) {
        if (value.is_null()) {
            return value;
        }

        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return op<cbrt<>>(value, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<cbrt<>>(value, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<cbrt<>>(value, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<cbrt<>>(value, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<cbrt<>>(value, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<cbrt<>>(value, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<cbrt<>>(value, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<cbrt<>>(value, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<cbrt<>>(value, &logical_value_t::value<uint64_t>);
            // case logical_type::HUGEINT:
            // return op<cbrt<>>(value, &logical_value_t::value<int128_t>);
            // case logical_type::UHUGEINT:
            // return op<cbrt<>>(value, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::cube_root unable to process given types");
        }
    }

    logical_value_t logical_value_t::factorial(const logical_value_t& value) {
        if (value.is_null()) {
            return value;
        }

        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return op<fact<>>(value, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<fact<>>(value, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<fact<>>(value, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<fact<>>(value, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<fact<>>(value, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<fact<>>(value, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<fact<>>(value, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<fact<>>(value, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<fact<>>(value, &logical_value_t::value<uint64_t>);
            // case logical_type::HUGEINT:
            // return op<fact<>>(value, &logical_value_t::value<int128_t>);
            // case logical_type::UHUGEINT:
            // return op<fact<>>(value, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::factorial unable to process given types");
        }
    }

    logical_value_t logical_value_t::absolute(const logical_value_t& value) {
        if (value.is_null()) {
            return value;
        }

        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return op<abs<>>(value, &logical_value_t::value<bool>);
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
            case logical_type::UHUGEINT:
                return value;
            case logical_type::TINYINT:
                return op<abs<>>(value, &logical_value_t::value<int8_t>);
            case logical_type::SMALLINT:
                return op<abs<>>(value, &logical_value_t::value<int16_t>);
            case logical_type::INTEGER:
                return op<abs<>>(value, &logical_value_t::value<int32_t>);
            case logical_type::BIGINT:
                return op<abs<>>(value, &logical_value_t::value<int64_t>);
            case logical_type::HUGEINT:
                return op<abs<>>(value, &logical_value_t::value<int128_t>);
            case logical_type::FLOAT:
                return op<abs<>>(value, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<abs<>>(value, &logical_value_t::value<double>);
            default:
                throw std::runtime_error("logical_value_t::absolute unable to process given types");
        }
    }
    logical_value_t logical_value_t::bit_and(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::bit_and<>>(value1, value2, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::bit_and unable to process given types");
        }
    }

    logical_value_t logical_value_t::bit_or(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::bit_or<>>(value1, value2, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::bit_or unable to process given types");
        }
    }

    logical_value_t logical_value_t::bit_xor(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::bit_xor<>>(value1, value2, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::bit_xor unable to process given types");
        }
    }

    logical_value_t logical_value_t::bit_not(const logical_value_t& value) {
        if (value.is_null()) {
            return value;
        }

        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return op<std::bit_not<>>(value, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<std::bit_not<>>(value, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<std::bit_not<>>(value, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<uint64_t>);
            case logical_type::HUGEINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<int128_t>);
            case logical_type::UHUGEINT:
                return op<std::bit_not<>>(value, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::bit_not unable to process given types");
        }
    }

    logical_value_t logical_value_t::bit_shift_l(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<shift_left<>>(value1, value2, &logical_value_t::value<uint64_t>);
            // case logical_type::HUGEINT:
            // return op<shift_left<>>(value1, value2, &logical_value_t::value<int128_t>);
            // case logical_type::UHUGEINT:
            // return op<shift_left<>>(value1, value2, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::bit_shift_l unable to process given types");
        }
    }

    logical_value_t logical_value_t::bit_shift_r(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        auto type = value1.is_null() ? value2.type().type() : value1.type().type();
        switch (type) {
            case logical_type::BOOLEAN:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<bool>);
            case logical_type::TINYINT:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<int8_t>);
            case logical_type::UTINYINT:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<uint8_t>);
            case logical_type::SMALLINT:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<int16_t>);
            case logical_type::USMALLINT:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<uint16_t>);
            case logical_type::INTEGER:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<int32_t>);
            case logical_type::UINTEGER:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<uint32_t>);
            case logical_type::BIGINT:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<int64_t>);
            case logical_type::UBIGINT:
                return op<shift_right<>>(value1, value2, &logical_value_t::value<uint64_t>);
            // case logical_type::HUGEINT:
            // return op<shift_right<>>(value1, value2, &logical_value_t::value<int128_t>);
            // case logical_type::UHUGEINT:
            // return op<shift_right<>>(value1, value2, &logical_value_t::value<uint128_t>);
            default:
                throw std::runtime_error("logical_value_t::bit_shift_r unable to process given types");
        }
    }

    void logical_value_t::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        type_.serialize(serializer);
        switch (type_.type()) {
            case logical_type::BOOLEAN:
                serializer->append(value<bool>());
                break;
            case logical_type::TINYINT:
                serializer->append(static_cast<int64_t>(value<int8_t>()));
                break;
            case logical_type::SMALLINT:
                serializer->append(static_cast<int64_t>(value<int16_t>()));
                break;
            case logical_type::INTEGER:
                serializer->append(static_cast<int64_t>(value<int32_t>()));
                break;
            case logical_type::BIGINT:
                serializer->append(value<int64_t>());
                break;
            case logical_type::FLOAT:
                serializer->append(value<float>());
                break;
            case logical_type::DOUBLE:
                serializer->append(value<double>());
                break;
            case logical_type::UTINYINT:
                serializer->append(static_cast<uint64_t>(value<uint8_t>()));
                break;
            case logical_type::USMALLINT:
                serializer->append(static_cast<uint64_t>(value<uint16_t>()));
                break;
            case logical_type::UINTEGER:
                serializer->append(static_cast<uint64_t>(value<uint32_t>()));
                break;
            case logical_type::UBIGINT:
                serializer->append(value<uint64_t>());
                break;
            case logical_type::HUGEINT:
                serializer->append(value<int128_t>());
                break;
            case logical_type::UHUGEINT:
                serializer->append(value<uint128_t>());
                break;
            case logical_type::TIMESTAMP_NS:
            case logical_type::TIMESTAMP_US:
            case logical_type::TIMESTAMP_MS:
            case logical_type::TIMESTAMP_SEC:
                serializer->append(value<int64_t>());
                break;
            case logical_type::STRING_LITERAL:
                serializer->append(*str_ptr());
                break;
            case logical_type::POINTER:
                assert(false && "not safe to serialize a pointer");
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT: {
                const auto& nested_values = *vec_ptr();
                serializer->start_array(nested_values.size());
                for (const auto& val : nested_values) {
                    val.serialize(serializer);
                }
                serializer->end_array();
                break;
            }
            default:
                serializer->append_null();
                serializer->end_array();
        }
    }

    logical_value_t logical_value_t::deserialize(std::pmr::memory_resource* r,
                                                 serializer::msgpack_deserializer_t* deserializer) {
        logical_value_t result(r, complex_logical_type{logical_type::NA});
        deserializer->advance_array(0);
        auto type = complex_logical_type::deserialize(r, deserializer);
        deserializer->pop_array();
        switch (type.type()) {
            case logical_type::BOOLEAN:
                result = logical_value_t(r, deserializer->deserialize_bool(1));
                break;
            case logical_type::TINYINT:
                result = logical_value_t(r, static_cast<int8_t>(deserializer->deserialize_int64(1)));
                break;
            case logical_type::SMALLINT:
                result = logical_value_t(r, static_cast<int16_t>(deserializer->deserialize_int64(1)));
                break;
            case logical_type::INTEGER:
                result = logical_value_t(r, static_cast<int32_t>(deserializer->deserialize_int64(1)));
                break;
            case logical_type::BIGINT:
                result = logical_value_t(r, deserializer->deserialize_int64(1));
                break;
            case logical_type::FLOAT:
                result = logical_value_t(r, static_cast<float>(deserializer->deserialize_double(1)));
                break;
            case logical_type::DOUBLE:
                result = logical_value_t(r, deserializer->deserialize_double(1));
                break;
            case logical_type::UTINYINT:
                result = logical_value_t(r, static_cast<uint8_t>(deserializer->deserialize_uint64(1)));
                break;
            case logical_type::USMALLINT:
                result = logical_value_t(r, static_cast<uint16_t>(deserializer->deserialize_uint64(1)));
                break;
            case logical_type::UINTEGER:
                result = logical_value_t(r, static_cast<uint32_t>(deserializer->deserialize_uint64(1)));
                break;
            case logical_type::UBIGINT:
                result = logical_value_t(r, deserializer->deserialize_uint64(1));
                break;
            case logical_type::HUGEINT:
                result = logical_value_t(r, deserializer->deserialize_uint128(1));
                break;
            case logical_type::UHUGEINT:
                result = logical_value_t(r, deserializer->deserialize_int128(1));
                break;
            case logical_type::TIMESTAMP_NS:
                result = logical_value_t(r, std::chrono::nanoseconds(deserializer->deserialize_int64(1)));
                break;
            case logical_type::TIMESTAMP_US:
                result = logical_value_t(r, std::chrono::microseconds(deserializer->deserialize_int64(1)));
                break;
            case logical_type::TIMESTAMP_MS:
                result = logical_value_t(r, std::chrono::milliseconds(deserializer->deserialize_int64(1)));
                break;
            case logical_type::TIMESTAMP_SEC:
                result = logical_value_t(r, std::chrono::seconds(deserializer->deserialize_int64(1)));
                break;
            case logical_type::STRING_LITERAL:
                result = logical_value_t(r, deserializer->deserialize_string(1));
                break;
            case logical_type::POINTER:
                assert(false && "not safe to deserialize a pointer");
                break;
            case logical_type::LIST: {
                std::vector<logical_value_t> nested_values;
                deserializer->advance_array(1);
                nested_values.reserve(deserializer->current_array_size());
                for (size_t i = 0; i < nested_values.capacity(); i++) {
                    deserializer->advance_array(i);
                    nested_values.emplace_back(deserialize(r, deserializer));
                    deserializer->pop_array();
                }
                deserializer->pop_array();
                result = create_list(r, type, std::move(nested_values));
                break;
            }
            case logical_type::ARRAY: {
                std::vector<logical_value_t> nested_values;
                deserializer->advance_array(1);
                nested_values.reserve(deserializer->current_array_size());
                for (size_t i = 0; i < nested_values.capacity(); i++) {
                    deserializer->advance_array(i);
                    nested_values.emplace_back(deserialize(r, deserializer));
                    deserializer->pop_array();
                }
                deserializer->pop_array();
                result = create_struct(r, type, std::move(nested_values));
                break;
            }
            case logical_type::MAP: {
                std::vector<logical_value_t> nested_values;
                deserializer->advance_array(1);
                nested_values.reserve(deserializer->current_array_size());
                for (size_t i = 0; i < nested_values.capacity(); i++) {
                    deserializer->advance_array(i);
                    nested_values.emplace_back(deserialize(r, deserializer));
                    deserializer->pop_array();
                }
                deserializer->pop_array();
                result = create_map(r, type, std::move(nested_values));
                break;
            }
            case logical_type::STRUCT: {
                std::vector<logical_value_t> nested_values;
                deserializer->advance_array(1);
                nested_values.reserve(deserializer->current_array_size());
                for (size_t i = 0; i < nested_values.capacity(); i++) {
                    deserializer->advance_array(i);
                    nested_values.emplace_back(deserialize(r, deserializer));
                    deserializer->pop_array();
                }
                deserializer->pop_array();
                result = create_struct(r, type, std::move(nested_values));
                break;
            }
            default:
                assert(false);
                return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }
        if (type.has_alias()) {
            result.set_alias(type.alias());
        }

        return result;
    }

    bool serialize_type_matches(const complex_logical_type& expected_type, const complex_logical_type& actual_type) {
        if (expected_type.type() != actual_type.type()) {
            return false;
        }
        if (expected_type.is_nested()) {
            return true;
        }
        return expected_type == actual_type;
    }

} // namespace components::types