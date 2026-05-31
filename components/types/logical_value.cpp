#include "logical_value.hpp"
#include "operations_helper.hpp"
#include <core/date/date_cast.hpp>

#include <boost/container_hash/hash.hpp>
#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace components::types {

    namespace {
        template<typename T>
        inline constexpr bool ext_is_signed_v = std::is_signed_v<T> || std::is_same_v<T, int128_t>;
    }

    logical_value_t::~logical_value_t() { destroy_heap(); }

    void logical_value_t::destroy_heap() {
        if (!data_) {
            return;
        }
        switch (type_.type()) {
            case logical_type::STRING_LITERAL:
                heap_delete(str_ptr());
                break;
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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

    logical_value_t::logical_value_t(std::pmr::memory_resource* r, const logical_value_t& other)
        : type_(other.type_)
        , resource_(r) {
        switch (type_.type()) {
            case logical_type::HUGEINT:
                data128_ = other.data128_;
                break;
            case logical_type::UHUGEINT:
                udata128_ = other.udata128_;
                break;
            case logical_type::DECIMAL:
                if (reinterpret_cast<decimal_logical_type_extension*>(type_.extension())->stored_as() ==
                    physical_type::INT128) {
                    data128_ = other.data128_;
                } else {
                    data_ = other.data_;
                }
                break;
            case logical_type::STRING_LITERAL:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::string>(*other.str_ptr()));
                break;
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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
            case logical_type::DECIMAL:
                if (reinterpret_cast<decimal_logical_type_extension*>(type_.extension())->stored_as() ==
                    physical_type::INT128) {
                    data128_ = other.data128_;
                } else {
                    data_ = other.data_;
                }
                break;
            case logical_type::STRING_LITERAL:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::string>(*other.str_ptr()));
                break;
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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
            case logical_type::DECIMAL:
                if (reinterpret_cast<decimal_logical_type_extension*>(type_.extension())->stored_as() ==
                    physical_type::INT128) {
                    data128_ = other.data128_;
                } else {
                    data_ = other.data_;
                }
                break;
            case logical_type::STRING_LITERAL:
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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
            case logical_type::DECIMAL:
                if (reinterpret_cast<decimal_logical_type_extension*>(type_.extension())->stored_as() ==
                    physical_type::INT128) {
                    data128_ = other.data128_;
                } else {
                    data_ = other.data_;
                }
                break;
            case logical_type::STRING_LITERAL:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::string>(*other.str_ptr()));
                break;
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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
            case logical_type::DECIMAL:
                if (reinterpret_cast<decimal_logical_type_extension*>(type_.extension())->stored_as() ==
                    physical_type::INT128) {
                    data128_ = other.data128_;
                } else {
                    data_ = other.data_;
                }
                break;
            case logical_type::STRING_LITERAL:
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
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
                if constexpr (ext_is_signed_v<RightValueType>) {
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

    logical_value_t logical_value_t::cast_as(const complex_logical_type& type,
                                             core::date::timezone_offset_t session_tz) const {
        if (type_ == type) {
            return logical_value_t(*this);
        }
        if (is_numeric(type.type()) || (type.type() == logical_type::STRING_LITERAL && is_numeric(type_.type()))) {
            // same problem as in physical_value
            // ideally use something like this
            // return logicaL_value<type.type()>{value<type_.type()>()};
            // but type is not a constexpr, so here is a huge switch:

            return double_simple_physical_type_switch<cast_callback_t>(type.to_physical_type(),
                                                                       type_.to_physical_type(),
                                                                       *this);
        } else if (type.type() == logical_type::DECIMAL && is_numeric(type_.type())) {
            const auto* decimal_extension = reinterpret_cast<const decimal_logical_type_extension*>(type.extension());
            auto create_decimal = [&]<typename T>() {
                return logical_value_t::create_decimal(
                    resource_,
                    type,
                    to_decimal<int128_t>(value<T>(), decimal_extension->width(), decimal_extension->scale()));
            };
            switch (type_.type()) {
                case logical_type::USMALLINT:
                    return create_decimal.operator()<uint16_t>();
                case logical_type::UINTEGER:
                    return create_decimal.operator()<uint32_t>();
                case logical_type::UBIGINT:
                    return create_decimal.operator()<uint64_t>();
                case logical_type::UHUGEINT:
                    return create_decimal.operator()<uint128_t>();
                case logical_type::SMALLINT:
                    return create_decimal.operator()<int16_t>();
                case logical_type::INTEGER:
                    return create_decimal.operator()<int32_t>();
                case logical_type::BIGINT:
                    return create_decimal.operator()<int64_t>();
                case logical_type::HUGEINT:
                    return create_decimal.operator()<int128_t>();
                case logical_type::FLOAT:
                    return create_decimal.operator()<float>();
                case logical_type::DOUBLE:
                    return create_decimal.operator()<double>();
                default:
                    assert(false && "incorrect type for conversion to decimal");
            }
        } else if (type_.type() == logical_type::DECIMAL && is_numeric(type.type())) {
            const auto* decimal_extension = reinterpret_cast<const decimal_logical_type_extension*>(type.extension());
            auto create_numeric_inner = [&]<typename From, typename To>() {
                if constexpr (std::is_floating_point_v<To>) {
                    return logical_value_t{resource_,
                                           decimal_to_floating<From, To>(value<From>(), decimal_extension->scale())};
                } else {
                    auto val = decimal_to_numeric<From, To>(value<From>(), decimal_extension->scale());
                    if (val.has_value()) {
                        return logical_value_t{resource_, val.value()};
                    } else {
                        return logical_value_t{resource_, logical_type::NA};
                    }
                }
            };
            auto create_numeric = [&]<typename To>() {
                switch (type_.to_physical_type()) {
                    case physical_type::INT16:
                        return create_numeric_inner.operator()<int16_t, To>();
                    case physical_type::INT32:
                        return create_numeric_inner.operator()<int32_t, To>();
                    case physical_type::INT64:
                        return create_numeric_inner.operator()<int64_t, To>();
                    case physical_type::INT128:
                        return create_numeric_inner.operator()<int128_t, To>();
                    default:
                        assert(false && "incorrect type for conversion to decimal");
                        return logical_value_t{resource_, logical_type::NA};
                }
            };
            switch (type.type()) {
                case logical_type::USMALLINT:
                    return create_numeric.operator()<uint16_t>();
                case logical_type::UINTEGER:
                    return create_numeric.operator()<uint32_t>();
                case logical_type::UBIGINT:
                    return create_numeric.operator()<uint64_t>();
                case logical_type::UHUGEINT:
                    return create_numeric.operator()<uint128_t>();
                case logical_type::SMALLINT:
                    return create_numeric.operator()<int16_t>();
                case logical_type::INTEGER:
                    return create_numeric.operator()<int32_t>();
                case logical_type::BIGINT:
                    return create_numeric.operator()<int64_t>();
                case logical_type::HUGEINT:
                    return create_numeric.operator()<int128_t>();
                case logical_type::FLOAT:
                    return create_numeric.operator()<float>();
                case logical_type::DOUBLE:
                    return create_numeric.operator()<double>();
                default:
                    assert(false && "incorrect type for conversion to decimal");
            }
        } else if (type_.type() == logical_type::STRUCT && type.type() == logical_type::STRUCT) {
            if (type_.child_types().size() != type.child_types().size()) {
                assert(false && "incorrect type");
                return logical_value_t{resource_, complex_logical_type{logical_type::NA}};
            }

            std::vector<logical_value_t> fields;
            fields.reserve(children().size());
            for (size_t i = 0; i < children().size(); i++) {
                fields.emplace_back(children()[i].cast_as(type.child_types()[i], session_tz));
            }

            return create_struct(resource_, type, fields);
        } else if (type_.type() == logical_type::ARRAY && type.type() == logical_type::ARRAY) {
            const auto& target_elem_type = type.child_type();
            std::vector<logical_value_t> elems;
            elems.reserve(children().size());
            for (const auto& child : children()) {
                elems.emplace_back(child.cast_as(target_elem_type, session_tz));
            }
            return create_array(resource_, target_elem_type, elems);
        } else if (type.type() == logical_type::ENUM) {
            if (type_.type() == logical_type::STRING_LITERAL) {
                auto string_val = value<std::string_view>();
                for (const auto& entry : static_cast<const enum_logical_type_extension*>(type.extension())->entries()) {
                    if (entry.type().alias() == string_val) {
                        logical_value_t result(resource_, type);
                        result.data_ = entry.data_;
                        return result;
                    }
                }
                return logical_value_t{resource_, complex_logical_type{logical_type::NA}};
            } else if (is_numeric(type_.type())) {
                const auto& enum_entries = static_cast<const enum_logical_type_extension*>(type.extension())->entries();
                auto src_as_enum = double_simple_physical_type_switch<cast_callback_t>(type.to_physical_type(),
                                                                                       type_.to_physical_type(),
                                                                                       *this);
                for (const auto& entry : enum_entries) {
                    if (src_as_enum.data_ == entry.data_) {
                        logical_value_t result(resource_, type);
                        result.data_ = src_as_enum.data_;
                        return result;
                    }
                }
                return logical_value_t{resource_, complex_logical_type{logical_type::NA}};
            }
        } else if (is_duration(type_.type()) && is_duration(type.type())) {
            using namespace core;
            switch (type_.type()) {
                case logical_type::DATE:
                    switch (type.type()) {
                        case logical_type::TIMESTAMP:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::timestamp_t>(value<date::date_t>(), session_tz)};
                        case logical_type::TIMESTAMP_TZ:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::timestamptz_t>(value<date::date_t>(), session_tz)};
                        default:
                            break;
                    }
                    break;
                case logical_type::TIMESTAMP:
                    switch (type.type()) {
                        case logical_type::DATE:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::date_t>(value<date::timestamp_t>(), session_tz)};
                        case logical_type::TIME:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::time_t>(value<date::timestamp_t>(), session_tz)};
                        case logical_type::TIMESTAMP_TZ:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::timestamptz_t>(value<date::timestamp_t>(), session_tz)};
                        default:
                            break;
                    }
                    break;
                case logical_type::TIMESTAMP_TZ:
                    switch (type.type()) {
                        case logical_type::DATE:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::date_t>(value<date::timestamptz_t>(), session_tz)};
                        case logical_type::TIME:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::time_t>(value<date::timestamptz_t>(), session_tz)};
                        case logical_type::TIMESTAMP:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::timestamp_t>(value<date::timestamptz_t>(), session_tz)};
                        case logical_type::TIME_TZ:
                            return logical_value_t{
                                resource_,
                                convert_date_time<date::timetz_t>(value<date::timestamptz_t>(), session_tz)};
                        default:
                            break;
                    }
                    break;
                case logical_type::TIME:
                    if (type.type() == logical_type::TIME_TZ)
                        return logical_value_t{resource_,
                                               convert_date_time<date::timetz_t>(value<date::time_t>(), session_tz)};
                    break;
                case logical_type::TIME_TZ:
                    if (type.type() == logical_type::TIME)
                        return logical_value_t{resource_,
                                               convert_date_time<date::time_t>(value<date::timetz_t>(), session_tz)};
                    break;
                default:
                    break;
            }
        }
        // assert(false && "cast to value is not implemented");
        return logical_value_t{resource_, complex_logical_type{logical_type::NA}};
    }

    void logical_value_t::set_alias(const std::string& alias) { type_.set_alias(alias); }

    size_t logical_value_t::hash() const noexcept {
        size_t h = std::hash<uint8_t>{}(static_cast<uint8_t>(type_.type()));
        switch (type_.type()) {
            case logical_type::NA:
                break;
            case logical_type::STRING_LITERAL:
                if (data_) {
                    boost::hash_combine(h, std::hash<std::string>{}(*str_ptr()));
                }
                break;
            case logical_type::HUGEINT:
                boost::hash_combine(h, static_cast<uint64_t>(data128_));
                boost::hash_combine(h, static_cast<uint64_t>(data128_ >> 64));
                break;
            case logical_type::UHUGEINT:
                boost::hash_combine(h, static_cast<uint64_t>(udata128_));
                boost::hash_combine(h, static_cast<uint64_t>(udata128_ >> 64));
                break;
            default:
                boost::hash_combine(h, data_);
                break;
        }
        return h;
    }

    size_t hash_row(const std::pmr::vector<logical_value_t>& row) noexcept {
        size_t h = 0;
        for (const auto& val : row) {
            boost::hash_combine(h, val.hash());
        }
        return h;
    }

    bool logical_value_t::operator==(const logical_value_t& rhs) const {
        assert(type_ == rhs.type_ && "logical_value_t has to be casted to the same type before comparison");
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
            case logical_type::ENUM:
                return data_ == rhs.data_;
            case logical_type::FLOAT:
                return core::is_equals(value<float>(), rhs.value<float>());
            case logical_type::DOUBLE:
                return core::is_equals(value<double>(), rhs.value<double>());
            case logical_type::STRING_LITERAL:
                return *str_ptr() == *rhs.str_ptr();
            case logical_type::DECIMAL:
                if (type_.to_physical_type() == physical_type::INT128) {
                    return data128_ == rhs.data128_;
                } else {
                    return data_ == rhs.data_;
                }
            case logical_type::DATE:
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                return data_ == rhs.data_;
            case logical_type::TIME_TZ:
                return value<core::date::timetz_t>() == rhs.value<core::date::timetz_t>();
            case logical_type::INTERVAL:
                return value<core::date::interval_t>() == rhs.value<core::date::interval_t>();
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

    bool logical_value_t::operator!=(const logical_value_t& rhs) const { return !(*this == rhs); }

    bool logical_value_t::operator<(const logical_value_t& rhs) const {
        assert(type_ == rhs.type_ && "logical_value_t has to be casted to the same type before comparison");
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
            case logical_type::DECIMAL:
                if (type_.to_physical_type() == physical_type::INT128) {
                    return data128_ < rhs.data128_;
                } else {
                    return data_ < rhs.data_;
                }
            case logical_type::DATE:
                return static_cast<int32_t>(data_) < static_cast<int32_t>(rhs.data_);
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                return static_cast<int64_t>(data_) < static_cast<int64_t>(rhs.data_);
            case logical_type::TIME_TZ:
                return value<core::date::timetz_t>() < rhs.value<core::date::timetz_t>();
            case logical_type::INTERVAL:
                return value<core::date::interval_t>() < rhs.value<core::date::interval_t>();
            case logical_type::STRUCT:
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP: {
                auto& lv = *vec_ptr();
                auto& rv = *rhs.vec_ptr();
                return std::lexicographical_compare(lv.begin(), lv.end(), rv.begin(), rv.end());
            }
            default:
                return false;
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
        std::pmr::vector<complex_logical_type> child_types(r);
        child_types.reserve(fields.size());
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
                return create_decimal(r, type, value);
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

    logical_value_t logical_value_t::create_decimal(std::pmr::memory_resource* r,
                                                    const complex_logical_type& decimal_type,
                                                    int64_t value) {
        logical_value_t result(r, decimal_type);
        result.data_ = static_cast<uint64_t>(value);
        return result;
    }

    logical_value_t logical_value_t::create_decimal(std::pmr::memory_resource* r,
                                                    const complex_logical_type& decimal_type,
                                                    int128_t value) {
        logical_value_t result(r, decimal_type);
        if (decimal_type.to_physical_type() == physical_type::INT128) {
            result.data128_ = value;
        } else {
            result.data_ = static_cast<uint64_t>(value);
        }
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
                                                  std::pmr::vector<complex_logical_type> types,
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
        return create_struct(r, complex_logical_type::create_variant(r), std::move(values));
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

    // session timezone cancels out in arithmetics, so we don't have to pass it
    constexpr auto place_holder_time_zone = core::date::timezone_offset_t{};

    logical_value_t logical_value_t::sum(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() && value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return sum(value1.cast_as(complex_logical_type(promoted), place_holder_time_zone),
                       value2.cast_as(complex_logical_type(promoted), place_holder_time_zone));
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
            case logical_type::FLOAT:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<double>);
            case logical_type::STRING_LITERAL:
                return op<std::plus<>>(value1, value2, &logical_value_t::value<std::string>);
            default:
                break;
        }
        // Temporal arithmetic (scalar-scalar path used by predicate evaluation)
        using namespace core::date;
        const auto t1 = value1.type().type();
        const auto t2 = value2.type().type();
        auto* r = value1.resource() ? value1.resource() : value2.resource();
        // DATE + INTERVAL → DATE
        if (t1 == logical_type::DATE && t2 == logical_type::INTERVAL) {
            const auto d = value1.value<date_t>().value;
            const auto iv = value2.value<interval_t>();
            auto sd = pg_epoch + std::chrono::days{d.count()};
            if (iv.month.count())
                sd = apply_months(sd, iv.month.count());
            sd += std::chrono::days{iv.day.count()};
            return logical_value_t{r, date_t{days{static_cast<int32_t>((sd - pg_epoch).count())}}};
        }
        // INTERVAL + DATE → DATE
        if (t1 == logical_type::INTERVAL && t2 == logical_type::DATE) {
            return logical_value_t::sum(value2, value1);
        }
        // TIMESTAMP/TZ + INTERVAL → TIMESTAMP/TZ
        if ((t1 == logical_type::TIMESTAMP || t1 == logical_type::TIMESTAMP_TZ) && t2 == logical_type::INTERVAL) {
            const auto ts = (t1 == logical_type::TIMESTAMP) ? value1.value<timestamp_t>().value
                                                            : value1.value<timestamptz_t>().value;
            const auto iv = value2.value<interval_t>();
            auto [d, tod] = split_timestamp(ts);
            auto sd = pg_epoch + std::chrono::days{d.count()};
            if (iv.month.count())
                sd = apply_months(sd, iv.month.count());
            sd += std::chrono::days{iv.day.count()};
            const auto result = from_sys_days_us(sd, tod + iv.time);
            if (t1 == logical_type::TIMESTAMP) {
                return logical_value_t{r, timestamp_t{result}};
            }
            return logical_value_t{r, timestamptz_t{result}};
        }
        // INTERVAL + TIMESTAMP/TZ → TIMESTAMP/TZ
        if (t1 == logical_type::INTERVAL && (t2 == logical_type::TIMESTAMP || t2 == logical_type::TIMESTAMP_TZ)) {
            return logical_value_t::sum(value2, value1);
        }
        // INTERVAL + INTERVAL → INTERVAL
        if (t1 == logical_type::INTERVAL && t2 == logical_type::INTERVAL) {
            const auto iv1 = value1.value<interval_t>();
            const auto iv2 = value2.value<interval_t>();
            return logical_value_t{r, interval_t{iv1.time + iv2.time, iv1.day + iv2.day, iv1.month + iv2.month}};
        }
        constexpr auto one_day = std::chrono::duration_cast<microseconds>(days{1});
        // TIME + INTERVAL → TIME (wrap-around)
        if (t1 == logical_type::TIME && t2 == logical_type::INTERVAL) {
            auto result = (value1.value<core::date::time_t>().value + value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, core::date::time_t{result}};
        }
        // INTERVAL + TIME → TIME (commutative)
        if (t1 == logical_type::INTERVAL && t2 == logical_type::TIME) {
            return logical_value_t::sum(value2, value1);
        }
        // TIME_TZ + INTERVAL → TIME_TZ (apply to local time, preserve offset)
        if (t1 == logical_type::TIME_TZ && t2 == logical_type::INTERVAL) {
            const auto tz = value1.value<timetz_t>();
            auto result = (tz.time + value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, timetz_t{result, tz.zone}};
        }
        // INTERVAL + TIME_TZ → TIME_TZ (commutative)
        if (t1 == logical_type::INTERVAL && t2 == logical_type::TIME_TZ) {
            return logical_value_t::sum(value2, value1);
        }
        throw std::runtime_error("logical_value_t::sum unable to process given types");
    }

    logical_value_t logical_value_t::subtract(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() && value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return subtract(value1.cast_as(complex_logical_type(promoted), place_holder_time_zone),
                            value2.cast_as(complex_logical_type(promoted), place_holder_time_zone));
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
            case logical_type::FLOAT:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<float>);
            case logical_type::DOUBLE:
                return op<std::minus<>>(value1, value2, &logical_value_t::value<double>);
            default:
                break;
        }
        using namespace core::date;
        const auto t1 = value1.type().type();
        const auto t2 = value2.type().type();
        auto* r = value1.resource() ? value1.resource() : value2.resource();
        constexpr auto one_day = std::chrono::duration_cast<microseconds>(days{1});
        // DATE - INTERVAL → DATE
        if (t1 == logical_type::DATE && t2 == logical_type::INTERVAL) {
            const auto iv = value2.value<interval_t>();
            auto sd = pg_epoch + std::chrono::days{value1.value<date_t>().value.count()};
            if (iv.month.count())
                sd = apply_months(sd, -iv.month.count());
            sd -= std::chrono::days{iv.day.count()};
            return logical_value_t{r, date_t{days{static_cast<int32_t>((sd - pg_epoch).count())}}};
        }
        // TIMESTAMP/TZ - INTERVAL → TIMESTAMP/TZ
        if ((t1 == logical_type::TIMESTAMP || t1 == logical_type::TIMESTAMP_TZ) && t2 == logical_type::INTERVAL) {
            const auto ts = (t1 == logical_type::TIMESTAMP) ? value1.value<timestamp_t>().value
                                                            : value1.value<timestamptz_t>().value;
            const auto iv = value2.value<interval_t>();
            auto [d, tod] = split_timestamp(ts);
            auto sd = pg_epoch + std::chrono::days{d.count()};
            if (iv.month.count())
                sd = apply_months(sd, -iv.month.count());
            sd -= std::chrono::days{iv.day.count()};
            const auto result = from_sys_days_us(sd, tod - iv.time);
            if (t1 == logical_type::TIMESTAMP) {
                return logical_value_t{r, timestamp_t{result}};
            }
            return logical_value_t{r, timestamptz_t{result}};
        }
        // TIME - INTERVAL → TIME (wrap-around)
        if (t1 == logical_type::TIME && t2 == logical_type::INTERVAL) {
            auto result = (value1.value<core::date::time_t>().value - value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, core::date::time_t{result}};
        }
        // TIME_TZ - INTERVAL → TIME_TZ (wrap-around, preserve offset)
        if (t1 == logical_type::TIME_TZ && t2 == logical_type::INTERVAL) {
            const auto tz = value1.value<timetz_t>();
            auto result = (tz.time - value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, timetz_t{result, tz.zone}};
        }
        // INTERVAL - INTERVAL → INTERVAL
        if (t1 == logical_type::INTERVAL && t2 == logical_type::INTERVAL) {
            const auto iv1 = value1.value<interval_t>();
            const auto iv2 = value2.value<interval_t>();
            return logical_value_t{r, interval_t{iv1.time - iv2.time, iv1.day - iv2.day, iv1.month - iv2.month}};
        }
        // DATE - DATE → INTERVAL (days component)
        if (t1 == logical_type::DATE && t2 == logical_type::DATE) {
            return logical_value_t{
                r,
                interval_t{microseconds{0}, value1.value<date_t>().value - value2.value<date_t>().value, months{0}}};
        }
        // TIMESTAMP/TZ - TIMESTAMP/TZ → INTERVAL (µs component)
        if ((t1 == logical_type::TIMESTAMP || t1 == logical_type::TIMESTAMP_TZ) &&
            (t2 == logical_type::TIMESTAMP || t2 == logical_type::TIMESTAMP_TZ)) {
            const auto ts1 = (t1 == logical_type::TIMESTAMP) ? value1.value<timestamp_t>().value
                                                             : value1.value<timestamptz_t>().value;
            const auto ts2 = (t2 == logical_type::TIMESTAMP) ? value2.value<timestamp_t>().value
                                                             : value2.value<timestamptz_t>().value;
            return logical_value_t{r, interval_t{ts1 - ts2, days{0}, months{0}}};
        }
        // TIME - TIME → INTERVAL
        if (t1 == logical_type::TIME && t2 == logical_type::TIME) {
            return logical_value_t{
                r,
                interval_t{value1.value<core::date::time_t>().value - value2.value<core::date::time_t>().value,
                           days{0},
                           months{0}}};
        }
        // TIME_TZ - TIME_TZ → INTERVAL (UTC-normalized)
        if (t1 == logical_type::TIME_TZ && t2 == logical_type::TIME_TZ) {
            const auto tz1 = value1.value<timetz_t>();
            const auto tz2 = value2.value<timetz_t>();
            const auto utc1 = tz1.time - std::chrono::duration_cast<microseconds>(tz1.zone);
            const auto utc2 = tz2.time - std::chrono::duration_cast<microseconds>(tz2.zone);
            return logical_value_t{r, interval_t{utc1 - utc2, days{0}, months{0}}};
        }
        throw std::runtime_error("logical_value_t::subtract unable to process given types");
    }

    logical_value_t logical_value_t::mult(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() && value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return mult(value1.cast_as(complex_logical_type(promoted), place_holder_time_zone),
                        value2.cast_as(complex_logical_type(promoted), place_holder_time_zone));
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
                break;
        }
        auto* r = value1.resource() ? value1.resource() : value2.resource();
        const auto t1 = value1.type().type();
        const auto t2 = value2.type().type();
        auto as_double = [](const logical_value_t& v) -> double {
            switch (v.type().type()) {
                case logical_type::TINYINT:
                    return static_cast<double>(v.value<int8_t>());
                case logical_type::UTINYINT:
                    return static_cast<double>(v.value<uint8_t>());
                case logical_type::SMALLINT:
                    return static_cast<double>(v.value<int16_t>());
                case logical_type::USMALLINT:
                    return static_cast<double>(v.value<uint16_t>());
                case logical_type::INTEGER:
                    return static_cast<double>(v.value<int32_t>());
                case logical_type::UINTEGER:
                    return static_cast<double>(v.value<uint32_t>());
                case logical_type::BIGINT:
                    return static_cast<double>(v.value<int64_t>());
                case logical_type::UBIGINT:
                    return static_cast<double>(v.value<uint64_t>());
                case logical_type::FLOAT:
                    return static_cast<double>(v.value<float>());
                case logical_type::DOUBLE:
                    return v.value<double>();
                default:
                    return 0.0;
            }
        };
        using namespace core::date;
        // INTERVAL * numeric → INTERVAL
        if (t1 == logical_type::INTERVAL && is_numeric(t2)) {
            const double f = as_double(value2);
            const auto iv = value1.value<interval_t>();
            return logical_value_t{
                r,
                interval_t{microseconds{std::llround(static_cast<double>(iv.time.count()) * f)},
                           days{static_cast<int32_t>(std::llround(static_cast<double>(iv.day.count()) * f))},
                           months{static_cast<int32_t>(std::llround(static_cast<double>(iv.month.count()) * f))}}};
        }
        // numeric * INTERVAL → INTERVAL (commutative)
        if (is_numeric(t1) && t2 == logical_type::INTERVAL) {
            return logical_value_t::mult(value2, value1);
        }
        throw std::runtime_error("logical_value_t::mult unable to process given types");
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

        if (!value1.is_null() && !value2.is_null() && value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return divide(value1.cast_as(complex_logical_type(promoted), place_holder_time_zone),
                          value2.cast_as(complex_logical_type(promoted), place_holder_time_zone));
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
                break;
        }
        // INTERVAL / numeric → INTERVAL
        // (division by zero already handled above — value2 == zero returns null)
        const auto t1 = value1.type().type();
        const auto t2 = value2.type().type();
        if (t1 == logical_type::INTERVAL && is_numeric(t2)) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            const double f = [&]() -> double {
                switch (t2) {
                    case logical_type::TINYINT:
                        return static_cast<double>(value2.value<int8_t>());
                    case logical_type::UTINYINT:
                        return static_cast<double>(value2.value<uint8_t>());
                    case logical_type::SMALLINT:
                        return static_cast<double>(value2.value<int16_t>());
                    case logical_type::USMALLINT:
                        return static_cast<double>(value2.value<uint16_t>());
                    case logical_type::INTEGER:
                        return static_cast<double>(value2.value<int32_t>());
                    case logical_type::UINTEGER:
                        return static_cast<double>(value2.value<uint32_t>());
                    case logical_type::BIGINT:
                        return static_cast<double>(value2.value<int64_t>());
                    case logical_type::UBIGINT:
                        return static_cast<double>(value2.value<uint64_t>());
                    case logical_type::FLOAT:
                        return static_cast<double>(value2.value<float>());
                    case logical_type::DOUBLE:
                        return value2.value<double>();
                    default:
                        return 0.0;
                }
            }();
            using namespace core::date;
            const auto iv = value1.value<interval_t>();
            return logical_value_t{
                r,
                interval_t{microseconds{std::llround(static_cast<double>(iv.time.count()) / f)},
                           days{static_cast<int32_t>(std::llround(static_cast<double>(iv.day.count()) / f))},
                           months{static_cast<int32_t>(std::llround(static_cast<double>(iv.month.count()) / f))}}};
        }
        throw std::runtime_error("logical_value_t::divide unable to process given types");
    }

    logical_value_t logical_value_t::modulus(const logical_value_t& value1, const logical_value_t& value2) {
        if (value1.is_null() && value2.is_null()) {
            return value1;
        }

        if (!value1.is_null() && !value2.is_null() && value1.type().type() != value2.type().type() &&
            is_numeric(value1.type().type()) && is_numeric(value2.type().type())) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            return modulus(value1.cast_as(complex_logical_type(promoted), place_holder_time_zone),
                           value2.cast_as(complex_logical_type(promoted), place_holder_time_zone));
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

    bool serialize_type_matches(const complex_logical_type& expected_type, const complex_logical_type& actual_type) {
        if (expected_type.type() != actual_type.type()) {
            return false;
        }
        if (expected_type.is_nested()) {
            return true;
        }
        return expected_type == actual_type;
    }

    bool enum_value_matches_string(const logical_value_t& enum_val, std::string_view target) {
        const auto* ext = static_cast<const enum_logical_type_extension*>(enum_val.type().extension());
        if (ext == nullptr) {
            return false;
        }
        const auto stored = enum_val.value<int32_t>();
        for (const auto& entry : ext->entries()) {
            if (entry.value<int32_t>() == stored) {
                return entry.type().alias() == target;
            }
        }
        return false;
    }

} // namespace components::types