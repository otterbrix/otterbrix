#include "logical_value.hpp"
#include "operations_helper.hpp"
#include <core/date/date_cast.hpp>

#include <algorithm>
#include <boost/container_hash/hash.hpp>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>

namespace components::types {

    namespace {
        template<typename T>
        inline constexpr bool ext_is_signed_v = std::is_signed_v<T> || std::is_same_v<T, int128_t>;

        // A physical type outside this fixed set (physical_type::NA, or a nested/complex type) would
        // trip the scalar switch's `default:` invariant abort; cast_as consults this first instead.
        constexpr bool is_scalar_castable_physical_type(physical_type pt) noexcept {
            switch (pt) {
                case physical_type::BOOL:
                case physical_type::UINT8:
                case physical_type::INT8:
                case physical_type::UINT16:
                case physical_type::INT16:
                case physical_type::UINT32:
                case physical_type::INT32:
                case physical_type::UINT64:
                case physical_type::INT64:
                case physical_type::UINT128:
                case physical_type::INT128:
                case physical_type::FLOAT:
                case physical_type::DOUBLE:
                case physical_type::STRING:
                    return true;
                default:
                    return false;
            }
        }
    } // namespace

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
            // UNION/VARIANT must stay vector-backed: create_union builds member slots through this same constructor.
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
            case logical_type::STRUCT:
            case logical_type::UNION:
            case logical_type::VARIANT:
                data_ = reinterpret_cast<uint64_t>(heap_new<std::vector<logical_value_t>>());
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
                // CAST(<numeric> AS boolean): 0 -> false, non-zero -> true (PostgreSQL's explicit int::boolean).
                return logical_value_t{r, !core::is_equals(value.template value<RightValueType>(), RightValueType{})};
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

    core::result_wrapper_t<logical_value_t> logical_value_t::cast_as(const complex_logical_type& type,
                                                                     core::date::timezone_offset_t session_tz) const {
        if (type_ == type) {
            return logical_value_t(*this);
        }
        auto conversion_failure = [this, &type]() {
            std::string message = "cannot cast logical_type " + std::to_string(static_cast<int>(type_.type())) +
                                  " to logical_type " + std::to_string(static_cast<int>(type.type()));
            return core::error_t{core::error_code_t::conversion_failure, std::pmr::string{message.c_str(), resource_}};
        };
        // A DECIMAL source stores value * 10^scale (NUMERIC(10,2) 3.00 -> 300 uncorrected); route it through
        // the descaling branch. BOOLEAN stays raw: payload truthiness equals value truthiness.
        const bool decimal_source_descale =
            type_.type() == logical_type::DECIMAL && type.type() != logical_type::BOOLEAN;
        if ((is_numeric(type.type()) && !decimal_source_descale) ||
            (type.type() == logical_type::STRING_LITERAL && is_numeric(type_.type()))) {
            if (!is_scalar_castable_physical_type(type.to_physical_type()) ||
                !is_scalar_castable_physical_type(type_.to_physical_type())) {
                return conversion_failure();
            }

            return double_simple_physical_type_switch<cast_callback_t>(type.to_physical_type(),
                                                                       type_.to_physical_type(),
                                                                       *this);
        } else if (type.type() == logical_type::DECIMAL && is_numeric(type_.type())) {
            const auto* decimal_extension = reinterpret_cast<const decimal_logical_type_extension*>(type.extension());
            auto create_decimal = [&]<typename T>() -> core::result_wrapper_t<logical_value_t> {
                const auto payload =
                    to_decimal<int128_t>(value<T>(), decimal_extension->width(), decimal_extension->scale());
                if (payload == decimal_limits::pos_inf<int128_t>() || payload == decimal_limits::neg_inf<int128_t>() ||
                    payload == decimal_limits::nan<int128_t>()) {
                    std::pmr::string message{resource_};
                    message.append("numeric field overflow: value does not fit DECIMAL(");
                    message.append(std::to_string(static_cast<int>(decimal_extension->width())).c_str());
                    message.append(",");
                    message.append(std::to_string(static_cast<int>(decimal_extension->scale())).c_str());
                    message.append(")");
                    return core::error_t{core::error_code_t::conversion_failure, std::move(message)};
                }
                return logical_value_t::create_decimal(resource_, type, payload);
            };
            switch (type_.type()) {
                case logical_type::TINYINT:
                    return create_decimal.operator()<int8_t>();
                case logical_type::UTINYINT:
                    return create_decimal.operator()<uint8_t>();
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
                    return conversion_failure();
            }
        } else if (type_.type() == logical_type::DECIMAL && is_numeric(type.type())) {
            const auto* decimal_extension = reinterpret_cast<const decimal_logical_type_extension*>(type_.extension());
            auto create_numeric_inner = [&]<typename From, typename To>() -> core::result_wrapper_t<logical_value_t> {
                if constexpr (std::is_floating_point_v<To>) {
                    return logical_value_t{resource_,
                                           decimal_to_floating<From, To>(value<From>(), decimal_extension->scale())};
                } else {
                    auto val = decimal_to_numeric<From, To>(value<From>(), decimal_extension->scale());
                    if (val.has_value()) {
                        return logical_value_t{resource_, val.value()};
                    }
                    return conversion_failure();
                }
            };
            auto create_numeric = [&]<typename To>() -> core::result_wrapper_t<logical_value_t> {
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
                        assert(false && "decimal source has no integer storage width");
                        return conversion_failure();
                }
            };
            switch (type.type()) {
                case logical_type::UTINYINT:
                    return create_numeric.operator()<uint8_t>();
                case logical_type::USMALLINT:
                    return create_numeric.operator()<uint16_t>();
                case logical_type::UINTEGER:
                    return create_numeric.operator()<uint32_t>();
                case logical_type::UBIGINT:
                    return create_numeric.operator()<uint64_t>();
                case logical_type::UHUGEINT:
                    return create_numeric.operator()<uint128_t>();
                case logical_type::TINYINT:
                    return create_numeric.operator()<int8_t>();
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
                    return conversion_failure();
            }
        } else if (type_.type() == logical_type::STRUCT && type.type() == logical_type::STRUCT) {
            if (type_.child_types().size() != type.child_types().size()) {
                // A field-count mismatch is a failed cast: row(1,2)::<one-field struct> is a legal request.
                return conversion_failure();
            }

            std::vector<logical_value_t> fields;
            fields.reserve(children().size());
            for (size_t i = 0; i < children().size(); i++) {
                if (children()[i].type().type() == logical_type::NA) {
                    fields.emplace_back(children()[i]);
                    continue;
                }
                auto casted = children()[i].cast_as(type.child_types()[i], session_tz);
                if (casted.has_error()) {
                    return casted.error();
                }
                fields.emplace_back(std::move(casted.value()));
            }

            return create_struct(resource_, type, fields);
        } else if ((type_.type() == logical_type::ARRAY || type_.type() == logical_type::LIST) &&
                   type.type() == logical_type::ARRAY) {
            // Casting keeps the SOURCE LENGTH (no truncate/pad); casts::array_cast reconciles length instead.
            const auto& target_elem_type = type.child_type();
            const auto& src = children();
            std::vector<logical_value_t> elems;
            elems.reserve(src.size());
            for (const auto& child : src) {
                if (child.type().type() == logical_type::NA) {
                    elems.emplace_back(child);
                    continue;
                }
                auto casted = child.cast_as(target_elem_type, session_tz);
                if (casted.has_error()) {
                    return casted.error();
                }
                elems.emplace_back(std::move(casted.value()));
            }
            return create_array(resource_, target_elem_type, elems);
        } else if ((type_.type() == logical_type::ARRAY || type_.type() == logical_type::LIST) &&
                   type.type() == logical_type::LIST) {
            const auto& target_elem_type = type.child_type();
            std::vector<logical_value_t> elems;
            elems.reserve(children().size());
            for (const auto& child : children()) {
                if (child.type().type() == logical_type::NA) {
                    elems.emplace_back(child);
                    continue;
                }
                auto casted = child.cast_as(target_elem_type, session_tz);
                if (casted.has_error()) {
                    return casted.error();
                }
                elems.emplace_back(std::move(casted.value()));
            }
            return create_list(resource_, target_elem_type, elems);
        } else if (type.type() == logical_type::ENUM) {
            if (type_.type() == logical_type::STRING_LITERAL) {
                const auto* enum_extension = static_cast<const enum_logical_type_extension*>(type.extension());
                auto string_val = value<std::string_view>();
                for (const auto& entry : enum_extension->entries()) {
                    if (entry.type().alias() == string_val) {
                        logical_value_t result(resource_, type);
                        result.data_ = entry.data_;
                        return result;
                    }
                }
                // An unmatched string must refuse, not answer NA (an ordinary NULL); PostgreSQL refuses too.
                std::pmr::string message{resource_};
                message.append("invalid input value for enum ");
                message.append(enum_extension->type_name());
                message.append(": \"");
                message.append(string_val);
                message.append("\"");
                return core::error_t{core::error_code_t::conversion_failure, std::move(message)};
            } else if (is_numeric(type_.type())) {
                const auto* enum_extension = static_cast<const enum_logical_type_extension*>(type.extension());
                auto src_as_enum = double_simple_physical_type_switch<cast_callback_t>(type.to_physical_type(),
                                                                                       type_.to_physical_type(),
                                                                                       *this);
                for (const auto& entry : enum_extension->entries()) {
                    if (src_as_enum.data_ == entry.data_) {
                        logical_value_t result(resource_, type);
                        result.data_ = src_as_enum.data_;
                        return result;
                    }
                }
                std::pmr::string message{resource_};
                message.append("invalid ordinal value for enum ");
                message.append(enum_extension->type_name());
                return core::error_t{core::error_code_t::conversion_failure, std::move(message)};
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
        // Structural equality (container keys / DISTINCT / sort): two NULLs are equal. SQL value equality over
        // a NULL is UNKNOWN, not FALSE -- that is compare_sql(), which predicate evaluators use instead.
        if (is_null() || rhs.is_null()) {
            return is_null() && rhs.is_null();
        }
        if ((type_.type() == logical_type::ARRAY || type_.type() == logical_type::LIST) &&
            (rhs.type_.type() == logical_type::ARRAY || rhs.type_.type() == logical_type::LIST)) {
            const auto& l = *vec_ptr();
            const auto& r = *rhs.vec_ptr();
            return std::equal(l.begin(), l.end(), r.begin(), r.end(), [](const auto& le, const auto& re) {
                return le.type_ == re.type_ && le == re;
            });
        }
        // assert alone is not enough: under NDEBUG a type mismatch reads the right payload as the left's type.
        assert(type_ == rhs.type_ && "logical_value_t has to be casted to the same type before comparison");
        if (!(type_ == rhs.type_)) {
            return false;
        }
        switch (type_.type()) {
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
        // A NULL carries logical_type::NA, so the type-directed switch below cannot order it directly.
        // Returning false in both directions (NA not< 5, 5 not< NA) would make equivalence non-transitive,
        // undefined behaviour for std::sort/std::map; order NULLs LAST instead, matching sort.cpp's ORDER BY rule.
        const bool lhs_null = is_null();
        const bool rhs_null = rhs.is_null();
        if (lhs_null || rhs_null) {
            return !lhs_null && rhs_null; // value < NULL; NULL < anything is false
        }
        if ((type_.type() == logical_type::ARRAY || type_.type() == logical_type::LIST) &&
            (rhs.type_.type() == logical_type::ARRAY || rhs.type_.type() == logical_type::LIST)) {
            const auto& lv = *vec_ptr();
            const auto& rv = *rhs.vec_ptr();
            return std::lexicographical_compare(lv.begin(), lv.end(), rv.begin(), rv.end());
        }
        // Falling to `return false` on a mismatch (as equality does) would make cross-type values mutually
        // equivalent while same-type values stay ordered -- non-transitive UB. Order by type tag instead.
        assert(type_ == rhs.type_ && "logical_value_t has to be casted to the same type before comparison");
        if (!(type_ == rhs.type_)) {
            return type_.type() < rhs.type_.type();
        }
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
                const auto& lv = *vec_ptr();
                const auto& rv = *rhs.vec_ptr();
                const size_t n = lv.size() < rv.size() ? lv.size() : rv.size();
                for (size_t i = 0; i < n; ++i) {
                    if (lv[i] < rv[i]) {
                        return true;
                    }
                    if (rv[i] < lv[i]) {
                        return false;
                    }
                }
                return lv.size() < rv.size();
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

    std::optional<compare_t> logical_value_t::compare_sql(const logical_value_t& rhs) const {
        // A comparison with a NULL operand is UNKNOWN in SQL three-valued logic; there is no TRUE/FALSE answer.
        if (is_null() || rhs.is_null()) {
            return std::nullopt;
        }
        return compare(rhs);
    }

    const std::vector<logical_value_t>& logical_value_t::children() const {
        // A NULL value carries no payload (data_ is zero); dereferencing vec_ptr() here would be UB.
        static const std::vector<logical_value_t> empty;
        if (is_null()) {
            return empty;
        }
        return *vec_ptr();
    }

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
                // Invariant violation, not user input: must not throw through the noexcept executor coroutine.
                assert(false && "logical_value_t::create_numeric: Numeric requires numeric type");
                std::abort();
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
        result.type_ = complex_logical_type::create_map(r, key_type, value_type);
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

    logical_value_t logical_value_t::create_list_from_type(std::pmr::memory_resource* r,
                                                           const complex_logical_type& list_type,
                                                           const std::vector<logical_value_t>& values) {
        assert(list_type.type() == logical_type::LIST);
        logical_value_t result(r, complex_logical_type{logical_type::NA});
        result.type_ = list_type;
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
    // SQL three-valued logic: the single chokepoint every scalar sum/subtract/mult/divide/modulus dispatches through.
    template<typename OP, typename GET>
    logical_value_t op(const logical_value_t& value, GET getter_function) {
        if (value.is_null()) {
            return logical_value_t{value.resource(), complex_logical_type{logical_type::NA}};
        }
        OP operation{};
        return logical_value_t{value.resource(), operation((value.*getter_function)())};
    }

    template<typename OP, typename GET>
    logical_value_t op(const logical_value_t& value1, const logical_value_t& value2, GET getter_function) {
        auto* r = value1.resource() ? value1.resource() : value2.resource();
        if (value1.is_null() || value2.is_null()) {
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }
        OP operation{};
        return logical_value_t{r, operation((value1.*getter_function)(), (value2.*getter_function)())};
    }

    constexpr auto place_holder_time_zone = core::date::timezone_offset_t{};

    namespace {
        core::error_t
        unsupported_operands(std::string_view what, const logical_value_t& value1, const logical_value_t& value2) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            std::pmr::string message{r};
            message.append(what);
            message.append(": unsupported operand types (");
            message.append(std::to_string(static_cast<int>(value1.type().type())).c_str());
            message.append(", ");
            message.append(std::to_string(static_cast<int>(value2.type().type())).c_str());
            message.append(")");
            return core::error_t{core::error_code_t::arithmetics_failure, message};
        }

        // assert-then-value() is not a guard: under NDEBUG a failed promotion hands a moved-from value onward.
        struct promoted_operands_t {
            logical_value_t lhs;
            logical_value_t rhs;
        };

        bool needs_numeric_promotion(const logical_value_t& value1, const logical_value_t& value2) {
            return !value1.is_null() && !value2.is_null() && value1.type().type() != value2.type().type() &&
                   is_numeric(value1.type().type()) && is_numeric(value2.type().type());
        }

        core::result_wrapper_t<promoted_operands_t> promote_numeric_operands(const logical_value_t& value1,
                                                                             const logical_value_t& value2) {
            auto promoted = promote_type(value1.type().type(), value2.type().type());
            auto lhs = value1.cast_as(complex_logical_type(promoted), place_holder_time_zone);
            auto rhs = value2.cast_as(complex_logical_type(promoted), place_holder_time_zone);
            if (lhs.has_error()) {
                return lhs.error();
            }
            if (rhs.has_error()) {
                return rhs.error();
            }
            return promoted_operands_t{std::move(lhs.value()), std::move(rhs.value())};
        }
    } // namespace

    core::result_wrapper_t<logical_value_t> logical_value_t::sum(const logical_value_t& value1,
                                                                 const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            const auto& lhs = promoted.value().lhs;
            const auto& rhs = promoted.value().rhs;
            return sum(lhs, rhs);
        }

        // Must never dispatch on the left type when the right differs: BIGINT+STRING would read the string's
        // heap pointer as an int64. A mismatch falls to the temporal combinations below, then to unsupported_operands.
        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
            // No STRING_LITERAL arm: SQL spells concatenation ||; text+text refuses here as in PostgreSQL.
            default:
                break;
        }
        using namespace core::date;
        const auto t1 = value1.type().type();
        const auto t2 = value2.type().type();
        auto* r = value1.resource() ? value1.resource() : value2.resource();
        if (t1 == logical_type::DATE && t2 == logical_type::INTERVAL) {
            const auto d = value1.value<date_t>().value;
            const auto iv = value2.value<interval_t>();
            auto sd = pg_epoch + std::chrono::days{d.count()};
            if (iv.month.count())
                sd = apply_months(sd, iv.month.count());
            sd += std::chrono::days{iv.day.count()};
            return logical_value_t{r, date_t{days{static_cast<int32_t>((sd - pg_epoch).count())}}};
        }
        if (t1 == logical_type::INTERVAL && t2 == logical_type::DATE) {
            return logical_value_t::sum(value2, value1);
        }
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
        if (t1 == logical_type::INTERVAL && (t2 == logical_type::TIMESTAMP || t2 == logical_type::TIMESTAMP_TZ)) {
            return logical_value_t::sum(value2, value1);
        }
        if (t1 == logical_type::INTERVAL && t2 == logical_type::INTERVAL) {
            const auto iv1 = value1.value<interval_t>();
            const auto iv2 = value2.value<interval_t>();
            return logical_value_t{r, interval_t{iv1.time + iv2.time, iv1.day + iv2.day, iv1.month + iv2.month}};
        }
        constexpr auto one_day = std::chrono::duration_cast<microseconds>(days{1});
        if (t1 == logical_type::TIME && t2 == logical_type::INTERVAL) {
            auto result = (value1.value<core::date::time_t>().value + value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, core::date::time_t{result}};
        }
        if (t1 == logical_type::INTERVAL && t2 == logical_type::TIME) {
            return logical_value_t::sum(value2, value1);
        }
        if (t1 == logical_type::TIME_TZ && t2 == logical_type::INTERVAL) {
            const auto tz = value1.value<timetz_t>();
            auto result = (tz.time + value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, timetz_t{result, tz.zone}};
        }
        if (t1 == logical_type::INTERVAL && t2 == logical_type::TIME_TZ) {
            return logical_value_t::sum(value2, value1);
        }
        return unsupported_operands("logical_value_t::sum", value1, value2);
    }

    core::result_wrapper_t<logical_value_t> logical_value_t::subtract(const logical_value_t& value1,
                                                                      const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            const auto& lhs = promoted.value().lhs;
            const auto& rhs = promoted.value().rhs;
            return subtract(lhs, rhs);
        }

        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
        if (t1 == logical_type::DATE && t2 == logical_type::INTERVAL) {
            const auto iv = value2.value<interval_t>();
            auto sd = pg_epoch + std::chrono::days{value1.value<date_t>().value.count()};
            if (iv.month.count())
                sd = apply_months(sd, -iv.month.count());
            sd -= std::chrono::days{iv.day.count()};
            return logical_value_t{r, date_t{days{static_cast<int32_t>((sd - pg_epoch).count())}}};
        }
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
        if (t1 == logical_type::TIME && t2 == logical_type::INTERVAL) {
            auto result = (value1.value<core::date::time_t>().value - value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, core::date::time_t{result}};
        }
        if (t1 == logical_type::TIME_TZ && t2 == logical_type::INTERVAL) {
            const auto tz = value1.value<timetz_t>();
            auto result = (tz.time - value2.value<interval_t>().time) % one_day;
            if (result.count() < 0)
                result += one_day;
            return logical_value_t{r, timetz_t{result, tz.zone}};
        }
        if (t1 == logical_type::INTERVAL && t2 == logical_type::INTERVAL) {
            const auto iv1 = value1.value<interval_t>();
            const auto iv2 = value2.value<interval_t>();
            return logical_value_t{r, interval_t{iv1.time - iv2.time, iv1.day - iv2.day, iv1.month - iv2.month}};
        }
        if (t1 == logical_type::DATE && t2 == logical_type::DATE) {
            return logical_value_t{
                r,
                interval_t{microseconds{0}, value1.value<date_t>().value - value2.value<date_t>().value, months{0}}};
        }
        if ((t1 == logical_type::TIMESTAMP || t1 == logical_type::TIMESTAMP_TZ) &&
            (t2 == logical_type::TIMESTAMP || t2 == logical_type::TIMESTAMP_TZ)) {
            const auto ts1 = (t1 == logical_type::TIMESTAMP) ? value1.value<timestamp_t>().value
                                                             : value1.value<timestamptz_t>().value;
            const auto ts2 = (t2 == logical_type::TIMESTAMP) ? value2.value<timestamp_t>().value
                                                             : value2.value<timestamptz_t>().value;
            return logical_value_t{r, interval_t{ts1 - ts2, days{0}, months{0}}};
        }
        if (t1 == logical_type::TIME && t2 == logical_type::TIME) {
            return logical_value_t{
                r,
                interval_t{value1.value<core::date::time_t>().value - value2.value<core::date::time_t>().value,
                           days{0},
                           months{0}}};
        }
        if (t1 == logical_type::TIME_TZ && t2 == logical_type::TIME_TZ) {
            const auto tz1 = value1.value<timetz_t>();
            const auto tz2 = value2.value<timetz_t>();
            const auto utc1 = tz1.time - std::chrono::duration_cast<microseconds>(tz1.zone);
            const auto utc2 = tz2.time - std::chrono::duration_cast<microseconds>(tz2.zone);
            return logical_value_t{r, interval_t{utc1 - utc2, days{0}, months{0}}};
        }
        return unsupported_operands("logical_value_t::subtract", value1, value2);
    }

    core::result_wrapper_t<logical_value_t> logical_value_t::mult(const logical_value_t& value1,
                                                                  const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            const auto& lhs = promoted.value().lhs;
            const auto& rhs = promoted.value().rhs;
            return mult(lhs, rhs);
        }

        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
        if (t1 == logical_type::INTERVAL && is_numeric(t2)) {
            const double f = as_double(value2);
            const auto iv = value1.value<interval_t>();
            return logical_value_t{
                r,
                interval_t{microseconds{std::llround(static_cast<double>(iv.time.count()) * f)},
                           days{static_cast<int32_t>(std::llround(static_cast<double>(iv.day.count()) * f))},
                           months{static_cast<int32_t>(std::llround(static_cast<double>(iv.month.count()) * f))}}};
        }
        if (is_numeric(t1) && t2 == logical_type::INTERVAL) {
            return logical_value_t::mult(value2, value1);
        }
        return unsupported_operands("logical_value_t::mult", value1, value2);
    }

    core::result_wrapper_t<logical_value_t> logical_value_t::divide(const logical_value_t& value1,
                                                                    const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
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

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            const auto& lhs = promoted.value().lhs;
            const auto& rhs = promoted.value().rhs;
            return divide(lhs, rhs);
        }

        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
        return unsupported_operands("logical_value_t::divide", value1, value2);
    }

    core::result_wrapper_t<logical_value_t> logical_value_t::modulus(const logical_value_t& value1,
                                                                     const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            const auto& lhs = promoted.value().lhs;
            const auto& rhs = promoted.value().rhs;
            return modulus(lhs, rhs);
        }

        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
                return unsupported_operands("logical_value_t::modulus", value1, value2);
        }
    }

    core::result_wrapper_t<logical_value_t> logical_value_t::exponent(const logical_value_t& value1,
                                                                      const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            return exponent(promoted.value().lhs, promoted.value().rhs);
        }
        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
                return unsupported_operands("logical_value_t::exponent", value1, value2);
        }
    }

    core::result_wrapper_t<logical_value_t> logical_value_t::bit_and(const logical_value_t& value1,
                                                                     const logical_value_t& value2) {
        if (value1.is_null() || value2.is_null()) {
            auto* r = value1.resource() ? value1.resource() : value2.resource();
            return logical_value_t{r, complex_logical_type{logical_type::NA}};
        }

        if (needs_numeric_promotion(value1, value2)) {
            auto promoted = promote_numeric_operands(value1, value2);
            if (promoted.has_error()) {
                return promoted.error();
            }
            return bit_and(promoted.value().lhs, promoted.value().rhs);
        }
        const auto type = value1.type().type() == value2.type().type() ? value1.type().type() : logical_type::INVALID;
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
                return unsupported_operands("logical_value_t::bit_and", value1, value2);
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