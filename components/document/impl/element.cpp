#include "element.hpp"

#include <components/document/impl/tape_ref.hpp>
#include <limits>

namespace components::document::impl {

    template<typename From,
             typename To,
             typename std::enable_if_t<std::is_signed<From>::value && std::is_signed<To>::value, uint8_t> = 0>
    document_result<To> cast_from(internal::tape_ref tape_) noexcept {
        From result = tape_.template next_tape_value<From>();
        if (result > From((std::numeric_limits<To>::max)()) || result < From((std::numeric_limits<To>::min)())) {
            return NUMBER_OUT_OF_RANGE;
        }
        return static_cast<To>(result);
    }
    template<typename From,
             typename To,
             typename std::enable_if<std::is_signed<From>::value && std::is_unsigned<To>::value, uint8_t>::type = 1>
    document_result<To> cast_from(internal::tape_ref tape_) noexcept {
        From result = tape_.template next_tape_value<From>();
        if (result < 0) {
            return NUMBER_OUT_OF_RANGE;
        }
        return static_cast<To>(result);
    }

    template<typename From, typename To, typename std::enable_if<std::is_unsigned<From>::value, uint8_t>::type = 2>
    document_result<To> cast_from(internal::tape_ref tape_) noexcept {
        From result = tape_.template next_tape_value<From>();
        // Wrapping max in parens to handle Windows issue
        if (result > From((std::numeric_limits<To>::max)())) {
            return NUMBER_OUT_OF_RANGE;
        }
        return static_cast<To>(result);
    }

    // specifications for absl::int128:
    template<
        typename From,
        typename To,
        typename std::enable_if_t<std::is_same<From, absl::int128>::value && std::is_signed<To>::value, uint8_t> = 0>
    document_result<To> cast_from(internal::tape_ref tape_) noexcept {
        From result = tape_.template next_tape_value<From>();
        if (result > From((std::numeric_limits<To>::max)()) || result < From((std::numeric_limits<To>::min)())) {
            return NUMBER_OUT_OF_RANGE;
        }
        return static_cast<To>(result);
    }

    template<
        typename From,
        typename To,
        typename std::enable_if_t<std::is_signed<From>::value && std::is_same<To, absl::int128>::value, uint8_t> = 0>
    document_result<To> cast_from(internal::tape_ref tape_) noexcept {
        From result = tape_.template next_tape_value<From>();
        if (result > From((std::numeric_limits<To>::max)()) || result < From((std::numeric_limits<To>::min)())) {
            return NUMBER_OUT_OF_RANGE;
        }
        return static_cast<To>(result);
    }
    template<typename From,
             typename To,
             typename std::enable_if<std::is_same<From, absl::int128>::value && std::is_unsigned<To>::value,
                                     uint8_t>::type = 1>
    document_result<To> cast_from(internal::tape_ref tape_) noexcept {
        From result = tape_.template next_tape_value<From>();
        if (result < 0) {
            return NUMBER_OUT_OF_RANGE;
        }
        return static_cast<To>(result);
    }

    element::element() noexcept
        : tape_() {}

    element::element(const internal::tape_ref& tape) noexcept
        : tape_{tape} {}

    types::logical_type element::logical_type() const noexcept {
        assert(tape_.usable());
        return types::to_logical(tape_.tape_ref_type());
    }

    types::physical_type element::physical_type() const noexcept {
        assert(tape_.usable());
        return tape_.tape_ref_type();
    }

    bool element::usable() const noexcept { return tape_.usable(); }

    // Cast this element to a null-terminated C string.
    document_result<const char*> element::get_c_str() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::STRING: {
                return tape_.get_c_str();
            }
            default:
                return INCORRECT_TYPE;
        }
    }
    document_result<size_t> element::get_string_length() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::STRING: {
                return tape_.get_string_length();
            }
            default:
                return INCORRECT_TYPE;
        }
    }
    document_result<std::string_view> element::get_string() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::STRING:
                return tape_.get_string_view();
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<uint8_t> element::get_uint8() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::UINT8: {
                return tape_.template next_tape_value<uint8_t>();
            }
            case types::physical_type::INT8: {
                return cast_from<int8_t, uint8_t>(tape_);
            }
            case types::physical_type::UINT16: {
                return cast_from<uint16_t, uint8_t>(tape_);
            }
            case types::physical_type::INT16: {
                return cast_from<int16_t, uint8_t>(tape_);
            }
            case types::physical_type::UINT32: {
                return cast_from<uint32_t, uint8_t>(tape_);
            }
            case types::physical_type::INT32: {
                return cast_from<int32_t, uint8_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, uint8_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, uint8_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, uint8_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, uint8_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, uint8_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<uint16_t> element::get_uint16() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::UINT8: {
                return uint16_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT8: {
                return uint16_t(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT16: {
                return tape_.template next_tape_value<uint16_t>();
            }
            case types::physical_type::INT16: {
                return cast_from<int16_t, uint16_t>(tape_);
            }
            case types::physical_type::UINT32: {
                return cast_from<uint32_t, uint16_t>(tape_);
            }
            case types::physical_type::INT32: {
                return cast_from<int32_t, uint16_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, uint16_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, uint16_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, uint16_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, uint16_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, uint16_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<uint32_t> element::get_uint32() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::UINT8: {
                return uint32_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT8: {
                return uint32_t(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT16: {
                return uint32_t(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT16: {
                return cast_from<int16_t, uint32_t>(tape_);
            }
            case types::physical_type::UINT32: {
                return tape_.template next_tape_value<uint32_t>();
            }
            case types::physical_type::INT32: {
                return cast_from<int32_t, uint32_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, uint32_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, uint32_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, uint32_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, uint32_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, uint32_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<uint64_t> element::get_uint64() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::UINT8: {
                return uint64_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT8: {
                return uint64_t(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT16: {
                return uint64_t(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::UINT32: {
                return uint64_t(tape_.template next_tape_value<uint32_t>());
            }
            case types::physical_type::INT16: {
                return cast_from<int16_t, uint64_t>(tape_);
            }
            case types::physical_type::INT32: {
                return cast_from<int32_t, uint64_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, uint64_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return tape_.template next_tape_value<uint64_t>();
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, uint64_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, uint64_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, uint64_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<absl::uint128> element::get_uint128() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::INT8: {
                return absl::uint128(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT8: {
                return absl::uint128(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT16: {
                return absl::uint128(tape_.template next_tape_value<int16_t>());
            }
            case types::physical_type::UINT16: {
                return absl::uint128(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT32: {
                return absl::uint128(tape_.template next_tape_value<int32_t>());
            }
            case types::physical_type::UINT32: {
                return absl::uint128(tape_.template next_tape_value<uint32_t>());
            }
            case types::physical_type::INT64: {
                return absl::uint128(tape_.template next_tape_value<int64_t>());
            }
            case types::physical_type::UINT64: {
                return absl::uint128(tape_.template next_tape_value<uint64_t>());
            }
            case types::physical_type::INT128: {
                return tape_.template next_tape_value<absl::uint128>();
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<int8_t> element::get_int8() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::INT8: {
                return tape_.template next_tape_value<int8_t>();
            }
            case types::physical_type::UINT8: {
                return int8_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT16: {
                return cast_from<int16_t, int8_t>(tape_);
            }
            case types::physical_type::UINT16: {
                return cast_from<uint16_t, int8_t>(tape_);
            }
            case types::physical_type::INT32: {
                return cast_from<int32_t, int8_t>(tape_);
            }
            case types::physical_type::UINT32: {
                return cast_from<uint32_t, int8_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, int8_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, int8_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, int8_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, int8_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, int8_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<int16_t> element::get_int16() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::INT8: {
                return int16_t(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT8: {
                return int16_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT16: {
                return tape_.template next_tape_value<int16_t>();
            }
            case types::physical_type::UINT16: {
                return cast_from<uint16_t, int16_t>(tape_);
            }
            case types::physical_type::INT32: {
                return cast_from<int32_t, int16_t>(tape_);
            }
            case types::physical_type::UINT32: {
                return cast_from<uint32_t, int16_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, int16_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, int16_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, int16_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, int16_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, int16_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<int32_t> element::get_int32() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::INT8: {
                return int32_t(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT8: {
                return int32_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT16: {
                return int32_t(tape_.template next_tape_value<int16_t>());
            }
            case types::physical_type::UINT16: {
                return int32_t(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT32: {
                return tape_.template next_tape_value<int32_t>();
            }
            case types::physical_type::UINT32: {
                return cast_from<uint32_t, int32_t>(tape_);
            }
            case types::physical_type::INT64: {
                return cast_from<int64_t, int32_t>(tape_);
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, int32_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, int32_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, int32_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, int32_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<int64_t> element::get_int64() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::INT8: {
                return int64_t(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT8: {
                return int64_t(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT16: {
                return int32_t(tape_.template next_tape_value<int16_t>());
            }
            case types::physical_type::UINT16: {
                return int64_t(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT32: {
                return int64_t(tape_.template next_tape_value<int32_t>());
            }
            case types::physical_type::UINT32: {
                return int64_t(tape_.template next_tape_value<uint32_t>());
            }
            case types::physical_type::INT64: {
                return tape_.template next_tape_value<int64_t>();
            }
            case types::physical_type::UINT64: {
                return cast_from<uint64_t, int64_t>(tape_);
            }
            case types::physical_type::INT128: {
                return cast_from<absl::int128, int64_t>(tape_);
            }
            case types::physical_type::FLOAT: {
                return cast_from<float, int64_t>(tape_);
            }
            case types::physical_type::DOUBLE: {
                return cast_from<double, int64_t>(tape_);
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<absl::int128> element::get_int128() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::INT8: {
                return absl::int128(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT8: {
                return absl::int128(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT16: {
                return absl::int128(tape_.template next_tape_value<int16_t>());
            }
            case types::physical_type::UINT16: {
                return absl::int128(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT32: {
                return absl::int128(tape_.template next_tape_value<int32_t>());
            }
            case types::physical_type::UINT32: {
                return absl::int128(tape_.template next_tape_value<uint32_t>());
            }
            case types::physical_type::INT64: {
                return absl::int128(tape_.template next_tape_value<int64_t>());
            }
            case types::physical_type::UINT64: {
                return absl::int128(tape_.template next_tape_value<uint64_t>());
            }
            case types::physical_type::INT128: {
                return tape_.template next_tape_value<absl::int128>();
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<float> element::get_float() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::FLOAT: {
                return tape_.template next_tape_value<float>();
            }
            case types::physical_type::DOUBLE: {
                return float(tape_.template next_tape_value<double>());
            }
            case types::physical_type::UINT8: {
                return float(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT8: {
                return float(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT16: {
                return float(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT16: {
                return float(tape_.template next_tape_value<int16_t>());
            }
            case types::physical_type::UINT32: {
                return float(tape_.template next_tape_value<uint32_t>());
            }
            case types::physical_type::INT32: {
                return float(tape_.template next_tape_value<int32_t>());
            }
            case types::physical_type::UINT64: {
                return float(tape_.template next_tape_value<uint64_t>());
            }
            case types::physical_type::INT64: {
                return float(tape_.template next_tape_value<int64_t>());
            }
            case types::physical_type::INT128: {
                return float(tape_.template next_tape_value<absl::int128>());
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<double> element::get_double() const noexcept {
        assert(tape_.usable());
        switch (tape_.tape_ref_type()) {
            case types::physical_type::FLOAT: {
                return double(tape_.template next_tape_value<float>());
            }
            case types::physical_type::DOUBLE: {
                return tape_.template next_tape_value<double>();
            }
            case types::physical_type::UINT8: {
                return double(tape_.template next_tape_value<uint8_t>());
            }
            case types::physical_type::INT8: {
                return double(tape_.template next_tape_value<int8_t>());
            }
            case types::physical_type::UINT16: {
                return double(tape_.template next_tape_value<uint16_t>());
            }
            case types::physical_type::INT16: {
                return double(tape_.template next_tape_value<int16_t>());
            }
            case types::physical_type::UINT32: {
                return double(tape_.template next_tape_value<uint32_t>());
            }
            case types::physical_type::INT32: {
                return double(tape_.template next_tape_value<int32_t>());
            }
            case types::physical_type::UINT64: {
                return double(tape_.template next_tape_value<uint64_t>());
            }
            case types::physical_type::INT64: {
                return double(tape_.template next_tape_value<int64_t>());
            }
            case types::physical_type::INT128: {
                return double(tape_.template next_tape_value<absl::int128>());
            }
            default:
                return INCORRECT_TYPE;
        }
    }

    document_result<bool> element::get_bool() const noexcept {
        assert(tape_.usable());
        if (tape_.is_bool()) {
            return tape_.template next_tape_value<bool>();
        }
        return INCORRECT_TYPE;
    }

    bool element::is_string() const noexcept { return is<std::string_view>(); }
    bool element::is_int32() const noexcept { return is<int32_t>(); }
    bool element::is_int64() const noexcept { return is<int64_t>(); }
    bool element::is_int128() const noexcept { return is<absl::int128>(); }
    bool element::is_uint128() const noexcept { return is<absl::uint128>(); }
    bool element::is_uint32() const noexcept { return is<uint32_t>(); }
    bool element::is_uint64() const noexcept { return is<uint64_t>(); }
    bool element::is_float() const noexcept { return is<float>(); }
    bool element::is_double() const noexcept { return is<double>(); }
    bool element::is_bool() const noexcept { return is<bool>(); }
    bool element::is_number() const noexcept { return is_int64() || is_uint64() || is_double(); }

    bool element::is_null() const noexcept { return tape_.is_null_on_tape(); }

    bool element::operator<(const element& other) const noexcept { return tape_.json_index_ < other.tape_.json_index_; }

    bool element::operator==(const element& other) const noexcept {
        return tape_.json_index_ == other.tape_.json_index_;
    }

} // namespace components::document::impl
