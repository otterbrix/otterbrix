#include <catch2/catch_test_macros.hpp>

#include <components/types/operations_helper.hpp>
#include <core/pmr.hpp>

#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

using namespace components::types;

// A typed scalar the type switches dispatch on: the value's bytes (or its text) and its physical type.
class box_t {
public:
    template<typename T>
    explicit box_t(T value)
        : physical_(to_physical_type(to_logical_type<T>())) {
        if constexpr (std::is_same_v<T, std::string>) {
            physical_ = physical_type::STRING;
            text_ = std::move(value);
        } else {
            std::memcpy(bytes_, &value, sizeof(T));
        }
    }

    struct type_view_t {
        physical_type physical;
        physical_type to_physical_type() const { return physical; }
    };
    type_view_t type() const { return {physical_}; }

    template<typename T>
    T value() const {
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, std::string>) {
            return T{text_};
        } else {
            T result;
            std::memcpy(&result, bytes_, sizeof(T));
            return result;
        }
    }

private:
    physical_type physical_;
    alignas(16) unsigned char bytes_[16]{};
    std::string text_;
};

template<typename T = void>
struct void_callback_t;
template<typename T = void>
struct bool_callback_t;
template<typename T = void>
struct double_void_callback_t;
template<typename T = void>
struct double_bool_callback_t;

template<>
struct void_callback_t<void> {
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const box_t& test_value, CheckValueType check_value) const
        requires core::CanCompare<TestValueType, CheckValueType> {
        REQUIRE(core::is_equals(test_value.template value<TestValueType>(), check_value));
    }
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const box_t&, CheckValueType) const requires(!core::CanCompare<TestValueType, CheckValueType>) {
        throw std::logic_error("given types do not have an == operator");
    }
};

template<>
struct bool_callback_t<void> {
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const box_t& test_value, CheckValueType check_value) const
        -> bool requires core::CanCompare<TestValueType, CheckValueType> {
        return core::is_equals(test_value.template value<TestValueType>(), check_value);
    }
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const box_t&, CheckValueType) const
        -> bool requires(!core::CanCompare<TestValueType, CheckValueType>) {
        throw std::logic_error("given types do not have an == operator");
        return false;
    }
};

template<>
struct double_void_callback_t<void> {
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const box_t& left_test_value, const box_t& right_test_value) const
        requires core::CanCompare<LeftTestValueType, RightTestValueType> {
        REQUIRE(core::is_equals(left_test_value.template value<LeftTestValueType>(),
                                right_test_value.template value<RightTestValueType>()));
    }
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const box_t&, const box_t&) const
        requires(!core::CanCompare<LeftTestValueType, RightTestValueType>) {
        throw std::logic_error("given types do not have an == operator");
    }
};

template<>
struct double_bool_callback_t<void> {
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const box_t& left_test_value, const box_t& right_test_value) const
        -> bool requires core::CanCompare<LeftTestValueType, RightTestValueType> {
        return core::is_equals(left_test_value.template value<LeftTestValueType>(),
                               right_test_value.template value<RightTestValueType>());
    }
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const box_t&, const box_t&) const
        -> bool requires(!core::CanCompare<LeftTestValueType, RightTestValueType>) {
        throw std::logic_error("given types do not have an == operator");
        return false;
    }
};

TEST_CASE("components::types::test_type_switch") {
    SECTION("callback - void") {
        const bool check_v1{false};
        const int8_t check_v2{-46};
        const uint32_t check_v3{1245};
        const float check_v4{5691.150f};
        const std::string check_v5{"String too long to fall under small string optimization"};

        box_t test_v1{check_v1};
        box_t test_v2{check_v2};
        box_t test_v3{check_v3};
        box_t test_v4{check_v4};
        box_t test_v5{check_v5};

        simple_physical_type_switch<void_callback_t>(test_v1.type().to_physical_type(), test_v1, check_v1);
        simple_physical_type_switch<void_callback_t>(test_v2.type().to_physical_type(), test_v2, check_v2);
        simple_physical_type_switch<void_callback_t>(test_v3.type().to_physical_type(), test_v3, check_v3);
        simple_physical_type_switch<void_callback_t>(test_v4.type().to_physical_type(), test_v4, check_v4);
        simple_physical_type_switch<void_callback_t>(test_v5.type().to_physical_type(), test_v5, check_v5);
    }

    SECTION("callback - with return") {
        const bool check_v1{true};
        const uint16_t check_v2{246};
        const int64_t check_v3{1243675};
        const double check_v4{5691001.150};
        const std::string check_v5{"small"}; // small string optimization

        box_t test_v1{check_v1};
        box_t test_v2{check_v2};
        box_t test_v3{check_v3};
        box_t test_v4{check_v4};
        box_t test_v5{check_v5};

        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v1.type().to_physical_type(), test_v1, check_v1));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v2.type().to_physical_type(), test_v2, check_v2));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v3.type().to_physical_type(), test_v3, check_v3));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v4.type().to_physical_type(), test_v4, check_v4));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v5.type().to_physical_type(), test_v5, check_v5));
    }

    SECTION("double type callback - void") {
        const uint16_t check_v1{1893};
        const int64_t check_v2{1893};

        box_t test_v1{check_v1};
        box_t test_v2{check_v2};

        double_simple_physical_type_switch<double_void_callback_t>(test_v1.type().to_physical_type(),
                                                                   test_v2.type().to_physical_type(),
                                                                   test_v1,
                                                                   test_v2);
    }

    SECTION("double type callback - with return") {
        const int16_t check_v1{23562};
        const uint64_t check_v2{23562};

        box_t test_v1{check_v1};
        box_t test_v2{check_v2};

        REQUIRE(double_simple_physical_type_switch<double_bool_callback_t>(test_v1.type().to_physical_type(),
                                                                           test_v2.type().to_physical_type(),
                                                                           test_v1,
                                                                           test_v2));
    }
}