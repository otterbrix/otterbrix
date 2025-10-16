#include <catch2/catch.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/operations_helper.hpp>

using namespace components::types;

template<typename, typename, typename = void>
struct has_equality_operator : std::false_type {};

template<typename T, typename U>
struct has_equality_operator<T, U, std::void_t<decltype(std::declval<T>() == std::declval<U>())>> : std::true_type {};

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
    template<typename TestValueType,
             typename CheckValueType,
             std::enable_if_t<has_equality_operator<TestValueType, CheckValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t& test_value, CheckValueType&& check_value) const {
        REQUIRE(test_value.template value<TestValueType>() == check_value);
    }
    template<typename TestValueType,
             typename CheckValueType,
             std::enable_if_t<!has_equality_operator<TestValueType, CheckValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t&, CheckValueType&&) const {
        throw std::logic_error("given types do not have an == operator");
    }
};

template<>
struct bool_callback_t<void> {
    template<typename TestValueType,
             typename CheckValueType,
             std::enable_if_t<has_equality_operator<TestValueType, CheckValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t& test_value, CheckValueType&& check_value) const -> bool {
        return test_value.template value<TestValueType>() == check_value;
    }
    template<typename TestValueType,
             typename CheckValueType,
             std::enable_if_t<!has_equality_operator<TestValueType, CheckValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t&, CheckValueType&&) const -> bool {
        throw std::logic_error("given types do not have an == operator");
        return false;
    }
};

template<>
struct double_void_callback_t<void> {
    template<typename LeftTestValueType,
             typename RightTestValueType,
             std::enable_if_t<has_equality_operator<LeftTestValueType, RightTestValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t& left_test_value, const logical_value_t& right_test_value) const {
        REQUIRE(left_test_value.template value<LeftTestValueType>() ==
                right_test_value.template value<RightTestValueType>());
    }
    template<typename LeftTestValueType,
             typename RightTestValueType,
             std::enable_if_t<!has_equality_operator<LeftTestValueType, RightTestValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t&, const logical_value_t&) const {
        throw std::logic_error("given types do not have an == operator");
    }
};

template<>
struct double_bool_callback_t<void> {
    template<typename LeftTestValueType,
             typename RightTestValueType,
             std::enable_if_t<has_equality_operator<LeftTestValueType, RightTestValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t& left_test_value, const logical_value_t& right_test_value) const
        -> bool {
        return left_test_value.template value<LeftTestValueType>() ==
               right_test_value.template value<RightTestValueType>();
    }
    template<typename LeftTestValueType,
             typename RightTestValueType,
             std::enable_if_t<!has_equality_operator<LeftTestValueType, RightTestValueType>::value, bool> = true>
    constexpr auto operator()(const logical_value_t&, const logical_value_t&) const -> bool {
        throw std::logic_error("given types do not have an == operator");
        return false;
    }
};

TEST_CASE("test_type_switch") {
    SECTION("callback - void") {
        const bool check_v1{false};
        const int8_t check_v2{-46};
        const uint32_t check_v3{1245};
        const float check_v4{5691.150f};
        const std::string check_v5{"String too long to fall under small string optimization"};

        logical_value_t test_v1{check_v1};
        logical_value_t test_v2{check_v2};
        logical_value_t test_v3{check_v3};
        logical_value_t test_v4{check_v4};
        logical_value_t test_v5{check_v5};

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

        logical_value_t test_v1{check_v1};
        logical_value_t test_v2{check_v2};
        logical_value_t test_v3{check_v3};
        logical_value_t test_v4{check_v4};
        logical_value_t test_v5{check_v5};

        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v1.type().to_physical_type(), test_v1, check_v1));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v2.type().to_physical_type(), test_v2, check_v2));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v3.type().to_physical_type(), test_v3, check_v3));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v4.type().to_physical_type(), test_v4, check_v4));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v5.type().to_physical_type(), test_v5, check_v5));
    }

    SECTION("double type callback - void") {
        const uint16_t check_v1{1893};
        const int64_t check_v2{1893};

        logical_value_t test_v1{check_v1};
        logical_value_t test_v2{check_v2};

        double_simple_physical_type_switch<double_void_callback_t>(test_v1.type().to_physical_type(),
                                                                   test_v2.type().to_physical_type(),
                                                                   test_v1,
                                                                   test_v2);
    }

    SECTION("double type callback - with return") {
        const int16_t check_v1{23562};
        const uint64_t check_v2{23562};

        logical_value_t test_v1{check_v1};
        logical_value_t test_v2{check_v2};

        REQUIRE(double_simple_physical_type_switch<double_bool_callback_t>(test_v1.type().to_physical_type(),
                                                                           test_v2.type().to_physical_type(),
                                                                           test_v1,
                                                                           test_v2));
    }
}