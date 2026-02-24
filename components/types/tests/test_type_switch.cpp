#include <catch2/catch.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/operations_helper.hpp>

using namespace components::types;

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
    auto operator()(const logical_value_t& test_value, CheckValueType check_value) const
        requires core::CanCompare<TestValueType, CheckValueType> {
        REQUIRE(core::is_equals(test_value.template value<TestValueType>(), check_value));
    }
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const logical_value_t&, CheckValueType) const
        requires(!core::CanCompare<TestValueType, CheckValueType>) {
        throw std::logic_error("given types do not have an == operator");
    }
};

template<>
struct bool_callback_t<void> {
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const logical_value_t& test_value, CheckValueType check_value) const
        -> bool requires core::CanCompare<TestValueType, CheckValueType> {
        return core::is_equals(test_value.template value<TestValueType>(), check_value);
    }
    template<typename TestValueType, typename CheckValueType>
    auto operator()(const logical_value_t&, CheckValueType) const
        -> bool requires(!core::CanCompare<TestValueType, CheckValueType>) {
        throw std::logic_error("given types do not have an == operator");
        return false;
    }
};

template<>
struct double_void_callback_t<void> {
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const logical_value_t& left_test_value, const logical_value_t& right_test_value) const
        requires core::CanCompare<LeftTestValueType, RightTestValueType> {
        REQUIRE(core::is_equals(left_test_value.template value<LeftTestValueType>(),
                                right_test_value.template value<RightTestValueType>()));
    }
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const logical_value_t&, const logical_value_t&) const
        requires(!core::CanCompare<LeftTestValueType, RightTestValueType>) {
        throw std::logic_error("given types do not have an == operator");
    }
};

template<>
struct double_bool_callback_t<void> {
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const logical_value_t& left_test_value, const logical_value_t& right_test_value) const
        -> bool requires core::CanCompare<LeftTestValueType, RightTestValueType> {
        return core::is_equals(left_test_value.template value<LeftTestValueType>(),
                               right_test_value.template value<RightTestValueType>());
    }
    template<typename LeftTestValueType, typename RightTestValueType>
    auto operator()(const logical_value_t&, const logical_value_t&) const
        -> bool requires(!core::CanCompare<LeftTestValueType, RightTestValueType>) {
        throw std::logic_error("given types do not have an == operator");
        return false;
    }
};

TEST_CASE("components::types::test_type_switch") {
    auto resource = std::pmr::synchronized_pool_resource();

    SECTION("callback - void") {
        const bool check_v1{false};
        const int8_t check_v2{-46};
        const uint32_t check_v3{1245};
        const float check_v4{5691.150f};
        const std::string check_v5{"String too long to fall under small string optimization"};

        logical_value_t test_v1{&resource, check_v1};
        logical_value_t test_v2{&resource, check_v2};
        logical_value_t test_v3{&resource, check_v3};
        logical_value_t test_v4{&resource, check_v4};
        logical_value_t test_v5{&resource, check_v5};

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

        logical_value_t test_v1{&resource, check_v1};
        logical_value_t test_v2{&resource, check_v2};
        logical_value_t test_v3{&resource, check_v3};
        logical_value_t test_v4{&resource, check_v4};
        logical_value_t test_v5{&resource, check_v5};

        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v1.type().to_physical_type(), test_v1, check_v1));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v2.type().to_physical_type(), test_v2, check_v2));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v3.type().to_physical_type(), test_v3, check_v3));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v4.type().to_physical_type(), test_v4, check_v4));
        REQUIRE(simple_physical_type_switch<bool_callback_t>(test_v5.type().to_physical_type(), test_v5, check_v5));
    }

    SECTION("double type callback - void") {
        const uint16_t check_v1{1893};
        const int64_t check_v2{1893};

        logical_value_t test_v1{&resource, check_v1};
        logical_value_t test_v2{&resource, check_v2};

        double_simple_physical_type_switch<double_void_callback_t>(test_v1.type().to_physical_type(),
                                                                   test_v2.type().to_physical_type(),
                                                                   test_v1,
                                                                   test_v2);
    }

    SECTION("double type callback - with return") {
        const int16_t check_v1{23562};
        const uint64_t check_v2{23562};

        logical_value_t test_v1{&resource, check_v1};
        logical_value_t test_v2{&resource, check_v2};

        REQUIRE(double_simple_physical_type_switch<double_bool_callback_t>(test_v1.type().to_physical_type(),
                                                                           test_v2.type().to_physical_type(),
                                                                           test_v1,
                                                                           test_v2));
    }
}