#include <catch2/catch.hpp>

#include <chrono>
#include <cstddef>
#include <random>
#include <type_traits>

#include <core/scalar.hpp>

template<typename T>
struct gen_scalar final {
    std::default_random_engine generator{};
    T value{};

    gen_scalar()
        : value{random_value()} {}

    template<typename U = T, std::enable_if_t<std::is_same<U, bool>::value, bool> = true>
    U random_value() {
        static std::bernoulli_distribution distribution{};
        return distribution(generator);
    }

    template<typename U = T,
             std::enable_if_t<(std::is_integral<U>::value && not std::is_same<U, bool>::value), bool> = true>
    U random_value() {
        static std::uniform_int_distribution<U> distribution{std::numeric_limits<T>::lowest(),
                                                             std::numeric_limits<T>::max()};
        return distribution(generator);
    }

    template<typename U = T, std::enable_if_t<std::is_floating_point<U>::value, bool> = true>
    U random_value() {
        auto const mean{100};
        auto const stddev{20};
        static std::normal_distribution<U> distribution(mean, stddev);
        return distribution(generator);
    }
};

TEMPLATE_TEST_CASE("unitialized",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    core::scalar<TestType> scalar(&mr);
    REQUIRE(nullptr != scalar.data());
}

TEMPLATE_TEST_CASE("initialValue",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    gen_scalar<TestType> gen;
    auto value = gen.value;
    core::scalar<TestType> scalar(&mr, value);
    REQUIRE(nullptr != scalar.data());
    REQUIRE(gen.value == scalar.value());
}

TEMPLATE_TEST_CASE("const ptr data",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    gen_scalar<TestType> gen;
    auto value = gen.value;
    core::scalar<TestType> scalar(&mr, value);
    auto const* data = scalar.data();
    REQUIRE(nullptr != data);
}

TEMPLATE_TEST_CASE("copy ctor",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    gen_scalar<TestType> gen;
    auto value = gen.value;
    core::scalar<TestType> scalar(&mr, value);
    REQUIRE(nullptr != scalar.data());
    REQUIRE(value == scalar.value());

    core::scalar<TestType> copy{&mr, scalar};
    REQUIRE(nullptr != copy.data());
    REQUIRE(copy.data() != scalar.data());
    REQUIRE(copy.value() == scalar.value());
}

TEMPLATE_TEST_CASE("move ctor",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    gen_scalar<TestType> gen;
    auto value = gen.value;
    core::scalar<TestType> scalar(&mr, value);
    REQUIRE(nullptr != scalar.data());
    REQUIRE(value == scalar.value());

    auto* original_pointer = scalar.data();
    auto original_value = scalar.value();

    core::scalar<TestType> moved_to{std::move(scalar)};
    REQUIRE(nullptr != moved_to.data());
    REQUIRE(moved_to.data() == original_pointer);
    REQUIRE(moved_to.value() == original_value);
    REQUIRE(nullptr == scalar.data());
}

TEMPLATE_TEST_CASE("set value",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    gen_scalar<TestType> gen;
    auto value = gen.value;
    core::scalar<TestType> scalar(&mr, value);
    REQUIRE(nullptr != scalar.data());

    auto expected = gen.random_value();

    scalar.set_value(expected);
    REQUIRE(expected == scalar.value());
}

TEMPLATE_TEST_CASE("set value to zero",
                   "[scalar][template]",
                   bool,
                   std::int8_t,
                   std::int16_t,
                   std::int32_t,
                   std::int64_t,
                   float,
                   double) {
    auto mr = std::pmr::synchronized_pool_resource();
    gen_scalar<TestType> gen;
    auto value = gen.value;
    core::scalar<TestType> scalar(&mr, value);
    REQUIRE(nullptr != scalar.data());

    scalar.set_value_to_zero();
    REQUIRE(TestType{0} == scalar.value());
}