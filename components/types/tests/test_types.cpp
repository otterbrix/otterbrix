#include "operations_helper.hpp"
#include <array>
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/types/physical_value.hpp>
#include <core/counting_resource.hpp>
#include <core/operations_helper.hpp>
#include <core/pmr.hpp>
#include <cstddef>
#include <memory_resource>
#include <random>
#include <string_view>

using namespace components::types;

namespace {
    // Not the process-global resource: create_decimal's refusal message needs a caller-owned arena.
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    core::result_wrapper_t<components::types::complex_logical_type>
    try_make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        return components::types::complex_logical_type::create_decimal(decimal_resource(),
                                                                       width,
                                                                       scale,
                                                                       std::move(alias));
    }

    // Every literal these tests use is inside the window, so this asserts rather than propagates.
    components::types::complex_logical_type make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = try_make_decimal(width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

TEST_CASE("components::types::physical_value") {
    std::vector<physical_value> values;
    std::string_view str1 = "test string";
    std::string_view str2 = "bigger test string but shouldn't be; b < t";

    INFO("initialization");
    {
        values.emplace_back();
        values.emplace_back(false);
        values.emplace_back(true);
        values.emplace_back(uint8_t(53));
        values.emplace_back(uint16_t(643));
        values.emplace_back(uint32_t(3167));
        values.emplace_back(uint64_t(47853));
        values.emplace_back(int8_t(-57));
        values.emplace_back(int16_t(-731));
        values.emplace_back(int32_t(-9691));
        values.emplace_back(int64_t(-478346));
        values.emplace_back(float(-63.239f));
        values.emplace_back(double(577.3910246));
        values.emplace_back(str1);
        values.emplace_back(str2);
    }

    INFO("value getters");
    {
        REQUIRE(values[0].value<physical_type::NA>() == nullptr);
        REQUIRE(values[1].value<physical_type::BOOL>() == false);
        REQUIRE(values[2].value<physical_type::BOOL>() == true);
        REQUIRE(values[3].value<physical_type::UINT8>() == uint8_t(53));
        REQUIRE(values[4].value<physical_type::UINT16>() == uint16_t(643));
        REQUIRE(values[5].value<physical_type::UINT32>() == uint32_t(3167));
        REQUIRE(values[6].value<physical_type::UINT64>() == uint64_t(47853));
        REQUIRE(values[7].value<physical_type::INT8>() == int8_t(-57));
        REQUIRE(values[8].value<physical_type::INT16>() == int16_t(-731));
        REQUIRE(values[9].value<physical_type::INT32>() == int32_t(-9691));
        REQUIRE(values[10].value<physical_type::INT64>() == int64_t(-478346));
        REQUIRE(core::is_equals(values[11].value<physical_type::FLOAT>(), -63.239f));
        REQUIRE(core::is_equals(values[12].value<physical_type::DOUBLE>(), 577.3910246));
        REQUIRE(values[13].value<physical_type::TEXT>() == str1);
        REQUIRE(values[14].value<physical_type::TEXT>() == str2);
    }

    INFO("sort");
    {
        std::shuffle(values.begin(), values.end(), std::default_random_engine{0});
        std::sort(values.begin(), values.end());

        REQUIRE(values[0].type() == physical_type::BOOL);
        REQUIRE(values[1].type() == physical_type::BOOL);
        REQUIRE(values[2].type() == physical_type::INT64);
        REQUIRE(values[3].type() == physical_type::INT32);
        REQUIRE(values[4].type() == physical_type::INT16);
        REQUIRE(values[5].type() == physical_type::FLOAT);
        REQUIRE(values[6].type() == physical_type::INT8);
        REQUIRE(values[7].type() == physical_type::UINT8);
        REQUIRE(values[8].type() == physical_type::DOUBLE);
        REQUIRE(values[9].type() == physical_type::UINT16);
        REQUIRE(values[10].type() == physical_type::UINT32);
        REQUIRE(values[11].type() == physical_type::UINT64);
        REQUIRE(values[12].type() == physical_type::TEXT);
        REQUIRE(values[12].value<physical_type::TEXT>() == str2);
        REQUIRE(values[13].type() == physical_type::TEXT);
        REQUIRE(values[13].value<physical_type::TEXT>() == str1);
        REQUIRE(values[14].type() == physical_type::NA);
    }
}

TEST_CASE("components::types::operations_helper::powers_of_ten") {
    for (size_t i = 1; i < sizeof(POWERS_OF_TEN) / sizeof(int128_t); i++) {
        REQUIRE(POWERS_OF_TEN[i - 1] * 10 == POWERS_OF_TEN[i]);
    }
}

TEST_CASE("components::types::decimal") {
    auto check_conversion =
        []<typename Storage, typename Source>(Source val, uint8_t width, uint8_t scale, const std::string& result) {
            Storage decimal_value = components::types::to_decimal<Storage, Source>(val, width, scale);
            REQUIRE(decimal_to_string(decimal_value, width, scale) == result);
        };
    auto check_arithmetics =
        []<typename Storage, typename Source>(Source val, uint8_t width, uint8_t scale, const std::string& result) {
            Storage decimal_value = components::types::to_decimal<Storage, Source>(val, width, scale);
            decimal_value /= 10;
            REQUIRE(decimal_to_string(decimal_value, width, scale) == result);
        };

    SECTION("int16_t") {
        static constexpr uint8_t width = 3;
        static constexpr uint8_t scale = 1;
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT16);

        SECTION("convert") {
            check_conversion.operator()<int16_t, double>(1.27, width, scale, "1.3");
            check_conversion.operator()<int16_t, double>(1.21, width, scale, "1.2");
            check_conversion.operator()<int16_t, int64_t>(1, width, scale, "1.0");
            check_conversion.operator()<int16_t, double>(-1.27, width, scale, "-1.3");
            check_conversion.operator()<int16_t, double>(-1.21, width, scale, "-1.2");
            check_conversion.operator()<int16_t, int64_t>(-1, width, scale, "-1.0");
            check_conversion.operator()<int16_t, double>(10000000, width, scale, "Infinity");
            check_conversion.operator()<int16_t, double>(-10000000, width, scale, "-Infinity");
            check_conversion.operator()<int16_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int16_t, int64_t>(10000000, width, scale, "Infinity");
            check_conversion.operator()<int16_t, int64_t>(-10000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int16_t, double>(1.27, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, double>(1.21, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, int64_t>(1, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, double>(-1.27, width, scale, "-0.1");
            check_arithmetics.operator()<int16_t, double>(-1.21, width, scale, "-0.1");
            check_arithmetics.operator()<int16_t, int64_t>(-1, width, scale, "-0.1");
        }
    }

    SECTION("int32_t") {
        static constexpr uint8_t width = 8;
        static constexpr uint8_t scale = 2;
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT32);

        SECTION("convert") {
            check_conversion.operator()<int32_t, double>(502.215, width, scale, "502.22");
            check_conversion.operator()<int32_t, double>(502.214, width, scale, "502.21");
            check_conversion.operator()<int32_t, int64_t>(502, width, scale, "502.00");
            check_conversion.operator()<int32_t, double>(-502.215, width, scale, "-502.22");
            check_conversion.operator()<int32_t, double>(-502.214, width, scale, "-502.21");
            check_conversion.operator()<int32_t, int64_t>(-502, width, scale, "-502.00");
            check_conversion.operator()<int32_t, double>(10000000000, width, scale, "Infinity");
            check_conversion.operator()<int32_t, double>(-10000000000, width, scale, "-Infinity");
            check_conversion.operator()<int32_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int32_t, int64_t>(10000000000, width, scale, "Infinity");
            check_conversion.operator()<int32_t, int64_t>(-10000000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int32_t, double>(502.215, width, scale, "50.22");
            check_arithmetics.operator()<int32_t, double>(502.214, width, scale, "50.22");
            check_arithmetics.operator()<int32_t, int64_t>(502, width, scale, "50.20");
            check_arithmetics.operator()<int32_t, double>(-502.215, width, scale, "-50.22");
            check_arithmetics.operator()<int32_t, double>(-502.214, width, scale, "-50.22");
            check_arithmetics.operator()<int32_t, int64_t>(-502, width, scale, "-50.20");
        }
    }

    SECTION("int64_t") {
        static constexpr uint8_t width = 12;
        static constexpr uint8_t scale = 3;
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT64);

        SECTION("convert") {
            check_conversion.operator()<int64_t, double>(502.2157, width, scale, "502.216");
            check_conversion.operator()<int64_t, double>(502.2151, width, scale, "502.215");
            check_conversion.operator()<int64_t, int64_t>(502, width, scale, "502.000");
            check_conversion.operator()<int64_t, double>(-502.2157, width, scale, "-502.216");
            check_conversion.operator()<int64_t, double>(-502.2151, width, scale, "-502.215");
            check_conversion.operator()<int64_t, int64_t>(-502, width, scale, "-502.000");
            check_conversion.operator()<int64_t, double>(10000000000000, width, scale, "Infinity");
            check_conversion.operator()<int64_t, double>(-10000000000000, width, scale, "-Infinity");
            check_conversion.operator()<int64_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int64_t, int64_t>(10000000000000, width, scale, "Infinity");
            check_conversion.operator()<int64_t, int64_t>(-10000000000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int64_t, double>(502.2157, width, scale, "50.221");
            check_arithmetics.operator()<int64_t, double>(502.2151, width, scale, "50.221");
            check_arithmetics.operator()<int64_t, int64_t>(502, width, scale, "50.200");
            check_arithmetics.operator()<int64_t, double>(-502.2157, width, scale, "-50.221");
            check_arithmetics.operator()<int64_t, double>(-502.2151, width, scale, "-50.221");
            check_arithmetics.operator()<int64_t, int64_t>(-502, width, scale, "-50.200");
        }
    }

    INFO("int128_t");
    {
        static constexpr uint8_t width = 20;
        static constexpr uint8_t scale = 4;
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT128);

        SECTION("convert") {
            check_conversion.operator()<int128_t, double>(502.21575, width, scale, "502.2158");
            check_conversion.operator()<int128_t, double>(502.21572, width, scale, "502.2157");
            check_conversion.operator()<int128_t, int64_t>(502, width, scale, "502.0000");
            check_conversion.operator()<int128_t, double>(-502.21575, width, scale, "-502.2158");
            check_conversion.operator()<int128_t, double>(-502.21572, width, scale, "-502.2157");
            check_conversion.operator()<int128_t, int64_t>(-502, width, scale, "-502.0000");
            check_conversion.operator()<int128_t, double>(1e30, width, scale, "Infinity");
            check_conversion.operator()<int128_t, double>(-1e30, width, scale, "-Infinity");
            check_conversion.operator()<int128_t, double>(std::numeric_limits<double>::quiet_NaN(),
                                                          width,
                                                          scale,
                                                          "NaN");
            check_conversion.operator()<int128_t, int128_t>(absl::MakeInt128(10000000, 0), width, scale, "Infinity");
            check_conversion.operator()<int128_t, int128_t>(absl::MakeInt128(-10000000, 0), width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int128_t, double>(502.21575, width, scale, "50.2215");
            check_arithmetics.operator()<int128_t, double>(502.21572, width, scale, "50.2215");
            check_arithmetics.operator()<int128_t, int64_t>(502, width, scale, "50.2000");
            check_arithmetics.operator()<int128_t, double>(-502.21575, width, scale, "-50.2215");
            check_arithmetics.operator()<int128_t, double>(-502.21572, width, scale, "-50.2215");
            check_arithmetics.operator()<int128_t, int64_t>(-502, width, scale, "-50.2000");
        }
    }
}
// ENUM type equality is the type name plus the numbers in order; the labels do not take part.
TEST_CASE("components::types::complex_logical_type::enum_equality_compares_name_and_numbers") {
    const auto a_xy = complex_logical_type::create_enum("a", {{"x", 0}, {"y", 1}});
    CHECK(a_xy == complex_logical_type::create_enum("a", {{"x", 0}, {"y", 1}}));
    CHECK(a_xy == complex_logical_type::create_enum("a", {{"p", 0}, {"q", 1}}));
    CHECK(a_xy != complex_logical_type::create_enum("a", {{"x", 0}, {"y", 2}}));
    CHECK(a_xy != complex_logical_type::create_enum("a", {{"x", 1}, {"y", 0}}));
    CHECK(a_xy != complex_logical_type::create_enum("a", {{"x", 0}}));
    CHECK(a_xy != complex_logical_type::create_enum("b", {{"x", 0}, {"y", 1}}));
}

TEST_CASE("components::types::complex_logical_type::create_decimal_reports_on_the_caller_arena") {
    // The refusal message is built and still live on the caller's arena after the result is moved --
    // error_t's copy assignment re-anchors onto the default resource, so a correct build can still fail.
    std::array<std::byte, 4096> storage{};
    std::pmr::monotonic_buffer_resource stack_arena{storage.data(), storage.size(), std::pmr::null_memory_resource()};
    core::pmr::counting_resource_t arena{&stack_arena};

    INFO("an out-of-window DECIMAL reports on the arena it was handed");
    auto refused = complex_logical_type::create_decimal(&arena, 39, 0);
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    CHECK(arena.allocations() >= 1);
    CHECK(refused.error().what.get_allocator().resource() == &arena);
    CHECK(std::string_view{refused.error().what}.find("DECIMAL(39,0)") != std::string_view::npos);

    INFO("a second refusal on a second arena does not drift back to the first");
    std::array<std::byte, 4096> other_storage{};
    std::pmr::monotonic_buffer_resource other_stack{other_storage.data(),
                                                    other_storage.size(),
                                                    std::pmr::null_memory_resource()};
    core::pmr::counting_resource_t other{&other_stack};
    const size_t first_arena_allocations = arena.allocations();
    auto refused_elsewhere = complex_logical_type::create_decimal(&other, 5, 7);
    REQUIRE(refused_elsewhere.has_error());
    CHECK(other.allocations() >= 1);
    CHECK(refused_elsewhere.error().what.get_allocator().resource() == &other);
    CHECK(arena.allocations() == first_arena_allocations);

    INFO("an in-window DECIMAL costs the arena nothing");
    const size_t before = arena.allocations();
    auto built = complex_logical_type::create_decimal(&arena, 18, 4);
    REQUIRE_FALSE(built.has_error());
    CHECK(built.value().type() == logical_type::DECIMAL);
    CHECK(arena.allocations() == before);
}

TEST_CASE("components::types::complex_logical_type::decimal_helpers_name_an_arena_of_their_own") {
    auto* helper_arena = decimal_resource();

    INFO("the helper's arena is not the process-global one");
    const std::pmr::string process_anchored;
    CHECK(helper_arena != process_anchored.get_allocator().resource());

    INFO("and a refusal routed through the HELPER's own call lands there and stays there");
    auto refused = try_make_decimal(39, 0);
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    CHECK(refused.error().what.get_allocator().resource() == helper_arena);
    CHECK(std::string_view{refused.error().what}.find("DECIMAL(39,0)") != std::string_view::npos);

    INFO("the same arena is the one the in-window helper path actually uses");
    const auto built = make_decimal(38, 20, "d");
    REQUIRE(built.type() == logical_type::DECIMAL);
    const auto* ext = built.extension_as<decimal_logical_type_extension>();
    REQUIRE(ext != nullptr);
    CHECK(ext->width() == 38);
    CHECK(ext->scale() == 20);
    CHECK(decimal_resource() == helper_arena);
}
