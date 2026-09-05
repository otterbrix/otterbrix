// The REVERSE direction of the DECIMAL overflow refusal.
//
// int -> DECIMAL was closed first: to_decimal reports "does not fit" with the decimal_limits
// sentinels, and logical_value_t::cast_as turns those into a conversion_failure reading
// "numeric field overflow" (components/types/logical_value.cpp, the create_decimal lambda).
// DECIMAL -> int is the same question asked backwards: decimal_to_numeric descales and hands
// back std::nullopt when the result does not fit the integer target, and that nullopt must not
// become a SILENT NA -- a success-shaped NULL standing in for a value that exists and simply
// does not fit.

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <memory_resource>

using namespace components::types;

namespace {
    complex_logical_type decimal_type(uint8_t width, uint8_t scale) {
        auto created = complex_logical_type::create_decimal(width, scale);
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

TEST_CASE("components::types::logical_value::a_descaled_decimal_that_does_not_fit_is_refused_not_nulled") {
    std::pmr::monotonic_buffer_resource resource;

    // DECIMAL(18,2): stored as an INT64 payload of value * 100.
    const auto dec = decimal_type(18, 2);

    SECTION("in range: the descale answers the value, so the refusal below is about the range") {
        const auto forty_two = logical_value_t::create_decimal(&resource, dec, int64_t{4200}); // 42.00
        auto casted = forty_two.cast_as(complex_logical_type{logical_type::TINYINT}, {});
        REQUIRE_FALSE(casted.has_error());
        REQUIRE_FALSE(casted.value().is_null());
        CHECK(casted.value().value<int8_t>() == 42);
    }

    SECTION("out of range for the target width: a refusal, never a NULL") {
        const auto too_big = logical_value_t::create_decimal(&resource, dec, int64_t{1234567}); // 12345.67
        auto casted = too_big.cast_as(complex_logical_type{logical_type::TINYINT}, {});
        INFO("cast of DECIMAL(18,2) 12345.67 to TINYINT");
        REQUIRE(casted.has_error());
        CHECK(casted.error().type == core::error_code_t::conversion_failure);
    }

    SECTION("a wider target that DOES hold it still answers") {
        const auto too_big = logical_value_t::create_decimal(&resource, dec, int64_t{1234567}); // 12345.67
        auto casted = too_big.cast_as(complex_logical_type{logical_type::INTEGER}, {});
        REQUIRE_FALSE(casted.has_error());
        // ROUNDED, not truncated, and that is the PostgreSQL answer for numeric::integer
        // (12345.67 -> 12346). Pinned here because the refusal above is a range decision made
        // on the DESCALED value, so which value that is has to be stated.
        CHECK(casted.value().value<int32_t>() == 12346);
    }

    SECTION("negative, out of range on the other side") {
        const auto very_negative = logical_value_t::create_decimal(&resource, dec, int64_t{-1234567});
        auto casted = very_negative.cast_as(complex_logical_type{logical_type::TINYINT}, {});
        REQUIRE(casted.has_error());
        CHECK(casted.error().type == core::error_code_t::conversion_failure);
    }
}
