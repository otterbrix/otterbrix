#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/index/logical_value_binary_codec.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>

namespace {
    // create_decimal reports an out-of-window (width, scale) through core::error_t now,
    // instead of an assert that vanished under NDEBUG. Every literal these tests use is
    // inside the window, so the helper checks the result and hands back the type.
    components::types::complex_logical_type make_decimal(uint8_t width, uint8_t scale) {
        auto created = components::types::complex_logical_type::create_decimal(width, scale);
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

TEST_CASE("logical_value_binary_codec: roundtrip_supported_types") {
    using components::index::codec::append_logical_value;
    using components::index::codec::read_logical_value;
    using components::types::complex_logical_type;
    using components::types::logical_value_t;

    auto resource = core::pmr::otterbrix_resource();

    std::vector<logical_value_t> values;
    values.emplace_back(&resource, complex_logical_type{components::types::logical_type::NA});
    values.emplace_back(&resource, true);
    values.emplace_back(&resource, int8_t{-7});
    values.emplace_back(&resource, uint8_t{7});
    values.emplace_back(&resource, int16_t{-1234});
    values.emplace_back(&resource, uint16_t{1234});
    values.emplace_back(&resource, int32_t{-123456});
    values.emplace_back(&resource, uint32_t{123456});
    values.emplace_back(&resource, int64_t{-9876543210LL});
    values.emplace_back(&resource, uint64_t{9876543210ULL});
    values.emplace_back(&resource, 1.25f);
    values.emplace_back(&resource, 3.5);
    values.emplace_back(&resource, std::string("hello-codec"));

    values.emplace_back(&resource, core::date::date_t{core::date::days{42}});
    values.emplace_back(&resource, core::date::time_t{core::date::microseconds{123456789}});
    values.emplace_back(&resource, core::date::timestamp_t{core::date::microseconds{7777777}});
    values.emplace_back(&resource, core::date::timestamptz_t{core::date::microseconds{-5555555}});
    values.emplace_back(
        logical_value_t::create_decimal(&resource, make_decimal(18, 2), 123456789));
    values.emplace_back(logical_value_t::create_decimal(&resource,
                                                        make_decimal(38, 8),
                                                        components::types::int128_t{1234567890123456789LL}));
    for (const auto& input : values) {
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, input);

        size_t pos = 0;
        const auto decoded = read_logical_value(&resource, encoded, pos);

        REQUIRE(pos == encoded.size());
        REQUIRE(decoded.type().type() == input.type().type());
        REQUIRE(decoded == input);
    }
}

TEST_CASE("logical_value_binary_codec: read_logical_value_as_view") {
    using components::index::codec::append_logical_value;
    using components::index::codec::read_logical_value_as_view;
    using components::types::logical_value_t;
    using components::types::physical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("bool") {
        logical_value_t val(&resource, true);
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, val);
        size_t pos = 0;
        auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::BOOL);
        REQUIRE(pv.value<physical_type::BOOL>() == true);
        REQUIRE(pos == encoded.size());
    }

    SECTION("int32") {
        logical_value_t val(&resource, int32_t{-123456});
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, val);
        size_t pos = 0;
        auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::INT32);
        REQUIRE(pv.value<physical_type::INT32>() == -123456);
        REQUIRE(pos == encoded.size());
    }

    SECTION("uint64") {
        logical_value_t val(&resource, uint64_t{9876543210ULL});
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, val);
        size_t pos = 0;
        auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::UINT64);
        REQUIRE(pv.value<physical_type::UINT64>() == 9876543210ULL);
        REQUIRE(pos == encoded.size());
    }

    SECTION("double") {
        logical_value_t val(&resource, 3.5);
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, val);
        size_t pos = 0;
        auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::DOUBLE);
        REQUIRE(pv.value<physical_type::DOUBLE>() == Catch::Approx(3.5));
        REQUIRE(pos == encoded.size());
    }

    SECTION("string zero-copy") {
        logical_value_t val(&resource, std::string("hello-codec"));
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, val);
        size_t pos = 0;
        auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::STRING);
        auto sv = pv.value<physical_type::STRING>();
        REQUIRE(sv == "hello-codec");
        REQUIRE(sv.data() >= encoded.data());
        REQUIRE(sv.data() < encoded.data() + encoded.size());
        REQUIRE(pos == encoded.size());
    }

    SECTION("na") {
        logical_value_t val(&resource, components::types::complex_logical_type{components::types::logical_type::NA});
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, val);
        size_t pos = 0;
        auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::NA);
        REQUIRE(pos == encoded.size());
    }
}

TEST_CASE("logical_value_binary_codec: skip_logical_value") {
    using components::index::codec::append_logical_value;
    using components::index::codec::skip_logical_value;
    using components::types::complex_logical_type;
    using components::types::logical_value_t;

    auto resource = core::pmr::otterbrix_resource();

    std::vector<logical_value_t> values;
    values.emplace_back(&resource, complex_logical_type{components::types::logical_type::NA});
    values.emplace_back(&resource, true);
    values.emplace_back(&resource, int8_t{-7});
    values.emplace_back(&resource, uint8_t{7});
    values.emplace_back(&resource, int16_t{-1234});
    values.emplace_back(&resource, uint16_t{1234});
    values.emplace_back(&resource, int32_t{-123456});
    values.emplace_back(&resource, uint32_t{123456});
    values.emplace_back(&resource, int64_t{-9876543210LL});
    values.emplace_back(&resource, uint64_t{9876543210ULL});
    values.emplace_back(&resource, 1.25f);
    values.emplace_back(&resource, 3.5);
    values.emplace_back(&resource, std::string("hello-codec"));
    values.emplace_back(
        logical_value_t::create_decimal(&resource, make_decimal(18, 2), 123456789));
    values.emplace_back(logical_value_t::create_decimal(&resource,
                                                        make_decimal(38, 8),
                                                        components::types::int128_t{1234567890123456789LL}));

    for (const auto& input : values) {
        std::pmr::string encoded(&resource);
        append_logical_value(encoded, input);

        size_t pos = 0;
        skip_logical_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pos == encoded.size());
    }
}
