#include <catch2/catch.hpp>

#include <components/index/logical_value_binary_codec.hpp>
#include <core/date/date_types.hpp>

TEST_CASE("logical_value_binary_codec: roundtrip_supported_types") {
    using components::index::codec::append_logical_value;
    using components::index::codec::read_logical_value;
    using components::types::complex_logical_type;
    using components::types::logical_value_t;

    auto resource = std::pmr::synchronized_pool_resource();

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
        logical_value_t::create_decimal(&resource, complex_logical_type::create_decimal(18, 2), 123456789));
    values.emplace_back(logical_value_t::create_decimal(&resource,
                                                        complex_logical_type::create_decimal(38, 8),
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
