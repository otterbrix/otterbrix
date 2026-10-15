#include <catch2/catch_test_macros.hpp>
#include <components/table/base_statistics.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/vector.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pmr.hpp>

#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <unistd.h>
#include <vector>

// base_statistics_t::serialize is a persisted checkpoint format. These tests pin today's exact bytes (and read
// them back through deserialize); a round trip alone stays green when both sides change together.

namespace {
    using namespace components::types;
    using namespace components::table;
    using components::vector::vector_t;

    std::string golden_db_path() {
        static const std::string path = "/tmp/test_otterbrix_stats_golden_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    struct golden_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;

        golden_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::string hex(const std::vector<uint8_t>& bytes) {
        static constexpr char digits[] = "0123456789ABCDEF";
        std::string out;
        for (const auto byte : bytes) {
            out += digits[byte >> 4U];
            out += digits[byte & 0xFU];
            out += ' ';
        }
        return out;
    }

    // Serializes `stats` followed by a 0xEE sentinel, checks the stream is exactly `expected` + sentinel, then
    // deserializes it and checks the reader stopped on the sentinel with the same statistics.
    void check_serialized(const base_statistics_t& stats, const std::vector<uint8_t>& expected) {
        std::remove(golden_db_path().c_str());
        golden_env_t env;
        storage::single_file_block_manager_t bm(env.buffer_manager, env.fs, golden_db_path());
        REQUIRE_FALSE(bm.create_new_database().has_error());
        storage::metadata_manager_t manager(bm);

        storage::meta_block_pointer_t pointer;
        {
            storage::metadata_writer_t writer(manager);
            stats.serialize(writer);
            writer.write<uint8_t>(0xEE);
            pointer = writer.get_block_pointer();
            REQUIRE_FALSE(writer.flush().has_error());
        }
        {
            storage::metadata_reader_t reader(manager, pointer);
            std::vector<uint8_t> actual(expected.size() + 1);
            reader.read_data(reinterpret_cast<std::byte*>(actual.data()), actual.size());
            REQUIRE_FALSE(reader.has_error());
            std::vector<uint8_t> with_sentinel = expected;
            with_sentinel.push_back(0xEE);
            INFO("actual:   " << hex(actual));
            INFO("expected: " << hex(with_sentinel));
            REQUIRE(actual == with_sentinel);
        }
        {
            storage::metadata_reader_t reader(manager, pointer);
            const auto back = base_statistics_t::deserialize(&env.resource, reader);
            REQUIRE_FALSE(reader.has_error());
            REQUIRE(reader.read<uint8_t>() == 0xEE);
            REQUIRE(back.type() == stats.type());
            REQUIRE(back.has_stats() == stats.has_stats());
            REQUIRE(back.null_count() == stats.null_count());
            REQUIRE(back.has_bounds() == (expected[10] == 1));
            if (expected[10] == 1) {
                switch (stats.type()) {
                    case logical_type::BOOLEAN:
                        REQUIRE(back.min_as<bool>() == stats.min_as<bool>());
                        REQUIRE(back.max_as<bool>() == stats.max_as<bool>());
                        break;
                    case logical_type::TINYINT:
                        REQUIRE(back.min_as<int8_t>() == stats.min_as<int8_t>());
                        REQUIRE(back.max_as<int8_t>() == stats.max_as<int8_t>());
                        break;
                    case logical_type::SMALLINT:
                        REQUIRE(back.min_as<int16_t>() == stats.min_as<int16_t>());
                        REQUIRE(back.max_as<int16_t>() == stats.max_as<int16_t>());
                        break;
                    case logical_type::INTEGER:
                        REQUIRE(back.min_as<int32_t>() == stats.min_as<int32_t>());
                        REQUIRE(back.max_as<int32_t>() == stats.max_as<int32_t>());
                        break;
                    case logical_type::BIGINT:
                        REQUIRE(back.min_as<int64_t>() == stats.min_as<int64_t>());
                        REQUIRE(back.max_as<int64_t>() == stats.max_as<int64_t>());
                        break;
                    case logical_type::UTINYINT:
                        REQUIRE(back.min_as<uint8_t>() == stats.min_as<uint8_t>());
                        REQUIRE(back.max_as<uint8_t>() == stats.max_as<uint8_t>());
                        break;
                    case logical_type::USMALLINT:
                        REQUIRE(back.min_as<uint16_t>() == stats.min_as<uint16_t>());
                        REQUIRE(back.max_as<uint16_t>() == stats.max_as<uint16_t>());
                        break;
                    case logical_type::UINTEGER:
                        REQUIRE(back.min_as<uint32_t>() == stats.min_as<uint32_t>());
                        REQUIRE(back.max_as<uint32_t>() == stats.max_as<uint32_t>());
                        break;
                    case logical_type::UBIGINT:
                        REQUIRE(back.min_as<uint64_t>() == stats.min_as<uint64_t>());
                        REQUIRE(back.max_as<uint64_t>() == stats.max_as<uint64_t>());
                        break;
                    case logical_type::FLOAT:
                        REQUIRE_FALSE((back.min_as<float>() < stats.min_as<float>() ||
                                       stats.min_as<float>() < back.min_as<float>()));
                        REQUIRE_FALSE((back.max_as<float>() < stats.max_as<float>() ||
                                       stats.max_as<float>() < back.max_as<float>()));
                        break;
                    case logical_type::DOUBLE:
                        REQUIRE_FALSE((back.min_as<double>() < stats.min_as<double>() ||
                                       stats.min_as<double>() < back.min_as<double>()));
                        REQUIRE_FALSE((back.max_as<double>() < stats.max_as<double>() ||
                                       stats.max_as<double>() < back.max_as<double>()));
                        break;
                    case logical_type::HUGEINT:
                        REQUIRE(back.min_as<int128_t>() == stats.min_as<int128_t>());
                        REQUIRE(back.max_as<int128_t>() == stats.max_as<int128_t>());
                        break;
                    case logical_type::UHUGEINT:
                        REQUIRE(back.min_as<uint128_t>() == stats.min_as<uint128_t>());
                        REQUIRE(back.max_as<uint128_t>() == stats.max_as<uint128_t>());
                        break;
                    case logical_type::DECIMAL:
                        REQUIRE(back.decimal_width() == stats.decimal_width());
                        REQUIRE(back.decimal_scale() == stats.decimal_scale());
                        REQUIRE(back.min_as<int128_t>() == stats.min_as<int128_t>());
                        REQUIRE(back.max_as<int128_t>() == stats.max_as<int128_t>());
                        break;
                    case logical_type::STRING_LITERAL:
                        REQUIRE(back.min_string() == stats.min_string());
                        REQUIRE(back.max_string() == stats.max_string());
                        break;
                    default:
                        FAIL("type without bounds");
                }
            }
        }
        std::remove(golden_db_path().c_str());
    }
} // namespace

TEST_CASE("statistics golden bytes: boolean_min_false_max_true") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::BOOLEAN, 0);
    stats.set_bounds<bool>(false, true);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0A, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: integer_with_nulls") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::INTEGER, 2);
    stats.set_bounds<int32_t>(int32_t{-5}, int32_t{100});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0D, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFB,
        0xFF, 0xFF, 0xFF, 0x64, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: bigint_with_nulls") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::BIGINT, 3);
    stats.set_bounds<int64_t>(int64_t{-5}, int64_t{100});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0E, 0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFB,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x64, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: ubigint_full_range") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::UBIGINT, 0);
    stats.set_bounds<uint64_t>(uint64_t{7}, std::numeric_limits<uint64_t>::max());
    // clang-format off
    const std::vector<uint8_t> expected{
        0x1E, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x07,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: float") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::FLOAT, 0);
    stats.set_bounds<float>(-1.5F, 2.5F);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0xC0, 0xBF, 0x00, 0x00, 0x20, 0x40};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: double") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::DOUBLE, 0);
    stats.set_bounds<double>(-1.5, 2.5);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x18, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0xBF, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x04, 0x40};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: hugeint") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::HUGEINT, 0);
    stats.set_bounds<int128_t>(-(int128_t{100000000000LL} * 1000000000LL), int128_t{100000000000LL} * 1000000000LL);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0F, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0xF0, 0x9C, 0xD2, 0xA1, 0x38, 0x94, 0xFA, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x10, 0x63, 0x2D, 0x5E, 0xC7, 0x6B, 0x05,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: uhugeint") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::UHUGEINT, 0);
    stats.set_bounds<uint128_t>(uint128_t{5}, uint128_t{1} << 100);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x1F, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x05,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: decimal_carries_width_scale_with_each_bound") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::DECIMAL, 1);
    stats.set_decimal_bounds(9, 2, -12345, 67890);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x16, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x09,
        0x02, 0xC7, 0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x09, 0x02, 0x32, 0x09, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: string_is_u32_length_prefixed") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::STRING_LITERAL, 4);
    stats.set_bounds<std::string_view>("apple", "pear");
    // clang-format off
    const std::vector<uint8_t> expected{
        0x23, 0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x05,
        0x00, 0x00, 0x00, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x04, 0x00, 0x00, 0x00,
        0x70, 0x65, 0x61, 0x72};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: string_empty_min") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::STRING_LITERAL, 0);
    stats.set_bounds<std::string_view>("", "z");
    // clang-format off
    const std::vector<uint8_t> expected{
        0x23, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x7A};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: no_stats_writes_no_bounds") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::BIGINT);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: no_stats_keeps_null_count") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::BIGINT);
    stats.set_null_count(5);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0E, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: type_without_minmax_writes_no_bounds") {
    core::pmr::otterbrix_resource resource;
    const base_statistics_t stats(&resource, logical_type::DATE, 1);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x10, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: tinyint") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::TINYINT, 1);
    stats.set_bounds<int8_t>(int8_t{-7}, int8_t{100});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0B, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xF9,
        0x64};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: smallint") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::SMALLINT, 0);
    stats.set_bounds<int16_t>(int16_t{-30000}, int16_t{30000});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0C, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xD0,
        0x8A, 0x30, 0x75};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: utinyint") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::UTINYINT, 0);
    stats.set_bounds<uint8_t>(uint8_t{3}, uint8_t{200});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x1B, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03,
        0xC8};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: usmallint") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::USMALLINT, 0);
    stats.set_bounds<uint16_t>(uint16_t{3}, uint16_t{60000});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x1C, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03,
        0x00, 0x60, 0xEA};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: uinteger") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::UINTEGER, 0);
    stats.set_bounds<uint32_t>(uint32_t{3}, uint32_t{4000000000U});
    // clang-format off
    const std::vector<uint8_t> expected{
        0x1D, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03,
        0x00, 0x00, 0x00, 0x00, 0x28, 0x6B, 0xEE};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: decimal_int128_storage") {
    core::pmr::otterbrix_resource resource;
    const int128_t wide = int128_t{123456789012345678LL} * 1000000000000LL + 901234567890LL;
    base_statistics_t stats(&resource, logical_type::DECIMAL, 0);
    stats.set_decimal_bounds(38, 4, -wide, wide);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x16, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x26,
        0x04, 0x2E, 0xF5, 0xC0, 0xB1, 0x11, 0x1F, 0x8C, 0x3C, 0x09, 0xF0, 0x16,
        0x71, 0xFE, 0xFF, 0xFF, 0xFF, 0x26, 0x04, 0xD2, 0x0A, 0x3F, 0x4E, 0xEE,
        0xE0, 0x73, 0xC3, 0xF6, 0x0F, 0xE9, 0x8E, 0x01, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: bounds_learned_from_a_vector") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::BIGINT);
    vector_t vec(&resource, logical_type::BIGINT, 3);
    vec.data<int64_t>()[0] = 100;
    vec.data<int64_t>()[1] = -5;
    vec.data<int64_t>()[2] = 0;
    vec.validity().set_invalid(2);
    stats.update(vec, 3);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x0E, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFB,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x64, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: string_bounds_learned_from_a_vector") {
    core::pmr::otterbrix_resource resource;
    base_statistics_t stats(&resource, logical_type::STRING_LITERAL);
    vector_t vec(&resource, logical_type::STRING_LITERAL, 3);
    vec.data<std::string_view>()[0] = "pear";
    vec.data<std::string_view>()[1] = "apple";
    vec.data<std::string_view>()[2] = "zzz";
    vec.validity().set_invalid(2);
    stats.update(vec, 3);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x23, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x05,
        0x00, 0x00, 0x00, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x04, 0x00, 0x00, 0x00,
        0x70, 0x65, 0x61, 0x72};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: decimal_bounds_learned_from_a_vector") {
    core::pmr::otterbrix_resource resource;
    auto created = complex_logical_type::create_decimal(&resource, 9, 2);
    REQUIRE_FALSE(created.has_error());
    base_statistics_t stats(&resource, logical_type::DECIMAL);
    vector_t vec(&resource, created.value(), 3);
    vec.data<int32_t>()[0] = 67890;
    vec.data<int32_t>()[1] = -12345;
    vec.data<int32_t>()[2] = 0;
    vec.validity().set_invalid(2);
    stats.update(vec, 3);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x16, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x09,
        0x02, 0xC7, 0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x09, 0x02, 0x32, 0x09, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

TEST_CASE("statistics golden bytes: decimal_int128_bounds_learned_from_a_vector") {
    core::pmr::otterbrix_resource resource;
    auto created = complex_logical_type::create_decimal(&resource, 38, 4);
    REQUIRE_FALSE(created.has_error());
    const int128_t wide = int128_t{123456789012345678LL} * 1000000000000LL + 901234567890LL;
    base_statistics_t stats(&resource, logical_type::DECIMAL);
    vector_t vec(&resource, created.value(), 2);
    vec.data<int128_t>()[0] = wide;
    vec.data<int128_t>()[1] = -wide;
    stats.update(vec, 2);
    // clang-format off
    const std::vector<uint8_t> expected{
        0x16, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x26,
        0x04, 0x2E, 0xF5, 0xC0, 0xB1, 0x11, 0x1F, 0x8C, 0x3C, 0x09, 0xF0, 0x16,
        0x71, 0xFE, 0xFF, 0xFF, 0xFF, 0x26, 0x04, 0xD2, 0x0A, 0x3F, 0x4E, 0xEE,
        0xE0, 0x73, 0xC3, 0xF6, 0x0F, 0xE9, 0x8E, 0x01, 0x00, 0x00, 0x00};
    // clang-format on
    check_serialized(stats, expected);
}

// column_data_t::append builds a fresh base_statistics_t per batch, update()s it and merges it into the column's.
TEST_CASE("statistics merge: DECIMAL bounds widen across batches") {
    core::pmr::otterbrix_resource resource;
    auto created = complex_logical_type::create_decimal(&resource, 9, 2);
    REQUIRE_FALSE(created.has_error());

    base_statistics_t column(&resource, logical_type::DECIMAL);
    for (const int32_t raw : {100, -500}) {
        base_statistics_t batch(&resource, logical_type::DECIMAL);
        vector_t vec(&resource, created.value(), 1);
        vec.data<int32_t>()[0] = raw;
        batch.update(vec, 1);
        column.merge(batch);
    }
    REQUIRE(column.min_as<int128_t>() == -500);
    REQUIRE(column.max_as<int128_t>() == 100);
}

TEST_CASE("statistics merge: HUGEINT bounds widen across batches") {
    core::pmr::otterbrix_resource resource;

    base_statistics_t column(&resource, logical_type::HUGEINT);
    for (const int128_t value : {int128_t{1}, int128_t{2}}) {
        base_statistics_t batch(&resource, logical_type::HUGEINT);
        vector_t vec(&resource, logical_type::HUGEINT, 1);
        vec.data<int128_t>()[0] = value;
        batch.update(vec, 1);
        column.merge(batch);
    }
    REQUIRE(column.min_as<int128_t>() == 1);
    REQUIRE(column.max_as<int128_t>() == 2);
}

TEST_CASE("statistics merge: UHUGEINT bounds widen across batches") {
    core::pmr::otterbrix_resource resource;

    base_statistics_t column(&resource, logical_type::UHUGEINT);
    for (const uint128_t value : {uint128_t{1} << 100, uint128_t{2}}) {
        base_statistics_t batch(&resource, logical_type::UHUGEINT);
        vector_t vec(&resource, logical_type::UHUGEINT, 1);
        vec.data<uint128_t>()[0] = value;
        batch.update(vec, 1);
        column.merge(batch);
    }
    REQUIRE(column.min_as<uint128_t>() == 2);
    REQUIRE(column.max_as<uint128_t>() == uint128_t{1} << 100);
}

TEST_CASE("statistics merge: STRING bounds widen across batches and survive a copy") {
    core::pmr::otterbrix_resource resource;

    base_statistics_t column(&resource, logical_type::STRING_LITERAL);
    for (const std::string_view value :
         {std::string_view{"pear"}, std::string_view{"apple"}, std::string_view{"zebra"}}) {
        base_statistics_t batch(&resource, logical_type::STRING_LITERAL);
        vector_t vec(&resource, logical_type::STRING_LITERAL, 1);
        vec.data<std::string_view>()[0] = value;
        batch.update(vec, 1);
        column.merge(batch);
    }
    const base_statistics_t copy(column);
    REQUIRE(copy.min_string() == "apple");
    REQUIRE(copy.max_string() == "zebra");
}
