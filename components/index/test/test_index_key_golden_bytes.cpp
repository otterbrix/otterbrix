#include <catch2/catch_test_macros.hpp>

#include <components/index/binary_codec.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>
#include <services/index/index_types.hpp>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

// Index key bytes are persisted (bitcask log/hash table, btree records). These tests pin today's exact bytes;
// an encode->decode round trip stays green when both sides change together, these do not.

namespace {
    using bytes_t = std::vector<uint8_t>;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    std::pmr::memory_resource* arena() {
        static core::pmr::otterbrix_resource resource;
        return &resource;
    }

    complex_logical_type make_decimal(uint8_t width, uint8_t scale) {
        auto created = complex_logical_type::create_decimal(arena(), width, scale);
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }

    std::string hex(const bytes_t& bytes) {
        static constexpr char digits[] = "0123456789ABCDEF";
        std::string out;
        for (const auto byte : bytes) {
            out += digits[byte >> 4U];
            out += digits[byte & 0xFU];
            out += ' ';
        }
        return out;
    }

    bytes_t to_bytes(std::string_view raw) {
        bytes_t out(raw.size());
        std::memcpy(out.data(), raw.data(), raw.size());
        return out;
    }

    void require_bytes(const bytes_t& actual, const bytes_t& expected) {
        INFO("actual:   " << hex(actual));
        INFO("expected: " << hex(expected));
        REQUIRE(actual.size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            INFO("byte " << i);
            REQUIRE(actual[i] == expected[i]);
        }
    }

    // bitcask / btree batch inserts, deletes, lookups and staged-probe matching.
    bytes_t vector_bytes(const components::vector::vector_t& keys) {
        std::pmr::string buffer(arena());
        bytes_t found;
        size_t seen = 0;
        const auto error =
            services::index::for_each_key_bytes(keys, 1, &buffer, [&](size_t emitted, std::string_view b) {
                REQUIRE(emitted == 0);
                found = to_bytes(b);
                ++seen;
                return core::error_t::no_error();
            });
        REQUIRE_FALSE(error.contains_error());
        REQUIRE(seen == 1);
        return found;
    }

    template<typename T>
    components::vector::vector_t make_keys(const complex_logical_type& type, T value) {
        components::vector::vector_t keys(arena(), type, 1);
        keys.data<T>()[0] = value;
        return keys;
    }

    constexpr uint32_t hash_seed = 0x9E3779B9U;

    uint32_t hash_of(components::vector::vector_t keys) {
        components::vector::vector_t hashes(arena(), logical_type::UINTEGER, 1);
        components::vector::vector_ops::hash32(keys, hashes, 1, hash_seed);
        return hashes.data<uint32_t>()[0];
    }

    // What bitcask_index_disk_t::hash_key_bytes does with stored key bytes on load / merge: decode them into a
    // one-row vector and hash32 it. (hash_key_bytes is private; this calls the same public functions in the
    // same order.)
    uint32_t hash_of_stored_bytes(const bytes_t& stored) {
        auto key = services::index::decode_key_bytes(
            arena(),
            std::string_view(reinterpret_cast<const char*>(stored.data()), stored.size()));
        REQUIRE_FALSE(key.has_error());
        return hash_of(std::move(key.value()));
    }

    // Insert hashes the column vector, load hashes the decoded stored bytes: both must land in one bucket.
    void require_hash_consistent(components::vector::vector_t column, const bytes_t& stored) {
        REQUIRE(hash_of(std::move(column)) == hash_of_stored_bytes(stored));
    }

    // Insert and lookup of one key meet on the same bytes.
    template<typename T>
    void pin(const complex_logical_type& type, T raw, const bytes_t& expected) {
        require_bytes(vector_bytes(make_keys<T>(type, raw)), expected);
        require_hash_consistent(make_keys<T>(type, raw), expected);
    }

    template<typename T>
    void pin_scalar(logical_type type, T value, const bytes_t& expected) {
        pin<T>(complex_logical_type{type}, value, expected);
    }

    template<typename T>
    void pin_decimal(uint8_t width, uint8_t scale, T raw, const bytes_t& expected) {
        const auto type = make_decimal(width, scale);
        pin<T>(type, raw, expected);
    }
} // namespace

TEST_CASE("index key bytes: integers") {
    // clang-format off
    pin_scalar<int8_t>(logical_type::TINYINT, int8_t{-7}, {0x0B, 0xF9});
    pin_scalar<uint8_t>(logical_type::UTINYINT, uint8_t{200}, {0x1B, 0xC8});
    pin_scalar<int16_t>(logical_type::SMALLINT, int16_t{-1234}, {0x0C, 0x2E, 0xFB});
    pin_scalar<uint16_t>(logical_type::USMALLINT, uint16_t{60000}, {0x1C, 0x60, 0xEA});
    pin_scalar<int32_t>(logical_type::INTEGER, int32_t{-123456}, {0x0D, 0xC0, 0x1D, 0xFE, 0xFF});
    pin_scalar<uint32_t>(logical_type::UINTEGER, uint32_t{4000000000U}, {0x1D, 0x00, 0x28, 0x6B, 0xEE});
    pin_scalar<int64_t>(logical_type::BIGINT,
                        int64_t{-9876543210LL},
                        {0x0E, 0x16, 0xE9, 0x4F, 0xB3, 0xFD, 0xFF, 0xFF, 0xFF});
    pin_scalar<uint64_t>(logical_type::UBIGINT,
                         std::numeric_limits<uint64_t>::max(),
                         {0x1E, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
    // clang-format on
}

TEST_CASE("index key bytes: boolean") {
    // clang-format off
    pin_scalar<bool>(logical_type::BOOLEAN, true, {0x0A, 0x01});
    pin_scalar<bool>(logical_type::BOOLEAN, false, {0x0A, 0x00});
    // clang-format on
}

TEST_CASE("index key bytes: floating point") {
    // clang-format off
    pin_scalar<float>(logical_type::FLOAT, 1.25F, {0x17, 0x00, 0x00, 0xA0, 0x3F});
    pin_scalar<float>(logical_type::FLOAT, 0.0F, {0x17, 0x00, 0x00, 0x00, 0x00});
    pin_scalar<float>(logical_type::FLOAT, -0.0F, {0x17, 0x00, 0x00, 0x00, 0x80});
    pin_scalar<float>(logical_type::FLOAT,
                      std::numeric_limits<float>::quiet_NaN(),
                      {0x17, 0x00, 0x00, 0xC0, 0x7F});
    pin_scalar<double>(logical_type::DOUBLE, 3.5, {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x40});
    pin_scalar<double>(logical_type::DOUBLE, 0.0, {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
    pin_scalar<double>(logical_type::DOUBLE, -0.0, {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80});
    pin_scalar<double>(logical_type::DOUBLE,
                       std::numeric_limits<double>::quiet_NaN(),
                       {0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F});
    // clang-format on
}

TEST_CASE("index key bytes: 0.0 and -0.0 are different keys to bitcask but equal to the btree comparator") {
    using components::index::codec::read_tagged_value;
    const bytes_t positive{0x18, 0, 0, 0, 0, 0, 0, 0x00, 0x00};
    const bytes_t negative{0x18, 0, 0, 0, 0, 0, 0, 0x00, 0x80};
    REQUIRE(positive != negative);
    size_t pos_p = 0;
    size_t pos_n = 0;
    bool ok = true;
    const auto view_p = read_tagged_value(reinterpret_cast<const char*>(positive.data()), positive.size(), pos_p, &ok);
    const auto view_n = read_tagged_value(reinterpret_cast<const char*>(negative.data()), negative.size(), pos_n, &ok);
    REQUIRE(ok);
    REQUIRE(view_p == view_n);
}

TEST_CASE("index key bytes: strings") {
    const auto text_keys = [](std::string_view text) {
        components::vector::vector_t keys(arena(), logical_type::STRING_LITERAL, 1);
        keys.data<std::string_view>()[0] = text;
        return keys;
    };
    const auto pin_text = [&](std::string_view text, const bytes_t& expected) {
        require_bytes(vector_bytes(text_keys(text)), expected);
        require_hash_consistent(text_keys(text), expected);
    };
    pin_text("hello", {0x23, 0x05, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F});
    pin_text("", {0x23, 0x00, 0x00, 0x00, 0x00});
    pin_text(std::string_view("a\0b", 3), {0x23, 0x03, 0x00, 0x00, 0x00, 0x61, 0x00, 0x62});
}

TEST_CASE("index key bytes: date and time types") {
    // clang-format off
    pin<int32_t>(complex_logical_type{logical_type::DATE},
                 int32_t{42},
                 {0x10, 0x2A, 0x00, 0x00, 0x00});
    pin<int64_t>(complex_logical_type{logical_type::TIME},
                 int64_t{123456789},
                 {0x11, 0x15, 0xCD, 0x5B, 0x07, 0x00, 0x00, 0x00, 0x00});
    pin<int64_t>(complex_logical_type{logical_type::TIMESTAMP},
                 int64_t{7777777},
                 {0x13, 0xF1, 0xAD, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00});
    pin<int64_t>(complex_logical_type{logical_type::TIMESTAMP_TZ},
                 int64_t{-5555555},
                 {0x14, 0x9D, 0x3A, 0xAB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
    // clang-format on
}

TEST_CASE("index key bytes: DECIMAL of each storage width") {
    // clang-format off
    using components::types::int128_t;
    pin_decimal<int16_t>(4, 2, int16_t{-1234}, {0x16, 0x04, 0x02, 0x2E, 0xFB});
    pin_decimal<int32_t>(9, 2, int32_t{-123456789}, {0x16, 0x09, 0x02, 0xEB, 0x32, 0xA4, 0xF8});
    pin_decimal<int64_t>(18,
                         2,
                         int64_t{-123456789012345678LL},
                         {0x16, 0x12, 0x02, 0xB2, 0x0C, 0xCF, 0x59, 0xB4, 0x64, 0x49, 0xFE});
    const int128_t wide = -(int128_t{123456789012345678LL} * 1000000000000LL + 901234567890LL);
    pin_decimal<int128_t>(38,
                          8,
                          wide,
                          {0x16, 0x26, 0x08, 0x2E, 0xF5, 0xC0, 0xB1, 0x11, 0x1F, 0x8C, 0x3C, 0x09, 0xF0, 0x16, 0x71,
                           0xFE, 0xFF, 0xFF, 0xFF});
    // clang-format on
}

TEST_CASE("index key bytes: NULL key is the single NA tag") {
    // A NULL row of any key type collapses to the same one byte.
    for (const auto type : {logical_type::BIGINT, logical_type::STRING_LITERAL, logical_type::DOUBLE}) {
        components::vector::vector_t keys(arena(), type, 1);
        keys.validity().set_invalid(0);
        require_bytes(vector_bytes(keys), {0x00});
        require_hash_consistent(keys, {0x00});
        REQUIRE(hash_of(std::move(keys)) == 0xF2C57506U);
    }
    for (const auto width : {4, 9, 18, 38}) {
        components::vector::vector_t decimal_keys(arena(), make_decimal(static_cast<uint8_t>(width), 2), 1);
        decimal_keys.validity().set_invalid(0);
        require_bytes(vector_bytes(decimal_keys), {0x00});
        require_hash_consistent(decimal_keys, {0x00});
        REQUIRE(hash_of(std::move(decimal_keys)) == 0xF2C57506U);
    }
}

TEST_CASE("index key bytes: HUGEINT is written but never read back") {
    using components::types::int128_t;
    const int128_t value = -(int128_t{123456789012345678LL} * 1000000000000LL + 901234567890LL);
    const bytes_t
        expected{0x0F, 0x2E, 0xF5, 0xC0, 0xB1, 0x11, 0x1F, 0x8C, 0x3C, 0x09, 0xF0, 0x16, 0x71, 0xFE, 0xFF, 0xFF, 0xFF};
    require_bytes(vector_bytes(make_keys<int128_t>(complex_logical_type{logical_type::HUGEINT}, value)), expected);

    const auto decoded = services::index::decode_key_bytes(
        arena(),
        std::string_view(reinterpret_cast<const char*>(expected.data()), expected.size()));
    REQUIRE(decoded.has_error());
    REQUIRE(decoded.error().type == core::error_code_t::data_corruption);
}

TEST_CASE("index key bytes: UHUGEINT and composite keys have no encoding") {
    {
        components::vector::vector_t keys(arena(), logical_type::UHUGEINT, 1);
        std::pmr::string buffer(arena());
        const auto error = services::index::for_each_key_bytes(keys, 1, &buffer, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::invalid_parameter);

        const std::string_view tag_only("\x1F", 1);
        REQUIRE(services::index::decode_key_bytes(arena(), tag_only).has_error());
    }
    {
        components::vector::vector_t keys(
            arena(),
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}),
            1);
        std::pmr::string buffer(arena());
        const auto error = services::index::for_each_key_bytes(keys, 1, &buffer, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::invalid_parameter);
    }
}

TEST_CASE("index key bytes: a batch emits one record per row, NULL rows included") {
    components::vector::vector_t keys(arena(), logical_type::INTEGER, 3);
    keys.data<int32_t>()[0] = 1;
    keys.data<int32_t>()[1] = 2;
    keys.data<int32_t>()[2] = 3;
    keys.validity().set_invalid(1);
    std::pmr::string buffer(arena());
    std::vector<bytes_t> rows;
    const auto error = services::index::for_each_key_bytes(keys, 3, &buffer, [&](size_t row, std::string_view b) {
        REQUIRE(row == rows.size());
        rows.push_back(to_bytes(b));
        return core::error_t::no_error();
    });
    REQUIRE_FALSE(error.contains_error());
    REQUIRE(rows.size() == 3);
    require_bytes(rows[0], {0x0D, 0x01, 0x00, 0x00, 0x00});
    require_bytes(rows[1], {0x00});
    require_bytes(rows[2], {0x0D, 0x03, 0x00, 0x00, 0x00});
}

TEST_CASE("index key bytes: decode of the pinned bytes (btree key view and key vector)") {
    using components::index::codec::read_tagged_value;
    using components::types::physical_type;

    const auto view_of = [](const bytes_t& encoded, bool& ok) {
        size_t pos = 0;
        auto view = read_tagged_value(reinterpret_cast<const char*>(encoded.data()), encoded.size(), pos, &ok);
        REQUIRE(pos == encoded.size());
        return view;
    };
    bool ok = true;
    const auto integer = view_of({0x0D, 0xC0, 0x1D, 0xFE, 0xFF}, ok);
    REQUIRE(ok);
    REQUIRE(integer.value<physical_type::INT32>() == -123456);
    // A STRING view borrows the encoded buffer, so it must outlive the view.
    const bytes_t encoded_text{0x23, 0x05, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F};
    const auto text = view_of(encoded_text, ok);
    REQUIRE(ok);
    REQUIRE(text == components::types::physical_value(std::string_view("hello")));
    const auto none = view_of({0x00}, ok);
    REQUIRE(ok);
    REQUIRE(none.type() == physical_type::NA);

    // The btree key view carries no width/scale, so it refuses DECIMAL outright.
    ok = true;
    const bytes_t decimal{0x16, 0x04, 0x02, 0x2E, 0xFB};
    size_t pos = 0;
    const auto refused = read_tagged_value(reinterpret_cast<const char*>(decimal.data()), decimal.size(), pos, &ok);
    REQUIRE(refused.type() == physical_type::NA);
    REQUIRE_FALSE(ok);

    // The key vector decode (bitcask merge / hash_key_bytes) does take DECIMAL.
    const auto decoded = services::index::decode_key_bytes(
        arena(),
        std::string_view(reinterpret_cast<const char*>(decimal.data()), decimal.size()));
    REQUIRE_FALSE(decoded.has_error());
    REQUIRE(decoded.value().type().type() == logical_type::DECIMAL);
    REQUIRE(decoded.value().data<int16_t>()[0] == -1234);
}

// key_hash sits in hash_index.bin but is recomputed from the stored key bytes on every open, so insert and decode
// must agree; the constants pin today's function. Per-type flat-vs-stored-bytes consistency is asserted inside pin().
// Under seed 0x9E3779B9; the STRING value is deliberately not a constant (it depends on std::hash<string_view>).
TEST_CASE("index key bytes: hash32 of a key under a fixed seed") {
    // A lookup hashes a constant one-row vector; the batch path hashes a flat one.
    REQUIRE(hash_of(make_keys<int64_t>(complex_logical_type{logical_type::BIGINT}, int64_t{1})) == 0x2A724163U);
    REQUIRE(hash_of(components::vector::make_scalar(arena(), int64_t{1})) == 0x2A724163U);

    components::vector::vector_t flat_text(arena(), logical_type::STRING_LITERAL, 1);
    flat_text.data<std::string_view>()[0] = "hello";
    REQUIRE(hash_of(flat_text) == hash_of(components::vector::make_scalar(arena(), std::string_view("hello"))));

    // 0.0 and -0.0 hash alike though their key bytes differ: same bucket, distinct keys.
    REQUIRE(hash_of(components::vector::make_scalar(arena(), 0.0)) == 0xFDA5B8D2U);
    REQUIRE(hash_of(components::vector::make_scalar(arena(), -0.0)) == 0xFDA5B8D2U);
    REQUIRE(hash_of(components::vector::make_null(arena(), complex_logical_type{logical_type::NA})) == 0xF2C57506U);
}
