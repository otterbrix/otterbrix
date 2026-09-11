#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/index/logical_value_binary_codec.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>

#include <limits>

namespace {
    // The one arena this file builds DECIMALs on: create_decimal allocates only on its refusal
    // path, and that message must live on a caller-owned arena, not the process-global one.
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    // create_decimal refuses an out-of-window (width, scale) via core::error_t; every literal
    // used here is in-window, so REQUIRE_FALSE is safe.
    components::types::complex_logical_type make_decimal(uint8_t width, uint8_t scale) {
        auto created = components::types::complex_logical_type::create_decimal(decimal_resource(), width, scale);
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
    values.emplace_back(logical_value_t::create_decimal(&resource, make_decimal(18, 2), 123456789));
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
    values.emplace_back(logical_value_t::create_decimal(&resource, make_decimal(18, 2), 123456789));
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

// Corrupt bytes must not kill the process: every buffer below is a key payload (b+tree leaf or bitcask
// segment) with no checksum, so the contract is decode-to-NA and report via `ok`, never abort.
namespace {
    // A stored key payload, byte for byte, built the way append_logical_value builds it.
    std::pmr::string bytes(std::pmr::memory_resource* resource, std::initializer_list<int> raw) {
        std::pmr::string out(resource);
        for (int b : raw) {
            out.push_back(static_cast<char>(static_cast<unsigned char>(b)));
        }
        return out;
    }

    constexpr int tag(components::types::logical_type t) { return static_cast<int>(t); }
} // namespace

TEST_CASE("logical_value_binary_codec: a corrupt logical tag refuses instead of aborting") {
    using components::index::codec::read_logical_value;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("one flipped bit turns a BIGINT key into an unsupported physical width") {
        // HUGEINT is physical INT128, an arm the key codec has no reader for.
        REQUIRE((tag(logical_type::BIGINT) ^ 1) == tag(logical_type::HUGEINT));
        auto buffer = bytes(&resource, {tag(logical_type::HUGEINT), 1, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        // The unflagged call is what the production read path makes; it too must return, not abort.
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }

    SECTION("a tag byte no logical type uses at all") {
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }

    SECTION("ENUM shares physical INT32 with INTEGER and DATE but has no arm") {
        auto buffer = bytes(&resource, {tag(logical_type::ENUM), 7, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }

    SECTION("a STRING length that runs past the end of the record") {
        // [tag][uint32 length][bytes]; the length is four stored bytes, so a flipped high
        // bit claims gigabytes of a five-byte record.
        auto buffer = bytes(&resource, {tag(logical_type::STRING_LITERAL), 0xFF, 0xFF, 0xFF, 0xFF, 'a'});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }

    SECTION("a DECIMAL width outside the representable window") {
        // 50 is past DECIMAL_MAX_WIDTH.
        REQUIRE((18 ^ 32) == 50);
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 50, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }
}

TEST_CASE("logical_value_binary_codec: a corrupt logical tag refuses in the view decoder") {
    using components::index::codec::read_logical_value_as_view;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("a tag byte no logical type uses at all") {
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_logical_value_as_view(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value_as_view(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }

    SECTION("a DECIMAL tag, which physical_value cannot carry") {
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 18, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_logical_value_as_view(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value_as_view(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }

    SECTION("a STRING length that runs past the end of the record") {
        auto buffer = bytes(&resource, {tag(logical_type::STRING_LITERAL), 0xFF, 0xFF, 0xFF, 0xFF, 'a'});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_logical_value_as_view(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value_as_view(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }

    SECTION("a record truncated inside the payload") {
        // Two bytes where an INT64 payload should be: with only an assert guarding it, the memcpy runs past
        // the end of the record under NDEBUG.
        auto buffer = bytes(&resource, {tag(logical_type::BIGINT), 1, 0});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_logical_value_as_view(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_logical_value_as_view(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }
}

TEST_CASE("logical_value_binary_codec: a corrupt logical tag refuses in the skipper") {
    using components::index::codec::skip_logical_value;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("a tag byte no logical type uses at all") {
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        skip_logical_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pos <= buffer.size());
    }

    SECTION("a DECIMAL width outside the representable window") {
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 50, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        skip_logical_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pos <= buffer.size());
    }

    SECTION("a STRING length that runs past the end of the record") {
        auto buffer = bytes(&resource, {tag(logical_type::STRING_LITERAL), 0xFF, 0xFF, 0xFF, 0xFF, 'a'});
        size_t pos = 0;
        bool ok = true;
        skip_logical_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        // The skipped position is what services::index::id_of then reads the row id from. Left past the
        // end of the record it turns the next read into an out-of-bounds one.
        CHECK(pos <= buffer.size());
    }
}

TEST_CASE("logical_value_binary_codec: read_le_raw refuses a short record") {
    using components::index::codec::read_le_raw;

    auto resource = core::pmr::otterbrix_resource();
    auto buffer = bytes(&resource, {1, 2});
    size_t pos = 0;
    // Eight bytes asked of a two-byte record: guarded only by an assert, release builds read six bytes past
    // the end of the buffer.
    bool ok = true;
    const auto v = read_le_raw<uint64_t>(buffer.data(), buffer.size(), pos, &ok);
    CHECK_FALSE(ok);
    CHECK(v == 0);
    CHECK(pos == 0);
}

// A well-formed record must not be reported as corrupt: `ok` is only ever set to FALSE, so a
// caller initialises it to true and one flag can cover a whole record of several values.
TEST_CASE("logical_value_binary_codec: a well-formed record leaves ok alone") {
    using components::index::codec::append_le;
    using components::index::codec::append_logical_value;
    using components::index::codec::read_le_raw;
    using components::index::codec::read_logical_value;
    using components::index::codec::read_logical_value_as_view;
    using components::index::codec::skip_logical_value;
    using components::types::logical_value_t;

    auto resource = core::pmr::otterbrix_resource();

    std::pmr::string encoded(&resource);
    append_logical_value(encoded, logical_value_t(&resource, std::string("a-real-key")));
    append_le<uint64_t>(encoded, uint64_t{4242});

    bool ok = true;
    size_t pos = 0;
    const auto decoded = read_logical_value(&resource, encoded, pos, &ok);
    CHECK(ok);
    CHECK(decoded.value<std::string_view>() == "a-real-key");

    pos = 0;
    const auto pv = read_logical_value_as_view(encoded.data(), encoded.size(), pos, &ok);
    CHECK(ok);
    CHECK(pv.type() == components::types::physical_type::STRING);

    // Exactly the services::index::id_of shape: skip the key, then read the row id behind it.
    pos = 0;
    skip_logical_value(encoded.data(), encoded.size(), pos, &ok);
    CHECK(ok);
    CHECK(read_le_raw<uint64_t>(encoded.data(), encoded.size(), pos, &ok) == 4242);
    CHECK(ok);
    CHECK(pos == encoded.size());
}

// A truncated record must not decode to a plausible zero in silence: read_le answers T{} without moving
// `pos`, so an unflagged short read turns a key clipped by a short write into the value 0 in the index.
TEST_CASE("logical_value_binary_codec: a truncated payload is a refusal, not a zero") {
    using components::index::codec::read_logical_value;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();
    auto buffer = bytes(&resource, {tag(logical_type::BIGINT), 1, 2, 3});

    bool ok = true;
    size_t pos = 0;
    const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
    CHECK_FALSE(ok);
    CHECK(decoded.type().type() == logical_type::NA);
}

// The bound must be spelled `pos > size || size - pos < sizeof(T)`, not `pos + sizeof(T) > size`:
// the addition is a size_t and wraps. A `pos` already past the end (exactly what a caller holds
// after ignoring one refusal) would then answer "in range" and memcpy from `in.data() + pos`.
TEST_CASE("logical_value_binary_codec: read_le cannot be walked past the end by an overflowing bound") {
    using components::index::codec::read_le;

    auto resource = core::pmr::otterbrix_resource();
    auto buffer = bytes(&resource, {1, 2, 3, 4, 5, 6, 7, 8});

    size_t pos = std::numeric_limits<size_t>::max() - 3;
    REQUIRE(pos + sizeof(uint64_t) < buffer.size());
    bool ok = true;
    const auto v = read_le<uint64_t>(buffer, pos, &ok);
    CHECK_FALSE(ok);
    CHECK(v == 0);
    CHECK(pos == std::numeric_limits<size_t>::max() - 3);

    size_t tail = 4;
    bool tail_ok = true;
    CHECK(read_le<uint64_t>(buffer, tail, &tail_ok) == 0);
    CHECK_FALSE(tail_ok);
    CHECK(tail == 4);
}

// encode_disk_hash_key runs on the path that opens a database: bitcask_index_disk_t's rebuild loop hands it
// a value decoded off disk (services/index/bitcask_index_disk.cpp), so an unhashable key type must be
// reported through `ok`, not aborted -- an abort here would make the database unopenable.
TEST_CASE("logical_value_binary_codec: an unhashable key type is reported, not aborted") {
    using components::index::codec::encode_disk_hash_key;
    using components::types::int128_t;
    using components::types::logical_value_t;

    auto resource = core::pmr::otterbrix_resource();

    // HUGEINT is physical INT128, which this encoder has no arm for.
    logical_value_t hugeint(&resource, int128_t{7});
    REQUIRE(hugeint.type().to_physical_type() == components::types::physical_type::INT128);

    bool ok = true;
    const auto encoded = encode_disk_hash_key(hugeint, &ok);
    CHECK_FALSE(ok);
    // The tag byte and nothing else: not a usable hash key, which is why `ok` has to be read.
    CHECK(encoded.size() == 1);

    bool good_ok = true;
    const auto good = encode_disk_hash_key(logical_value_t(&resource, int64_t{7}), &good_ok);
    CHECK(good_ok);
    CHECK(good.size() == 1 + sizeof(int64_t));
}

// The same arm on the OTHER encoder of this file. append_logical_value is reached with a
// disk-decoded value through bitcask's merge relocation (serialize_payload over the key
// read_rows_at just handed back), so it must refuse for the same reason.
TEST_CASE("logical_value_binary_codec: append_logical_value reports an unencodable key type") {
    using components::index::codec::append_logical_value;
    using components::types::int128_t;
    using components::types::logical_value_t;

    auto resource = core::pmr::otterbrix_resource();
    std::pmr::string out(&resource);
    bool ok = true;
    append_logical_value(out, logical_value_t(&resource, int128_t{7}), &ok);
    CHECK_FALSE(ok);
    CHECK(out.size() == 1);
}

// Checks, not just asserts, the codec header's claim that the twelve remaining `assert(false)` guards can
// never be reached by any tag byte, across all three decode entry points.
TEST_CASE("logical_value_binary_codec: no tag byte steers a decode assert") {
    using components::index::codec::read_logical_value;
    using components::index::codec::read_logical_value_as_view;
    using components::index::codec::skip_logical_value;

    auto resource = core::pmr::otterbrix_resource();

    for (int raw = 0; raw < 256; ++raw) {
        INFO("tag byte = " << raw);
        // Generous payload: wider than any fixed width this codec reads, so a tag that IS
        // representable decodes rather than merely running short -- a short read would exit
        // through the length check and never reach the arm under test.
        std::pmr::string buffer(&resource);
        buffer.push_back(static_cast<char>(static_cast<unsigned char>(raw)));
        for (int i = 0; i < 32; ++i) {
            buffer.push_back(char{1});
        }

        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK(pos <= buffer.size());
        if (!ok) {
            CHECK(decoded.type().type() == components::types::logical_type::NA);
        }

        pos = 0;
        ok = true;
        const auto view = read_logical_value_as_view(buffer.data(), buffer.size(), pos, &ok);
        CHECK(pos <= buffer.size());
        (void) view;

        pos = 0;
        ok = true;
        skip_logical_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK(pos <= buffer.size());
    }
}
