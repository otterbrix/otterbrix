#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/index/binary_codec.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>
#include <services/index/index_types.hpp>

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

    // The bytes a lookup, an insert and a staged probe all meet on.
    std::pmr::string encode(std::pmr::memory_resource* resource, const components::vector::vector_t& key) {
        std::pmr::string out(resource);
        const auto error = services::index::for_each_key_bytes(key, 1, &out, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE_FALSE(error.contains_error());
        return out;
    }

    std::vector<components::vector::vector_t> supported_keys(std::pmr::memory_resource* resource) {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        namespace vec = components::vector;
        std::vector<vec::vector_t> keys;
        keys.push_back(vec::make_scalar(resource, true));
        keys.push_back(vec::make_scalar(resource, int8_t{-7}));
        keys.push_back(vec::make_scalar(resource, uint8_t{7}));
        keys.push_back(vec::make_scalar(resource, int16_t{-1234}));
        keys.push_back(vec::make_scalar(resource, uint16_t{1234}));
        keys.push_back(vec::make_scalar(resource, int32_t{-123456}));
        keys.push_back(vec::make_scalar(resource, uint32_t{123456}));
        keys.push_back(vec::make_scalar(resource, int64_t{-9876543210LL}));
        keys.push_back(vec::make_scalar(resource, uint64_t{9876543210ULL}));
        keys.push_back(vec::make_scalar(resource, 1.25f));
        keys.push_back(vec::make_scalar(resource, 3.5));
        keys.push_back(vec::make_scalar(resource, std::string_view("hello-codec")));
        keys.push_back(vec::make_scalar(resource, complex_logical_type{logical_type::DATE}, int32_t{42}));
        keys.push_back(vec::make_scalar(resource, complex_logical_type{logical_type::TIME}, int64_t{123456789}));
        keys.push_back(vec::make_scalar(resource, complex_logical_type{logical_type::TIMESTAMP}, int64_t{7777777}));
        keys.push_back(vec::make_scalar(resource, complex_logical_type{logical_type::TIMESTAMP_TZ}, int64_t{-5555555}));
        keys.push_back(vec::make_decimal(resource, make_decimal(18, 2), 123456789));
        keys.push_back(
            vec::make_decimal(resource, make_decimal(38, 8), components::types::int128_t{1234567890123456789LL}));
        return keys;
    }
} // namespace

TEST_CASE("binary_codec: roundtrip_supported_types") {
    auto resource = core::pmr::otterbrix_resource();

    for (const auto& input : supported_keys(&resource)) {
        const auto encoded = encode(&resource, input);

        const auto decoded = services::index::decode_key_bytes(&resource, encoded);

        REQUIRE_FALSE(decoded.has_error());
        REQUIRE(decoded.value().type() == input.type());
        REQUIRE(encode(&resource, decoded.value()) == encoded);
    }

    SECTION("a NULL key of any type is the single NA byte and decodes to a NULL") {
        const auto encoded = encode(&resource,
                                    components::vector::make_null(&resource,
                                                                  components::types::complex_logical_type{
                                                                      components::types::logical_type::BIGINT}));
        REQUIRE(encoded == std::pmr::string(1, '\0', &resource));
        const auto decoded = services::index::decode_key_bytes(&resource, encoded);
        REQUIRE_FALSE(decoded.has_error());
        REQUIRE(decoded.value().type().type() == components::types::logical_type::NA);
        REQUIRE(decoded.value().is_null(0));
    }
}

TEST_CASE("binary_codec: read_tagged_value") {
    using components::index::codec::read_tagged_value;
    using components::types::physical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("bool") {
        const auto encoded = encode(&resource, components::vector::make_scalar(&resource, true));
        size_t pos = 0;
        auto pv = read_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::BOOL);
        REQUIRE(pv.value<physical_type::BOOL>() == true);
        REQUIRE(pos == encoded.size());
    }

    SECTION("int32") {
        const auto encoded = encode(&resource, components::vector::make_scalar(&resource, int32_t{-123456}));
        size_t pos = 0;
        auto pv = read_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::INT32);
        REQUIRE(pv.value<physical_type::INT32>() == -123456);
        REQUIRE(pos == encoded.size());
    }

    SECTION("uint64") {
        const auto encoded = encode(&resource, components::vector::make_scalar(&resource, uint64_t{9876543210ULL}));
        size_t pos = 0;
        auto pv = read_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::UINT64);
        REQUIRE(pv.value<physical_type::UINT64>() == 9876543210ULL);
        REQUIRE(pos == encoded.size());
    }

    SECTION("double") {
        const auto encoded = encode(&resource, components::vector::make_scalar(&resource, 3.5));
        size_t pos = 0;
        auto pv = read_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::DOUBLE);
        REQUIRE(pv.value<physical_type::DOUBLE>() == Catch::Approx(3.5));
        REQUIRE(pos == encoded.size());
    }

    SECTION("string zero-copy") {
        const auto encoded =
            encode(&resource, components::vector::make_scalar(&resource, std::string_view("hello-codec")));
        size_t pos = 0;
        auto pv = read_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::STRING);
        auto sv = pv.value<physical_type::STRING>();
        REQUIRE(sv == "hello-codec");
        REQUIRE(sv.data() >= encoded.data());
        REQUIRE(sv.data() < encoded.data() + encoded.size());
        REQUIRE(pos == encoded.size());
    }

    SECTION("na") {
        const auto encoded = encode(&resource,
                                    components::vector::make_null(&resource,
                                                                  components::types::complex_logical_type{
                                                                      components::types::logical_type::BIGINT}));
        size_t pos = 0;
        auto pv = read_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pv.type() == physical_type::NA);
        REQUIRE(pos == encoded.size());
    }
}

TEST_CASE("binary_codec: skip_tagged_value") {
    using components::index::codec::skip_tagged_value;

    auto resource = core::pmr::otterbrix_resource();

    auto keys = supported_keys(&resource);
    keys.push_back(components::vector::make_null(
        &resource,
        components::types::complex_logical_type{components::types::logical_type::BIGINT}));
    for (const auto& input : keys) {
        const auto encoded = encode(&resource, input);

        size_t pos = 0;
        skip_tagged_value(encoded.data(), encoded.size(), pos);
        REQUIRE(pos == encoded.size());
    }
}

// Corrupt bytes must not kill the process: every buffer below is a key payload (b+tree leaf or bitcask
// segment) with no checksum, so the contract is to refuse through the error channel, never abort.
namespace {
    // A stored key payload, byte for byte, as for_each_key_bytes writes it.
    std::pmr::string bytes(std::pmr::memory_resource* resource, std::initializer_list<int> raw) {
        std::pmr::string out(resource);
        for (int b : raw) {
            out.push_back(static_cast<char>(static_cast<unsigned char>(b)));
        }
        return out;
    }

    constexpr int tag(components::types::logical_type t) { return static_cast<int>(t); }
} // namespace

TEST_CASE("binary_codec: a corrupt logical tag refuses instead of aborting") {
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("one flipped bit turns a BIGINT key into an unsupported physical width") {
        // HUGEINT is physical INT128, an arm the key codec has no reader for.
        REQUIRE((tag(logical_type::BIGINT) ^ 1) == tag(logical_type::HUGEINT));
        auto buffer = bytes(&resource, {tag(logical_type::HUGEINT), 1, 0, 0, 0, 0, 0, 0, 0});
        const auto decoded = services::index::decode_key_bytes(&resource, buffer);
        CHECK(decoded.has_error());
    }

    SECTION("a tag byte no logical type uses at all") {
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        const auto decoded = services::index::decode_key_bytes(&resource, buffer);
        CHECK(decoded.has_error());
    }

    SECTION("ENUM shares physical INT32 with INTEGER and DATE but has no arm") {
        auto buffer = bytes(&resource, {tag(logical_type::ENUM), 7, 0, 0, 0});
        const auto decoded = services::index::decode_key_bytes(&resource, buffer);
        CHECK(decoded.has_error());
    }

    SECTION("a STRING length that runs past the end of the record") {
        // [tag][uint32 length][bytes]; the length is four stored bytes, so a flipped high
        // bit claims gigabytes of a five-byte record.
        auto buffer = bytes(&resource, {tag(logical_type::STRING_LITERAL), 0xFF, 0xFF, 0xFF, 0xFF, 'a'});
        const auto decoded = services::index::decode_key_bytes(&resource, buffer);
        CHECK(decoded.has_error());
    }

    SECTION("a DECIMAL width outside the representable window") {
        // 50 is past DECIMAL_MAX_WIDTH.
        REQUIRE((18 ^ 32) == 50);
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 50, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        const auto decoded = services::index::decode_key_bytes(&resource, buffer);
        REQUIRE(decoded.has_error());
        CHECK(decoded.error().type == core::error_code_t::data_corruption);
    }
}

TEST_CASE("binary_codec: a corrupt logical tag refuses in the view decoder") {
    using components::index::codec::read_tagged_value;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("a tag byte no logical type uses at all") {
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_tagged_value(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }

    SECTION("a DECIMAL tag, which physical_value cannot carry") {
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 18, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_tagged_value(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }

    SECTION("a STRING length that runs past the end of the record") {
        auto buffer = bytes(&resource, {tag(logical_type::STRING_LITERAL), 0xFF, 0xFF, 0xFF, 0xFF, 'a'});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_tagged_value(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }

    SECTION("a record truncated inside the payload") {
        // Two bytes where an INT64 payload should be: with only an assert guarding it, the memcpy runs past
        // the end of the record under NDEBUG.
        auto buffer = bytes(&resource, {tag(logical_type::BIGINT), 1, 0});
        size_t pos = 0;
        bool ok = true;
        const auto pv = read_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pv.type() == components::types::physical_type::NA);
        size_t unflagged_pos = 0;
        CHECK(read_tagged_value(buffer.data(), buffer.size(), unflagged_pos).type() ==
              components::types::physical_type::NA);
    }
}

TEST_CASE("binary_codec: a corrupt logical tag refuses in the skipper") {
    using components::index::codec::skip_tagged_value;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    SECTION("a tag byte no logical type uses at all") {
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        skip_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pos <= buffer.size());
    }

    SECTION("a DECIMAL width outside the representable window") {
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 50, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        skip_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        CHECK(pos <= buffer.size());
    }

    SECTION("a STRING length that runs past the end of the record") {
        auto buffer = bytes(&resource, {tag(logical_type::STRING_LITERAL), 0xFF, 0xFF, 0xFF, 0xFF, 'a'});
        size_t pos = 0;
        bool ok = true;
        skip_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK_FALSE(ok);
        // The skipped position is what services::index::id_of then reads the row id from. Left past the
        // end of the record it turns the next read into an out-of-bounds one.
        CHECK(pos <= buffer.size());
    }
}

TEST_CASE("binary_codec: read_le_raw refuses a short record") {
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
TEST_CASE("binary_codec: a well-formed record leaves ok alone") {
    using components::index::codec::append_le;
    using components::index::codec::read_le_raw;
    using components::index::codec::read_tagged_value;
    using components::index::codec::skip_tagged_value;

    auto resource = core::pmr::otterbrix_resource();

    auto encoded = encode(&resource, components::vector::make_scalar(&resource, std::string_view("a-real-key")));
    append_le<int64_t>(encoded, int64_t{4242});

    bool ok = true;
    const auto decoded =
        services::index::decode_key_bytes(&resource,
                                          std::string_view(encoded).substr(0, encoded.size() - sizeof(int64_t)));
    REQUIRE_FALSE(decoded.has_error());
    CHECK(encode(&resource, decoded.value()) == encoded.substr(0, encoded.size() - sizeof(int64_t)));

    size_t pos = 0;
    const auto pv = read_tagged_value(encoded.data(), encoded.size(), pos, &ok);
    CHECK(ok);
    CHECK(pv.type() == components::types::physical_type::STRING);

    // Exactly the services::index::id_of shape: skip the key, then read the row id behind it.
    pos = 0;
    skip_tagged_value(encoded.data(), encoded.size(), pos, &ok);
    CHECK(ok);
    CHECK(read_le_raw<uint64_t>(encoded.data(), encoded.size(), pos, &ok) == 4242);
    CHECK(ok);
    CHECK(pos == encoded.size());
}

// A truncated record must not decode to a plausible zero in silence: read_le answers T{} without moving
// `pos`, so an unflagged short read turns a key clipped by a short write into the value 0 in the index.
TEST_CASE("binary_codec: a truncated payload is a refusal, not a zero") {
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();
    auto buffer = bytes(&resource, {tag(logical_type::BIGINT), 1, 2, 3});

    CHECK(services::index::decode_key_bytes(&resource, buffer).has_error());
}

// The bound must be spelled `pos > size || size - pos < sizeof(T)`, not `pos + sizeof(T) > size`:
// the addition is a size_t and wraps. A `pos` already past the end (exactly what a caller holds
// after ignoring one refusal) would then answer "in range" and memcpy from `in.data() + pos`.
TEST_CASE("binary_codec: read_le cannot be walked past the end by an overflowing bound") {
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

// Checks, not just asserts, the codec header's claim that the remaining `assert(false)` guards can
// never be reached by any tag byte, across all three decode entry points.
TEST_CASE("binary_codec: no tag byte steers a decode assert") {
    using components::index::codec::read_tagged_value;
    using components::index::codec::skip_tagged_value;

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

        // Reaching the end of this call is the check: a decode assert or abort would end the process.
        const auto decoded = services::index::decode_key_bytes(&resource, buffer);
        if (!decoded.has_error()) {
            CHECK(encode(&resource, decoded.value()) == buffer);
        }

        size_t pos = 0;
        bool ok = true;
        const auto view = read_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK(pos <= buffer.size());
        (void) view;

        pos = 0;
        ok = true;
        skip_tagged_value(buffer.data(), buffer.size(), pos, &ok);
        CHECK(pos <= buffer.size());
    }
}
