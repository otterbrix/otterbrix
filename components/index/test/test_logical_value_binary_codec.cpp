#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/index/logical_value_binary_codec.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>

#include <limits>

namespace {
    // The ONE arena this file builds DECIMALs on. create_decimal allocates only on its refusal
    // path, and that message belongs to the caller, so the caller has to name an arena it owns
    // rather than reach for the process-global one (rule 14).
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    // create_decimal reports an out-of-window (width, scale) through core::error_t. Every
    // literal these tests use is inside the window, so the helper checks the result and
    // hands back the type.
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

// -----------------------------------------------------------------------------
// CORRUPT STORED BYTES MUST NOT KILL THE PROCESS.
//
// Every buffer below is what a b+tree leaf or a bitcask segment hands the decoder: a stored key payload.
// Neither file carries a checksum over that payload, so a single flipped bit reaches these functions
// verbatim. An assert(false) + std::abort() on such a case takes the test binary down in a Debug build and
// the HOST PROCESS of the embedded engine down in a Release build, leaving the database unopenable rather
// than merely wrong. The contract pinned here is the opposite one: a corrupt payload decodes to NA (or a
// default physical_value), the caller is told through `ok`, and the process stays up so the query can fail
// and DROP INDEX can still remove the object.
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
        // BIGINT is 14; flipping bit 0 gives 15 = HUGEINT, whose physical type is INT128 --
        // an arm the key codec has no reader for. This is the cheapest possible corruption:
        // ONE BIT in the tag byte of an ordinary integer key.
        REQUIRE((tag(logical_type::BIGINT) ^ 1) == tag(logical_type::HUGEINT));
        auto buffer = bytes(&resource, {tag(logical_type::HUGEINT), 1, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        // The same call WITHOUT a flag is what the read path makes today, and it must still
        // come back rather than take the process with it.
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }

    SECTION("a tag byte no logical type uses at all") {
        // The tag byte is dense over roughly 30 of its 256 values, so most corruptions land
        // on a value that maps to physical_type::INVALID.
        auto buffer = bytes(&resource, {200, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        // The same call WITHOUT a flag is what the read path makes today, and it must still
        // come back rather than take the process with it.
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
        // The same call WITHOUT a flag is what the read path makes today, and it must still
        // come back rather than take the process with it.
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
        // The same call WITHOUT a flag is what the read path makes today, and it must still
        // come back rather than take the process with it.
        size_t unflagged_pos = 0;
        CHECK(read_logical_value(&resource, buffer, unflagged_pos).type().type() == logical_type::NA);
    }

    SECTION("a DECIMAL width outside the representable window") {
        // width 18 is 0b010010; flipping bit 5 gives 50, past DECIMAL_MAX_WIDTH.
        REQUIRE((18 ^ 32) == 50);
        auto buffer = bytes(&resource, {tag(logical_type::DECIMAL), 50, 2, 0, 0, 0, 0, 0, 0, 0, 0});
        size_t pos = 0;
        bool ok = true;
        const auto decoded = read_logical_value(&resource, buffer, pos, &ok);
        CHECK_FALSE(ok);
        CHECK(decoded.type().type() == logical_type::NA);
        // The same call WITHOUT a flag is what the read path makes today, and it must still
        // come back rather than take the process with it.
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
        // Two bytes where an INT64 payload should be. With only an assert guarding it the
        // memcpy runs PAST THE END of the record under NDEBUG.
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
    // Eight bytes asked of a two-byte record. Guarded only by an assert, the build users ship
    // reads six bytes PAST THE END of the buffer.
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

// A truncated record must not decode to a plausible ZERO in silence: read_le answers T{}
// without moving `pos`, so an unflagged short read turns a key clipped by a short write into
// the value 0 in the index. Same class of corruption as a bad tag, same answer.
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

// BOTH PRIMITIVES MUST SPELL THE BOUND AS `pos > size || size - pos < sizeof(T)`.
// The obvious form, `pos + sizeof(T) > in.size()`, is a size_t addition and WRAPS: a `pos` already past the
// end -- which is exactly what a caller holds after ignoring one refusal, and read_typed_value's nested arms
// walk `pos` through several values before anyone looks -- then answers "in range" and lets the memcpy read
// from `in.data() + pos`. With the wrapping form, pos = SIZE_MAX-3 wraps to 4, 4 > 8 is false, and read_le
// memcpy's eight bytes from an address about 2^64 past the buffer.
TEST_CASE("logical_value_binary_codec: read_le cannot be walked past the end by an overflowing bound") {
    using components::index::codec::read_le;

    auto resource = core::pmr::otterbrix_resource();
    auto buffer = bytes(&resource, {1, 2, 3, 4, 5, 6, 7, 8});

    // SIZE_MAX - 3 + sizeof(uint64_t) wraps to 4, which is inside an 8-byte buffer.
    size_t pos = std::numeric_limits<size_t>::max() - 3;
    REQUIRE(pos + sizeof(uint64_t) < buffer.size()); // the wrap, spelled out
    bool ok = true;
    const auto v = read_le<uint64_t>(buffer, pos, &ok);
    CHECK_FALSE(ok);
    CHECK(v == 0);
    CHECK(pos == std::numeric_limits<size_t>::max() - 3);

    // The ordinary short read still refuses, and `pos` still does not move.
    size_t tail = 4;
    bool tail_ok = true;
    CHECK(read_le<uint64_t>(buffer, tail, &tail_ok) == 0);
    CHECK_FALSE(tail_ok);
    CHECK(tail == 4);
}

// THE ENCODER ON THE PATH THAT OPENS A DATABASE MUST NOT KILL THE PROCESS.
//
// An abort on encode_disk_hash_key's `default:` arm would be justified by "the encoder is handed a
// logical_value_t this process built and the CREATE INDEX gate vetted". That sentence is false for its
// actual caller: bitcask_index_disk_t's rebuild loop deserialize_payload()s a record OFF THE DISK and hands
// the result to key_bytes_for_hash -> normalize_hash_key -> here (services/index/bitcask_index_disk.cpp,
// and again on the merge relocation). A key type this build has no hash arm for would take the whole process
// down -- in release builds too, where the process is the HOST of an embedded engine -- and the database
// could not be opened at all. It reports through `ok` instead, and bitcask turns that into a refused open.
TEST_CASE("logical_value_binary_codec: an unhashable key type is reported, not aborted") {
    using components::index::codec::encode_disk_hash_key;
    using components::types::int128_t;
    using components::types::logical_value_t;

    auto resource = core::pmr::otterbrix_resource();

    // HUGEINT is physical INT128, which this encoder has no arm for. It is also the type ONE
    // FLIPPED BIT in an ordinary BIGINT tag names (14 -> 15), which is how a stored byte
    // steers a value into this arm.
    logical_value_t hugeint(&resource, int128_t{7});
    REQUIRE(hugeint.type().to_physical_type() == components::types::physical_type::INT128);

    bool ok = true;
    const auto encoded = encode_disk_hash_key(hugeint, &ok);
    CHECK_FALSE(ok);
    // The tag byte and nothing else: not a usable hash key, which is why `ok` has to be read.
    CHECK(encoded.size() == 1);

    // A representable key is unaffected, and `ok` is only ever set to false.
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

// THE CLAIM THE HEADER COMMENT MAKES, CHECKED RATHER THAN ASSERTED.
//
// Twelve `assert(false)` guards remain on the decode side -- eleven in read_logical_value, one in
// read_decimal_payload -- and an assert(false) IS an abort in a Debug build, which is the build this binary
// is. Their justification is that they guard a DERIVATION and not the input: `physical` comes from `logical`
// through to_physical_type(), and for each of those arms exactly one logical type maps to that width, so no
// value of the stored tag byte can reach one.
//
// This walks ALL 256 tag bytes through all three decode entry points. If any one of them steers into a
// guard, this test does not fail politely -- the process aborts, which is exactly the failure mode the
// guards are claimed not to have. It also pins the weaker half of the contract: whatever a tag byte does,
// the call RETURNS, and a `pos` it moved never leaves the buffer.
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
