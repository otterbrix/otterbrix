#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/index/logical_value_binary_codec.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/pmr.hpp>

#include <cstring>
#include <optional>
#include <string>

// pg_attribute.attdefspec round-trip.
//
// The flat-text form this replaced ("type_name:value") lost data in four ways, all of
// them silently, and all of them by writing "" — which the decoder read back as "this
// column has no default":
//   1. composite types were not persisted AT ALL (scalar_type_to_name returned "" and
//      the caller forwarded that as success), so a DEFAULT on ARRAY / LIST / STRUCT
//      simply disappeared;
//   2. nor were most scalars: the encoder's switch covered BOOLEAN, the integers,
//      FLOAT/DOUBLE and STRING_LITERAL, so DATE, TIME, TIMESTAMP and DECIMAL fell into
//      `default: return ""` the same way;
//   3. an explicit `DEFAULT NULL` was indistinguishable from having no default (both
//      decoded to nullopt) even though the checks used the distinction;
//   4. floats were rendered by std::to_string — six digits — so a DOUBLE default was
//      rounded on its way to disk.
// Each case below is one of those.

using namespace components::catalog;
using namespace components::types;

namespace {
    // The ONE arena this file builds DECIMALs on. create_decimal allocates only on its refusal
    // path, and that message belongs to the caller, so the caller has to name an arena it owns
    // rather than reach for the process-global one (rule 14).
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    // create_decimal reports an out-of-window (width, scale) through core::error_t now,
    // instead of an assert that vanished under NDEBUG. Every literal these tests use is
    // inside the window, so the helper checks the result and hands back the type.
    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = components::types::complex_logical_type::create_decimal(decimal_resource(), width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

namespace {
    // The same arena the DECIMAL helper above names, for the same reason (rule 14): this file
    // has one answer to "where does this live", not two.
    auto* g_resource = decimal_resource();

    // The persisted path, both directions: encode as CREATE TABLE would, decode as the
    // plan does, with the column type (which lives next door in atttypspec).
    std::optional<logical_value_t> round_trip(const logical_value_t& v, const complex_logical_type& column_type) {
        std::string spec;
        REQUIRE_FALSE(encode_default_spec(g_resource, v, spec).contains_error());
        REQUIRE_FALSE(spec.empty()); // "" means NO DEFAULT and must never encode a value
        std::optional<logical_value_t> out;
        REQUIRE_FALSE(decode_default_spec(g_resource, column_type, spec, out).contains_error());
        return out;
    }

    std::optional<logical_value_t> round_trip(const logical_value_t& v) { return round_trip(v, v.type()); }

    // Bit-for-bit identity. `==` on floating point is what the old encoder's six-digit
    // rendering was allowed to fail, so the comparison here is on the representation.
    template<typename T>
    bool same_bits(T a, T b) {
        return std::memcmp(&a, &b, sizeof(T)) == 0;
    }
} // namespace

TEST_CASE("catalog::default_spec::scalar_round_trip") {
    // Every scalar the engine can hold as a key or a value. The flat-text form covered
    // only the first eleven of these.
    CHECK(round_trip(logical_value_t(g_resource, true)).value() == logical_value_t(g_resource, true));
    CHECK(round_trip(logical_value_t(g_resource, std::int8_t{-7})).value() ==
          logical_value_t(g_resource, std::int8_t{-7}));
    CHECK(round_trip(logical_value_t(g_resource, std::uint8_t{200})).value() ==
          logical_value_t(g_resource, std::uint8_t{200}));
    CHECK(round_trip(logical_value_t(g_resource, std::int16_t{-30000})).value() ==
          logical_value_t(g_resource, std::int16_t{-30000}));
    CHECK(round_trip(logical_value_t(g_resource, std::uint16_t{60000})).value() ==
          logical_value_t(g_resource, std::uint16_t{60000}));
    CHECK(round_trip(logical_value_t(g_resource, std::int32_t{-123456})).value() ==
          logical_value_t(g_resource, std::int32_t{-123456}));
    CHECK(round_trip(logical_value_t(g_resource, std::uint32_t{4000000000u})).value() ==
          logical_value_t(g_resource, std::uint32_t{4000000000u}));
    CHECK(round_trip(logical_value_t(g_resource, std::int64_t{-9007199254740993LL})).value() ==
          logical_value_t(g_resource, std::int64_t{-9007199254740993LL}));
    CHECK(round_trip(logical_value_t(g_resource, std::uint64_t{18446744073709551615ULL})).value() ==
          logical_value_t(g_resource, std::uint64_t{18446744073709551615ULL}));
    CHECK(round_trip(logical_value_t(g_resource, std::string{"untagged"})).value() ==
          logical_value_t(g_resource, std::string{"untagged"}));
    // A string with an embedded NUL and a quote: the payload is hex-armoured, so no
    // byte in it can be mistaken for structure by anything that carries the column.
    CHECK(round_trip(logical_value_t(g_resource, std::string("a\0b'c:d", 7))).value() ==
          logical_value_t(g_resource, std::string("a\0b'c:d", 7)));
}

TEST_CASE("catalog::default_spec::temporal_and_decimal_round_trip") {
    // Loss (2): these all encoded to "" and were read back as "no default".
    CHECK(round_trip(logical_value_t(g_resource, core::date::date_t{core::date::days{19000}})).value() ==
          logical_value_t(g_resource, core::date::date_t{core::date::days{19000}}));
    CHECK(round_trip(logical_value_t(g_resource, core::date::time_t{core::date::microseconds{86399123456LL}}))
              .value() == logical_value_t(g_resource, core::date::time_t{core::date::microseconds{86399123456LL}}));
    CHECK(round_trip(logical_value_t(g_resource, core::date::timestamp_t{core::date::microseconds{1700000000000000LL}}))
              .value() ==
          logical_value_t(g_resource, core::date::timestamp_t{core::date::microseconds{1700000000000000LL}}));
    CHECK(
        round_trip(logical_value_t(g_resource, core::date::timestamptz_t{core::date::microseconds{1700000000000000LL}}))
            .value() ==
        logical_value_t(g_resource, core::date::timestamptz_t{core::date::microseconds{1700000000000000LL}}));

    // DECIMAL keeps width and scale: they come from the column type, which the decoder
    // is handed rather than having to find in the payload.
    const auto dec_type = make_decimal(10, 2);
    const auto dec = logical_value_t::create_decimal(g_resource, dec_type, std::int64_t{1234});
    auto dec_back = round_trip(dec, dec_type);
    REQUIRE(dec_back.has_value());
    CHECK(dec_back->type().type() == logical_type::DECIMAL);
    CHECK(dec_back->value<std::int64_t>() == 1234);

    // A 38-digit DECIMAL is INT128-backed; the scaled integer survives whole.
    const auto wide_type = make_decimal(38, 4);
    const auto wide =
        logical_value_t::create_decimal(g_resource,
                                        wide_type,
                                        static_cast<int128_t>(std::int64_t{-9007199254740993LL}) * 1000000);
    auto wide_back = round_trip(wide, wide_type);
    REQUIRE(wide_back.has_value());
    CHECK(wide_back->value<int128_t>() ==
          static_cast<int128_t>(std::int64_t{-9007199254740993LL}) * 1000000);
}

TEST_CASE("catalog::default_spec::floats_keep_every_bit") {
    // Loss (4): std::to_string prints six digits, so a DOUBLE default came back rounded.
    // The comparison is on the BITS, not on a printed form.
    const double third = 1.0 / 3.0;
    auto d_back = round_trip(logical_value_t(g_resource, third));
    REQUIRE(d_back.has_value());
    CHECK(same_bits(d_back->value<double>(), third));

    const double tenth = 0.1;
    CHECK(same_bits(round_trip(logical_value_t(g_resource, tenth))->value<double>(), tenth));

    const double huge = 1.7976931348623157e308;
    CHECK(same_bits(round_trip(logical_value_t(g_resource, huge))->value<double>(), huge));

    const float f_third = 1.0f / 3.0f;
    CHECK(same_bits(round_trip(logical_value_t(g_resource, f_third))->value<float>(), f_third));
}

TEST_CASE("catalog::default_spec::composite_round_trip") {
    // Loss (1): a DEFAULT on a composite type was not persisted at all.
    const auto elem = complex_logical_type{logical_type::INTEGER};

    const auto array_type = complex_logical_type::create_array(elem, 3);
    std::vector<logical_value_t> elems{logical_value_t(g_resource, std::int32_t{1}),
                                       logical_value_t(g_resource, std::int32_t{2}),
                                       logical_value_t(g_resource, std::int32_t{3})};
    auto array_back = round_trip(logical_value_t::create_array(g_resource, elem, elems), array_type);
    REQUIRE(array_back.has_value());
    REQUIRE(array_back->children().size() == 3);
    CHECK(array_back->children()[0].value<std::int32_t>() == 1);
    CHECK(array_back->children()[2].value<std::int32_t>() == 3);

    // A NULL ELEMENT inside a composite is a distinct state from a NULL composite, and
    // both must come back as themselves.
    std::vector<logical_value_t> holed{logical_value_t(g_resource, std::int32_t{1}),
                                       logical_value_t(g_resource, complex_logical_type{logical_type::NA}),
                                       logical_value_t(g_resource, std::int32_t{3})};
    auto holed_back = round_trip(logical_value_t::create_array(g_resource, elem, holed), array_type);
    REQUIRE(holed_back.has_value());
    REQUIRE(holed_back->children().size() == 3);
    CHECK(holed_back->children()[0].value<std::int32_t>() == 1);
    CHECK(holed_back->children()[1].is_null());
    CHECK(holed_back->children()[2].value<std::int32_t>() == 3);

    const auto list_type = complex_logical_type::create_list(elem);
    std::vector<logical_value_t> list_elems{logical_value_t(g_resource, std::int32_t{9}),
                                            logical_value_t(g_resource, std::int32_t{8})};
    auto list_back = round_trip(logical_value_t::create_list(g_resource, elem, list_elems), list_type);
    REQUIRE(list_back.has_value());
    REQUIRE(list_back->children().size() == 2);
    CHECK(list_back->children()[0].value<std::int32_t>() == 9);

    std::pmr::vector<complex_logical_type> fields{g_resource};
    fields.emplace_back(complex_logical_type{logical_type::BIGINT, "a"});
    fields.emplace_back(complex_logical_type{logical_type::STRING_LITERAL, "b"});
    const auto struct_type = complex_logical_type::create_struct("s", fields);
    std::vector<logical_value_t> field_values{logical_value_t(g_resource, std::int64_t{42}),
                                              logical_value_t(g_resource, std::string{"x"})};
    auto struct_back = round_trip(logical_value_t::create_struct(g_resource, struct_type, field_values), struct_type);
    REQUIRE(struct_back.has_value());
    REQUIRE(struct_back->children().size() == 2);
    CHECK(struct_back->children()[0].value<std::int64_t>() == 42);
    CHECK(struct_back->children()[1].value<std::string_view>() == "x");
}

TEST_CASE("catalog::default_spec::explicit_null_default_is_not_absence") {
    // Loss (3). These are three DIFFERENT facts and the encoding must keep them apart:
    // no default; an explicit DEFAULT NULL; a value.
    const complex_logical_type column_type{logical_type::BIGINT};

    std::optional<logical_value_t> absent;
    REQUIRE_FALSE(decode_default_spec(g_resource, column_type, "", absent).contains_error());
    CHECK_FALSE(absent.has_value()); // "" is the ONLY spelling of "no default"

    // A NULL value is NA-typed here (logical_value_t::is_null() IS type()==NA); that is
    // how the parser hands `DEFAULT NULL` down.
    std::string null_spec;
    REQUIRE_FALSE(encode_default_spec(g_resource,
                                      logical_value_t(g_resource, complex_logical_type{logical_type::NA}),
                                      null_spec)
                      .contains_error());
    CHECK_FALSE(null_spec.empty());
    std::optional<logical_value_t> null_default;
    REQUIRE_FALSE(decode_default_spec(g_resource, column_type, null_spec, null_default).contains_error());
    REQUIRE(null_default.has_value()); // a default IS present...
    CHECK(null_default->is_null());    // ...and its value is NULL

    std::optional<logical_value_t> value_default;
    std::string value_spec;
    REQUIRE_FALSE(
        encode_default_spec(g_resource, logical_value_t(g_resource, std::int64_t{5}), value_spec).contains_error());
    REQUIRE_FALSE(decode_default_spec(g_resource, column_type, value_spec, value_default).contains_error());
    REQUIRE(value_default.has_value());
    CHECK_FALSE(value_default->is_null());
    CHECK(value_default->value<std::int64_t>() == 5);

    CHECK(null_spec != value_spec);
}

TEST_CASE("catalog::default_spec::unencodable_default_is_an_error") {
    // Rule 6. A type the value codec cannot carry must FAIL at CREATE TABLE / ALTER SET
    // DEFAULT. The old encoder answered "" here, which every reader downstream took to
    // mean the column had no default — a loss nobody was told about.
    const logical_value_t hugeint(g_resource, static_cast<int128_t>(1) << 100);
    std::string spec;
    auto ec = encode_default_spec(g_resource, hugeint, spec);
    CHECK(ec.contains_error());
    CHECK(spec.empty());

    // An unrepresentable LEAF makes the whole composite unrepresentable — a partially
    // encoded default is not an option.
    const auto blob_array = complex_logical_type::create_array(complex_logical_type{logical_type::BLOB}, 2);
    CHECK_FALSE(components::index::codec::is_encodable_value_type(blob_array));
}

TEST_CASE("catalog::default_spec::corrupt_spec_is_reported_not_ignored") {
    // A non-empty spec that does not decode is CATALOG CORRUPTION. Reading it as "no
    // default" is exactly the silent path this encoding exists to close: the constraint
    // layer would clear a row on the strength of a default the write path then failed
    // to apply.
    const complex_logical_type column_type{logical_type::BIGINT};
    std::optional<logical_value_t> out;

    CHECK(decode_default_spec(g_resource, column_type, "int8:5", out).contains_error()); // the OLD format
    CHECK_FALSE(out.has_value());
    CHECK(decode_default_spec(g_resource, column_type, "VZZ", out).contains_error()); // not hex
    CHECK(decode_default_spec(g_resource, column_type, "V01", out).contains_error()); // truncated payload
    CHECK(decode_default_spec(g_resource, column_type, "N!", out).contains_error());  // NULL marker with a tail

    // A payload encoded for a DIFFERENT type does not silently reinterpret: an INTEGER
    // default read against a BIGINT column is short by four bytes.
    std::string int_spec;
    REQUIRE_FALSE(
        encode_default_spec(g_resource, logical_value_t(g_resource, std::int32_t{5}), int_spec).contains_error());
    CHECK(decode_default_spec(g_resource, column_type, int_spec, out).contains_error());
}
