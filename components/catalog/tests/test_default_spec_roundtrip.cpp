#include <catch2/catch_test_macros.hpp>
#include <components/catalog/default_cell_codec.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/pmr.hpp>

#include <cstring>
#include <optional>
#include <span>
#include <string>

// pg_attribute.attdefspec replaces a flat-text form ("type_name:value") that silently lost data as "" (read
// back as "no default"): composites were not persisted at all, most non-trivial scalars fell into the same
// "" case, DEFAULT NULL was indistinguishable from no default, and floats were rounded to six digits.

using namespace components::catalog;
using namespace components::types;
namespace vec = components::vector;

namespace {
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    components::types::complex_logical_type make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created =
            components::types::complex_logical_type::create_decimal(decimal_resource(), width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

namespace {
    auto* g_resource = decimal_resource();

    template<typename T>
    vec::vector_t scalar(T value) {
        return vec::make_scalar(g_resource, std::move(value));
    }

    // make_scalar picks the logical type of T; the temporal types name theirs.
    template<typename T>
    vec::vector_t typed(logical_type type, T value) {
        return vec::make_scalar(g_resource, complex_logical_type{type}, std::move(value));
    }

    // Encode as CREATE TABLE would, decode as the plan does (column type from atttypspec).
    std::optional<vec::vector_t> round_trip(const vec::vector_t& v, const complex_logical_type& column_type) {
        std::string spec;
        REQUIRE_FALSE(encode_default_spec(g_resource, v, spec).contains_error());
        REQUIRE_FALSE(spec.empty());
        std::optional<vec::vector_t> out;
        REQUIRE_FALSE(decode_default_spec(g_resource, column_type, spec, out).contains_error());
        return out;
    }

    std::optional<vec::vector_t> round_trip(const vec::vector_t& v) { return round_trip(v, v.type()); }

    // The decoded cell has the column type and holds the same value as what was encoded.
    bool same(const std::optional<vec::vector_t>& back, const vec::vector_t& original) {
        return back.has_value() && back->type() == original.type() && vec::cells_equal(*back, 0, original, 0);
    }

    // `==` on floating point would let a rounded value slip through, so the comparison is bit-for-bit.
    template<typename T>
    bool same_bits(T a, T b) {
        return std::memcmp(&a, &b, sizeof(T)) == 0;
    }
} // namespace

TEST_CASE("catalog::default_spec::scalar_round_trip") {
    CHECK(same(round_trip(scalar(true)), scalar(true)));
    CHECK(same(round_trip(scalar(std::int8_t{-7})), scalar(std::int8_t{-7})));
    CHECK(same(round_trip(scalar(std::uint8_t{200})), scalar(std::uint8_t{200})));
    CHECK(same(round_trip(scalar(std::int16_t{-30000})), scalar(std::int16_t{-30000})));
    CHECK(same(round_trip(scalar(std::uint16_t{60000})), scalar(std::uint16_t{60000})));
    CHECK(same(round_trip(scalar(std::int32_t{-123456})), scalar(std::int32_t{-123456})));
    CHECK(same(round_trip(scalar(std::uint32_t{4000000000u})), scalar(std::uint32_t{4000000000u})));
    CHECK(same(round_trip(scalar(std::int64_t{-9007199254740993LL})), scalar(std::int64_t{-9007199254740993LL})));
    CHECK(same(round_trip(scalar(std::uint64_t{18446744073709551615ULL})),
               scalar(std::uint64_t{18446744073709551615ULL})));
    CHECK(same(round_trip(scalar(std::string{"untagged"})), scalar(std::string{"untagged"})));
    // Embedded NUL and quote: the payload is hex-armoured, so no byte can be mistaken for structure.
    CHECK(same(round_trip(scalar(std::string("a\0b'c:d", 7))), scalar(std::string("a\0b'c:d", 7))));
}

TEST_CASE("catalog::default_spec::temporal_and_decimal_round_trip") {
    CHECK(same(round_trip(typed(logical_type::DATE, std::int32_t{19000})),
               typed(logical_type::DATE, std::int32_t{19000})));
    CHECK(same(round_trip(typed(logical_type::TIME, std::int64_t{86399123456LL})),
               typed(logical_type::TIME, std::int64_t{86399123456LL})));
    CHECK(same(round_trip(typed(logical_type::TIMESTAMP, std::int64_t{1700000000000000LL})),
               typed(logical_type::TIMESTAMP, std::int64_t{1700000000000000LL})));
    CHECK(same(round_trip(typed(logical_type::TIMESTAMP_TZ, std::int64_t{1700000000000000LL})),
               typed(logical_type::TIMESTAMP_TZ, std::int64_t{1700000000000000LL})));

    const auto dec_type = make_decimal(10, 2);
    const auto dec = vec::make_decimal(g_resource, dec_type, 1234);
    auto dec_back = round_trip(dec, dec_type);
    REQUIRE(dec_back.has_value());
    CHECK(dec_back->type().type() == logical_type::DECIMAL);
    CHECK(dec_back->get_value<std::int64_t>(0) == 1234);

    // A 38-digit DECIMAL is INT128-backed; the scaled integer survives whole.
    const auto wide_type = make_decimal(38, 4);
    const auto wide =
        vec::make_decimal(g_resource, wide_type, static_cast<int128_t>(std::int64_t{-9007199254740993LL}) * 1000000);
    auto wide_back = round_trip(wide, wide_type);
    REQUIRE(wide_back.has_value());
    CHECK(wide_back->get_value<int128_t>(0) == static_cast<int128_t>(std::int64_t{-9007199254740993LL}) * 1000000);
}

TEST_CASE("catalog::default_spec::floats_keep_every_bit") {
    const double third = 1.0 / 3.0;
    auto d_back = round_trip(scalar(third));
    REQUIRE(d_back.has_value());
    CHECK(same_bits(d_back->get_value<double>(0), third));

    const double tenth = 0.1;
    CHECK(same_bits(round_trip(scalar(tenth))->get_value<double>(0), tenth));

    const double huge = 1.7976931348623157e308;
    CHECK(same_bits(round_trip(scalar(huge))->get_value<double>(0), huge));

    const float f_third = 1.0f / 3.0f;
    CHECK(same_bits(round_trip(scalar(f_third))->get_value<float>(0), f_third));
}

TEST_CASE("catalog::default_spec::composite_round_trip") {
    const auto elem = complex_logical_type{logical_type::INTEGER};

    const auto array_type = complex_logical_type::create_array(elem, 3);
    const vec::vector_t elems[] = {scalar(std::int32_t{1}), scalar(std::int32_t{2}), scalar(std::int32_t{3})};
    auto array_back = round_trip(vec::make_array(g_resource, elem, elems), array_type);
    REQUIRE(array_back.has_value());
    CHECK(array_back->entry().get_value<std::int32_t>(0) == 1);
    CHECK(array_back->entry().get_value<std::int32_t>(2) == 3);

    // A NULL element inside a composite is a distinct state from a NULL composite.
    const vec::vector_t holed[] = {scalar(std::int32_t{1}), vec::make_null(g_resource, elem), scalar(std::int32_t{3})};
    auto holed_back = round_trip(vec::make_array(g_resource, elem, holed), array_type);
    REQUIRE(holed_back.has_value());
    CHECK_FALSE(holed_back->is_null(0));
    CHECK(holed_back->entry().get_value<std::int32_t>(0) == 1);
    CHECK(holed_back->entry().is_null(1));
    CHECK(holed_back->entry().get_value<std::int32_t>(2) == 3);

    const auto list_type = complex_logical_type::create_list(elem);
    const vec::vector_t list_elems[] = {scalar(std::int32_t{9}), scalar(std::int32_t{8})};
    auto list_back = round_trip(vec::make_list(g_resource, elem, list_elems), list_type);
    REQUIRE(list_back.has_value());
    REQUIRE(list_back->data<list_entry_t>()[0].length == 2);
    CHECK(list_back->entry().get_value<std::int32_t>(0) == 9);

    std::pmr::vector<complex_logical_type> fields{g_resource};
    fields.emplace_back(complex_logical_type{logical_type::BIGINT, "a"});
    fields.emplace_back(complex_logical_type{logical_type::TEXT, "b"});
    const auto struct_type = complex_logical_type::create_struct("s", fields);
    const vec::vector_t field_values[] = {scalar(std::int64_t{42}), scalar(std::string{"x"})};
    auto struct_back = round_trip(vec::make_struct(g_resource, struct_type, field_values), struct_type);
    REQUIRE(struct_back.has_value());
    REQUIRE(struct_back->entries().size() == 2);
    CHECK(struct_back->entries()[0]->get_value<std::int64_t>(0) == 42);
    CHECK(struct_back->entries()[1]->get_value<std::string_view>(0) == "x");
}

TEST_CASE("catalog::default_spec::explicit_null_default_is_not_absence") {
    const complex_logical_type column_type{logical_type::BIGINT};

    std::optional<vec::vector_t> absent;
    REQUIRE_FALSE(decode_default_spec(g_resource, column_type, "", absent).contains_error());
    CHECK_FALSE(absent.has_value()); // "" is the ONLY spelling of "no default"

    // DEFAULT NULL is a null row of the column's type; "is NULL" is is_null(0), never "the type is NA".
    std::string null_spec;
    REQUIRE_FALSE(encode_default_spec(g_resource, vec::make_null(g_resource, column_type), null_spec).contains_error());
    CHECK_FALSE(null_spec.empty());
    std::optional<vec::vector_t> null_default;
    REQUIRE_FALSE(decode_default_spec(g_resource, column_type, null_spec, null_default).contains_error());
    REQUIRE(null_default.has_value());
    CHECK(null_default->is_null(0));
    CHECK(null_default->type() == column_type);

    std::optional<vec::vector_t> value_default;
    std::string value_spec;
    REQUIRE_FALSE(encode_default_spec(g_resource, scalar(std::int64_t{5}), value_spec).contains_error());
    REQUIRE_FALSE(decode_default_spec(g_resource, column_type, value_spec, value_default).contains_error());
    REQUIRE(value_default.has_value());
    CHECK_FALSE(value_default->is_null(0));
    CHECK(value_default->get_value<std::int64_t>(0) == 5);

    CHECK(null_spec != value_spec);
}

TEST_CASE("catalog::default_spec::unencodable_default_is_an_error") {
    const auto hugeint = typed(logical_type::HUGEINT, static_cast<int128_t>(1) << 100);
    std::string spec;
    auto ec = encode_default_spec(g_resource, hugeint, spec);
    CHECK(ec.contains_error());
    CHECK(spec.empty());

    // An unrepresentable leaf makes the whole composite unrepresentable; a partially encoded default is not an option.
    const auto blob_array = complex_logical_type::create_array(complex_logical_type{logical_type::BLOB}, 2);
    CHECK_FALSE(cell_codec::is_encodable_type(blob_array));
}

TEST_CASE("catalog::default_spec::corrupt_spec_is_reported_not_ignored") {
    // A non-empty spec that fails to decode is catalog corruption, not "no default": the OLD
    // format, invalid hex, a truncated payload, and a NULL marker with a trailing byte.
    const complex_logical_type column_type{logical_type::BIGINT};
    std::optional<vec::vector_t> out;

    CHECK(decode_default_spec(g_resource, column_type, "int8:5", out).contains_error());
    CHECK_FALSE(out.has_value());
    CHECK(decode_default_spec(g_resource, column_type, "VZZ", out).contains_error());
    CHECK(decode_default_spec(g_resource, column_type, "V01", out).contains_error());
    CHECK(decode_default_spec(g_resource, column_type, "N!", out).contains_error());

    // An INTEGER default read against a BIGINT column is short by four bytes, not silently reinterpreted.
    std::string int_spec;
    REQUIRE_FALSE(encode_default_spec(g_resource, scalar(std::int32_t{5}), int_spec).contains_error());
    CHECK(decode_default_spec(g_resource, column_type, int_spec, out).contains_error());
}
