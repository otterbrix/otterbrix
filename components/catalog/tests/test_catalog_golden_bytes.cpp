#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>

#include <limits>
#include <optional>
#include <span>
#include <string>
#include <vector>

// pg_attribute.attdefspec and atttypspec are persisted. These tests pin today's exact text; a round trip alone
// stays green when encoder and decoder change together and old databases stop opening.

using namespace components::catalog;
using namespace components::types;
namespace vec = components::vector;

namespace {
    std::pmr::memory_resource* arena() {
        static core::pmr::otterbrix_resource resource;
        return &resource;
    }

    template<typename T>
    vec::vector_t scalar(T value) {
        return vec::make_scalar(arena(), std::move(value));
    }

    // make_scalar picks the logical type of T; the temporal and wider types name theirs.
    template<typename T>
    vec::vector_t typed(logical_type type, T value) {
        return vec::make_scalar(arena(), complex_logical_type{type}, std::move(value));
    }

    uint64_t list_length(const vec::vector_t& list) { return list.data<list_entry_t>()[0].length; }

    complex_logical_type make_decimal(uint8_t width, uint8_t scale) {
        auto created = complex_logical_type::create_decimal(arena(), width, scale);
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }

    std::string default_spec_of(const vec::vector_t& value) {
        std::string spec;
        REQUIRE_FALSE(encode_default_spec(arena(), value, spec).contains_error());
        return spec;
    }

    // The decoder must read the pinned text back to the same value, not just the encoder reproduce it.
    std::optional<vec::vector_t> decode_spec(const complex_logical_type& column_type, const std::string& spec) {
        std::optional<vec::vector_t> out;
        REQUIRE_FALSE(decode_default_spec(arena(), column_type, spec, out).contains_error());
        return out;
    }
} // namespace

// Layout: 'N' | 'V' + uppercase hex of [present=01][type tag][payload]; nested children repeat [present][tag][payload]
// (a NULL child is the single byte 00); LIST/ARRAY carry a u32 count, STRUCT carries none; DECIMAL carries only the
// scaled integer at its storage width -- width/scale come from the column type.
TEST_CASE("catalog golden: DEFAULT absent and NULL") {
    SECTION("absent default is the empty spec") {
        auto absent = decode_spec(complex_logical_type{logical_type::BIGINT}, "");
        REQUIRE_FALSE(absent.has_value());
    }
    SECTION("DEFAULT NULL is a lone 'N'") {
        REQUIRE(default_spec_of(vec::make_null(arena(), complex_logical_type{logical_type::BIGINT})) == "N");
        auto back = decode_spec(complex_logical_type{logical_type::BIGINT}, "N");
        REQUIRE(back.has_value());
        REQUIRE(back->is_null(0));
        REQUIRE(back->type() == complex_logical_type{logical_type::BIGINT});
    }
}

TEST_CASE("catalog golden: DEFAULT scalars") {
    REQUIRE(default_spec_of(scalar(std::int64_t{42})) == "V010E2A00000000000000");
    REQUIRE(default_spec_of(scalar(std::int64_t{-9007199254740993LL})) == "V010EFFFFFFFFFFFFDFFF");
    REQUIRE(default_spec_of(scalar(std::int32_t{-123456})) == "V010DC01DFEFF");
    REQUIRE(default_spec_of(scalar(3.5)) == "V01180000000000000C40");
    REQUIRE(default_spec_of(scalar(true)) == "V010A01");
    REQUIRE(default_spec_of(scalar(false)) == "V010A00");
    REQUIRE(default_spec_of(scalar(std::string{"abc"})) == "V012303000000616263");
    REQUIRE(default_spec_of(scalar(std::string{})) == "V012300000000");

    auto bigint = decode_spec(complex_logical_type{logical_type::BIGINT}, "V010E2A00000000000000");
    REQUIRE(bigint.has_value());
    REQUIRE(bigint->get_value<std::int64_t>(0) == 42);
    auto text = decode_spec(complex_logical_type{logical_type::TEXT}, "V012303000000616263");
    REQUIRE(text.has_value());
    REQUIRE(text->get_value<std::string_view>(0) == "abc");
}

TEST_CASE("catalog golden: DEFAULT DECIMAL carries no width/scale") {
    const auto narrow = make_decimal(10, 2);
    REQUIRE(default_spec_of(vec::make_decimal(arena(), narrow, 1234)) == "V0116D204000000000000");
    auto narrow_back = decode_spec(narrow, "V0116D204000000000000");
    REQUIRE(narrow_back.has_value());
    REQUIRE(narrow_back->get_value<std::int64_t>(0) == 1234);

    const auto small = make_decimal(4, 2);
    REQUIRE(default_spec_of(vec::make_decimal(arena(), small, -1234)) == "V01162EFB");

    const auto wide = make_decimal(38, 4);
    const int128_t raw = static_cast<int128_t>(std::int64_t{-9007199254740993LL}) * 1000000;
    REQUIRE(default_spec_of(vec::make_decimal(arena(), wide, raw)) == "V0116C0BDF0FFFFFFFFB717FEFFFFFFFFFFFF");
}

TEST_CASE("catalog golden: DEFAULT nested LIST, ARRAY, STRUCT") {
    const auto element = complex_logical_type{logical_type::INTEGER};

    SECTION("LIST") {
        const auto type = complex_logical_type::create_list(element);
        const vec::vector_t items[] = {scalar(std::int32_t{9}), scalar(std::int32_t{8})};
        const std::string spec = "V0165"
                                 "02000000"
                                 "010D09000000"
                                 "010D08000000";
        REQUIRE(default_spec_of(vec::make_list(arena(), element, items)) == spec);
        auto back = decode_spec(type, spec);
        REQUIRE(back.has_value());
        REQUIRE(list_length(*back) == 2);
        REQUIRE(back->entry().get_value<std::int32_t>(1) == 8);
    }
    SECTION("ARRAY, with a NULL element") {
        const auto type = complex_logical_type::create_array(element, 3);
        const vec::vector_t items[] = {scalar(std::int32_t{1}),
                                       vec::make_null(arena(), element),
                                       scalar(std::int32_t{3})};
        const std::string spec = "V016D"
                                 "03000000"
                                 "010D01000000"
                                 "00"
                                 "010D03000000";
        REQUIRE(default_spec_of(vec::make_array(arena(), element, items)) == spec);
        auto back = decode_spec(type, spec);
        REQUIRE(back.has_value());
        REQUIRE(back->entry().is_null(1));
        REQUIRE_FALSE(back->entry().is_null(2));
    }
    SECTION("STRUCT") {
        std::pmr::vector<complex_logical_type> fields{arena()};
        fields.emplace_back(complex_logical_type{logical_type::BIGINT, "a"});
        fields.emplace_back(complex_logical_type{logical_type::TEXT, "b"});
        const auto type = complex_logical_type::create_struct("s", fields);
        const vec::vector_t values[] = {scalar(std::int64_t{42}), scalar(std::string{"x"})};
        const std::string spec = "V0164"
                                 "010E2A00000000000000"
                                 "0123"
                                 "01000000"
                                 "78";
        REQUIRE(default_spec_of(vec::make_struct(arena(), type, values)) == spec);
        auto back = decode_spec(type, spec);
        REQUIRE(back.has_value());
        REQUIRE(back->entries().size() == 2);
        REQUIRE(back->entries()[0]->get_value<std::int64_t>(0) == 42);
    }
}

// atttypspec for ENUM is flat text: "ENUM:" name ":" label "=" value {"," label "=" value}, with
// \ ( ) , : = backslash-escaped inside names and labels.
TEST_CASE("catalog golden: ENUM type spec with three labels") {
    const auto type = complex_logical_type::create_enum("color", {{"red", 0}, {"green", 1}, {"blue", 2}});
    REQUIRE(encode_type_spec(type) == "ENUM:color:red=0,green=1,blue=2");

    auto decoded = decode_type_spec(arena(), "ENUM:color:red=0,green=1,blue=2");
    REQUIRE_FALSE(decoded.has_error());
    REQUIRE(decoded.value().type() == logical_type::ENUM);
    REQUIRE(decoded.value().type_name() == "color");
    const auto* ext = static_cast<const enum_logical_type_extension*>(decoded.value().extension());
    REQUIRE(ext->entries().size() == 3);
    REQUIRE(ext->entries()[0].label == "red");
    REQUIRE(ext->entries()[1].label == "green");
    REQUIRE(ext->entries()[2].label == "blue");
    REQUIRE(ext->entries()[2].value == 2);
}

TEST_CASE("catalog golden: ENUM type spec escapes delimiters") {
    const auto type = complex_logical_type::create_enum("mo:od", {{"a=b,c:d", 0}, {"(x)\\", 7}});
    REQUIRE(encode_type_spec(type) == "ENUM:mo\\:od:a\\=b\\,c\\:d=0,\\(x\\)\\\\=7");
}

// DATE shares its payload case with INTEGER and TIME / TIMESTAMP / TIMESTAMP_TZ with BIGINT, but each keeps its own tag.
TEST_CASE("catalog golden: DEFAULT remaining scalar widths and temporal types") {
    // clang-format off
    REQUIRE(default_spec_of(scalar(std::int8_t{-7})) == "V010BF9");
    REQUIRE(default_spec_of(scalar(std::uint8_t{200})) == "V011BC8");
    REQUIRE(default_spec_of(scalar(std::int16_t{-30000})) == "V010CD08A");
    REQUIRE(default_spec_of(scalar(std::uint16_t{60000})) == "V011C60EA");
    REQUIRE(default_spec_of(scalar(std::uint32_t{4000000000U})) == "V011D00286BEE");
    REQUIRE(default_spec_of(scalar(std::numeric_limits<std::uint64_t>::max())) ==
            "V011EFFFFFFFFFFFFFFFF");
    REQUIRE(default_spec_of(scalar(1.25F)) == "V01170000A03F");
    REQUIRE(default_spec_of(vec::make_decimal(arena(), make_decimal(9, 2), -123456789)) ==
            "V0116EB32A4F8");
    REQUIRE(default_spec_of(typed(logical_type::DATE, std::int32_t{19000})) ==
            "V0110384A0000");
    REQUIRE(default_spec_of(typed(logical_type::TIME, std::int64_t{86399123456LL})) ==
            "V01110000CA1D14000000");
    REQUIRE(default_spec_of(typed(logical_type::TIMESTAMP, std::int64_t{1700000000000000LL})) ==
            "V011300401E18240A0600");
    REQUIRE(default_spec_of(typed(logical_type::TIMESTAMP_TZ, std::int64_t{1700000000000000LL})) ==
            "V011400401E18240A0600");
    // clang-format on

    auto date = decode_spec(complex_logical_type{logical_type::DATE}, "V0110384A0000");
    REQUIRE(date.has_value());
    REQUIRE(date->type().type() == logical_type::DATE);
    auto stamp = decode_spec(complex_logical_type{logical_type::TIMESTAMP_TZ}, "V011400401E18240A0600");
    REQUIRE(stamp.has_value());
    REQUIRE(stamp->type().type() == logical_type::TIMESTAMP_TZ);
    auto small = decode_spec(complex_logical_type{logical_type::SMALLINT}, "V010CD08A");
    REQUIRE(small.has_value());
    REQUIRE(small->get_value<std::int16_t>(0) == -30000);
}

TEST_CASE("catalog golden: DEFAULT further nested shapes") {
    const complex_logical_type text{logical_type::TEXT};
    const complex_logical_type integer{logical_type::INTEGER};

    SECTION("LIST of TEXT") {
        const vec::vector_t items[] = {scalar(std::string{"a"}), scalar(std::string{"bc"})};
        const std::string spec = "V0165"
                                 "02000000"
                                 "012301000000"
                                 "61"
                                 "012302000000"
                                 "6263";
        REQUIRE(default_spec_of(vec::make_list(arena(), text, items)) == spec);
        auto back = decode_spec(complex_logical_type::create_list(text), spec);
        REQUIRE(back.has_value());
        REQUIRE(list_length(*back) == 2);
        REQUIRE(back->entry().get_value<std::string_view>(1) == "bc");
    }
    SECTION("empty LIST") {
        const std::string spec = "V016500000000";
        REQUIRE(default_spec_of(vec::make_list(arena(), integer, std::span<const vec::vector_t>{})) == spec);
        auto back = decode_spec(complex_logical_type::create_list(integer), spec);
        REQUIRE(back.has_value());
        REQUIRE(list_length(*back) == 0);
    }
    SECTION("STRUCT with a NULL field") {
        std::pmr::vector<complex_logical_type> fields{arena()};
        fields.emplace_back(complex_logical_type{logical_type::BIGINT, "a"});
        fields.emplace_back(complex_logical_type{logical_type::TEXT, "b"});
        const auto type = complex_logical_type::create_struct("s", fields);
        const vec::vector_t values[] = {vec::make_null(arena(), complex_logical_type{logical_type::BIGINT}),
                                        scalar(std::string{"x"})};
        const std::string spec = "V0164"
                                 "00"
                                 "012301000000"
                                 "78";
        REQUIRE(default_spec_of(vec::make_struct(arena(), type, values)) == spec);
        auto back = decode_spec(type, spec);
        REQUIRE(back.has_value());
        REQUIRE(back->entries().size() == 2);
        REQUIRE(back->entries()[0]->is_null(0));
        REQUIRE(back->entries()[1]->get_value<std::string_view>(0) == "x");
    }
    SECTION("LIST of LIST") {
        const auto inner_type = complex_logical_type::create_list(integer);
        const vec::vector_t first[] = {scalar(std::int32_t{1}), scalar(std::int32_t{2})};
        const vec::vector_t outer[] = {vec::make_list(arena(), integer, first),
                                       vec::make_list(arena(), integer, std::span<const vec::vector_t>{})};
        const std::string spec = "V0165"
                                 "02000000"
                                 "0165"
                                 "02000000"
                                 "010D01000000"
                                 "010D02000000"
                                 "0165"
                                 "00000000";
        REQUIRE(default_spec_of(vec::make_list(arena(), inner_type, outer)) == spec);
        auto back = decode_spec(complex_logical_type::create_list(inner_type), spec);
        REQUIRE(back.has_value());
        REQUIRE(list_length(*back) == 2);
        REQUIRE(back->entry().data<list_entry_t>()[0].length == 2);
        REQUIRE(back->entry().data<list_entry_t>()[1].length == 0);
    }
}
